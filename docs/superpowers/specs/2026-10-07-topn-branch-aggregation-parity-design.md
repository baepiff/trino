# Aggregation parity for the base OpenSearch connector (topn branch)

Date: 2026-10-07. Branch: `opensearch-aggregation-topn-pushdown` (worktree `../trino-topn-wt`).

## Purpose

The base `opensearch` connector on this branch pushes down `count`, `min`, `max`, `sum` and `avg` through the search API (DSL), but never over `BIGINT` columns. Measured on 2026-10-06 against the `process_composer_request` index (about 4.06 million documents), `min`, `max`, `sum` and `avg` of the `BIGINT` column `requestDuration`, every query filtered by a `text` field, and `GROUP BY` on a `text` field all hit the 10-minute cap because Trino scans the whole index. The branch `opensearch-sql-connector` has opt-in features that remove those scans. The goal is to bring every one of those aggregation features that does not need the OpenSearch SQL plugin into the base connector, using the search API only.

Success criteria:

1. With the new options on, the 2026-10-06 benchmark queries A to E, G, H, K and L run in seconds through the `opensearch` catalog, not 10 minutes or more.
2. With the options off (the defaults), behaviour is identical to today's branch.
3. A result that may be inexact is never returned silently: it fails with a clear error.

## Scope

In scope (agreed with the user on 2026-10-07, items 1 to 3 of the comparison):

1. `BIGINT` `min`, `max`, `sum`, `avg`, opt-in, with a precision guard.
2. `stddev` and `variance` (`stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`), opt-in.
3. Equality filters (`=`, `IN`) and `GROUP BY` on `text` fields through the `.keyword` subfield, opt-in, with the existing guards.

Out of scope: the OpenSearch SQL plugin engine (`POST /_plugins/_sql`, the `opensearch_sql` connector), expression pushdown, `INTEGER` `sum`/`avg` cast pushdown, joins. Nothing is pushed to any remote.

## Design

### 3. Text equality and text GROUP BY: take the existing commits

These features are DSL-only and already reviewed on `opensearch-sql-connector`. Cherry-pick, in order: `03fa076` (text equality), `83e5989` (session switch), `5b2040d` (property naming), `eb9e8e2` (text GROUP BY), `a784512` (verify coverage in every page, require sub-field doc values), and their documentation commits (`277663d`, `c9358cc`). Resolve conflicts against this branch; do not take any change that exists only for the SQL connector (`executeSql`, SQL plugin properties, `SQL_FUNCTIONS`). Options: `opensearch.text-equality-pushdown-enabled` and `opensearch.text-groupby-pushdown-enabled`, both default false, with session properties of the same name in snake case.

### 1. BIGINT min, max, sum, avg: new DSL code

- Option `opensearch.bigint-aggregation-pushdown-enabled`, session property `bigint_aggregation_pushdown_enabled`, default false. Same style as the other options in `OpenSearchConfig` and `OpenSearchSessionProperties`.
- `MetricAggregation.from(..., allowBigintInput)` is added (the shape already used on `opensearch-sql-connector`) and `OpenSearchMetadata` passes the session value. With the option off, `BIGINT` stays excluded.
- Guard in `AggregationResponseReader`: after reading a `min`, `max` or `sum` value over a `BIGINT` column, if its magnitude is 2^53 or more, throw `TrinoException(OPENSEARCH_QUERY_FAILURE, ...)` with a message that names the function, the column and the option to turn off. `avg` is not guarded because its Trino type is `DOUBLE`. Null and empty-input handling stay as they are.
- The guard checks the final value only. A `sum` over mixed-sign values can round in an intermediate step and still end below the limit; this is documented, as on `opensearch-sql-connector`.
- A `BIGINT` result must be returned to Trino as a `long`. The reader currently returns `Double`; the conversion for `BIGINT` outputs must be verified in the page source path and covered by a test (see Risks).
- The whole-table `SUM(requestDuration)` (about 9.08e15, above 2^53) is expected to fail with the guard error. This is by design.

### 2. stddev and variance: new DSL code

- Option `opensearch.statistical-pushdown-enabled`, session property `statistical_pushdown_enabled`, default false.
- `MetricAggregation` gains the four statistical function names and `canonicalFunctionName` (`stddev` to `stddev_samp`, `variance` to `var_samp`), as on `opensearch-sql-connector`. `BIGINT` input is never accepted for them. Supported inputs: `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE`.
- `OpenSearchQueryBuilder` builds `AggregationBuilders.extendedStats(alias).field(field)`; the reader takes the matching value from `ExtendedStats` (`getStdDeviationSampling`, `getStdDeviationPopulation`, `getVarianceSampling`, `getVariancePopulation`).
- SQL semantics: sample statistics over fewer than two values must be NULL, and population statistics over one value must be handled as Trino does. The expected results are decided by the tests in this spec's test plan, not assumed.
- Precision: `extended_stats` is believed to use a sum-of-squares method like the SQL plugin, so it may lose precision for large values with a small spread (the `1e9 + (0..4)` example returned 0.0 on the SQL plugin). This is an assumption. It is verified first, in a test against OpenSearch 2.19.4, before the option is documented; the option stays opt-in and documented as imprecise in either case.

### Interaction rules

- All new options default to false, so the default behaviour of the branch does not change.
- The options apply to global aggregates and to `GROUP BY` through the existing composite aggregation path; the guard applies to every bucket's value.
- Existing exclusions stay: `DISTINCT`, filtered aggregates, ordered aggregates, `REAL` for `sum`/`avg`, and `INTEGER` `sum`/`avg` (which Trino plans over a cast).

## Testing

- Unit tests: `MetricAggregation.from` for each function and input type with the BIGINT and statistical options on and off; the guard (below, at and above 2^53, both signs, null, `avg` unguarded); the query builder for `extended_stats`; `AggregationResponseReader` for statistical values and empty input.
- Plan tests: `BIGINT` min/max/sum/avg and statistical aggregates are pushed only with their option on; text filter and text `GROUP BY` plans as in the cherry-picked tests.
- Docker integration tests on OpenSearch 2.19.4 (and the versions the cherry-picked text tests already cover): results equal Trino's own results for small data; the guard error for a `BIGINT` sum at 2^53; the precision experiment for `extended_stats`.
- Formatting: `./mvnw airstyle:check` with `-Dair.check.skip-enforcer=true` (the machine uses Zulu 25); `./mvnw validate` likewise.
- Benchmark: rerun queries A to E, G, H, K and L through the `opensearch` catalog with the options on, plus one run of K and L with the options off to compare group counts if the 10-minute cap allows. Update the Confluence page after re-reading its latest version.

## Risks

1. `Double` to `BIGINT` conversion in the aggregate page source may not exist for `BIGINT` outputs. Verified first with a test; the fix is local to `AggregateQueryPageSource` or the reader.
2. Cherry-picks may conflict with changes made on this branch after the commits were written (`e6ab575`, `0904fd3`, `2580986`). Resolve by hand and rerun the cherry-picked tests.
3. The `extended_stats` precision behaviour is unverified (above).
4. Changing this branch changes the "plain DSL" baseline measured on 2026-10-06 (commit `25809865f3b`). The Confluence page keeps that section and adds the new results beside it.
5. Reviewers of the original `opensearch-sql-connector` work should confirm the shared option names do not clash when the two branches meet again.
