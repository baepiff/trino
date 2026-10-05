# OpenSearch SQL connector, sub-project 1: foundation and aggregation

Date: 2026-10-02
New module: `plugin/trino-opensearch-sql` (connector name `opensearch-sql`)
Builds on: `plugin/trino-opensearch` branch `opensearch-aggregation-topn-pushdown` (DSL aggregation and TopN pushdown, unmerged).
Target: OpenSearch 2.19 (SQL plugin, V2 engine).

## Goal

A connector that uses the OpenSearch SQL plugin (`POST /_plugins/_sql`) to push down aggregations the search-DSL connector cannot or does not push, without losing the shard-parallel scans and paginated DSL `GROUP BY` of `trino-opensearch`.

## Roadmap (this document covers only sub-project 1)

1. **Foundation and aggregation (this spec).** Module, SQL transport, WHERE rendering, global aggregates via SQL including `stddev`/`variance`, correctness guards.
2. Expression pushdown: `ConnectorExpression` to SQL for `WHERE`, `GROUP BY`, `ORDER BY` and aggregate arguments.
3. Join and subquery feasibility spike against a live cluster (V2 has no joins or subqueries; V1 supports joins best effort).
4. Hardening: docs, Docker integration tests on 2.19, comparison with the DSL connector.

## Non-goals (sub-project 1)

- Scans through the SQL plugin (scans keep the shard-parallel scroll path).
- `GROUP BY` through the SQL plugin (see "Verified behaviour": silent truncation at 1,000 groups).
- Expressions in predicates, projections, grouping or aggregate arguments.
- Joins, subqueries, `count(DISTINCT)`, `BIGINT` `min`/`max`/`sum`/`avg`, SQL cursors, writes.

## Verified behaviour of the SQL plugin on OpenSearch 2.19

Probed read-only against a shared AWS OpenSearch Service 2.19 domain (reports `7.10.2` in compatibility mode, Lucene 9.12.1, `opensearch-sql` plugin present), using a 2.1M-document index with `long`, `float`, `boolean`, `date` and `text`+`keyword` fields. No data values were recorded.

| Behaviour | Result | Consequence |
|---|---|---|
| Plain `SELECT`, no `LIMIT` | 10,000 rows plus a cursor; `fetch_size` cursors walk further (V2 cursors work) | Not used in this sub-project; scans stay on scroll |
| `GROUP BY` with more than 1,000 groups | Exactly 1,000 rows returned even with `LIMIT 50000`; no cursor; explain shows a composite aggregation `size: 1000` that is never paged | `GROUP BY` cannot be pushed through SQL safely; truncation is silent |
| `sum()` over zero rows | Returns `0` (SQL requires NULL); `avg`/`min`/`max` return NULL | Emit `count(col)` beside every `sum` and map `count = 0` to NULL |
| `BIGINT` arithmetic in `max`/`min` | `max(version*0 + 9007199254740993)` returned `...992` (double precision); large `sum` saturates at `Long.MAX_VALUE` | `BIGINT` `min`/`max`/`sum`/`avg` never pushed |
| `count(DISTINCT x)` | Runs, but OpenSearch implements it with approximate `cardinality` | Never pushed |
| `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp` | Run, return `double` | Pushed for global aggregates |
| Aggregates and `GROUP BY` over expressions | Run (the docs say otherwise) | Deferred to sub-project 2 |
| `filter` request parameter with an aggregate | **Silently ignored**: filtered `count(*)` returned all rows and the result schema flipped to `double` (V1 engine fallback); `stddev` with a filter returned an empty result | `WHERE` must be rendered into the SQL text; `filter` is never used |
| V2 failure | Query silently falls back to V1, which changes result types | Validate the returned schema types against expected types and fail on mismatch |
| `text` fields | `GROUP BY text_field` uses its `.keyword` subfield automatically; `text.keyword` cannot be named | Not relevant to global aggregates; note for sub-project 2 |
| Type names in `schema` | `long`, `float`, `double`, `boolean`, `timestamp`, `text`, `keyword`, `integer` | Used for the type check |
| `ORDER BY ... LIMIT n` | Pushed as sorted search with `size = n` (like the DSL TopN) | Nothing to do |

## Architecture

```
Trino ── opensearch-sql connector (plugin/trino-opensearch-sql)
          ├─ OpenSearchSqlMetadata extends OpenSearchMetadata
          │    applyAggregation: global + SQL-eligible → Type.SQL_AGGREGATION handle
          │                      otherwise → super.applyAggregation (DSL path)
          ├─ OpenSearchSqlPageSourceProvider (wraps the base provider)
          │    SQL_AGGREGATION → SqlAggregatePageSource ; everything else → base provider
          ├─ SqlWhereRenderer   TupleDomain/regexes/LIKE → SQL predicate text
          ├─ SqlAggregationQueryBuilder  handle → SQL text + expected schema
          └─ OpenSearchSqlClient  POST /_plugins/_sql, parse schema/datarows
trino-opensearch (library dependency, additive changes only)
```

### Wiring

- New Maven module `plugin/trino-opensearch-sql` depends on `trino-opensearch` (as `trino-lakehouse` depends on the Hive/Iceberg/Delta plugins). It registers `OpenSearchSqlPlugin` with a connector factory named `opensearch-sql` that installs the existing connector module with overridden bindings for `ConnectorMetadata` and `ConnectorPageSourceProvider`.
- All existing `opensearch.*` properties apply unchanged (hosts, TLS, auth, scroll size, `opensearch.aggregation-pushdown-enabled`, `opensearch.max-aggregation-buckets`, projection pushdown). New property: `opensearch.sql.global-aggregation-engine` = `SQL` (default) | `DSL`. With `DSL`, global aggregates use the existing DSL metric aggregations, and the SQL path is used only for queries containing a `stddev`/`variance` function (the DSL path cannot compute those). Session property `opensearch.sql.global_aggregation_engine` mirrors it (catalog session property name `global_aggregation_engine`).
- Additive changes in `trino-opensearch`: `OpenSearchTableHandle.Type` gains `SQL_AGGREGATION` (carries `metricAggregations` only, `termAggregations` empty); `OpenSearchSplitManager` treats it like `AGGREGATION` (one whole-index split); `MetricAggregation.from` gets an overload taking the set of allowed function names (default set unchanged; `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP` constants added); `OpenSearchClient` exposes a method to send a JSON request to an arbitrary endpoint through the existing authenticated, backpressure-aware client; `PushdownColumns` and `BuiltinColumns` helpers become public.

### `applyAggregation` rules (SQL path)

Eligible when all hold: `opensearch.aggregation-pushdown-enabled`, engine `SQL` (or the query contains a statistical function), one grouping set that is empty (global), no existing TopN/limit/aggregation on the handle, not a passthrough query. Each aggregate must be one of: `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`; no `DISTINCT`, filter or ordering; the argument is a plain column handle passing `PushdownColumns.isDocValuesPushdownSupported`. `min`/`max` accept `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE`; `sum`/`avg`/statistical functions accept `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE`. `BIGINT` and `REAL` (for `sum`/`avg`/statistical) are excluded, as in the DSL connector. If any aggregate is ineligible the whole call falls back to `super.applyAggregation`, which may push a DSL aggregation or nothing.

Output types: `count` → `BIGINT`, `sum` over integral → `BIGINT`, `sum`/`avg`/statistical over `DOUBLE` → `DOUBLE`, `avg` over integral → `DOUBLE`, `min`/`max` → input type. Synthetic output columns are named `_pushdown_<i>` and never support predicates.

### SQL generation

`SELECT <agg list> FROM \`<index>\` [WHERE <predicate>]`, single statement, no `LIMIT`.

- Each `sum(x)` is followed by a hidden `count(x)` column; the reader maps `count = 0` to NULL for the sum. `avg`, `min`, `max`, and statistical functions already return NULL on empty input; statistical functions with fewer rows than their definition needs are mapped per the SQL plugin's result and covered by integration tests.
- Identifiers are backtick-quoted with embedded backticks rejected. String literals use single quotes with `'` doubled; the renderer supports only: `BOOLEAN`, `TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT` equality and ranges, `REAL`/`DOUBLE` ranges, `VARCHAR` (keyword) equality, ranges and `IN` lists, `TIMESTAMP(3)` as `'yyyy-MM-dd HH:mm:ss.SSS'` UTC literals, `NULL`/`NOT NULL`, regex-backed `LIKE`. Anything else (including a raw `query` string on the handle) disables the SQL path, so the DSL path or no pushdown applies instead.
- The renderer is a pure function with a golden test per type and predicate shape.

### Execution and errors

- `SqlAggregatePageSource` sends one request and converts the single result row. The index comes from the handle (alias and wildcard indices work in SQL as in DSL).
- Expected schema types are derived from the aggregates (`count` → `long`, `avg` → `double`, `min`/`max`/`sum` → field type or `long`/`double`, statistical → `double`). A response whose schema or column count differs is a V1 fallback or a plugin error: fail with `OPENSEARCH_QUERY_FAILURE` naming the expected and actual types and pointing at `opensearch.sql.global-aggregation-engine=DSL`.
- HTTP errors and `status != 200` map to the existing OpenSearch error codes with the plugin's `reason`/`details` text (no query text with literals in logs at INFO).
- Timeouts, retries and backpressure come from the existing client.

## Testing

- Unit: `SqlWhereRenderer` goldens (every supported type and shape, quoting edge cases such as `'`, backtick, unicode, `NaN`/infinities rejected); `SqlAggregationQueryBuilder` (companion `count` for `sum`, aliasing, expected schema); response reader (empty input, `count = 0` sum → NULL, schema mismatch, error payloads); `OpenSearchSqlMetadata.applyAggregation` accepted and rejected shapes, fallback to DSL, engine switch, session property.
- Integration (OpenSearch 2.19 testcontainer via the existing `OpenSearchServer`): global aggregates on `integer`/`double`/`float`/`long` fields including empty input and fields with missing values; `stddev`/`variance`; predicates of each type feeding the SQL `WHERE`; `GROUP BY` stays on the DSL path and returns more than 1,000 groups correctly; schema-mismatch error path; `BIGINT` aggregates stay in Trino.
- Plan assertions use `isFullyPushedDown` / `isNotFullyPushedDown(AggregationNode.class)` as in the DSL connector tests.

## Risks and open items

- Documentation for the SQL plugin is unreliable (it contradicts observed V2 behaviour in several places). Every behaviour in the table above is probed; the integration tests must re-verify them on the testcontainer image before merge.
- Behaviour differs across OpenSearch versions (the legacy-fallback rule, size caps). The connector supports the versions the testcontainer suite covers; it is validated on 2.19 first.
- The SQL path currently adds little for the base aggregate set; its value in this sub-project is `stddev`/`variance`, the shared infrastructure, and the A/B switch against the DSL path. The payoff comes with sub-project 2.
- `trino-opensearch` is not yet merged; this module depends on its unmerged branch and must be rebased with it.
- Authentication beyond what `trino-opensearch` already supports (basic, AWS SigV4) is out of scope.
