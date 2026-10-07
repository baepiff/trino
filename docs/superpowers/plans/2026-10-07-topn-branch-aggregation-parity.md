# Aggregation parity for the base OpenSearch connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the base `opensearch` connector on branch `opensearch-aggregation-topn-pushdown` the aggregation features of `opensearch-sql-connector` that need no SQL plugin: text equality and text `GROUP BY` pushdown, opt-in `BIGINT` `min`/`max`/`sum`/`avg` with a 2^53 guard, and opt-in `stddev`/`variance`.

**Architecture:** Text features are cherry-picked from `opensearch-sql-connector`. `BIGINT` and statistical pushdown are new code in `MetricAggregation`, `OpenSearchQueryBuilder`, `AggregationResponseReader`, `OpenSearchConfig` and `OpenSearchSessionProperties`. All options default to false.

**Tech Stack:** Java 25, Maven (`./mvnw`), JUnit 5 and AssertJ, OpenSearch high-level REST client aggregations, Testcontainers OpenSearch 2.19.4.

**Spec:** `docs/superpowers/specs/2026-10-07-topn-branch-aggregation-parity-design.md`

## Global Constraints

- Work in the main checkout `/Users/Joongjin.Bae/github/trino` on the current branch `opensearch-aggregation-topn-pushdown`. Do not switch branches. Commit locally; never push.
- Before writing Java, read `.github/DEVELOPMENT.md` in full (project rule in `CLAUDE.md`). Run `./mvnw airstyle:format -pl plugin/trino-opensearch -Dair.check.skip-enforcer=true -nsu` after Java edits.
- The machine uses Zulu JDK 25, which the Maven enforcer rejects. Use `-Dair.check.skip-all=true -nsu` for test runs and `-Dair.check.skip-enforcer=true -nsu` when running `validate` or `airstyle`.
- Docker must be running for the integration tests (Rancher Desktop with the dockerd engine). `docker info` printed nothing on 2026-10-06, so start it first.
- New option names, all default `false`: `opensearch.bigint-aggregation-pushdown-enabled` (session `bigint_aggregation_pushdown_enabled`) and `opensearch.statistical-pushdown-enabled` (session `statistical_pushdown_enabled`). The cherry-picked options are `opensearch.text-equality-pushdown-enabled` and `opensearch.text-groupby-pushdown-enabled`.
- The precision limit is 2^53 = 9007199254740992. A `min`, `max` or `sum` result over a `BIGINT` column with magnitude of 2^53 or more fails with `TrinoException(OPENSEARCH_QUERY_FAILURE, ...)`. `avg` is never guarded. `BIGINT` input is never accepted for statistical functions.
- Never write the cluster URL or tenant id into any file; benchmarks take them from environment variables only.
- Commit messages end with the line `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. The git identity is not configured: pass `-c user.name='Joongjin Bae' -c user.email='joongjin.bae@automationanywhere.com'` on each commit.

## Review Focus

- A `BIGINT` `sum` of exactly 2^53 and of -2^53 fails with the guard error; 2^53 - 1 returns exactly. (Task 3)
- `BIGINT` `min`/`max`/`sum`/`avg` over an empty input or all-NULL column returns NULL (not 0, not an error). (Task 3)
- `GROUP BY` with a `BIGINT` aggregate where only one bucket exceeds 2^53 fails the whole query, not just that row. (Task 3)
- `stddev_samp` and `var_samp` over exactly one value return NULL; `stddev_pop` and `var_pop` over one value return 0. (Task 4)
- With every new option off, plans are identical to the branch before this work, including for `BIGINT` aggregates. (Task 3, Task 4)

---

### Task 1: Cherry-pick text equality pushdown

**Files:**
- Modify (via cherry-pick): `plugin/trino-opensearch/src/main/**` (`OpenSearchConfig`, `OpenSearchSessionProperties`, `OpenSearchMetadata`, `OpenSearchQueryBuilder`, `OpenSearchColumnHandle`, `client/IndexMetadata`), `docs/src/main/sphinx/connector/opensearch.md`, matching tests

**Interfaces:**
- Produces: option `opensearch.text-equality-pushdown-enabled`, session property `text_equality_pushdown_enabled`, `OpenSearchSessionProperties.isTextEqualityPushdownEnabled(ConnectorSession)`, `OpenSearchColumnHandle.keywordSubField()` returning `Optional<...>`. Task 2 builds on these.

- [ ] **Step 1: Cherry-pick in order** `03fa076a26f`, `539e9ed81af`, `83e59892252`, `eccad611934`, `5b2040da335` with `git cherry-pick`. These commits come from a branch with a different history (merge base `e6ab57534c0`), so conflicts are likely. Resolve each by keeping this branch's code plus the commit's change. Do not bring in anything for `trino-opensearch-sql`, `executeSql`, `SQL_FUNCTIONS` or the SQL cold-start retry (`23acd938daa`, `31519fc3e9b`). If a commit needs one of those, take only the lines it needs and note them in the commit message.
- [ ] **Step 2: Run the text-equality tests** that came with the commits: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchConfig,TestOpenSearchQueryBuilder,TestOpenSearchMetadata' -Dair.check.skip-all=true -nsu`, then the integration methods for text equality in `BaseOpenSearchConnectorTest` through `-Dtest=TestOpenSearchLatestConnectorTest#<method>` (list the methods with `git show --stat 03fa076a26f 83e59892252`). Expected: PASS.
- [ ] **Step 3: Verify defaults** by running `TestOpenSearchConfig` and confirming `text-equality-pushdown-enabled` defaults to false. Expected: PASS.
- [ ] **Step 4: Format and commit** the resolution (the cherry-picks themselves are commits; add one more only if the formatter changes files).

### Task 2: Cherry-pick text GROUP BY pushdown

**Files:**
- Modify (via cherry-pick): `OpenSearchConfig`, `OpenSearchSessionProperties`, `OpenSearchMetadata`, `TermAggregation`, `AggregateQueryPageSource`, `OpenSearchQueryBuilder`, `docs/src/main/sphinx/connector/opensearch.md`, matching tests

**Interfaces:**
- Consumes: Task 1's `keywordSubField()` and text-equality option.
- Produces: option `opensearch.text-groupby-pushdown-enabled`, session property `text_groupby_pushdown_enabled`.

- [ ] **Step 1: Cherry-pick in order** `eb9e8e27040`, `277663d247a`, `a7845127390`, `c9358cc5010`, resolving conflicts as in Task 1.
- [ ] **Step 2: Run** the `TermAggregation`/`AggregateQueryPageSource` unit tests and the text `GROUP BY` integration methods that came with `eb9e8e2` and `a784512` (`git show --stat` lists them). Expected: PASS, including the guard that fails when documents are missing from the `.keyword` subfield.
- [ ] **Step 3: Verify defaults and off behaviour:** a plan test (from the cherry-pick) shows a `GROUP BY` on a `text` column is not pushed down with the option off. Expected: PASS.

### Task 3: Opt-in BIGINT min, max, sum, avg

**Files:**
- Modify: `plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/{MetricAggregation,AggregationResponseReader,OpenSearchMetadata,OpenSearchConfig,OpenSearchSessionProperties}.java`, `docs/src/main/sphinx/connector/opensearch.md`
- Test: `TestAggregationModel`, `TestAggregationResponseReader`, `TestOpenSearchConfig`, `BaseOpenSearchConnectorTest` (`testBigintAggregatesAreNotPushedDown` at about line 283 is changed)

**Interfaces:**
- Produces:
  - `MetricAggregation.from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias, Set<String> allowedFunctions, boolean allowBigintInput): Optional<MetricAggregation>`; the existing three-argument `from` delegates with `DEFAULT_FUNCTIONS` and `false`.
  - `MetricAggregation.DEFAULT_FUNCTIONS: Set<String>` (`count`, `min`, `max`, `sum`, `avg`).
  - `OpenSearchConfig.isBigintAggregationPushdownEnabled(): boolean` and `setBigintAggregationPushdownEnabled(boolean): OpenSearchConfig` with `@Config("opensearch.bigint-aggregation-pushdown-enabled")`.
  - `OpenSearchSessionProperties.isBigintAggregationPushdownEnabled(ConnectorSession): boolean`, property `bigint_aggregation_pushdown_enabled`.
  - `AggregationResponseReader` private `checkBigintPrecision(MetricAggregation metric, @Nullable Double value)`, throwing `TrinoException(OPENSEARCH_QUERY_FAILURE, ...)` whose message names the function, the column, and `opensearch.bigint-aggregation-pushdown-enabled` (session `bigint_aggregation_pushdown_enabled`).

- [ ] **Step 1: Write failing unit tests** in `TestAggregationModel`: `testBigintIsAcceptedForMinMaxSumAvgWhenAllowed` (`from(..., DEFAULT_FUNCTIONS, true)` is present for each of the four functions) and change `testBigintIsRejectedForNumericFunctions` to also assert `from(..., DEFAULT_FUNCTIONS, false)` is empty. In `TestAggregationResponseReader`: `testBigintGuardBoundary` (a `sum` of 9007199254740991 returns that value; 9007199254740992 and -9007199254740992 each throw `TrinoException` with `OPENSEARCH_QUERY_FAILURE`, and the message contains `bigint-aggregation-pushdown-enabled`), `testBigintAvgIsNotGuarded` (an `avg` of 1.0E16 returns it), `testBigintGuardOnEmptyInput` (empty input still returns null for `sum`/`min`/`max`), and `testGroupedBigintGuardFailsWholeQuery` (two buckets, one at 2^53, throws). In `TestOpenSearchConfig`: defaults and explicit property mapping for `opensearch.bigint-aggregation-pushdown-enabled`.
- [ ] **Step 2: Run** `./mvnw -pl plugin/trino-opensearch test -Dtest='TestAggregationModel,TestAggregationResponseReader,TestOpenSearchConfig' -Dair.check.skip-all=true -nsu`. Expected: FAIL (new methods and properties missing).
- [ ] **Step 3: Implement** the signatures above. In `MetricAggregation.isSupportedInput`, accept `BIGINT` for `min`, `max`, `sum`, `avg` only when `allowBigintInput` is true (same shape as on `opensearch-sql-connector`). In `OpenSearchMetadata.applyAggregation`, pass `isBigintAggregationPushdownEnabled(session)`. In `AggregationResponseReader.metricValue`, call `checkBigintPrecision` for `min`, `max`, `sum` when the column type is `BIGINT`, using the limit constant `9007199254740992L` and comparing the absolute value with `>=`. The `BigintDecoder` already converts the `Double` to `long` via `Number.longValue()`; no page source change is expected.
- [ ] **Step 4: Run the Step 2 command.** Expected: PASS.
- [ ] **Step 5: Change the integration test.** Replace `testBigintAggregatesAreNotPushedDown` with `testBigintAggregatesAreNotPushedDownByDefault` (the existing assertions, unchanged) and add `testBigintAggregatesWithPushdownEnabled` using a session with `bigint_aggregation_pushdown_enabled=true`: `SELECT sum(nationkey), min(nationkey), max(nationkey), avg(nationkey) FROM nation` equals `VALUES (BIGINT '300', BIGINT '0', BIGINT '24', DOUBLE '12.0')` and is fully pushed down; `GROUP BY regionkey` is fully pushed down; a table with one `long` value of 9007199254740992 fails `sum` with a message containing `2^53`; the same query with the session property false returns the exact value through Trino. Run with `-Dtest=TestOpenSearchLatestConnectorTest#testBigintAggregatesWithPushdownEnabled+testBigintAggregatesAreNotPushedDownByDefault`. Expected: PASS (Docker running).
- [ ] **Step 6: Document** the option in `opensearch.md` next to the existing statement that `BIGINT` aggregates are not pushed down (about line 542): what it enables, the 2^53 guard, that `avg` is unguarded, that mixed-sign sums can round in intermediate steps, and the default.
- [ ] **Step 7: Format and commit** (`airstyle:format`, then `git add` the changed files and commit "Add opt-in pushdown of BIGINT min, max, sum and avg to the base connector").

### Task 4: Opt-in stddev and variance

**Files:**
- Modify: `MetricAggregation`, `OpenSearchQueryBuilder` (aggregation switch at about line 108), `AggregationResponseReader`, `OpenSearchMetadata`, `OpenSearchConfig`, `OpenSearchSessionProperties`, `docs/src/main/sphinx/connector/opensearch.md`
- Test: `TestAggregationModel`, `TestOpenSearchQueryBuilder`, `TestAggregationResponseReader` (its `REGISTRY` needs an `extended_stats` entry for `ParsedExtendedStats`), `TestOpenSearchConfig`, `BaseOpenSearchConnectorTest`

**Interfaces:**
- Consumes: Task 3's `from(..., Set<String> allowedFunctions, boolean allowBigintInput)`.
- Produces:
  - Constants `MetricAggregation.STDDEV_SAMP = "stddev_samp"`, `STDDEV_POP = "stddev_pop"`, `VAR_SAMP = "var_samp"`, `VAR_POP = "var_pop"`, `STATISTICAL_FUNCTIONS: Set<String>`, and `canonicalFunctionName(String): String` (`stddev` to `stddev_samp`, `variance` to `var_samp`, others unchanged), used inside `from`.
  - `OpenSearchConfig.isStatisticalPushdownEnabled()/setStatisticalPushdownEnabled(boolean)` with `@Config("opensearch.statistical-pushdown-enabled")`; `OpenSearchSessionProperties.isStatisticalPushdownEnabled(ConnectorSession)`, property `statistical_pushdown_enabled`.
  - Query builder: statistical functions build `AggregationBuilders.extendedStats(alias).field(field)`.

- [ ] **Step 1: Precision experiment first (decides the documentation).** Add `testExtendedStatsPrecision` to a new `TestOpenSearchExtendedStats` (Docker, OpenSearch 2.19.4, following `TestOpenSearchLatestConnectorTest`'s server setup): index five `double` values `1e9, 1e9+1, 1e9+2, 1e9+3, 1e9+4`, run an `extended_stats` aggregation through the client, and assert it with the values it actually returns. Record the observed variance and standard deviation in the test's comment and in Task 4's commit message. The correct values are variance 2.0 (population) and 2.5 (sample). Expected: the test passes with whatever OpenSearch returns; the observed values decide whether the docs say "imprecise" (spec assumption) or not.
- [ ] **Step 2: Write failing unit tests:** `TestAggregationModel.testStatisticalFunctionsAcceptedInputs` (accepted for `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE` only when the allowed set includes `STATISTICAL_FUNCTIONS`; rejected for `BIGINT` even with `allowBigintInput` true; `stddev` and `variance` map to `stddev_samp` and `var_samp`); `TestOpenSearchQueryBuilder.testStatisticalAggregationUsesExtendedStats`; `TestAggregationResponseReader.testStatisticalValues` with count 5 and `testSampleStatisticsOverOneValueAreNull` (`stddev_samp`, `var_samp` return null at count 1, `stddev_pop` and `var_pop` return 0.0) and `testStatisticalOverEmptyInputIsNull`; `TestOpenSearchConfig` for the new property.
- [ ] **Step 3: Run** `./mvnw -pl plugin/trino-opensearch test -Dtest='TestAggregationModel,TestOpenSearchQueryBuilder,TestAggregationResponseReader,TestOpenSearchConfig' -Dair.check.skip-all=true -nsu`. Expected: FAIL.
- [ ] **Step 4: Implement** the signatures above. In `OpenSearchMetadata.applyAggregation`, the allowed set is `DEFAULT_FUNCTIONS` plus `STATISTICAL_FUNCTIONS` when `isStatisticalPushdownEnabled(session)`. In the reader, read the value from `ExtendedStats` with `getStdDeviationSampling`, `getStdDeviationPopulation`, `getVarianceSampling`, `getVariancePopulation`; return null for NaN, infinite, or empty input, and for the two sample functions when the count is below 2.
- [ ] **Step 5: Run the Step 3 command.** Expected: PASS.
- [ ] **Step 6: Integration test** `testStatisticalAggregatesWithPushdownEnabled` in `BaseOpenSearchConnectorTest`: with `statistical_pushdown_enabled=true`, `SELECT stddev(<double column>), variance(<double column>) FROM <small table>` equals the result with the property false (compare with `assertThat(query(session, sql)).matches(query(offSession, sql))`-style checks on a data set that is not precision sensitive) and is fully pushed down; with the default session it is not pushed down. Run with `-Dtest=TestOpenSearchLatestConnectorTest#testStatisticalAggregatesWithPushdownEnabled`. Expected: PASS.
- [ ] **Step 7: Document** the option in `opensearch.md` using the Step 1 result, and **commit** ("Add opt-in pushdown of stddev and variance to the base connector").

### Task 5: Verify, measure and report

**Files:**
- Create (untracked, never committed): `plugin/trino-opensearch/src/test/java/io/trino/plugin/opensearch/BenchmarkOpenSearchTopNCluster.java` (copy of the file in `../trino-topn-wt`; set options `opensearch.bigint-aggregation-pushdown-enabled`, `opensearch.statistical-pushdown-enabled`, `opensearch.text-equality-pushdown-enabled`, `opensearch.text-groupby-pushdown-enabled` to `true` through `OS_BENCH_OPTIONS=on`)
- Modify: Confluence page 7952007348 (after re-reading its latest version)

- [ ] **Step 1: Full checks.** `./mvnw validate -pl plugin/trino-opensearch -Dair.check.skip-enforcer=true -nsu` and `./mvnw airstyle:check -pl plugin/trino-opensearch -Dair.check.skip-enforcer=true -nsu`. Expected: both succeed with no output under `-q`.
- [ ] **Step 2: Full module tests.** `./mvnw -pl plugin/trino-opensearch test -Dair.check.skip-all=true -nsu` (Docker running). Expected: 0 failures. Record the skipped count.
- [ ] **Step 3: Benchmark options on.** Copy the benchmark class, then run with the cluster URL and tenant id as environment variables only: `OS_SQL_URL=... OS_BENCH_TENANT=... OS_BENCH_OPTIONS=on OS_BENCH_ROUNDS=25 OS_BENCH_ONLY=A,B,C,D,E,G,H,K,L ./mvnw -pl plugin/trino-opensearch test -Dtest=BenchmarkOpenSearchTopNCluster -Dair.check.skip-all=true -nsu`. Expected: every query finishes in seconds except the whole-table `SUM` (query D), which fails with the guard error by design. Keep the laptop awake and on the VPN.
- [ ] **Step 4: Update Confluence.** Read the latest page first, then add the results beside the existing "Base connector on the topn branch" section (keep that section, which describes commit `25809865f3b`), update the settings table and the "Known gaps", and keep the page's AI-assisted status line.
- [ ] **Step 5: Report** the commit list, test counts, the benchmark table, and anything not done (the group-count comparison against an option-off run is still unavailable because of the 10-minute cap).
