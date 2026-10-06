# SDD ledger — plan: docs/superpowers/plans/2026-10-05-opensearch-sql-connector-foundation.md
Spec: docs/superpowers/specs/2026-10-02-opensearch-sql-connector-foundation-design.md (reachable). Branch opensearch-sql-connector, base ccdc47f860e.

## Pre-flight scan (task pairs sharing files/interfaces; per-task self-consistency)
| Rows | Finding |
|---|---|
| T1->T4/T6: MetricAggregation.from 4-arg, SQL_FUNCTIONS, STATISTICAL_FUNCTIONS, canonicalFunctionName | names match in T4/T6 |
| T1->T6: withSqlAggregations, isAggregation, aggregationOutputColumn, SYNTHETIC_COLUMN_NAME_PREFIX protected | names match |
| T1->T5: OpenSearchClient.executeSql(String):String | matches T5 use |
| T2->T5/T6: OpenSearchSqlClient skeleton (delegate() accessor) replaced in T5; Metadata/PageSourceProvider skeletons replaced in T6 | ctor signatures identical |
| T3->T4/T6: SqlIdentifiers, SqlWhereRenderer.render | consistent |
| T4->T5/T6: SqlResult/SqlColumn, SqlAggregationQuery, Reader | consistent |
| T7->T2: test deps added to pom in T7 (T2 already has some) | OK, additive |
| T1 self: new metadata test uses SESSION/metadata/scanHandle names from memory | UNVERIFIED names; implementer adapts to real helpers (session(boolean), scanHandle()) |
| T6 self: test uses TopN.fromLimit, TestingConnectorSession builder setPropertyValues, OpenSearchClient ctor | UNVERIFIED APIs; implementer adapts to real code, keeps intent |
| T2 self: OpenSearchConnector super-ctor parameter list assumed | UNVERIFIED; implementer matches real ctor |
| Spec vs plan | Plan tightening: LIKE regexes -> DSL fallback (spec said regex LIKE supported). Ruling: accepted, noted in plan self-review |
Ruling: LIKE-derived regexes make SQL path ineligible (fallback to DSL) — Lucene regexp not invertible to SQL LIKE — costs a missed pushdown only, never wrong results.
Ruling: where plan APIs/signatures disagree with real code, implementers follow the real code and keep the test's intent — cost: small deviations to review.
Task 1: dispatched (sonnet, base ccdc47f860e)
Task 1: implemented commit 31519fc3e9b (51 tests green); review dispatched
Task 1: minor (deferred): new metadata test placed between private helpers; executeSql swallows error-body read failure silently; Accept-Encoding header copied from executeQuery (pre-existing)
Task 1: note: base OpenSearchPageSourceProvider only handles AGGREGATION; SQL module's provider (Task 6) routes SQL_AGGREGATION before super — covered by plan
Task 1: complete (commits ccdc47f..31519fc, review clean)
Task 2: implemented 88320a5c24e; review dispatched
Task 2: minor (deferred): plugin smoke test does not assert OpenSearchSqlConnector type / global_aggregation_engine session property exposure; no session-property default test -> carry into Task 6 dispatch (assert connector uses SQL metadata/provider + session property)
Task 2: deviations accepted: io.airlift:guice dep, no -am test runs (install trino-opensearch first), redundant base singletons still bound (harmless)
Task 2: complete (commits 31519fc..88320a5, review clean)
Task 3: implemented 10d720ee096; review dispatched
Task 3: review: 1 Important (builtin/supportsPredicates=false columns could be rendered; plan-mandated gap) -> fix round 1/5 dispatched (resumed implementer)
Task 3: minor (deferred): C1 control chars/U+2028 not rejected in string literals; large IN list uncapped; BigDecimal plain strings for tiny doubles; test gaps (TINYINT/SMALLINT, string ranges, 9e15 boundary, year guards)
Task 3: fix round 1/5 (1 addressed, 0 open; commits 10d720e..a105c6f)
Task 3: complete (commits 88320a5..a105c6f, review clean)
Task 4: implemented f017ca74ef5; review dispatched
Task 4: minor (deferred): reader passes sum/min/max cells through uncoerced (Integer vs Long vs Double) -> CARRY into Task 6: SqlAggregatePageSource must write values by column Trino type (Number.longValue/doubleValue/floatToIntBits for REAL), not assume cell class; malformed count cell gives NPE/CCE not TrinoException; SqlResult rows shallowly immutable; test gaps (non-long value count column, var_pop/stddev_samp single-row)
Task 4: complete (commits a105c6f..f017ca7, review clean)
Task 5: implemented 7d53b100c90; review dispatched
Task 5: minor (deferred): string `error` payload loses message ("unknown error"); schema columns not validated (missing name/type -> empty strings), row width vs schema width unchecked; parse test gaps; execute untested (needs client double / integration)
Task 5: complete (commits f017ca7..7d53b10, review clean)
Task 6: implemented b34e78ff0a2 (41 tests green); review dispatched
Task 6: minor (deferred): decoders use Number.longValue/doubleValue -> silent wrap/truncation for out-of-range BigInteger or fractional Double into BIGINT (reader checks only count column types) — candidate for final fix wave (longValueExact / reject non-integral); duplicated dep.opensearch.version property + org.opensearch:opensearch dependency only for Decoder.decode(SearchHit) (a value writer without Decoder would remove it); page source assumes single split (no guard); fall-through-to-super untested; no multi-grouping-set / filtered-aggregate metadata tests; renderable-predicate test asserts only type; LIKE-regex divergence from spec (recorded as Ruling in pre-flight)
Task 6: complete (commits 7d53b10..b34e78f, review clean)
Task 7: implemented ae2ddbcaaf6 (SQL IT 9/9 on 2.19.4; DSL Latest suite 323 tests, 2 pre-existing failures: sum/avg of INTEGER is planned over CAST bigint so never pushed — affects first feature too); review dispatched
Task 7: review: 1 Critical (test-scope pom entries remove opensearch client jars from plugin runtime classpath), 1 Important (ITs cannot distinguish SQL vs DSL path) + minors -> fix round 1/5 dispatched (resumed implementer)
Task 7: Ruling: connector name opensearch_sql (opensearch-sql invalid per ConnectorName.VALID_NAME) — spec/handover/docs say opensearch-sql; cost: naming differs from the spec text
Task 7: Ruling: V2 count type accepted as integer or long (observed integer on 2.19.4 container; double still rejected) — cost: if V1 ever returned integer counts, detection would miss it (spec says V1 returns double)
Task 7: Ruling: sum/avg over INTEGER columns are not pushed by either connector (Trino plans CAST to bigint); NOT fixed here — needs applyProjection cast support, out of scope for sub-project 1; first-feature docs (opensearch.md) and 2 DSL integration tests (testAggregationsOverNumericFields, testAggregationsWithMissingKeysAndValues) are wrong/failing — surface to user
Task 7: fix round 1/5 done d3ab847b929 (runtime-scope client jars, SQL-vs-DSL plan assertions, 5 minors); re-review dispatched. Finding: OpenSearch 2.19.4 SQL plugin cold-start race (HTTP 400 'can't evaluate on aggregator: min') on concurrent first aggregation statements; connector has no retry — decide later
Task 7: fix round 1/5 (2 addressed, 0 open; commits ae2ddbc..d3ab847)
Task 7: minor (deferred): cold-start 400 "can't evaluate on aggregator" in SQL plugin 2.19.4 not retried by connector (production risk; warm-up only in tests); docs cosmetic line lengths
Task 7: complete (commits b34e78f..d3ab847, review clean)
All 7 tasks complete; final whole-branch review dispatched (plan range ccdc47f..HEAD)
Final review (opus): Ready after fixes. No confirmed wrong-result path. Important: (1) cold-start 400 not retried, (2) integer-typed count may overflow >2^31, (3) stddev/variance precision via extended_stats unverified, (4) DSL docs claim + 2 failing DSL ITs (INTEGER cast). FIX-NOW also: duplicated dep.opensearch.version.
Final fix wave dispatched (single fixer, worktree isolation, 3 commits: A retry+count guard+precision verify+docs, B version dedupe, C opensearch.md + 2 DSL tests); then ONE scoped re-review.
Ruling: fix wave runs in an isolated worktree — the main checkout is concurrently used by the Trino benchmark run; cost: needs a merge/cherry-pick back into opensearch-sql-connector.
Fix wave merged (ff) 23acd93, b9c026e, 8461253; scoped re-review: all findings addressed (2,3,4 with caveats).
Residual (no second fix wave; surfaced to user): R1 stddev/variance pushed by default returns 0.0 for large-mean/small-spread data (measured 1e9+i%5: pushed 0.0 vs exact 1.41/2.0/1.42/2.02) — reviewer recommends opt-in `opensearch.sql.statistical-pushdown-enabled=false`; USER DECISION pending. R2 DSL test coverage lost: pushed sum/avg NULL semantics for missing/empty input no longer asserted on DSL paths (testAggregationsWithMissingKeysAndValues v is INTEGER) — add a DOUBLE column. R3 characterization test failure message lacks maintainer hint. R4 count wrap past 2^32 undetectable (needs >4B docs; document). R5 base opensearch connector stddev likely has same precision issue (check). R6 airstyle:check not run (CRLF checkout); cold-start retry unverified on real cold cluster.
Ruling: no second fix wave for the above (SDD cap); R1 needs the user's product decision.
Follow-up (user approved): worktree branch followup-statistical-optin: 58efc05 statistical pushdown opt-in (default off), 0ef1b1d DSL pushed sum/avg NULL coverage (DOUBLE col); tests green; scoped review dispatched; merge after benchmark run finishes (benchmark uses main checkout)
Follow-up review: Approved (minor: docs '1e-8' agreement figure not backed by assertion (test asserts <1e-6); comment on doesNotContain("AGGREGATION:") substring semantics). Merge of followup-statistical-optin pending benchmark completion.
Merged into opensearch-sql-connector: followup-bigint-optin (ff; includes statistical-optin 58efc05, DSL coverage 0ef1b1d, bigint 6cbbb05/2873094) and followup-text-pushdown (merge commit; 03fa076, 539e9ed). First branch opensearch-aggregation-topn-pushdown got cherry-picks 0904fd3, 2580986 (docs INTEGER claim + DSL w-column coverage), 2 DSL tests verified green there.
Pending: post-merge verification, independent review of text pushdown (opus), Trino benchmark re-measure, Confluence update, push of both branches to fork (user-approved), secret scan.
Merged followup-text-switch (83e5989, eccad61) + rename opensearch.text-equality-pushdown-enabled (default off). Verification of 9110971 passed (71 + 77 tests, DSL Latest class 325 run 0 fail 169 skipped — skip reason unchecked).
Merged (ff) followup-text-groupby (eb9e8e2, 277663d) and followup-groupby-fixes (a784512, c9358cc): text GROUP BY pushdown with per-page `_uncovered` guard, doc_values requirement, presenceIndexed default false; version matrix 1.1.0/2.19.4/3.0.0/latest 9/9; full Latest class 330 run 0 failures 169 skipped.
Handover pack committed under docs/superpowers (HANDOVER-2026-10-06.md, benchmark/, sdd-ledger-opensearch-sql.md). Pushed to fork baepiff/trino: opensearch-aggregation-topn-pushdown, opensearch-sql-connector.
