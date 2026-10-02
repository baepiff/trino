# OpenSearch aggregation and TopN pushdown

Date: 2026-10-02
Module: `plugin/trino-opensearch`
Reference: trinodb/trino#27251 (Elasticsearch equivalent, unmerged), Starburst OpenSearch connector docs.

## Goal

Push `GROUP BY` aggregations and `ORDER BY ... LIMIT` (TopN) from Trino into OpenSearch, with
`opensearch.aggregation-pushdown-enabled` as the switch for aggregation pushdown.

## Non-goals

- Grouping sets, `CUBE`, `ROLLUP`.
- `DISTINCT` aggregates, filtered aggregates, ordered aggregates.
- Statistical functions beyond `COUNT`, `MIN`, `MAX`, `SUM`, `AVG`.
- Pushing `ORDER BY`, `LIMIT` or TopN into an aggregation result. Trino sorts and limits aggregation output.
- A config switch for TopN pushdown.

## Table handle

`OpenSearchTableHandle` changes:

- `Type` gains `AGGREGATION`.
- `OptionalLong limit` is replaced by `Optional<TopN> topN`.
- New fields `List<TermAggregation> termAggregations` and `List<MetricAggregation> metricAggregations`.
- Every construction site is updated: `applyLimit`, `applyFilter`, `withColumns`, the convenience constructor.
- `toString` prints `topN` and the aggregations.

New classes, all in package `io.trino.plugin.opensearch` (the builtin-column check needs the package-private `BuiltinColumns`):

- `TopN(long limit, List<TopNSortItem> sortItems)`. Empty `sortItems` means a plain `LIMIT`.
  `TopNSortItem(String field, SortOrder order, Optional<String> unmappedType)` converts to an OpenSearch `FieldSortBuilder`.
  `unmappedType` is the OpenSearch field type name, sent as `unmapped_type` so an alias or wildcard table over indices that do not all map the field sorts them as missing instead of failing. The default `_doc` sort has none.
  `NULLS FIRST` maps to `missing("_first")`, `NULLS LAST` is the default `_last`.
  No `NO_LIMIT` sentinel: a `TopN` exists only when there is a limit.
- `MetricAggregation(functionName, outputType, Optional<OpenSearchColumnHandle> columnHandle, alias)`.
- `TermAggregation(term, type)`.
- `AggregationResponseReader`: turns a search response's aggregations into rows (kept separate from `AggregateQueryPageSource` so it is unit-testable).

All three are Jackson-serializable, as table handles are sent to workers.

## Metadata

### `applyAggregation`

Returns empty when:

- aggregation pushdown is disabled (config or session property),
- the handle is a passthrough query, already an `AGGREGATION`, or already has a `topN`,
- more than one grouping set is requested.

Per aggregate:

- Rejected: `DISTINCT`, a filter, sort items, any function other than `count`, `min`, `max`, `sum`, `avg`.
- `count(*)` has no column.
- The argument must be a plain `Variable` mapped to an `OpenSearchColumnHandle` that supports predicates.
- `min`, `max`: `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE` input. `sum`, `avg`: `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE` input.
  `BIGINT` is excluded (metric aggregations return doubles, so values above 2^53 lose precision). `REAL` is excluded from `sum` and `avg`
  because OpenSearch accumulates in double while Trino accumulates in single precision, so results can differ.
  `COUNT(col)` accepts any predicate-capable column including `BIGINT` and keyword.
- Builtin columns (`_id`, `_source`, `_score`) are never used as aggregate arguments or group-by columns (`_id` reports `supportsPredicates=true`, but OpenSearch cannot aggregate on it).
- Columns read with a raw JSON decoder are never used as aggregate arguments (including `count(col)`) or group-by columns: their Trino value is the JSON text of the whole field, which OpenSearch does not aggregate on. One package-private helper (`PushdownColumns`) holds this check for aggregation and TopN.
- The output type needs a decoder (real, double, tinyint, smallint, integer, bigint, varchar, boolean).

Group-by columns must support predicates and be keyword, integral numeric or boolean.
Timestamp, real and double keys stay in Trino until the composite-key decoding is verified.

Result: new `AGGREGATION` handle, output variables named `_pushdown_<i>` backed by synthetic
`OpenSearchColumnHandle`s (`supportsPredicates=false`). `AggregationApplicationResult` uses an empty
grouping-column mapping and `precalculateStatistics=false`. The handle computes the aggregates completely,
so Trino drops its own aggregation node.

### `applyTopN`

Returns empty for a passthrough query, an `AGGREGATION` handle, or a handle that already has a `topN`.
Every sort column must support predicates and must not be a builtin or raw JSON column (`_id` reports `supportsPredicates=true`, but sorting on it needs `_id` fielddata). `text`, `scaled_float`, arrays and rows are rejected. `TopNSortItem.unmappedType` is filled from the column's OpenSearch type: the primitive type name, or `date` for date columns.
Returns `TopNApplicationResult(handle, topNGuaranteed=false, precalculateStatistics=false)`:
each shard returns its local top n and Trino merges.

### `applyLimit`, `applyFilter`, `applyProjection`

- `applyLimit`: empty for an `AGGREGATION` handle. Otherwise builds `TopN(limit, [])` when no `topN` exists, or keeps the smaller limit.
- `applyFilter`: empty for an `AGGREGATION` handle (the PR does not guard this).
- `applyProjection`: unchanged; synthetic columns are plain variables.

## Config and session properties

- `opensearch.aggregation-pushdown-enabled`, boolean, default `true`.
- `opensearch.max-aggregation-buckets`, int, default 65535, `@Min(1)`. Page size of each composite aggregation request. Name matches Starburst.
- Session property `aggregation_pushdown_enabled`, defaulting to the config value, following `projection_pushdown_enabled`.
- `@ConfigDescription` on both setters. `TestOpenSearchConfig` covers defaults and explicit mappings.

## Execution

- `OpenSearchSplitManager`: an `AGGREGATION` table gets a single split covering the whole index (as for `QUERY`), so OpenSearch merges shard results.
- `OpenSearchPageSourceProvider`: routes `AGGREGATION` to the new `AggregateQueryPageSource`, passing the bucket page size.
- `OpenSearchClient`:
  - `beginSearch(...)` takes `List<TopNSortItem> sortItems, OptionalLong limit` instead of `Optional<String> sort, OptionalLong limit`, and applies the sort items. Scroll is unchanged.
  - New `beginAggregationSearch(index, query, aggregations)`: `size=0`, `trackTotalHits(true)`, no scroll, no shard preference, partial search results disallowed (a failed shard fails the query instead of returning too-low counts).
- `OpenSearchQueryBuilder.buildAggregationQuery(termAggregations, metricAggregations, pageSize, after)`:
  - with group-by: a `composite` aggregation, one `terms` source per column with `missingBucket(true)`, metric sub-aggregations, `aggregateAfter` for pagination;
  - without group-by: top-level metric aggregations;
  - `max`, `min`, `avg`, `value_count` map directly; `sum` uses `stats` so an empty input can be reported as NULL;
  - `count(*)` adds no aggregation (bucket `doc_count`, or total hits for a global count).
- `AggregateQueryPageSource`:
  - fetches pages until the composite response has no `afterKey`;
  - decodes with the existing decoders using the aggregation column names;
  - infinite min/max/avg values become NULL.
- `ScanQueryPageSource` and `CountQueryPageSource` read `topN.limit()`.
  With no sort items the scan keeps today's default (`_doc`, or relevance when a raw `query` is set).

## Semantics

- Global aggregation returns exactly one row. On empty input `COUNT` is 0 and `SUM`, `MIN`, `MAX`, `AVG` are NULL.
- Grouped aggregation returns no rows on empty input. NULL group keys form their own group.
- `SUM` over integer types is computed as a double: exact up to 2^53, documented as a limitation.
- `MIN`/`MAX` on keyword columns are not pushed (OpenSearch metric aggregations do not support them); confirm in integration tests.

## Tests

Unit:

- `TestOpenSearchConfig`: defaults and explicit mappings for the two new properties.
- `TestOpenSearchMetadata`: `applyAggregation` accepted and each rejection reason; `applyTopN` accepted and rejected columns, existing `topN`, aggregation handle, passthrough; `applyLimit` and `applyFilter` over an aggregation handle.
- `TestOpenSearchQueryBuilder`: aggregation request JSON for global, grouped, multi-key and paginated cases; sort item null ordering for all four direction and null combinations.
- `TestAggregationResponseReader` with hand-written responses (no mocks): global, grouped, NULL group, empty input, pagination.

Integration (`BaseOpenSearchConnectorTest`, Docker required):

- `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg` global and grouped; NULL groups; filter plus aggregation; empty table; `max-aggregation-buckets=2` to force pagination; plan assertions that the aggregation is pushed.
- `ORDER BY ... LIMIT` with NULLs in each ordering, a limit above the scroll size, a sort on a non-pushable column, `ORDER BY` over an aggregation (stays in Trino).
- Update the base test's behavior flags so the shared `BaseConnectorTest` cases for aggregation and TopN pushdown run.

## Docs

Add a pushdown section to the OpenSearch connector page: supported functions, restrictions (`BIGINT`, group-by types, `SUM` precision), and both config properties.

## Risks

- Integration behavior (keyword min/max, composite key types, `trackTotalHits`) is verified only by the Docker-based tests; local Docker availability is unconfirmed.
- The reference PR is unmerged and written against the Elasticsearch plugin; behavior is re-derived for the OpenSearch client (3.x high-level client), not copied.
