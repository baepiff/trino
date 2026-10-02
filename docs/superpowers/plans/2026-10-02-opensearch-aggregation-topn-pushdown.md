# OpenSearch Aggregation and TopN Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Push `GROUP BY` aggregations and `ORDER BY ... LIMIT` (TopN) from Trino into OpenSearch, with `opensearch.aggregation-pushdown-enabled` controlling aggregation pushdown.

**Architecture:** `OpenSearchTableHandle` gains an `AGGREGATION` type, `termAggregations`, `metricAggregations`, and an `Optional<TopN>` that replaces `OptionalLong limit`. `applyAggregation` builds an `AGGREGATION` handle whose page source runs one composite (grouped) or metric-only (global) aggregation search against the whole index and pages with `after_key`. `applyTopN` stores sort items that `ScanQueryPageSource` sends as OpenSearch sorts; each shard returns its local top n and Trino keeps its TopN operator on top.

**Tech Stack:** Java 25, Trino SPI (`applyAggregation`, `applyTopN`), OpenSearch 3.8 high-level REST client (`org.opensearch.search.aggregations.*`), JUnit 5, AssertJ, Docker-based `OpenSearchServer` for integration tests.

**Spec:** `docs/superpowers/specs/2026-10-02-opensearch-aggregation-topn-pushdown-design.md`

## Global Constraints

- Module: `plugin/trino-opensearch`. Package for all new main classes: `io.trino.plugin.opensearch`. New tests live in the same package under `src/test/java`.
- Read `.github/DEVELOPMENT.md` before writing code. Rules that apply here: license header on every new file (copy the header from any existing file), no wildcard imports, braces on every `if`/`for`/`while`, no `@author`, no mocking libraries, AssertJ for assertions, Guava immutable collections, avoid abbreviations, avoid `get` in new method names (except bean getters), avoid ternary operators except trivial ones, `var` only when the type is obvious.
- Config property names use dashes, session property names use snake_case, every `@Config` has `@ConfigDescription`, validation annotations go on setters (`.claude/rules/trino-config-properties.md`).
- Config names (exact): `opensearch.aggregation-pushdown-enabled` (boolean, default `true`), `opensearch.max-aggregation-buckets` (int, default `65535`, `@Min(1)`). Session property (exact): `aggregation_pushdown_enabled`.
- Aggregate functions pushed: `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg`. `min`/`max` inputs: `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE`. `sum`/`avg` inputs: `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE`. `BIGINT` is excluded from `min`/`max`/`sum`/`avg`. `count(col)` accepts any predicate-capable column.
- Group-by columns: predicate-capable and `VARCHAR` (keyword), `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, or `BOOLEAN`. Builtin columns (`_id`, `_source`, `_score`) are never aggregated or grouped.
- Synthetic aggregate output columns are named `_pushdown_<index>`.
- Do not commit unless the user asks. Each task ends with a format-and-test checkpoint instead of a commit.
- Formatting: run `mvnd airstyle:format` as the project guide says; `mvnd` is not on PATH on this machine, so use `./mvnw -pl plugin/trino-opensearch airstyle:format` from `C:\github\trino`.
- Test command pattern (from `C:\github\trino`): `./mvnw -pl plugin/trino-opensearch test -Dtest=<ClassName> -Dair.check.skip-all=true -nsu`. If dependencies are missing the first time, run `./mvnw install -pl plugin/trino-opensearch -am -DskipTests -Dair.check.skip-all=true -nsu` once.
- Integration tests (`TestOpenSearchLatestConnectorTest`, which extends `BaseOpenSearchConnectorTest`) need Docker. Docker CLI is installed (Rancher Desktop); start Docker before running them. Single-method pattern: `-Dtest='TestOpenSearchLatestConnectorTest#methodName'`.
- Shared base test flags stay as they are: `SUPPORTS_LIMIT_PUSHDOWN`, `SUPPORTS_TOPN_PUSHDOWN` remain `false` and `SUPPORTS_AGGREGATION_PUSHDOWN` is not enabled. The base test treats these flags as "fully pushed down" and its aggregation checks use `BIGINT` columns, which this change deliberately does not push. Dedicated tests are added instead.

## File Structure

Create (main):
- `TopN.java` — `TopN(limit, sortItems)` and nested `TopNSortItem` with `toSortBuilder()`.
- `TermAggregation.java` — group-by column record with eligibility check.
- `MetricAggregation.java` — aggregate function record with eligibility check.
- `AggregationResponseReader.java` — converts response aggregations into rows (pure, unit-testable).
- `AggregateQueryPageSource.java` — runs aggregation searches, pages with `after_key`, decodes rows.

Modify (main): `OpenSearchConfig`, `OpenSearchSessionProperties`, `OpenSearchTableHandle`, `OpenSearchMetadata`, `OpenSearchQueryBuilder`, `client/OpenSearchClient`, `ScanQueryPageSource`, `CountQueryPageSource`, `OpenSearchPageSourceProvider`, `OpenSearchSplitManager`.

Create (test): `TestTopN.java`, `TestAggregationResponseReader.java`.
Modify (test): `TestOpenSearchConfig`, `TestOpenSearchMetadata`, `TestOpenSearchQueryBuilder`, `BaseOpenSearchConnectorTest`.
Modify (docs): `docs/src/main/sphinx/connector/opensearch.md`.

---

### Task 1: Config properties and session property

**Files:**
- Modify: `plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/OpenSearchConfig.java`
- Modify: `plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/OpenSearchSessionProperties.java`
- Test: `plugin/trino-opensearch/src/test/java/io/trino/plugin/opensearch/TestOpenSearchConfig.java`

**Interfaces:**
- Produces: `OpenSearchConfig.isAggregationPushdownEnabled(): boolean`, `OpenSearchConfig.setAggregationPushdownEnabled(boolean): OpenSearchConfig`, `OpenSearchConfig.getMaxAggregationBuckets(): int`, `OpenSearchConfig.setMaxAggregationBuckets(int): OpenSearchConfig`, `OpenSearchSessionProperties.isAggregationPushdownEnabled(ConnectorSession): boolean`.

- [ ] **Step 1: Write the failing test**

In `TestOpenSearchConfig.testDefaults`, add after `.setProjectionPushdownEnabled(true)`:

```java
                .setAggregationPushdownEnabled(true)
                .setMaxAggregationBuckets(65535)
```

In `testExplicitPropertyMappings`, add after `.put("opensearch.projection-pushdown-enabled", "false")`:

```java
                .put("opensearch.aggregation-pushdown-enabled", "false")
                .put("opensearch.max-aggregation-buckets", "1000")
```

and after `.setProjectionPushdownEnabled(false)` in the `expected` chain:

```java
                .setAggregationPushdownEnabled(false)
                .setMaxAggregationBuckets(1000)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchConfig -Dair.check.skip-all=true -nsu`
Expected: FAIL with a compilation error (`cannot find symbol setAggregationPushdownEnabled`).

- [ ] **Step 3: Write minimal implementation**

In `OpenSearchConfig`, add fields after `private boolean projectionPushDownEnabled = true;`:

```java
    private boolean aggregationPushdownEnabled = true;
    private int maxAggregationBuckets = 65_535;
```

Add methods after `setProjectionPushdownEnabled`:

```java
    public boolean isAggregationPushdownEnabled()
    {
        return aggregationPushdownEnabled;
    }

    @Config("opensearch.aggregation-pushdown-enabled")
    @ConfigDescription("Push down supported aggregations to OpenSearch")
    public OpenSearchConfig setAggregationPushdownEnabled(boolean aggregationPushdownEnabled)
    {
        this.aggregationPushdownEnabled = aggregationPushdownEnabled;
        return this;
    }

    @Min(1)
    public int getMaxAggregationBuckets()
    {
        return maxAggregationBuckets;
    }

    @Config("opensearch.max-aggregation-buckets")
    @ConfigDescription("Maximum number of buckets requested in each aggregation search request")
    public OpenSearchConfig setMaxAggregationBuckets(int maxAggregationBuckets)
    {
        this.maxAggregationBuckets = maxAggregationBuckets;
        return this;
    }
```

In `OpenSearchSessionProperties`, add the constant and registration, and the accessor:

```java
    private static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
```

```java
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Push down supported aggregations to OpenSearch",
                        openSearchConfig.isAggregationPushdownEnabled(),
                        false))
```

(inside the `ImmutableList.<PropertyMetadata<?>>builder()` chain, after the projection property and before `.build()`), and

```java
    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchConfig -Dair.check.skip-all=true -nsu`
Expected: PASS (2 tests).

- [ ] **Step 5: Format checkpoint**

Run: `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu` then re-run the test from Step 4. Do not commit.

---

### Task 2: `TopN` type and `limit` → `topN` refactor

Replaces `OptionalLong limit` in the table handle with `Optional<TopN>`. Behavior for plain `LIMIT` stays identical.

**Files:**
- Create: `plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/TopN.java`
- Modify: `OpenSearchTableHandle.java`, `OpenSearchMetadata.java` (`applyLimit`, `applyFilter`, imports), `ScanQueryPageSource.java`, `CountQueryPageSource.java`, `client/OpenSearchClient.java` (`beginSearch`)
- Test: `TestTopN.java` (create), `TestOpenSearchMetadata.java` (modify)

**Interfaces:**
- Produces:
  - `record TopN(long limit, List<TopNSortItem> sortItems)` with `static TopN fromLimit(long)`.
  - `record TopN.TopNSortItem(String field, SortOrder order)` with `static final TopNSortItem SORT_BY_DOC` and `SortBuilder<?> toSortBuilder()`.
  - `OpenSearchTableHandle` component order: `type, schema, index, constraint, regexes, query, topN, columns`; `withColumns(Set<OpenSearchColumnHandle>)`; `withTopN(TopN)`.
  - `OpenSearchClient.beginSearch(String index, int shard, QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, List<TopN.TopNSortItem> sortItems, OptionalLong limit): SearchResponse`.

- [ ] **Step 1: Write the failing tests**

Create `TestTopN.java`:

```java
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.spi.connector.SortOrder;
import org.junit.jupiter.api.Test;
import org.opensearch.search.sort.SortBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTopN
{
    @Test
    public void testSortBuilderMapsDirectionAndNullOrdering()
    {
        assertThat(new TopNSortItem("field", SortOrder.ASC_NULLS_LAST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.ASC));
        assertThat(new TopNSortItem("field", SortOrder.ASC_NULLS_FIRST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.ASC).missing("_first"));
        assertThat(new TopNSortItem("field", SortOrder.DESC_NULLS_LAST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.DESC));
        assertThat(new TopNSortItem("field", SortOrder.DESC_NULLS_FIRST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.DESC).missing("_first"));
    }

    @Test
    public void testFromLimitHasNoSortItems()
    {
        TopN topN = TopN.fromLimit(5);

        assertThat(topN.limit()).isEqualTo(5);
        assertThat(topN.sortItems()).isEmpty();
    }

    @Test
    public void testRejectsNegativeLimit()
    {
        assertThatThrownBy(() -> new TopN(-1, ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit is negative");
    }
}
```

Rewrite `TestOpenSearchMetadata.java` to add a shared metadata fixture and the `applyLimit` tests (keep the existing `testLikeToRegexp` and `likeToRegexp` helper unchanged):

```java
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.QUERY;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOpenSearchMetadata
{
    private static final ConnectorSession SESSION = session(true);

    private OpenSearchClient client;
    private OpenSearchMetadata metadata;

    @BeforeAll
    public void setUp()
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        metadata = new OpenSearchMetadata(TESTING_TYPE_MANAGER, client, config);
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        client.close();
    }

    // existing testLikeToRegexp stays here unchanged

    @Test
    public void testApplyLimitCreatesLimitOnlyTopN()
    {
        LimitApplicationResult<ConnectorTableHandle> result = metadata.applyLimit(SESSION, scanHandle(), 5).orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).topN()).hasValue(TopN.fromLimit(5));
    }

    @Test
    public void testApplyLimitKeepsSortItemsWhenNarrowing()
    {
        TopN existing = new TopN(10, ImmutableList.of(new TopNSortItem("regionkey", SortOrder.DESC_NULLS_FIRST)));

        LimitApplicationResult<ConnectorTableHandle> result = metadata.applyLimit(SESSION, scanHandle().withTopN(existing), 5).orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).topN())
                .hasValue(new TopN(5, existing.sortItems()));
    }

    @Test
    public void testApplyLimitDoesNotWidenExistingTopN()
    {
        assertThat(metadata.applyLimit(SESSION, scanHandle().withTopN(TopN.fromLimit(5)), 10)).isEmpty();
        assertThat(metadata.applyLimit(SESSION, scanHandle().withTopN(TopN.fromLimit(5)), 5)).isEmpty();
    }

    @Test
    public void testApplyLimitRejectsPassthroughQuery()
    {
        assertThat(metadata.applyLimit(SESSION, new OpenSearchTableHandle(QUERY, "default", "nation", Optional.of("{}")), 5)).isEmpty();
    }

    static OpenSearchTableHandle scanHandle()
    {
        return new OpenSearchTableHandle(SCAN, "default", "nation", Optional.empty());
    }

    static ConnectorSession session(boolean aggregationPushdownEnabled)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", aggregationPushdownEnabled))
                .build();
    }
}
```

The existing `testLikeToRegexp` and its private `likeToRegexp` helper stay in the class; they use `io.airlift.slice.Slices`, `java.util.Optional` and `assertThat`, which the import list above already includes. Do not import `ImmutableSet` (unused).

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestTopN,TestOpenSearchMetadata' -Dair.check.skip-all=true -nsu`
Expected: FAIL with compilation errors (`TopN`, `withTopN` missing).

- [ ] **Step 3: Write minimal implementation**

Create `TopN.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record TopN(long limit, List<TopNSortItem> sortItems)
{
    public TopN
    {
        checkArgument(limit >= 0, "limit is negative: %s", limit);
        sortItems = ImmutableList.copyOf(requireNonNull(sortItems, "sortItems is null"));
    }

    public static TopN fromLimit(long limit)
    {
        return new TopN(limit, ImmutableList.of());
    }

    public record TopNSortItem(String field, SortOrder order)
    {
        // sorting by _doc (index order) gets special treatment in OpenSearch and is more efficient
        public static final TopNSortItem SORT_BY_DOC = new TopNSortItem("_doc", SortOrder.ASC_NULLS_LAST);

        public TopNSortItem
        {
            requireNonNull(field, "field is null");
            requireNonNull(order, "order is null");
        }

        public SortBuilder<?> toSortBuilder()
        {
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort(field);
            if (order.isAscending()) {
                sortBuilder.order(org.opensearch.search.sort.SortOrder.ASC);
            }
            else {
                sortBuilder.order(org.opensearch.search.sort.SortOrder.DESC);
            }
            if (order.isNullsFirst()) {
                // the default for missing values is _last in both directions
                sortBuilder.missing("_first");
            }
            return sortBuilder;
        }
    }
}
```

Replace `OpenSearchTableHandle.java` body (keep license header and package line):

```java
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record OpenSearchTableHandle(
        Type type,
        String schema,
        String index,
        TupleDomain<ColumnHandle> constraint,
        Map<String, String> regexes,
        Optional<String> query,
        Optional<TopN> topN,
        Set<OpenSearchColumnHandle> columns)
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY
    }

    public OpenSearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this(type,
                schema,
                index,
                TupleDomain.all(),
                ImmutableMap.of(),
                query,
                Optional.empty(),
                ImmutableSet.of());
    }

    public OpenSearchTableHandle withColumns(Set<OpenSearchColumnHandle> columns)
    {
        return new OpenSearchTableHandle(type, schema, index, constraint, regexes, query, topN, columns);
    }

    public OpenSearchTableHandle withTopN(TopN topN)
    {
        return new OpenSearchTableHandle(type, schema, index, constraint, regexes, query, Optional.of(topN), columns);
    }

    public OpenSearchTableHandle
    {
        requireNonNull(type, "type is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(index, "index is null");
        requireNonNull(constraint, "constraint is null");
        regexes = ImmutableMap.copyOf(requireNonNull(regexes, "regexes is null"));
        columns = ImmutableSet.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(query, "query is null");
        requireNonNull(topN, "topN is null");
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(type + ":" + index);

        StringBuilder attributes = new StringBuilder();
        if (!regexes.isEmpty()) {
            attributes.append("regexes=[");
            attributes.append(regexes.entrySet().stream()
                    .map(regex -> regex.getKey() + ":" + regex.getValue())
                    .collect(Collectors.joining(", ")));
            attributes.append("]");
        }
        topN.ifPresent(value -> attributes.append("topN=" + value));
        query.ifPresent(value -> attributes.append("query" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
```

`OpenSearchMetadata`: remove `import java.util.OptionalLong;`. Replace the body of `applyLimit` after the passthrough check:

```java
        if (handle.topN().isPresent() && handle.topN().orElseThrow().limit() <= limit) {
            return Optional.empty();
        }

        TopN topN = handle.topN()
                .map(existing -> new TopN(limit, existing.sortItems()))
                .orElseGet(() -> TopN.fromLimit(limit));

        return Optional.of(new LimitApplicationResult<>(handle.withTopN(topN).withColumns(ImmutableSet.of()), false, false));
```

In `applyFilter`, change the final `new OpenSearchTableHandle(...)` call so the arguments after `handle.query()` are `handle.topN(), ImmutableSet.of()` (replacing `handle.limit(), ImmutableSet.of()`).

`CountQueryPageSource` constructor, replace the limit block:

```java
        if (table.topN().isPresent()) {
            count = Math.min(table.topN().orElseThrow().limit(), count);
        }
```

`ScanQueryPageSource` constructor, replace the sort/limit logic (the `Optional<String> sort ...` block through the `SearchHitIterator` creation) with:

```java
        List<TopNSortItem> sortItems = table.topN()
                .map(TopN::sortItems)
                .orElse(ImmutableList.of());
        if (sortItems.isEmpty() && table.query().isEmpty()) {
            // sorting by _doc (index order) gets special treatment in OpenSearch and is more efficient.
            // However, if we're using a custom OpenSearch query, use default sorting:
            // documents will be scored and returned based on relevance
            sortItems = ImmutableList.of(SORT_BY_DOC);
        }

        OptionalLong limit = OptionalLong.empty();
        if (table.topN().isPresent()) {
            limit = OptionalLong.of(table.topN().orElseThrow().limit());
        }

        long start = System.nanoTime();
        SearchResponse searchResponse = client.beginSearch(
                split.index(),
                split.shard(),
                OpenSearchQueryBuilder.buildSearchQuery(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast), table.query(), table.regexes()),
                needAllFields ? Optional.empty() : Optional.of(requiredFields),
                documentFields,
                sortItems,
                limit);
        readTimeNanos += System.nanoTime() - start;
        this.iterator = new SearchHitIterator(client, () -> searchResponse, limit);
```

Add imports `io.trino.plugin.opensearch.TopN.TopNSortItem` and `static io.trino.plugin.opensearch.TopN.TopNSortItem.SORT_BY_DOC`.

`OpenSearchClient.beginSearch`: change the signature and sort handling:

```java
    public SearchResponse beginSearch(String index, int shard, QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, List<TopNSortItem> sortItems, OptionalLong limit)
```

replace `sort.ifPresent(sourceBuilder::sort);` with

```java
        sortItems.forEach(sortItem -> sourceBuilder.sort(sortItem.toSortBuilder()));
```

and add `import io.trino.plugin.opensearch.TopN.TopNSortItem;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestTopN,TestOpenSearchMetadata,TestOpenSearchConfig,TestOpenSearchQueryBuilder' -Dair.check.skip-all=true -nsu`
Expected: PASS. Also confirm the module compiles with tests: `./mvnw -pl plugin/trino-opensearch test-compile -Dair.check.skip-all=true -nsu`.

- [ ] **Step 5: Regression check and format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`. With Docker running, run `-Dtest='TestOpenSearchLatestConnectorTest#testLimitPushdown+testTopNPushdown+testSelectAll'`; expected PASS (limit still works through `TopN.fromLimit`). Do not commit.

---

### Task 3: TopN pushdown (`applyTopN`)

**Files:**
- Modify: `OpenSearchMetadata.java`
- Test: `TestOpenSearchMetadata.java`, `BaseOpenSearchConnectorTest.java`

**Interfaces:**
- Consumes: `TopN`, `TopNSortItem`, `OpenSearchTableHandle.withTopN`.
- Produces: `OpenSearchMetadata.applyTopN(ConnectorSession, ConnectorTableHandle, long, List<SortItem>, Map<String, ColumnHandle>): Optional<TopNApplicationResult<ConnectorTableHandle>>`. Test helpers in `TestOpenSearchMetadata`: `keywordColumn(String)`, `bigintColumn(String)`, `textColumn(String)`.

- [ ] **Step 1: Write the failing tests**

Add to `TestOpenSearchMetadata` (imports: `io.trino.plugin.opensearch.client.IndexMetadata`, `io.trino.plugin.opensearch.decoders.BigintDecoder`, `io.trino.plugin.opensearch.decoders.VarcharDecoder`, `io.trino.spi.connector.SortItem`, `io.trino.spi.connector.TopNApplicationResult`, `java.util.Map`, `static io.trino.spi.type.BigintType.BIGINT`, `static io.trino.spi.type.VarcharType.VARCHAR`):

```java
    @Test
    public void testApplyTopNCreatesSortItems()
    {
        TopNApplicationResult<ConnectorTableHandle> result = metadata.applyTopN(
                SESSION,
                scanHandle(),
                5,
                List.of(new SortItem("regionkey", SortOrder.DESC_NULLS_FIRST), new SortItem("name", SortOrder.ASC_NULLS_LAST)),
                Map.of("regionkey", bigintColumn("regionkey"), "name", keywordColumn("name")))
                .orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).topN()).hasValue(new TopN(
                5,
                List.of(
                        new TopNSortItem("regionkey", SortOrder.DESC_NULLS_FIRST),
                        new TopNSortItem("name", SortOrder.ASC_NULLS_LAST))));
        assertThat(result.isTopNGuaranteed()).isFalse();
    }

    @Test
    public void testApplyTopNRejectsUnsupportedSortColumn()
    {
        assertThat(metadata.applyTopN(
                SESSION,
                scanHandle(),
                5,
                List.of(new SortItem("description", SortOrder.ASC_NULLS_LAST)),
                Map.of("description", textColumn("description"))))
                .isEmpty();
    }

    @Test
    public void testApplyTopNRejectsExistingTopN()
    {
        assertThat(metadata.applyTopN(
                SESSION,
                scanHandle().withTopN(TopN.fromLimit(10)),
                5,
                List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)),
                Map.of("regionkey", bigintColumn("regionkey"))))
                .isEmpty();
    }

    @Test
    public void testApplyTopNRejectsPassthroughQuery()
    {
        assertThat(metadata.applyTopN(
                SESSION,
                new OpenSearchTableHandle(QUERY, "default", "nation", Optional.of("{}")),
                5,
                List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)),
                Map.of("regionkey", bigintColumn("regionkey"))))
                .isEmpty();
    }

    static OpenSearchColumnHandle bigintColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle keywordColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle textColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor(name), false);
    }
```

Add integration tests to `BaseOpenSearchConnectorTest` (new imports: `io.trino.sql.planner.plan.TopNNode`; `createIndex`, `index`, `deleteIndex` already exist as private helpers):

```java
    @Test
    public void testTopNWithMultipleSortColumnsAndMissingValues()
            throws IOException
    {
        String tableName = "test_topn_sorting_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "id": { "type": "keyword" },
                        "sort_key": { "type": "long" },
                        "tie_key": { "type": "long" }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.of("id", "1", "sort_key", 2, "tie_key", 10));
            index(tableName, ImmutableMap.of("id", "2", "tie_key", 90));
            index(tableName, ImmutableMap.of("id", "3", "sort_key", 1, "tie_key", 10));
            index(tableName, ImmutableMap.of("id", "4", "sort_key", 1, "tie_key", 20));

            assertThat(query(format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key ASC NULLS FIRST, tie_key DESC LIMIT 3", tableName)))
                    .ordered()
                    .matches("VALUES (CAST('2' AS VARCHAR), CAST(NULL AS BIGINT), BIGINT '90'), (CAST('4' AS VARCHAR), BIGINT '1', BIGINT '20'), (CAST('3' AS VARCHAR), BIGINT '1', BIGINT '10')")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key DESC NULLS LAST, tie_key ASC LIMIT 3", tableName)))
                    .ordered()
                    .matches("VALUES (CAST('1' AS VARCHAR), BIGINT '2', BIGINT '10'), (CAST('3' AS VARCHAR), BIGINT '1', BIGINT '10'), (CAST('4' AS VARCHAR), BIGINT '1', BIGINT '20')")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key ASC NULLS LAST, tie_key ASC LIMIT 2", tableName)))
                    .ordered()
                    .matches("VALUES (CAST('3' AS VARCHAR), BIGINT '1', BIGINT '10'), (CAST('4' AS VARCHAR), BIGINT '1', BIGINT '20')")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key DESC NULLS FIRST, tie_key DESC LIMIT 2", tableName)))
                    .ordered()
                    .matches("VALUES (CAST('2' AS VARCHAR), CAST(NULL AS BIGINT), BIGINT '90'), (CAST('1' AS VARCHAR), BIGINT '2', BIGINT '10')")
                    .isNotFullyPushedDown(TopNNode.class);
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testTopNWithLimitAboveScrollSize()
    {
        // scroll size is 1000, so these limits require several scroll pages per shard
        assertQueryOrdered("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 2500");
        assertQueryOrdered("SELECT orderkey, custkey FROM orders ORDER BY custkey ASC, orderkey ASC LIMIT 1500");
    }

    @Test
    public void testTopNOverNonPushableSortColumn()
    {
        assertQueryOrdered("SELECT name, comment FROM nation ORDER BY comment, name LIMIT 5");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchMetadata -Dair.check.skip-all=true -nsu`
Expected: FAIL (`applyTopN` returns `Optional.empty()` from the SPI default, so `orElseThrow` throws `NoSuchElementException`).

- [ ] **Step 3: Write minimal implementation**

Add to `OpenSearchMetadata` (imports: `io.trino.plugin.opensearch.TopN.TopNSortItem`, `io.trino.spi.connector.SortItem`, `io.trino.spi.connector.TopNApplicationResult`), placed after `applyLimit`:

```java
    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // TopN pushdown currently not supported for passthrough query
            return Optional.empty();
        }
        if (handle.topN().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<TopNSortItem> topNSortItems = ImmutableList.builder();
        for (SortItem sortItem : sortItems) {
            OpenSearchColumnHandle column = (OpenSearchColumnHandle) assignments.get(sortItem.getName());
            verifyNotNull(column, "No assignment for %s", sortItem.getName());
            if (!column.supportsPredicates()) {
                return Optional.empty();
            }
            topNSortItems.add(new TopNSortItem(column.name(), sortItem.getSortOrder()));
        }

        // every shard returns its local top n and Trino merges them, so the TopN is not guaranteed
        return Optional.of(new TopNApplicationResult<>(handle.withTopN(new TopN(topNCount, topNSortItems.build())), false, false));
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchMetadata -Dair.check.skip-all=true -nsu`
Expected: PASS. With Docker running:
`./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchLatestConnectorTest#testTopNWithMultipleSortColumnsAndMissingValues+testTopNWithLimitAboveScrollSize+testTopNOverNonPushableSortColumn+testTopNPushdown+testSortItemsReflectedInExplain' -Dair.check.skip-all=true -nsu`
Expected: PASS. If a sort-order assertion fails, the null ordering in `TopNSortItem.toSortBuilder` is wrong; fix there, not in the test.

- [ ] **Step 5: Format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`. Do not commit.

---

### Task 4: Aggregation model classes and table handle fields

**Files:**
- Create: `TermAggregation.java`, `MetricAggregation.java`
- Modify: `OpenSearchTableHandle.java`, `OpenSearchMetadata.java` (`applyFilter` constructor call)
- Test: `TestAggregationModel.java` (create)

**Interfaces:**
- Produces:
  - `record TermAggregation(String term, Type type)` with `static Optional<TermAggregation> fromColumn(OpenSearchColumnHandle column)`.
  - `record MetricAggregation(String functionName, Type outputType, Optional<OpenSearchColumnHandle> columnHandle, String alias)` with constants `COUNT = "count"`, `MIN = "min"`, `MAX = "max"`, `SUM = "sum"`, `AVG = "avg"` and `static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias)`.
  - `OpenSearchTableHandle` final component order: `type, schema, index, constraint, regexes, query, topN, columns, termAggregations, metricAggregations`; `Type` gains `AGGREGATION`; new `withAggregations(List<TermAggregation>, List<MetricAggregation>)` returns a handle of type `AGGREGATION`.

- [ ] **Step 1: Write the failing test**

Create `TestAggregationModel.java`:

```java
package io.trino.plugin.opensearch;

import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.BooleanDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.TestOpenSearchMetadata.bigintColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.keywordColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.textColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationModel
{
    @Test
    public void testTermAggregationAcceptsKeywordIntegralAndBoolean()
    {
        assertThat(TermAggregation.fromColumn(keywordColumn("name"))).hasValue(new TermAggregation("name", io.trino.spi.type.VarcharType.VARCHAR));
        assertThat(TermAggregation.fromColumn(bigintColumn("regionkey"))).hasValue(new TermAggregation("regionkey", BIGINT));
        assertThat(TermAggregation.fromColumn(column("flag", BOOLEAN, "boolean", true))).isPresent();
        assertThat(TermAggregation.fromColumn(column("count", INTEGER, "integer", true))).isPresent();
    }

    @Test
    public void testTermAggregationRejectsOtherColumns()
    {
        assertThat(TermAggregation.fromColumn(textColumn("description"))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("price", DOUBLE, "double", true))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("rating", REAL, "float", true))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("_id", io.trino.spi.type.VarcharType.VARCHAR, "text", true))).isEmpty();
    }

    @Test
    public void testCountStar()
    {
        assertThat(MetricAggregation.from(function("count", BIGINT), Map.of(), "_pushdown_0"))
                .hasValue(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"));
    }

    @Test
    public void testCountColumnAcceptsBigintAndKeyword()
    {
        OpenSearchColumnHandle bigint = bigintColumn("regionkey");
        OpenSearchColumnHandle keyword = keywordColumn("name");

        assertThat(MetricAggregation.from(function("count", BIGINT, "regionkey", BIGINT), Map.of("regionkey", bigint), "a"))
                .hasValue(new MetricAggregation("count", BIGINT, Optional.of(bigint), "a"));
        assertThat(MetricAggregation.from(function("count", BIGINT, "name", io.trino.spi.type.VarcharType.VARCHAR), Map.of("name", keyword), "a"))
                .isPresent();
    }

    @Test
    public void testNumericFunctionsAcceptedInputs()
    {
        OpenSearchColumnHandle integer = column("i", INTEGER, "integer", true);
        OpenSearchColumnHandle doubleColumn = column("d", DOUBLE, "double", true);
        OpenSearchColumnHandle real = column("r", REAL, "float", true);

        for (String name : List.of("min", "max", "sum", "avg")) {
            assertThat(MetricAggregation.from(function(name, name.equals("avg") ? DOUBLE : INTEGER, "i", INTEGER), Map.of("i", integer), "a")).as(name + " integer").isPresent();
            assertThat(MetricAggregation.from(function(name, DOUBLE, "d", DOUBLE), Map.of("d", doubleColumn), "a")).as(name + " double").isPresent();
        }
        // REAL is accepted for min and max only
        assertThat(MetricAggregation.from(function("min", REAL, "r", REAL), Map.of("r", real), "a")).isPresent();
        assertThat(MetricAggregation.from(function("max", REAL, "r", REAL), Map.of("r", real), "a")).isPresent();
        assertThat(MetricAggregation.from(function("sum", REAL, "r", REAL), Map.of("r", real), "a")).isEmpty();
        assertThat(MetricAggregation.from(function("avg", REAL, "r", REAL), Map.of("r", real), "a")).isEmpty();
    }

    @Test
    public void testBigintIsRejectedForNumericFunctions()
    {
        Map<String, ColumnHandle> assignments = Map.of("regionkey", bigintColumn("regionkey"));
        for (String name : List.of("min", "max", "sum", "avg")) {
            assertThat(MetricAggregation.from(function(name, BIGINT, "regionkey", BIGINT), assignments, "a")).as(name).isEmpty();
        }
    }

    @Test
    public void testRejectsUnsupportedShapes()
    {
        Map<String, ColumnHandle> assignments = Map.of("i", column("i", INTEGER, "integer", true));
        Variable variable = new Variable("i", INTEGER);

        // DISTINCT
        assertThat(MetricAggregation.from(new AggregateFunction("count", BIGINT, List.of(variable), List.of(), true, Optional.empty()), assignments, "a")).isEmpty();
        // filter
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(variable), List.of(), false, Optional.of(new Variable("f", BOOLEAN))), assignments, "a")).isEmpty();
        // ordering
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(variable), List.of(new SortItem("i", SortOrder.ASC_NULLS_LAST)), false, Optional.empty()), assignments, "a")).isEmpty();
        // unsupported function
        assertThat(MetricAggregation.from(new AggregateFunction("stddev", DOUBLE, List.of(variable), List.of(), false, Optional.empty()), assignments, "a")).isEmpty();
        // expression argument
        ConnectorExpression expression = new Call(INTEGER, new io.trino.spi.function.FunctionName("$add"), List.of(variable, variable));
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(expression), List.of(), false, Optional.empty()), assignments, "a")).isEmpty();
        // text column and builtin column
        assertThat(MetricAggregation.from(function("count", BIGINT, "t", io.trino.spi.type.VarcharType.VARCHAR), Map.of("t", textColumn("t")), "a")).isEmpty();
        assertThat(MetricAggregation.from(function("count", BIGINT, "_id", io.trino.spi.type.VarcharType.VARCHAR), Map.of("_id", column("_id", io.trino.spi.type.VarcharType.VARCHAR, "text", true)), "a")).isEmpty();
    }

    private static AggregateFunction function(String name, Type outputType)
    {
        return new AggregateFunction(name, outputType, List.of(), List.of(), false, Optional.empty());
    }

    private static AggregateFunction function(String name, Type outputType, String variable, Type inputType)
    {
        return new AggregateFunction(name, outputType, List.of(new Variable(variable, inputType)), List.of(), false, Optional.empty());
    }

    private static OpenSearchColumnHandle column(String name, Type type, String opensearchType, boolean supportsPredicates)
    {
        return new OpenSearchColumnHandle(List.of(name), type, new IndexMetadata.PrimitiveType(opensearchType), decoder(type, name), supportsPredicates);
    }

    private static DecoderDescriptor decoder(Type type, String name)
    {
        if (type.equals(BOOLEAN)) {
            return new BooleanDecoder.Descriptor(name);
        }
        if (type.equals(DOUBLE)) {
            return new DoubleDecoder.Descriptor(name);
        }
        if (type.equals(REAL)) {
            return new RealDecoder.Descriptor(name);
        }
        return new IntegerDecoder.Descriptor(name);
    }
}
```

(add the license header, and replace the fully qualified `io.trino.spi.type.VarcharType.VARCHAR` references with a static import of `VARCHAR` and `io.trino.spi.function.FunctionName` with an import, so the file passes checkstyle.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestAggregationModel -Dair.check.skip-all=true -nsu`
Expected: FAIL with compilation errors (`TermAggregation`, `MetricAggregation` missing).

- [ ] **Step 3: Write minimal implementation**

`TermAggregation.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.opensearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public record TermAggregation(String term, Type type)
{
    public TermAggregation
    {
        requireNonNull(term, "term is null");
        requireNonNull(type, "type is null");
    }

    public static Optional<TermAggregation> fromColumn(OpenSearchColumnHandle column)
    {
        if (!column.supportsPredicates() || isBuiltinColumn(column.name()) || !isSupportedGroupingType(column.type())) {
            return Optional.empty();
        }
        return Optional.of(new TermAggregation(column.name(), column.type()));
    }

    private static boolean isSupportedGroupingType(Type type)
    {
        return type instanceof VarcharType
                || type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(BOOLEAN);
    }
}
```

`MetricAggregation.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.opensearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public record MetricAggregation(String functionName, Type outputType, Optional<OpenSearchColumnHandle> columnHandle, String alias)
{
    public static final String COUNT = "count";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String SUM = "sum";
    public static final String AVG = "avg";

    private static final Set<String> SUPPORTED_FUNCTIONS = ImmutableSet.of(COUNT, MIN, MAX, SUM, AVG);

    public MetricAggregation
    {
        requireNonNull(functionName, "functionName is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(alias, "alias is null");
    }

    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias)
    {
        if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty()) {
            return Optional.empty();
        }

        String functionName = function.getFunctionName();
        if (!SUPPORTED_FUNCTIONS.contains(functionName)) {
            return Optional.empty();
        }

        if (functionName.equals(COUNT) && function.getArguments().isEmpty()) {
            return Optional.of(new MetricAggregation(COUNT, function.getOutputType(), Optional.empty(), alias));
        }

        if (function.getArguments().size() != 1 || !(function.getArguments().getFirst() instanceof Variable variable)) {
            return Optional.empty();
        }
        if (!(assignments.get(variable.getName()) instanceof OpenSearchColumnHandle column)
                || !column.supportsPredicates()
                || isBuiltinColumn(column.name())
                || !isSupportedInput(functionName, column.type())) {
            return Optional.empty();
        }
        return Optional.of(new MetricAggregation(functionName, function.getOutputType(), Optional.of(column), alias));
    }

    private static boolean isSupportedInput(String functionName, Type inputType)
    {
        return switch (functionName) {
            // value_count works on any field type that supports predicates
            case COUNT -> true;
            // BIGINT is excluded: metric aggregations return doubles, so values above 2^53 lose precision
            case MIN, MAX -> inputType.equals(TINYINT) || inputType.equals(SMALLINT) || inputType.equals(INTEGER) || inputType.equals(REAL) || inputType.equals(DOUBLE);
            // REAL is excluded: OpenSearch accumulates in double while Trino accumulates in single precision
            case SUM, AVG -> inputType.equals(TINYINT) || inputType.equals(SMALLINT) || inputType.equals(INTEGER) || inputType.equals(DOUBLE);
            default -> false;
        };
    }
}
```

Replace `OpenSearchTableHandle.java` with the final version (keep license header and package):

```java
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record OpenSearchTableHandle(
        Type type,
        String schema,
        String index,
        TupleDomain<ColumnHandle> constraint,
        Map<String, String> regexes,
        Optional<String> query,
        Optional<TopN> topN,
        Set<OpenSearchColumnHandle> columns,
        List<TermAggregation> termAggregations,
        List<MetricAggregation> metricAggregations)
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY, AGGREGATION
    }

    public OpenSearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this(type,
                schema,
                index,
                TupleDomain.all(),
                ImmutableMap.of(),
                query,
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableList.of(),
                ImmutableList.of());
    }

    public OpenSearchTableHandle withColumns(Set<OpenSearchColumnHandle> columns)
    {
        return new OpenSearchTableHandle(type, schema, index, constraint, regexes, query, topN, columns, termAggregations, metricAggregations);
    }

    public OpenSearchTableHandle withTopN(TopN topN)
    {
        return new OpenSearchTableHandle(type, schema, index, constraint, regexes, query, Optional.of(topN), columns, termAggregations, metricAggregations);
    }

    public OpenSearchTableHandle withAggregations(List<TermAggregation> termAggregations, List<MetricAggregation> metricAggregations)
    {
        return new OpenSearchTableHandle(Type.AGGREGATION, schema, index, constraint, regexes, query, topN, columns, termAggregations, metricAggregations);
    }

    public OpenSearchTableHandle
    {
        requireNonNull(type, "type is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(index, "index is null");
        requireNonNull(constraint, "constraint is null");
        regexes = ImmutableMap.copyOf(requireNonNull(regexes, "regexes is null"));
        columns = ImmutableSet.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(query, "query is null");
        requireNonNull(topN, "topN is null");
        termAggregations = ImmutableList.copyOf(requireNonNull(termAggregations, "termAggregations is null"));
        metricAggregations = ImmutableList.copyOf(requireNonNull(metricAggregations, "metricAggregations is null"));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(type + ":" + index);

        StringBuilder attributes = new StringBuilder();
        if (!regexes.isEmpty()) {
            attributes.append("regexes=[");
            attributes.append(regexes.entrySet().stream()
                    .map(regex -> regex.getKey() + ":" + regex.getValue())
                    .collect(Collectors.joining(", ")));
            attributes.append("]");
        }
        topN.ifPresent(value -> attributes.append("topN=" + value));
        query.ifPresent(value -> attributes.append("query" + value));
        if (!termAggregations.isEmpty()) {
            attributes.append("groupBy=" + termAggregations);
        }
        if (!metricAggregations.isEmpty()) {
            attributes.append("aggregations=" + metricAggregations);
        }

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
```

In `OpenSearchMetadata.applyFilter`, the final `new OpenSearchTableHandle(...)` call now ends with `handle.topN(), ImmutableSet.of(), handle.termAggregations(), handle.metricAggregations());`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestAggregationModel,TestOpenSearchMetadata,TestTopN' -Dair.check.skip-all=true -nsu`
Expected: PASS.

- [ ] **Step 5: Format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`. Do not commit.

---

### Task 5: `applyAggregation` and handle guards

**Files:**
- Modify: `OpenSearchMetadata.java`
- Test: `TestOpenSearchMetadata.java`

**Interfaces:**
- Consumes: `TermAggregation.fromColumn`, `MetricAggregation.from`, `OpenSearchTableHandle.withAggregations`, `OpenSearchSessionProperties.isAggregationPushdownEnabled`.
- Produces: `OpenSearchMetadata.applyAggregation(ConnectorSession, ConnectorTableHandle, List<AggregateFunction>, Map<String, ColumnHandle>, List<List<ColumnHandle>>): Optional<AggregationApplicationResult<ConnectorTableHandle>>`. Output variable and column names are `_pushdown_<i>`.

- [ ] **Step 1: Write the failing tests**

Add to `TestOpenSearchMetadata` (extra imports: `io.trino.spi.connector.AggregateFunction`, `io.trino.spi.connector.AggregationApplicationResult`, `io.trino.spi.connector.Constraint`, `io.trino.spi.expression.Variable`, `static io.trino.spi.type.DoubleType.DOUBLE`, `static io.trino.spi.type.IntegerType.INTEGER`, `static io.trino.spi.type.BooleanType.BOOLEAN`):

```java
    @Test
    public void testApplyGlobalCountStar()
    {
        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                scanHandle(),
                List.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                Map.of(),
                List.of(List.of()))
                .orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.type()).isEqualTo(OpenSearchTableHandle.Type.AGGREGATION);
        assertThat(handle.termAggregations()).isEmpty();
        assertThat(handle.metricAggregations()).containsExactly(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"));
        assertThat(result.getProjections()).containsExactly(new Variable("_pushdown_0", BIGINT));
        assertThat(result.getAssignments()).singleElement().satisfies(assignment -> {
            assertThat(assignment.getVariable()).isEqualTo("_pushdown_0");
            assertThat(assignment.getType()).isEqualTo(BIGINT);
            OpenSearchColumnHandle column = (OpenSearchColumnHandle) assignment.getColumn();
            assertThat(column.name()).isEqualTo("_pushdown_0");
            assertThat(column.supportsPredicates()).isFalse();
        });
    }

    @Test
    public void testApplyGroupedSum()
    {
        OpenSearchColumnHandle value = integerColumn("value");
        OpenSearchColumnHandle group = keywordColumn("kind");

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                scanHandle(),
                List.of(new AggregateFunction("sum", BIGINT, List.of(new Variable("value", INTEGER)), List.of(), false, Optional.empty())),
                Map.of("value", value),
                List.of(List.of(group)))
                .orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.termAggregations()).containsExactly(new TermAggregation("kind", io.trino.spi.type.VarcharType.VARCHAR));
        assertThat(handle.metricAggregations()).containsExactly(new MetricAggregation("sum", BIGINT, Optional.of(value), "_pushdown_0"));
    }

    @Test
    public void testApplyAggregationOutputTypes()
    {
        OpenSearchColumnHandle value = integerColumn("value");
        OpenSearchColumnHandle price = doubleColumn("price");

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                scanHandle(),
                List.of(
                        new AggregateFunction("min", INTEGER, List.of(new Variable("value", INTEGER)), List.of(), false, Optional.empty()),
                        new AggregateFunction("avg", DOUBLE, List.of(new Variable("value", INTEGER)), List.of(), false, Optional.empty()),
                        new AggregateFunction("max", DOUBLE, List.of(new Variable("price", DOUBLE)), List.of(), false, Optional.empty())),
                Map.of("value", value, "price", price),
                List.of(List.of()))
                .orElseThrow();

        assertThat(result.getAssignments()).extracting(assignment -> assignment.getVariable() + ":" + assignment.getType())
                .containsExactly("_pushdown_0:integer", "_pushdown_1:double", "_pushdown_2:double");
    }

    @Test
    public void testApplyAggregationRejections()
    {
        OpenSearchColumnHandle value = integerColumn("value");
        OpenSearchColumnHandle group = keywordColumn("kind");
        AggregateFunction sum = new AggregateFunction("sum", BIGINT, List.of(new Variable("value", INTEGER)), List.of(), false, Optional.empty());
        Map<String, ColumnHandle> assignments = Map.of("value", value);

        // multiple grouping sets
        assertThat(metadata.applyAggregation(SESSION, scanHandle(), List.of(sum), assignments, List.of(List.of(group), List.of()))).isEmpty();
        // unsupported group-by column
        assertThat(metadata.applyAggregation(SESSION, scanHandle(), List.of(sum), assignments, List.of(List.of(textColumn("description"))))).isEmpty();
        // unsupported aggregate
        assertThat(metadata.applyAggregation(
                SESSION,
                scanHandle(),
                List.of(new AggregateFunction("sum", BIGINT, List.of(new Variable("regionkey", BIGINT)), List.of(), false, Optional.empty())),
                Map.of("regionkey", bigintColumn("regionkey")),
                List.of(List.of()))).isEmpty();
        // already an aggregation
        OpenSearchTableHandle aggregation = scanHandle().withAggregations(List.of(), List.of());
        assertThat(metadata.applyAggregation(SESSION, aggregation, List.of(sum), assignments, List.of(List.of()))).isEmpty();
        // existing TopN
        assertThat(metadata.applyAggregation(SESSION, scanHandle().withTopN(TopN.fromLimit(5)), List.of(sum), assignments, List.of(List.of()))).isEmpty();
        // passthrough query
        assertThat(metadata.applyAggregation(SESSION, new OpenSearchTableHandle(QUERY, "default", "nation", Optional.of("{}")), List.of(sum), assignments, List.of(List.of()))).isEmpty();
        // disabled by session property
        assertThat(metadata.applyAggregation(session(false), scanHandle(), List.of(sum), assignments, List.of(List.of()))).isEmpty();
    }

    @Test
    public void testLimitTopNAndFilterAreRejectedOverAggregation()
    {
        OpenSearchTableHandle aggregation = scanHandle().withAggregations(List.of(), List.of());

        assertThat(metadata.applyLimit(SESSION, aggregation, 5)).isEmpty();
        assertThat(metadata.applyTopN(SESSION, aggregation, 5, List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)), Map.of("regionkey", bigintColumn("regionkey")))).isEmpty();
        assertThat(metadata.applyFilter(SESSION, aggregation, new Constraint(TupleDomain.all()))).isEmpty();
    }

    static OpenSearchColumnHandle integerColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle doubleColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor(name), true);
    }
```

(Replace the fully qualified `VARCHAR` with the static import already added in Task 3; add `IntegerDecoder`, `DoubleDecoder` imports and `ColumnHandle` import.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchMetadata -Dair.check.skip-all=true -nsu`
Expected: FAIL (`applyAggregation` falls back to the SPI default and returns `Optional.empty()`; guards for limit/topN/filter missing).

- [ ] **Step 3: Write minimal implementation**

In `OpenSearchMetadata` add imports: `io.trino.spi.connector.AggregateFunction`, `io.trino.spi.connector.AggregationApplicationResult`, `static com.google.common.base.Verify.verify`, `static io.trino.plugin.opensearch.OpenSearchSessionProperties.isAggregationPushdownEnabled`, `static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.AGGREGATION`.

Add constant near the other constants:

```java
    private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "_pushdown_";
```

Guards (insert after the `isPassthroughQuery(handle)` early return in each method):

```java
        // in applyLimit, applyTopN and applyFilter
        if (handle.type() == AGGREGATION) {
            // pushing a limit, sort or filter below an already pushed aggregation would change its meaning
            return Optional.empty();
        }
```

`applyAggregation` (place after `applyTopN`):

```java
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }

        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;
        if (isPassthroughQuery(handle) || handle.type() == AGGREGATION || handle.topN().isPresent()) {
            return Optional.empty();
        }

        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");
        if (groupingSets.size() != 1) {
            // GROUPING SETS, CUBE and ROLLUP are not supported
            return Optional.empty();
        }

        ImmutableList.Builder<TermAggregation> termAggregations = ImmutableList.builder();
        for (ColumnHandle columnHandle : groupingSets.getFirst()) {
            Optional<TermAggregation> termAggregation = TermAggregation.fromColumn((OpenSearchColumnHandle) columnHandle);
            if (termAggregation.isEmpty()) {
                return Optional.empty();
            }
            termAggregations.add(termAggregation.get());
        }

        ImmutableList.Builder<MetricAggregation> metricAggregations = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index);
            String name = SYNTHETIC_COLUMN_NAME_PREFIX + index;

            Optional<MetricAggregation> metricAggregation = MetricAggregation.from(function, assignments, name);
            Optional<OpenSearchColumnHandle> outputColumn = aggregationOutputColumn(name, function.getOutputType());
            if (metricAggregation.isEmpty() || outputColumn.isEmpty()) {
                return Optional.empty();
            }
            metricAggregations.add(metricAggregation.get());
            projections.add(new Variable(name, function.getOutputType()));
            resultAssignments.add(new Assignment(name, outputColumn.get(), function.getOutputType()));
        }

        return Optional.of(new AggregationApplicationResult<>(
                handle.withAggregations(termAggregations.build(), metricAggregations.build()),
                projections.build(),
                resultAssignments.build(),
                ImmutableMap.of(),
                false));
    }

    private static Optional<OpenSearchColumnHandle> aggregationOutputColumn(String name, Type type)
    {
        if (type.equals(BIGINT)) {
            return Optional.of(syntheticColumn(name, type, "long", new BigintDecoder.Descriptor(name)));
        }
        if (type.equals(INTEGER)) {
            return Optional.of(syntheticColumn(name, type, "integer", new IntegerDecoder.Descriptor(name)));
        }
        if (type.equals(SMALLINT)) {
            return Optional.of(syntheticColumn(name, type, "short", new SmallintDecoder.Descriptor(name)));
        }
        if (type.equals(TINYINT)) {
            return Optional.of(syntheticColumn(name, type, "byte", new TinyintDecoder.Descriptor(name)));
        }
        if (type.equals(DOUBLE)) {
            return Optional.of(syntheticColumn(name, type, "double", new DoubleDecoder.Descriptor(name)));
        }
        if (type.equals(REAL)) {
            return Optional.of(syntheticColumn(name, type, "float", new RealDecoder.Descriptor(name)));
        }
        return Optional.empty();
    }

    private static OpenSearchColumnHandle syntheticColumn(String name, Type type, String opensearchType, DecoderDescriptor decoderDescriptor)
    {
        // synthetic columns never support predicates
        return new OpenSearchColumnHandle(ImmutableList.of(name), type, new PrimitiveType(opensearchType), decoderDescriptor, false);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchMetadata,TestAggregationModel' -Dair.check.skip-all=true -nsu`
Expected: PASS.

- [ ] **Step 5: Format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`. Do not commit.

---

### Task 6: Aggregation request building and client method

**Files:**
- Modify: `OpenSearchQueryBuilder.java`, `client/OpenSearchClient.java`
- Test: `TestOpenSearchQueryBuilder.java`

**Interfaces:**
- Consumes: `TermAggregation`, `MetricAggregation`.
- Produces:
  - `OpenSearchQueryBuilder.COMPOSITE_AGGREGATION_NAME = "groupBy"` (public constant).
  - `OpenSearchQueryBuilder.buildAggregationQuery(List<TermAggregation> termAggregations, List<MetricAggregation> metricAggregations, int pageSize, Optional<Map<String, Object>> after): List<AggregationBuilder>`. `count(*)` produces no builder. `sum` produces a `stats` builder; `count(col)` produces `value_count`.
  - `OpenSearchClient.beginAggregationSearch(String index, QueryBuilder query, List<AggregationBuilder> aggregations): SearchResponse` (`size=0`, `trackTotalHits=true`, no scroll, no shard preference).

- [ ] **Step 1: Write the failing tests**

Add to `TestOpenSearchQueryBuilder` (imports: `java.util.List`, `org.opensearch.search.aggregations.AggregationBuilder`, `org.opensearch.search.aggregations.AggregationBuilders`, `org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder`, `org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder`, `static io.trino.spi.type.BigintType.BIGINT`, `static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery`; `NAME`, `AGE`, `SCORE` fixtures already exist in the file):

```java
    @Test
    public void testGlobalAggregations()
    {
        List<AggregationBuilder> builders = buildAggregationQuery(
                ImmutableList.of(),
                ImmutableList.of(
                        new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"),
                        new MetricAggregation("count", BIGINT, Optional.of(AGE), "_pushdown_1"),
                        new MetricAggregation("sum", BIGINT, Optional.of(AGE), "_pushdown_2"),
                        new MetricAggregation("avg", DOUBLE, Optional.of(AGE), "_pushdown_3"),
                        new MetricAggregation("min", INTEGER, Optional.of(AGE), "_pushdown_4"),
                        new MetricAggregation("max", DOUBLE, Optional.of(SCORE), "_pushdown_5")),
                100,
                Optional.empty());

        // count(*) needs no aggregation, sum is computed with stats so an empty input can be reported as NULL
        assertThat(builders).containsExactly(
                AggregationBuilders.count("_pushdown_1").field("age"),
                AggregationBuilders.stats("_pushdown_2").field("age"),
                AggregationBuilders.avg("_pushdown_3").field("age"),
                AggregationBuilders.min("_pushdown_4").field("age"),
                AggregationBuilders.max("_pushdown_5").field("score"));
    }

    @Test
    public void testCountStarOnlyBuildsNoAggregations()
    {
        assertThat(buildAggregationQuery(
                ImmutableList.of(),
                ImmutableList.of(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0")),
                100,
                Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testGroupedAggregations()
    {
        List<AggregationBuilder> builders = buildAggregationQuery(
                ImmutableList.of(new TermAggregation("name", VARCHAR), new TermAggregation("age", INTEGER)),
                ImmutableList.of(new MetricAggregation("max", DOUBLE, Optional.of(SCORE), "_pushdown_0")),
                50,
                Optional.of(ImmutableMap.of("name", "alice", "age", 30)));

        assertThat(builders).containsExactly(
                new CompositeAggregationBuilder(
                        "groupBy",
                        ImmutableList.of(
                                new TermsValuesSourceBuilder("name").field("name").missingBucket(true),
                                new TermsValuesSourceBuilder("age").field("age").missingBucket(true)))
                        .size(50)
                        .aggregateAfter(ImmutableMap.of("name", "alice", "age", 30))
                        .subAggregation(AggregationBuilders.max("_pushdown_0").field("score")));
    }
```

(`import io.trino.plugin.opensearch.MetricAggregation` is unnecessary since the test is in the same package.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchQueryBuilder -Dair.check.skip-all=true -nsu`
Expected: FAIL with a compilation error (`buildAggregationQuery` missing).

- [ ] **Step 3: Write minimal implementation**

In `OpenSearchQueryBuilder` add imports (`com.google.common.collect.ImmutableList` already present; add `org.opensearch.search.aggregations.AggregationBuilder`, `org.opensearch.search.aggregations.AggregationBuilders`, `org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder`, `org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder`, `org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder`, `static com.google.common.collect.ImmutableList.toImmutableList`, `static io.trino.plugin.opensearch.MetricAggregation.AVG`, `COUNT`, `MAX`, `MIN`, `SUM`) and add:

```java
    public static final String COMPOSITE_AGGREGATION_NAME = "groupBy";

    public static List<AggregationBuilder> buildAggregationQuery(
            List<TermAggregation> termAggregations,
            List<MetricAggregation> metricAggregations,
            int pageSize,
            Optional<Map<String, Object>> after)
    {
        List<AggregationBuilder> metrics = metricAggregations.stream()
                .flatMap(aggregation -> buildMetricAggregation(aggregation).stream())
                .collect(toImmutableList());
        if (termAggregations.isEmpty()) {
            return metrics;
        }

        ImmutableList.Builder<CompositeValuesSourceBuilder<?>> sources = ImmutableList.builder();
        for (TermAggregation termAggregation : termAggregations) {
            // missingBucket keeps rows whose grouping column is NULL as their own group
            sources.add(new TermsValuesSourceBuilder(termAggregation.term())
                    .field(termAggregation.term())
                    .missingBucket(true));
        }
        CompositeAggregationBuilder composite = new CompositeAggregationBuilder(COMPOSITE_AGGREGATION_NAME, sources.build())
                .size(pageSize);
        after.ifPresent(composite::aggregateAfter);
        metrics.forEach(composite::subAggregation);
        return ImmutableList.of(composite);
    }

    private static Optional<AggregationBuilder> buildMetricAggregation(MetricAggregation aggregation)
    {
        if (aggregation.columnHandle().isEmpty()) {
            // count(*) is answered from the bucket doc_count or the total hits
            return Optional.empty();
        }
        String field = aggregation.columnHandle().orElseThrow().name();
        String alias = aggregation.alias();
        AggregationBuilder builder = switch (aggregation.functionName()) {
            case COUNT -> AggregationBuilders.count(alias).field(field);
            case MIN -> AggregationBuilders.min(alias).field(field);
            case MAX -> AggregationBuilders.max(alias).field(field);
            // stats instead of sum: the sum aggregation returns 0 for an empty input where SQL requires NULL
            case SUM -> AggregationBuilders.stats(alias).field(field);
            case AVG -> AggregationBuilders.avg(alias).field(field);
            default -> throw new IllegalArgumentException("Unsupported aggregation function: " + aggregation.functionName());
        };
        return Optional.of(builder);
    }
```

In `OpenSearchClient`, extract the shared request execution from `beginSearch` and add the aggregation method. Replace the `long start = System.nanoTime(); try { return client.search(request); } ... finally { ... }` block at the end of `beginSearch` with `return search(request);` and add:

```java
    public SearchResponse beginAggregationSearch(String index, QueryBuilder query, List<AggregationBuilder> aggregations)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query)
                .size(0)
                // accurate total hits are required for count(*), the default stops counting at 10000
                .trackTotalHits(true);
        aggregations.forEach(sourceBuilder::aggregation);

        LOG.debug("Begin aggregation search: %s, query: %s", index, sourceBuilder);

        SearchRequest request = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                .source(sourceBuilder);

        return search(request);
    }

    private SearchResponse search(SearchRequest request)
    {
        long start = System.nanoTime();
        try {
            return client.search(request);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        catch (OpenSearchStatusException e) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed.length > 0) {
                Throwable cause = suppressed[0];
                if (cause instanceof ResponseException responseException) {
                    throw propagate(responseException);
                }
            }

            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            searchStats.add(Duration.nanosSince(start));
        }
    }
```

(add `import org.opensearch.search.aggregations.AggregationBuilder;`; the catch blocks above are the exact ones moved out of `beginSearch`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchQueryBuilder,TestOpenSearchMetadata' -Dair.check.skip-all=true -nsu`
Expected: PASS. Then `./mvnw -pl plugin/trino-opensearch test-compile -Dair.check.skip-all=true -nsu` to confirm the client refactor compiles.

- [ ] **Step 5: Format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`. Do not commit.

---

### Task 7: Response reader, aggregate page source, and wiring

**Files:**
- Create: `AggregationResponseReader.java`, `AggregateQueryPageSource.java`, `TestAggregationResponseReader.java`
- Modify: `OpenSearchPageSourceProvider.java`, `OpenSearchSplitManager.java`

**Interfaces:**
- Consumes: `OpenSearchQueryBuilder.buildAggregationQuery`, `OpenSearchQueryBuilder.COMPOSITE_AGGREGATION_NAME`, `OpenSearchClient.beginAggregationSearch`, `TermAggregation`, `MetricAggregation`.
- Produces:
  - `AggregationResponseReader.read(Aggregations aggregations, long totalHits, List<TermAggregation> termAggregations, List<MetricAggregation> metricAggregations): AggregationResponseReader.Result` (`aggregations` may be `null`).
  - `record Result(List<Map<String, Object>> rows, Optional<Map<String, Object>> afterKey, int bucketCount)`. Row keys: `TermAggregation.term()` for group columns, `MetricAggregation.alias()` for aggregates. Values: `Long` for counts, `Double` for other metrics, `null` for SQL NULL, group keys as returned by OpenSearch (booleans normalized to `Boolean`).
  - `AggregateQueryPageSource(OpenSearchClient client, OpenSearchTableHandle table, List<OpenSearchColumnHandle> columns, int pageSize)`.

- [ ] **Step 1: Write the failing test**

Create `TestAggregationResponseReader.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.AggregationResponseReader.Result;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite;
import org.opensearch.search.aggregations.metrics.ParsedAvg;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedStats;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.integerColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationResponseReader
{
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(ImmutableList.of(
            entry("composite", (parser, name) -> ParsedComposite.fromXContent(parser, (String) name)),
            entry("max", (parser, name) -> ParsedMax.fromXContent(parser, (String) name)),
            entry("min", (parser, name) -> ParsedMin.fromXContent(parser, (String) name)),
            entry("avg", (parser, name) -> ParsedAvg.fromXContent(parser, (String) name)),
            entry("stats", (parser, name) -> ParsedStats.fromXContent(parser, (String) name)),
            entry("value_count", (parser, name) -> ParsedValueCount.fromXContent(parser, (String) name))));

    private static final MetricAggregation COUNT_STAR = new MetricAggregation("count", BIGINT, Optional.empty(), "c_star");
    private static final MetricAggregation COUNT_VALUE = new MetricAggregation("count", BIGINT, Optional.of(integerColumn("v")), "c_value");
    private static final MetricAggregation SUM_VALUE = new MetricAggregation("sum", BIGINT, Optional.of(integerColumn("v")), "s_value");
    private static final MetricAggregation AVG_VALUE = new MetricAggregation("avg", DOUBLE, Optional.of(integerColumn("v")), "a_value");
    private static final MetricAggregation MIN_VALUE = new MetricAggregation("min", INTEGER, Optional.of(integerColumn("v")), "min_value");
    private static final MetricAggregation MAX_VALUE = new MetricAggregation("max", INTEGER, Optional.of(integerColumn("v")), "max_value");

    @Test
    public void testGlobalAggregations()
            throws IOException
    {
        Aggregations aggregations = parse("""
                {
                  "value_count#c_value": {"value": 3},
                  "stats#s_value": {"count": 3, "min": 1.0, "max": 5.0, "avg": 3.0, "sum": 9.0},
                  "avg#a_value": {"value": 3.0},
                  "min#min_value": {"value": 1.0},
                  "max#max_value": {"value": 5.0}
                }
                """);

        Result result = AggregationResponseReader.read(
                aggregations,
                3,
                ImmutableList.of(),
                ImmutableList.of(COUNT_STAR, COUNT_VALUE, SUM_VALUE, AVG_VALUE, MIN_VALUE, MAX_VALUE));

        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row)
                .containsEntry("c_star", 3L)
                .containsEntry("c_value", 3L)
                .containsEntry("s_value", 9.0)
                .containsEntry("a_value", 3.0)
                .containsEntry("min_value", 1.0)
                .containsEntry("max_value", 5.0));
        assertThat(result.afterKey()).isEmpty();
    }

    @Test
    public void testGlobalAggregationsOverEmptyInput()
            throws IOException
    {
        Aggregations aggregations = parse("""
                {
                  "value_count#c_value": {"value": 0},
                  "stats#s_value": {"count": 0, "min": null, "max": null, "avg": null, "sum": 0.0},
                  "avg#a_value": {"value": null},
                  "min#min_value": {"value": null},
                  "max#max_value": {"value": null}
                }
                """);

        Result result = AggregationResponseReader.read(
                aggregations,
                0,
                ImmutableList.of(),
                ImmutableList.of(COUNT_STAR, COUNT_VALUE, SUM_VALUE, AVG_VALUE, MIN_VALUE, MAX_VALUE));

        Map<String, Object> expected = new HashMap<>();
        expected.put("c_star", 0L);
        expected.put("c_value", 0L);
        expected.put("s_value", null);
        expected.put("a_value", null);
        expected.put("min_value", null);
        expected.put("max_value", null);
        assertThat(result.rows()).containsExactly(expected);
    }

    @Test
    public void testCountStarWithoutAggregations()
    {
        Result result = AggregationResponseReader.read(null, 7, ImmutableList.of(), ImmutableList.of(COUNT_STAR));

        assertThat(result.rows()).containsExactly(Map.of("c_star", 7L));
        assertThat(result.afterKey()).isEmpty();
    }

    @Test
    public void testGroupedAggregations()
            throws IOException
    {
        Aggregations aggregations = parse("""
                {
                  "composite#groupBy": {
                    "after_key": {"kind": "b"},
                    "buckets": [
                      {"key": {"kind": "a"}, "doc_count": 2, "stats#s_value": {"count": 2, "min": 10.0, "max": 20.0, "avg": 15.0, "sum": 30.0}},
                      {"key": {"kind": "b"}, "doc_count": 1, "stats#s_value": {"count": 1, "min": 5.0, "max": 5.0, "avg": 5.0, "sum": 5.0}}
                    ]
                  }
                }
                """);

        Result result = AggregationResponseReader.read(
                aggregations,
                3,
                ImmutableList.of(new TermAggregation("kind", VARCHAR)),
                ImmutableList.of(COUNT_STAR, SUM_VALUE));

        assertThat(result.rows()).containsExactly(
                Map.of("kind", "a", "c_star", 2L, "s_value", 30.0),
                Map.of("kind", "b", "c_star", 1L, "s_value", 5.0));
        assertThat(result.afterKey()).hasValue(Map.of("kind", "b"));
        assertThat(result.bucketCount()).isEqualTo(2);
    }

    @Test
    public void testGroupedNullKeyAndBooleanKey()
            throws IOException
    {
        Aggregations aggregations = parse("""
                {
                  "composite#groupBy": {
                    "after_key": {"flag": 1, "kind": "x"},
                    "buckets": [
                      {"key": {"flag": null, "kind": null}, "doc_count": 4},
                      {"key": {"flag": 1, "kind": "x"}, "doc_count": 1}
                    ]
                  }
                }
                """);

        Result result = AggregationResponseReader.read(
                aggregations,
                5,
                ImmutableList.of(new TermAggregation("flag", BOOLEAN), new TermAggregation("kind", VARCHAR)),
                ImmutableList.of(COUNT_STAR));

        Map<String, Object> nullGroup = new HashMap<>();
        nullGroup.put("flag", null);
        nullGroup.put("kind", null);
        nullGroup.put("c_star", 4L);
        assertThat(result.rows()).containsExactly(
                nullGroup,
                Map.of("flag", true, "kind", "x", "c_star", 1L));
    }

    @Test
    public void testGroupedWithNoBuckets()
            throws IOException
    {
        Aggregations aggregations = parse("""
                {
                  "composite#groupBy": {"buckets": []}
                }
                """);

        Result result = AggregationResponseReader.read(
                aggregations,
                0,
                ImmutableList.of(new TermAggregation("kind", VARCHAR)),
                ImmutableList.of(COUNT_STAR));

        assertThat(result.rows()).isEmpty();
        assertThat(result.afterKey()).isEmpty();
        assertThat(result.bucketCount()).isEqualTo(0);
    }

    private static NamedXContentRegistry.Entry entry(String name, ContextParser<Object, Aggregation> parser)
    {
        return new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(name), parser);
    }

    private static Aggregations parse(String json)
            throws IOException
    {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(REGISTRY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            parser.nextToken();
            return Aggregations.fromXContent(parser);
        }
    }
}
```

(Remove the unused `List` import. If a type or package name in these imports is rejected by the compiler, locate the class with `javap -cp <jar>` using the OpenSearch 3.8.0 jars under `~/.m2/repository/org/opensearch/`; on Windows the `javap -cp` separator is `;`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestAggregationResponseReader -Dair.check.skip-all=true -nsu`
Expected: FAIL with a compilation error (`AggregationResponseReader` missing).

- [ ] **Step 3: Write minimal implementation**

`AggregationResponseReader.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import jakarta.annotation.Nullable;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.ValueCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.plugin.opensearch.MetricAggregation.COUNT;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.COMPOSITE_AGGREGATION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

final class AggregationResponseReader
{
    private AggregationResponseReader() {}

    record Result(List<Map<String, Object>> rows, Optional<Map<String, Object>> afterKey, int bucketCount) {}

    static Result read(
            @Nullable Aggregations aggregations,
            long totalHits,
            List<TermAggregation> termAggregations,
            List<MetricAggregation> metricAggregations)
    {
        if (termAggregations.isEmpty()) {
            Map<String, Object> row = new HashMap<>();
            for (MetricAggregation metric : metricAggregations) {
                row.put(metric.alias(), metricValue(metric, aggregations, totalHits));
            }
            return new Result(List.of(row), Optional.empty(), 1);
        }

        CompositeAggregation composite = aggregations == null ? null : aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (composite == null) {
            return new Result(List.of(), Optional.empty(), 0);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (CompositeAggregation.Bucket bucket : composite.getBuckets()) {
            Map<String, Object> row = new HashMap<>();
            Map<String, Object> key = bucket.getKey();
            for (TermAggregation term : termAggregations) {
                row.put(term.term(), normalizeKey(term, key.get(term.term())));
            }
            for (MetricAggregation metric : metricAggregations) {
                row.put(metric.alias(), metricValue(metric, bucket.getAggregations(), bucket.getDocCount()));
            }
            rows.add(row);
        }
        return new Result(rows, Optional.ofNullable(composite.afterKey()), rows.size());
    }

    private static Object normalizeKey(TermAggregation term, @Nullable Object value)
    {
        // boolean fields may be reported as 0/1 in composite keys
        if (term.type().equals(BOOLEAN) && value instanceof Number number) {
            return number.longValue() != 0;
        }
        return value;
    }

    @Nullable
    private static Object metricValue(MetricAggregation metric, @Nullable Aggregations aggregations, long documentCount)
    {
        if (metric.columnHandle().isEmpty()) {
            // count(*)
            return documentCount;
        }

        Aggregation aggregation = aggregations == null ? null : aggregations.get(metric.alias());
        verifyNotNull(aggregation, "Missing aggregation result for %s", metric.alias());
        return switch (metric.functionName()) {
            case COUNT -> ((ValueCount) aggregation).getValue();
            case SUM -> sumValue((Stats) aggregation);
            default -> singleValue((NumericMetricsAggregation.SingleValue) aggregation);
        };
    }

    @Nullable
    private static Double sumValue(Stats stats)
    {
        // the sum of an empty input is NULL in SQL, but OpenSearch reports 0
        if (stats.getCount() == 0) {
            return null;
        }
        return stats.getSum();
    }

    @Nullable
    private static Double singleValue(NumericMetricsAggregation.SingleValue aggregation)
    {
        // min, max and avg over an empty input are reported as +/-Infinity or NaN by the parsed response
        double value = aggregation.value();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return value;
    }
}
```

`AggregateQueryPageSource.java`:

```java
/* (license header) */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.AggregationResponseReader.Result;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static java.util.Objects.requireNonNull;

public class AggregateQueryPageSource
        implements ConnectorPageSource
{
    // decoders of aggregation output columns do not read from the hit
    private static final SearchHit NO_HIT = new SearchHit(0);

    private final OpenSearchClient client;
    private final OpenSearchTableHandle table;
    private final List<OpenSearchColumnHandle> columns;
    private final List<Decoder> decoders;
    private final QueryBuilder query;
    private final int pageSize;

    private Optional<Map<String, Object>> after = Optional.empty();
    private boolean finished;
    private long readTimeNanos;

    public AggregateQueryPageSource(OpenSearchClient client, OpenSearchTableHandle table, List<OpenSearchColumnHandle> columns, int pageSize)
    {
        this.client = requireNonNull(client, "client is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.decoders = this.columns.stream()
                .map(OpenSearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
        this.query = buildSearchQuery(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast), table.query(), table.regexes());
        this.pageSize = pageSize;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        SearchResponse response = client.beginAggregationSearch(
                table.index(),
                query,
                buildAggregationQuery(table.termAggregations(), table.metricAggregations(), pageSize, after));
        readTimeNanos += System.nanoTime() - start;

        verifyNotNull(response.getHits().getTotalHits(), "Total hits are missing from the aggregation response");
        Result result = AggregationResponseReader.read(
                response.getAggregations(),
                response.getHits().getTotalHits().value(),
                table.termAggregations(),
                table.metricAggregations());

        after = result.afterKey();
        // a composite page smaller than requested is the last one
        finished = table.termAggregations().isEmpty() || result.afterKey().isEmpty() || result.bucketCount() < pageSize;

        if (result.rows().isEmpty()) {
            return null;
        }
        if (columns.isEmpty()) {
            return SourcePage.create(result.rows().size());
        }

        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = columns.get(i).type().createBlockBuilder(null, result.rows().size());
        }
        for (Map<String, Object> row : result.rows()) {
            for (int i = 0; i < builders.length; i++) {
                String name = columns.get(i).name();
                decoders.get(i).decode(NO_HIT, () -> row.get(name), builders[i]);
            }
        }

        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public void close() {}
}
```

`OpenSearchPageSourceProvider`: add field and constructor parameter, and route aggregation tables (imports: `static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.AGGREGATION`):

```java
    private final int maxAggregationBuckets;

    @Inject
    public OpenSearchPageSourceProvider(OpenSearchClient client, TypeManager typeManager, OpenSearchConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxAggregationBuckets = requireNonNull(config, "config is null").getMaxAggregationBuckets();
    }
```

and between the `QUERY` check and the `columns.isEmpty()` check:

```java
        if (opensearchTable.type().equals(AGGREGATION)) {
            return new AggregateQueryPageSource(
                    client,
                    opensearchTable,
                    columns.stream()
                            .map(OpenSearchColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    maxAggregationBuckets);
        }
```

`OpenSearchSplitManager.getSplits`: change the first branch to

```java
        if (tableHandle.type().equals(OpenSearchTableHandle.Type.QUERY) || tableHandle.type().equals(OpenSearchTableHandle.Type.AGGREGATION)) {
            // aggregations run against the whole index so that OpenSearch merges shard results
            return new FixedSplitSource(new OpenSearchSplit(tableHandle.index(), 0, Optional.empty()));
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestAggregationResponseReader,TestOpenSearchMetadata,TestOpenSearchQueryBuilder,TestAggregationModel,TestTopN,TestOpenSearchConfig' -Dair.check.skip-all=true -nsu`
Expected: PASS. If `TestAggregationResponseReader` fails on a null-valued `min`/`max`/`avg` (a parsed response returning `0.0` instead of an infinity or NaN), switch those aggregations to read through `Stats` or add a sibling `value_count`; record what the parsed response actually returns before changing the reader. If `TotalHits.value()` does not compile, the Lucene version in use exposes the `value` field instead; use that.

- [ ] **Step 5: Format checkpoint**

Run `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu` and `./mvnw -pl plugin/trino-opensearch test-compile -Dair.check.skip-all=true -nsu`. Do not commit.

---

### Task 8: Integration tests, docs, and full verification

**Files:**
- Modify: `BaseOpenSearchConnectorTest.java`, `docs/src/main/sphinx/connector/opensearch.md`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the integration tests**

Add to `BaseOpenSearchConnectorTest` (new imports: `io.trino.sql.planner.plan.AggregationNode`, `io.trino.sql.query.QueryAssertions`; reuse existing `ImmutableMap`, `Language`, `randomNameSuffix`, `format`, `assertThat`):

```java
    @Test
    public void testCountPushdown()
    {
        assertThat(query("SELECT count(*) FROM nation"))
                .matches("VALUES BIGINT '25'")
                .isFullyPushedDown();
        assertThat(query("SELECT count(*) FROM nation WHERE regionkey = 1"))
                .matches("VALUES BIGINT '5'")
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*), count(nationkey) FROM nation GROUP BY regionkey"))
                .matches("VALUES (BIGINT '0', BIGINT '5', BIGINT '5'), (BIGINT '1', BIGINT '5', BIGINT '5'), (BIGINT '2', BIGINT '5', BIGINT '5'), (BIGINT '3', BIGINT '5', BIGINT '5'), (BIGINT '4', BIGINT '5', BIGINT '5')")
                .isFullyPushedDown();
        // HAVING is evaluated by Trino on top of the pushed aggregation
        assertQuery(
                "SELECT regionkey, count(*) FROM nation GROUP BY regionkey HAVING count(*) > 4 ORDER BY regionkey",
                "VALUES (0, 5), (1, 5), (2, 5), (3, 5), (4, 5)");
        // ORDER BY and LIMIT over an aggregation stay in Trino
        assertQueryOrdered("SELECT regionkey, count(*) FROM nation GROUP BY regionkey ORDER BY regionkey LIMIT 2");
    }

    @Test
    public void testBigintAggregatesAreNotPushedDown()
    {
        assertThat(query("SELECT sum(nationkey), min(nationkey), max(nationkey), avg(nationkey) FROM nation"))
                .matches("VALUES (BIGINT '300', BIGINT '0', BIGINT '24', DOUBLE '12.0')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey"))
                .isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testAggregationPushdownCanBeDisabledWithSessionProperty()
    {
        Session disabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "aggregation_pushdown_enabled", "false")
                .build();

        assertThat(query(disabled, "SELECT regionkey, count(*) FROM nation GROUP BY regionkey"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT regionkey, count(*) FROM nation GROUP BY regionkey"))
                .isFullyPushedDown();
    }

    @Test
    public void testAggregationsOverNumericFields()
            throws IOException
    {
        String tableName = "test_aggregation_numeric_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "g": { "type": "keyword" },
                        "i": { "type": "integer" },
                        "d": { "type": "double" }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.of("g", "a", "i", 1, "d", 1.5));
            index(tableName, ImmutableMap.of("g", "a", "i", 3, "d", 2.5));
            index(tableName, ImmutableMap.of("g", "b", "i", 10, "d", 0.5));

            assertThat(query(format("SELECT g, count(*), count(i), sum(i), min(i), max(i), avg(i), sum(d), min(d), max(d) FROM %s GROUP BY g", tableName)))
                    .matches("VALUES " +
                            "(CAST('a' AS VARCHAR), BIGINT '2', BIGINT '2', BIGINT '4', INTEGER '1', INTEGER '3', DOUBLE '2.0', DOUBLE '4.0', DOUBLE '1.5', DOUBLE '2.5'), " +
                            "(CAST('b' AS VARCHAR), BIGINT '1', BIGINT '1', BIGINT '10', INTEGER '10', INTEGER '10', DOUBLE '10.0', DOUBLE '0.5', DOUBLE '0.5', DOUBLE '0.5')")
                    .isFullyPushedDown();

            assertThat(query(format("SELECT count(*), sum(i), min(i), max(i), avg(i) FROM %s", tableName)))
                    .matches("VALUES (BIGINT '3', BIGINT '14', INTEGER '1', INTEGER '10', DOUBLE '4.666666666666667')")
                    .isFullyPushedDown();

            // keyword min/max is not supported by OpenSearch metric aggregations and stays in Trino
            assertThat(query(format("SELECT min(g), max(g) FROM %s", tableName)))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')")
                    .isNotFullyPushedDown(AggregationNode.class);
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testAggregationsWithMissingKeysAndValues()
            throws IOException
    {
        String tableName = "test_aggregation_missing_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "g": { "type": "keyword" },
                        "v": { "type": "integer" }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.of("g", "a", "v", 10));
            index(tableName, ImmutableMap.of("g", "a", "v", 20));
            index(tableName, ImmutableMap.of("g", "b", "v", 5));
            index(tableName, ImmutableMap.of("v", 7));
            index(tableName, ImmutableMap.of("v", 8));
            index(tableName, ImmutableMap.of("g", "c"));

            @Language("SQL")
            String groupedQuery = format("SELECT g, count(*), sum(v) FROM %s GROUP BY g", tableName);
            String expected = "VALUES " +
                    "(CAST(NULL AS VARCHAR), BIGINT '2', BIGINT '15'), " +
                    "(CAST('a' AS VARCHAR), BIGINT '2', BIGINT '30'), " +
                    "(CAST('b' AS VARCHAR), BIGINT '1', BIGINT '5'), " +
                    "(CAST('c' AS VARCHAR), BIGINT '1', CAST(NULL AS BIGINT))";
            assertThat(query(groupedQuery)).matches(expected).isFullyPushedDown();

            // a group whose values are all missing
            assertThat(query(format("SELECT count(*), count(v), sum(v), avg(v), min(v), max(v) FROM %s WHERE g = 'c'", tableName)))
                    .matches("VALUES (BIGINT '1', BIGINT '0', CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))")
                    .isFullyPushedDown();

            // empty input
            assertThat(query(format("SELECT count(*), count(v), sum(v), avg(v), min(v), max(v) FROM %s WHERE g = 'no_such_group'", tableName)))
                    .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))")
                    .isFullyPushedDown();
            assertThat(query(format("SELECT g, count(*) FROM %s WHERE g = 'no_such_group' GROUP BY g", tableName)))
                    .returnsEmptyResult();

            // two buckets per request force pagination, including a NULL group key in the after_key
            try (QueryAssertions assertions = new QueryAssertions(createAdHocQueryRunner(Map.of("opensearch.max-aggregation-buckets", "2")))) {
                assertThat(assertions.query(groupedQuery)).matches(expected).isFullyPushedDown();
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testAggregationPaginationWithSmallPageSize()
            throws Exception
    {
        try (QueryAssertions assertions = new QueryAssertions(createAdHocQueryRunner(Map.of("opensearch.max-aggregation-buckets", "2")))) {
            assertThat(assertions.query("SELECT regionkey, nationkey, count(*) FROM nation GROUP BY regionkey, nationkey"))
                    .matches("SELECT regionkey, nationkey, BIGINT '1' FROM nation")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testAggregationOnBooleanGroupingColumn()
            throws IOException
    {
        String tableName = "test_aggregation_boolean_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "flag": { "type": "boolean" }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.of("flag", true));
            index(tableName, ImmutableMap.of("flag", true));
            index(tableName, ImmutableMap.of("flag", false));

            assertThat(query(format("SELECT flag, count(*) FROM %s GROUP BY flag", tableName)))
                    .matches("VALUES (true, BIGINT '2'), (false, BIGINT '1')")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    private QueryRunner createAdHocQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return OpenSearchQueryRunner.builder(opensearch.getAddress())
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jmx.base-name", randomNameSuffix())
                        .putAll(connectorProperties)
                        .buildOrThrow())
                .build();
    }
```

Notes for the implementer: the ad hoc runner points at the same server and does not load TPCH tables (no `setInitialTables`), so it reads the indexes the main runner already created. If the boolean group-by test fails because composite keys for boolean fields are not `Boolean`/`0`/`1`, inspect the raw response with a `curl` against the container and adjust `AggregationResponseReader.normalizeKey`; do not remove the boolean case from `TermAggregation` without telling the user.

- [ ] **Step 2: Run the integration tests**

Docker must be running.
Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchLatestConnectorTest#testCountPushdown+testBigintAggregatesAreNotPushedDown+testAggregationPushdownCanBeDisabledWithSessionProperty+testAggregationsOverNumericFields+testAggregationsWithMissingKeysAndValues+testAggregationPaginationWithSmallPageSize+testAggregationOnBooleanGroupingColumn' -Dair.check.skip-all=true -nsu`
Expected: PASS. Failures here point at real behavior differences; diagnose from the OpenSearch response (the debug log in `OpenSearchClient.beginAggregationSearch` prints the request) and fix the production code or the documented limitation, not by weakening an assertion silently. Report any behavior that contradicts the spec to the user.

- [ ] **Step 3: Update the docs**

In `docs/src/main/sphinx/connector/opensearch.md`, add two rows to the configuration table after the `opensearch.projection-pushdown-enabled` row (before the closing `:::`):

```markdown
* - `opensearch.aggregation-pushdown-enabled`
  - Push down supported aggregations to OpenSearch. The catalog session property
    `aggregation_pushdown_enabled` overrides this value for a session.
  - `true`
* - `opensearch.max-aggregation-buckets`
  - Maximum number of buckets requested in each aggregation search request. The
    connector pages through larger results. Must not exceed the cluster
    setting `search.max_buckets`.
  - `65535`
```

Append after the `No other data types are supported for predicate push down.` line (and before the link reference definitions):

```markdown
(opensearch-aggregation-pushdown)=
### Aggregation push down

The connector supports [aggregation push down](aggregation-pushdown) for these
aggregate functions:

* `count(*)` and `count(column)`
* `min`, `max` on columns of type `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`,
  `DOUBLE`
* `sum`, `avg` on columns of type `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE`

Aggregation push down is applied only when all of the following hold:

* The query groups by none or by columns of type `VARCHAR` (`keyword`),
  `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, or `BOOLEAN`, with a single
  grouping set. `GROUPING SETS`, `CUBE` and `ROLLUP` are not pushed down.
* The aggregate arguments are plain columns that support predicate push down.
* The aggregates do not use `DISTINCT`, a `FILTER` clause, or an `ORDER BY`
  clause.
* The table is not accessed with the `raw_query` table function.

`min`, `max`, `sum` and `avg` over `BIGINT` columns are not pushed down because
OpenSearch computes metric aggregations with double precision, which cannot
represent all `BIGINT` values. `sum` over integer columns is computed in double
precision and is exact up to 2^53. `min` and `max` over `keyword` columns are
not pushed down.

Set `opensearch.aggregation-pushdown-enabled` to `false` to disable aggregation
push down.

(opensearch-topn-pushdown)=
### TopN push down

The connector supports [TopN push down](topn-pushdown) for queries with
`ORDER BY ... LIMIT n`. It is applied when every sort column supports predicate
push down. Each shard returns its sorted top `n` rows and Trino merges them.
```

- [ ] **Step 4: Full verification**

Run, from `C:\github\trino`:
1. `./mvnw -pl plugin/trino-opensearch airstyle:format -nsu`
2. `./mvnw -pl plugin/trino-opensearch validate` (checkstyle, modernizer, airstyle, dependency analysis).
3. `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchConfig,TestOpenSearchMetadata,TestOpenSearchQueryBuilder,TestAggregationModel,TestAggregationResponseReader,TestTopN' -Dair.check.skip-all=true -nsu`
4. With Docker running, the full integration suite: `./mvnw -pl plugin/trino-opensearch test -Dtest=TestOpenSearchLatestConnectorTest -Dair.check.skip-all=true -nsu`

Expected: all pass. If a test in step 4 fails and looks unrelated to this change, report its name and failure to the user instead of changing it. If `validate` reports unused imports or missing license headers, fix them.

- [ ] **Step 5: Report**

Report to the user: what was implemented, which commands were run and their results, which integration tests could not be run (if Docker was unavailable), and the two documented limitations (BIGINT aggregates and keyword min/max stay in Trino). Remind them to review the AI-generated change and docs before opening a PR. Do not commit unless asked.

---

## Self-Review

**Spec coverage:** table handle changes (Task 2, 4); `TopN`/`MetricAggregation`/`TermAggregation` (Task 2, 4); `applyAggregation` rules, output columns and `precalculateStatistics=false` (Task 5); `applyTopN` (Task 3); `applyLimit`/`applyFilter` guards over aggregations (Task 3, 5); `applyProjection` unchanged (no task needed); config, bucket page size and session property (Task 1, 7); single-split execution (Task 7); client `beginSearch` TopN and `beginAggregationSearch` (Task 2, 6); composite aggregation with `missingBucket`, pagination, `stats` for sum (Task 6, 7); empty-input semantics (Task 7 reader tests, Task 8 integration tests); unit and integration tests (Tasks 1-8); docs (Task 8). Spec items intentionally changed after the research: `REAL` excluded from `sum`/`avg`, builtin columns excluded, classes in the main package; the spec file was updated to match.

**Placeholder scan:** none; the only conditional guidance is the explicit fallback instructions for parsed-response null handling (Task 7 step 4) and boolean composite keys (Task 8 step 1), each tied to a concrete check.

**Type consistency:** `TopN.sortItems()`, `TopNSortItem(field, order)`, `OpenSearchTableHandle.withTopN/withAggregations/withColumns`, `MetricAggregation.from(AggregateFunction, Map<String, ColumnHandle>, String)`, `TermAggregation.fromColumn(OpenSearchColumnHandle)`, `OpenSearchQueryBuilder.buildAggregationQuery(List, List, int, Optional)`, `OpenSearchClient.beginAggregationSearch(String, QueryBuilder, List<AggregationBuilder>)`, `AggregationResponseReader.read(Aggregations, long, List, List)` and `Result(rows, afterKey, bucketCount)` are used with the same names and signatures in every task that references them. Test helpers `scanHandle()`, `session(boolean)`, `bigintColumn`, `keywordColumn`, `textColumn` (Task 2/3) and `integerColumn`, `doubleColumn` (Task 5) are declared in `TestOpenSearchMetadata` as package-private statics before the tests that import them (`TestAggregationModel` in Task 4 imports `bigintColumn`, `keywordColumn`, `textColumn` from Task 3; `TestAggregationResponseReader` in Task 7 imports `integerColumn` from Task 5).
