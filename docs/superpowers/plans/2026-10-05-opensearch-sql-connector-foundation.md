# OpenSearch SQL Connector (sub-project 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `opensearch-sql` connector module that pushes global aggregates (including `stddev`/`variance`) through the OpenSearch SQL plugin, while scans and `GROUP BY` keep using the existing `trino-opensearch` paths.

**Architecture:** `plugin/trino-opensearch-sql` depends on `trino-opensearch` as a library and subclasses `OpenSearchMetadata`, `OpenSearchPageSourceProvider` and `OpenSearchConnector`. A new table-handle type `SQL_AGGREGATION` (added to `trino-opensearch`) marks pushed global aggregates; `SqlAggregatePageSource` renders SQL from the handle (`SqlWhereRenderer` + `SqlAggregationQueryBuilder`), posts it to `/_plugins/_sql` through the existing authenticated client, validates the result (`SqlAggregationResponseReader`) and decodes one row. Anything not eligible falls back to `super.applyAggregation` (the DSL path).

**Tech Stack:** Java 25, Trino SPI, `trino-base` toolkit, OpenSearch 2.19 SQL plugin (`POST /_plugins/_sql`), Jackson, JUnit 5, AssertJ, Docker (OpenSearch 2.19.4 testcontainer) for integration tests.

**Spec:** `docs/superpowers/specs/2026-10-02-opensearch-sql-connector-foundation-design.md`

## Global Constraints

- New module: `plugin/trino-opensearch-sql`, artifact `trino-opensearch-sql`, packaging `trino-plugin`, connector name `opensearch-sql`, main and test package `io.trino.plugin.opensearch.sql`.
- It depends on the branch `opensearch-aggregation-topn-pushdown` work already in this branch's history; changes to `plugin/trino-opensearch` are additive only (Task 1).
- Config (exact): `opensearch.sql.global-aggregation-engine` enum `SQL` (default) | `DSL`; session property (exact): `global_aggregation_engine`. All `opensearch.*` properties of `trino-opensearch` apply unchanged.
- SQL pushdown applies only to **global** aggregates (one empty grouping set) on a plain `SCAN` handle with no TopN/limit, no raw `query`, no `regexes`. `GROUP BY` stays on the DSL path.
- Functions through SQL: `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop` (Trino names `stddev`/`variance` are canonicalised to `stddev_samp`/`var_samp`). `min`/`max` inputs: `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE`; `sum`/`avg`/statistical inputs: `TINYINT`, `SMALLINT`, `INTEGER`, `DOUBLE`. `BIGINT` aggregates, `REAL` sum/avg/statistical, `count(DISTINCT)`, `DISTINCT`/filtered/ordered aggregates, builtin and raw-JSON columns, nested (`path.size() > 1`) columns are never pushed to SQL.
- Verified SQL-plugin facts the code must respect: the REST `filter` parameter is silently ignored (never use it; render `WHERE` text); `sum()` over zero rows returns `0` (emit a companion `count(col)` and map `count = 0` to NULL); `stddev_pop`/`var_pop` over one row return NULL where Trino returns `0.0` (companion count: `count == 1` → `0.0`; `stddev_samp`/`var_samp` with `count <= 1` → NULL); a V2 failure silently falls back to the V1 engine whose `count` columns come back as `double` (always append a sentinel `count(*)` and require its schema type to be `long`); identifiers are backtick-quoted (index and column names containing a backtick or control character are rejected); timestamps render as `'yyyy-MM-dd HH:mm:ss.SSS'` UTC string literals; string literals double single quotes and reject backslashes and control characters; float values are widened to `double` before rendering.
- Rules from `.github/DEVELOPMENT.md` and `.claude/rules/trino-config-properties.md`: license header on every new file (copy it from any existing file), no wildcard imports, braces always, no mocks, AssertJ, Guava immutables, avoid abbreviations, avoid `get` in new method names (except bean getters and config getters), avoid ternary except trivial, `@ConfigDescription` on every `@Config`, config names with dashes and session names with snake_case.
- Unused imports fail checkstyle. `dependency:analyze` fails on used-undeclared and unused-declared dependencies.
- Build/test commands (run from `C:\github\trino`): unit tests `./mvnw -pl <module> test -Dtest='A,B' -Dair.check.skip-all=true -nsu`; module checks `./mvnw -pl <module> verify -DskipTests -Dair.check.skip-enforcer=true -Dair.check.skip-airstyle=true -nsu` (the enforcer rejects a non-Temurin JDK; airstyle flags unchanged files because of CRLF working copies); building the new module needs its sibling: use `-pl plugin/trino-opensearch-sql -am` or first `./mvnw install -pl plugin/trino-opensearch -DskipTests -Dair.check.skip-all=true -nsu`.
- Formatting: `./mvnw -pl <module> airstyle:format -nsu` rewrites line endings of every file in the module; afterwards restore files not part of your task with `git checkout -- <file>` (verify with `git diff --ignore-cr-at-eol --stat`) and keep each edited file's existing line endings.
- One commit per task on branch `opensearch-sql-connector`, ending with the trailer line `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Never push.
- Docker may be unavailable; integration tests (Task 7) must be written and compile, and are reported NOT RUN when Docker is down.

## File Structure

Modify in `plugin/trino-opensearch` (Task 1): `OpenSearchTableHandle.java`, `OpenSearchSplitManager.java`, `OpenSearchMetadata.java`, `MetricAggregation.java`, `PushdownColumns.java`, `client/OpenSearchClient.java`; tests `TestAggregationModel.java`, `TestOpenSearchMetadata.java`, new `client/TestSqlErrorMessage.java`.

Create in `plugin/trino-opensearch-sql` (`src/main/java/io/trino/plugin/opensearch/sql/` unless noted):
- `pom.xml` — module build (Task 2)
- `OpenSearchSqlPlugin`, `OpenSearchSqlConnectorFactory`, `OpenSearchSqlConnectorModule`, `OpenSearchSqlConnector`, `OpenSearchSqlConfig`, `OpenSearchSqlSessionProperties` — wiring (Task 2)
- `SqlIdentifiers`, `SqlWhereRenderer` — pure SQL text helpers (Task 3)
- `SqlAggregationQuery`, `SqlAggregationQueryBuilder`, `SqlResult`, `SqlAggregationResponseReader` — query shape and result validation (Task 4)
- `OpenSearchSqlClient` — transport and response parsing (Task 5)
- `OpenSearchSqlMetadata`, `OpenSearchSqlPageSourceProvider`, `SqlAggregatePageSource` — pushdown and execution (Task 6)
- Tests under `src/test/java/io/trino/plugin/opensearch/sql/`; integration support (`OpenSearchServer`, runner) and `TestOpenSearchSqlConnector` (Task 7)
- Docs: `docs/src/main/sphinx/connector/opensearch-sql.md`, entry in `docs/src/main/sphinx/connector.md` (Task 7)

Registrations (Task 2): root `pom.xml` module list, `core/trino-server/src/main/provisio/trino.xml`, `.github/config/labeler-config.yml`, `.github/schedule-config.yml`, `.github/workflows/ci.yml`.

---

### Task 1: Additive changes in trino-opensearch

**Files:**
- Modify: `plugin/trino-opensearch/src/main/java/io/trino/plugin/opensearch/OpenSearchTableHandle.java`, `OpenSearchSplitManager.java`, `OpenSearchMetadata.java`, `MetricAggregation.java`, `PushdownColumns.java`, `client/OpenSearchClient.java`
- Test: `plugin/trino-opensearch/src/test/java/io/trino/plugin/opensearch/TestAggregationModel.java`, `TestOpenSearchMetadata.java`; create `.../client/TestSqlErrorMessage.java`

**Interfaces:**
- Produces:
  - `OpenSearchTableHandle.Type.SQL_AGGREGATION`; `OpenSearchTableHandle.withSqlAggregations(List<MetricAggregation>): OpenSearchTableHandle` (type `SQL_AGGREGATION`, empty `termAggregations`).
  - `MetricAggregation`: constants `STDDEV_SAMP="stddev_samp"`, `STDDEV_POP="stddev_pop"`, `VAR_SAMP="var_samp"`, `VAR_POP="var_pop"`; `public static final Set<String> STATISTICAL_FUNCTIONS`, `DEFAULT_FUNCTIONS` (count,min,max,sum,avg), `SQL_FUNCTIONS` (default + statistical); `public static String canonicalFunctionName(String)` (`stddev`→`stddev_samp`, `variance`→`var_samp`, else unchanged); `public static Optional<MetricAggregation> from(AggregateFunction, Map<String, ColumnHandle>, String alias, Set<String> allowedFunctions)`. The existing 3-argument `from` keeps its behaviour (delegates with `DEFAULT_FUNCTIONS`).
  - `PushdownColumns` becomes `public final class` with `public static boolean isDocValuesPushdownSupported(OpenSearchColumnHandle)`.
  - `OpenSearchMetadata`: `protected static final String SYNTHETIC_COLUMN_NAME_PREFIX`, `protected static boolean isPassthroughQuery(OpenSearchTableHandle)`, `protected static boolean isAggregation(OpenSearchTableHandle)` (type `AGGREGATION` or `SQL_AGGREGATION`), `protected static Optional<OpenSearchColumnHandle> aggregationOutputColumn(String, Type)`. All former `handle.type() == AGGREGATION` guards use `isAggregation(handle)`.
  - `OpenSearchSplitManager.getSplits`: `QUERY`, `AGGREGATION` and `SQL_AGGREGATION` get the single whole-index split.
  - `OpenSearchClient.executeSql(String requestBody): String` — POSTs the body to `/_plugins/_sql` through the existing low-level client; HTTP error statuses throw `TrinoException(OPENSEARCH_QUERY_FAILURE, ...)` with the plugin's `reason`/`details`; I/O errors throw `OPENSEARCH_CONNECTION_ERROR`. `static String formatSqlError(int statusCode, String body)` (package-private) builds the message.

- [ ] **Step 1: Write the failing tests**

In `TestAggregationModel` add (imports as needed: `io.trino.plugin.opensearch.MetricAggregation` is same package; `Set`):

```java
    @Test
    public void testStatisticalFunctionsOnlyThroughSqlFunctionSet()
    {
        OpenSearchColumnHandle doubleColumn = column("d", DOUBLE, "double", true);
        OpenSearchColumnHandle integer = column("i", INTEGER, "integer", true);
        Map<String, ColumnHandle> assignments = Map.of("d", doubleColumn, "i", integer);

        for (String name : List.of("stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop")) {
            AggregateFunction function = function(name, DOUBLE, "d", DOUBLE);
            assertThat(MetricAggregation.from(function, assignments, "a")).as(name + " default set").isEmpty();
            assertThat(MetricAggregation.from(function, assignments, "a", MetricAggregation.SQL_FUNCTIONS)).as(name + " sql set").isPresent();
        }
        assertThat(MetricAggregation.from(function("stddev", DOUBLE, "d", DOUBLE), assignments, "a", MetricAggregation.SQL_FUNCTIONS).orElseThrow().functionName())
                .isEqualTo("stddev_samp");
        assertThat(MetricAggregation.from(function("variance", DOUBLE, "d", DOUBLE), assignments, "a", MetricAggregation.SQL_FUNCTIONS).orElseThrow().functionName())
                .isEqualTo("var_samp");
        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "i", INTEGER), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isPresent();
        assertThat(MetricAggregation.canonicalFunctionName("sum")).isEqualTo("sum");
    }

    @Test
    public void testStatisticalFunctionsRejectBigintAndReal()
    {
        Map<String, ColumnHandle> assignments = Map.of(
                "b", bigintColumn("b"),
                "r", column("r", REAL, "float", true));

        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "b", BIGINT), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isEmpty();
        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "r", REAL), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isEmpty();
    }
```

In `TestOpenSearchMetadata` add:

```java
    @Test
    public void testSqlAggregationHandleRejectsLimitTopNFilterAndAggregation()
    {
        OpenSearchTableHandle sqlAggregation = scanHandle().withSqlAggregations(List.of());

        assertThat(sqlAggregation.type()).isEqualTo(OpenSearchTableHandle.Type.SQL_AGGREGATION);
        assertThat(metadata.applyLimit(SESSION, sqlAggregation, 5)).isEmpty();
        assertThat(metadata.applyTopN(SESSION, sqlAggregation, 5, List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)), Map.of("regionkey", bigintColumn("regionkey")))).isEmpty();
        assertThat(metadata.applyFilter(SESSION, sqlAggregation, new Constraint(TupleDomain.withColumnDomains(Map.of(bigintColumn("regionkey"), Domain.singleValue(BIGINT, 1L)))))).isEmpty();
        assertThat(metadata.applyAggregation(
                SESSION,
                sqlAggregation,
                List.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                Map.of(),
                List.of(List.of()))).isEmpty();
    }
```

Create `client/TestSqlErrorMessage.java`:

```java
package io.trino.plugin.opensearch.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlErrorMessage
{
    @Test
    public void testPluginErrorBody()
    {
        assertThat(OpenSearchClient.formatSqlError(400, """
                {"error":{"reason":"Invalid SQL query","details":"can't resolve Symbol(namespace=FIELD_NAME, name=x) in type env","type":"SemanticCheckException"},"status":400}"""))
                .isEqualTo("HTTP 400: Invalid SQL query: can't resolve Symbol(namespace=FIELD_NAME, name=x) in type env");
    }

    @Test
    public void testReasonWithoutDetails()
    {
        assertThat(OpenSearchClient.formatSqlError(500, "{\"error\":{\"reason\":\"boom\"},\"status\":500}"))
                .isEqualTo("HTTP 500: boom");
    }

    @Test
    public void testUnparseableBody()
    {
        assertThat(OpenSearchClient.formatSqlError(503, "not json")).isEqualTo("HTTP 503");
        assertThat(OpenSearchClient.formatSqlError(502, "")).isEqualTo("HTTP 502");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestAggregationModel,TestOpenSearchMetadata,TestSqlErrorMessage' -Dair.check.skip-all=true -nsu`
Expected: FAIL with compilation errors (`SQL_AGGREGATION`, `withSqlAggregations`, `SQL_FUNCTIONS`, `formatSqlError` missing).

- [ ] **Step 3: Implement**

`OpenSearchTableHandle`: change the enum to `SCAN, QUERY, AGGREGATION, SQL_AGGREGATION` and add after `withAggregations`:

```java
    public OpenSearchTableHandle withSqlAggregations(List<MetricAggregation> metricAggregations)
    {
        return new OpenSearchTableHandle(Type.SQL_AGGREGATION, schema, index, constraint, regexes, query, topN, columns, ImmutableList.of(), metricAggregations);
    }
```

`OpenSearchSplitManager.getSplits`: change the first condition to

```java
        if (tableHandle.type() == OpenSearchTableHandle.Type.QUERY
                || tableHandle.type() == OpenSearchTableHandle.Type.AGGREGATION
                || tableHandle.type() == OpenSearchTableHandle.Type.SQL_AGGREGATION) {
```
(keep the existing body: one `OpenSearchSplit(tableHandle.index(), 0, Optional.empty())`; if the current condition is written with `.equals(...)`, keep that style).

`OpenSearchMetadata`:
- Change `private static final String SYNTHETIC_COLUMN_NAME_PREFIX` to `protected static final`, `private static boolean isPassthroughQuery(OpenSearchTableHandle)` to `protected static`, and `private static Optional<OpenSearchColumnHandle> aggregationOutputColumn(String, Type)` to `protected static`.
- Add next to `isPassthroughQuery`:

```java
    protected static boolean isAggregation(OpenSearchTableHandle table)
    {
        return table.type() == AGGREGATION || table.type() == SQL_AGGREGATION;
    }
```
  with `import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SQL_AGGREGATION;` next to the existing `AGGREGATION` static import.
- Replace every `handle.type() == AGGREGATION` in `applyLimit`, `applyTopN`, `applyFilter` and `applyAggregation` with `isAggregation(handle)` (find them with `grep -n "== AGGREGATION" OpenSearchMetadata.java`; leave the `withAggregations` call untouched).

`MetricAggregation` (keep all existing behaviour):

```java
    public static final String STDDEV_SAMP = "stddev_samp";
    public static final String STDDEV_POP = "stddev_pop";
    public static final String VAR_SAMP = "var_samp";
    public static final String VAR_POP = "var_pop";

    public static final Set<String> STATISTICAL_FUNCTIONS = ImmutableSet.of(STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP);
    public static final Set<String> DEFAULT_FUNCTIONS = ImmutableSet.of(COUNT, MIN, MAX, SUM, AVG);
    public static final Set<String> SQL_FUNCTIONS = ImmutableSet.<String>builder()
            .addAll(DEFAULT_FUNCTIONS)
            .addAll(STATISTICAL_FUNCTIONS)
            .build();
```
Replace the private `SUPPORTED_FUNCTIONS` field with `DEFAULT_FUNCTIONS`. Rename the existing `from` body into the four-argument overload and add:

```java
    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias)
    {
        return from(function, assignments, alias, DEFAULT_FUNCTIONS);
    }

    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias, Set<String> allowedFunctions)
    {
        // (existing body) but: String functionName = canonicalFunctionName(function.getFunctionName());
        //                      if (!allowedFunctions.contains(functionName)) { return Optional.empty(); }
    }

    public static String canonicalFunctionName(String functionName)
    {
        return switch (functionName) {
            case "stddev" -> STDDEV_SAMP;
            case "variance" -> VAR_SAMP;
            default -> functionName;
        };
    }
```
and in `isSupportedInput` change the `SUM, AVG` arm to `case SUM, AVG, STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP -> ...` (same input types as before). Add imports `com.google.common.collect.ImmutableSet`, `java.util.Set` if missing.

`PushdownColumns`: make the class `public final class PushdownColumns` and the method `public static boolean isDocValuesPushdownSupported(OpenSearchColumnHandle column)` (keep the private constructor).

`OpenSearchClient`: add (imports `org.apache.http.util.EntityUtils` and `ByteArrayEntity`, `ContentType`/`APPLICATION_JSON`, `Response`, `ResponseException` are already used by `executeQuery`; reuse the same static imports):

```java
    public String executeSql(String requestBody)
    {
        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest(
                            "POST",
                            "/_plugins/_sql",
                            ImmutableMap.of(),
                            new ByteArrayEntity(requestBody.getBytes(UTF_8), APPLICATION_JSON),
                            new BasicHeader("Accept-Encoding", "application/json"));
        }
        catch (ResponseException e) {
            String body;
            try {
                body = EntityUtils.toString(e.getResponse().getEntity());
            }
            catch (Exception ignored) {
                body = "";
            }
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL request failed: " + formatSqlError(e.getResponse().getStatusLine().getStatusCode(), body), e);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }

        try {
            return EntityUtils.toString(response.getEntity());
        }
        catch (Exception e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
        }
    }

    static String formatSqlError(int statusCode, String body)
    {
        String prefix = "HTTP " + statusCode;
        try {
            JsonNode error = JSON_MAPPER.readTree(body).path("error");
            String reason = error.path("reason").asText("");
            if (reason.isEmpty()) {
                return prefix;
            }
            String details = error.path("details").asText("");
            if (details.isEmpty()) {
                return prefix + ": " + reason;
            }
            return prefix + ": " + reason + ": " + details;
        }
        catch (IOException | RuntimeException e) {
            return prefix;
        }
    }
```
`OPENSEARCH_QUERY_FAILURE` is imported from `OpenSearchErrorCode` (add the static import if absent); `JsonNode` and `JSON_MAPPER` already exist in the file. Because `ResponseException extends IOException`, keep the `ResponseException` catch first.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchConfig,TestOpenSearchMetadata,TestOpenSearchQueryBuilder,TestAggregationModel,TestAggregationResponseReader,TestTopN,TestSqlErrorMessage' -Dair.check.skip-all=true -nsu`
Expected: PASS (all existing tests still pass).

- [ ] **Step 5: Checkpoint and commit**

Run `./mvnw -pl plugin/trino-opensearch verify -DskipTests -Dair.check.skip-enforcer=true -Dair.check.skip-airstyle=true -nsu` (BUILD SUCCESS), format the changed files, restore line-ending noise, then commit only the changed files with message `Prepare trino-opensearch for the SQL connector module`.

---

### Task 2: Module scaffold, wiring and registrations

**Files:**
- Create: `plugin/trino-opensearch-sql/pom.xml`; main classes `OpenSearchSqlPlugin`, `OpenSearchSqlConnectorFactory`, `OpenSearchSqlConnectorModule`, `OpenSearchSqlConnector`, `OpenSearchSqlConfig`, `OpenSearchSqlSessionProperties`, `OpenSearchSqlMetadata`, `OpenSearchSqlPageSourceProvider` (skeleton), `OpenSearchSqlClient` (skeleton, constructor only)
- Modify: root `pom.xml`, `core/trino-server/src/main/provisio/trino.xml`, `.github/config/labeler-config.yml`, `.github/schedule-config.yml`, `.github/workflows/ci.yml`
- Test: `TestOpenSearchSqlConfig`, `TestOpenSearchSqlPlugin`

**Interfaces:**
- Consumes: `OpenSearchConnectorModule`, `OpenSearchConnector`, `OpenSearchMetadata`, `OpenSearchPageSourceProvider`, `OpenSearchSplitManager`, `NodesSystemTable`, `OpenSearchClient`, `OpenSearchConfig` (all public in `io.trino.plugin.opensearch`).
- Produces:
  - `OpenSearchSqlConfig` with nested `enum GlobalAggregationEngine { SQL, DSL }`; `getGlobalAggregationEngine()` / `setGlobalAggregationEngine(GlobalAggregationEngine)`; property `opensearch.sql.global-aggregation-engine`.
  - `OpenSearchSqlSessionProperties` (`SessionPropertiesProvider`) with `static GlobalAggregationEngine globalAggregationEngine(ConnectorSession)`.
  - `OpenSearchSqlClient` (public class, `@Inject public OpenSearchSqlClient(OpenSearchClient client)`; `execute` added in Task 5).
  - `OpenSearchSqlMetadata extends OpenSearchMetadata` with `@Inject public OpenSearchSqlMetadata(TypeManager, OpenSearchClient, OpenSearchConfig)` (pushdown logic in Task 6).
  - `OpenSearchSqlPageSourceProvider extends OpenSearchPageSourceProvider` with `@Inject public OpenSearchSqlPageSourceProvider(OpenSearchClient, TypeManager, OpenSearchConfig, OpenSearchSqlClient)` (routing in Task 6).

- [ ] **Step 1: Write the failing tests**

`TestOpenSearchSqlConfig`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestOpenSearchSqlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenSearchSqlConfig.class)
                .setGlobalAggregationEngine(GlobalAggregationEngine.SQL));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of("opensearch.sql.global-aggregation-engine", "DSL");

        OpenSearchSqlConfig expected = new OpenSearchSqlConfig()
                .setGlobalAggregationEngine(GlobalAggregationEngine.DSL);

        assertFullMapping(properties, expected);
    }
}
```

`TestOpenSearchSqlPlugin`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenSearchSqlPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new OpenSearchSqlPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThat(factory.getName()).isEqualTo("opensearch-sql");
        // building the connector exercises the Guice wiring (base module + SQL overrides) without needing a server
        factory.create("test", ImmutableMap.of("opensearch.host", "localhost"), new TestingConnectorContext())
                .shutdown();
    }
}
```

- [ ] **Step 2: Create the module build files and verify the tests fail**

`plugin/trino-opensearch-sql/pom.xml` (starting point; complete it by running the analysis in Step 4):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.trino</groupId>
        <artifactId>trino-root</artifactId>
        <version>484-SNAPSHOT</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>

    <artifactId>trino-opensearch-sql</artifactId>
    <packaging>trino-plugin</packaging>
    <name>${project.artifactId}</name>
    <description>Trino - OpenSearch SQL connector</description>

    <properties>
        <air.compiler.fail-warnings>true</air.compiler.fail-warnings>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
        </dependency>

        <dependency>
            <groupId>com.google.inject</groupId>
            <artifactId>guice</artifactId>
        </dependency>

        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>bootstrap</artifactId>
        </dependency>

        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>configuration</artifactId>
        </dependency>

        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>json</artifactId>
        </dependency>

        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-opensearch</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-plugin-toolkit</artifactId>
        </dependency>

        <dependency>
            <groupId>jakarta.validation</groupId>
            <artifactId>jakarta.validation-api</artifactId>
        </dependency>

        <dependency>
            <groupId>org.weakref</groupId>
            <artifactId>jmxutils</artifactId>
        </dependency>

        <!-- provided -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>slice</artifactId>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-api</artifactId>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-context</artifactId>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-spi</artifactId>
            <scope>provided</scope>
        </dependency>

        <!-- test: start from the test-scope blocks of plugin/trino-opensearch/pom.xml (configuration-testing, junit-extensions, trino-main, trino-testing, trino-testing-services, assertj, junit) -->
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>configuration-testing</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-main</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-testing</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

Add `<module>plugin/trino-opensearch-sql</module>` to the root `pom.xml` directly after `<module>plugin/trino-opensearch</module>`. Run: `./mvnw -pl plugin/trino-opensearch-sql -am test -Dtest='TestOpenSearchSqlConfig,TestOpenSearchSqlPlugin' -Dair.check.skip-all=true -nsu -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL with compilation errors (`OpenSearchSqlConfig`, `OpenSearchSqlPlugin` missing).

- [ ] **Step 3: Implement the wiring classes**

`OpenSearchSqlConfig`:

```java
package io.trino.plugin.opensearch.sql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class OpenSearchSqlConfig
{
    public enum GlobalAggregationEngine
    {
        SQL,
        DSL,
    }

    private GlobalAggregationEngine globalAggregationEngine = GlobalAggregationEngine.SQL;

    @NotNull
    public GlobalAggregationEngine getGlobalAggregationEngine()
    {
        return globalAggregationEngine;
    }

    @Config("opensearch.sql.global-aggregation-engine")
    @ConfigDescription("Engine for global aggregations: SQL pushes them through the OpenSearch SQL plugin; DSL uses search aggregations and uses SQL only for statistical functions")
    public OpenSearchSqlConfig setGlobalAggregationEngine(GlobalAggregationEngine globalAggregationEngine)
    {
        this.globalAggregationEngine = globalAggregationEngine;
        return this;
    }
}
```

`OpenSearchSqlSessionProperties`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public final class OpenSearchSqlSessionProperties
        implements SessionPropertiesProvider
{
    public static final String GLOBAL_AGGREGATION_ENGINE = "global_aggregation_engine";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OpenSearchSqlSessionProperties(OpenSearchSqlConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        GLOBAL_AGGREGATION_ENGINE,
                        "Engine for global aggregations: SQL or DSL",
                        GlobalAggregationEngine.class,
                        config.getGlobalAggregationEngine(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static GlobalAggregationEngine globalAggregationEngine(ConnectorSession session)
    {
        return session.getProperty(GLOBAL_AGGREGATION_ENGINE, GlobalAggregationEngine.class);
    }
}
```

`OpenSearchSqlClient` (skeleton; `execute` is added in Task 5):

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Inject;
import io.trino.plugin.opensearch.client.OpenSearchClient;

import static java.util.Objects.requireNonNull;

public class OpenSearchSqlClient
{
    private final OpenSearchClient client;

    @Inject
    public OpenSearchSqlClient(OpenSearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }
}
```
(an unused private field makes errorprone/compiler warnings fail the build: add `OpenSearchClient client()` package-private accessor `OpenSearchClient delegate() { return client; }` in this skeleton and remove it in Task 5.)

`OpenSearchSqlMetadata`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Inject;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchMetadata;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.type.TypeManager;

public class OpenSearchSqlMetadata
        extends OpenSearchMetadata
{
    @Inject
    public OpenSearchSqlMetadata(TypeManager typeManager, OpenSearchClient client, OpenSearchConfig config)
    {
        super(typeManager, client, config);
    }
}
```

`OpenSearchSqlPageSourceProvider` (skeleton; routing in Task 6):

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Inject;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchPageSourceProvider;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class OpenSearchSqlPageSourceProvider
        extends OpenSearchPageSourceProvider
{
    private final OpenSearchSqlClient sqlClient;

    @Inject
    public OpenSearchSqlPageSourceProvider(OpenSearchClient client, TypeManager typeManager, OpenSearchConfig config, OpenSearchSqlClient sqlClient)
    {
        super(client, typeManager, config);
        this.sqlClient = requireNonNull(sqlClient, "sqlClient is null");
    }

    OpenSearchSqlClient sqlClient()
    {
        return sqlClient;
    }
}
```

`OpenSearchSqlConnector`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.opensearch.NodesSystemTable;
import io.trino.plugin.opensearch.OpenSearchConnector;
import io.trino.plugin.opensearch.OpenSearchSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Set;

public class OpenSearchSqlConnector
        extends OpenSearchConnector
{
    @Inject
    public OpenSearchSqlConnector(
            LifeCycleManager lifeCycleManager,
            OpenSearchSqlMetadata metadata,
            OpenSearchSplitManager splitManager,
            OpenSearchSqlPageSourceProvider pageSourceProvider,
            NodesSystemTable nodesSystemTable,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionPropertiesProviders)
    {
        super(lifeCycleManager, metadata, splitManager, pageSourceProvider, nodesSystemTable, connectorTableFunctions, sessionPropertiesProviders);
    }
}
```

`OpenSearchSqlConnectorModule`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.opensearch.OpenSearchConnectorModule;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class OpenSearchSqlConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new OpenSearchConnectorModule());

        configBinder(binder).bindConfig(OpenSearchSqlConfig.class);

        binder.bind(OpenSearchSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchSqlMetadata.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchSqlPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(OpenSearchSqlConnector.class).in(Scopes.SINGLETON);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(OpenSearchSqlSessionProperties.class).in(Scopes.SINGLETON);
    }
}
```

`OpenSearchSqlConnectorFactory` (same shape as `OpenSearchConnectorFactory`):

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class OpenSearchSqlConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "opensearch-sql";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new MBeanModule(),
                new MBeanServerModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.opensearch", "trino.plugin.opensearch"),
                new JsonModule(),
                new TypeDeserializerModule(),
                new OpenSearchSqlConnectorModule(),
                new ConnectorContextModule(catalogName, context));

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(OpenSearchSqlConnector.class);
    }
}
```

`OpenSearchSqlPlugin`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class OpenSearchSqlPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new OpenSearchSqlConnectorFactory());
    }
}
```

Registrations: in `core/trino-server/src/main/provisio/trino.xml`, directly after the `plugin/opensearch` artifactSet add

```xml
    <artifactSet to="plugin/opensearch-sql">
        <artifact id="${project.groupId}:trino-opensearch-sql:zip:${project.version}">
            <unpack />
        </artifact>
    </artifactSet>
```
In `.github/config/labeler-config.yml` add a second glob line under `opensearch:`: `    - any-glob-to-any-file: 'plugin/trino-opensearch-sql/**'` (keep the block's existing indentation). In `.github/schedule-config.yml` add `- { modules: plugin/trino-opensearch-sql }` after the `plugin/trino-opensearch` line. In `.github/workflows/ci.yml` add `            !:trino-opensearch-sql,` after the `!:trino-opensearch,` line.

The server artifact list also needs the module's `META-INF/services/io.trino.spi.Plugin`; the `trino-plugin` packaging generates it from the `Plugin` implementation automatically (verify with `unzip -l plugin/trino-opensearch-sql/target/*.zip` after `verify`, and compare with how `trino-opensearch` behaves).

- [ ] **Step 4: Run tests and module checks**

Run: `./mvnw -pl plugin/trino-opensearch-sql -am test -Dtest='TestOpenSearchSqlConfig,TestOpenSearchSqlPlugin' -Dair.check.skip-all=true -nsu -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (2 + 1 tests).
Then `./mvnw -pl plugin/trino-opensearch-sql verify -DskipTests -Dair.check.skip-enforcer=true -Dair.check.skip-airstyle=true -nsu`. Fix `dependency:analyze` complaints by adding the reported used-undeclared artifacts and removing the unused-declared ones (keep the provided/compile scopes consistent with `plugin/trino-opensearch/pom.xml`; add `<version>` only where the root pom does not manage it) until BUILD SUCCESS, including `sortpom`. If `TestOpenSearchSqlPlugin` fails on Guice errors, the cause is usually a duplicate or missing binding between `OpenSearchConnectorModule` and the SQL module: read the first error and fix the wiring, not the test.

- [ ] **Step 5: Format and commit**

Format only the new module and the edited registration files, restore noise, commit with message `Add the opensearch-sql connector module skeleton`.

---

### Task 3: SQL identifiers and WHERE rendering

**Files:**
- Create: `plugin/trino-opensearch-sql/src/main/java/io/trino/plugin/opensearch/sql/SqlIdentifiers.java`, `SqlWhereRenderer.java`
- Test: `.../sql/TestSqlIdentifiers.java`, `TestSqlWhereRenderer.java`

**Interfaces:**
- Produces:
  - `final class SqlIdentifiers` (package-private): `static boolean isQuotable(String name)`, `static String quote(String name)` (throws `IllegalArgumentException` when not quotable).
  - `final class SqlWhereRenderer` (package-private): `static Optional<String> render(TupleDomain<OpenSearchColumnHandle> constraint)`; `Optional.of("")` means no predicate; `Optional.empty()` means the predicate cannot be rendered exactly.

- [ ] **Step 1: Write the failing tests**

`TestSqlIdentifiers`:

```java
package io.trino.plugin.opensearch.sql;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlIdentifiers
{
    @Test
    public void testQuote()
    {
        assertThat(SqlIdentifiers.isQuotable("metric_logs_20260701")).isTrue();
        assertThat(SqlIdentifiers.quote("metric_logs_20260701")).isEqualTo("`metric_logs_20260701`");
        assertThat(SqlIdentifiers.quote("tenant-id")).isEqualTo("`tenant-id`");
        assertThat(SqlIdentifiers.quote("naïve")).isEqualTo("`naïve`");
    }

    @Test
    public void testRejected()
    {
        for (String name : new String[] {"", "a`b", "a\nb", "a\u0000b", "a\u007fb"}) {
            assertThat(SqlIdentifiers.isQuotable(name)).as(name).isFalse();
            assertThatThrownBy(() -> SqlIdentifiers.quote(name)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
```

`TestSqlWhereRenderer` (helper `column(name, type, opensearchType, decoder)` builds an `OpenSearchColumnHandle`; imports: `io.trino.plugin.opensearch.OpenSearchColumnHandle`, `io.trino.plugin.opensearch.client.IndexMetadata`, decoders `BigintDecoder`, `BooleanDecoder`, `DoubleDecoder`, `IntegerDecoder`, `RealDecoder`, `TimestampDecoder`, `VarcharDecoder`, `io.trino.spi.predicate.*`, `io.trino.spi.type.*`):

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.BooleanDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.plugin.opensearch.decoders.TimestampDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlWhereRenderer
{
    private static final OpenSearchColumnHandle AGE = new OpenSearchColumnHandle(List.of("age"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("age"), true);
    private static final OpenSearchColumnHandle ID = new OpenSearchColumnHandle(List.of("id"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("id"), true);
    private static final OpenSearchColumnHandle NAME = new OpenSearchColumnHandle(List.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("name"), true);
    private static final OpenSearchColumnHandle SCORE = new OpenSearchColumnHandle(List.of("score"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("score"), true);
    private static final OpenSearchColumnHandle RATING = new OpenSearchColumnHandle(List.of("rating"), REAL, new IndexMetadata.PrimitiveType("float"), new RealDecoder.Descriptor("rating"), true);
    private static final OpenSearchColumnHandle DELETED = new OpenSearchColumnHandle(List.of("deleted"), BOOLEAN, new IndexMetadata.PrimitiveType("boolean"), new BooleanDecoder.Descriptor("deleted"), true);
    private static final OpenSearchColumnHandle CREATED = new OpenSearchColumnHandle(List.of("createdOn"), TIMESTAMP_MILLIS, new IndexMetadata.DateTimeType(List.of()), new TimestampDecoder.Descriptor("createdOn"), true);

    private static TupleDomain<OpenSearchColumnHandle> domains(Object... columnsAndDomains)
    {
        ImmutableMap.Builder<OpenSearchColumnHandle, Domain> builder = ImmutableMap.builder();
        for (int i = 0; i < columnsAndDomains.length; i += 2) {
            builder.put((OpenSearchColumnHandle) columnsAndDomains[i], (Domain) columnsAndDomains[i + 1]);
        }
        return TupleDomain.withColumnDomains(builder.buildOrThrow());
    }

    private static Optional<String> render(Object... columnsAndDomains)
    {
        return SqlWhereRenderer.render(domains(columnsAndDomains));
    }

    @Test
    public void testNoPredicate()
    {
        assertThat(SqlWhereRenderer.render(TupleDomain.all())).hasValue("");
        assertThat(render(AGE, Domain.all(INTEGER))).hasValue("");
    }

    @Test
    public void testNoneIsNotRendered()
    {
        assertThat(SqlWhereRenderer.render(TupleDomain.none())).isEmpty();
    }

    @Test
    public void testSingleValuesRangesAndNulls()
    {
        assertThat(render(AGE, Domain.singleValue(INTEGER, 5L))).hasValue("`age` = 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 5L)), false))).hasValue("`age` > 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 5L)), false))).hasValue("`age` <= 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 10L, false)), false))).hasValue("(`age` >= 1 AND `age` < 10)");
        assertThat(render(AGE, Domain.multipleValues(INTEGER, List.of(1L, 2L, 3L)))).hasValue("(`age` = 1 OR `age` = 2 OR `age` = 3)");
        assertThat(render(AGE, Domain.onlyNull(INTEGER))).hasValue("`age` IS NULL");
        assertThat(render(AGE, Domain.notNull(INTEGER))).hasValue("`age` IS NOT NULL");
        assertThat(render(AGE, Domain.create(ValueSet.of(INTEGER, 5L), true))).hasValue("(`age` = 5 OR `age` IS NULL)");
        assertThat(render(ID, Domain.singleValue(BIGINT, -9007199254740993L))).hasValue("`id` = -9007199254740993");
    }

    @Test
    public void testColumnsAreJoinedInNameOrder()
    {
        assertThat(render(
                NAME, Domain.singleValue(VARCHAR, utf8Slice("bob")),
                AGE, Domain.singleValue(INTEGER, 5L)))
                .hasValue("`age` = 5 AND `name` = 'bob'");
    }

    @Test
    public void testStringLiterals()
    {
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("it's")))).hasValue("`name` = 'it''s'");
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("naïve 中文")))).hasValue("`name` = 'naïve 中文'");
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\\b")))).isEmpty();
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\nb")))).isEmpty();
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\u0000b")))).isEmpty();
    }

    @Test
    public void testFloatingPoint()
    {
        assertThat(render(SCORE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 0.1)), false))).hasValue("`score` > 0.1");
        // REAL values are widened to double so that they compare equal to the stored float
        assertThat(render(RATING, Domain.create(ValueSet.ofRanges(Range.greaterThan(REAL, (long) Float.floatToRawIntBits(0.1f))), false))).hasValue("`rating` > 0.10000000149011612");
        assertThat(render(SCORE, Domain.singleValue(DOUBLE, Double.NaN))).isEmpty();
        assertThat(render(SCORE, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY))).isEmpty();
        assertThat(render(SCORE, Domain.singleValue(DOUBLE, 1e300))).isEmpty();
    }

    @Test
    public void testBooleanAndTimestamp()
    {
        assertThat(render(DELETED, Domain.singleValue(BOOLEAN, true))).hasValue("`deleted` = true");
        assertThat(render(DELETED, Domain.singleValue(BOOLEAN, false))).hasValue("`deleted` = false");

        long micros = micros(Instant.parse("2026-07-15T00:00:00.123Z"));
        assertThat(render(CREATED, Domain.create(ValueSet.ofRanges(Range.greaterThan(TIMESTAMP_MILLIS, micros)), false)))
                .hasValue("`createdOn` > '2026-07-15 00:00:00.123'");
        assertThat(render(CREATED, Domain.singleValue(TIMESTAMP_MILLIS, micros(Instant.parse("1969-12-31T23:59:59.001Z")))))
                .hasValue("`createdOn` = '1969-12-31 23:59:59.001'");
        // sub-millisecond precision cannot be rendered exactly
        assertThat(render(CREATED, Domain.singleValue(TIMESTAMP_MILLIS, micros + 1))).isEmpty();
    }

    @Test
    public void testUnsupportedShapes()
    {
        OpenSearchColumnHandle nested = new OpenSearchColumnHandle(List.of("a", "b"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("a.b"), true);
        assertThat(render(nested, Domain.singleValue(INTEGER, 1L))).isEmpty();

        OpenSearchColumnHandle backtick = new OpenSearchColumnHandle(List.of("a`b"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("a`b"), true);
        assertThat(render(backtick, Domain.singleValue(INTEGER, 1L))).isEmpty();

        OpenSearchColumnHandle array = new OpenSearchColumnHandle(List.of("tags"), new ArrayType(VARCHAR), new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("tags"), true);
        assertThat(render(array, Domain.onlyNull(new ArrayType(VARCHAR)))).isEmpty();
    }

    private static long micros(Instant instant)
    {
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }
}
```
(Unused-imports check: remove `ImmutableList` if unused; `Optional` and `Instant` are used.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestSqlIdentifiers,TestSqlWhereRenderer' -Dair.check.skip-all=true -nsu`
Expected: FAIL with compilation errors (classes missing).

- [ ] **Step 3: Implement**

`SqlIdentifiers`:

```java
package io.trino.plugin.opensearch.sql;

import static com.google.common.base.Preconditions.checkArgument;

final class SqlIdentifiers
{
    private SqlIdentifiers() {}

    static boolean isQuotable(String name)
    {
        if (name.isEmpty()) {
            return false;
        }
        return name.chars().noneMatch(character -> character == '`' || character < 0x20 || character == 0x7f);
    }

    static String quote(String name)
    {
        checkArgument(isQuotable(name), "Identifier cannot be quoted: %s", name);
        return "`" + name + "`";
    }
}
```

`SqlWhereRenderer`:

```java
package io.trino.plugin.opensearch.sql;

import io.airlift.slice.Slice;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

final class SqlWhereRenderer
{
    // doubles beyond this magnitude are not rendered: very large plain decimals are parsed unreliably by the server
    private static final double MAX_RENDERED_DOUBLE = 9.0e15;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");

    private SqlWhereRenderer() {}

    /**
     * Renders the constraint as the text of a SQL predicate. An empty string means "no predicate";
     * an empty Optional means the constraint cannot be rendered exactly.
     */
    static Optional<String> render(TupleDomain<OpenSearchColumnHandle> constraint)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        List<Map.Entry<OpenSearchColumnHandle, Domain>> entries = constraint.getDomains().orElseThrow().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(OpenSearchColumnHandle::name)))
                .collect(toImmutableList());

        List<String> conjuncts = new ArrayList<>();
        for (Map.Entry<OpenSearchColumnHandle, Domain> entry : entries) {
            Optional<String> predicate = renderDomain(entry.getKey(), entry.getValue());
            if (predicate.isEmpty()) {
                return Optional.empty();
            }
            if (!predicate.get().isEmpty()) {
                conjuncts.add(predicate.get());
            }
        }
        return Optional.of(String.join(" AND ", conjuncts));
    }

    private static Optional<String> renderDomain(OpenSearchColumnHandle column, Domain domain)
    {
        if (column.path().size() != 1 || !SqlIdentifiers.isQuotable(column.name())) {
            return Optional.empty();
        }
        if (domain.isAll()) {
            return Optional.of("");
        }
        if (domain.isNone()) {
            return Optional.empty();
        }

        String name = SqlIdentifiers.quote(column.name());
        ValueSet values = domain.getValues();
        if (values.isNone()) {
            // only NULL is allowed
            return Optional.of(name + " IS NULL");
        }
        if (values.isAll()) {
            // every non-NULL value
            return Optional.of(name + " IS NOT NULL");
        }

        List<String> disjuncts = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            Optional<String> rendered = renderRange(name, column.type(), range);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            disjuncts.add(rendered.get());
        }
        if (domain.isNullAllowed()) {
            disjuncts.add(name + " IS NULL");
        }
        if (disjuncts.size() == 1) {
            return Optional.of(disjuncts.getFirst());
        }
        return Optional.of("(" + String.join(" OR ", disjuncts) + ")");
    }

    private static Optional<String> renderRange(String name, Type type, Range range)
    {
        if (range.isSingleValue()) {
            return literal(type, range.getSingleValue()).map(literal -> name + " = " + literal);
        }

        List<String> parts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            Optional<String> literal = literal(type, range.getLowBoundedValue());
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            parts.add(name + (range.isLowInclusive() ? " >= " : " > ") + literal.get());
        }
        if (!range.isHighUnbounded()) {
            Optional<String> literal = literal(type, range.getHighBoundedValue());
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            parts.add(name + (range.isHighInclusive() ? " <= " : " < ") + literal.get());
        }
        if (parts.isEmpty()) {
            return Optional.of(name + " IS NOT NULL");
        }
        if (parts.size() == 1) {
            return Optional.of(parts.getFirst());
        }
        return Optional.of("(" + String.join(" AND ", parts) + ")");
    }

    private static Optional<String> literal(Type type, Object value)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of((Boolean) value ? "true" : "false");
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            return Optional.of(Long.toString((Long) value));
        }
        if (type.equals(REAL)) {
            float floatValue = Float.intBitsToFloat((int) (long) (Long) value);
            return doubleLiteral(floatValue);
        }
        if (type.equals(DOUBLE)) {
            return doubleLiteral((Double) value);
        }
        if (type instanceof VarcharType) {
            return stringLiteral(((Slice) value).toStringUtf8());
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return timestampLiteral((Long) value);
        }
        return Optional.empty();
    }

    private static Optional<String> doubleLiteral(double value)
    {
        if (Double.isNaN(value) || Double.isInfinite(value) || Math.abs(value) >= MAX_RENDERED_DOUBLE) {
            return Optional.empty();
        }
        return Optional.of(BigDecimal.valueOf(value).toPlainString());
    }

    private static Optional<String> stringLiteral(String value)
    {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\\' || character < 0x20 || character == 0x7f) {
                return Optional.empty();
            }
        }
        return Optional.of("'" + value.replace("'", "''") + "'");
    }

    private static Optional<String> timestampLiteral(long epochMicros)
    {
        if (floorMod(epochMicros, 1000) != 0) {
            return Optional.empty();
        }
        Instant instant = Instant.ofEpochSecond(floorDiv(epochMicros, 1_000_000), floorMod(epochMicros, 1_000_000) * 1000);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        if (dateTime.getYear() < 1 || dateTime.getYear() > 9999) {
            return Optional.empty();
        }
        return Optional.of("'" + TIMESTAMP_FORMAT.format(dateTime) + "'");
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestSqlIdentifiers,TestSqlWhereRenderer' -Dair.check.skip-all=true -nsu`
Expected: PASS. If a golden string differs only in `BigDecimal` formatting (for example `0.1` vs `0.10`), fix the renderer so the expectation in the test (shortest plain decimal) holds; do not weaken the test.

- [ ] **Step 5: Format and commit**

Format the new files, restore noise, run `verify -DskipTests` for the module (BUILD SUCCESS), commit with message `Render pushed-down predicates as OpenSearch SQL`.

---

### Task 4: SQL aggregation query builder and response reader

**Files:**
- Create: `.../sql/SqlAggregationQuery.java`, `SqlAggregationQueryBuilder.java`, `SqlResult.java`, `SqlAggregationResponseReader.java`
- Test: `.../sql/TestSqlAggregationQueryBuilder.java`, `TestSqlAggregationResponseReader.java`

**Interfaces:**
- Consumes: `MetricAggregation` (Task 1), `SqlIdentifiers` (Task 3).
- Produces:
  - `record SqlResult(List<SqlColumn> schema, List<List<Object>> rows)` and `record SqlColumn(String name, String type)` (package-private, in `SqlResult.java`).
  - `record SqlAggregationQuery(String sql, List<Output> outputs, int sentinelIndex, int columnCount)` with nested `record Output(MetricAggregation aggregation, int valueIndex, OptionalInt countIndex)`.
  - `SqlAggregationQueryBuilder.build(String index, List<MetricAggregation> aggregations, String whereClause): Optional<SqlAggregationQuery>` (empty when an identifier is not quotable).
  - `SqlAggregationResponseReader.read(SqlAggregationQuery query, SqlResult result): Map<String, Object>` keyed by `MetricAggregation.alias()`; values are `Long`, `Double`, other `Number`s from the response, or `null` (a `HashMap`, nulls allowed). Failures throw `TrinoException` with `OPENSEARCH_QUERY_FAILURE`.

- [ ] **Step 1: Write the failing tests**

`TestSqlAggregationQueryBuilder` (helper builds `MetricAggregation`s over columns `version` INTEGER, `duration` DOUBLE using `OpenSearchColumnHandle` as in Task 3; alias names `_pushdown_0`...):

```java
package io.trino.plugin.opensearch.sql;

import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlAggregationQueryBuilder
{
    private static final OpenSearchColumnHandle VERSION = new OpenSearchColumnHandle(List.of("version"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("version"), true);
    private static final OpenSearchColumnHandle DURATION = new OpenSearchColumnHandle(List.of("duration"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("duration"), true);

    private static MetricAggregation aggregation(String function, io.trino.spi.type.Type outputType, OpenSearchColumnHandle column, int index)
    {
        return new MetricAggregation(function, outputType, Optional.ofNullable(column), "_pushdown_" + index);
    }

    @Test
    public void testCountStarOnly()
    {
        SqlAggregationQuery query = SqlAggregationQueryBuilder.build("metric_logs", List.of(aggregation("count", BIGINT, null, 0)), "").orElseThrow();

        assertThat(query.sql()).isEqualTo("SELECT count(*), count(*) FROM `metric_logs`");
        assertThat(query.sentinelIndex()).isEqualTo(1);
        assertThat(query.columnCount()).isEqualTo(2);
        assertThat(query.outputs()).singleElement().satisfies(output -> {
            assertThat(output.valueIndex()).isEqualTo(0);
            assertThat(output.countIndex()).isEmpty();
        });
    }

    @Test
    public void testMixedAggregationsWithCompanionCounts()
    {
        List<MetricAggregation> aggregations = List.of(
                aggregation("count", BIGINT, VERSION, 0),
                aggregation("sum", BIGINT, VERSION, 1),
                aggregation("min", INTEGER, VERSION, 2),
                aggregation("avg", DOUBLE, VERSION, 3),
                aggregation("stddev_pop", DOUBLE, DURATION, 4),
                aggregation("var_samp", DOUBLE, DURATION, 5));

        SqlAggregationQuery query = SqlAggregationQueryBuilder.build("metric_logs", aggregations, "`version` > 5").orElseThrow();

        assertThat(query.sql()).isEqualTo(
                "SELECT count(`version`), sum(`version`), count(`version`), min(`version`), avg(`version`), "
                        + "stddev_pop(`duration`), count(`duration`), var_samp(`duration`), count(`duration`), count(*) "
                        + "FROM `metric_logs` WHERE `version` > 5");
        assertThat(query.columnCount()).isEqualTo(10);
        assertThat(query.sentinelIndex()).isEqualTo(9);
        assertThat(query.outputs()).extracting(SqlAggregationQuery.Output::valueIndex).containsExactly(0, 1, 3, 4, 5, 7);
        assertThat(query.outputs()).extracting(SqlAggregationQuery.Output::countIndex)
                .containsExactly(OptionalInt.empty(), OptionalInt.of(2), OptionalInt.empty(), OptionalInt.empty(), OptionalInt.of(6), OptionalInt.of(8));
    }

    @Test
    public void testUnquotableIdentifiers()
    {
        assertThat(SqlAggregationQueryBuilder.build("bad`index", List.of(aggregation("count", BIGINT, null, 0)), "")).isEmpty();

        OpenSearchColumnHandle badColumn = new OpenSearchColumnHandle(List.of("a`b"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("a`b"), true);
        assertThat(SqlAggregationQueryBuilder.build("metric_logs", List.of(aggregation("sum", BIGINT, badColumn, 0)), "")).isEmpty();
    }
}
```
(Replace the inline `io.trino.spi.type.Type` with an import.)

`TestSqlAggregationResponseReader`:

```java
package io.trino.plugin.opensearch.sql;

import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlAggregationResponseReader
{
    private static final OpenSearchColumnHandle VERSION = new OpenSearchColumnHandle(List.of("version"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("version"), true);
    private static final OpenSearchColumnHandle DURATION = new OpenSearchColumnHandle(List.of("duration"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("duration"), true);

    private static final List<MetricAggregation> AGGREGATIONS = List.of(
            new MetricAggregation("count", BIGINT, Optional.empty(), "a0"),
            new MetricAggregation("sum", BIGINT, Optional.of(VERSION), "a1"),
            new MetricAggregation("min", INTEGER, Optional.of(VERSION), "a2"),
            new MetricAggregation("avg", DOUBLE, Optional.of(VERSION), "a3"),
            new MetricAggregation("stddev_pop", DOUBLE, Optional.of(DURATION), "a4"),
            new MetricAggregation("var_samp", DOUBLE, Optional.of(DURATION), "a5"));

    private static final SqlAggregationQuery QUERY = SqlAggregationQueryBuilder.build("metric_logs", AGGREGATIONS, "").orElseThrow();

    // columns: count(*), sum, count, min, avg, stddev_pop, count, var_samp, count, sentinel
    private static final List<String> TYPES = List.of("long", "long", "long", "integer", "double", "double", "long", "double", "long", "long");

    private static SqlResult result(List<String> types, Object... row)
    {
        List<SqlColumn> schema = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            schema.add(new SqlColumn("c" + i, types.get(i)));
        }
        return new SqlResult(schema, List.of(Arrays.asList(row)));
    }

    @Test
    public void testValues()
    {
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 10L, 55L, 10L, 1, 5.5, 2.5, 10L, 7.0, 10L, 10L));

        assertThat(values).containsEntry("a0", 10L).containsEntry("a1", 55L).containsEntry("a2", 1).containsEntry("a3", 5.5)
                .containsEntry("a4", 2.5).containsEntry("a5", 7.0);
    }

    @Test
    public void testEmptyInput()
    {
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 0L, 0L, 0L, null, null, null, 0L, null, 0L, 0L));

        Map<String, Object> expected = new HashMap<>();
        expected.put("a0", 0L);
        expected.put("a1", null);
        expected.put("a2", null);
        expected.put("a3", null);
        expected.put("a4", null);
        expected.put("a5", null);
        assertThat(values).isEqualTo(expected);
    }

    @Test
    public void testSingleRowStatistics()
    {
        // population statistics over one row are 0.0 in SQL; the plugin returns NULL, sample statistics stay NULL
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 1L, 4L, 1L, 4, 4.0, null, 1L, null, 1L, 1L));

        assertThat(values.get("a4")).isEqualTo(0.0);
        assertThat(values.get("a5")).isNull();
    }

    @Test
    public void testLegacyEngineFallbackIsDetected()
    {
        List<String> legacyTypes = new ArrayList<>(TYPES);
        legacyTypes.set(legacyTypes.size() - 1, "double");

        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, result(legacyTypes, 10L, 55L, 10L, 1, 5.5, 2.5, 10L, 7.0, 10L, 10.0)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("legacy engine")
                .hasMessageContaining("opensearch.sql.global-aggregation-engine");
    }

    @Test
    public void testCompanionCountTypeIsChecked()
    {
        List<String> badTypes = new ArrayList<>(TYPES);
        badTypes.set(2, "double");

        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, result(badTypes, 10L, 55L, 10.0, 1, 5.5, 2.5, 10L, 7.0, 10L, 10L)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("legacy engine");
    }

    @Test
    public void testShapeMismatch()
    {
        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, new SqlResult(List.of(new SqlColumn("c0", "long")), List.of(List.of(1L)))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("expected 10 columns");

        SqlResult noRows = new SqlResult(result(TYPES, 1L, 1L, 1L, 1, 1.0, 1.0, 1L, 1.0, 1L, 1L).schema(), List.of());
        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, noRows))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("expected one row");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestSqlAggregationQueryBuilder,TestSqlAggregationResponseReader' -Dair.check.skip-all=true -nsu`
Expected: FAIL (compilation errors).

- [ ] **Step 3: Implement**

`SqlResult.java`:

```java
package io.trino.plugin.opensearch.sql;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

record SqlResult(List<SqlColumn> schema, List<List<Object>> rows)
{
    SqlResult
    {
        schema = List.copyOf(requireNonNull(schema, "schema is null"));
        // rows may contain NULL cells, so they are copied with the null-tolerant java.util.List.copyOf avoided
        rows = requireNonNull(rows, "rows is null").stream()
                .map(java.util.ArrayList::new)
                .collect(toImmutableList());
    }
}

record SqlColumn(String name, String type)
{
    SqlColumn
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }
}
```
(Two package-private top-level records in one file is allowed; checkstyle may require one top-level type per file: if it complains, move `SqlColumn` to `SqlColumn.java`. Replace the fully qualified `java.util.ArrayList` with an import.)

`SqlAggregationQuery.java`:

```java
package io.trino.plugin.opensearch.sql;

import io.trino.plugin.opensearch.MetricAggregation;

import java.util.List;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

record SqlAggregationQuery(String sql, List<Output> outputs, int sentinelIndex, int columnCount)
{
    SqlAggregationQuery
    {
        requireNonNull(sql, "sql is null");
        outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
    }

    record Output(MetricAggregation aggregation, int valueIndex, OptionalInt countIndex)
    {
        Output
        {
            requireNonNull(aggregation, "aggregation is null");
            requireNonNull(countIndex, "countIndex is null");
        }
    }
}
```

`SqlAggregationQueryBuilder.java`:

```java
package io.trino.plugin.opensearch.sql;

import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.sql.SqlAggregationQuery.Output;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;

final class SqlAggregationQueryBuilder
{
    private SqlAggregationQueryBuilder() {}

    static Optional<SqlAggregationQuery> build(String index, List<MetricAggregation> aggregations, String whereClause)
    {
        if (!SqlIdentifiers.isQuotable(index)) {
            return Optional.empty();
        }

        List<String> selectItems = new ArrayList<>();
        List<Output> outputs = new ArrayList<>();
        for (MetricAggregation aggregation : aggregations) {
            if (aggregation.columnHandle().isEmpty()) {
                // count(*)
                outputs.add(new Output(aggregation, selectItems.size(), OptionalInt.empty()));
                selectItems.add("count(*)");
                continue;
            }

            OpenSearchColumnHandle column = aggregation.columnHandle().orElseThrow();
            if (column.path().size() != 1 || !SqlIdentifiers.isQuotable(column.name())) {
                return Optional.empty();
            }
            String quoted = SqlIdentifiers.quote(column.name());

            int valueIndex = selectItems.size();
            selectItems.add(aggregation.functionName() + "(" + quoted + ")");

            OptionalInt countIndex = OptionalInt.empty();
            if (aggregation.functionName().equals(SUM) || STATISTICAL_FUNCTIONS.contains(aggregation.functionName())) {
                // the plugin returns 0 for sum() over no rows, and NULL for population statistics over one row
                countIndex = OptionalInt.of(selectItems.size());
                selectItems.add("count(" + quoted + ")");
            }
            outputs.add(new Output(aggregation, valueIndex, countIndex));
        }

        // the sentinel proves the V2 engine answered: the legacy engine returns count(*) as a double
        int sentinelIndex = selectItems.size();
        selectItems.add("count(*)");

        String sql = "SELECT " + String.join(", ", selectItems) + " FROM " + SqlIdentifiers.quote(index);
        if (!whereClause.isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return Optional.of(new SqlAggregationQuery(sql, outputs, sentinelIndex, selectItems.size()));
    }
}
```

`SqlAggregationResponseReader.java`:

```java
package io.trino.plugin.opensearch.sql;

import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.sql.SqlAggregationQuery.Output;
import io.trino.spi.TrinoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.opensearch.MetricAggregation.COUNT;
import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.STDDEV_POP;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;
import static io.trino.plugin.opensearch.MetricAggregation.VAR_POP;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static java.lang.String.format;

final class SqlAggregationResponseReader
{
    private static final String COUNT_TYPE = "long";

    private SqlAggregationResponseReader() {}

    static Map<String, Object> read(SqlAggregationQuery query, SqlResult result)
    {
        if (result.rows().size() != 1) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format("OpenSearch SQL returned %s rows, expected one row for a global aggregation", result.rows().size()));
        }
        List<Object> row = result.rows().getFirst();
        if (result.schema().size() != query.columnCount() || row.size() != query.columnCount()) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format(
                    "OpenSearch SQL returned %s schema columns and %s values, expected %s columns",
                    result.schema().size(),
                    row.size(),
                    query.columnCount()));
        }

        verifyCountColumn(result, query.sentinelIndex());
        Map<String, Object> values = new HashMap<>();
        for (Output output : query.outputs()) {
            MetricAggregation aggregation = output.aggregation();
            Object value = row.get(output.valueIndex());
            String function = aggregation.functionName();

            if (aggregation.columnHandle().isEmpty() || function.equals(COUNT)) {
                verifyCountColumn(result, output.valueIndex());
                values.put(aggregation.alias(), ((Number) value).longValue());
                continue;
            }

            if (output.countIndex().isPresent()) {
                verifyCountColumn(result, output.countIndex().getAsInt());
                long count = ((Number) row.get(output.countIndex().getAsInt())).longValue();
                values.put(aggregation.alias(), companionValue(function, count, value));
                continue;
            }

            values.put(aggregation.alias(), value);
        }
        return values;
    }

    private static Object companionValue(String function, long count, Object value)
    {
        if (count == 0) {
            return null;
        }
        if (function.equals(SUM)) {
            return value;
        }
        if (STATISTICAL_FUNCTIONS.contains(function)) {
            if (count == 1) {
                // population statistics of a single value are 0, sample statistics are undefined
                return (function.equals(STDDEV_POP) || function.equals(VAR_POP)) ? 0.0 : null;
            }
            return value;
        }
        return value;
    }

    private static void verifyCountColumn(SqlResult result, int index)
    {
        String actual = result.schema().get(index).type();
        if (!actual.equals(COUNT_TYPE)) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format(
                    "OpenSearch SQL returned type '%s' for a count column, expected '%s'. The query was probably answered by the legacy engine. "
                            + "Set opensearch.sql.global-aggregation-engine=DSL to avoid the SQL path.",
                    actual,
                    COUNT_TYPE));
        }
    }
}
```
(The ternary is trivial; if checkstyle/readability objects, expand to `if`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestSqlAggregationQueryBuilder,TestSqlAggregationResponseReader' -Dair.check.skip-all=true -nsu`
Expected: PASS.

- [ ] **Step 5: Format and commit**

Format, restore noise, `verify -DskipTests` (BUILD SUCCESS), commit `Build and validate OpenSearch SQL global aggregation queries`.

---

### Task 5: SQL client and response parsing

**Files:**
- Modify: `.../sql/OpenSearchSqlClient.java` (replace the Task 2 skeleton)
- Test: `.../sql/TestOpenSearchSqlClientParsing.java`

**Interfaces:**
- Consumes: `OpenSearchClient.executeSql` (Task 1), `SqlResult`/`SqlColumn` (Task 4).
- Produces: `OpenSearchSqlClient.execute(String sql): SqlResult`; `static SqlResult parse(String body)` (package-private); throws `TrinoException` `OPENSEARCH_QUERY_FAILURE` for an `error` payload and `OPENSEARCH_INVALID_RESPONSE` for a malformed one.

- [ ] **Step 1: Write the failing test**

```java
package io.trino.plugin.opensearch.sql;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenSearchSqlClientParsing
{
    @Test
    public void testParseAggregateResponse()
    {
        SqlResult result = OpenSearchSqlClient.parse("""
                {
                  "schema": [
                    {"name": "count(*)", "type": "long"},
                    {"name": "sum(version)", "alias": "s", "type": "long"},
                    {"name": "avg(version)", "type": "double"},
                    {"name": "max(flag)", "type": "boolean"},
                    {"name": "max(name)", "type": "keyword"}
                  ],
                  "datarows": [[2150451, 5155344, 6.190872704696464, true, null]],
                  "total": 1,
                  "size": 1,
                  "status": 200
                }
                """);

        assertThat(result.schema()).containsExactly(
                new SqlColumn("count(*)", "long"),
                new SqlColumn("s", "long"),
                new SqlColumn("avg(version)", "double"),
                new SqlColumn("max(flag)", "boolean"),
                new SqlColumn("max(name)", "keyword"));
        assertThat(result.rows()).containsExactly(Arrays.asList(2150451L, 5155344L, 6.190872704696464, true, null));
    }

    @Test
    public void testLargeIntegersKeepPrecision()
    {
        SqlResult result = OpenSearchSqlClient.parse("""
                {"schema":[{"name":"x","type":"long"}],"datarows":[[9007199254740993]],"status":200}""");

        assertThat(result.rows().getFirst().getFirst()).isEqualTo(9007199254740993L);
    }

    @Test
    public void testEmptyRows()
    {
        SqlResult result = OpenSearchSqlClient.parse("""
                {"schema":[{"name":"x","type":"long"}],"datarows":[],"total":0,"size":0,"status":200}""");

        assertThat(result.rows()).isEmpty();
    }

    @Test
    public void testErrorPayload()
    {
        assertThatThrownBy(() -> OpenSearchSqlClient.parse("""
                {"error":{"reason":"Invalid SQL query","details":"boom","type":"SemanticCheckException"},"status":400}"""))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid SQL query")
                .hasMessageContaining("boom");
    }

    @Test
    public void testMalformedPayloads()
    {
        for (String body : List.of("", "not json", "{}", "{\"schema\":[]}", "{\"datarows\":[]}", "{\"schema\":{},\"datarows\":[]}")) {
            assertThatThrownBy(() -> OpenSearchSqlClient.parse(body))
                    .as(body)
                    .isInstanceOf(TrinoException.class);
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest=TestOpenSearchSqlClientParsing -Dair.check.skip-all=true -nsu`
Expected: FAIL (`parse` missing).

- [ ] **Step 3: Implement**

Replace `OpenSearchSqlClient.java`:

```java
package io.trino.plugin.opensearch.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenSearchSqlClient
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private final OpenSearchClient client;

    @Inject
    public OpenSearchSqlClient(OpenSearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    SqlResult execute(String sql)
    {
        String requestBody;
        try {
            requestBody = JSON_MAPPER.writeValueAsString(ImmutableMap.of("query", sql));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encode the OpenSearch SQL request", e);
        }
        return parse(client.executeSql(requestBody));
    }

    static SqlResult parse(String body)
    {
        JsonNode root;
        try {
            root = JSON_MAPPER.readTree(body);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned a response that is not JSON", e);
        }
        if (root == null || !root.isObject()) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned an empty or non-object response");
        }

        JsonNode error = root.get("error");
        if (error != null) {
            String reason = error.path("reason").asText("unknown error");
            String details = error.path("details").asText("");
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL error: " + reason + (details.isEmpty() ? "" : ": " + details));
        }

        JsonNode schemaNode = root.get("schema");
        JsonNode rowsNode = root.get("datarows");
        if (schemaNode == null || !schemaNode.isArray() || rowsNode == null || !rowsNode.isArray()) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL response does not contain a schema and datarows");
        }

        ImmutableList.Builder<SqlColumn> schema = ImmutableList.builder();
        for (JsonNode column : schemaNode) {
            String name = column.hasNonNull("alias") ? column.get("alias").asText() : column.path("name").asText();
            schema.add(new SqlColumn(name, column.path("type").asText()));
        }

        List<List<Object>> rows = new ArrayList<>();
        for (JsonNode rowNode : rowsNode) {
            if (!rowNode.isArray()) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned a data row that is not an array");
            }
            List<Object> row = new ArrayList<>();
            for (JsonNode cell : rowNode) {
                row.add(cell(cell));
            }
            rows.add(row);
        }
        return new SqlResult(schema.build(), rows);
    }

    private static Object cell(JsonNode node)
    {
        if (node.isNull()) {
            return null;
        }
        if (node.isIntegralNumber()) {
            return node.canConvertToLong() ? (Object) node.longValue() : node.bigIntegerValue();
        }
        if (node.isFloatingPointNumber()) {
            return node.doubleValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        return node.toString();
    }
}
```
(`ObjectMapper` import is unneeded: remove it. Drop the `delegate()` accessor from the Task 2 skeleton. A `JsonMapperProvider` mapper is configured to fail on unknown properties only for bound classes; `readTree` is unaffected.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestOpenSearchSqlClientParsing,TestOpenSearchSqlPlugin' -Dair.check.skip-all=true -nsu`
Expected: PASS.

- [ ] **Step 5: Format and commit**

Format, restore noise, `verify -DskipTests`, commit `Add the OpenSearch SQL client`.

---

### Task 6: SQL aggregation pushdown (metadata, page source, routing)

**Files:**
- Modify: `.../sql/OpenSearchSqlMetadata.java`, `OpenSearchSqlPageSourceProvider.java`
- Create: `.../sql/SqlAggregatePageSource.java`
- Test: `.../sql/TestOpenSearchSqlMetadata.java`

**Interfaces:**
- Consumes: everything from Tasks 1-5. From `trino-opensearch`: `OpenSearchSessionProperties.isAggregationPushdownEnabled(ConnectorSession)`, `OpenSearchMetadata.aggregationOutputColumn`, `isPassthroughQuery`, `isAggregation`, `SYNTHETIC_COLUMN_NAME_PREFIX`, `MetricAggregation.from(..., SQL_FUNCTIONS)`, `PushdownColumns`.
- Produces: `OpenSearchSqlMetadata.applyAggregation` (SQL path first, else `super`), `SqlAggregatePageSource(OpenSearchSqlClient, OpenSearchTableHandle, List<OpenSearchColumnHandle>)`, and `OpenSearchSqlPageSourceProvider.createPageSource` routing `SQL_AGGREGATION` handles to it.

- [ ] **Step 1: Write the failing test**

`TestOpenSearchSqlMetadata` (uses the real `OpenSearchClient` constructed without connecting, as `TestOpenSearchMetadata` in `trino-opensearch` does):

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchSessionProperties;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.AGGREGATION;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SQL_AGGREGATION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOpenSearchSqlMetadata
{
    private static final OpenSearchColumnHandle VERSION = new OpenSearchColumnHandle(List.of("version"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("version"), true);
    private static final OpenSearchColumnHandle DURATION = new OpenSearchColumnHandle(List.of("duration"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("duration"), true);
    private static final OpenSearchColumnHandle TENANT = new OpenSearchColumnHandle(List.of("tenantId"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("tenantId"), true);
    private static final OpenSearchColumnHandle KIND = new OpenSearchColumnHandle(List.of("kind"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("kind"), true);

    private OpenSearchClient client;
    private OpenSearchSqlMetadata metadata;

    @BeforeAll
    public void setUp()
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        metadata = new OpenSearchSqlMetadata(TESTING_TYPE_MANAGER, client, config);
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        client.close();
    }

    private static ConnectorSession session(GlobalAggregationEngine engine, boolean aggregationPushdownEnabled)
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig()).getSessionProperties())
                .build();
        return TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .setPropertyValues(ImmutableMap.of(
                        "aggregation_pushdown_enabled", aggregationPushdownEnabled,
                        "global_aggregation_engine", engine))
                .build();
    }

    private static OpenSearchTableHandle scanHandle()
    {
        return new OpenSearchTableHandle(SCAN, "default", "metric_logs", Optional.empty());
    }

    private static OpenSearchTableHandle scanHandle(TupleDomain<ColumnHandle> constraint, Map<String, String> regexes)
    {
        return new OpenSearchTableHandle(SCAN, "default", "metric_logs", constraint, regexes, Optional.empty(), Optional.empty(), Set.of(), List.of(), List.of());
    }

    private static AggregateFunction function(String name, io.trino.spi.type.Type outputType, String variable, io.trino.spi.type.Type inputType)
    {
        return new AggregateFunction(name, outputType, List.of(new Variable(variable, inputType)), List.of(), false, Optional.empty());
    }

    private static AggregateFunction countStar()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, OpenSearchTableHandle handle, List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets)
    {
        return metadata.applyAggregation(session, handle, aggregates, Map.of("version", VERSION, "duration", DURATION, "tenantId", TENANT, "kind", KIND), groupingSets);
    }

    @Test
    public void testGlobalAggregatesUseSql()
    {
        AggregationApplicationResult<ConnectorTableHandle> result = apply(
                session(GlobalAggregationEngine.SQL, true),
                scanHandle(),
                List.of(countStar(), function("sum", BIGINT, "version", INTEGER), function("max", INTEGER, "version", INTEGER)),
                List.of(List.of()))
                .orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.type()).isEqualTo(SQL_AGGREGATION);
        assertThat(handle.termAggregations()).isEmpty();
        assertThat(handle.metricAggregations()).extracting(MetricAggregation::functionName).containsExactly("count", "sum", "max");
        assertThat(handle.metricAggregations()).extracting(MetricAggregation::alias).containsExactly("_pushdown_0", "_pushdown_1", "_pushdown_2");
        assertThat(result.getAssignments()).extracting(assignment -> assignment.getVariable() + ":" + assignment.getType())
                .containsExactly("_pushdown_0:bigint", "_pushdown_1:bigint", "_pushdown_2:integer");
    }

    @Test
    public void testStatisticalFunctionsUseSqlAndAreCanonicalised()
    {
        AggregationApplicationResult<ConnectorTableHandle> result = apply(
                session(GlobalAggregationEngine.SQL, true),
                scanHandle(),
                List.of(function("stddev", DOUBLE, "duration", DOUBLE), function("variance", DOUBLE, "duration", DOUBLE), function("var_pop", DOUBLE, "duration", DOUBLE)),
                List.of(List.of()))
                .orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.type()).isEqualTo(SQL_AGGREGATION);
        assertThat(handle.metricAggregations()).extracting(MetricAggregation::functionName).containsExactly("stddev_samp", "var_samp", "var_pop");
    }

    @Test
    public void testDslEngineKeepsDslForTheBaseSetButSqlForStatistics()
    {
        ConnectorSession dsl = session(GlobalAggregationEngine.DSL, true);

        OpenSearchTableHandle sum = (OpenSearchTableHandle) apply(dsl, scanHandle(), List.of(function("sum", BIGINT, "version", INTEGER)), List.of(List.of())).orElseThrow().getHandle();
        assertThat(sum.type()).isEqualTo(AGGREGATION);

        OpenSearchTableHandle stddev = (OpenSearchTableHandle) apply(dsl, scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of())).orElseThrow().getHandle();
        assertThat(stddev.type()).isEqualTo(SQL_AGGREGATION);
    }

    @Test
    public void testGroupByStaysOnTheDslPath()
    {
        OpenSearchTableHandle grouped = (OpenSearchTableHandle) apply(
                session(GlobalAggregationEngine.SQL, true),
                scanHandle(),
                List.of(function("sum", BIGINT, "version", INTEGER)),
                List.of(List.of(KIND)))
                .orElseThrow()
                .getHandle();
        assertThat(grouped.type()).isEqualTo(AGGREGATION);
        assertThat(grouped.termAggregations()).hasSize(1);

        // statistical functions cannot be grouped at all
        assertThat(apply(session(GlobalAggregationEngine.SQL, true), scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of(KIND)))).isEmpty();
    }

    @Test
    public void testRenderablePredicateIsKeptUnrenderableFallsBack()
    {
        ConnectorSession session = session(GlobalAggregationEngine.SQL, true);

        TupleDomain<ColumnHandle> renderable = TupleDomain.withColumnDomains(Map.of(KIND, Domain.singleValue(VARCHAR, utf8Slice("a"))));
        assertThat(((OpenSearchTableHandle) apply(session, scanHandle(renderable, Map.of()), List.of(countStar()), List.of(List.of())).orElseThrow().getHandle()).type())
                .isEqualTo(SQL_AGGREGATION);

        TupleDomain<ColumnHandle> unrenderable = TupleDomain.withColumnDomains(Map.of(KIND, Domain.singleValue(VARCHAR, utf8Slice("a\\b"))));
        assertThat(((OpenSearchTableHandle) apply(session, scanHandle(unrenderable, Map.of()), List.of(countStar()), List.of(List.of())).orElseThrow().getHandle()).type())
                .isEqualTo(AGGREGATION);

        // a LIKE-derived regexp cannot be turned back into SQL
        assertThat(((OpenSearchTableHandle) apply(session, scanHandle(TupleDomain.all(), Map.of("kind", "a.*")), List.of(countStar()), List.of(List.of())).orElseThrow().getHandle()).type())
                .isEqualTo(AGGREGATION);
    }

    @Test
    public void testRejections()
    {
        ConnectorSession session = session(GlobalAggregationEngine.SQL, true);

        // BIGINT inputs are never pushed, by either engine
        assertThat(apply(session, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT)), List.of(List.of()))).isEmpty();
        // count(DISTINCT) is approximate in OpenSearch
        assertThat(apply(session, scanHandle(), List.of(new AggregateFunction("count", BIGINT, List.of(new Variable("version", INTEGER)), List.of(), true, Optional.empty())), List.of(List.of()))).isEmpty();
        // pushdown disabled
        assertThat(apply(session(GlobalAggregationEngine.SQL, false), scanHandle(), List.of(countStar()), List.of(List.of()))).isEmpty();
        // existing TopN
        assertThat(apply(session, scanHandle().withTopN(io.trino.plugin.opensearch.TopN.fromLimit(5)), List.of(countStar()), List.of(List.of()))).isEmpty();
    }
}
```
(Replace inline `io.trino.spi.type.Type` and `io.trino.plugin.opensearch.TopN` with imports.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest=TestOpenSearchSqlMetadata -Dair.check.skip-all=true -nsu`
Expected: FAIL (the skeleton metadata has no SQL path, so `SQL_AGGREGATION` assertions fail).

- [ ] **Step 3: Implement**

`OpenSearchSqlMetadata` (replace the skeleton):

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchMetadata;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.MetricAggregation.SQL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.OpenSearchSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.plugin.opensearch.sql.OpenSearchSqlSessionProperties.globalAggregationEngine;

public class OpenSearchSqlMetadata
        extends OpenSearchMetadata
{
    @Inject
    public OpenSearchSqlMetadata(TypeManager typeManager, OpenSearchClient client, OpenSearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> sqlResult = applySqlAggregation(session, (OpenSearchTableHandle) table, aggregates, assignments, groupingSets);
        if (sqlResult.isPresent()) {
            return sqlResult;
        }
        return super.applyAggregation(session, table, aggregates, assignments, groupingSets);
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> applySqlAggregation(
            ConnectorSession session,
            OpenSearchTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }
        if (handle.type() != SCAN || handle.topN().isPresent() || handle.query().isPresent() || !handle.regexes().isEmpty()) {
            return Optional.empty();
        }
        // only global aggregates: the SQL plugin silently truncates GROUP BY results at 1,000 groups
        if (groupingSets.size() != 1 || !groupingSets.getFirst().isEmpty() || aggregates.isEmpty()) {
            return Optional.empty();
        }

        boolean hasStatisticalFunction = aggregates.stream()
                .anyMatch(aggregate -> STATISTICAL_FUNCTIONS.contains(MetricAggregation.canonicalFunctionName(aggregate.getFunctionName())));
        if (globalAggregationEngine(session) == GlobalAggregationEngine.DSL && !hasStatisticalFunction) {
            return Optional.empty();
        }

        Optional<String> whereClause = SqlWhereRenderer.render(handle.constraint().transformKeys(OpenSearchColumnHandle.class::cast));
        if (whereClause.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<MetricAggregation> metricAggregations = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index);
            String name = SYNTHETIC_COLUMN_NAME_PREFIX + index;

            Optional<MetricAggregation> metricAggregation = MetricAggregation.from(function, assignments, name, SQL_FUNCTIONS);
            Optional<OpenSearchColumnHandle> outputColumn = aggregationOutputColumn(name, function.getOutputType());
            if (metricAggregation.isEmpty() || outputColumn.isEmpty()) {
                return Optional.empty();
            }
            metricAggregations.add(metricAggregation.get());
            projections.add(new Variable(name, function.getOutputType()));
            resultAssignments.add(new Assignment(name, outputColumn.get(), function.getOutputType()));
        }

        List<MetricAggregation> aggregations = metricAggregations.build();
        // the final check proves that the whole statement can be generated
        if (SqlAggregationQueryBuilder.build(handle.index(), aggregations, whereClause.get()).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new AggregationApplicationResult<>(
                handle.withSqlAggregations(aggregations),
                projections.build(),
                resultAssignments.build(),
                ImmutableMap.of(),
                false));
    }
}
```

`SqlAggregatePageSource`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

class SqlAggregatePageSource
        implements ConnectorPageSource
{
    private final OpenSearchSqlClient client;
    private final OpenSearchTableHandle table;
    private final List<OpenSearchColumnHandle> columns;
    private final List<String> columnNames;
    private final List<Decoder> decoders;

    private boolean finished;
    private long readTimeNanos;

    SqlAggregatePageSource(OpenSearchSqlClient client, OpenSearchTableHandle table, List<OpenSearchColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnNames = this.columns.stream()
                .map(OpenSearchColumnHandle::name)
                .collect(toImmutableList());
        this.decoders = this.columns.stream()
                .map(column -> column.decoderDescriptor().createDecoder())
                .collect(toImmutableList());
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
        finished = true;

        String whereClause = SqlWhereRenderer.render(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast))
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "The pushed-down predicate can no longer be rendered as SQL"));
        SqlAggregationQuery query = SqlAggregationQueryBuilder.build(table.index(), table.metricAggregations(), whereClause)
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "The pushed-down aggregation can no longer be rendered as SQL"));

        long start = System.nanoTime();
        SqlResult result = client.execute(query.sql());
        readTimeNanos += System.nanoTime() - start;

        Map<String, Object> values = SqlAggregationResponseReader.read(query, result);
        if (columns.isEmpty()) {
            // a global aggregation always produces exactly one row
            return SourcePage.create(1);
        }

        Block[] blocks = new Block[columns.size()];
        for (int index = 0; index < blocks.length; index++) {
            BlockBuilder builder = columns.get(index).type().createBlockBuilder(null, 1);
            Object value = values.get(columnNames.get(index));
            // decoders of synthetic aggregation columns never read from the search hit
            decoders.get(index).decode(null, () -> value, builder);
            blocks[index] = builder.build();
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public void close() {}
}
```
(Remove the unused `Optional` import. If a decoder dereferences the hit and throws on `null`, the cause is a non-synthetic column in `columns`: the SQL handle's columns are always `_pushdown_<i>` columns.)

`OpenSearchSqlPageSourceProvider` (replace the skeleton):

```java
package io.trino.plugin.opensearch.sql;

import com.google.inject.Inject;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchPageSourceProvider;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SQL_AGGREGATION;
import static java.util.Objects.requireNonNull;

public class OpenSearchSqlPageSourceProvider
        extends OpenSearchPageSourceProvider
{
    private final OpenSearchSqlClient sqlClient;

    @Inject
    public OpenSearchSqlPageSourceProvider(OpenSearchClient client, TypeManager typeManager, OpenSearchConfig config, OpenSearchSqlClient sqlClient)
    {
        super(client, typeManager, config);
        this.sqlClient = requireNonNull(sqlClient, "sqlClient is null");
    }

    @Override
    @SuppressWarnings("deprecation") // the base class implements the deprecated overload as well
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (table instanceof OpenSearchTableHandle handle && handle.type() == SQL_AGGREGATION) {
            return new SqlAggregatePageSource(
                    sqlClient,
                    handle,
                    columns.stream()
                            .map(OpenSearchColumnHandle.class::cast)
                            .collect(toImmutableList()));
        }
        return super.createPageSource(transaction, session, split, table, tableCredentials, columns, dynamicFilter);
    }
}
```

- [ ] **Step 4: Run tests and module checks**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest='TestOpenSearchSqlMetadata,TestOpenSearchSqlPlugin,TestSqlWhereRenderer,TestSqlAggregationQueryBuilder,TestSqlAggregationResponseReader,TestOpenSearchSqlClientParsing,TestOpenSearchSqlConfig,TestSqlIdentifiers' -Dair.check.skip-all=true -nsu`
Expected: PASS (all module unit tests).
Then `./mvnw -pl plugin/trino-opensearch-sql verify -DskipTests -Dair.check.skip-enforcer=true -Dair.check.skip-airstyle=true -nsu` (BUILD SUCCESS).

- [ ] **Step 5: Format and commit**

Format, restore noise, commit `Push global aggregates through the OpenSearch SQL plugin`.

---

### Task 7: Integration tests, docs, and final verification

**Files:**
- Create (test): `plugin/trino-opensearch-sql/src/test/java/io/trino/plugin/opensearch/sql/OpenSearchServer.java`, `OpenSearchSqlQueryRunner.java`, `TestOpenSearchSqlConnector.java`
- Create (docs): `docs/src/main/sphinx/connector/opensearch-sql.md`
- Modify: `docs/src/main/sphinx/connector.md`, `plugin/trino-opensearch-sql/pom.xml` (test dependencies)

**Interfaces:**
- Consumes: the whole module.

- [ ] **Step 1: Test support classes**

`OpenSearchServer` — copy `plugin/trino-opensearch/src/test/java/io/trino/plugin/opensearch/OpenSearchServer.java` into the new test package (change only the package line, and make the constant `OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.19.4"`). Add the test dependencies that file needs (`org.opensearch:opensearch-testcontainers`, `org.testcontainers:testcontainers`, `com.google.guava:guava` is already compile) by copying their blocks and versions from `plugin/trino-opensearch/pom.xml` with `<scope>test</scope>`.

`OpenSearchSqlQueryRunner`:

```java
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OpenSearchSqlQueryRunner
{
    private OpenSearchSqlQueryRunner() {}

    public static DistributedQueryRunner create(HostAndPort address, Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_sql")
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            Map<String, String> properties = new HashMap<>(ImmutableMap.<String, String>builder()
                    .put("opensearch.host", address.getHost())
                    .put("opensearch.port", Integer.toString(address.getPort()))
                    // node discovery relies on the publish address, which is wrong behind a Docker port mapping
                    .put("opensearch.ignore-publish-address", "true")
                    .put("opensearch.default-schema-name", "default")
                    .put("opensearch.request-timeout", "2m")
                    .buildOrThrow());
            properties.putAll(extraProperties);

            queryRunner.installPlugin(new OpenSearchSqlPlugin());
            queryRunner.createCatalog("opensearch_sql", "opensearch-sql", properties);
            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }
}
```
Add `io.trino:trino-testing` / `trino-main` test-jar dependencies that `DistributedQueryRunner` needs, using the same blocks as `plugin/trino-opensearch/pom.xml` (it declares both the main jar and `<type>test-jar</type>` of `trino-main` for tests; copy both).

- [ ] **Step 2: Write the integration tests**

`TestOpenSearchSqlConnector` (extends `AbstractTestQueryFramework`; requires Docker). Use the low-level REST client from `RestHighLevelClient` exactly as `RestClientUtils.createClient(address)` does in `trino-opensearch` tests: copy that small helper (`RestClientUtils`) into the new test package, adapting only the package. Helpers: `createIndex(name, mappingJson)`, `index(name, Map<String,Object>)` (PUT `/{index}/_doc/{id}?refresh`), `deleteIndex(name)`, copied from `BaseOpenSearchConnectorTest`. Assertion helper `assertPushedDown(sql)`: `computeActual("EXPLAIN " + sql).getOnlyValue().toString()` must not contain the text `Aggregate` and must contain `TableScan`.

Tests (each creates its own uniquely named index with `randomNameSuffix()` and deletes it in `finally`; field mapping: `g` keyword, `i` integer, `d` double, `l` long, `f` float, `b` boolean, `ts` date):

```java
    @Test
    public void testGlobalAggregatesAreAnsweredBySql()
    {
        // docs: (g=a,i=1,d=1.0), (g=a,i=3,d=2.0), (g=b,i=10,d=4.0)
        String sql = "SELECT count(*), count(i), sum(i), min(i), max(i), avg(i), sum(d), min(d), max(d) FROM " + table;
        assertThat(query(sql))
                .matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '14', INTEGER '1', INTEGER '10', DOUBLE '4.666666666666667', DOUBLE '7.0', DOUBLE '1.0', DOUBLE '4.0')")
                .isFullyPushedDown();
    }

    @Test
    public void testStatisticalFunctions()
    {
        // same three documents; population/sample statistics of d = [1.0, 2.0, 4.0]
        String sql = "SELECT stddev(d), stddev_pop(d), variance(d), var_pop(d) FROM " + table;
        assertPushedDown(sql);
        MaterializedRow row = getOnlyElement(computeActual(sql).getMaterializedRows());
        assertThat((Double) row.getField(0)).isCloseTo(1.5275252316519468, within(1e-9));
        assertThat((Double) row.getField(1)).isCloseTo(1.247219128924647, within(1e-9));
        assertThat((Double) row.getField(2)).isCloseTo(2.3333333333333335, within(1e-9));
        assertThat((Double) row.getField(3)).isCloseTo(1.5555555555555556, within(1e-9));
    }

    @Test
    public void testEmptyAndSingleRowSemantics()
    {
        // empty input: count 0, everything else NULL (the plugin returns 0 for sum)
        assertThat(query("SELECT count(*), count(i), sum(i), avg(i), min(i), max(i), stddev(d), var_pop(d) FROM " + table + " WHERE g = 'no_such_value'"))
                .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
        assertPushedDown("SELECT sum(i), stddev(d) FROM " + table + " WHERE g = 'no_such_value'");

        // one row: population statistics are 0.0, sample statistics NULL
        assertThat(query("SELECT stddev_pop(d), var_pop(d), stddev(d), variance(d) FROM " + table + " WHERE g = 'b'"))
                .matches("VALUES (DOUBLE '0.0', DOUBLE '0.0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
    }

    @Test
    public void testPredicatesAreRenderedForEachType()
    {
        // assert counts for: keyword equality and IN, integer range, long range with a negative bound, double range,
        // float range (a float value widened to double), boolean, date range with milliseconds, IS NULL / IS NOT NULL,
        // a string containing a single quote, and an OR of ranges. Each is `SELECT count(*), sum(i) FROM t WHERE ...`
        // and must be pushed down (assertPushedDown) and equal the count computed by Trino itself with pushdown
        // disabled: Session.builder(getSession()).setCatalogSessionProperty("opensearch_sql", "aggregation_pushdown_enabled", "false").
    }

    @Test
    public void testGroupByStaysOnTheDslPathAndIsNotTruncated()
    {
        // 2,500 documents with distinct keyword values in field g
        assertThat(computeActual("SELECT g, count(*) FROM " + table + " GROUP BY g").getRowCount()).isEqualTo(2500);
        assertQuery("SELECT count(*) FROM (SELECT g FROM " + table + " GROUP BY g)", "VALUES 2500");
    }

    @Test
    public void testBigintAggregatesAndDistinctStayInTrino()
    {
        assertThat(query("SELECT sum(l), min(l), max(l), avg(l) FROM " + table)).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT count(DISTINCT g) FROM " + table)).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testEngineSessionProperty()
    {
        Session dsl = Session.builder(getSession()).setCatalogSessionProperty("opensearch_sql", "global_aggregation_engine", "DSL").build();
        // base set stays pushed (DSL aggregation), statistical functions still go through SQL
        assertThat(query(dsl, "SELECT count(*), sum(i) FROM " + table)).isFullyPushedDown();
        assertPushedDown(dsl, "SELECT stddev(d) FROM " + table);
    }
```
The comment-only body of `testPredicatesAreRenderedForEachType` must be replaced by real code: write one `assertPredicate(String where)` helper that runs `SELECT count(*), sum(i) FROM <table> WHERE <where>` with the default session and with pushdown disabled (`aggregation_pushdown_enabled=false`), asserts both results are equal via `assertThat(pushed).isEqualTo(unpushed)` on the `MaterializedResult`s' rows, and calls `assertPushedDown` for the default session; then call it for each predicate kind listed in the comment, using a dedicated index whose documents cover NULL/missing values, negative longs, floats such as `0.1`, booleans, dates with millisecond parts and a keyword value containing `'`. The class needs `@TestInstance(PER_CLASS)`, one `OpenSearchServer` started in `createQueryRunner()` and closed in `@AfterAll`, and a fresh index per test. Use the exact assertions above; do not weaken a failing one: a failure here is a finding about the SQL plugin or the renderer.

- [ ] **Step 3: Run the integration tests (requires Docker)**

Run: `./mvnw -pl plugin/trino-opensearch-sql test -Dtest=TestOpenSearchSqlConnector -Dair.check.skip-all=true -nsu`
Expected: PASS. If Docker is unavailable, confirm the class compiles (`test-compile`) and report the tests as NOT RUN. For any failing test, capture the generated SQL (enable `LOG` of the statement at debug level in `SqlAggregatePageSource` temporarily) and the plugin response, decide whether the renderer, the guard or the expectation is wrong, and fix the cause; report anything that contradicts the spec.

- [ ] **Step 4: Docs**

Create `docs/src/main/sphinx/connector/opensearch-sql.md` covering: purpose and requirements (OpenSearch 2.19+ with the SQL plugin, `plugins.sql.enabled` not disabled), configuration (all `opensearch.*` properties apply; `opensearch.sql.global-aggregation-engine`; session property `global_aggregation_engine`), the list-table of pushed aggregates with input types, what stays in Trino (grouping, `BIGINT`, `DISTINCT`, expressions), the result guards (empty `sum` → NULL, `stddev_pop`/`var_pop` of one row = 0.0, legacy-engine fallback detection error and how to avoid it with `DSL`), and a limitations section (global aggregates only; SQL cursors unused; the SQL plugin silently truncates `GROUP BY` at 1,000 groups, which is why grouping uses the search API). Match the heading levels, `{list-table}` and label style of `docs/src/main/sphinx/connector/opensearch.md`. Add `OpenSearch SQL      <connector/opensearch-sql>` directly after the `OpenSearch      <connector/opensearch>` line in `docs/src/main/sphinx/connector.md` (keep the column alignment of that list).

- [ ] **Step 5: Full verification and report**

From `C:\github\trino`:
1. `./mvnw -pl plugin/trino-opensearch,plugin/trino-opensearch-sql verify -DskipTests -Dair.check.skip-enforcer=true -Dair.check.skip-airstyle=true -nsu` → BUILD SUCCESS.
2. `./mvnw -pl plugin/trino-opensearch test -Dtest='TestOpenSearchConfig,TestOpenSearchMetadata,TestOpenSearchQueryBuilder,TestAggregationModel,TestAggregationResponseReader,TestTopN,TestSqlErrorMessage' -Dair.check.skip-all=true -nsu` → PASS.
3. `./mvnw -pl plugin/trino-opensearch-sql test -Dair.check.skip-all=true -nsu -Dtest='!TestOpenSearchSqlConnector'` → PASS.
4. With Docker: `TestOpenSearchSqlConnector` (Step 3) and the DSL integration tests `TestOpenSearchLatestConnectorTest` in `trino-opensearch` (the Task 1 handle changes must not regress them).

Report what ran, what did not (Docker), every deviation from this plan, and any finding about the SQL plugin that contradicts the spec. Commit the integration tests and docs with message `Add OpenSearch SQL connector integration tests and documentation`.

---

## Self-Review

**Spec coverage:** module and wiring (Task 2); additive trino-opensearch changes — `SQL_AGGREGATION`, split manager, `MetricAggregation` overload, `PushdownColumns`, SQL transport hook (Task 1); `opensearch.sql.global-aggregation-engine` and session property (Task 2); `applyAggregation` rules and DSL fallback (Task 6); WHERE rendering and literal rules (Task 3); SQL generation with companion counts and sentinel (Task 4); schema/type validation and legacy-fallback error naming the DSL setting (Task 4); error mapping and no-`filter` rule (Tasks 1, 5); `GROUP BY` kept on DSL with a >1,000-group test (Tasks 6, 7); `BIGINT`/`DISTINCT`/nested/builtin exclusions (Tasks 1, 4, 6, 7); unit and integration tests, docs (Tasks 1-7). One tightening versus the spec: `regexes`-derived `LIKE` predicates make the SQL path ineligible (falls back to DSL) instead of being rendered, because the stored Lucene regexp cannot be turned back into SQL `LIKE`; the spec said "regex-backed `LIKE`" is supported. Also, statistical functions accept `INTEGER` columns in the model but Trino passes them as `DOUBLE` after an implicit cast, so in practice only `DOUBLE` column arguments reach the SQL path.

**Placeholder scan:** Task 7's `testPredicatesAreRenderedForEachType` is specified by a helper contract and an explicit list of predicate kinds rather than literal code; every other step contains its code. The `pom.xml` dependency list is a starting point completed by the stated `dependency:analyze` loop.

**Type consistency:** `MetricAggregation.from(.., Set)`, `STATISTICAL_FUNCTIONS`, `SQL_FUNCTIONS`, `canonicalFunctionName`, `withSqlAggregations`, `isAggregation`, `aggregationOutputColumn`, `SYNTHETIC_COLUMN_NAME_PREFIX` (Task 1) are used with the same names in Tasks 4 and 6; `SqlWhereRenderer.render`, `SqlAggregationQueryBuilder.build`, `SqlAggregationQuery.Output`, `SqlAggregationResponseReader.read`, `SqlResult`/`SqlColumn`, `OpenSearchSqlClient.execute/parse` (Tasks 3-5) match their uses in `SqlAggregatePageSource` and `OpenSearchSqlMetadata` (Task 6); `OpenSearchSqlConfig.GlobalAggregationEngine` and `OpenSearchSqlSessionProperties.globalAggregationEngine` (Task 2) match Task 6.
