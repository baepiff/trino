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
package io.trino.plugin.opensearch.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchSessionProperties;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.TopN;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
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
import io.trino.spi.type.Type;
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
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.QUERY;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SQL_AGGREGATION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOpenSearchSqlMetadata
{
    private static final OpenSearchColumnHandle VERSION = new OpenSearchColumnHandle(List.of("version"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("version"), true);
    private static final OpenSearchColumnHandle DURATION = new OpenSearchColumnHandle(List.of("duration"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("duration"), true);
    private static final OpenSearchColumnHandle RATIO = new OpenSearchColumnHandle(List.of("ratio"), REAL, new IndexMetadata.PrimitiveType("float"), new RealDecoder.Descriptor("ratio"), true);
    private static final OpenSearchColumnHandle TENANT = new OpenSearchColumnHandle(List.of("tenantId"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("tenantId"), true);
    private static final OpenSearchColumnHandle KIND = new OpenSearchColumnHandle(List.of("kind"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("kind"), true);
    private static final OpenSearchColumnHandle NESTED = new OpenSearchColumnHandle(List.of("owner", "id"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("owner.id"), true);
    private static final OpenSearchColumnHandle SCORE = new OpenSearchColumnHandle(List.of("_score"), REAL, new IndexMetadata.PrimitiveType("float"), new RealDecoder.Descriptor("_score"), false);

    private static final Map<String, ColumnHandle> ASSIGNMENTS = ImmutableMap.<String, ColumnHandle>builder()
            .put("version", VERSION)
            .put("duration", DURATION)
            .put("ratio", RATIO)
            .put("tenantId", TENANT)
            .put("kind", KIND)
            .put("owner.id", NESTED)
            .put("_score", SCORE)
            .buildOrThrow();

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
        return session(engine, aggregationPushdownEnabled, true);
    }

    private static ConnectorSession session(GlobalAggregationEngine engine, boolean aggregationPushdownEnabled, boolean statisticalPushdownEnabled)
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig()).getSessionProperties())
                .build();
        return TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .setPropertyValues(ImmutableMap.of(
                        "aggregation_pushdown_enabled", aggregationPushdownEnabled,
                        "global_aggregation_engine", engine.name(),
                        "statistical_pushdown_enabled", statisticalPushdownEnabled))
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

    private static AggregateFunction function(String name, Type outputType, String variable, Type inputType)
    {
        return new AggregateFunction(name, outputType, List.of(new Variable(variable, inputType)), List.of(), false, Optional.empty());
    }

    private static AggregateFunction countStar()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, OpenSearchTableHandle handle, List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets)
    {
        return metadata.applyAggregation(session, handle, aggregates, ASSIGNMENTS, groupingSets);
    }

    private OpenSearchTableHandle.Type applyGlobal(ConnectorSession session, OpenSearchTableHandle handle, List<AggregateFunction> aggregates)
    {
        return ((OpenSearchTableHandle) apply(session, handle, aggregates, List.of(List.of())).orElseThrow().getHandle()).type();
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
    public void testStatisticalFunctionsAreNotPushedByDefault()
    {
        // the default session property value comes from the default config
        ConnectorSession defaults = TestingConnectorSession.builder()
                .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                        .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                        .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig()).getSessionProperties())
                        .build())
                .build();
        assertThat(apply(defaults, scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of()))).isEmpty();

        ConnectorSession off = session(GlobalAggregationEngine.SQL, true, false);
        for (String name : List.of("stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop")) {
            assertThat(apply(off, scanHandle(), List.of(function(name, DOUBLE, "duration", DOUBLE)), List.of(List.of()))).as(name).isEmpty();
        }
        // one statistical function keeps the whole aggregation in Trino
        assertThat(apply(off, scanHandle(), List.of(countStar(), function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of()))).isEmpty();
        // the DSL engine does not bring them back
        assertThat(apply(session(GlobalAggregationEngine.DSL, true, false), scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of()))).isEmpty();
        // other aggregates are still pushed
        assertThat(applyGlobal(off, scanHandle(), List.of(countStar(), function("sum", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);
    }

    @Test
    public void testStatisticalPushdownSessionPropertyOverridesConfig()
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig().setStatisticalPushdownEnabled(true)).getSessionProperties())
                .build();
        ConnectorSession configEnabled = TestingConnectorSession.builder().setPropertyMetadata(properties).build();
        assertThat(applyGlobal(configEnabled, scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);

        ConnectorSession sessionDisabled = TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .setPropertyValues(ImmutableMap.of("statistical_pushdown_enabled", false))
                .build();
        assertThat(apply(sessionDisabled, scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of()))).isEmpty();

        ConnectorSession sessionEnabled = session(GlobalAggregationEngine.SQL, true, true);
        assertThat(applyGlobal(sessionEnabled, scanHandle(), List.of(countStar(), function("var_pop", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);
    }

    @Test
    public void testDslEngineKeepsDslForTheBaseSetButSqlForStatistics()
    {
        ConnectorSession dsl = session(GlobalAggregationEngine.DSL, true);

        assertThat(applyGlobal(dsl, scanHandle(), List.of(function("sum", BIGINT, "version", INTEGER)))).isEqualTo(AGGREGATION);
        assertThat(applyGlobal(dsl, scanHandle(), List.of(function("stddev", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);
        // one statistical function moves the whole statement to SQL
        assertThat(applyGlobal(dsl, scanHandle(), List.of(countStar(), function("stddev", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);
    }

    private static ConnectorSession bigintSession(GlobalAggregationEngine engine, boolean bigintPushdownEnabled)
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig()).getSessionProperties())
                .build();
        return TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .setPropertyValues(ImmutableMap.of(
                        "global_aggregation_engine", engine.name(),
                        "statistical_pushdown_enabled", true,
                        "bigint_aggregation_pushdown_enabled", bigintPushdownEnabled))
                .build();
    }

    @Test
    public void testBigintAggregatesAreNotPushedByDefault()
    {
        ConnectorSession defaults = TestingConnectorSession.builder()
                .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                        .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                        .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig()).getSessionProperties())
                        .build())
                .build();
        for (AggregateFunction function : bigintFunctions()) {
            assertThat(apply(defaults, scanHandle(), List.of(function), List.of(List.of()))).as(function.getFunctionName()).isEmpty();
            assertThat(apply(bigintSession(GlobalAggregationEngine.SQL, false), scanHandle(), List.of(function), List.of(List.of()))).as(function.getFunctionName()).isEmpty();
        }
    }

    private static List<AggregateFunction> bigintFunctions()
    {
        return List.of(
                function("min", BIGINT, "tenantId", BIGINT),
                function("max", BIGINT, "tenantId", BIGINT),
                function("sum", BIGINT, "tenantId", BIGINT),
                function("avg", DOUBLE, "tenantId", BIGINT));
    }

    @Test
    public void testBigintAggregatesArePushedWhenEnabled()
    {
        AggregationApplicationResult<ConnectorTableHandle> result = apply(
                bigintSession(GlobalAggregationEngine.SQL, true),
                scanHandle(),
                bigintFunctions(),
                List.of(List.of()))
                .orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.type()).isEqualTo(SQL_AGGREGATION);
        assertThat(handle.metricAggregations()).extracting(MetricAggregation::functionName).containsExactly("min", "max", "sum", "avg");
        assertThat(result.getAssignments()).extracting(assignment -> assignment.getVariable() + ":" + assignment.getType())
                .containsExactly("_pushdown_0:bigint", "_pushdown_1:bigint", "_pushdown_2:bigint", "_pushdown_3:double");
    }

    @Test
    public void testBigintPushdownConfigAndSessionOverride()
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .addAll(new OpenSearchSqlSessionProperties(new OpenSearchSqlConfig().setBigintAggregationPushdownEnabled(true)).getSessionProperties())
                .build();
        AggregateFunction sum = function("sum", BIGINT, "tenantId", BIGINT);

        ConnectorSession configEnabled = TestingConnectorSession.builder().setPropertyMetadata(properties).build();
        assertThat(applyGlobal(configEnabled, scanHandle(), List.of(sum))).isEqualTo(SQL_AGGREGATION);

        ConnectorSession sessionDisabled = TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .setPropertyValues(ImmutableMap.of("bigint_aggregation_pushdown_enabled", false))
                .build();
        assertThat(apply(sessionDisabled, scanHandle(), List.of(sum), List.of(List.of()))).isEmpty();
    }

    @Test
    public void testBigintPushdownWithMixedAggregates()
    {
        ConnectorSession enabled = bigintSession(GlobalAggregationEngine.SQL, true);

        assertThat(applyGlobal(enabled, scanHandle(), List.of(
                countStar(),
                function("sum", BIGINT, "tenantId", BIGINT),
                function("max", INTEGER, "version", INTEGER),
                function("avg", DOUBLE, "duration", DOUBLE)))).isEqualTo(SQL_AGGREGATION);

        // with the setting off, one BIGINT aggregate keeps the whole statement out of the SQL path
        assertThat(apply(bigintSession(GlobalAggregationEngine.SQL, false), scanHandle(), List.of(
                countStar(),
                function("sum", BIGINT, "tenantId", BIGINT),
                function("max", INTEGER, "version", INTEGER)), List.of(List.of()))).isEmpty();
    }

    @Test
    public void testBigintPushdownDoesNotCoverStatisticalFunctionsOrDsl()
    {
        // statistical functions over BIGINT stay in Trino even when the setting is on
        ConnectorSession enabled = bigintSession(GlobalAggregationEngine.SQL, true);
        for (String name : List.of("stddev", "stddev_pop", "variance", "var_samp")) {
            assertThat(apply(enabled, scanHandle(), List.of(function(name, DOUBLE, "tenantId", BIGINT)), List.of(List.of()))).as(name).isEmpty();
        }
        assertThat(apply(enabled, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT), function("stddev", DOUBLE, "tenantId", BIGINT)), List.of(List.of()))).isEmpty();

        // the DSL engine never pushes BIGINT, even when a statistical function moves the statement to SQL
        ConnectorSession dsl = bigintSession(GlobalAggregationEngine.DSL, true);
        assertThat(apply(dsl, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT)), List.of(List.of()))).isEmpty();
        assertThat(apply(dsl, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT), function("stddev", DOUBLE, "duration", DOUBLE)), List.of(List.of()))).isEmpty();

        // grouped BIGINT aggregates are not pushed either
        assertThat(apply(enabled, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT)), List.of(List.of(KIND)))).isEmpty();
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
        assertThat(applyGlobal(session, scanHandle(renderable, Map.of()), List.of(countStar()))).isEqualTo(SQL_AGGREGATION);

        TupleDomain<ColumnHandle> unrenderable = TupleDomain.withColumnDomains(Map.of(KIND, Domain.singleValue(VARCHAR, utf8Slice("a\\b"))));
        assertThat(applyGlobal(session, scanHandle(unrenderable, Map.of()), List.of(countStar()))).isEqualTo(AGGREGATION);

        // a LIKE-derived regexp cannot be turned back into SQL
        assertThat(applyGlobal(session, scanHandle(TupleDomain.all(), Map.of("kind", "a.*")), List.of(countStar()))).isEqualTo(AGGREGATION);
    }

    @Test
    public void testColumnsThatCannotBeRenderedFallBackToDsl()
    {
        ConnectorSession session = session(GlobalAggregationEngine.SQL, true);

        // nested columns have no SQL identifier
        assertThat(applyGlobal(session, scanHandle(), List.of(function("max", INTEGER, "owner.id", INTEGER)))).isEqualTo(AGGREGATION);
        // REAL sum is supported by neither engine, REAL min and max are
        assertThat(apply(session, scanHandle(), List.of(function("sum", DOUBLE, "ratio", REAL)), List.of(List.of()))).isEmpty();
        assertThat(applyGlobal(session, scanHandle(), List.of(function("max", REAL, "ratio", REAL)))).isEqualTo(SQL_AGGREGATION);
        // builtin columns are not doc values
        assertThat(apply(session, scanHandle(), List.of(function("max", REAL, "_score", REAL)), List.of(List.of()))).isEmpty();
        // a single unrenderable aggregate keeps the whole statement on the DSL path
        assertThat(applyGlobal(session, scanHandle(), List.of(countStar(), function("max", INTEGER, "owner.id", INTEGER)))).isEqualTo(AGGREGATION);
    }

    @Test
    public void testRejections()
    {
        ConnectorSession session = session(GlobalAggregationEngine.SQL, true);

        // BIGINT inputs are not pushed unless the opt-in setting is enabled, see the BIGINT tests below
        assertThat(apply(session, scanHandle(), List.of(function("sum", BIGINT, "tenantId", BIGINT)), List.of(List.of()))).isEmpty();
        // count(DISTINCT) is approximate in OpenSearch
        assertThat(apply(session, scanHandle(), List.of(new AggregateFunction("count", BIGINT, List.of(new Variable("version", INTEGER)), List.of(), true, Optional.empty())), List.of(List.of()))).isEmpty();
        // pushdown disabled
        assertThat(apply(session(GlobalAggregationEngine.SQL, false), scanHandle(), List.of(countStar()), List.of(List.of()))).isEmpty();
        // existing TopN
        assertThat(apply(session, scanHandle().withTopN(TopN.fromLimit(5)), List.of(countStar()), List.of(List.of()))).isEmpty();
        // passthrough queries and already aggregated handles are left alone
        assertThat(apply(session, new OpenSearchTableHandle(QUERY, "default", "metric_logs", Optional.of("{}")), List.of(countStar()), List.of(List.of()))).isEmpty();
        assertThat(apply(session, scanHandle().withSqlAggregations(List.of()), List.of(countStar()), List.of(List.of()))).isEmpty();
    }
}
