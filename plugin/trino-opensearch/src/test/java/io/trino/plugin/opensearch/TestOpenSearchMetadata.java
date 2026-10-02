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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.QUERY;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
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

    @Test
    public void testLikeToRegexp()
    {
        assertThat(likeToRegexp("a_b_c", Optional.empty())).isEqualTo("a.b.c");
        assertThat(likeToRegexp("a%b%c", Optional.empty())).isEqualTo("a.*b.*c");
        assertThat(likeToRegexp("a%b_c", Optional.empty())).isEqualTo("a.*b.c");
        assertThat(likeToRegexp("a[b", Optional.empty())).isEqualTo("a\\[b");
        assertThat(likeToRegexp("a_\\_b", Optional.of("\\"))).isEqualTo("a._b");
        assertThat(likeToRegexp("a$_b", Optional.of("$"))).isEqualTo("a_b");
        assertThat(likeToRegexp("s_.m%ex\\t", Optional.of("$"))).isEqualTo("s.\\.m.*ex\\\\t");
        assertThat(likeToRegexp("\000%", Optional.empty())).isEqualTo("\000.*");
        assertThat(likeToRegexp("\000%", Optional.of("\000"))).isEqualTo("%");
        assertThat(likeToRegexp("中文%", Optional.empty())).isEqualTo("中文.*");
        assertThat(likeToRegexp("こんにちは%", Optional.empty())).isEqualTo("こんにちは.*");
        assertThat(likeToRegexp("안녕하세요%", Optional.empty())).isEqualTo("안녕하세요.*");
        assertThat(likeToRegexp("Привет%", Optional.empty())).isEqualTo("Привет.*");
    }

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
        assertThat(handle.termAggregations()).containsExactly(new TermAggregation("kind", VARCHAR));
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

    private static String likeToRegexp(String pattern, Optional<String> escapeChar)
    {
        return OpenSearchMetadata.likeToRegexp(Slices.utf8Slice(pattern), escapeChar.map(Slices::utf8Slice));
    }

    static OpenSearchTableHandle scanHandle()
    {
        return new OpenSearchTableHandle(SCAN, "default", "nation", Optional.empty());
    }

    static OpenSearchColumnHandle bigintColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle integerColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle doubleColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle keywordColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor(name), true);
    }

    static OpenSearchColumnHandle textColumn(String name)
    {
        return new OpenSearchColumnHandle(List.of(name), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor(name), false);
    }

    static ConnectorSession session(boolean aggregationPushdownEnabled)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", aggregationPushdownEnabled))
                .build();
    }
}
