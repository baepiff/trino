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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slices;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RawJsonDecoder;
import io.trino.plugin.opensearch.decoders.TimestampDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
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
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.QUERY;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOpenSearchMetadata
{
    private static final ConnectorSession SESSION = session(true);
    private static final ConnectorSession TEXT_EQUALITY_SESSION = textEqualitySession(new OpenSearchConfig(), true);
    private static final ConnectorSession TEXT_GROUP_BY_SESSION = textGroupBySession(new OpenSearchConfig(), true);

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
        TopN existing = new TopN(10, ImmutableList.of(new TopNSortItem("regionkey", SortOrder.DESC_NULLS_FIRST, Optional.of("long"))));

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
                        new TopNSortItem("regionkey", SortOrder.DESC_NULLS_FIRST, Optional.of("long")),
                        new TopNSortItem("name", SortOrder.ASC_NULLS_LAST, Optional.of("keyword")))));
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
    public void testApplyTopNUsesOpenSearchTypeAsUnmappedType()
    {
        OpenSearchColumnHandle timestamp = new OpenSearchColumnHandle(
                List.of("created"),
                TIMESTAMP_MILLIS,
                new IndexMetadata.DateTimeType(List.of("strict_date_optional_time")),
                new TimestampDecoder.Descriptor("created"),
                true);

        TopNApplicationResult<ConnectorTableHandle> result = metadata.applyTopN(
                        SESSION,
                        scanHandle(),
                        5,
                        List.of(new SortItem("created", SortOrder.ASC_NULLS_FIRST), new SortItem("value", SortOrder.DESC_NULLS_LAST)),
                        Map.of("created", timestamp, "value", integerColumn("value")))
                .orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).topN().orElseThrow().sortItems()).containsExactly(
                new TopNSortItem("created", SortOrder.ASC_NULLS_FIRST, Optional.of("date")),
                new TopNSortItem("value", SortOrder.DESC_NULLS_LAST, Optional.of("integer")));
    }

    @Test
    public void testApplyTopNRejectsColumnsWithoutPlainDocValues()
    {
        // _id reports supportsPredicates but sorting on it requires _id fielddata
        OpenSearchColumnHandle id = (OpenSearchColumnHandle) BuiltinColumns.ID.getColumnHandle();
        assertThat(id.supportsPredicates()).isTrue();
        assertThat(applyTopN(id)).isEmpty();

        // the Trino value is the JSON text of the whole field, which OpenSearch cannot sort by
        OpenSearchColumnHandle rawJson = new OpenSearchColumnHandle(List.of("payload"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new RawJsonDecoder.Descriptor("payload"), true);
        assertThat(applyTopN(rawJson)).isEmpty();

        // no OpenSearch type name is known for the unmapped_type of the sort
        OpenSearchColumnHandle scaledFloat = new OpenSearchColumnHandle(List.of("price"), DOUBLE, new IndexMetadata.ScaledFloatType(100), new DoubleDecoder.Descriptor("price"), true);
        assertThat(applyTopN(scaledFloat)).isEmpty();

        assertThat(applyTopN(keywordColumn("name"))).isPresent();
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
    public void testApplyLimitRejectsAggregation()
    {
        assertThat(metadata.applyLimit(SESSION, aggregationHandle(), 5)).isEmpty();
    }

    @Test
    public void testApplyTopNRejectsAggregation()
    {
        assertThat(metadata.applyTopN(
                SESSION,
                aggregationHandle(),
                5,
                List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)),
                Map.of("regionkey", bigintColumn("regionkey"))))
                .isEmpty();
    }

    @Test
    public void testApplyFilterRejectsAggregation()
    {
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(bigintColumn("regionkey"), Domain.singleValue(BIGINT, 1L))));

        // the same constraint is accepted for a scan, so the rejection is caused by the aggregation
        assertThat(metadata.applyFilter(SESSION, scanHandle(), constraint)).isPresent();
        assertThat(metadata.applyFilter(SESSION, aggregationHandle(), constraint)).isEmpty();
    }

    @Test
    public void testApplyFilterPushesEqualityOnTextColumnWithKeywordSubField()
    {
        OpenSearchColumnHandle tenant = textColumnWithKeyword("tenantId", OptionalInt.of(20));

        Domain single = Domain.singleValue(VARCHAR, utf8Slice("tenant-a"));
        assertFullyPushed(tenant, single);

        Domain list = Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b"), utf8Slice("c")));
        assertFullyPushed(tenant, list);

        // the longest literal the sub-field can index, counted in characters
        assertFullyPushed(tenant, Domain.singleValue(VARCHAR, utf8Slice("a".repeat(20))));
        assertFullyPushed(tenant, Domain.singleValue(VARCHAR, utf8Slice("é".repeat(20))));
        // without ignore_above only Lucene's term length limit applies
        assertFullyPushed(textColumnWithKeyword("tenantId", OptionalInt.empty()), Domain.singleValue(VARCHAR, utf8Slice("a".repeat(32766))));
    }

    @Test
    public void testApplyFilterKeepsInexactTextPredicatesInTrino()
    {
        OpenSearchColumnHandle tenant = textColumnWithKeyword("tenantId", OptionalInt.of(20));

        // a longer literal can only equal values the sub-field did not index
        assertNotPushed(tenant, Domain.singleValue(VARCHAR, utf8Slice("a".repeat(21))));
        // one character outside the basic multilingual plane counts as two
        assertNotPushed(textColumnWithKeyword("tenantId", OptionalInt.of(1)), Domain.singleValue(VARCHAR, utf8Slice("😀")));
        // one unindexable value keeps the whole list in Trino
        assertNotPushed(tenant, Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b".repeat(21)))));
        assertNotPushed(textColumnWithKeyword("tenantId", OptionalInt.empty()), Domain.singleValue(VARCHAR, utf8Slice("a".repeat(32767))));
        // ranges, including <>, would miss the values the sub-field did not index
        assertNotPushed(tenant, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("a"))), false));
        assertNotPushed(tenant, Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("a")), Range.greaterThan(VARCHAR, utf8Slice("a"))), false));
        assertNotPushed(tenant, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("a")), Range.greaterThan(VARCHAR, utf8Slice("m"))), false));
        // null checks
        assertNotPushed(tenant, Domain.onlyNull(VARCHAR));
        assertNotPushed(tenant, Domain.notNull(VARCHAR));
        assertNotPushed(tenant, Domain.singleValue(VARCHAR, utf8Slice("a"), true));
        // very long lists
        assertNotPushed(tenant, Domain.multipleValues(VARCHAR, IntStream.range(0, 1025).mapToObj(value -> utf8Slice(String.valueOf(value))).collect(toImmutableList())));
        // text columns without a suitable sub-field
        assertNotPushed(textColumn("description"), Domain.singleValue(VARCHAR, utf8Slice("a")));
    }

    @Test
    public void testApplyFilterPushesOnlyTheExactTextColumns()
    {
        OpenSearchColumnHandle tenant = textColumnWithKeyword("tenantId", OptionalInt.of(20));
        OpenSearchColumnHandle workspace = textColumnWithKeyword("workspaceType", OptionalInt.of(20));
        OpenSearchColumnHandle regionkey = bigintColumn("regionkey");
        Domain tenantDomain = Domain.singleValue(VARCHAR, utf8Slice("a"));
        Domain workspaceDomain = Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("a"))), false);
        Domain regionkeyDomain = Domain.singleValue(BIGINT, 1L);

        ConstraintApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(
                        TEXT_EQUALITY_SESSION,
                        scanHandle(),
                        new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain, workspace, workspaceDomain, regionkey, regionkeyDomain))))
                .orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).constraint())
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain, regionkey, regionkeyDomain)));
        assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.of(workspace, workspaceDomain)));

        // a second filter narrows the pushed values
        ConstraintApplicationResult<ConnectorTableHandle> narrowed = metadata.applyFilter(
                        TEXT_EQUALITY_SESSION,
                        new OpenSearchTableHandle(
                                SCAN,
                                "default",
                                "nation",
                                TupleDomain.withColumnDomains(Map.of(tenant, Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b"))))),
                                Map.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Set.of(),
                                List.of(),
                                List.of()),
                        new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain))))
                .orElseThrow();
        assertThat(((OpenSearchTableHandle) narrowed.getHandle()).constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain)));
    }

    @Test
    public void testKeywordSubFieldSelection()
    {
        IndexMetadata.SubField keyword = subField("keyword", "keyword", OptionalInt.of(256), Optional.empty());
        assertThat(keywordSubField(textField(keyword))).hasValue(keyword);
        IndexMetadata.SubField unlimited = subField("raw", "keyword", OptionalInt.empty(), Optional.empty());
        assertThat(keywordSubField(textField(unlimited))).hasValue(unlimited);

        // the sub-field indexing the longest values wins
        assertThat(keywordSubField(textField(keyword, unlimited))).hasValue(unlimited);
        IndexMetadata.SubField shorter = subField("short", "keyword", OptionalInt.of(10), Optional.empty());
        assertThat(keywordSubField(textField(shorter, keyword))).hasValue(keyword);

        // a normalizer changes the indexed terms
        assertThat(keywordSubField(textField(subField("keyword", "keyword", OptionalInt.of(256), Optional.of("lowercase"))))).isEmpty();
        // only keyword sub-fields index the value as one term
        assertThat(keywordSubField(textField(subField("english", "text", OptionalInt.empty(), Optional.empty())))).isEmpty();
        assertThat(keywordSubField(textField(subField("wildcard", "wildcard", OptionalInt.empty(), Optional.empty())))).isEmpty();
        // not indexed, or indexing a term for missing values
        assertThat(keywordSubField(textField(new IndexMetadata.SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), false, false, true)))).isEmpty();
        assertThat(keywordSubField(textField(new IndexMetadata.SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), true, true, true)))).isEmpty();
        // nothing can be indexed
        assertThat(keywordSubField(textField(subField("keyword", "keyword", OptionalInt.of(0), Optional.empty())))).isEmpty();
        // a text field without sub-fields
        assertThat(keywordSubField(textField())).isEmpty();

        // keyword columns support predicates directly, and other columns are not read as a plain VARCHAR
        assertThat(OpenSearchMetadata.keywordSubField(new IndexMetadata.Field(false, false, "a", new IndexMetadata.PrimitiveType("keyword"), List.of(keyword)), VARCHAR, new VarcharDecoder.Descriptor("a"))).isEmpty();
        assertThat(OpenSearchMetadata.keywordSubField(new IndexMetadata.Field(true, false, "a", new IndexMetadata.PrimitiveType("text"), List.of(keyword)), VARCHAR, new RawJsonDecoder.Descriptor("a"))).isEmpty();
        assertThat(OpenSearchMetadata.keywordSubField(new IndexMetadata.Field(false, true, "a", new IndexMetadata.PrimitiveType("text"), List.of(keyword)), new ArrayType(VARCHAR), new VarcharDecoder.Descriptor("a"))).isEmpty();
    }

    @Test
    public void testColumnHandleJsonRoundTrip()
    {
        JsonMapper mapper = new JsonMapperProvider().get()
                .rebuild()
                .addModule(new SimpleModule().addDeserializer(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .build();
        JsonCodec<OpenSearchColumnHandle> codec = new JsonCodecFactory(mapper).jsonCodec(OpenSearchColumnHandle.class);

        OpenSearchColumnHandle withSubField = textColumnWithKeyword("tenantId", OptionalInt.of(256));
        assertThat(codec.fromJson(codec.toJson(withSubField))).isEqualTo(withSubField);
        OpenSearchColumnHandle withoutLimit = textColumnWithKeyword("tenantId", OptionalInt.empty());
        assertThat(codec.fromJson(codec.toJson(withoutLimit))).isEqualTo(withoutLimit);
        OpenSearchColumnHandle keyword = keywordColumn("name");
        assertThat(codec.fromJson(codec.toJson(keyword))).isEqualTo(keyword);

        // a handle serialized without the sub-field
        String json = codec.toJson(keyword).replaceAll(",\\s*\"keywordSubField\"\\s*:\\s*null", "");
        assertThat(json).doesNotContain("keywordSubField");
        assertThat(codec.fromJson(json)).isEqualTo(keyword);

        OpenSearchColumnHandle groupable = groupableTextColumn("tenantId", OptionalInt.of(20));
        assertThat(codec.fromJson(codec.toJson(groupable))).isEqualTo(groupable);
        // a handle serialized before the presence of a value was tracked cannot be grouped on
        String withoutPresence = codec.toJson(groupable).replaceAll(",\\s*\"presenceIndexed\"\\s*:\\s*true", "");
        assertThat(withoutPresence).doesNotContain("presenceIndexed");
        assertThat(codec.fromJson(withoutPresence)).isEqualTo(textColumnWithKeyword("tenantId", OptionalInt.of(20)));
        // nor one serialized before the doc values of the sub-field were tracked
        String withoutDocValues = codec.toJson(groupable).replaceAll(",\\s*\"docValues\"\\s*:\\s*true", "");
        assertThat(withoutDocValues).doesNotContain("docValues");
        assertThat(codec.fromJson(withoutDocValues).keywordSubField().orElseThrow().docValues()).isFalse();
        assertThat(TermAggregation.fromKeywordSubField(codec.fromJson(withoutDocValues))).isEmpty();
    }

    @Test
    public void testTermAggregationJsonRoundTrip()
    {
        JsonMapper mapper = new JsonMapperProvider().get()
                .rebuild()
                .addModule(new SimpleModule().addDeserializer(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .build();
        JsonCodec<TermAggregation> codec = new JsonCodecFactory(mapper).jsonCodec(TermAggregation.class);

        TermAggregation subField = new TermAggregation("tenantId", VARCHAR, Optional.of("keyword"));
        assertThat(codec.fromJson(codec.toJson(subField))).isEqualTo(subField);
        assertThat(codec.toJson(subField)).doesNotContain("field\"");
        TermAggregation keyword = new TermAggregation("name", VARCHAR);
        assertThat(codec.fromJson(codec.toJson(keyword))).isEqualTo(keyword);

        // a term serialized before text columns could be grouped on
        String json = codec.toJson(keyword).replaceAll(",\\s*\"subField\"\\s*:\\s*null", "");
        assertThat(json).doesNotContain("subField");
        assertThat(codec.fromJson(json)).isEqualTo(keyword);
    }

    @Test
    public void testGroupByTextColumnNotPushedByDefault()
    {
        OpenSearchColumnHandle tenant = groupableTextColumn("tenantId", OptionalInt.of(20));

        assertThat(applyCountGroupedBy(SESSION, tenant)).isEmpty();
        assertThat(applyCountGroupedBy(textGroupBySession(new OpenSearchConfig(), false), tenant)).isEmpty();
        // the equality switch does not enable it
        assertThat(applyCountGroupedBy(TEXT_EQUALITY_SESSION, tenant)).isEmpty();
    }

    @Test
    public void testGroupByTextColumnPushedToKeywordSubField()
    {
        OpenSearchColumnHandle tenant = groupableTextColumn("tenantId", OptionalInt.of(20));

        AggregationApplicationResult<ConnectorTableHandle> result = applyCountGroupedBy(TEXT_GROUP_BY_SESSION, tenant).orElseThrow();

        OpenSearchTableHandle handle = (OpenSearchTableHandle) result.getHandle();
        assertThat(handle.type()).isEqualTo(OpenSearchTableHandle.Type.AGGREGATION);
        assertThat(handle.termAggregations()).containsExactly(new TermAggregation("tenantId", VARCHAR, Optional.of("keyword")));
        assertThat(handle.termAggregations().getFirst().field()).isEqualTo("tenantId.keyword");
        assertThat(handle.metricAggregations()).containsExactly(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"));
        // the grouping column itself is the output, read under its own name and type
        assertThat(result.getGroupingColumnMapping()).isEmpty();
        assertThat(result.getAssignments()).extracting(Assignment::getVariable).containsExactly("_pushdown_0");
    }

    @Test
    public void testGroupByTextColumnSessionOverridesConfig()
    {
        OpenSearchColumnHandle tenant = groupableTextColumn("tenantId", OptionalInt.of(20));

        // config on, session switches it off
        OpenSearchConfig enabled = new OpenSearchConfig().setTextGroupByPushdownEnabled(true);
        assertThat(applyCountGroupedBy(textGroupBySession(enabled, null), tenant)).isPresent();
        assertThat(applyCountGroupedBy(textGroupBySession(enabled, false), tenant)).isEmpty();

        // config off, session switches it on
        OpenSearchConfig disabled = new OpenSearchConfig();
        assertThat(applyCountGroupedBy(textGroupBySession(disabled, null), tenant)).isEmpty();
        assertThat(applyCountGroupedBy(textGroupBySession(disabled, true), tenant)).isPresent();
    }

    @Test
    public void testGroupByTextColumnWithoutEligibleSubFieldNotPushed()
    {
        // the mapping-level eligibility is the one of the equality push down
        for (IndexMetadata.Field field : List.of(
                textField(),
                textField(subField("keyword", "keyword", OptionalInt.of(256), Optional.of("lowercase"))),
                textField(subField("english", "text", OptionalInt.empty(), Optional.empty())),
                textField(new IndexMetadata.SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), false, false, true)),
                textField(new IndexMetadata.SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), true, true, true)))) {
            Optional<IndexMetadata.SubField> subField = keywordSubField(field);
            OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                    List.of(field.name()),
                    VARCHAR,
                    field.type(),
                    new VarcharDecoder.Descriptor(field.name()),
                    false,
                    subField,
                    subField.isPresent());
            assertThat(applyCountGroupedBy(TEXT_GROUP_BY_SESSION, column)).as(field.toString()).isEmpty();
        }

        // a sub-field without doc values, which the terms source of the composite aggregation cannot read; the equality push down can still use it
        IndexMetadata.SubField withoutDocValues = new IndexMetadata.SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), true, false, false);
        assertThat(keywordSubField(textField(withoutDocValues))).hasValue(withoutDocValues);
        OpenSearchColumnHandle withoutDocValuesColumn = new OpenSearchColumnHandle(
                List.of("tenantId"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor("tenantId"),
                false,
                Optional.of(withoutDocValues),
                true);
        assertThat(applyCountGroupedBy(TEXT_GROUP_BY_SESSION, withoutDocValuesColumn)).isEmpty();

        // an eligible sub-field when the presence of a value is not indexed, so uncovered documents cannot be detected
        assertThat(applyCountGroupedBy(TEXT_GROUP_BY_SESSION, textColumnWithKeyword("tenantId", OptionalInt.of(20)))).isEmpty();
        // a text column without sub-field
        assertThat(applyCountGroupedBy(TEXT_GROUP_BY_SESSION, textColumn("description"))).isEmpty();
    }

    @Test
    public void testGroupByKeywordAndTextColumns()
    {
        OpenSearchColumnHandle tenant = groupableTextColumn("tenantId", OptionalInt.of(20));
        OpenSearchColumnHandle kind = keywordColumn("kind");
        OpenSearchColumnHandle value = integerColumn("value");

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                        TEXT_GROUP_BY_SESSION,
                        scanHandle(),
                        List.of(new AggregateFunction("max", INTEGER, List.of(new Variable("value", INTEGER)), List.of(), false, Optional.empty())),
                        Map.of("value", value),
                        List.of(List.of(kind, tenant)))
                .orElseThrow();

        assertThat(((OpenSearchTableHandle) result.getHandle()).termAggregations()).containsExactly(
                new TermAggregation("kind", VARCHAR),
                new TermAggregation("tenantId", VARCHAR, Optional.of("keyword")));

        // one grouping column that cannot be pushed keeps the whole aggregation in Trino
        assertThat(metadata.applyAggregation(
                TEXT_GROUP_BY_SESSION,
                scanHandle(),
                List.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                Map.of(),
                List.of(List.of(tenant, textColumn("description"))))).isEmpty();
    }

    @Test
    public void testTextColumnStillRejectedForSortAndMetrics()
    {
        OpenSearchColumnHandle tenant = groupableTextColumn("tenantId", OptionalInt.of(20));

        assertThat(metadata.applyTopN(
                TEXT_GROUP_BY_SESSION,
                scanHandle(),
                5,
                List.of(new SortItem("tenantId", SortOrder.ASC_NULLS_LAST)),
                Map.of("tenantId", tenant))).isEmpty();
        for (String function : List.of("min", "max", "count")) {
            Type outputType = function.equals("count") ? BIGINT : VARCHAR;
            assertThat(metadata.applyAggregation(
                    TEXT_GROUP_BY_SESSION,
                    scanHandle(),
                    List.of(new AggregateFunction(function, outputType, List.of(new Variable("tenantId", VARCHAR)), List.of(), false, Optional.empty())),
                    Map.of("tenantId", tenant),
                    List.of(List.of())))
                    .as(function)
                    .isEmpty();
        }
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> applyCountGroupedBy(ConnectorSession session, OpenSearchColumnHandle column)
    {
        return metadata.applyAggregation(
                session,
                scanHandle(),
                List.of(new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty())),
                Map.of(),
                List.of(List.of(column)));
    }

    @Test
    public void testApplyFilterKeepsTextPredicatesInTrinoByDefault()
    {
        OpenSearchColumnHandle tenant = textColumnWithKeyword("tenantId", OptionalInt.of(20));
        OpenSearchColumnHandle regionkey = bigintColumn("regionkey");
        Domain tenantDomain = Domain.singleValue(VARCHAR, utf8Slice("a"));
        Domain regionkeyDomain = Domain.singleValue(BIGINT, 1L);

        // the switch is off by default, so nothing is pushed for the text column
        assertThat(metadata.applyFilter(SESSION, scanHandle(), new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain))))).isEmpty();
        assertThat(metadata.applyFilter(textEqualitySession(new OpenSearchConfig(), false), scanHandle(), new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain))))).isEmpty();

        // other columns are still pushed, and the text domain remains in the remaining filter
        ConstraintApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(
                        SESSION,
                        scanHandle(),
                        new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain, regionkey, regionkeyDomain))))
                .orElseThrow();
        assertThat(((OpenSearchTableHandle) result.getHandle()).constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(regionkey, regionkeyDomain)));
        assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.of(tenant, tenantDomain)));
    }

    @Test
    public void testTextEqualityPushdownSessionOverridesConfig()
    {
        OpenSearchColumnHandle tenant = textColumnWithKeyword("tenantId", OptionalInt.of(20));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(tenant, Domain.singleValue(VARCHAR, utf8Slice("a")))));

        // config on, session switches it off
        OpenSearchConfig enabled = new OpenSearchConfig().setTextEqualityPushdownEnabled(true);
        assertThat(metadata.applyFilter(textEqualitySession(enabled, null), scanHandle(), constraint)).isPresent();
        assertThat(metadata.applyFilter(textEqualitySession(enabled, false), scanHandle(), constraint)).isEmpty();

        // config off, session switches it on
        OpenSearchConfig disabled = new OpenSearchConfig();
        assertThat(metadata.applyFilter(textEqualitySession(disabled, null), scanHandle(), constraint)).isEmpty();
        assertThat(metadata.applyFilter(textEqualitySession(disabled, true), scanHandle(), constraint)).isPresent();
    }

    private void assertFullyPushed(OpenSearchColumnHandle column, Domain domain)
    {
        ConstraintApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(TEXT_EQUALITY_SESSION, scanHandle(), new Constraint(TupleDomain.withColumnDomains(Map.of(column, domain))))
                .orElseThrow();
        assertThat(((OpenSearchTableHandle) result.getHandle()).constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(column, domain)));
        assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
    }

    private void assertNotPushed(OpenSearchColumnHandle column, Domain domain)
    {
        // nothing changes, so the filter stays in Trino
        assertThat(metadata.applyFilter(TEXT_EQUALITY_SESSION, scanHandle(), new Constraint(TupleDomain.withColumnDomains(Map.of(column, domain))))).isEmpty();
    }

    private static Optional<IndexMetadata.SubField> keywordSubField(IndexMetadata.Field field)
    {
        return OpenSearchMetadata.keywordSubField(field, VARCHAR, new VarcharDecoder.Descriptor(field.name()));
    }

    private static IndexMetadata.Field textField(IndexMetadata.SubField... subFields)
    {
        return new IndexMetadata.Field(false, false, "tenantId", new IndexMetadata.PrimitiveType("text"), List.of(subFields));
    }

    private static IndexMetadata.SubField subField(String name, String type, OptionalInt ignoreAbove, Optional<String> normalizer)
    {
        return new IndexMetadata.SubField(name, type, ignoreAbove, normalizer, true, false, true);
    }

    static OpenSearchColumnHandle textColumnWithKeyword(String name, OptionalInt ignoreAbove)
    {
        return new OpenSearchColumnHandle(
                List.of(name),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor(name),
                false,
                Optional.of(subField("keyword", "keyword", ignoreAbove, Optional.empty())));
    }

    /**
     * A text column with a keyword sub-field whose index records the presence of a value, as the defaults of a mapping do.
     */
    static OpenSearchColumnHandle groupableTextColumn(String name, OptionalInt ignoreAbove)
    {
        return new OpenSearchColumnHandle(
                List.of(name),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor(name),
                false,
                Optional.of(subField("keyword", "keyword", ignoreAbove, Optional.empty())),
                true);
    }

    private Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(OpenSearchColumnHandle column)
    {
        return metadata.applyTopN(
                SESSION,
                scanHandle(),
                5,
                List.of(new SortItem(column.name(), SortOrder.ASC_NULLS_LAST)),
                Map.of(column.name(), column));
    }

    private static String likeToRegexp(String pattern, Optional<String> escapeChar)
    {
        return OpenSearchMetadata.likeToRegexp(Slices.utf8Slice(pattern), escapeChar.map(Slices::utf8Slice));
    }

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

    private static OpenSearchTableHandle aggregationHandle()
    {
        return scanHandle().withAggregations(List.of(), List.of());
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

    /**
     * A session whose defaults come from the given config, with the text equality session property overridden
     * when {@code override} is not null.
     */
    static ConnectorSession textEqualitySession(OpenSearchConfig config, Boolean override)
    {
        TestingConnectorSession.Builder builder = TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenSearchSessionProperties(config).getSessionProperties());
        if (override != null) {
            builder.setPropertyValues(ImmutableMap.of("text_equality_pushdown_enabled", override));
        }
        return builder.build();
    }

    /**
     * A session whose defaults come from the given config, with the text GROUP BY session property overridden
     * when {@code override} is not null.
     */
    static ConnectorSession textGroupBySession(OpenSearchConfig config, Boolean override)
    {
        TestingConnectorSession.Builder builder = TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenSearchSessionProperties(config).getSessionProperties());
        if (override != null) {
            builder.setPropertyValues(ImmutableMap.of("text_groupby_pushdown_enabled", override));
        }
        return builder.build();
    }

    static ConnectorSession session(boolean aggregationPushdownEnabled)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenSearchSessionProperties(new OpenSearchConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", aggregationPushdownEnabled))
                .build();
    }
}
