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
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenSearchQueryBuilder
{
    private static final OpenSearchColumnHandle NAME = new OpenSearchColumnHandle(ImmutableList.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("name"), true);
    private static final OpenSearchColumnHandle AGE = new OpenSearchColumnHandle(ImmutableList.of("age"), INTEGER, new IndexMetadata.PrimitiveType("int"), new IntegerDecoder.Descriptor("age"), true);
    private static final OpenSearchColumnHandle SCORE = new OpenSearchColumnHandle(ImmutableList.of("score"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("score"), true);
    private static final OpenSearchColumnHandle LENGTH = new OpenSearchColumnHandle(ImmutableList.of("length"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("length"), true);

    @Test
    public void testMatchAll()
    {
        assertQueryBuilder(
                ImmutableMap.of(),
                new MatchAllQueryBuilder());
    }

    @Test
    public void testOneConstraint()
    {
        // SingleValue
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L)),
                new BoolQueryBuilder().filter(new TermQueryBuilder(AGE.name(), 1L)));

        // Range
        assertQueryBuilder(
                ImmutableMap.of(SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder().filter(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0)));

        // List
        assertQueryBuilder(
                ImmutableMap.of(NAME, Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("alice"), utf8Slice("bob")))),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(NAME.name(), "alice"))
                                .should(new TermQueryBuilder(NAME.name(), "bob"))));
        // all
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.all(INTEGER)),
                new MatchAllQueryBuilder());

        // notNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.notNull(INTEGER)),
                new BoolQueryBuilder().filter(new ExistsQueryBuilder(AGE.name())));

        // isNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.onlyNull(INTEGER)),
                new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.name())));

        // isNullAllowed
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L, true)),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(AGE.name(), 1L))
                                .should(new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.name())))));
    }

    @Test
    public void testMultiConstraint()
    {
        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 1L),
                        SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.name(), 1L))
                        .filter(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0)));

        assertQueryBuilder(
                ImmutableMap.of(
                        LENGTH, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 160.0, true, 180.0, true)), false),
                        SCORE, Domain.create(ValueSet.ofRanges(
                                Range.range(DOUBLE, 65.0, false, 80.0, true),
                                Range.equal(DOUBLE, 90.0)), false)),
                new BoolQueryBuilder()
                        .filter(new RangeQueryBuilder(LENGTH.name()).gte(160.0).lte(180.0))
                        .filter(new BoolQueryBuilder()
                                .should(new RangeQueryBuilder(SCORE.name()).gt(65.0).lte(80.0))
                                .should(new TermQueryBuilder(SCORE.name(), 90.0))));

        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 10L),
                        SCORE, Domain.onlyNull(DOUBLE)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.name(), 10L))
                        .mustNot(new ExistsQueryBuilder(SCORE.name())));
    }

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

    private static void assertQueryBuilder(Map<OpenSearchColumnHandle, Domain> domains, QueryBuilder expected)
    {
        QueryBuilder actual = OpenSearchQueryBuilder.buildSearchQuery(TupleDomain.withColumnDomains(domains), Optional.empty(), Map.of());
        assertThat(actual).isEqualTo(expected);
    }
}
