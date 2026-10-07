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
import io.trino.plugin.opensearch.AggregationResponseReader.Result;
import io.trino.spi.TrinoException;
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
import org.opensearch.search.aggregations.metrics.ParsedExtendedStats;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedStats;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.bigintColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.doubleColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.integerColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAggregationResponseReader
{
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(ImmutableList.of(
            entry("composite", (parser, name) -> ParsedComposite.fromXContent(parser, (String) name)),
            entry("max", (parser, name) -> ParsedMax.fromXContent(parser, (String) name)),
            entry("min", (parser, name) -> ParsedMin.fromXContent(parser, (String) name)),
            entry("avg", (parser, name) -> ParsedAvg.fromXContent(parser, (String) name)),
            entry("stats", (parser, name) -> ParsedStats.fromXContent(parser, (String) name)),
            entry("extended_stats", (parser, name) -> ParsedExtendedStats.fromXContent(parser, (String) name)),
            entry("value_count", (parser, name) -> ParsedValueCount.fromXContent(parser, (String) name))));

    private static final MetricAggregation COUNT_STAR = new MetricAggregation("count", BIGINT, Optional.empty(), "c_star");
    private static final MetricAggregation COUNT_VALUE = new MetricAggregation("count", BIGINT, Optional.of(integerColumn("v")), "c_value");
    private static final MetricAggregation SUM_VALUE = new MetricAggregation("sum", BIGINT, Optional.of(integerColumn("v")), "s_value");
    private static final MetricAggregation AVG_VALUE = new MetricAggregation("avg", DOUBLE, Optional.of(integerColumn("v")), "a_value");
    private static final MetricAggregation MIN_VALUE = new MetricAggregation("min", INTEGER, Optional.of(integerColumn("v")), "min_value");
    private static final MetricAggregation MAX_VALUE = new MetricAggregation("max", INTEGER, Optional.of(integerColumn("v")), "max_value");

    private static final List<MetricAggregation> STATISTICAL = ImmutableList.of(
            new MetricAggregation("stddev_samp", DOUBLE, Optional.of(doubleColumn("d")), "stddev_samp"),
            new MetricAggregation("stddev_pop", DOUBLE, Optional.of(doubleColumn("d")), "stddev_pop"),
            new MetricAggregation("var_samp", DOUBLE, Optional.of(doubleColumn("d")), "var_samp"),
            new MetricAggregation("var_pop", DOUBLE, Optional.of(doubleColumn("d")), "var_pop"));

    private static final MetricAggregation BIGINT_SUM = new MetricAggregation("sum", BIGINT, Optional.of(bigintColumn("big")), "big_sum");
    private static final MetricAggregation BIGINT_MIN = new MetricAggregation("min", BIGINT, Optional.of(bigintColumn("big")), "big_min");
    private static final MetricAggregation BIGINT_MAX = new MetricAggregation("max", BIGINT, Optional.of(bigintColumn("big")), "big_max");
    private static final MetricAggregation BIGINT_AVG = new MetricAggregation("avg", DOUBLE, Optional.of(bigintColumn("big")), "big_avg");

    @Test
    public void testGlobalAggregations()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
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
        Aggregations aggregations = parse(
                """
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
        Aggregations aggregations = parse(
                """
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
        Aggregations aggregations = parse(
                """
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
        Aggregations aggregations = parse(
                """
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

    @Test
    public void testBigintGuardBoundary()
            throws IOException
    {
        Result result = AggregationResponseReader.read(
                parse(bigintSum(9007199254740991.0)), 1, ImmutableList.of(), ImmutableList.of(BIGINT_SUM));
        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row).containsEntry("big_sum", 9007199254740991.0));

        for (double value : new double[] {9007199254740992.0, -9007199254740992.0}) {
            Aggregations aggregations = parse(bigintSum(value));
            assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 1, ImmutableList.of(), ImmutableList.of(BIGINT_SUM)))
                    .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()))
                    .hasMessageContaining("bigint-aggregation-pushdown-enabled")
                    .hasMessageContaining("sum")
                    .hasMessageContaining("big");
        }
    }

    @Test
    public void testSumOverNonBigintInputIsNotGuarded()
            throws IOException
    {
        // the guard keys off the input column type: an INTEGER column sums to a BIGINT but cannot lose precision in the response
        Result result = AggregationResponseReader.read(
                parse(
                        """
                        {"stats#s_value": {"count": 1, "min": 1.0, "max": 1.0, "avg": 1.0, "sum": 9.007199254740992E15}}
                        """),
                1,
                ImmutableList.of(),
                ImmutableList.of(SUM_VALUE));
        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row).containsEntry("s_value", 9.007199254740992e15));
    }

    @Test
    public void testBigintGuardAppliesToMinAndMax()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
                {
                  "min#big_min": {"value": -9.007199254740992E15},
                  "max#big_max": {"value": 9.007199254740992E15}
                }
                """);
        assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 1, ImmutableList.of(), ImmutableList.of(BIGINT_MIN)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("min");
        assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 1, ImmutableList.of(), ImmutableList.of(BIGINT_MAX)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("max");
    }

    @Test
    public void testBigintAvgIsNotGuarded()
            throws IOException
    {
        Result result = AggregationResponseReader.read(
                parse(
                        """
                        {"avg#big_avg": {"value": 1.0E16}}
                        """),
                1,
                ImmutableList.of(),
                ImmutableList.of(BIGINT_AVG));
        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row).containsEntry("big_avg", 1.0e16));
    }

    @Test
    public void testBigintGuardOnEmptyInput()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
                {
                  "stats#big_sum": {"count": 0, "min": null, "max": null, "avg": null, "sum": 0.0},
                  "min#big_min": {"value": null},
                  "max#big_max": {"value": null}
                }
                """);
        Result result = AggregationResponseReader.read(aggregations, 0, ImmutableList.of(), ImmutableList.of(BIGINT_SUM, BIGINT_MIN, BIGINT_MAX));

        Map<String, Object> expected = new HashMap<>();
        expected.put("big_sum", null);
        expected.put("big_min", null);
        expected.put("big_max", null);
        assertThat(result.rows()).containsExactly(expected);
    }

    @Test
    public void testGroupedBigintGuardFailsWholeQuery()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
                {
                  "composite#groupBy": {
                    "after_key": {"kind": "b"},
                    "buckets": [
                      {"key": {"kind": "a"}, "doc_count": 1, "stats#big_sum": {"count": 1, "min": 1.0, "max": 1.0, "avg": 1.0, "sum": 1.0}},
                      {"key": {"kind": "b"}, "doc_count": 1, "stats#big_sum": {"count": 1, "min": 9.007199254740992E15, "max": 9.007199254740992E15, "avg": 9.007199254740992E15, "sum": 9.007199254740992E15}}
                    ]
                  }
                }
                """);
        assertThatThrownBy(() -> AggregationResponseReader.read(
                aggregations,
                2,
                ImmutableList.of(new TermAggregation("kind", VARCHAR)),
                ImmutableList.of(BIGINT_SUM)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()));
    }

    @Test
    public void testBigintSumFailsWhenInputsCancelOutAfterRounding()
            throws IOException
    {
        // 2^53+1 and -2^53 become the doubles 2^53 and -2^53, so the sum is 0 instead of 1
        Aggregations aggregations = parse(bigintStats(2, -9007199254740992.0, 9007199254740992.0, 0.0));
        assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 2, ImmutableList.of(), ImmutableList.of(BIGINT_SUM)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()))
                .hasMessageContaining("bigint-aggregation-pushdown-enabled")
                .hasMessageContaining("sum(big)")
                .hasMessageContaining("9007199254740992")
                .hasMessageNotContaining("E15");
    }

    @Test
    public void testBigintSumFailsWhenSingleInputIsNotExactlyRepresentable()
            throws IOException
    {
        Aggregations aggregations = parse(bigintStats(3, 0.0, 9007199254740992.0, 10.0));
        assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 3, ImmutableList.of(), ImmutableList.of(BIGINT_SUM)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("bigint-aggregation-pushdown-enabled");
    }

    @Test
    public void testBigintSumFailsWhenMixedSignPartialSumsMayBeInexact()
            throws IOException
    {
        // count * max(|min|, |max|) = 10 * 1e15 exceeds 2^53 although every value and the final sum are exact
        Aggregations aggregations = parse(bigintStats(10, -1.0e15, 1.0e15, 0.0));
        assertThatThrownBy(() -> AggregationResponseReader.read(aggregations, 10, ImmutableList.of(), ImmutableList.of(BIGINT_SUM)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()))
                .hasMessageContaining("bigint-aggregation-pushdown-enabled")
                .hasMessageContaining("partial sums");
    }

    @Test
    public void testBigintSumOverSmallMixedSignDataIsReturned()
            throws IOException
    {
        Result result = AggregationResponseReader.read(
                parse(bigintStats(100, -1.0e10, 1.0e10, 5.0e9)), 100, ImmutableList.of(), ImmutableList.of(BIGINT_SUM));
        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row).containsEntry("big_sum", 5.0e9));
    }

    @Test
    public void testBigintSumOverNonNegativeDataIsCheckedByFinalValueOnly()
            throws IOException
    {
        // count * max is 4e15 * 1000 beyond 2^53, but all values are non-negative so every partial sum is bounded by the final sum
        Result result = AggregationResponseReader.read(
                parse(bigintStats(4_000_000, 0.0, 1.0e9, 4.0e15)), 4_000_000, ImmutableList.of(), ImmutableList.of(BIGINT_SUM));
        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row).containsEntry("big_sum", 4.0e15));
    }

    @Test
    public void testGroupedBigintSumFailsWholeQueryOnMixedSignBucket()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
                {
                  "composite#groupBy": {
                    "after_key": {"kind": "b"},
                    "buckets": [
                      {"key": {"kind": "a"}, "doc_count": 1, "stats#big_sum": {"count": 1, "min": 1.0, "max": 1.0, "avg": 1.0, "sum": 1.0}},
                      {"key": {"kind": "b"}, "doc_count": 10, "stats#big_sum": {"count": 10, "min": -1.0E15, "max": 1.0E15, "avg": 0.0, "sum": 0.0}}
                    ]
                  }
                }
                """);
        assertThatThrownBy(() -> AggregationResponseReader.read(
                aggregations,
                11,
                ImmutableList.of(new TermAggregation("kind", VARCHAR)),
                ImmutableList.of(BIGINT_SUM)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("partial sums");
    }

    @Test
    public void testStatisticalValues()
            throws IOException
    {
        Aggregations aggregations = parse("{" + statisticalResults(5, "2.0", "2.5") + "}");

        Result result = AggregationResponseReader.read(aggregations, 5, ImmutableList.of(), STATISTICAL);

        assertThat(result.rows()).singleElement().satisfies(row -> assertThat(row)
                .containsEntry("stddev_samp", Math.sqrt(2.5))
                .containsEntry("stddev_pop", Math.sqrt(2.0))
                .containsEntry("var_samp", 2.5)
                .containsEntry("var_pop", 2.0));
    }

    @Test
    public void testSampleStatisticsOverOneValueAreNull()
            throws IOException
    {
        Aggregations aggregations = parse("{" + statisticalResults(1, "0.0", null) + "}");

        Result result = AggregationResponseReader.read(aggregations, 1, ImmutableList.of(), STATISTICAL);

        Map<String, Object> expected = new HashMap<>();
        expected.put("stddev_samp", null);
        expected.put("var_samp", null);
        expected.put("stddev_pop", 0.0);
        expected.put("var_pop", 0.0);
        assertThat(result.rows()).containsExactly(expected);
    }

    @Test
    public void testSampleStatisticsOverOneValueAreNullEvenWhenResponseCarriesFiniteValues()
            throws IOException
    {
        // the count check must not rely on the response reporting NaN (null) for the sampling values
        Aggregations aggregations = parse("{" + statisticalResults(1, "0.0", "1.5") + "}");

        Result result = AggregationResponseReader.read(aggregations, 1, ImmutableList.of(), STATISTICAL);

        Map<String, Object> expected = new HashMap<>();
        expected.put("stddev_samp", null);
        expected.put("var_samp", null);
        expected.put("stddev_pop", 0.0);
        expected.put("var_pop", 0.0);
        assertThat(result.rows()).containsExactly(expected);
    }

    @Test
    public void testStatisticalOverEmptyInputIsNull()
            throws IOException
    {
        Aggregations aggregations = parse("{" + statisticalResults(0, null, null) + "}");

        Result result = AggregationResponseReader.read(aggregations, 0, ImmutableList.of(), STATISTICAL);

        Map<String, Object> expected = new HashMap<>();
        expected.put("stddev_samp", null);
        expected.put("var_samp", null);
        expected.put("stddev_pop", null);
        expected.put("var_pop", null);
        assertThat(result.rows()).containsExactly(expected);
    }

    @Test
    public void testStatisticalInGroupedAggregation()
            throws IOException
    {
        Aggregations aggregations = parse(
                """
                {
                  "composite#groupBy": {
                    "after_key": {"kind": "b"},
                    "buckets": [
                      {"key": {"kind": "a"}, "doc_count": 5, %s},
                      {"key": {"kind": "b"}, "doc_count": 1, %s}
                    ]
                  }
                }
                """.formatted(statisticalResults(5, "2.0", "2.5"), statisticalResults(1, "0.0", null)));

        Result result = AggregationResponseReader.read(aggregations, 6, ImmutableList.of(new TermAggregation("kind", VARCHAR)), STATISTICAL);

        assertThat(result.rows()).hasSize(2);
        assertThat(result.rows().get(0)).containsEntry("kind", "a").containsEntry("var_samp", 2.5).containsEntry("var_pop", 2.0);
        assertThat(result.rows().get(1)).containsEntry("kind", "b").containsEntry("var_samp", null).containsEntry("var_pop", 0.0);
    }

    /**
     * One extended_stats result per function, named after the function. Population statistics are null for an empty input
     * and sample statistics are null below two values, as OpenSearch reports NaN as null in the response.
     */
    private static String statisticalResults(int count, String variancePopulation, String varianceSampling)
    {
        String stdPopulation = variancePopulation == null ? null : Double.toString(Math.sqrt(Double.parseDouble(variancePopulation)));
        String stdSampling = varianceSampling == null ? null : Double.toString(Math.sqrt(Double.parseDouble(varianceSampling)));
        return Stream.of("stddev_samp", "stddev_pop", "var_samp", "var_pop")
                .map(alias -> """
                              "extended_stats#%s": {"count": %s, "min": 1.0, "max": 5.0, "avg": 3.0, "sum": 15.0, "sum_of_squares": 55.0,
                                "variance": %s, "variance_population": %s, "variance_sampling": %s,
                                "std_deviation": %s, "std_deviation_population": %s, "std_deviation_sampling": %s,
                                "std_deviation_bounds": {"upper": null, "lower": null, "upper_population": null, "lower_population": null, "upper_sampling": null, "lower_sampling": null}}
                              """.formatted(alias, count, variancePopulation, variancePopulation, varianceSampling, stdPopulation, stdPopulation, stdSampling))
                .collect(Collectors.joining(","));
    }

    private static String bigintStats(long count, double min, double max, double sum)
    {
        return "{\"stats#big_sum\": {\"count\": %s, \"min\": %s, \"max\": %s, \"avg\": 0.0, \"sum\": %s}}".formatted(count, min, max, sum);
    }

    private static String bigintSum(double sum)
    {
        return "{\"stats#big_sum\": {\"count\": 1, \"min\": 1.0, \"max\": 1.0, \"avg\": 1.0, \"sum\": %s}}".formatted(sum);
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
