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
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.TestOpenSearchMetadata.integerColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
