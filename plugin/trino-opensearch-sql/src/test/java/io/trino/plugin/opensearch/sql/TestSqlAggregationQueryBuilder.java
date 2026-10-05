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

import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.spi.type.Type;
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

    private static MetricAggregation aggregation(String function, Type outputType, OpenSearchColumnHandle column, int index)
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
