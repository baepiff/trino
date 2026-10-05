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

    // columns: count(*) (also the sentinel), sum, count, min, avg, stddev_pop, count, var_samp
    private static final List<String> TYPES = List.of("long", "long", "long", "integer", "double", "double", "long", "double");

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
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 10L, 55L, 10L, 1, 5.5, 2.5, 10L, 7.0));

        assertThat(values).containsEntry("a0", 10L).containsEntry("a1", 55L).containsEntry("a2", 1).containsEntry("a3", 5.5)
                .containsEntry("a4", 2.5).containsEntry("a5", 7.0);
    }

    @Test
    public void testIntegerCountColumnsAreAccepted()
    {
        // OpenSearch 2.19 reports count as integer
        List<String> integerCountTypes = List.of("integer", "long", "integer", "integer", "double", "double", "integer", "double");

        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(integerCountTypes, 10, 55L, 10, 1, 5.5, 2.5, 10, 7.0));

        assertThat(values).containsEntry("a0", 10L).containsEntry("a1", 55L);
    }

    @Test
    public void testEmptyInput()
    {
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 0L, 0L, 0L, null, null, null, 0L, null));

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
        // population statistics over one row are NULL in the SQL plugin but 0.0 in Trino, sample statistics stay NULL
        Map<String, Object> values = SqlAggregationResponseReader.read(QUERY, result(TYPES, 1L, 4L, 1L, 4, 4.0, null, 1L, null));

        assertThat(values.get("a4")).isEqualTo(0.0);
        assertThat(values.get("a5")).isNull();
    }

    @Test
    public void testLegacyEngineFallbackIsDetected()
    {
        List<String> legacyTypes = new ArrayList<>(TYPES);
        legacyTypes.set(0, "double");

        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, result(legacyTypes, 10.0, 55L, 10L, 1, 5.5, 2.5, 10L, 7.0)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("legacy engine")
                .hasMessageContaining("opensearch.sql.global-aggregation-engine");
    }

    @Test
    public void testCompanionCountTypeIsChecked()
    {
        List<String> badTypes = new ArrayList<>(TYPES);
        badTypes.set(2, "double");

        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, result(badTypes, 10L, 55L, 10.0, 1, 5.5, 2.5, 10L, 7.0)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("legacy engine");
    }

    @Test
    public void testShapeMismatch()
    {
        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, new SqlResult(List.of(new SqlColumn("c0", "long")), List.of(List.of(1L)))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("expected 8 columns");

        SqlResult noRows = new SqlResult(result(TYPES, 1L, 1L, 1L, 1, 1.0, 1.0, 1L, 1.0, 1L, 1L).schema(), List.of());
        assertThatThrownBy(() -> SqlAggregationResponseReader.read(QUERY, noRows))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("expected one row");
    }
}
