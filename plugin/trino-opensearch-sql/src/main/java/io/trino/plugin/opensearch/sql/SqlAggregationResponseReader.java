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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.sql.SqlAggregationQuery.Output;
import io.trino.spi.TrinoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.opensearch.MetricAggregation.COUNT;
import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.STDDEV_POP;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;
import static io.trino.plugin.opensearch.MetricAggregation.VAR_POP;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static java.lang.String.format;

final class SqlAggregationResponseReader
{
    // the V2 engine reports count as integer (observed on OpenSearch 2.19) or long, the legacy engine as double
    private static final Set<String> COUNT_TYPES = ImmutableSet.of("integer", "long");

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

        countValue(result, query.sentinelIndex(), row.get(query.sentinelIndex()));
        Map<String, Object> values = new HashMap<>();
        for (Output output : query.outputs()) {
            MetricAggregation aggregation = output.aggregation();
            Object value = row.get(output.valueIndex());
            String function = aggregation.functionName();

            if (aggregation.columnHandle().isEmpty() || function.equals(COUNT)) {
                values.put(aggregation.alias(), countValue(result, output.valueIndex(), value));
                continue;
            }

            if (output.countIndex().isPresent()) {
                int countIndex = output.countIndex().orElseThrow();
                long count = countValue(result, countIndex, row.get(countIndex));
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
        if (STATISTICAL_FUNCTIONS.contains(function) && count == 1) {
            // population statistics of a single value are 0, sample statistics are undefined
            if (function.equals(STDDEV_POP) || function.equals(VAR_POP)) {
                return 0.0;
            }
            return null;
        }
        return value;
    }

    private static long countValue(SqlResult result, int index, Object value)
    {
        String type = result.schema().get(index).type();
        if (!COUNT_TYPES.contains(type)) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format(
                    "OpenSearch SQL returned type '%s' for a count column, expected 'integer' or 'long'. The query was probably answered by the legacy engine. "
                            + "Set opensearch.sql.global-aggregation-engine=DSL to avoid the SQL path.",
                    type));
        }
        if (!(value instanceof Number number)) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format("OpenSearch SQL returned %s for a count column, expected a number", value == null ? "NULL" : "a non-numeric value"));
        }
        long count = number.longValue();
        // The V2 engine types count as a 32-bit integer on OpenSearch 2.19, so a larger count may have been capped or wrapped without notice
        if (type.equals("integer") && (count < 0 || count >= Integer.MAX_VALUE)) {
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, format(
                    "OpenSearch SQL returned %s for an integer-typed count column, the count may have overflowed 32 bits. "
                            + "Set opensearch.sql.global-aggregation-engine=DSL or use the opensearch connector to get an exact count.",
                    count));
        }
        return count;
    }
}
