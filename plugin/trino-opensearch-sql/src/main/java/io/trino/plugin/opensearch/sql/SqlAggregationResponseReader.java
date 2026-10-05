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
                verifyCountColumn(result, output.countIndex().orElseThrow());
                long count = ((Number) row.get(output.countIndex().orElseThrow())).longValue();
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
