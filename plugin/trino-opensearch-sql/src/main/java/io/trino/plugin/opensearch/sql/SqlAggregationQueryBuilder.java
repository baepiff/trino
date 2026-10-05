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
import io.trino.plugin.opensearch.sql.SqlAggregationQuery.Output;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;

final class SqlAggregationQueryBuilder
{
    private SqlAggregationQueryBuilder() {}

    static Optional<SqlAggregationQuery> build(String index, List<MetricAggregation> aggregations, String whereClause)
    {
        if (!SqlIdentifiers.isQuotable(index)) {
            return Optional.empty();
        }

        List<String> selectItems = new ArrayList<>();
        List<Output> outputs = new ArrayList<>();
        for (MetricAggregation aggregation : aggregations) {
            if (aggregation.columnHandle().isEmpty()) {
                // count(*)
                outputs.add(new Output(aggregation, selectItems.size(), OptionalInt.empty()));
                selectItems.add("count(*)");
                continue;
            }

            OpenSearchColumnHandle column = aggregation.columnHandle().orElseThrow();
            if (column.path().size() != 1 || !SqlIdentifiers.isQuotable(column.name())) {
                return Optional.empty();
            }
            String quoted = SqlIdentifiers.quote(column.name());

            int valueIndex = selectItems.size();
            selectItems.add(aggregation.functionName() + "(" + quoted + ")");

            OptionalInt countIndex = OptionalInt.empty();
            if (aggregation.functionName().equals(SUM) || STATISTICAL_FUNCTIONS.contains(aggregation.functionName())) {
                // the plugin returns 0 for sum() over no rows, and NULL for population statistics over one row
                countIndex = OptionalInt.of(selectItems.size());
                selectItems.add("count(" + quoted + ")");
            }
            outputs.add(new Output(aggregation, valueIndex, countIndex));
        }

        // the sentinel proves the V2 engine answered: the legacy engine returns count(*) as a double
        int sentinelIndex = selectItems.size();
        selectItems.add("count(*)");

        String sql = "SELECT " + String.join(", ", selectItems) + " FROM " + SqlIdentifiers.quote(index);
        if (!whereClause.isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return Optional.of(new SqlAggregationQuery(sql, outputs, sentinelIndex, selectItems.size()));
    }
}
