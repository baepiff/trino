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

import java.util.List;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

record SqlAggregationQuery(String sql, List<Output> outputs, int sentinelIndex, int columnCount)
{
    SqlAggregationQuery
    {
        requireNonNull(sql, "sql is null");
        outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
    }

    record Output(MetricAggregation aggregation, int valueIndex, OptionalInt countIndex)
    {
        Output
        {
            requireNonNull(aggregation, "aggregation is null");
            requireNonNull(countIndex, "countIndex is null");
        }
    }
}
