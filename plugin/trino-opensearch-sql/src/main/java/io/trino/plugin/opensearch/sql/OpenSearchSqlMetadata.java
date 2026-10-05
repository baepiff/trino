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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchMetadata;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.MetricAggregation.SQL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.STATISTICAL_FUNCTIONS;
import static io.trino.plugin.opensearch.MetricAggregation.canonicalFunctionName;
import static io.trino.plugin.opensearch.OpenSearchSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.plugin.opensearch.sql.OpenSearchSqlSessionProperties.globalAggregationEngine;

public class OpenSearchSqlMetadata
        extends OpenSearchMetadata
{
    @Inject
    public OpenSearchSqlMetadata(TypeManager typeManager, OpenSearchClient client, OpenSearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> sqlResult = applySqlAggregation(session, (OpenSearchTableHandle) table, aggregates, assignments, groupingSets);
        if (sqlResult.isPresent()) {
            return sqlResult;
        }
        return super.applyAggregation(session, table, aggregates, assignments, groupingSets);
    }

    private static Optional<AggregationApplicationResult<ConnectorTableHandle>> applySqlAggregation(
            ConnectorSession session,
            OpenSearchTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }
        if (handle.type() != SCAN || handle.topN().isPresent() || handle.query().isPresent() || !handle.regexes().isEmpty()) {
            return Optional.empty();
        }
        // only global aggregates: the SQL plugin silently truncates GROUP BY results at 1,000 groups
        if (groupingSets.size() != 1 || !groupingSets.getFirst().isEmpty() || aggregates.isEmpty()) {
            return Optional.empty();
        }

        boolean hasStatisticalFunction = aggregates.stream()
                .anyMatch(aggregate -> STATISTICAL_FUNCTIONS.contains(canonicalFunctionName(aggregate.getFunctionName())));
        if (globalAggregationEngine(session) == GlobalAggregationEngine.DSL && !hasStatisticalFunction) {
            return Optional.empty();
        }

        Optional<String> whereClause = SqlWhereRenderer.render(handle.constraint().transformKeys(OpenSearchColumnHandle.class::cast));
        if (whereClause.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<MetricAggregation> metricAggregations = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index);
            String name = SYNTHETIC_COLUMN_NAME_PREFIX + index;

            Optional<MetricAggregation> metricAggregation = MetricAggregation.from(function, assignments, name, SQL_FUNCTIONS);
            Optional<OpenSearchColumnHandle> outputColumn = aggregationOutputColumn(name, function.getOutputType());
            if (metricAggregation.isEmpty() || outputColumn.isEmpty()) {
                return Optional.empty();
            }
            metricAggregations.add(metricAggregation.get());
            projections.add(new Variable(name, function.getOutputType()));
            resultAssignments.add(new Assignment(name, outputColumn.get(), function.getOutputType()));
        }

        List<MetricAggregation> aggregations = metricAggregations.build();
        // the final check proves that the whole statement can be generated
        if (SqlAggregationQueryBuilder.build(handle.index(), aggregations, whereClause.get()).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new AggregationApplicationResult<>(
                handle.withSqlAggregations(aggregations),
                projections.build(),
                resultAssignments.build(),
                ImmutableMap.of(),
                false));
    }
}
