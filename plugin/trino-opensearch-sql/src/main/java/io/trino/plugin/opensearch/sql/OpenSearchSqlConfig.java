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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class OpenSearchSqlConfig
{
    public enum GlobalAggregationEngine
    {
        SQL,
        DSL,
    }

    private GlobalAggregationEngine globalAggregationEngine = GlobalAggregationEngine.SQL;
    private boolean statisticalPushdownEnabled;
    private boolean bigintAggregationPushdownEnabled;

    @NotNull
    public GlobalAggregationEngine getGlobalAggregationEngine()
    {
        return globalAggregationEngine;
    }

    @Config("opensearch.sql.global-aggregation-engine")
    @ConfigDescription("Engine for global aggregations: SQL pushes them through the OpenSearch SQL plugin; DSL uses search aggregations and uses SQL only for statistical functions when statistical pushdown is enabled")
    public OpenSearchSqlConfig setGlobalAggregationEngine(GlobalAggregationEngine globalAggregationEngine)
    {
        this.globalAggregationEngine = globalAggregationEngine;
        return this;
    }

    public boolean isBigintAggregationPushdownEnabled()
    {
        return bigintAggregationPushdownEnabled;
    }

    @Config("opensearch.sql.bigint-aggregation-pushdown-enabled")
    @ConfigDescription("Push down min, max, sum and avg over BIGINT columns to the OpenSearch SQL plugin. The plugin computes them with double precision, which is exact only up to 2^53, so a min, max or sum result with a magnitude of 2^53 or more fails the query instead of returning a possibly inexact value. Only applies to the SQL engine")
    public OpenSearchSqlConfig setBigintAggregationPushdownEnabled(boolean bigintAggregationPushdownEnabled)
    {
        this.bigintAggregationPushdownEnabled = bigintAggregationPushdownEnabled;
        return this;
    }

    public boolean isStatisticalPushdownEnabled()
    {
        return statisticalPushdownEnabled;
    }

    @Config("opensearch.sql.statistical-pushdown-enabled")
    @ConfigDescription("Push down stddev, stddev_samp, stddev_pop, variance, var_samp and var_pop to the OpenSearch SQL plugin. The plugin computes them from the sum of squares and loses precision when the values are large compared to their spread, so they are computed by Trino unless this is enabled")
    public OpenSearchSqlConfig setStatisticalPushdownEnabled(boolean statisticalPushdownEnabled)
    {
        this.statisticalPushdownEnabled = statisticalPushdownEnabled;
        return this;
    }
}
