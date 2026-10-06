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
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public final class OpenSearchSqlSessionProperties
        implements SessionPropertiesProvider
{
    public static final String GLOBAL_AGGREGATION_ENGINE = "global_aggregation_engine";
    public static final String STATISTICAL_PUSHDOWN_ENABLED = "statistical_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OpenSearchSqlSessionProperties(OpenSearchSqlConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        GLOBAL_AGGREGATION_ENGINE,
                        "Engine for global aggregations: SQL or DSL",
                        GlobalAggregationEngine.class,
                        config.getGlobalAggregationEngine(),
                        false))
                .add(booleanProperty(
                        STATISTICAL_PUSHDOWN_ENABLED,
                        "Push down stddev, variance and related functions to the OpenSearch SQL plugin, which can lose precision for large values with a small spread",
                        config.isStatisticalPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static GlobalAggregationEngine globalAggregationEngine(ConnectorSession session)
    {
        return session.getProperty(GLOBAL_AGGREGATION_ENGINE, GlobalAggregationEngine.class);
    }

    public static boolean isStatisticalPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICAL_PUSHDOWN_ENABLED, Boolean.class);
    }
}
