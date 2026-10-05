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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opensearch.sql.OpenSearchSqlConfig.GlobalAggregationEngine;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenSearchSqlPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new OpenSearchSqlPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThat(factory.getName()).isEqualTo("opensearch-sql");
        // building the connector exercises the Guice wiring (base module + SQL overrides) without needing a server
        Connector connector = factory.create("test", ImmutableMap.of("opensearch.host", "localhost"), new TestingConnectorContext());
        try {
            assertThat(connector).isInstanceOf(OpenSearchSqlConnector.class);
            assertThat(connector.getMetadata(SESSION, connector.beginTransaction(READ_COMMITTED, true, true))).isInstanceOf(OpenSearchSqlMetadata.class);
            assertThat(connector.getPageSourceProvider()).isInstanceOf(OpenSearchSqlPageSourceProvider.class);

            assertThat(connector.getSessionProperties()).extracting(PropertyMetadata::getName)
                    .contains("aggregation_pushdown_enabled", "global_aggregation_engine");
            PropertyMetadata<?> engine = connector.getSessionProperties().stream()
                    .filter(property -> property.getName().equals("global_aggregation_engine"))
                    .collect(onlyElement());
            assertThat(engine.getDefaultValue()).isEqualTo(GlobalAggregationEngine.SQL);
        }
        finally {
            connector.shutdown();
        }
    }
}
