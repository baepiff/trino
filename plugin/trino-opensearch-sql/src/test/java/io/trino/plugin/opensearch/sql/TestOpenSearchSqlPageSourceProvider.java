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
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenSearchSqlPageSourceProvider
{
    @Test
    public void testSqlAggregationHandlesAreRoutedToTheSqlPageSource()
            throws IOException
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        OpenSearchClient client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        try {
            OpenSearchSqlPageSourceProvider provider = new OpenSearchSqlPageSourceProvider(client, TESTING_TYPE_MANAGER, config, new OpenSearchSqlClient(client));
            OpenSearchTableHandle table = new OpenSearchTableHandle(SCAN, "default", "metric_logs", Optional.empty())
                    .withSqlAggregations(List.of(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0")));
            OpenSearchColumnHandle count = new OpenSearchColumnHandle(List.of("_pushdown_0"), BIGINT, new PrimitiveType("long"), new BigintDecoder.Descriptor("_pushdown_0"), false);

            // the split is not used for SQL aggregations, which are answered by a single statement
            try (ConnectorPageSource pageSource = provider.createPageSource(null, SESSION, null, table, Optional.empty(), List.of(count), DynamicFilter.EMPTY)) {
                assertThat(pageSource).isInstanceOf(SqlAggregatePageSource.class);
                assertThat(pageSource.isFinished()).isFalse();
            }
        }
        finally {
            client.close();
        }
    }
}
