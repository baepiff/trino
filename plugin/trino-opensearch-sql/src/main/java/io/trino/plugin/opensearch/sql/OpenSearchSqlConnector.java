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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.opensearch.NodesSystemTable;
import io.trino.plugin.opensearch.OpenSearchConnector;
import io.trino.plugin.opensearch.OpenSearchSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Set;

public class OpenSearchSqlConnector
        extends OpenSearchConnector
{
    @Inject
    public OpenSearchSqlConnector(
            LifeCycleManager lifeCycleManager,
            OpenSearchSqlMetadata metadata,
            OpenSearchSplitManager splitManager,
            OpenSearchSqlPageSourceProvider pageSourceProvider,
            NodesSystemTable nodesSystemTable,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionPropertiesProviders)
    {
        super(lifeCycleManager, metadata, splitManager, pageSourceProvider, nodesSystemTable, connectorTableFunctions, sessionPropertiesProviders);
    }
}
