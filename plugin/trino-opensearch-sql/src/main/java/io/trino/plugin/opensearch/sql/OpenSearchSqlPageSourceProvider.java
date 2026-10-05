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
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchPageSourceProvider;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SQL_AGGREGATION;
import static java.util.Objects.requireNonNull;

public class OpenSearchSqlPageSourceProvider
        extends OpenSearchPageSourceProvider
{
    private final OpenSearchSqlClient sqlClient;

    @Inject
    public OpenSearchSqlPageSourceProvider(OpenSearchClient client, TypeManager typeManager, OpenSearchConfig config, OpenSearchSqlClient sqlClient)
    {
        super(client, typeManager, config);
        this.sqlClient = requireNonNull(sqlClient, "sqlClient is null");
    }

    @Override
    @SuppressWarnings("deprecation") // TODO (https://github.com/trinodb/trino/issues/29959) migrate together with the base class to the non-deprecated createPageSource overload
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (table instanceof OpenSearchTableHandle handle && handle.type() == SQL_AGGREGATION) {
            return new SqlAggregatePageSource(
                    sqlClient,
                    handle,
                    columns.stream()
                            .map(OpenSearchColumnHandle.class::cast)
                            .collect(toImmutableList()));
        }
        return super.createPageSource(transaction, session, split, table, tableCredentials, columns, dynamicFilter);
    }
}
