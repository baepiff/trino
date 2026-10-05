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
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchPageSourceProvider;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.type.TypeManager;

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

    OpenSearchSqlClient sqlClient()
    {
        return sqlClient;
    }
}
