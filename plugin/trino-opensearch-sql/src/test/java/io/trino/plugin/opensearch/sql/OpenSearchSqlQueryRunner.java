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
import com.google.common.net.HostAndPort;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OpenSearchSqlQueryRunner
{
    private OpenSearchSqlQueryRunner() {}

    public static DistributedQueryRunner create(HostAndPort address, Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_sql")
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            Map<String, String> properties = new HashMap<>(ImmutableMap.<String, String>builder()
                    .put("opensearch.host", address.getHost())
                    .put("opensearch.port", Integer.toString(address.getPort()))
                    // node discovery relies on the publish address, which is wrong behind a Docker port mapping
                    .put("opensearch.ignore-publish-address", "true")
                    .put("opensearch.default-schema-name", "default")
                    .put("opensearch.request-timeout", "2m")
                    .buildOrThrow());
            properties.putAll(extraProperties);

            queryRunner.installPlugin(new OpenSearchSqlPlugin());
            queryRunner.createCatalog("opensearch_sql", "opensearch_sql", properties);
            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }
}
