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

import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_CONNECTION_ERROR;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenSearchSqlClientRetry
{
    private static final String COLD_START_FAILURE = "OpenSearch SQL request failed: HTTP 400: Invalid SQL query: can't evaluate on aggregator: min";
    private static final String SUCCESS = "{\"schema\":[{\"name\":\"count(*)\",\"type\":\"integer\"}],\"datarows\":[[3]]}";

    @Test
    public void testColdStartErrorIsRetriedOnce()
            throws IOException
    {
        withClient(client -> {
            ScriptedSqlClient sqlClient = new ScriptedSqlClient(client, false, new TrinoException(OPENSEARCH_QUERY_FAILURE, COLD_START_FAILURE), SUCCESS);

            SqlResult result = sqlClient.execute("SELECT min(x) FROM t");

            assertThat(result.rows()).containsExactly(List.of(3L));
            assertThat(sqlClient.calls()).isEqualTo(2);
            assertThat(sqlClient.pauses()).isEqualTo(1);
        });
    }

    @Test
    public void testPersistentColdStartErrorSurfaces()
            throws IOException
    {
        withClient(client -> {
            TrinoException first = new TrinoException(OPENSEARCH_QUERY_FAILURE, COLD_START_FAILURE + " (first)");
            TrinoException second = new TrinoException(OPENSEARCH_QUERY_FAILURE, COLD_START_FAILURE + " (second)");
            ScriptedSqlClient sqlClient = new ScriptedSqlClient(client, false, first, second);

            assertThatThrownBy(() -> sqlClient.execute("SELECT min(x) FROM t"))
                    .isSameAs(second)
                    .hasSuppressedException(first);
            assertThat(sqlClient.calls()).isEqualTo(2);
        });
    }

    @Test
    public void testOtherErrorsAreNotRetried()
            throws IOException
    {
        withClient(client -> {
            ScriptedSqlClient otherQueryFailure = new ScriptedSqlClient(
                    client,
                    false,
                    new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL request failed: HTTP 400: Invalid SQL query"),
                    SUCCESS);
            assertThatThrownBy(() -> otherQueryFailure.execute("SELECT min(x) FROM t"))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("Invalid SQL query");
            assertThat(otherQueryFailure.calls()).isEqualTo(1);
            assertThat(otherQueryFailure.pauses()).isEqualTo(0);

            // the message alone is not enough, the error code must be the query failure
            ScriptedSqlClient otherErrorCode = new ScriptedSqlClient(client, false, new TrinoException(OPENSEARCH_CONNECTION_ERROR, COLD_START_FAILURE), SUCCESS);
            assertThatThrownBy(() -> otherErrorCode.execute("SELECT min(x) FROM t"))
                    .isInstanceOf(TrinoException.class);
            assertThat(otherErrorCode.calls()).isEqualTo(1);
        });
    }

    @Test
    public void testInterruptDuringPauseRestoresTheInterruptFlag()
            throws IOException
    {
        withClient(client -> {
            ScriptedSqlClient sqlClient = new ScriptedSqlClient(client, true, new TrinoException(OPENSEARCH_QUERY_FAILURE, COLD_START_FAILURE), SUCCESS);
            Thread.currentThread().interrupt();
            try {
                // the interrupt flag makes the real pause fail immediately
                assertThatThrownBy(() -> sqlClient.execute("SELECT min(x) FROM t"))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageContaining("Interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(sqlClient.calls()).isEqualTo(1);
            }
            finally {
                // clear the flag so it does not leak into other tests
                Thread.interrupted();
            }
        });
    }

    private interface ClientConsumer
    {
        void accept(OpenSearchClient client);
    }

    private static void withClient(ClientConsumer consumer)
            throws IOException
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        OpenSearchClient client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        try {
            consumer.accept(client);
        }
        finally {
            client.close();
        }
    }

    private static class ScriptedSqlClient
            extends OpenSearchSqlClient
    {
        private final boolean realPause;
        private final List<Object> script;
        private final List<String> requests = new ArrayList<>();
        private int pauses;

        ScriptedSqlClient(OpenSearchClient client, boolean realPause, Object... script)
        {
            super(client);
            this.realPause = realPause;
            this.script = new ArrayList<>(List.of(script));
        }

        int calls()
        {
            return requests.size();
        }

        int pauses()
        {
            return pauses;
        }

        @Override
        String send(String requestBody)
        {
            requests.add(requestBody);
            Object next = script.removeFirst();
            if (next instanceof TrinoException failure) {
                throw failure;
            }
            return (String) next;
        }

        @Override
        void pauseBeforeRetry()
        {
            pauses++;
            if (realPause) {
                super.pauseBeforeRetry();
            }
        }
    }
}
