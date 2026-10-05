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
package io.trino.plugin.opensearch.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlErrorMessage
{
    @Test
    public void testPluginErrorBody()
    {
        assertThat(OpenSearchClient.formatSqlError(400,
                """
                {"error":{"reason":"Invalid SQL query","details":"can't resolve Symbol(namespace=FIELD_NAME, name=x) in type env","type":"SemanticCheckException"},"status":400}"""))
                .isEqualTo("HTTP 400: Invalid SQL query: can't resolve Symbol(namespace=FIELD_NAME, name=x) in type env");
    }

    @Test
    public void testReasonWithoutDetails()
    {
        assertThat(OpenSearchClient.formatSqlError(500, "{\"error\":{\"reason\":\"boom\"},\"status\":500}"))
                .isEqualTo("HTTP 500: boom");
    }

    @Test
    public void testUnparseableBody()
    {
        assertThat(OpenSearchClient.formatSqlError(503, "not json")).isEqualTo("HTTP 503");
        assertThat(OpenSearchClient.formatSqlError(502, "")).isEqualTo("HTTP 502");
    }
}
