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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenSearchSqlClientParsing
{
    @Test
    public void testParseAggregateResponse()
    {
        SqlResult result = OpenSearchSqlClient.parse(
                """
                {
                  "schema": [
                    {"name": "count(*)", "type": "long"},
                    {"name": "sum(version)", "alias": "s", "type": "long"},
                    {"name": "avg(version)", "type": "double"},
                    {"name": "max(flag)", "type": "boolean"},
                    {"name": "max(name)", "type": "keyword"}
                  ],
                  "datarows": [[2150451, 5155344, 6.190872704696464, true, null]],
                  "total": 1,
                  "size": 1,
                  "status": 200
                }
                """);

        assertThat(result.schema()).containsExactly(
                new SqlColumn("count(*)", "long"),
                new SqlColumn("s", "long"),
                new SqlColumn("avg(version)", "double"),
                new SqlColumn("max(flag)", "boolean"),
                new SqlColumn("max(name)", "keyword"));
        assertThat(result.rows()).containsExactly(Arrays.asList(2150451L, 5155344L, 6.190872704696464, true, null));
    }

    @Test
    public void testLargeIntegersKeepPrecision()
    {
        SqlResult result = OpenSearchSqlClient.parse(
                """
                {"schema":[{"name":"x","type":"long"}],"datarows":[[9007199254740993]],"status":200}""");

        assertThat(result.rows().getFirst().getFirst()).isEqualTo(9007199254740993L);
    }

    @Test
    public void testEmptyRows()
    {
        SqlResult result = OpenSearchSqlClient.parse(
                """
                {"schema":[{"name":"x","type":"long"}],"datarows":[],"total":0,"size":0,"status":200}""");

        assertThat(result.rows()).isEmpty();
    }

    @Test
    public void testErrorPayload()
    {
        assertThatThrownBy(() -> OpenSearchSqlClient.parse(
                """
                {"error":{"reason":"Invalid SQL query","details":"boom","type":"SemanticCheckException"},"status":400}"""))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()))
                .hasMessageContaining("Invalid SQL query")
                .hasMessageContaining("boom");
    }

    @Test
    public void testMalformedPayloads()
    {
        for (String body : List.of("", "not json", "{}", "{\"schema\":[]}", "{\"datarows\":[]}", "{\"schema\":{},\"datarows\":[]}")) {
            assertThatThrownBy(() -> OpenSearchSqlClient.parse(body))
                    .as(body)
                    .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_INVALID_RESPONSE.toErrorCode()));
        }
    }
}
