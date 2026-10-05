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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenSearchSqlClient
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private final OpenSearchClient client;

    @Inject
    public OpenSearchSqlClient(OpenSearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    SqlResult execute(String sql)
    {
        String requestBody;
        try {
            requestBody = JSON_MAPPER.writeValueAsString(ImmutableMap.of("query", sql));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encode the OpenSearch SQL request", e);
        }
        return parse(client.executeSql(requestBody));
    }

    static SqlResult parse(String body)
    {
        JsonNode root;
        try {
            root = JSON_MAPPER.readTree(body);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned a response that is not JSON", e);
        }
        if (root == null || !root.isObject()) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned an empty or non-object response");
        }

        JsonNode error = root.get("error");
        if (error != null) {
            String reason = error.path("reason").asText("unknown error");
            String details = error.path("details").asText("");
            if (details.isEmpty()) {
                throw new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL error: " + reason);
            }
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL error: " + reason + ": " + details);
        }

        JsonNode schemaNode = root.get("schema");
        JsonNode rowsNode = root.get("datarows");
        if (schemaNode == null || !schemaNode.isArray() || rowsNode == null || !rowsNode.isArray()) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL response does not contain a schema and datarows");
        }

        ImmutableList.Builder<SqlColumn> schema = ImmutableList.builder();
        for (JsonNode column : schemaNode) {
            String name = column.path("name").asText();
            if (column.hasNonNull("alias")) {
                name = column.get("alias").asText();
            }
            schema.add(new SqlColumn(name, column.path("type").asText()));
        }

        List<List<Object>> rows = new ArrayList<>();
        for (JsonNode rowNode : rowsNode) {
            if (!rowNode.isArray()) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, "OpenSearch SQL returned a data row that is not an array");
            }
            List<Object> row = new ArrayList<>();
            for (JsonNode cell : rowNode) {
                row.add(cell(cell));
            }
            rows.add(row);
        }
        return new SqlResult(schema.build(), rows);
    }

    private static Object cell(JsonNode node)
    {
        if (node.isNull()) {
            return null;
        }
        if (node.isIntegralNumber()) {
            if (node.canConvertToLong()) {
                return node.longValue();
            }
            return node.bigIntegerValue();
        }
        if (node.isFloatingPointNumber()) {
            return node.doubleValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        return node.toString();
    }
}
