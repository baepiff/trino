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

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.trino.plugin.opensearch.client.IndexMetadata.Field;
import io.trino.plugin.opensearch.client.IndexMetadata.ObjectType;
import io.trino.plugin.opensearch.client.IndexMetadata.SubField;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.opensearch.client.OpenSearchClient.parseIndexMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class TestIndexMetadataParsing
{
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    private static final SubField KEYWORD_256 = new SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), true, false);

    @Test
    public void testDynamicTextMapping()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {"index": {"mappings": {"properties": {
                    "tenantId": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                    "plain": {"type": "text"},
                    "name": {"type": "keyword"}
                }}}}
                """);

        assertThat(fields).containsExactly(
                new Field(false, false, "tenantId", new IndexMetadata.PrimitiveType("text"), List.of(KEYWORD_256)),
                new Field(false, false, "plain", new IndexMetadata.PrimitiveType("text")),
                new Field(false, false, "name", new IndexMetadata.PrimitiveType("keyword")));
    }

    @Test
    public void testSubFieldParameters()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {"index": {"mappings": {"properties": {
                    "a": {"type": "text", "fields": {
                        "lower": {"type": "keyword", "normalizer": "lowercase"},
                        "unindexed": {"type": "keyword", "index": false},
                        "with_null": {"type": "keyword", "null_value": "NULL"},
                        "english": {"type": "text", "analyzer": "english"},
                        "bad_limit": {"type": "keyword", "ignore_above": "many"},
                        "untyped": {"ignore_above": 10}
                    }}
                }}}}
                """);

        assertThat(fields.getFirst().subFields()).containsExactly(
                new SubField("lower", "keyword", OptionalInt.empty(), Optional.of("lowercase"), true, false),
                new SubField("unindexed", "keyword", OptionalInt.empty(), Optional.empty(), false, false),
                new SubField("with_null", "keyword", OptionalInt.empty(), Optional.empty(), true, true),
                new SubField("english", "text", OptionalInt.empty(), Optional.empty(), true, false));
    }

    @Test
    public void testNestedFieldsDoNotRetainSubFields()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {"index": {"mappings": {"properties": {
                    "owner": {"properties": {
                        "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
                    }}
                }}}}
                """);

        assertThat(((ObjectType) fields.getFirst().type()).fields())
                .containsExactly(new Field(false, false, "name", new IndexMetadata.PrimitiveType("text")));
    }

    @Test
    public void testSubFieldsMustBeConsistentAcrossIndexes()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {
                    "index_1": {"mappings": {"properties": {
                        "same": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "other_limit": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "missing_sub_field": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "missing_field": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "keyword_elsewhere": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
                    }}},
                    "index_2": {"mappings": {"properties": {
                        "same": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "other_limit": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 10}}},
                        "missing_sub_field": {"type": "text"},
                        "keyword_elsewhere": {"type": "keyword", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
                    }}}
                }
                """);

        assertThat(fields).extracting(Field::name, Field::subFields).containsExactly(
                tuple("same", List.of(KEYWORD_256)),
                tuple("other_limit", List.of()),
                tuple("missing_sub_field", List.of()),
                tuple("missing_field", List.of()),
                tuple("keyword_elsewhere", List.of()));
    }

    @Test
    public void testCopyToTargetsDoNotRetainSubFields()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {"index": {"mappings": {"properties": {
                    "source_a": {"type": "keyword", "copy_to": "target"},
                    "owner": {"properties": {"source_b": {"type": "keyword", "copy_to": ["other", "sub_target.keyword"]}}},
                    "target": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                    "sub_target": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                    "untouched": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
                }}}}
                """);

        assertThat(fields).filteredOn(field -> field.type() instanceof IndexMetadata.PrimitiveType primitiveType && primitiveType.name().equals("text"))
                .extracting(Field::name, Field::subFields)
                .containsExactly(
                        tuple("target", List.of()),
                        tuple("sub_target", List.of()),
                        tuple("untouched", List.of(KEYWORD_256)));
    }

    @Test
    public void testEmptyMappings()
            throws IOException
    {
        assertThat(parse("{\"index\": {\"mappings\": {}}}")).isEmpty();
    }

    private static List<Field> parse(@Language("JSON") String response)
            throws IOException
    {
        return parseIndexMetadata(JSON_MAPPER.readTree(response)).schema().fields();
    }
}
