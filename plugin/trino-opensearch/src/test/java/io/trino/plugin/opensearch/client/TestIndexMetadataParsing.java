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

    private static final SubField KEYWORD_256 = new SubField("keyword", "keyword", OptionalInt.of(256), Optional.empty(), true, false, true);

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
                new Field(false, false, "tenantId", new IndexMetadata.PrimitiveType("text"), List.of(KEYWORD_256), true),
                new Field(false, false, "plain", new IndexMetadata.PrimitiveType("text"), List.of(), true),
                new Field(false, false, "name", new IndexMetadata.PrimitiveType("keyword"), List.of(), true));
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
                        "untyped": {"ignore_above": 10},
                        "without_doc_values": {"type": "keyword", "doc_values": false}
                    }}
                }}}}
                """);

        assertThat(fields.getFirst().subFields()).containsExactly(
                new SubField("lower", "keyword", OptionalInt.empty(), Optional.of("lowercase"), true, false, true),
                new SubField("unindexed", "keyword", OptionalInt.empty(), Optional.empty(), false, false, true),
                new SubField("with_null", "keyword", OptionalInt.empty(), Optional.empty(), true, true, true),
                new SubField("english", "text", OptionalInt.empty(), Optional.empty(), true, false, true),
                new SubField("without_doc_values", "keyword", OptionalInt.empty(), Optional.empty(), true, false, false));
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

        // nor is the presence of a value determined for them
        assertThat(((ObjectType) fields.getFirst().type()).fields())
                .containsExactly(new Field(false, false, "name", new IndexMetadata.PrimitiveType("text"), List.of(), false));
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
    public void testPresenceIndexed()
            throws IOException
    {
        List<Field> fields = parse(
                """
                {"index": {"mappings": {"properties": {
                    "default": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "not_indexed": {"type": "text", "index": false, "fields": {"keyword": {"type": "keyword"}}},
                    "without_norms": {"type": "text", "norms": false, "fields": {"keyword": {"type": "keyword"}}}
                }}}}
                """);
        // without norms, an exists query reads the _field_names meta field
        assertThat(fields).extracting(Field::name, Field::presenceIndexed).containsExactly(
                tuple("default", true),
                tuple("not_indexed", false),
                tuple("without_norms", true));

        List<Field> withoutFieldNames = parse(
                """
                {"index": {"mappings": {"_field_names": {"enabled": false}, "properties": {
                    "default": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "without_norms": {"type": "text", "norms": false, "fields": {"keyword": {"type": "keyword"}}}
                }}}}
                """);
        assertThat(withoutFieldNames).extracting(Field::name, Field::presenceIndexed).containsExactly(
                tuple("default", true),
                tuple("without_norms", false));

        // every index behind the table must record it
        List<Field> acrossIndexes = parse(
                """
                {
                    "index_1": {"mappings": {"properties": {
                        "same": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "not_indexed_elsewhere": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
                    }}},
                    "index_2": {"mappings": {"_field_names": {"enabled": false}, "properties": {
                        "same": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "not_indexed_elsewhere": {"type": "text", "norms": false, "fields": {"keyword": {"type": "keyword"}}}
                    }}}
                }
                """);
        assertThat(acrossIndexes).extracting(Field::name, Field::presenceIndexed).containsExactly(
                tuple("same", true),
                tuple("not_indexed_elsewhere", false));
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
