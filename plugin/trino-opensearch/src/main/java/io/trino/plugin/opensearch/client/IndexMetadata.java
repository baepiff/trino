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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record IndexMetadata(ObjectType schema)
{
    public IndexMetadata
    {
        requireNonNull(schema, "schema is null");
    }

    /**
     * @param subFields the multi-fields declared under {@code fields} in the mapping; only retained for
     *         top-level fields, and only when every index behind the table declares them identically
     * @param presenceIndexed whether an {@code exists} query on the {@code text} field matches every document with a
     *         value for it, including an empty string, in every index behind the table: the field is indexed, and
     *         either keeps its norms or the index keeps the {@code _field_names} meta field; only determined for
     *         top-level fields, and only meaningful for {@code text} fields
     */
    public record Field(boolean asRawJson, boolean isArray, String name, Type type, List<SubField> subFields, boolean presenceIndexed)
    {
        @JsonCreator
        public Field
        {
            checkArgument(
                    !asRawJson || !isArray,
                    "A column, (%s) cannot be declared as a Trino array and also be rendered as json.",
                    name);
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            // absent in handles serialized before sub-fields were retained
            subFields = subFields == null ? ImmutableList.of() : ImmutableList.copyOf(subFields);
        }

        public Field(boolean asRawJson, boolean isArray, String name, Type type, List<SubField> subFields)
        {
            this(asRawJson, isArray, name, type, subFields, true);
        }

        public Field(boolean asRawJson, boolean isArray, String name, Type type)
        {
            this(asRawJson, isArray, name, type, ImmutableList.of());
        }
    }

    /**
     * A multi-field of a mapped field, for example the {@code keyword} sub-field that dynamic mapping adds to
     * every {@code text} field. Only the mapping parameters that affect which terms are indexed are kept.
     */
    public record SubField(String name, String type, OptionalInt ignoreAbove, Optional<String> normalizer, boolean indexed, boolean hasNullValue)
    {
        @JsonCreator
        public SubField
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            requireNonNull(ignoreAbove, "ignoreAbove is null");
            requireNonNull(normalizer, "normalizer is null");
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = DateTimeType.class, name = "date_time_type"),
            @JsonSubTypes.Type(value = ObjectType.class, name = "object_type"),
            @JsonSubTypes.Type(value = PrimitiveType.class, name = "primitive_type"),
            @JsonSubTypes.Type(value = ScaledFloatType.class, name = "scaled_float_type"),
    })
    public interface Type {}

    public record PrimitiveType(String name)
            implements Type
    {
        public PrimitiveType
        {
            requireNonNull(name, "name is null");
        }
    }

    public record DateTimeType(List<String> formats)
            implements Type
    {
        public DateTimeType
        {
            requireNonNull(formats, "formats is null");
            formats = ImmutableList.copyOf(formats);
        }
    }

    public record ObjectType(List<Field> fields)
            implements Type
    {
        public ObjectType
        {
            requireNonNull(fields, "fields is null");
            fields = ImmutableList.copyOf(fields);
        }
    }

    public record ScaledFloatType(double scale)
            implements Type {}
}
