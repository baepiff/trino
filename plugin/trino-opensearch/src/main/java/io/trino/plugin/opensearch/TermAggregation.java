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
package io.trino.plugin.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.opensearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.plugin.opensearch.PushdownColumns.isDocValuesPushdownSupported;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * @param term the grouping column, which also names the composite source and the key in the buckets
 * @param subField the {@code keyword} sub-field that is grouped on instead of a {@code text} column, see
 *         {@link #fromKeywordSubField}
 */
public record TermAggregation(String term, Type type, Optional<String> subField)
{
    @JsonCreator
    public TermAggregation
    {
        requireNonNull(term, "term is null");
        requireNonNull(type, "type is null");
        // absent in handles serialized before text columns could be grouped on
        if (subField == null) {
            subField = Optional.empty();
        }
    }

    public TermAggregation(String term, Type type)
    {
        this(term, type, Optional.empty());
    }

    /**
     * The field whose values OpenSearch groups by.
     */
    @JsonIgnore
    public String field()
    {
        return subField.map(name -> term + "." + name).orElse(term);
    }

    public static Optional<TermAggregation> fromColumn(OpenSearchColumnHandle column)
    {
        if (!isDocValuesPushdownSupported(column) || !isSupportedGroupingType(column.type())) {
            return Optional.empty();
        }
        return Optional.of(new TermAggregation(column.name(), column.type()));
    }

    /**
     * Groups a {@code text} column by its {@code keyword} sub-field. The groups are the same as Trino's only if the
     * sub-field indexes the value of every document that has one: a value longer than the {@code ignore_above} of the
     * sub-field, or indexed before the sub-field was added to the mapping, is missing from the sub-field and would be
     * counted in the NULL group. The mapping cannot tell, so the page source counts such documents with an
     * {@code exists} query on the column before it returns any group and fails the query if there are any, which
     * requires the presence of a value to be indexed for the column.
     */
    public static Optional<TermAggregation> fromKeywordSubField(OpenSearchColumnHandle column)
    {
        if (column.keywordSubField().isEmpty()
                || !column.presenceIndexed()
                || isBuiltinColumn(column.name())
                || !column.type().equals(VARCHAR)) {
            return Optional.empty();
        }
        return Optional.of(new TermAggregation(column.name(), column.type(), Optional.of(column.keywordSubField().get().name())));
    }

    private static boolean isSupportedGroupingType(Type type)
    {
        return type instanceof VarcharType
                || type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(BOOLEAN);
    }
}
