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

import io.airlift.slice.Slice;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

final class SqlWhereRenderer
{
    // doubles beyond this magnitude are not rendered: very large plain decimals are parsed unreliably by the server
    private static final double MAX_RENDERED_DOUBLE = 9.0e15;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");

    private SqlWhereRenderer() {}

    /**
     * Renders the constraint as the text of a SQL predicate. An empty string means "no predicate";
     * an empty Optional means the constraint cannot be rendered exactly.
     */
    static Optional<String> render(TupleDomain<OpenSearchColumnHandle> constraint)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        List<Map.Entry<OpenSearchColumnHandle, Domain>> entries = constraint.getDomains().orElseThrow().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(OpenSearchColumnHandle::name)))
                .collect(toImmutableList());

        List<String> conjuncts = new ArrayList<>();
        for (Map.Entry<OpenSearchColumnHandle, Domain> entry : entries) {
            Optional<String> predicate = renderDomain(entry.getKey(), entry.getValue());
            if (predicate.isEmpty()) {
                return Optional.empty();
            }
            if (!predicate.get().isEmpty()) {
                conjuncts.add(predicate.get());
            }
        }
        return Optional.of(String.join(" AND ", conjuncts));
    }

    private static Optional<String> renderDomain(OpenSearchColumnHandle column, Domain domain)
    {
        // builtin columns, nested columns and columns unsupported for predicates are never rendered
        if (!column.supportsPredicates() || isBuiltinColumn(column.name()) || column.path().size() != 1 || !SqlIdentifiers.isQuotable(column.name())) {
            return Optional.empty();
        }
        if (domain.isAll()) {
            return Optional.of("");
        }
        if (domain.isNone() || !isRenderableType(column.type())) {
            return Optional.empty();
        }

        String name = SqlIdentifiers.quote(column.name());
        ValueSet values = domain.getValues();
        if (values.isNone()) {
            // only NULL is allowed
            return Optional.of(name + " IS NULL");
        }
        if (values.isAll()) {
            // every non-NULL value
            return Optional.of(name + " IS NOT NULL");
        }

        List<String> disjuncts = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            Optional<String> rendered = renderRange(name, column.type(), range);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            disjuncts.add(rendered.get());
        }
        if (domain.isNullAllowed()) {
            disjuncts.add(name + " IS NULL");
        }
        if (disjuncts.size() == 1) {
            return Optional.of(disjuncts.getFirst());
        }
        return Optional.of("(" + String.join(" OR ", disjuncts) + ")");
    }

    private static Optional<String> renderRange(String name, Type type, Range range)
    {
        if (range.isSingleValue()) {
            return literal(type, range.getSingleValue()).map(literal -> name + " = " + literal);
        }

        List<String> parts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            Optional<String> literal = literal(type, range.getLowBoundedValue());
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            parts.add(name + (range.isLowInclusive() ? " >= " : " > ") + literal.get());
        }
        if (!range.isHighUnbounded()) {
            Optional<String> literal = literal(type, range.getHighBoundedValue());
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            parts.add(name + (range.isHighInclusive() ? " <= " : " < ") + literal.get());
        }
        // an unbounded range is never present here: isAll is handled earlier, so parts is not empty
        if (parts.size() == 1) {
            return Optional.of(parts.getFirst());
        }
        return Optional.of("(" + String.join(" AND ", parts) + ")");
    }

    private static boolean isBuiltinColumn(String name)
    {
        // mapping fields cannot be named like this, so a name check identifies the builtin columns
        return name.equals("_id") || name.equals("_source") || name.equals("_score");
    }

    private static boolean isRenderableType(Type type)
    {
        return type.equals(BOOLEAN)
                || type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type instanceof VarcharType
                || type.equals(TIMESTAMP_MILLIS);
    }

    private static Optional<String> literal(Type type, Object value)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of((Boolean) value ? "true" : "false");
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            return Optional.of(Long.toString((Long) value));
        }
        if (type.equals(REAL)) {
            float floatValue = Float.intBitsToFloat((int) (long) (Long) value);
            return doubleLiteral(floatValue);
        }
        if (type.equals(DOUBLE)) {
            return doubleLiteral((Double) value);
        }
        if (type instanceof VarcharType) {
            return stringLiteral(((Slice) value).toStringUtf8());
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return timestampLiteral((Long) value);
        }
        return Optional.empty();
    }

    private static Optional<String> doubleLiteral(double value)
    {
        if (Double.isNaN(value) || Double.isInfinite(value) || Math.abs(value) >= MAX_RENDERED_DOUBLE) {
            return Optional.empty();
        }
        return Optional.of(BigDecimal.valueOf(value).toPlainString());
    }

    private static Optional<String> stringLiteral(String value)
    {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\\' || character < 0x20 || character == 0x7F) {
                return Optional.empty();
            }
        }
        return Optional.of("'" + value.replace("'", "''") + "'");
    }

    private static Optional<String> timestampLiteral(long epochMicros)
    {
        if (floorMod(epochMicros, 1000) != 0) {
            return Optional.empty();
        }
        Instant instant = Instant.ofEpochSecond(floorDiv(epochMicros, 1_000_000), floorMod(epochMicros, 1_000_000) * 1000);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        if (dateTime.getYear() < 1 || dateTime.getYear() > 9999) {
            return Optional.empty();
        }
        return Optional.of("'" + TIMESTAMP_FORMAT.format(dateTime) + "'");
    }
}
