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
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.BooleanDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.plugin.opensearch.decoders.TimestampDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlWhereRenderer
{
    private static final OpenSearchColumnHandle AGE = new OpenSearchColumnHandle(List.of("age"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("age"), true);
    private static final OpenSearchColumnHandle ID = new OpenSearchColumnHandle(List.of("id"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("id"), true);
    private static final OpenSearchColumnHandle NAME = new OpenSearchColumnHandle(List.of("name"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("name"), true);
    private static final OpenSearchColumnHandle SCORE = new OpenSearchColumnHandle(List.of("score"), DOUBLE, new IndexMetadata.PrimitiveType("double"), new DoubleDecoder.Descriptor("score"), true);
    private static final OpenSearchColumnHandle RATING = new OpenSearchColumnHandle(List.of("rating"), REAL, new IndexMetadata.PrimitiveType("float"), new RealDecoder.Descriptor("rating"), true);
    private static final OpenSearchColumnHandle DELETED = new OpenSearchColumnHandle(List.of("deleted"), BOOLEAN, new IndexMetadata.PrimitiveType("boolean"), new BooleanDecoder.Descriptor("deleted"), true);
    private static final OpenSearchColumnHandle CREATED = new OpenSearchColumnHandle(List.of("createdOn"), TIMESTAMP_MILLIS, new IndexMetadata.DateTimeType(List.of()), new TimestampDecoder.Descriptor("createdOn"), true);

    private static TupleDomain<OpenSearchColumnHandle> domains(Object... columnsAndDomains)
    {
        ImmutableMap.Builder<OpenSearchColumnHandle, Domain> builder = ImmutableMap.builder();
        for (int i = 0; i < columnsAndDomains.length; i += 2) {
            builder.put((OpenSearchColumnHandle) columnsAndDomains[i], (Domain) columnsAndDomains[i + 1]);
        }
        return TupleDomain.withColumnDomains(builder.buildOrThrow());
    }

    private static Optional<String> render(Object... columnsAndDomains)
    {
        return SqlWhereRenderer.render(domains(columnsAndDomains));
    }

    @Test
    public void testNoPredicate()
    {
        assertThat(SqlWhereRenderer.render(TupleDomain.all())).hasValue("");
        assertThat(render(AGE, Domain.all(INTEGER))).hasValue("");
    }

    @Test
    public void testNoneIsNotRendered()
    {
        assertThat(SqlWhereRenderer.render(TupleDomain.none())).isEmpty();
    }

    @Test
    public void testSingleValuesRangesAndNulls()
    {
        assertThat(render(AGE, Domain.singleValue(INTEGER, 5L))).hasValue("`age` = 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 5L)), false))).hasValue("`age` > 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 5L)), false))).hasValue("`age` <= 5");
        assertThat(render(AGE, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 10L, false)), false))).hasValue("(`age` >= 1 AND `age` < 10)");
        assertThat(render(AGE, Domain.multipleValues(INTEGER, List.of(1L, 2L, 3L)))).hasValue("(`age` = 1 OR `age` = 2 OR `age` = 3)");
        assertThat(render(AGE, Domain.onlyNull(INTEGER))).hasValue("`age` IS NULL");
        assertThat(render(AGE, Domain.notNull(INTEGER))).hasValue("`age` IS NOT NULL");
        assertThat(render(AGE, Domain.create(ValueSet.of(INTEGER, 5L), true))).hasValue("(`age` = 5 OR `age` IS NULL)");
        assertThat(render(ID, Domain.singleValue(BIGINT, -9007199254740993L))).hasValue("`id` = -9007199254740993");
    }

    @Test
    public void testColumnsAreJoinedInNameOrder()
    {
        assertThat(render(
                NAME,
                Domain.singleValue(VARCHAR, utf8Slice("bob")),
                AGE,
                Domain.singleValue(INTEGER, 5L)))
                .hasValue("`age` = 5 AND `name` = 'bob'");
    }

    @Test
    public void testStringLiterals()
    {
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("it's")))).hasValue("`name` = 'it''s'");
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("naïve 中文")))).hasValue("`name` = 'naïve 中文'");
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\\b")))).isEmpty();
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\nb")))).isEmpty();
        assertThat(render(NAME, Domain.singleValue(VARCHAR, utf8Slice("a\u0000b")))).isEmpty();
    }

    @Test
    public void testFloatingPoint()
    {
        assertThat(render(SCORE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 0.1)), false))).hasValue("`score` > 0.1");
        // REAL values are widened to double so that they compare equal to the stored float
        assertThat(render(RATING, Domain.create(ValueSet.ofRanges(Range.greaterThan(REAL, (long) Float.floatToRawIntBits(0.1f))), false))).hasValue("`rating` > 0.10000000149011612");
        // there is no NaN assertion: Domain and Range reject NaN, so such a constraint cannot be built
        assertThat(render(SCORE, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY))).isEmpty();
        assertThat(render(SCORE, Domain.singleValue(DOUBLE, 1e300))).isEmpty();
    }

    @Test
    public void testBooleanAndTimestamp()
    {
        assertThat(render(DELETED, Domain.singleValue(BOOLEAN, true))).hasValue("`deleted` = true");
        assertThat(render(DELETED, Domain.singleValue(BOOLEAN, false))).hasValue("`deleted` = false");

        long micros = micros(Instant.parse("2026-07-15T00:00:00.123Z"));
        assertThat(render(CREATED, Domain.create(ValueSet.ofRanges(Range.greaterThan(TIMESTAMP_MILLIS, micros)), false)))
                .hasValue("`createdOn` > '2026-07-15 00:00:00.123'");
        assertThat(render(CREATED, Domain.singleValue(TIMESTAMP_MILLIS, micros(Instant.parse("1969-12-31T23:59:59.001Z")))))
                .hasValue("`createdOn` = '1969-12-31 23:59:59.001'");
        // sub-millisecond precision cannot be rendered exactly
        assertThat(render(CREATED, Domain.singleValue(TIMESTAMP_MILLIS, micros + 1))).isEmpty();
    }

    @Test
    public void testUnsupportedShapes()
    {
        OpenSearchColumnHandle nested = new OpenSearchColumnHandle(List.of("a", "b"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("a.b"), true);
        assertThat(render(nested, Domain.singleValue(INTEGER, 1L))).isEmpty();

        OpenSearchColumnHandle backtick = new OpenSearchColumnHandle(List.of("a`b"), INTEGER, new IndexMetadata.PrimitiveType("integer"), new IntegerDecoder.Descriptor("a`b"), true);
        assertThat(render(backtick, Domain.singleValue(INTEGER, 1L))).isEmpty();

        OpenSearchColumnHandle array = new OpenSearchColumnHandle(List.of("tags"), new ArrayType(VARCHAR), new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("tags"), true);
        assertThat(render(array, Domain.onlyNull(new ArrayType(VARCHAR)))).isEmpty();
    }

    @Test
    public void testBuiltinAndPredicateUnsupportedColumnsAreNotRendered()
    {
        OpenSearchColumnHandle id = new OpenSearchColumnHandle(List.of("_id"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("_id"), true);
        assertThat(render(id, Domain.singleValue(VARCHAR, utf8Slice("x")))).isEmpty();

        OpenSearchColumnHandle score = new OpenSearchColumnHandle(List.of("_score"), REAL, new IndexMetadata.PrimitiveType("float"), new RealDecoder.Descriptor("_score"), false);
        assertThat(render(score, Domain.create(ValueSet.ofRanges(Range.greaterThan(REAL, (long) Float.floatToRawIntBits(1.0f))), false)))
                .isEmpty();

        OpenSearchColumnHandle source = new OpenSearchColumnHandle(List.of("_source"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new VarcharDecoder.Descriptor("_source"), true);
        assertThat(render(source, Domain.onlyNull(VARCHAR))).isEmpty();

        // text-mapped and scaled_float columns are flagged as unsupported for predicates
        OpenSearchColumnHandle text = new OpenSearchColumnHandle(List.of("message"), VARCHAR, new IndexMetadata.PrimitiveType("text"), new VarcharDecoder.Descriptor("message"), false);
        assertThat(render(text, Domain.singleValue(VARCHAR, utf8Slice("x")))).isEmpty();

        OpenSearchColumnHandle scaled = new OpenSearchColumnHandle(List.of("price"), DOUBLE, new IndexMetadata.ScaledFloatType(100), new DoubleDecoder.Descriptor("price"), false);
        assertThat(render(scaled, Domain.singleValue(DOUBLE, 1.5))).isEmpty();
    }

    private static long micros(Instant instant)
    {
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }
}
