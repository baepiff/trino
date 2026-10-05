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

import io.trino.plugin.opensearch.DecoderDescriptor;
import io.trino.plugin.opensearch.MetricAggregation;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IdColumnDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.plugin.opensearch.decoders.SmallintDecoder;
import io.trino.plugin.opensearch.decoders.TinyintDecoder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlAggregatePageSource
{
    private static final OpenSearchColumnHandle COUNT_COLUMN = new OpenSearchColumnHandle(List.of("_pushdown_0"), BIGINT, new PrimitiveType("long"), new BigintDecoder.Descriptor("_pushdown_0"), false);
    private static final OpenSearchColumnHandle MAX_COLUMN = new OpenSearchColumnHandle(List.of("_pushdown_1"), INTEGER, new PrimitiveType("integer"), new IntegerDecoder.Descriptor("_pushdown_1"), false);

    private static final OpenSearchColumnHandle VERSION = new OpenSearchColumnHandle(List.of("version"), INTEGER, new PrimitiveType("integer"), new IntegerDecoder.Descriptor("version"), true);

    private static List<Decoder> decoders(List<OpenSearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> column.decoderDescriptor().createDecoder())
                .collect(toImmutableList());
    }

    private static OpenSearchColumnHandle synthetic(String name, Type type, String opensearchType, DecoderDescriptor descriptor)
    {
        return new OpenSearchColumnHandle(List.of(name), type, new PrimitiveType(opensearchType), descriptor, false);
    }

    @Test
    public void testValuesAreWrittenAccordingToTheColumnType()
    {
        List<OpenSearchColumnHandle> columns = List.of(
                synthetic("bigint_from_long", BIGINT, "long", new BigintDecoder.Descriptor("bigint_from_long")),
                synthetic("bigint_from_double", BIGINT, "long", new BigintDecoder.Descriptor("bigint_from_double")),
                synthetic("bigint_from_big_integer", BIGINT, "long", new BigintDecoder.Descriptor("bigint_from_big_integer")),
                synthetic("integer_from_long", INTEGER, "integer", new IntegerDecoder.Descriptor("integer_from_long")),
                synthetic("smallint_from_long", SMALLINT, "short", new SmallintDecoder.Descriptor("smallint_from_long")),
                synthetic("tinyint_from_long", TINYINT, "byte", new TinyintDecoder.Descriptor("tinyint_from_long")),
                synthetic("double_from_long", DOUBLE, "double", new DoubleDecoder.Descriptor("double_from_long")),
                synthetic("double_from_double", DOUBLE, "double", new DoubleDecoder.Descriptor("double_from_double")),
                synthetic("real_from_double", REAL, "float", new RealDecoder.Descriptor("real_from_double")),
                synthetic("real_from_long", REAL, "float", new RealDecoder.Descriptor("real_from_long")),
                synthetic("null_bigint", BIGINT, "long", new BigintDecoder.Descriptor("null_bigint")),
                synthetic("null_double", DOUBLE, "double", new DoubleDecoder.Descriptor("null_double")),
                synthetic("missing_real", REAL, "float", new RealDecoder.Descriptor("missing_real")));

        Map<String, Object> values = new HashMap<>();
        values.put("bigint_from_long", 42L);
        values.put("bigint_from_double", 43.0);
        values.put("bigint_from_big_integer", BigInteger.valueOf(44));
        values.put("integer_from_long", 7L);
        values.put("smallint_from_long", 8L);
        values.put("tinyint_from_long", 9L);
        values.put("double_from_long", 10L);
        values.put("double_from_double", 2.5);
        values.put("real_from_double", 1.5);
        values.put("real_from_long", 3L);
        values.put("null_bigint", null);
        values.put("null_double", null);

        SourcePage page = SqlAggregatePageSource.buildPage(columns, decoders(columns), values);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(page.getChannelCount()).isEqualTo(columns.size());
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(42L);
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(43L);
        assertThat(BIGINT.getLong(page.getBlock(2), 0)).isEqualTo(44L);
        assertThat(INTEGER.getInt(page.getBlock(3), 0)).isEqualTo(7);
        assertThat(SMALLINT.getShort(page.getBlock(4), 0)).isEqualTo((short) 8);
        assertThat(TINYINT.getByte(page.getBlock(5), 0)).isEqualTo((byte) 9);
        assertThat(DOUBLE.getDouble(page.getBlock(6), 0)).isEqualTo(10.0);
        assertThat(DOUBLE.getDouble(page.getBlock(7), 0)).isEqualTo(2.5);
        assertThat(REAL.getFloat(page.getBlock(8), 0)).isEqualTo(1.5f);
        assertThat(REAL.getFloat(page.getBlock(9), 0)).isEqualTo(3.0f);
        assertThat(page.getBlock(10).isNull(0)).isTrue();
        assertThat(page.getBlock(11).isNull(0)).isTrue();
        assertThat(page.getBlock(12).isNull(0)).isTrue();
    }

    @Test
    public void testOutOfRangeValueIsRejected()
    {
        List<OpenSearchColumnHandle> columns = List.of(synthetic("value", INTEGER, "integer", new IntegerDecoder.Descriptor("value")));
        Map<String, Object> values = new HashMap<>();
        values.put("value", 1L << 40);

        assertThatThrownBy(() -> SqlAggregatePageSource.buildPage(columns, decoders(columns), values))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    public void testNoColumnsProducesOneRow()
    {
        // count(*) can be satisfied without any output column; the global aggregation still has exactly one row
        SourcePage page = SqlAggregatePageSource.buildPage(List.of(), List.of(), Map.of());

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(page.getChannelCount()).isEqualTo(0);
    }

    @Test
    public void testReadsOnePageThroughTheSqlClient()
            throws IOException
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        OpenSearchClient client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        try {
            FakeSqlClient sqlClient = new FakeSqlClient(client);
            OpenSearchTableHandle table = new OpenSearchTableHandle(SCAN, "default", "metric_logs", Optional.empty())
                    .withSqlAggregations(List.of(
                            new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"),
                            new MetricAggregation("max", INTEGER, Optional.of(VERSION), "_pushdown_1")));

            SqlAggregatePageSource pageSource = new SqlAggregatePageSource(sqlClient, table, List.of(COUNT_COLUMN, MAX_COLUMN));
            assertThat(pageSource.isFinished()).isFalse();

            SourcePage page = pageSource.getNextSourcePage();
            assertThat(sqlClient.statements()).containsExactly("SELECT count(*), max(`version`) FROM `metric_logs`");
            assertThat(page.getPositionCount()).isEqualTo(1);
            assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(5L);
            // OpenSearch SQL returns the max of an INTEGER field as a long
            assertThat(INTEGER.getInt(page.getBlock(1), 0)).isEqualTo(7);

            assertThat(pageSource.isFinished()).isTrue();
            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(sqlClient.statements()).hasSize(1);

            // a count(*) without output columns still runs the statement and produces one row
            FakeSqlClient countOnlyClient = new FakeSqlClient(client);
            SourcePage countPage = new SqlAggregatePageSource(countOnlyClient, table, List.of()).getNextSourcePage();
            assertThat(countPage.getPositionCount()).isEqualTo(1);
            assertThat(countPage.getChannelCount()).isEqualTo(0);
            assertThat(countOnlyClient.statements()).hasSize(1);
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testUnrenderablePredicateFailsInsteadOfReturningWrongData()
            throws IOException
    {
        OpenSearchConfig config = new OpenSearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
        OpenSearchClient client = new OpenSearchClient(config, Optional.empty(), Optional.empty());
        try {
            FakeSqlClient sqlClient = new FakeSqlClient(client);
            OpenSearchColumnHandle builtin = new OpenSearchColumnHandle(List.of("_id"), VARCHAR, new PrimitiveType("keyword"), new IdColumnDecoder.Descriptor(), false);
            OpenSearchTableHandle table = new OpenSearchTableHandle(
                    SCAN,
                    "default",
                    "metric_logs",
                    TupleDomain.withColumnDomains(Map.of(builtin, Domain.singleValue(VARCHAR, utf8Slice("a")))),
                    Map.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Set.of(),
                    List.of(),
                    List.of(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0")));

            assertThatThrownBy(() -> new SqlAggregatePageSource(sqlClient, table, List.of(COUNT_COLUMN)).getNextSourcePage())
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("can no longer be rendered");
            assertThat(sqlClient.statements()).isEmpty();
        }
        finally {
            client.close();
        }
    }

    private static class FakeSqlClient
            extends OpenSearchSqlClient
    {
        private final List<String> statements = new ArrayList<>();

        FakeSqlClient(OpenSearchClient client)
        {
            super(client);
        }

        List<String> statements()
        {
            return statements;
        }

        @Override
        SqlResult execute(String sql)
        {
            statements.add(sql);
            return new SqlResult(
                    List.of(new SqlColumn("c0", "long"), new SqlColumn("c1", "integer")),
                    List.of(List.of(5L, 7L)));
        }
    }
}
