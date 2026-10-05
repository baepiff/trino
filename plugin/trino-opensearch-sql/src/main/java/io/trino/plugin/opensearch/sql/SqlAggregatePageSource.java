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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

class SqlAggregatePageSource
        implements ConnectorPageSource
{
    private final OpenSearchSqlClient client;
    private final OpenSearchTableHandle table;
    private final List<OpenSearchColumnHandle> columns;
    private final List<Decoder> decoders;

    private boolean finished;
    private long readTimeNanos;

    SqlAggregatePageSource(OpenSearchSqlClient client, OpenSearchTableHandle table, List<OpenSearchColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.decoders = this.columns.stream()
                .map(column -> column.decoderDescriptor().createDecoder())
                .collect(toImmutableList());
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        finished = true;

        String whereClause = SqlWhereRenderer.render(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast))
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "The pushed-down predicate can no longer be rendered as SQL"));
        SqlAggregationQuery query = SqlAggregationQueryBuilder.build(table.index(), table.metricAggregations(), whereClause)
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "The pushed-down aggregation can no longer be rendered as SQL"));

        long start = System.nanoTime();
        SqlResult result = client.execute(query.sql());
        readTimeNanos += System.nanoTime() - start;

        return buildPage(columns, decoders, SqlAggregationResponseReader.read(query, result));
    }

    /**
     * Builds the single row page of a global aggregation. The values are raw SQL cells (Long, Double, BigInteger, ...)
     * that may not match the Trino type of the column, so each one is written by the decoder of its column, which converts any Number.
     */
    static SourcePage buildPage(List<OpenSearchColumnHandle> columns, List<Decoder> decoders, Map<String, Object> values)
    {
        if (columns.isEmpty()) {
            // a global aggregation always produces exactly one row
            return SourcePage.create(1);
        }

        Block[] blocks = new Block[columns.size()];
        for (int index = 0; index < blocks.length; index++) {
            OpenSearchColumnHandle column = columns.get(index);
            BlockBuilder builder = column.type().createBlockBuilder(null, 1);
            Object value = values.get(column.name());
            // decoders of synthetic aggregation columns never read from the search hit
            decoders.get(index).decode(null, () -> value, builder);
            blocks[index] = builder.build();
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public void close() {}
}
