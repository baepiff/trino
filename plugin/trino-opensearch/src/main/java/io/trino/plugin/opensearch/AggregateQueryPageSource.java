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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.AggregationResponseReader.Result;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static java.util.Objects.requireNonNull;

public class AggregateQueryPageSource
        implements ConnectorPageSource
{
    // decoders of aggregation output columns do not read from the hit
    private static final SearchHit NO_HIT = new SearchHit(0);

    private final OpenSearchClient client;
    private final OpenSearchTableHandle table;
    private final List<OpenSearchColumnHandle> columns;
    private final List<String> columnNames;
    private final List<Decoder> decoders;
    private final QueryBuilder query;
    private final int pageSize;

    private Optional<Map<String, Object>> after = Optional.empty();
    private boolean finished;
    private long readTimeNanos;

    public AggregateQueryPageSource(OpenSearchClient client, OpenSearchTableHandle table, List<OpenSearchColumnHandle> columns, int pageSize)
    {
        this.client = requireNonNull(client, "client is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnNames = this.columns.stream()
                .map(OpenSearchColumnHandle::name)
                .collect(toImmutableList());
        this.decoders = this.columns.stream()
                .map(OpenSearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
        this.query = buildSearchQuery(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast), table.query(), table.regexes());
        this.pageSize = pageSize;
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

        long start = System.nanoTime();
        SearchResponse response = client.beginAggregationSearch(
                table.index(),
                query,
                buildAggregationQuery(table.termAggregations(), table.metricAggregations(), pageSize, after));
        readTimeNanos += System.nanoTime() - start;

        verifyNotNull(response.getHits().getTotalHits(), "Total hits are missing from the aggregation response");
        Result result = AggregationResponseReader.read(
                response.getAggregations(),
                response.getHits().getTotalHits().value(),
                table.termAggregations(),
                table.metricAggregations());

        after = result.afterKey();
        // a composite page smaller than requested is the last one
        finished = table.termAggregations().isEmpty() || result.afterKey().isEmpty() || result.bucketCount() < pageSize;

        if (result.rows().isEmpty()) {
            return null;
        }
        if (columns.isEmpty()) {
            return SourcePage.create(result.rows().size());
        }

        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = columns.get(i).type().createBlockBuilder(null, result.rows().size());
        }
        for (Map<String, Object> row : result.rows()) {
            for (int i = 0; i < builders.length; i++) {
                String name = columnNames.get(i);
                decoders.get(i).decode(NO_HIT, () -> row.get(name), builders[i]);
            }
        }

        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public void close() {}
}
