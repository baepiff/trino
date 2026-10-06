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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.UNCOVERED_DOCUMENTS_AGGREGATION_NAME;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static io.trino.plugin.opensearch.OpenSearchSessionProperties.TEXT_GROUPBY_PUSHDOWN_ENABLED;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

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
    private final boolean verifyKeywordSubFieldCoverage;
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
        this.verifyKeywordSubFieldCoverage = table.termAggregations().stream().anyMatch(termAggregation -> termAggregation.subField().isPresent());
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
        if (verifyKeywordSubFieldCoverage) {
            verifyKeywordSubFieldCoverage(response);
        }
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

    /**
     * Fails the query when a grouping on a {@code keyword} sub-field would count documents in the NULL group although
     * their {@code text} column has a value, see {@link TermAggregation#fromKeywordSubField}. Every page request counts
     * these documents among the documents of the query, in the same search as the buckets of the page, see
     * {@link OpenSearchQueryBuilder#buildAggregationQuery}. The first page fails before any group is returned, a later
     * page when such a document was indexed while the pages were read.
     */
    private void verifyKeywordSubFieldCoverage(SearchResponse response)
    {
        Aggregations aggregations = verifyNotNull(response.getAggregations(), "Aggregations are missing from the aggregation response");
        Filter uncoveredDocuments = verifyNotNull(aggregations.get(UNCOVERED_DOCUMENTS_AGGREGATION_NAME), "Count of the documents not covered by the keyword sub-fields is missing from the aggregation response");
        long uncovered = uncoveredDocuments.getDocCount();
        if (uncovered > 0) {
            String columns = table.termAggregations().stream()
                    .filter(termAggregation -> termAggregation.subField().isPresent())
                    .map(TermAggregation::field)
                    .collect(joining(", "));
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, ("GROUP BY on text columns cannot be pushed down through their keyword sub-fields (%s): " +
                    "%s matching documents have a value that is not indexed in the sub-field, because the value is longer than the ignore_above of the sub-field " +
                    "or the document was indexed before the sub-field was added to the mapping. " +
                    "Disable the catalog property opensearch.text-groupby-pushdown-enabled or the catalog session property %s, " +
                    "or reindex the documents, for example with _update_by_query")
                    .formatted(columns, uncovered, TEXT_GROUPBY_PUSHDOWN_ENABLED));
        }
    }

    @Override
    public void close() {}
}
