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
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.plugin.opensearch.OpenSearchMetadata.aggregationOutputColumn;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildUncoveredDocumentsQuery;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.SCAN;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.bigintColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.groupableTextColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.keywordColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAggregateQueryPageSource
{
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(ImmutableList.of(
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField("composite"), (ContextParser<Object, Aggregation>) (parser, name) -> ParsedComposite.fromXContent(parser, (String) name))));

    private static final OpenSearchColumnHandle TENANT = groupableTextColumn("tenantId", OptionalInt.of(20));
    private static final OpenSearchColumnHandle KIND = keywordColumn("kind");
    private static final OpenSearchColumnHandle REGIONKEY = bigintColumn("regionkey");
    private static final MetricAggregation COUNT_STAR = new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0");
    private static final OpenSearchColumnHandle COUNT_COLUMN = aggregationOutputColumn("_pushdown_0", BIGINT).orElseThrow();

    @Test
    public void testUncoveredDocumentsFailTheQuery()
            throws IOException
    {
        OpenSearchTableHandle table = filteredTable().withAggregations(List.of(new TermAggregation("tenantId", VARCHAR, Optional.of("keyword"))), List.of(COUNT_STAR));
        RecordingClient client = new RecordingClient(3, List.of());
        try {
            AggregateQueryPageSource pageSource = new AggregateQueryPageSource(client, table, List.of(TENANT, COUNT_COLUMN), 2);

            assertThatThrownBy(pageSource::getNextSourcePage)
                    .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(OPENSEARCH_QUERY_FAILURE.toErrorCode()))
                    .hasMessage("GROUP BY on text columns cannot be pushed down through their keyword sub-fields (tenantId.keyword): " +
                            "3 matching documents have a value that is not indexed in the sub-field, because the value is longer than the ignore_above of the sub-field " +
                            "or the document was indexed before the sub-field was added to the mapping. " +
                            "Disable the catalog property opensearch.text-groupby-pushdown-enabled or the catalog session property text_groupby_pushdown_enabled, " +
                            "or reindex the documents, for example with _update_by_query");

            // the count uses the filter of the aggregation, which is never run
            assertThat(client.countQueries).containsExactly(expectedUncoveredDocumentsQuery(table));
            assertThat(client.aggregationRequests).isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testCoveredDocumentsAreGroupedOverPages()
            throws IOException
    {
        OpenSearchTableHandle table = filteredTable().withAggregations(List.of(new TermAggregation("tenantId", VARCHAR, Optional.of("keyword"))), List.of(COUNT_STAR));
        RecordingClient client = new RecordingClient(0, List.of(
                """
                {"took": 1, "timed_out": false, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
                 "hits": {"total": {"value": 6, "relation": "eq"}, "max_score": null, "hits": []},
                 "aggregations": {"composite#groupBy": {"after_key": {"tenantId": ""}, "buckets": [
                   {"key": {"tenantId": null}, "doc_count": 2},
                   {"key": {"tenantId": ""}, "doc_count": 1}]}}}
                """,
                """
                {"took": 1, "timed_out": false, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
                 "hits": {"total": {"value": 6, "relation": "eq"}, "max_score": null, "hits": []},
                 "aggregations": {"composite#groupBy": {"after_key": {"tenantId": "a"}, "buckets": [
                   {"key": {"tenantId": "a"}, "doc_count": 3}]}}}
                """));
        try {
            AggregateQueryPageSource pageSource = new AggregateQueryPageSource(client, table, List.of(TENANT, COUNT_COLUMN), 2);

            List<String> groups = new ArrayList<>();
            List<Long> counts = new ArrayList<>();
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block tenants = page.getBlock(0);
                Block values = page.getBlock(1);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    groups.add(tenants.isNull(position) ? null : VARCHAR.getSlice(tenants, position).toStringUtf8());
                    counts.add(BIGINT.getLong(values, position));
                }
            }

            // the NULL group and the empty string stay apart
            assertThat(groups).containsExactly(null, "", "a");
            assertThat(counts).containsExactly(2L, 1L, 3L);
            // one verification for the whole query, before the first page
            assertThat(client.countQueries).containsExactly(expectedUncoveredDocumentsQuery(table));
            // the second page continues after the empty string, the last key of the first page
            assertThat(client.aggregationRequests).containsExactly(
                    buildAggregationQuery(table.termAggregations(), table.metricAggregations(), 2, Optional.empty()),
                    buildAggregationQuery(table.termAggregations(), table.metricAggregations(), 2, Optional.of(Map.of("tenantId", ""))));
            assertThat(client.aggregationRequests.getFirst().getFirst().toString()).contains("\"field\":\"tenantId.keyword\"");
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testNoVerificationWithoutSubFieldGrouping()
            throws IOException
    {
        OpenSearchTableHandle table = filteredTable().withAggregations(List.of(new TermAggregation("kind", VARCHAR)), List.of(COUNT_STAR));
        RecordingClient client = new RecordingClient(3, List.of(
                """
                {"took": 1, "timed_out": false, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
                 "hits": {"total": {"value": 1, "relation": "eq"}, "max_score": null, "hits": []},
                 "aggregations": {"composite#groupBy": {"buckets": [{"key": {"kind": "a"}, "doc_count": 1}]}}}
                """));
        try {
            AggregateQueryPageSource pageSource = new AggregateQueryPageSource(client, table, List.of(KIND, COUNT_COLUMN), 2);

            assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(1);
            assertThat(pageSource.isFinished()).isTrue();
            assertThat(client.countQueries).isEmpty();
        }
        finally {
            client.close();
        }
    }

    private static OpenSearchTableHandle filteredTable()
    {
        return new OpenSearchTableHandle(
                SCAN,
                "default",
                "nation",
                TupleDomain.withColumnDomains(Map.of(REGIONKEY, Domain.singleValue(BIGINT, 1L))),
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                Set.of(),
                List.of(),
                List.of());
    }

    private static QueryBuilder expectedUncoveredDocumentsQuery(OpenSearchTableHandle table)
    {
        QueryBuilder filter = buildSearchQuery(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast), table.query(), table.regexes());
        return buildUncoveredDocumentsQuery(filter, table.termAggregations()).orElseThrow();
    }

    private static class RecordingClient
            extends OpenSearchClient
    {
        private final long uncoveredDocuments;
        private final Deque<String> responses;
        private final List<QueryBuilder> countQueries = new ArrayList<>();
        private final List<List<AggregationBuilder>> aggregationRequests = new ArrayList<>();

        public RecordingClient(long uncoveredDocuments, List<String> responses)
        {
            super(new OpenSearchConfig().setHosts(List.of("localhost")).setDefaultSchema("default"), Optional.empty(), Optional.empty());
            this.uncoveredDocuments = uncoveredDocuments;
            this.responses = new ArrayDeque<>(responses);
        }

        @Override
        public long countDocuments(String index, QueryBuilder query)
        {
            countQueries.add(query);
            return uncoveredDocuments;
        }

        @Override
        public SearchResponse beginAggregationSearch(String index, QueryBuilder query, List<AggregationBuilder> aggregations)
        {
            aggregationRequests.add(aggregations);
            return parseResponse(responses.removeFirst());
        }

        private static SearchResponse parseResponse(@Language("JSON") String json)
        {
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(REGISTRY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
                return SearchResponse.fromXContent(parser);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
