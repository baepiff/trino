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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public class TestOpenSearchExtendedStats
{
    @Test
    public void testExtendedStatsPrecision()
            throws IOException
    {
        try (OpenSearchServer server = new OpenSearchServer("opensearchproject/opensearch:2.19.4", false, ImmutableMap.of());
                RestHighLevelClient client = RestClientUtils.createClient(server.getAddress())) {
            Request createIndex = new Request("PUT", "/precision");
            createIndex.setJsonEntity(
                    """
                    {"mappings": {"properties": {"v": {"type": "double"}}}}
                    """);
            client.getLowLevelClient().performRequest(createIndex);
            for (int offset = 0; offset < 5; offset++) {
                client.index(new IndexRequest("precision").source(ImmutableMap.of("v", 1e9 + offset)).setRefreshPolicy(RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
            }

            SearchResponse response = client.search(
                    new SearchRequest("precision").source(new SearchSourceBuilder()
                            .size(0)
                            .aggregation(AggregationBuilders.extendedStats("stats").field("v"))),
                    RequestOptions.DEFAULT);
            ExtendedStats stats = response.getAggregations().get("stats");

            // The values 1e9 .. 1e9+4 have population variance 2.0, sample variance 2.5, population standard deviation
            // about 1.414 and sample standard deviation about 1.581.
            // Observed on OpenSearch 2.19.4: variance_population=0.0, variance_sampling=0.0,
            // std_deviation_population=0.0 and std_deviation_sampling=0.0. The aggregation derives the variance from the
            // sum of squares minus the squared mean, which cancels catastrophically for large values with a small spread.
            // The assertions record this behavior so that the documented warning stays backed by a test.
            assertThat(stats.getCount()).isEqualTo(5);
            assertThat(stats.getVariancePopulation()).isEqualTo(0.0);
            assertThat(stats.getVarianceSampling()).isEqualTo(0.0);
            assertThat(stats.getStdDeviationPopulation()).isEqualTo(0.0);
            assertThat(stats.getStdDeviationSampling()).isEqualTo(0.0);
        }
    }
}
