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

import jakarta.annotation.Nullable;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.ValueCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.plugin.opensearch.MetricAggregation.COUNT;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.COMPOSITE_AGGREGATION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

final class AggregationResponseReader
{
    private AggregationResponseReader() {}

    record Result(List<Map<String, Object>> rows, Optional<Map<String, Object>> afterKey, int bucketCount) {}

    static Result read(
            @Nullable Aggregations aggregations,
            long totalHits,
            List<TermAggregation> termAggregations,
            List<MetricAggregation> metricAggregations)
    {
        if (termAggregations.isEmpty()) {
            Map<String, Object> row = new HashMap<>();
            for (MetricAggregation metric : metricAggregations) {
                row.put(metric.alias(), metricValue(metric, aggregations, totalHits));
            }
            return new Result(List.of(row), Optional.empty(), 1);
        }

        CompositeAggregation composite = aggregations == null ? null : aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (composite == null) {
            return new Result(List.of(), Optional.empty(), 0);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (CompositeAggregation.Bucket bucket : composite.getBuckets()) {
            Map<String, Object> row = new HashMap<>();
            Map<String, Object> key = bucket.getKey();
            for (TermAggregation term : termAggregations) {
                row.put(term.term(), normalizeKey(term, key.get(term.term())));
            }
            for (MetricAggregation metric : metricAggregations) {
                row.put(metric.alias(), metricValue(metric, bucket.getAggregations(), bucket.getDocCount()));
            }
            rows.add(row);
        }
        return new Result(rows, Optional.ofNullable(composite.afterKey()), rows.size());
    }

    private static Object normalizeKey(TermAggregation term, @Nullable Object value)
    {
        // boolean fields may be reported as 0/1 in composite keys
        if (term.type().equals(BOOLEAN) && value instanceof Number number) {
            return number.longValue() != 0;
        }
        return value;
    }

    @Nullable
    private static Object metricValue(MetricAggregation metric, @Nullable Aggregations aggregations, long documentCount)
    {
        if (metric.columnHandle().isEmpty()) {
            // count(*)
            return documentCount;
        }

        Aggregation aggregation = aggregations == null ? null : aggregations.get(metric.alias());
        verifyNotNull(aggregation, "Missing aggregation result for %s", metric.alias());
        return switch (metric.functionName()) {
            case COUNT -> ((ValueCount) aggregation).getValue();
            case SUM -> sumValue((Stats) aggregation);
            default -> singleValue((NumericMetricsAggregation.SingleValue) aggregation);
        };
    }

    @Nullable
    private static Double sumValue(Stats stats)
    {
        // the sum of an empty input is NULL in SQL, but OpenSearch reports 0
        if (stats.getCount() == 0) {
            return null;
        }
        return stats.getSum();
    }

    @Nullable
    private static Double singleValue(NumericMetricsAggregation.SingleValue aggregation)
    {
        // min, max and avg over an empty input are reported as +/-Infinity or NaN by the parsed response
        double value = aggregation.value();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return value;
    }
}
