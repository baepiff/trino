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

import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
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
import static io.trino.plugin.opensearch.MetricAggregation.MAX;
import static io.trino.plugin.opensearch.MetricAggregation.MIN;
import static io.trino.plugin.opensearch.MetricAggregation.STDDEV_POP;
import static io.trino.plugin.opensearch.MetricAggregation.STDDEV_SAMP;
import static io.trino.plugin.opensearch.MetricAggregation.SUM;
import static io.trino.plugin.opensearch.MetricAggregation.VAR_POP;
import static io.trino.plugin.opensearch.MetricAggregation.VAR_SAMP;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.COMPOSITE_AGGREGATION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Math.abs;
import static java.lang.String.format;

final class AggregationResponseReader
{
    // doubles represent every integer exactly only up to 2^53
    private static final double BIGINT_EXACT_LIMIT = 9007199254740992L;

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
            case SUM -> checkBigintPrecision(metric, sumValue((Stats) aggregation));
            case MIN, MAX -> checkBigintPrecision(metric, singleValue((NumericMetricsAggregation.SingleValue) aggregation));
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP -> statisticalValue(metric.functionName(), (ExtendedStats) aggregation);
            default -> singleValue((NumericMetricsAggregation.SingleValue) aggregation);
        };
    }

    @Nullable
    private static Double checkBigintPrecision(MetricAggregation metric, @Nullable Double value)
    {
        // the input column type decides: sum over INTEGER also has a BIGINT output but is exact
        if (value != null && metric.columnHandle().filter(column -> column.type().equals(BIGINT)).isPresent() && abs(value) >= BIGINT_EXACT_LIMIT) {
            throw new TrinoException(
                    OPENSEARCH_QUERY_FAILURE,
                    format(
                            "Result of %s(%s) is %s, which is not guaranteed to be exact because OpenSearch computes it as a double (limit is 2^53). " +
                                    "Disable opensearch.bigint-aggregation-pushdown-enabled (session property bigint_aggregation_pushdown_enabled) to compute this aggregation in Trino",
                            metric.functionName(),
                            metric.columnHandle().map(OpenSearchColumnHandle::name).orElse("*"),
                            value));
        }
        return value;
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
    private static Double statisticalValue(String functionName, ExtendedStats stats)
    {
        long count = stats.getCount();
        boolean sample = functionName.equals(STDDEV_SAMP) || functionName.equals(VAR_SAMP);
        // SQL requires NULL for the sample statistics over fewer than two values and for any statistic over an empty input
        if (count == 0 || (sample && count < 2)) {
            return null;
        }
        double value = switch (functionName) {
            case STDDEV_SAMP -> stats.getStdDeviationSampling();
            case STDDEV_POP -> stats.getStdDeviationPopulation();
            case VAR_SAMP -> stats.getVarianceSampling();
            case VAR_POP -> stats.getVariancePopulation();
            default -> throw new IllegalArgumentException("Unsupported statistical function: " + functionName);
        };
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return value;
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
