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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.opensearch.PushdownColumns.isDocValuesPushdownSupported;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public record MetricAggregation(String functionName, Type outputType, Optional<OpenSearchColumnHandle> columnHandle, String alias)
{
    public static final String COUNT = "count";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String SUM = "sum";
    public static final String AVG = "avg";

    public static final String STDDEV_SAMP = "stddev_samp";
    public static final String STDDEV_POP = "stddev_pop";
    public static final String VAR_SAMP = "var_samp";
    public static final String VAR_POP = "var_pop";

    public static final Set<String> STATISTICAL_FUNCTIONS = ImmutableSet.of(STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP);
    public static final Set<String> DEFAULT_FUNCTIONS = ImmutableSet.of(COUNT, MIN, MAX, SUM, AVG);
    public static final Set<String> SQL_FUNCTIONS = ImmutableSet.<String>builder()
            .addAll(DEFAULT_FUNCTIONS)
            .addAll(STATISTICAL_FUNCTIONS)
            .build();

    public MetricAggregation
    {
        requireNonNull(functionName, "functionName is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(alias, "alias is null");
    }

    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias)
    {
        return from(function, assignments, alias, DEFAULT_FUNCTIONS);
    }

    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias, Set<String> allowedFunctions)
    {
        return from(function, assignments, alias, allowedFunctions, false);
    }

    /**
     * @param allowBigintInput accept BIGINT input columns for min, max, sum and avg. OpenSearch computes these with doubles, so the
     *         caller must be able to detect results that are not exact (magnitude of 2^53 or more). Statistical functions never accept BIGINT.
     */
    public static Optional<MetricAggregation> from(AggregateFunction function, Map<String, ColumnHandle> assignments, String alias, Set<String> allowedFunctions, boolean allowBigintInput)
    {
        if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty()) {
            return Optional.empty();
        }

        String functionName = canonicalFunctionName(function.getFunctionName());
        if (!allowedFunctions.contains(functionName)) {
            return Optional.empty();
        }

        if (functionName.equals(COUNT) && function.getArguments().isEmpty()) {
            return Optional.of(new MetricAggregation(COUNT, function.getOutputType(), Optional.empty(), alias));
        }

        if (function.getArguments().size() != 1 || !(function.getArguments().getFirst() instanceof Variable variable)) {
            return Optional.empty();
        }
        if (!(assignments.get(variable.getName()) instanceof OpenSearchColumnHandle column)
                || !isDocValuesPushdownSupported(column)
                || !isSupportedInput(functionName, column.type(), allowBigintInput)) {
            return Optional.empty();
        }
        return Optional.of(new MetricAggregation(functionName, function.getOutputType(), Optional.of(column), alias));
    }

    public static String canonicalFunctionName(String functionName)
    {
        return switch (functionName) {
            case "stddev" -> STDDEV_SAMP;
            case "variance" -> VAR_SAMP;
            default -> functionName;
        };
    }

    private static boolean isSupportedInput(String functionName, Type inputType, boolean allowBigintInput)
    {
        return switch (functionName) {
            // value_count works on any field type that supports predicates
            case COUNT -> true;
            // BIGINT is excluded unless the caller opts in: metric aggregations return doubles, so values above 2^53 lose precision
            case MIN, MAX -> (allowBigintInput && inputType.equals(BIGINT)) || inputType.equals(TINYINT) || inputType.equals(SMALLINT) || inputType.equals(INTEGER) || inputType.equals(REAL) || inputType.equals(DOUBLE);
            // REAL is excluded: OpenSearch accumulates in double while Trino accumulates in single precision
            // The integer input types are not reachable today: Trino plans sum and avg over them on a CAST to BIGINT or DOUBLE, which is never pushed down
            case SUM, AVG -> (allowBigintInput && inputType.equals(BIGINT)) || inputType.equals(TINYINT) || inputType.equals(SMALLINT) || inputType.equals(INTEGER) || inputType.equals(DOUBLE);
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP -> inputType.equals(TINYINT) || inputType.equals(SMALLINT) || inputType.equals(INTEGER) || inputType.equals(DOUBLE);
            default -> false;
        };
    }
}
