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

import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.decoders.BooleanDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.RawJsonDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.opensearch.TestOpenSearchMetadata.bigintColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.keywordColumn;
import static io.trino.plugin.opensearch.TestOpenSearchMetadata.textColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationModel
{
    @Test
    public void testTermAggregationAcceptsKeywordIntegralAndBoolean()
    {
        assertThat(TermAggregation.fromColumn(keywordColumn("name"))).hasValue(new TermAggregation("name", VARCHAR));
        assertThat(TermAggregation.fromColumn(bigintColumn("regionkey"))).hasValue(new TermAggregation("regionkey", BIGINT));
        assertThat(TermAggregation.fromColumn(column("flag", BOOLEAN, "boolean", true))).isPresent();
        assertThat(TermAggregation.fromColumn(column("count", INTEGER, "integer", true))).isPresent();
    }

    @Test
    public void testTermAggregationRejectsOtherColumns()
    {
        assertThat(TermAggregation.fromColumn(textColumn("description"))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("price", DOUBLE, "double", true))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("rating", REAL, "float", true))).isEmpty();
        assertThat(TermAggregation.fromColumn(column("_id", VARCHAR, "text", true))).isEmpty();
    }

    @Test
    public void testRawJsonColumnsAreNotPushed()
    {
        OpenSearchColumnHandle rawJson = new OpenSearchColumnHandle(List.of("payload"), VARCHAR, new IndexMetadata.PrimitiveType("keyword"), new RawJsonDecoder.Descriptor("payload"), true);

        assertThat(TermAggregation.fromColumn(rawJson)).isEmpty();
        for (String name : List.of("count", "min", "max", "sum", "avg")) {
            assertThat(MetricAggregation.from(function(name, BIGINT, "payload", VARCHAR), Map.of("payload", rawJson), "a")).as(name).isEmpty();
        }
        // the same keyword mapping without the raw JSON transform is pushed
        assertThat(TermAggregation.fromColumn(keywordColumn("payload"))).isPresent();
        assertThat(MetricAggregation.from(function("count", BIGINT, "payload", VARCHAR), Map.of("payload", keywordColumn("payload")), "a")).isPresent();
    }

    @Test
    public void testCountStar()
    {
        assertThat(MetricAggregation.from(function("count", BIGINT), Map.of(), "_pushdown_0"))
                .hasValue(new MetricAggregation("count", BIGINT, Optional.empty(), "_pushdown_0"));
    }

    @Test
    public void testCountColumnAcceptsBigintAndKeyword()
    {
        OpenSearchColumnHandle bigint = bigintColumn("regionkey");
        OpenSearchColumnHandle keyword = keywordColumn("name");

        assertThat(MetricAggregation.from(function("count", BIGINT, "regionkey", BIGINT), Map.of("regionkey", bigint), "a"))
                .hasValue(new MetricAggregation("count", BIGINT, Optional.of(bigint), "a"));
        assertThat(MetricAggregation.from(function("count", BIGINT, "name", VARCHAR), Map.of("name", keyword), "a"))
                .isPresent();
    }

    @Test
    public void testNumericFunctionsAcceptedInputs()
    {
        OpenSearchColumnHandle integer = column("i", INTEGER, "integer", true);
        OpenSearchColumnHandle doubleColumn = column("d", DOUBLE, "double", true);
        OpenSearchColumnHandle real = column("r", REAL, "float", true);

        for (String name : List.of("min", "max", "sum", "avg")) {
            assertThat(MetricAggregation.from(function(name, name.equals("avg") ? DOUBLE : INTEGER, "i", INTEGER), Map.of("i", integer), "a")).as(name + " integer").isPresent();
            assertThat(MetricAggregation.from(function(name, DOUBLE, "d", DOUBLE), Map.of("d", doubleColumn), "a")).as(name + " double").isPresent();
        }
        // REAL is accepted for min and max only
        assertThat(MetricAggregation.from(function("min", REAL, "r", REAL), Map.of("r", real), "a")).isPresent();
        assertThat(MetricAggregation.from(function("max", REAL, "r", REAL), Map.of("r", real), "a")).isPresent();
        assertThat(MetricAggregation.from(function("sum", REAL, "r", REAL), Map.of("r", real), "a")).isEmpty();
        assertThat(MetricAggregation.from(function("avg", REAL, "r", REAL), Map.of("r", real), "a")).isEmpty();
    }

    @Test
    public void testBigintIsRejectedForNumericFunctions()
    {
        Map<String, ColumnHandle> assignments = Map.of("regionkey", bigintColumn("regionkey"));
        for (String name : List.of("min", "max", "sum", "avg")) {
            assertThat(MetricAggregation.from(function(name, BIGINT, "regionkey", BIGINT), assignments, "a")).as(name).isEmpty();
        }
    }

    @Test
    public void testBigintIsAcceptedOnlyWhenRequested()
    {
        OpenSearchColumnHandle bigint = bigintColumn("regionkey");
        Map<String, ColumnHandle> assignments = Map.of("regionkey", bigint);
        for (String name : List.of("min", "max", "sum", "avg")) {
            Type outputType = name.equals("avg") ? DOUBLE : BIGINT;
            AggregateFunction function = function(name, outputType, "regionkey", BIGINT);
            assertThat(MetricAggregation.from(function, assignments, "a", MetricAggregation.DEFAULT_FUNCTIONS, false)).as(name + " flag off").isEmpty();
            assertThat(MetricAggregation.from(function, assignments, "a", MetricAggregation.SQL_FUNCTIONS)).as(name + " default overload").isEmpty();
            assertThat(MetricAggregation.from(function, assignments, "a", MetricAggregation.DEFAULT_FUNCTIONS, true)).as(name + " flag on")
                    .hasValue(new MetricAggregation(name, outputType, Optional.of(bigint), "a"));
        }
    }

    @Test
    public void testBigintFlagDoesNotAcceptStatisticalFunctionsOrUnsupportedColumns()
    {
        Map<String, ColumnHandle> assignments = Map.of(
                "b", bigintColumn("b"),
                "r", column("r", REAL, "float", true));

        for (String name : List.of("stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop")) {
            assertThat(MetricAggregation.from(function(name, DOUBLE, "b", BIGINT), assignments, "a", MetricAggregation.SQL_FUNCTIONS, true)).as(name).isEmpty();
        }
        // the flag does not widen the other input types
        assertThat(MetricAggregation.from(function("sum", REAL, "r", REAL), assignments, "a", MetricAggregation.SQL_FUNCTIONS, true)).isEmpty();
    }

    @Test
    public void testStatisticalFunctionsOnlyThroughSqlFunctionSet()
    {
        OpenSearchColumnHandle doubleColumn = column("d", DOUBLE, "double", true);
        OpenSearchColumnHandle integer = column("i", INTEGER, "integer", true);
        Map<String, ColumnHandle> assignments = Map.of("d", doubleColumn, "i", integer);

        for (String name : List.of("stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop")) {
            AggregateFunction function = function(name, DOUBLE, "d", DOUBLE);
            assertThat(MetricAggregation.from(function, assignments, "a")).as(name + " default set").isEmpty();
            assertThat(MetricAggregation.from(function, assignments, "a", MetricAggregation.SQL_FUNCTIONS)).as(name + " sql set").isPresent();
        }
        assertThat(MetricAggregation.from(function("stddev", DOUBLE, "d", DOUBLE), assignments, "a", MetricAggregation.SQL_FUNCTIONS).orElseThrow().functionName())
                .isEqualTo("stddev_samp");
        assertThat(MetricAggregation.from(function("variance", DOUBLE, "d", DOUBLE), assignments, "a", MetricAggregation.SQL_FUNCTIONS).orElseThrow().functionName())
                .isEqualTo("var_samp");
        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "i", INTEGER), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isPresent();
        assertThat(MetricAggregation.canonicalFunctionName("sum")).isEqualTo("sum");
    }

    @Test
    public void testStatisticalFunctionsRejectBigintAndReal()
    {
        Map<String, ColumnHandle> assignments = Map.of(
                "b", bigintColumn("b"),
                "r", column("r", REAL, "float", true));

        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "b", BIGINT), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isEmpty();
        assertThat(MetricAggregation.from(function("var_pop", DOUBLE, "r", REAL), assignments, "a", MetricAggregation.SQL_FUNCTIONS)).isEmpty();
    }

    @Test
    public void testRejectsUnsupportedShapes()
    {
        Map<String, ColumnHandle> assignments = Map.of("i", column("i", INTEGER, "integer", true));
        Variable variable = new Variable("i", INTEGER);

        // DISTINCT
        assertThat(MetricAggregation.from(new AggregateFunction("count", BIGINT, List.of(variable), List.of(), true, Optional.empty()), assignments, "a")).isEmpty();
        // filter
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(variable), List.of(), false, Optional.of(new Variable("f", BOOLEAN))), assignments, "a")).isEmpty();
        // ordering
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(variable), List.of(new SortItem("i", SortOrder.ASC_NULLS_LAST)), false, Optional.empty()), assignments, "a")).isEmpty();
        // unsupported function
        assertThat(MetricAggregation.from(new AggregateFunction("stddev", DOUBLE, List.of(variable), List.of(), false, Optional.empty()), assignments, "a")).isEmpty();
        // expression argument
        ConnectorExpression expression = new Call(INTEGER, new FunctionName("$add"), List.of(variable, variable));
        assertThat(MetricAggregation.from(new AggregateFunction("sum", BIGINT, List.of(expression), List.of(), false, Optional.empty()), assignments, "a")).isEmpty();
        // text column and builtin column
        assertThat(MetricAggregation.from(function("count", BIGINT, "t", VARCHAR), Map.of("t", textColumn("t")), "a")).isEmpty();
        assertThat(MetricAggregation.from(function("count", BIGINT, "_id", VARCHAR), Map.of("_id", column("_id", VARCHAR, "text", true)), "a")).isEmpty();
    }

    private static AggregateFunction function(String name, Type outputType)
    {
        return new AggregateFunction(name, outputType, List.of(), List.of(), false, Optional.empty());
    }

    private static AggregateFunction function(String name, Type outputType, String variable, Type inputType)
    {
        return new AggregateFunction(name, outputType, List.of(new Variable(variable, inputType)), List.of(), false, Optional.empty());
    }

    private static OpenSearchColumnHandle column(String name, Type type, String opensearchType, boolean supportsPredicates)
    {
        return new OpenSearchColumnHandle(List.of(name), type, new IndexMetadata.PrimitiveType(opensearchType), decoder(type, name), supportsPredicates);
    }

    private static DecoderDescriptor decoder(Type type, String name)
    {
        if (type.equals(BOOLEAN)) {
            return new BooleanDecoder.Descriptor(name);
        }
        if (type.equals(DOUBLE)) {
            return new DoubleDecoder.Descriptor(name);
        }
        if (type.equals(REAL)) {
            return new RealDecoder.Descriptor(name);
        }
        return new IntegerDecoder.Descriptor(name);
    }
}
