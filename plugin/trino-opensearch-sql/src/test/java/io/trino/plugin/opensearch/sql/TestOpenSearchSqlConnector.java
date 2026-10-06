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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.Request;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.opensearch.sql.OpenSearchServer.OPENSEARCH_IMAGE;
import static io.trino.plugin.opensearch.sql.RestClientUtils.createClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOpenSearchSqlConnector
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "opensearch_sql";
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    private static final String MAPPING =
            """
            {
                "properties": {
                    "g": {"type": "keyword"},
                    "i": {"type": "integer"},
                    "d": {"type": "double"},
                    "l": {"type": "long"},
                    "f": {"type": "float"},
                    "b": {"type": "boolean"},
                    "ts": {"type": "date"}
                }
            }
            """;

    private OpenSearchServer opensearch;
    private RestHighLevelClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        opensearch = new OpenSearchServer(OPENSEARCH_IMAGE, false, ImmutableMap.of());
        client = createClient(opensearch.getAddress());
        warmUpSqlPlugin();
        return OpenSearchSqlQueryRunner.create(opensearch.getAddress(), ImmutableMap.of());
    }

    // Observed on OpenSearch 2.19.4: aggregation statements that arrive at the same time while the SQL plugin is still
    // cold can fail with "HTTP 400: can't evaluate on aggregator: <function>" (the plugin evaluates the aggregation in
    // memory instead of pushing it down). The tests run in parallel, so the plugin is exercised with one statement
    // per aggregate function before the first test starts.
    private void warmUpSqlPlugin()
            throws IOException
    {
        String warmUpIndex = "warm_up_" + randomNameSuffix();
        createIndex(warmUpIndex, MAPPING);
        try {
            index(warmUpIndex, ImmutableMap.of("g", "a", "i", 1, "d", 1.0, "l", 1));
            for (String function : List.of("count(*)", "count(`i`)", "min(`i`)", "max(`i`)", "min(`l`)", "max(`l`)", "sum(`l`)", "avg(`l`)", "sum(`d`)", "avg(`d`)", "stddev_samp(`d`)", "stddev_pop(`d`)", "var_samp(`d`)", "var_pop(`d`)")) {
                Request request = new Request("POST", "/_plugins/_sql");
                request.setJsonEntity(JSON_MAPPER.writeValueAsString(ImmutableMap.of("query", "SELECT %s FROM `%s`".formatted(function, warmUpIndex))));
                client.getLowLevelClient().performRequest(request);
            }
        }
        finally {
            deleteIndex(warmUpIndex);
        }
    }

    @AfterAll
    public final void destroy()
            throws IOException
    {
        if (client != null) {
            client.close();
            client = null;
        }
        if (opensearch != null) {
            opensearch.close();
            opensearch = null;
        }
    }

    @Test
    public void testGlobalAggregatesAreAnsweredBySql()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            // sum and avg of an INTEGER column are planned over CAST(i AS bigint), which cannot be pushed down: see testSumAndAvgOverIntegerColumnsStayInTrino
            String sql = "SELECT count(*), count(i), min(i), max(i), sum(d), avg(d), min(d), max(d) FROM " + table;
            assertThat(query(sql))
                    .matches("VALUES (BIGINT '3', BIGINT '3', INTEGER '1', INTEGER '10', DOUBLE '7.0', DOUBLE '2.3333333333333335', DOUBLE '1.0', DOUBLE '4.0')")
                    .isFullyPushedDown();
            assertPushedDownBySql(sql);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testStatisticalFunctions()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            // population and sample statistics of d = [1.0, 2.0, 4.0]
            String sql = "SELECT stddev(d), stddev_pop(d), variance(d), var_pop(d) FROM " + table;
            Session statistical = statisticalPushdownSession();
            assertPushedDownBySql(statistical, sql);
            MaterializedRow row = getOnlyElement(computeActual(statistical, sql).getMaterializedRows());
            assertThat((Double) row.getField(0)).isCloseTo(1.5275252316519468, within(1e-9));
            assertThat((Double) row.getField(1)).isCloseTo(1.247219128924647, within(1e-9));
            assertThat((Double) row.getField(2)).isCloseTo(2.3333333333333335, within(1e-9));
            assertThat((Double) row.getField(3)).isCloseTo(1.5555555555555556, within(1e-9));
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testStatisticalFunctionsLosePrecisionOverLargeValuesWithSmallSpread()
            throws IOException
    {
        String table = "test_sql_precision_" + randomNameSuffix();
        createIndex(table, MAPPING);
        try {
            StringBuilder payload = new StringBuilder();
            double[] values = new double[100];
            for (int id = 0; id < values.length; id++) {
                values[id] = 1e9 + id % 5;
                payload.append(JSON_MAPPER.writeValueAsString(ImmutableMap.of("index", ImmutableMap.of("_index", table, "_id", String.valueOf(id)))))
                        .append("\n")
                        .append(JSON_MAPPER.writeValueAsString(ImmutableMap.of("d", values[id])))
                        .append("\n");
            }
            bulkIndex(payload.toString());

            // two-pass reference
            double mean = 0;
            for (double value : values) {
                mean += value;
            }
            mean /= values.length;
            double squares = 0;
            for (double value : values) {
                squares += (value - mean) * (value - mean);
            }
            double variancePopulation = squares / values.length;
            double varianceSample = squares / (values.length - 1);

            double[] expected = {Math.sqrt(variancePopulation), variancePopulation, Math.sqrt(varianceSample), varianceSample};

            String sql = "SELECT stddev_pop(d), var_pop(d), stddev(d), variance(d) FROM " + table;
            Session statistical = statisticalPushdownSession();
            assertPushedDownBySql(statistical, sql);
            MaterializedRow pushed = getOnlyElement(computeActual(statistical, sql).getMaterializedRows());

            // Characterization of a precision problem of the OpenSearch 2.19 SQL plugin: it derives the variance from
            // the sum of squares, so the cancellation at 1e9 values with a spread of about 1 loses all digits. Observed:
            // 0.0 for all four functions instead of 1.414.., 2.0, 1.421.. and 2.020... This is the reason why the
            // statistical functions are pushed down only when statistical_pushdown_enabled is set. This test is expected
            // to change when the plugin computes the statistics in a numerically stable way.
            for (int column = 0; column < expected.length; column++) {
                double relativeError = Math.abs((Double) pushed.getField(column) - expected[column]) / expected[column];
                assertThat(relativeError)
                        .as("column %s: precision problem appears fixed upstream; update opensearch-sql.md and this test", column)
                        .isGreaterThan(1e-6);
            }

            // Trino computes the same statistics accurately when the aggregation is not pushed down
            Session unpushed = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, "aggregation_pushdown_enabled", "false")
                    .build();
            MaterializedRow accurate = getOnlyElement(computeActual(unpushed, sql).getMaterializedRows());
            // the default session does not push the statistical functions and returns the same accurate values
            assertThat(getOnlyElement(computeActual(sql).getMaterializedRows())).isEqualTo(accurate);
            for (int column = 0; column < expected.length; column++) {
                assertThat((Double) accurate.getField(column)).as("column %s", column).isCloseTo(expected[column], within(expected[column] * 1e-6));
            }
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testStatisticalFunctionsAreNotPushedDownByDefault()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            String sql = "SELECT stddev(d), stddev_pop(d), variance(d), var_pop(d) FROM " + table;
            assertThat(explain(getSession(), sql)).contains("Aggregate").doesNotContain("SQL_AGGREGATION");
            assertThat(query(sql)).isNotFullyPushedDown(AggregationNode.class);

            MaterializedRow row = getOnlyElement(computeActual(sql).getMaterializedRows());
            assertThat((Double) row.getField(0)).isCloseTo(1.5275252316519468, within(1e-9));
            assertThat((Double) row.getField(1)).isCloseTo(1.247219128924647, within(1e-9));
            assertThat((Double) row.getField(2)).isCloseTo(2.3333333333333335, within(1e-9));
            assertThat((Double) row.getField(3)).isCloseTo(1.5555555555555556, within(1e-9));

            // one statistical function keeps the whole aggregation in Trino
            String mixedSql = "SELECT count(*), stddev(d) FROM " + table;
            assertThat(explain(getSession(), mixedSql)).contains("Aggregate").doesNotContain("SQL_AGGREGATION");
            assertThat(query(mixedSql)).isNotFullyPushedDown(AggregationNode.class);

            // aggregates without statistical functions are still pushed down
            assertPushedDownBySql("SELECT count(*), sum(d) FROM " + table);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testEmptyAndSingleRowSemantics()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            // empty input: count 0, everything else NULL (the plugin returns 0 for sum)
            Session statistical = statisticalPushdownSession();
            String emptySql = "SELECT count(*), count(i), sum(d), avg(d), min(i), max(i), stddev(d), var_pop(d) FROM " + table + " WHERE g = 'no_such_value'";
            assertThat(query(statistical, emptySql))
                    .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))")
                    .isFullyPushedDown();
            assertPushedDownBySql(statistical, emptySql);
            assertPushedDownBySql(statistical, "SELECT sum(d), stddev(d) FROM " + table + " WHERE g = 'no_such_value'");

            // one row: population statistics are 0.0, sample statistics NULL
            String singleRowSql = "SELECT stddev_pop(d), var_pop(d), stddev(d), variance(d) FROM " + table + " WHERE g = 'b'";
            assertThat(query(statistical, singleRowSql))
                    .matches("VALUES (DOUBLE '0.0', DOUBLE '0.0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))")
                    .isFullyPushedDown();
            assertPushedDownBySql(statistical, singleRowSql);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testPredicatesAreRenderedForEachType()
            throws IOException
    {
        String table = "test_sql_predicates_" + randomNameSuffix();
        createIndex(table, MAPPING);
        try {
            index(table, ImmutableMap.<String, Object>builder()
                    .put("g", "a").put("i", 1).put("d", 1.5).put("l", -10).put("f", 0.1).put("b", true).put("ts", "2024-01-01T00:00:00.000Z")
                    .buildOrThrow());
            index(table, ImmutableMap.<String, Object>builder()
                    .put("g", "b").put("i", 2).put("d", -2.5).put("l", 5).put("f", 0.5).put("b", false).put("ts", "2024-01-01T10:20:30.123Z")
                    .buildOrThrow());
            index(table, ImmutableMap.<String, Object>builder()
                    .put("g", "it's").put("i", 3).put("d", 0.0).put("l", -5).put("f", -0.25).put("b", true).put("ts", "2024-06-15T12:00:00.999Z")
                    .buildOrThrow());
            index(table, ImmutableMap.<String, Object>builder()
                    .put("g", "a").put("i", 10).put("d", 3.25).put("l", 100).put("f", 0.75).put("b", false).put("ts", "2024-12-31T23:59:59.001Z")
                    .buildOrThrow());
            // only a keyword value, every other field is missing
            index(table, ImmutableMap.of("g", "c"));
            // no keyword value
            index(table, ImmutableMap.<String, Object>builder()
                    .put("i", 7).put("d", 7.0).put("l", 7).put("f", 7.5).put("b", true).put("ts", "2025-03-01T00:00:00.500Z")
                    .buildOrThrow());
            // no value at all
            index(table, ImmutableMap.of());

            Session unpushed = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, "aggregation_pushdown_enabled", "false")
                    .build();

            // keyword equality and IN
            assertPredicate(table, unpushed, "g = 'a'", 2L, 4.75);
            assertPredicate(table, unpushed, "g IN ('a', 'b')", 3L, 2.25);
            // a string containing a single quote
            assertPredicate(table, unpushed, "g = 'it''s'", 1L, 0.0);
            // integer range
            assertPredicate(table, unpushed, "i >= 2 AND i < 10", 3L, 4.5);
            // long range with a negative bound
            assertPredicate(table, unpushed, "l >= -5 AND l <= 5", 2L, -2.5);
            assertPredicate(table, unpushed, "l < 0", 2L, 1.5);
            // double range
            assertPredicate(table, unpushed, "d > 0.0 AND d <= 3.25", 2L, 4.75);
            assertPredicate(table, unpushed, "d < 0", 1L, -2.5);
            assertPredicate(table, unpushed, "d = 0.0", 1L, 0.0);
            // float range, the REAL value is widened to double when the predicate is rendered
            assertPredicate(table, unpushed, "f = REAL '0.1'", 1L, 1.5);
            assertPredicate(table, unpushed, "f >= REAL '0.1' AND f < REAL '0.6'", 2L, -1.0);
            assertPredicate(table, unpushed, "f < REAL '0'", 1L, 0.0);
            // boolean
            assertPredicate(table, unpushed, "b = true", 3L, 8.5);
            assertPredicate(table, unpushed, "b = false", 2L, 0.75);
            // date range with milliseconds
            assertPredicate(table, unpushed, "ts >= TIMESTAMP '2024-01-01 10:20:30.123' AND ts < TIMESTAMP '2024-06-15 12:00:00.999'", 1L, -2.5);
            assertPredicate(table, unpushed, "ts = TIMESTAMP '2024-12-31 23:59:59.001'", 1L, 3.25);
            assertPredicate(table, unpushed, "ts > TIMESTAMP '2024-06-15 12:00:00.998'", 3L, 10.25);
            // IS NULL and IS NOT NULL
            assertPredicate(table, unpushed, "g IS NULL", 2L, 7.0);
            assertPredicate(table, unpushed, "g IS NOT NULL", 5L, 2.25);
            assertPredicate(table, unpushed, "i IS NOT NULL", 5L, 9.25);
            assertPredicate(table, unpushed, "i IS NULL", 2L, null);
            // OR of ranges
            assertPredicate(table, unpushed, "i < 2 OR i > 7", 2L, 4.75);
            assertPredicate(table, unpushed, "l < -5 OR l > 50", 2L, 4.75);
            assertPredicate(table, unpushed, "i < 2 OR i IS NULL", 3L, 1.5);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testGroupByStaysOnTheDslPathAndIsNotTruncated()
            throws IOException
    {
        String table = "test_sql_groups_" + randomNameSuffix();
        createIndex(table, MAPPING);
        try {
            StringBuilder payload = new StringBuilder();
            for (int id = 0; id < 2500; id++) {
                payload.append(JSON_MAPPER.writeValueAsString(ImmutableMap.of("index", ImmutableMap.of("_index", table, "_id", String.valueOf(id)))))
                        .append("\n")
                        .append(JSON_MAPPER.writeValueAsString(ImmutableMap.of("g", "g%04d".formatted(id))))
                        .append("\n");
            }
            bulkIndex(payload.toString());

            // the SQL plugin would silently truncate the result at 1,000 groups
            assertThat(computeActual("SELECT g, count(*) FROM " + table + " GROUP BY g").getRowCount()).isEqualTo(2500);
            assertThat(query("SELECT count(*) FROM (SELECT g FROM " + table + " GROUP BY g)"))
                    .matches("VALUES BIGINT '2500'");
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testBigintAggregatesAndDistinctStayInTrino()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            assertThat(query("SELECT sum(l), min(l), max(l), avg(l) FROM " + table)).isNotFullyPushedDown(AggregationNode.class);
            assertThat(query("SELECT count(DISTINCT g) FROM " + table)).isNotFullyPushedDown(AggregationNode.class);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testBigintAggregatesAreComputedByTrinoByDefault()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            String sql = "SELECT sum(l), min(l), max(l), avg(l) FROM " + table;
            assertThat(query(sql))
                    .matches("VALUES (BIGINT '6', BIGINT '1', BIGINT '3', DOUBLE '2.0')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(explain(getSession(), sql)).contains("Aggregate").doesNotContain("AGGREGATION:");
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testBigintAggregatesArePushedWhenEnabled()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            Session bigint = bigintPushdownSession();
            String sql = "SELECT sum(l), min(l), max(l), avg(l) FROM " + table;
            assertThat(query(bigint, sql))
                    .matches("VALUES (BIGINT '6', BIGINT '1', BIGINT '3', DOUBLE '2.0')")
                    .isFullyPushedDown();
            assertPushedDownBySql(bigint, sql);

            String filtered = "SELECT count(*), sum(l), min(l), max(l), avg(l) FROM " + table + " WHERE g = 'a'";
            assertThat(query(bigint, filtered)).matches("VALUES (BIGINT '2', BIGINT '3', BIGINT '1', BIGINT '2', DOUBLE '1.5')").isFullyPushedDown();
            assertPushedDownBySql(bigint, filtered);

            // empty input: count is 0 and the other aggregates are NULL
            String empty = "SELECT count(*), sum(l), min(l), max(l), avg(l) FROM " + table + " WHERE g = 'no_such_value'";
            assertThat(query(bigint, empty))
                    .matches("VALUES (BIGINT '0', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE))")
                    .isFullyPushedDown();
            assertPushedDownBySql(bigint, empty);

            // mixed with other column types
            String mixed = "SELECT count(*), sum(l), max(i), sum(d) FROM " + table;
            assertThat(query(bigint, mixed)).matches("VALUES (BIGINT '3', BIGINT '6', INTEGER '10', DOUBLE '7.0')").isFullyPushedDown();
            assertPushedDownBySql(bigint, mixed);

            // statistical functions over BIGINT stay in Trino
            assertThat(explain(statisticalAndBigintPushdownSession(), "SELECT stddev(l) FROM " + table)).contains("Aggregate").doesNotContain("AGGREGATION:");

            // the DSL engine does not push BIGINT
            Session dsl = Session.builder(bigint)
                    .setCatalogSessionProperty(CATALOG, "global_aggregation_engine", "DSL")
                    .build();
            assertThat(explain(dsl, sql)).contains("Aggregate").doesNotContain("AGGREGATION:");
        }
        finally {
            deleteIndex(table);
        }
    }

    // OpenSearch computes these aggregates with doubles: 2^53 + 1 cannot be represented, so the connector refuses to return a result of that size
    @Test
    public void testBigintAggregatesAboveTwoToThe53FailLoudly()
            throws IOException
    {
        String table = "test_sql_bigint_large_" + randomNameSuffix();
        createIndex(table, MAPPING);
        try {
            index(table, ImmutableMap.of("g", "a", "l", 9007199254740993L));
            index(table, ImmutableMap.of("g", "a", "l", 1L));

            Session bigint = bigintPushdownSession();
            String guardMessage = "with magnitude >= 2^53";
            assertThatThrownBy(() -> computeActual(bigint, "SELECT max(l) FROM " + table)).hasMessageContaining("max over BIGINT column 'l'").hasMessageContaining(guardMessage);
            assertThatThrownBy(() -> computeActual(bigint, "SELECT sum(l) FROM " + table)).hasMessageContaining("sum over BIGINT column 'l'").hasMessageContaining(guardMessage);
            // results below the limit are exact even when an input is above it
            assertThat(query(bigint, "SELECT min(l) FROM " + table)).matches("VALUES BIGINT '1'");
            // a filter that excludes the large value avoids the failure
            assertThat(query(bigint, "SELECT max(l), sum(l) FROM " + table + " WHERE l < 100")).matches("VALUES (BIGINT '1', BIGINT '1')");

            // Trino computes the exact result with the setting off
            assertThat(query("SELECT max(l), sum(l), min(l) FROM " + table)).matches("VALUES (BIGINT '9007199254740993', BIGINT '9007199254740994', BIGINT '1')");
        }
        finally {
            deleteIndex(table);
        }
    }

    // Characterization test: it records the current behavior and is expected to change if cast pushdown
    // (applyProjection of CAST) is added to the connector.
    @Test
    public void testSumAndAvgOverIntegerColumnsStayInTrino()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            // Trino plans sum and avg of an INTEGER column over CAST(i AS bigint), and a cast between the scan and
            // the aggregation prevents the aggregation push down, for the SQL and the DSL path alike
            assertThat(query("SELECT sum(i), avg(i) FROM " + table))
                    .matches("VALUES (BIGINT '14', DOUBLE '4.666666666666667')")
                    .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        }
        finally {
            deleteIndex(table);
        }
    }

    @Test
    public void testEngineSessionProperty()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            Session dsl = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, "global_aggregation_engine", "DSL")
                    .setCatalogSessionProperty(CATALOG, "statistical_pushdown_enabled", "true")
                    .build();
            // base set stays pushed as search aggregations (AGGREGATION), statistical functions still go through SQL
            // when their push down is enabled
            String baseSql = "SELECT count(*), sum(d) FROM " + table;
            assertThat(query(dsl, baseSql)).matches("VALUES (BIGINT '3', DOUBLE '7.0')").isFullyPushedDown();
            assertPushedDownByDsl(dsl, baseSql);
            String statisticalSql = "SELECT stddev(d) FROM " + table;
            assertThat(query(dsl, statisticalSql)).isFullyPushedDown();
            assertPushedDownBySql(dsl, statisticalSql);

            // without the statistical push down they stay in Trino with the DSL engine as well
            Session dslWithoutStatistical = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, "global_aggregation_engine", "DSL")
                    .build();
            // "AGGREGATION:" is also a substring of "SQL_AGGREGATION:", so this assertion excludes both pushdown engines
            assertThat(explain(dslWithoutStatistical, statisticalSql)).contains("Aggregate").doesNotContain("AGGREGATION:");

            // the default engine answers the base set through SQL
            assertPushedDownBySql(baseSql);
        }
        finally {
            deleteIndex(table);
        }
    }

    private void assertPredicate(String table, Session unpushedSession, String where, long expectedCount, Double expectedSum)
    {
        String sql = "SELECT count(*), sum(d) FROM " + table + " WHERE " + where;
        assertPushedDownBySql(sql);

        MaterializedRow pushed = getOnlyElement(computeActual(sql).getMaterializedRows());
        MaterializedRow unpushed = getOnlyElement(computeActual(unpushedSession, sql).getMaterializedRows());
        assertThat(pushed).as(where).isEqualTo(unpushed);
        assertThat(pushed.getField(0)).as(where).isEqualTo(expectedCount);
        assertThat(pushed.getField(1)).as(where).isEqualTo(expectedSum);
    }

    // The EXPLAIN output prints the table handle, which names the engine: SQL_AGGREGATION:<index> for the SQL plugin
    // and AGGREGATION:<index> for search aggregations.
    private void assertPushedDownBySql(String sql)
    {
        assertPushedDownBySql(getSession(), sql);
    }

    private void assertPushedDownBySql(Session session, String sql)
    {
        String plan = explain(session, sql);
        assertThat(plan).doesNotContain("Aggregate").contains("TableScan").contains("SQL_AGGREGATION:");
    }

    private void assertPushedDownByDsl(Session session, String sql)
    {
        String plan = explain(session, sql);
        assertThat(plan).doesNotContain("Aggregate").contains("TableScan").contains("AGGREGATION:").doesNotContain("SQL_AGGREGATION");
    }

    private Session bigintPushdownSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, "bigint_aggregation_pushdown_enabled", "true")
                .build();
    }

    private Session statisticalAndBigintPushdownSession()
    {
        return Session.builder(bigintPushdownSession())
                .setCatalogSessionProperty(CATALOG, "statistical_pushdown_enabled", "true")
                .build();
    }

    private Session statisticalPushdownSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, "statistical_pushdown_enabled", "true")
                .build();
    }

    private String explain(Session session, String sql)
    {
        return computeActual(session, "EXPLAIN " + sql).getOnlyValue().toString();
    }

    // docs: (g=a,i=1,d=1.0,l=1), (g=a,i=3,d=2.0,l=2), (g=b,i=10,d=4.0,l=3)
    private String createStandardIndex()
            throws IOException
    {
        String table = "test_sql_aggregates_" + randomNameSuffix();
        createIndex(table, MAPPING);
        try {
            index(table, ImmutableMap.of("g", "a", "i", 1, "d", 1.0, "l", 1));
            index(table, ImmutableMap.of("g", "a", "i", 3, "d", 2.0, "l", 2));
            index(table, ImmutableMap.of("g", "b", "i", 10, "d", 4.0, "l", 3));
        }
        catch (IOException | RuntimeException e) {
            deleteIndex(table);
            throw e;
        }
        return table;
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = JSON_MAPPER.writeValueAsString(document);
        Request request = new Request("PUT", "/%s/_doc/%s?refresh".formatted(index, System.nanoTime()));
        request.setJsonEntity(json);
        client.getLowLevelClient().performRequest(request);
    }

    private void bulkIndex(String payload)
            throws IOException
    {
        Request request = new Request("POST", "/_bulk?refresh");
        request.setJsonEntity(payload);
        client.getLowLevelClient().performRequest(request);
    }

    private void createIndex(String indexName, String properties)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity("{\"mappings\": " + properties + "}");
        client.getLowLevelClient().performRequest(request);
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        Request request = new Request("DELETE", "/" + indexName);
        client.getLowLevelClient().performRequest(request);
    }
}
