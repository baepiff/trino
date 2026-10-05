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
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.opensearch.sql.OpenSearchServer.OPENSEARCH_IMAGE;
import static io.trino.plugin.opensearch.sql.RestClientUtils.createClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
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
        return OpenSearchSqlQueryRunner.create(opensearch.getAddress(), ImmutableMap.of());
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
            assertPushedDown(sql);
            MaterializedRow row = getOnlyElement(computeActual(sql).getMaterializedRows());
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
    public void testEmptyAndSingleRowSemantics()
            throws IOException
    {
        String table = createStandardIndex();
        try {
            // empty input: count 0, everything else NULL (the plugin returns 0 for sum)
            assertThat(query("SELECT count(*), count(i), sum(d), avg(d), min(i), max(i), stddev(d), var_pop(d) FROM " + table + " WHERE g = 'no_such_value'"))
                    .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
            assertPushedDown("SELECT sum(d), stddev(d) FROM " + table + " WHERE g = 'no_such_value'");

            // one row: population statistics are 0.0, sample statistics NULL
            assertThat(query("SELECT stddev_pop(d), var_pop(d), stddev(d), variance(d) FROM " + table + " WHERE g = 'b'"))
                    .matches("VALUES (DOUBLE '0.0', DOUBLE '0.0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
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
                    .build();
            // base set stays pushed (DSL aggregation), statistical functions still go through SQL
            assertThat(query(dsl, "SELECT count(*), sum(d) FROM " + table)).isFullyPushedDown();
            assertPushedDown(dsl, "SELECT stddev(d) FROM " + table);
        }
        finally {
            deleteIndex(table);
        }
    }

    private void assertPredicate(String table, Session unpushedSession, String where, long expectedCount, Double expectedSum)
    {
        String sql = "SELECT count(*), sum(d) FROM " + table + " WHERE " + where;
        assertPushedDown(sql);

        MaterializedRow pushed = getOnlyElement(computeActual(sql).getMaterializedRows());
        MaterializedRow unpushed = getOnlyElement(computeActual(unpushedSession, sql).getMaterializedRows());
        assertThat(pushed).as(where).isEqualTo(unpushed);
        assertThat(pushed.getField(0)).as(where).isEqualTo(expectedCount);
        assertThat(pushed.getField(1)).as(where).isEqualTo(expectedSum);
    }

    private void assertPushedDown(String sql)
    {
        assertPushedDown(getSession(), sql);
    }

    private void assertPushedDown(Session session, String sql)
    {
        String plan = computeActual(session, "EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(plan).doesNotContain("Aggregate").contains("TableScan");
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
