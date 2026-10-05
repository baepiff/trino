# OpenSearch SQL connector

```{raw} html
<img src="../_static/img/opensearch.png" class="connector-logo">
```

The OpenSearch SQL connector allows access to [OpenSearch](https://opensearch.org/)
data from Trino. It extends the [](/connector/opensearch) and answers global
aggregations by running a SQL statement through the [OpenSearch SQL
plugin](https://opensearch.org/docs/latest/search-plugins/sql/index/) instead
of a search request with aggregations. Everything else, including reading data,
predicate push down, TopN push down and grouped aggregations, behaves as in the
OpenSearch connector.

## Requirements

- OpenSearch 2.19, the version this connector was developed against. Other
  versions are not verified.
- The OpenSearch SQL plugin installed and enabled. The cluster setting
  `plugins.sql.enabled` must not be set to `false`.
- Network access from the Trino coordinator and workers to the OpenSearch nodes.

## Configuration

To configure the OpenSearch SQL connector, create a catalog properties file
`etc/catalog/example.properties` with the following content, replacing the
properties as appropriate for your setup:

```text
connector.name=opensearch_sql
opensearch.host=search.example.com
opensearch.port=9200
opensearch.default-schema-name=default
```

All configuration properties of the OpenSearch connector, for example
`opensearch.host`, the authentication and TLS properties, and
`opensearch.aggregation-pushdown-enabled`, apply to this connector in the same
way. See the [](/connector/opensearch) documentation for details. The
connector adds the following property:

:::{list-table} OpenSearch SQL configuration properties
:widths: 35, 55, 10
:header-rows: 1

* - Property name
  - Description
  - Default
* - `opensearch.sql.global-aggregation-engine`
  - Engine that answers global aggregations. With `SQL`, supported global
    aggregations are pushed down as SQL statements to the OpenSearch SQL
    plugin. With `DSL`, the aggregations that the OpenSearch connector supports
    are pushed down as search aggregations, and only the statistical functions
    `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp` and `var_pop`
    use the SQL plugin. The
    catalog session property `global_aggregation_engine` overrides this value
    for a session.
  - `SQL`
:::

Aggregation push down as a whole can still be disabled with
`opensearch.aggregation-pushdown-enabled` or the catalog session property
`aggregation_pushdown_enabled`.

(opensearch-sql-aggregation-pushdown)=
## Aggregation push down

The connector pushes a global aggregation, which is an aggregation without
`GROUP BY`, down to the SQL plugin when all of the following hold:

- The aggregate functions and their input types are listed in the following
  table.
- The aggregate arguments are plain columns that support predicate push down.
  Built-in columns such as `_id` and nested columns are not pushed down.
- The aggregates do not use `DISTINCT`, a `FILTER` clause, or an `ORDER BY`
  clause.
- The table is not accessed with the `raw_query` table function, has no pushed
  down `LIKE` pattern, and has no pushed down limit or TopN.
- The predicates that are pushed down to the table can be written as a SQL
  `WHERE` clause. This covers conditions on `BOOLEAN`, `TINYINT`, `SMALLINT`,
  `INTEGER`, `BIGINT`, `REAL`, `DOUBLE`, `VARCHAR` (`keyword`) and
  `TIMESTAMP(3)` columns. Strings that contain a backslash or control
  characters, `TIMESTAMP` values with a precision above milliseconds and `DOUBLE`
  values with a very large magnitude are not written, and the aggregation
  is then not pushed down to the SQL plugin.

If any of these conditions is not met, the connector falls back to the push down
of the OpenSearch connector, or to the processing in Trino.

:::{list-table} Aggregate functions pushed down to the SQL plugin
:widths: 40, 60
:header-rows: 1

* - Function
  - Input types
* - `count(*)`
  -
* - `count(column)`
  - Any column that supports predicate push down.
* - `min(column)`, `max(column)`
  - `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE`
* - `sum(column)`, `avg(column)`
  - `DOUBLE`. Trino evaluates `sum` and `avg` of `TINYINT`, `SMALLINT` and
    `INTEGER` columns over a `BIGINT` cast of the column, and such a cast
    prevents the push down. These aggregates stay in Trino.
* - `stddev(column)`, `stddev_samp(column)`, `stddev_pop(column)`,
    `variance(column)`, `var_samp(column)`, `var_pop(column)`
  - `DOUBLE`. Arguments that Trino casts first, such as `INTEGER` columns, are
    not pushed down.
:::

The following operations stay in Trino:

- Any aggregation with `GROUP BY`, `GROUPING SETS`, `CUBE` or `ROLLUP`. The
  OpenSearch connector pushes supported grouped aggregations down as search
  aggregations.
- `min`, `max`, `sum` and `avg` over `BIGINT` columns. The connector does not
  push them down because the aggregate values can exceed the range of
  `DOUBLE` values that are represented exactly.
- Aggregates with `DISTINCT`, such as `count(DISTINCT column)`.
- Aggregates over expressions rather than plain columns.

The connector reads the aggregate values from the SQL plugin response and
adjusts the following results so that they match the results of Trino:

- `sum` over no rows returns `NULL`. The SQL plugin returns `0` for this case.
  The connector adds a `count(column)` to the statement to detect the case. The
  statement always includes a `count(*)` check column too.
- `stddev_pop` and `var_pop` over exactly one row return `0.0`. The SQL plugin
  returns `NULL` for this case.
- `stddev`, `stddev_samp`, `variance`, `var_samp` and the other sample
  statistics over fewer than two rows return `NULL`.

(opensearch-sql-legacy-engine)=
### Legacy engine detection

The SQL plugin can answer a statement with its legacy engine when the new engine
does not support the statement. The legacy engine can return different result
types, for example a floating point value for `count(*)`. Every generated
statement includes a `count(*)` check column, which is shared with a `count(*)`
in your query, and the connector checks its result type. If the response does not have the expected type, the query fails with an
error that names the property `opensearch.sql.global-aggregation-engine=DSL`.
To avoid the SQL path for such a cluster, set the property to `DSL`. With that
setting only the statistical functions require the SQL plugin.

## Limitations

- Only global aggregations use the SQL plugin. Grouped aggregations use search
  aggregations with composite paging, because the SQL plugin truncates the
  result of a `GROUP BY` query at 1,000 groups without reporting an error.
- The connector does not use SQL cursors. Because a global aggregation returns
  a single row, they are not needed.
- Pushed down aggregations use the SQL plugin of the cluster. The plugin is not
  part of every OpenSearch distribution or deployment. Query failures that
  originate in the SQL plugin are reported with the error message of the plugin.
- The OpenSearch SQL plugin is a separate component with its own limits and
  behavior, and the connector was developed against OpenSearch 2.19. Run
  representative queries on your own cluster and version before relying on the
  results.
