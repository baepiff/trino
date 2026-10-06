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
  `plugins.sql.enabled` must not be set to `false`. On a cluster without the
  plugin, use the [](/connector/opensearch) instead, or set the catalog session
  property `aggregation_pushdown_enabled` to `false`. With
  `opensearch.sql.global-aggregation-engine=DSL` and
  `opensearch.sql.statistical-pushdown-enabled=true` the statistical functions
  still need the SQL plugin.
- When the OpenSearch security plugin is enabled, the user that Trino
  authenticates as needs the permission to use the SQL plugin in addition to
  the permissions to read the indices. See the OpenSearch security
  documentation for the permissions of the SQL plugin.
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
    use the SQL plugin, if their push down is enabled with
    `opensearch.sql.statistical-pushdown-enabled`. The
    catalog session property `global_aggregation_engine` overrides this value
    for a session.
  - `SQL`
* - `opensearch.sql.statistical-pushdown-enabled`
  - Push down the statistical functions `stddev`, `stddev_samp`, `stddev_pop`,
    `variance`, `var_samp` and `var_pop` to the SQL plugin. The plugin loses
    precision for some data, so Trino computes these functions unless you
    enable this property. See
    [](opensearch-sql-statistical-precision). The catalog session property
    `statistical_pushdown_enabled` overrides this value for a session.
  - `false`
* - `opensearch.sql.bigint-aggregation-pushdown-enabled`
  - Push down `min`, `max`, `sum` and `avg` over `BIGINT` columns to the SQL
    plugin. The plugin computes them with `double` values, which are exact only
    up to 2^53, so a `min`, `max` or `sum` result with a magnitude of 2^53 or
    more fails the query. See [](opensearch-sql-bigint-precision). Applies only
    with `opensearch.sql.global-aggregation-engine=SQL`. The catalog session
    property `bigint_aggregation_pushdown_enabled` overrides this value for a
    session.
  - `false`
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
  - `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`, `DOUBLE`, and `BIGINT` only with
    the opt-in property `opensearch.sql.bigint-aggregation-pushdown-enabled`.
* - `sum(column)`, `avg(column)`
  - `DOUBLE`. Trino evaluates `sum` and `avg` of `TINYINT`, `SMALLINT` and
    `INTEGER` columns over a `BIGINT` cast of the column, and such a cast
    prevents the push down. These aggregates stay in Trino.
    `BIGINT` columns only with the opt-in property
    `opensearch.sql.bigint-aggregation-pushdown-enabled`.
* - `stddev(column)`, `stddev_samp(column)`, `stddev_pop(column)`,
    `variance(column)`, `var_samp(column)`, `var_pop(column)`
  - `DOUBLE`, only when `opensearch.sql.statistical-pushdown-enabled` or the
    session property `statistical_pushdown_enabled` is `true`. Arguments that
    Trino casts first, such as `INTEGER` columns, are not pushed down. By
    default these functions are computed by Trino.
:::

The following operations stay in Trino:

- Any aggregation with `GROUP BY`, `GROUPING SETS`, `CUBE` or `ROLLUP`. The
  OpenSearch connector pushes supported grouped aggregations down as search
  aggregations.
- `min`, `max`, `sum` and `avg` over `BIGINT` columns, unless you enable
  `opensearch.sql.bigint-aggregation-pushdown-enabled`. The statistical
  functions over `BIGINT` columns always stay in Trino. With
  `opensearch.sql.global-aggregation-engine=DSL` the `BIGINT` aggregates stay
  in Trino as well, because the search aggregations of the OpenSearch
  connector do not accept `BIGINT` columns.
- Aggregates with `DISTINCT`, such as `count(DISTINCT column)`.
- Aggregates over expressions rather than plain columns.
- The statistical functions, unless their push down is enabled. If an
  aggregation contains one statistical function, the whole aggregation stays in
  Trino.

The connector reads the aggregate values from the SQL plugin response and
adjusts the following results so that they match the results of Trino:

- `sum` over no rows returns `NULL`. The SQL plugin returns `0` for this case.
  The connector adds a `count(column)` to the statement to detect the case. The
  statement always includes a `count(*)` check column too.
- `stddev_pop` and `var_pop` over exactly one row return `0.0`. The SQL plugin
  returns `NULL` for this case.
- `stddev`, `stddev_samp`, `variance`, `var_samp` and the other sample
  statistics over fewer than two rows return `NULL`.

These adjustments for the statistical functions apply only when their push down
is enabled.

(opensearch-sql-legacy-engine)=
### Legacy engine detection

The SQL plugin can answer a statement with its legacy engine when the new engine
does not support the statement. The legacy engine can return different result
types, for example a floating point value for `count(*)`. Every generated
statement includes a `count(*)` check column, which is shared with a `count(*)`
in your query, and the connector checks its result type. If the response does not have the expected type, the query fails with an
error that names the property `opensearch.sql.global-aggregation-engine=DSL`.
To avoid the SQL path for such a cluster, set the property to `DSL`. With that
setting only the statistical functions require the SQL plugin, and only when
their push down is enabled.

(opensearch-sql-cold-start)=
### Retry on cold start

On OpenSearch 2.19.4 the SQL plugin sometimes rejects the first aggregation
statements that arrive concurrently on a cold cluster with the HTTP status 400
and a message that contains `can't evaluate on aggregator`. The same statement
succeeds moments later. The connector retries such a statement once, after 500
milliseconds. Any other error is not retried. If the retry fails as well, the
query fails with the error of the retry, and the first error is attached to it
as a suppressed exception.

### Count range

The new engine of the SQL plugin reports `count` as a 32-bit `integer` on
OpenSearch 2.19, and a larger count could be capped or wrapped without notice.
The connector fails the query when an `integer` typed count is negative or
equal to or greater than 2,147,483,647 (`Integer.MAX_VALUE`). The error message
names the property `opensearch.sql.global-aggregation-engine=DSL`, which
answers counts with search aggregations. A count with the type `long` is
accepted at any size. A `NULL` or non-numeric value in a count column also fails
the query. A count that wraps around past 2^32, which needs more than 4 billion
matching documents, cannot be detected.

(opensearch-sql-statistical-precision)=
### Statistical functions

The connector does not push down `stddev`, `stddev_samp`, `stddev_pop`,
`variance`, `var_samp` and `var_pop` by default, and Trino computes them. The
SQL plugin of OpenSearch 2.19 appears to compute these functions from the sum of
squares, which loses precision when the values are large compared to their
spread. In a test on OpenSearch 2.19.4 with 100 `double` values of the form
`1e9 + (n mod 5)`, the plugin returned `0.0` for `stddev_pop`, `var_pop`,
`stddev` and `variance`, where the exact results are `1.41421...`, `2.0`,
`1.42134...` and `2.0202...`. Trino with push down disabled returned values that
agree with the exact ones to within 1e-6. Pushing the functions down by
default would silently return such wrong results.

If your data has small spreads relative to the values, keep the default. If the
precision is sufficient for your data and you want the SQL plugin to compute the
statistics, set the catalog property
`opensearch.sql.statistical-pushdown-enabled=true`, or the catalog session
property `statistical_pushdown_enabled` to `true` for a session. An aggregation
that contains a statistical function stays completely in Trino when the push
down is not enabled, including the other aggregates of the same query.

(opensearch-sql-bigint-precision)=
### BIGINT aggregates

The SQL plugin computes `min`, `max`, `sum` and `avg` with `double` values. A
`double` represents every integer exactly only up to 2^53
(9,007,199,254,740,992), so the plugin can return a rounded result for larger
`BIGINT` values. Trino computes these aggregates over `BIGINT` columns exactly,
and by default it does so for the connector too, which means it reads all rows
of the column. On a large index this can take minutes, where the SQL plugin
answers in a fraction of a second.

Set `opensearch.sql.bigint-aggregation-pushdown-enabled=true`, or the catalog
session property `bigint_aggregation_pushdown_enabled` to `true` for a session,
to push these aggregates down. The setting has an effect only with
`opensearch.sql.global-aggregation-engine=SQL`. The connector then checks the
result of every pushed down `min`, `max` and `sum` over a `BIGINT` column:

- A result with a magnitude below 2^53 is exact. For `min` and `max` the
  result is one of the stored values, and a stored value below 2^53 is
  represented exactly. A result that is a rounded value of a larger stored
  value is at least 2^53, so it is detected.
- For `sum` of values that all have the same sign, the partial sums grow
  monotonically towards the result. If the result is below 2^53 then every
  partial sum is below 2^53 and exact, so the result is exact.
- A result with a magnitude of 2^53 or more may be rounded. The query then
  fails with an error that names the aggregate function, the column, and the
  settings. It does not contain data values. Add a filter that excludes the large
  values, or set the property to `false` to let Trino compute the exact result.
- `avg` over `BIGINT` returns a `DOUBLE` in Trino as well, so it is not
  checked. For values above 2^53 the result can differ from the result of Trino
  in the last digits.
- A `NULL` result, which an empty input produces, is returned as is.

The check examines only the final value. For a `sum` over values of mixed signs,
an intermediate partial sum above 2^53 could be rounded in the SQL plugin while
the final sum is below 2^53 again, and the check cannot detect this. Typical
non-negative measures, such as durations or counts, are not affected.

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
