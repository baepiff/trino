# OpenSearch connector

```{raw} html
<img src="../_static/img/opensearch.png" class="connector-logo">
```

The OpenSearch connector allows access to [OpenSearch](https://opensearch.org/)
data from Trino. This document describes how to configure a catalog with the
OpenSearch connector to run SQL queries against OpenSearch.

## Requirements

- OpenSearch 1.1.0 or higher.
- Network access from the Trino coordinator and workers to the OpenSearch nodes.

## Configuration

To configure the OpenSearch connector, create a catalog properties file
`etc/catalog/example.properties` with the following content, replacing the
properties as appropriate for your setup:

```text
connector.name=opensearch
opensearch.host=search.example.com
opensearch.port=9200
opensearch.default-schema-name=default
```

The following table details all general configuration properties:

:::{list-table} OpenSearch configuration properties
:widths: 35, 55, 10
:header-rows: 1

* - Property name
  - Description
  - Default
* - `opensearch.host`
  - The comma-separated list of host names of the OpenSearch cluster. This
    property is required.
  -
* - `opensearch.port`
  - Port to use to connect to OpenSearch.
  - `9200`
* - `opensearch.default-schema-name`
  - The schema that contains all tables defined without a qualifying schema
    name.
  - `default`
* - `opensearch.scroll-size`
  - Sets the maximum number of hits that can be returned with each [OpenSearch
    scroll request](https://opensearch.org/docs/latest/api-reference/scroll/).
  - `1000`
* - `opensearch.scroll-timeout`
  - [Duration](prop-type-duration) for OpenSearch to keep the search context
    alive for scroll requests.
  - `1m`
* - `opensearch.request-timeout`
  - Timeout [duration](prop-type-duration) for all OpenSearch requests.
  - `10s`
* - `opensearch.connect-timeout`
  - Timeout [duration](prop-type-duration) for all OpenSearch connection
    attempts.
  - `1s`
* - `opensearch.backoff-init-delay`
  - The minimum [duration](prop-type-duration) between backpressure retry
    attempts for a single request to OpenSearch. Setting it too low can
    overwhelm an already struggling cluster.
  - `500ms`
* - `opensearch.backoff-max-delay`
  - The maximum [duration](prop-type-duration) between backpressure retry
    attempts for a single request.
  - `20s`
* - `opensearch.max-retry-time`
  - The maximum [duration](prop-type-duration) across all retry attempts for a
    single request.
  - `30s`
* - `opensearch.node-refresh-interval`
  - [Duration](prop-type-duration) between requests to refresh the list of
    available OpenSearch nodes.
  - `1m`
* - `opensearch.ignore-publish-address`
  - Disable using the address published by the OpenSearch API to connect for
    queries. Some deployments map OpenSearch ports to a random public port and
    enabling this property can help in these cases.
  - `false`
* - `opensearch.projection-pushdown-enabled`
  - Read only projected fields from row columns while performing `SELECT` queries
  - `true`
* - `opensearch.aggregation-pushdown-enabled`
  - Push down supported aggregations to OpenSearch. The catalog session property
    `aggregation_pushdown_enabled` overrides this value for a session.
  - `true`
* - `opensearch.text-equality-pushdown.enabled`
  - Push down equality predicates on `text` fields with a `keyword` sub-field,
    see [](opensearch-text-equality-pushdown). The catalog session property
    `text_equality_pushdown_enabled` overrides this value for a session.
  - `false`
* - `opensearch.max-aggregation-buckets`
  - Maximum number of buckets requested in each aggregation search request. The
    connector pages through larger results. Must not exceed the cluster
    setting `search.max_buckets`.
  - `65535`
:::

### Authentication

The connection to OpenSearch can use AWS or password authentication.

To enable AWS authentication and authorization using IAM policies, the
`opensearch.security` option must be set to `AWS`. Additionally, the
following options must be configured:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `opensearch.aws.region`
  - AWS region of the OpenSearch endpoint. This option is required.
* - `opensearch.aws.access-key`
  - AWS access key to use to connect to the OpenSearch domain. If not set, the
    default AWS credentials provider chain is used.
* - `opensearch.aws.secret-key`
  - AWS secret key to use to connect to the OpenSearch domain. If not set, the
    default AWS credentials provider chain is used.
* - `opensearch.aws.iam-role`
  - Optional ARN of an IAM role to assume to connect to OpenSearch. Note that
    the configured IAM user must be able to assume this role.
* - `opensearch.aws.external-id`
  - Optional external ID to pass while assuming an AWS IAM role.
* - `opensearch.aws.deployment-type`
  - AWS OpenSearch deployment type. Possible values are `PROVISIONED` & `SERVERLESS`. This option is required.
:::

To enable password authentication, the `opensearch.security` option must be set
to `PASSWORD`. Additionally the following options must be configured:

:::{list-table}
:widths: 45, 55
:header-rows: 1

* - Property name
  - Description
* - `opensearch.auth.user`
  - Username to use to connect to OpenSearch.
* - `opensearch.auth.password`
  - Password to use to connect to OpenSearch.
:::

### Connection security with TLS

The connector provides additional security options to connect to OpenSearch
clusters with TLS enabled.

If your cluster uses globally-trusted certificates, you only need to
enable TLS. If you require custom configuration for certificates, the connector
supports key stores and trust stores in P12 (PKCS) or Java Key Store (JKS) format.

The available configuration values are listed in the following table:

:::{list-table} TLS configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `opensearch.tls.enabled`
  - Enable TLS security. Defaults to `false`.
* - `opensearch.tls.keystore-path`
  - The path to the P12 (PKCS) or [JKS](/security/inspect-jks)
    key store.
* - `opensearch.tls.truststore-path`
  - The path to P12 (PKCS) or [JKS](/security/inspect-jks)
    trust store.
* - `opensearch.tls.keystore-password`
  - The password for the key store specified by
    `opensearch.tls.keystore-path`.
* - `opensearch.tls.truststore-password`
  - The password for the trust store specified by
    `opensearch.tls.truststore-path`.
* - `opensearch.tls.verify-hostnames`
  - Flag to determine if the hostnames in the certificates must be verified.
    Defaults to `true`.
:::

(opensearch-type-mapping)=
## Type mapping

Because Trino and OpenSearch each support types that the other does not, the
connector [maps some types](type-mapping-overview) when reading data.

### OpenSearch type to Trino type mapping

The connector maps OpenSearch types to the corresponding Trino types
according to the following table:

:::{list-table} OpenSearch type to Trino type mapping
:widths: 30, 30, 50
:header-rows: 1

* - OpenSearch type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `FLOAT`
  - `REAL`
  -
* - `BYTE`
  - `TINYINT`
  -
* - `SHORT`
  - `SMALLINT`
  -
* - `INTEGER`
  - `INTEGER`
  -
* - `LONG`
  - `BIGINT`
  -
* - `KEYWORD`
  - `VARCHAR`
  -
* - `TEXT`
  - `VARCHAR`
  -
* - `DATE`
  - `TIMESTAMP`
  - For more information, see [](opensearch-date-types).
* - `IPADDRESS`
  - `IP`
  -
:::

No other types are supported.

(opensearch-array-types)=
### Array types

Fields in OpenSearch can contain [zero or more
values](https://opensearch.org/docs/latest/field-types/supported-field-types/date/#custom-formats),
but there is no dedicated array type. To indicate a field contains an array, it
can be annotated in a Trino-specific structure in the
[\_meta](https://opensearch.org/docs/latest/field-types/index/#get-a-mapping)
section of the index mapping in OpenSearch.

For example, you can have an OpenSearch index that contains documents with the
following structure:

```json
{
    "array_string_field": ["trino","the","lean","machine-ohs"],
    "long_field": 314159265359,
    "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
    "timestamp_field": "2025-09-17T06:22:48.000Z",
    "object_field": {
        "array_int_field": [86,75,309],
        "int_field": 2
    }
}
```

The array fields of this structure can be defined by using the following command
to add the field property definition to the `_meta.trino` property of the target
index mapping with OpenSearch available at `search.example.com:9200`:

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "_meta": {
        "trino":{
            "array_string_field":{
                "isArray":true
            },
            "object_field":{
                "array_int_field":{
                    "isArray":true
                }
            },
        }
    }
}'
```

:::{note}
It is not allowed to use `asRawJson` and `isArray` flags simultaneously for the same column.
:::

(opensearch-date-types)=
### Date types

The OpenSearch connector supports only the default `date` type. All other
OpenSearch [date] formats including [built-in date formats] and [custom date
formats] are not supported. Dates with the [format] property are ignored.

### Raw JSON transform

Documents in OpenSearch can include more complex structures that are not
represented in the mapping. For example, a single `keyword` field can have
widely different content including a single `keyword` value, an array, or a
multidimensional `keyword` array with any level of nesting.

The following command configures `array_string_field` mapping with OpenSearch
available at `search.example.com:9200`:

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "properties": {
        "array_string_field":{
            "type": "keyword"
        }
    }
}'
```

All the following documents are legal for OpenSearch with `array_string_field`
mapping:

```json
[
    {
        "array_string_field": "trino"
    },
    {
        "array_string_field": ["trino","is","the","best"]
    },
    {
        "array_string_field": ["trino",["is","the","best"]]
    },
    {
        "array_string_field": ["trino",["is",["the","best"]]]
    }
]
```

See the [OpenSearch array
documentation](https://opensearch.org/docs/latest/field-types/supported-field-types/index/#arrays)
for more details.

Further, OpenSearch supports types, such as [k-NN
vector](https://opensearch.org/docs/latest/field-types/supported-field-types/knn-vector/),
that are not supported in Trino. These and other types can cause parsing
exceptions for users that use of these types in OpenSearch. To manage all of
these scenarios, you can transform fields to raw JSON by annotating it in a
Trino-specific structure in the
[\_meta](https://opensearch.org/docs/latest/field-types/index/) section of the
OpenSearch index mapping. This indicates to Trino that the field, and all nested
fields beneath, must be cast to a `VARCHAR` field that contains the raw JSON
content. These fields can be defined by using the following command to add the
field property definition to the `_meta.trino` property of the target index
mapping.

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "_meta": {
      "trino":{
        "array_string_field":{
            "asRawJson":true
        }
      }
    }
}'
```

The preceding configuration causes Trino to return the `array_string_field`
field as a `VARCHAR` containing raw JSON. You can parse these fields with the
[built-in JSON functions](/functions/json).

:::{note}
It is not allowed to use `asRawJson` and `isArray` flags simultaneously for the same column.
:::

## Special columns

The following hidden columns are available:

:::{list-table}
:widths: 25, 75
:header-rows: 1

* - Column
  - Description
* - `_id`
  - The OpenSearch document ID.
* - `_score`
  - The document score returned by the OpenSearch query.
* - `_source`
  - The source of the original document.
:::

(opensearch-sql-support)=
## SQL support

The connector provides [globally available](sql-globally-available) and
[read operation](sql-read-operations) statements to access data and
metadata in the OpenSearch catalog.

### Wildcard table

The connector provides support to query multiple tables using a concise
[wildcard table](https://opensearch.org/docs/latest/api-reference/multi-search/#metadata-only-options)
notation.

```sql
SELECT *
FROM example.web."page_views_*";
```

### Table functions

The connector provides specific [table functions](/functions/table) to
access OpenSearch.

(opensearch-raw-query-function)=
#### `raw_query(varchar) -> table`

The `raw_query` function allows you to query the underlying database directly
using the [OpenSearch Query
DSL](https://opensearch.org/docs/latest/query-dsl/index/) syntax. The full DSL
query is pushed down and processed in OpenSearch. This can be useful for
accessing native features which are not available in Trino, or for improving
query performance in situations where running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

The `raw_query` function requires three parameters:

- `schema`: The schema in the catalog that the query is to be executed on.
- `index`: The index in OpenSearch to search.
- `query`: The query to execute, written in [OpenSearch Query DSL](https://opensearch.org/docs/latest/query-dsl).

The function returns a single row with a `result` column of type `VARCHAR`
containing the JSON response from OpenSearch. Trino does not reorder the
hits within this response. Use the `sort` parameter in the query to specify
the order of the hits. If you expand the hits into SQL rows, use `ORDER BY`
in the outermost query to order those rows.

For example, query the `example` catalog and use the `raw_query` table function
to search for documents in the `orders` index where the country name is
`ALGERIA` as defined as a JSON-formatted query matcher and passed to the
`raw_query` table function in the `query` parameter:

```sql
SELECT
  *
FROM
  TABLE(
    example.system.raw_query(
      schema => 'sales',
      index => 'orders',
      query => '{
        "query": {
          "match": {
            "name": "ALGERIA"
          }
        }
      }'
    )
  );
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

### Parallel data access

The connector requests data from multiple nodes of the OpenSearch cluster for
query processing in parallel.

### Predicate push down

The connector supports [predicate push down](predicate-pushdown) for the
following data types:

:::{list-table}
:widths: 50, 50
:header-rows: 1

* - OpenSearch
  - Trino
* - `boolean`
  - `BOOLEAN`
* - `double`
  - `DOUBLE`
* - `float`
  - `REAL`
* - `byte`
  - `TINYINT`
* - `short`
  - `SMALLINT`
* - `integer`
  - `INTEGER`
* - `long`
  - `BIGINT`
* - `keyword`
  - `VARCHAR`
* - `date`
  - `TIMESTAMP`
:::

No other data types are supported for predicate push down, with one exception
for `text` fields described in the next section.

(opensearch-text-equality-pushdown)=
#### Equality on `text` fields with a `keyword` sub-field

Dynamic mapping, and many explicit mappings, index a string as a `text` field
with a `keyword` sub-field, for example:

```json
"tenantId": {
  "type": "text",
  "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } }
}
```

This push down is disabled by default. Set `opensearch.text-equality-pushdown.enabled`
to `true` in the catalog, or the catalog session property
`text_equality_pushdown_enabled` to `true` for a session, to enable it.

The push down assumes that every document was indexed under the current mapping
of the sub-field. The connector cannot detect from the mapping whether this is
the case. If the `keyword` sub-field was added to an existing `text` field after
documents were indexed, or `ignore_above` was raised later, the older documents
have no terms in the sub-field, and a pushed down predicate silently omits them
from the results. Run `_update_by_query` on the index, or reindex it, before
you enable the push down for such an index. Indices that were created with
dynamic mapping from the start are not affected.

A predicate on such a column that uses `=` or `IN` is pushed down as a `term`
or `terms` query on the `keyword` sub-field, for example on `tenantId.keyword`.
The query returns the same rows as the comparison in Trino, which is
case-sensitive and compares the whole value. The predicate is pushed down only
when all of the following hold:

* The column is a top-level `text` field mapped to `VARCHAR`, without an array
  or raw JSON transform.
* The field has a sub-field of type `keyword` that is indexed and has no
  `normalizer` and no `null_value`. A normalizer, such as a lowercase
  normalizer, changes the indexed terms.
* Every literal is at most `ignore_above` characters long, or at most 32766
  bytes in UTF-8 when the sub-field does not set `ignore_above`. A longer value
  is not indexed in the sub-field, so the predicate stays in Trino to return
  the documents with such a value.
* The predicate lists at most 1024 values, and does not also accept `NULL`.
* When the table is an alias or a wildcard table, every index behind it maps the
  field and the sub-field identically. No field is copied into the field or the
  sub-field with `copy_to`.

Range predicates such as `<`, `>`, `<>` and `BETWEEN`, `LIKE`, and `IS NULL`
and `IS NOT NULL` on `text` fields are not pushed down, and are evaluated by
Trino. Grouping, sorting and aggregations on `text` fields are not pushed down.

Two behaviors follow from the predicate being evaluated by OpenSearch on the
indexed terms instead of by Trino on the values read from `_source`. A document
whose `_source` value is an array, a boolean, or a number in a non-canonical
form such as `1.50` can match, or be counted by, a pushed down predicate, while
without the push down reading that document fails or returns a different value.
Fields excluded from `_source` are read as `NULL`, but still match a pushed
down predicate.

(opensearch-aggregation-pushdown)=
### Aggregation push down

The connector supports [aggregation push down](aggregation-pushdown) for these
aggregate functions:

* `count(*)` and `count(column)`
* `min`, `max` on columns of type `TINYINT`, `SMALLINT`, `INTEGER`, `REAL`,
  `DOUBLE`
* `sum`, `avg` on columns of type `DOUBLE`

Aggregation push down is applied only when all of the following hold:

* The query groups by none or by columns of type `VARCHAR` (`keyword`),
  `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, or `BOOLEAN`, with a single
  grouping set. `GROUPING SETS`, `CUBE` and `ROLLUP` are not pushed down.
* The group-by columns and aggregate arguments are plain columns that support
  predicate push down. Built-in columns such as `_id` and columns with a raw
  JSON transform are not pushed down.
* The aggregates do not use `DISTINCT`, a `FILTER` clause, or an `ORDER BY`
  clause.
* The table is not accessed with the `raw_query` table function.
* The query does not already have a pushed-down limit or TopN. For example,
  `SELECT count(*) FROM (SELECT * FROM t LIMIT 10)` is not pushed down.

`min`, `max`, `sum` and `avg` over `BIGINT` columns are not pushed down because
OpenSearch computes metric aggregations with double precision, which cannot
represent all `BIGINT` values. `sum` and `avg` over `TINYINT`, `SMALLINT` and
`INTEGER` columns are not pushed down either: Trino evaluates them over a cast
of the column to `BIGINT`, and a cast between the table scan and the aggregation
prevents the push down, so these aggregates stay in Trino. `min` and `max` over
`keyword` columns are not pushed down.

Pushed-down aggregation, sorting and grouping use the OpenSearch doc values of
a field, so the result can differ from a computation in Trino for `keyword`
fields that use a `normalizer`, `ignore_above`, `copy_to`, or
`doc_values: false`. The aggregation search applies the filter of an index
alias, as a regular search does. A failed shard fails the query instead of
returning partial results.

Set `opensearch.aggregation-pushdown-enabled` to `false` to disable aggregation
push down.

(opensearch-topn-pushdown)=
### TopN push down

The connector supports [TopN push down](topn-pushdown) for queries with
`ORDER BY ... LIMIT n`. It is applied when every sort column supports predicate
push down. Built-in columns such as `_id` and columns with a raw JSON transform
are not pushed down. Each shard returns its sorted top `n` rows and Trino
merges them. The sort uses the OpenSearch doc values of the field. Indices of an
alias or wildcard table that do not map a sort column are treated as having no
value for it.

[built-in date formats]: https://opensearch.org/docs/latest/field-types/supported-field-types/date/#custom-formats
[custom date formats]: https://opensearch.org/docs/latest/field-types/supported-field-types/date/#custom-formats
[date]: https://opensearch.org/docs/latest/field-types/supported-field-types/date/
[format]: https://opensearch.org/docs/latest/query-dsl/term/range/#format
[full text query]: https://opensearch.org/docs/latest/query-dsl/full-text/query-string/
