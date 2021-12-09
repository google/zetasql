

# Approximate aggregate functions

Approximate aggregate functions are scalable in terms of memory usage and time,
but produce approximate results instead of exact results. These functions
typically require less memory than [exact aggregation functions][aggregate-functions-reference]
like `COUNT(DISTINCT ...)`, but also introduce statistical uncertainty.
This makes approximate aggregation appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

The approximate aggregate functions in this section work directly on the
input data, rather than an intermediate estimation of the data. These functions
_do not allow_ users to specify the precision for the estimation with
sketches. If you would like specify precision with sketches, see:

+  [HyperLogLog++ functions][hll-functions] to estimate cardinality.
+  [KLL16 functions][kll-functions] to estimate quantile values.

### APPROX_COUNT_DISTINCT

```sql
APPROX_COUNT_DISTINCT(
  expression
)
```

**Description**

Returns the approximate result for `COUNT(DISTINCT expression)`. The value
returned is a statistical estimate&mdash;not necessarily the actual value.

This function is less accurate than `COUNT(DISTINCT expression)`, but performs
better on huge input.

**Supported Argument Types**

Any data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

INT64

**Examples**

```sql
SELECT APPROX_COUNT_DISTINCT(x) as approx_distinct
FROM UNNEST([0, 1, 1, 2, 3, 5]) as x;

+-----------------+
| approx_distinct |
+-----------------+
| 5               |
+-----------------+
```

### APPROX_QUANTILES

```sql
APPROX_QUANTILES(
  [DISTINCT]
  expression, number
  [{IGNORE|RESPECT} NULLS]
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate boundaries for a group of `expression` values, where
`number` represents the number of quantiles to create. This function returns
an array of `number` + 1 elements, where the first element is the approximate
minimum and the last element is the approximate maximum.

**Supported Argument Types**

`expression` can be any supported data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

`number` must be INT64.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified, the `NULL` values are excluded from the result. If
    `RESPECT NULLS` is specified, the `NULL` values are included in the
    result. If neither is specified, the `NULL`
    values are excluded from the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of the type specified by the `expression`
parameter.

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT APPROX_QUANTILES(x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 5, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 100)[OFFSET(90)] AS percentile_90
FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS x;

+---------------+
| percentile_90 |
+---------------+
| 9             |
+---------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 6, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 4, 10]    |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 6, 10]    |
+------------------+
```

### APPROX_TOP_COUNT

```sql
APPROX_TOP_COUNT(
  expression, number
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate top elements of `expression`. The `number` parameter
specifies the number of elements returned.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields. The first field
(named `value`) contains an input value. The second field (named `count`)
contains an INT64 specifying the number of times the
value was returned.

Returns `NULL` if there are zero input rows.

**Examples**

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST(["apple", "apple", "pear", "pear", "pear", "banana"]) as x;

+-------------------------+
| approx_top_count        |
+-------------------------+
| [{pear, 3}, {apple, 2}] |
+-------------------------+
```

**NULL handling**

APPROX_TOP_COUNT does not ignore NULLs in the input. For example:

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST([NULL, "pear", "pear", "pear", "apple", NULL]) as x;

+------------------------+
| approx_top_count       |
+------------------------+
| [{pear, 3}, {NULL, 2}] |
+------------------------+
```

### APPROX_TOP_SUM

```sql
APPROX_TOP_SUM(
  expression, weight, number
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate top elements of `expression`, based on the sum of an
assigned `weight`. The `number` parameter specifies the number of elements
returned.

If the `weight` input is negative or `NaN`, this function returns an error.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`weight` must be one of the following:

<ul>
<li>INT64</li>

<li>UINT64</li>

<li>NUMERIC</li>

<li>BIGNUMERIC</li>

<li>DOUBLE</li>
</ul>

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields: `value` and `sum`.
The `value` field contains the value of the input expression. The `sum` field is
the same type as `weight`, and is the approximate sum of the input weight
associated with the `value` field.

Returns `NULL` if there are zero input rows.

**Examples**

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([
  STRUCT("apple" AS x, 3 AS weight),
  ("pear", 2),
  ("apple", 0),
  ("banana", 5),
  ("pear", 4)
]);

+--------------------------+
| approx_top_sum           |
+--------------------------+
| [{pear, 6}, {banana, 5}] |
+--------------------------+
```

**NULL handling**

APPROX_TOP_SUM does not ignore NULL values for the `expression` and `weight`
parameters.

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, NULL AS weight), ("pear", 0), ("pear", NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{pear, 0}, {apple, NULL}] |
+----------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, 2)]);

+-------------------------+
| approx_top_sum          |
+-------------------------+
| [{NULL, 2}, {apple, 0}] |
+-------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{apple, 0}, {NULL, NULL}] |
+----------------------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[hll-functions]: https://github.com/google/zetasql/blob/master/docs/hll_functions.md#hyperloglog_functions

[kll-functions]: https://github.com/google/zetasql/blob/master/docs/kll_functions.md#kll16_quantile_functions

[aggregate-functions-reference]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

<!-- mdlint on -->

