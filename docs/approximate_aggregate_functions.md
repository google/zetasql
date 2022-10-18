

# Approximate aggregate functions

The following approximate aggregate functions are available in
ZetaSQL. To learn about the syntax for aggregate function calls, see
[Aggregate function calls][agg-function-calls].

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
+  [KLL functions][kll-functions] to estimate quantile values.

### APPROX_COUNT_DISTINCT

```sql
APPROX_COUNT_DISTINCT(
  expression
)
```

**Description**

Returns the approximate result for `COUNT(DISTINCT expression)`. The value
returned is a statistical estimate, not necessarily the actual value.

This function is less accurate than `COUNT(DISTINCT expression)`, but performs
better on huge input.

**Supported Argument Types**

Any data type **except**:

+ `ARRAY`
+ `STRUCT`
+ `PROTO`

**Returned Data Types**

`INT64`

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
  [ DISTINCT ]
  expression, number
  [ { IGNORE | RESPECT } NULLS ]
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate boundaries for a group of `expression` values, where
`number` represents the number of quantiles to create. This function returns
an array of `number` + 1 elements, where the first element is the approximate
minimum and the last element is the approximate maximum.

Returns `NULL` if there are zero input rows or `expression` evaluates to
`NULL` for all rows.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any supported data type **except**:

  + `ARRAY`
  + `STRUCT`
  + `PROTO`
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<T>` where `T` is the type specified by `expression`.

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
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate top elements of `expression` as an array of `STRUCT`s.
The `number` parameter specifies the number of elements returned.

Each `STRUCT` contains two fields. The first field (named `value`) contains an
input value. The second field (named `count`) contains an `INT64` specifying the
number of times the value was returned.

Returns `NULL` if there are zero input rows.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any data type that the `GROUP BY` clause supports.
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<STRUCT>`

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

`APPROX_TOP_COUNT` does not ignore `NULL`s in the input. For example:

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
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate top elements of `expression`, based on the sum of an
assigned `weight`. The `number` parameter specifies the number of elements
returned.

If the `weight` input is negative or `NaN`, this function returns an error.

The elements are returned as an array of `STRUCT`s.
Each `STRUCT` contains two fields: `value` and `sum`.
The `value` field contains the value of the input expression. The `sum` field is
the same type as `weight`, and is the approximate sum of the input weight
associated with the `value` field.

Returns `NULL` if there are zero input rows.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any data type that the `GROUP BY` clause supports.
+ `weight`: One of the following:

  + `INT64`
  + `UINT64`
  + `NUMERIC`
  + `BIGNUMERIC`
  + `DOUBLE`
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<STRUCT>`

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

`APPROX_TOP_SUM` does not ignore `NULL` values for the `expression` and `weight`
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

[hll-functions]: https://github.com/google/zetasql/blob/master/docs/hll_functions.md#hyperloglog_functions

[kll-functions]: https://github.com/google/zetasql/blob/master/docs/kll_functions.md#kll_quantile_functions

[aggregate-functions-reference]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

