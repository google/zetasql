

# Aggregate functions

An *aggregate function* is a function that summarizes the rows of a group into a
single value. `COUNT`, `MIN` and `MAX` are examples of aggregate functions.

```sql
SELECT COUNT(*) as total_count, COUNT(fruit) as non_null_count,
       MIN(fruit) as min, MAX(fruit) as max
FROM (SELECT NULL as fruit UNION ALL
      SELECT "apple" as fruit UNION ALL
      SELECT "pear" as fruit UNION ALL
      SELECT "orange" as fruit)

+-------------+----------------+-------+------+
| total_count | non_null_count | min   | max  |
+-------------+----------------+-------+------+
| 4           | 3              | apple | pear |
+-------------+----------------+-------+------+
```

When used in conjunction with a `GROUP BY` clause, the groups summarized
typically have at least one row. When the associated `SELECT` has no `GROUP BY`
clause or when certain aggregate function modifiers filter rows from the group
to be summarized it is possible that the aggregate function needs to summarize
an empty group. In this case, the `COUNT` and `COUNTIF` functions return `0`,
while all other aggregate functions return `NULL`.

The following sections describe the aggregate functions that ZetaSQL
supports.

### ANY_VALUE

```sql
ANY_VALUE(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
rows for which `expression` is `NULL` are considered and may be selected.

**Supported Argument Types**

Any

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

Matches the input data type.

**Examples**

```sql
SELECT ANY_VALUE(fruit) as any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+-----------+
| any_value |
+-----------+
| apple     |
+-----------+
```

```sql
SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+--------+-----------+
| fruit  | any_value |
+--------+-----------+
| pear   | pear      |
| apple  | pear      |
| banana | apple     |
+--------+-----------+
```

### ARRAY_AGG
```sql
ARRAY_AGG(
  [DISTINCT]
  expression
  [{IGNORE|RESPECT} NULLS]
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
[OVER (...)]
```

**Description**

Returns an ARRAY of `expression` values.

**Supported Argument Types**

All data types except ARRAY.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified, the `NULL` values are excluded from the result. If
    `RESPECT NULLS` is specified, the `NULL` values are included in the
    result. If
    neither is specified, the `NULL` values are included in the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

ARRAY

If there are zero input rows, this function returns `NULL`.

**Examples**

```sql
SELECT ARRAY_AGG(x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [2, 1, -2, 3, -2, 1, 2] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+---------------+
| array_agg     |
+---------------+
| [2, 1, -2, 3] |
+---------------+
```

```sql
SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [1, -2, 3, -2, 1] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [1, 1, 2, -2, -2, 2, 3] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [2, 1, -2, 3, -2] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x LIMIT 2) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-----------+
| array_agg |
+-----------+
| [-2, 1]   |
+-----------+
```

```sql
SELECT
  x,
  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+----+-------------------------+
| x  | array_agg               |
+----+-------------------------+
| 1  | [1, 1]                  |
| 1  | [1, 1]                  |
| 2  | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| 2  | [1, 1, 2, -2, -2, 2]    |
| 3  | [1, 1, 2, -2, -2, 2, 3] |
+----+-------------------------+
```

### ARRAY_CONCAT_AGG

```sql
ARRAY_CONCAT_AGG(
  expression
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
```

**Description**

Concatenates elements from `expression` of type
ARRAY, returning a single
ARRAY as a result. This function ignores NULL input
arrays, but respects the NULL elements in non-NULL input arrays.

**Supported Argument Types**

ARRAY

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input arrays, not
    the number of elements in the arrays. An empty array counts as 1. A NULL
    array is not counted.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

ARRAY

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x)) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [5, 6, 7, 8, 9, 1, 2, 3, 4]       |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+--------------------------+
| array_concat_agg         |
+--------------------------+
| [1, 2, 3, 4, 5, 6]       |
+--------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+------------------+
| array_concat_agg |
+------------------+
| [5, 6, 7, 8, 9]  |
+------------------+
```

### AVG
```sql
AVG(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the average of non-`NULL` input values, or `NaN` if the input contains a
`NaN`.

**Supported Argument Types**

Any numeric input type, such as  INT64. Note that, for
floating point input types, the return result is non-deterministic, which
means you might receive a different result each time you use this function.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT AVG(x) as avg
FROM UNNEST([0, 2, 4, 4, 5]) as x;

+-----+
| avg |
+-----+
| 3   |
+-----+
```

```sql
SELECT AVG(DISTINCT x) AS avg
FROM UNNEST([0, 2, 4, 4, 5]) AS x;

+------+
| avg  |
+------+
| 2.75 |
+------+
```

```sql
SELECT
  x,
  AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x;

+------+------+
| x    | avg  |
+------+------+
| NULL | NULL |
| 0    | 0    |
| 2    | 1    |
| 4    | 3    |
| 4    | 4    |
| 5    | 4.5  |
+------+------+
```

### BIT_AND
```sql
BIT_AND(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise AND operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x;

+---------+
| bit_and |
+---------+
| 1       |
+---------+
```

### BIT_OR
```sql
BIT_OR(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise OR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x;

+--------+
| bit_or |
+--------+
| 61601  |
+--------+
```

### BIT_XOR
```sql
BIT_XOR(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise XOR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 5678    |
+---------+
```

```sql
SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

### COUNT

1.

```sql
COUNT(*)  [OVER (...)]
```

2.

```sql
COUNT(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

**Supported Argument Types**

`expression` can be any data type. If
`DISTINCT` is present, `expression` can only be a data type that is
[groupable][agg-data-type-properties].

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

INT64

**Examples**

You can use the `COUNT` function to return the number of rows in a table or the
number of distinct values of an expression. For example:

```sql
SELECT
  COUNT(*) AS count_star,
  COUNT(DISTINCT x) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------------+--------------+
| count_star | count_dist_x |
+------------+--------------+
| 4          | 3            |
+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------+------------+--------------+
| x    | count_star | count_dist_x |
+------+------------+--------------+
| 1    | 3          | 2            |
| 4    | 3          | 2            |
| 4    | 3          | 2            |
| 5    | 1          | 1            |
+------+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(x) OVER (PARTITION BY MOD(x, 3)) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

+------+------------+---------+
| x    | count_star | count_x |
+------+------------+---------+
| NULL | 1          | 0       |
| 1    | 3          | 3       |
| 4    | 3          | 3       |
| 4    | 3          | 3       |
| 5    | 1          | 1       |
+------+------------+---------+
```

If you want to count the number of distinct values of an expression for which a
certain condition is satisfied, this is one recipe that you can use:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Here, `IF` will return the value of `expression` if `condition` is `TRUE`, or
`NULL` otherwise. The surrounding `COUNT(DISTINCT ...)` will ignore the `NULL`
values, so it will count only the distinct values of `expression` for which
`condition` is `TRUE`.

For example, to count the number of distinct positive values of `x`:

```sql
SELECT COUNT(DISTINCT IF(x > 0, x, NULL)) AS distinct_positive
FROM UNNEST([1, -2, 4, 1, -5, 4, 1, 3, -6, 1]) AS x;

+-------------------+
| distinct_positive |
+-------------------+
| 3                 |
+-------------------+
```

Or to count the number of distinct dates on which a certain kind of event
occurred:

```sql
WITH Events AS (
  SELECT DATE '2021-01-01' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-03' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
)
SELECT
  COUNT(DISTINCT IF(event_type = 'FAILURE', event_date, NULL))
    AS distinct_dates_with_failures
FROM Events;

+------------------------------+
| distinct_dates_with_failures |
+------------------------------+
| 2                            |
+------------------------------+
```

### COUNTIF
```sql
COUNTIF(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the count of `TRUE` values for `expression`. Returns `0` if there are
zero input rows, or if `expression` evaluates to `FALSE` or `NULL` for all rows.

Since `expression` must be a `BOOL`, the form
`COUNTIF(DISTINCT ...)` is generally not useful: there is only one distinct
value of `TRUE`. So `COUNTIF(DISTINCT ...)` will return 1 if `expression`
evaluates to `TRUE` for one or more input rows, or 0 otherwise.
Usually when someone wants to combine `COUNTIF` and `DISTINCT`, they
want to count the number of distinct values of an expression for which a certain
condition is satisfied. One recipe to achieve this is the following:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Note that this uses `COUNT`, not `COUNTIF`; the `IF` part has been moved inside.
To learn more, see the examples for [`COUNT`](#count).

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

INT64

**Examples**

```sql
SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive
FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x;

+--------------+--------------+
| num_negative | num_positive |
+--------------+--------------+
| 3            | 4            |
+--------------+--------------+
```

```sql
SELECT
  x,
  COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS num_negative
FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x;

+------+--------------+
| x    | num_negative |
+------+--------------+
| NULL | 0            |
| 0    | 1            |
| -2   | 1            |
| 3    | 1            |
| 4    | 0            |
| 5    | 0            |
| 6    | 1            |
| -7   | 2            |
| -10  | 2            |
+------+--------------+
```

### LOGICAL_AND
```sql
LOGICAL_AND(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the logical AND of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_AND(x) AS logical_and FROM UNNEST([true, false, true]) AS x;

+-------------+
| logical_and |
+-------------+
| false       |
+-------------+
```

### LOGICAL_OR
```sql
LOGICAL_OR(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the logical OR of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_OR(x) AS logical_or FROM UNNEST([true, false, true]) AS x;

+------------+
| logical_or |
+------------+
| true       |
+------------+
```

### MAX
```sql
MAX(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the maximum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties].

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MAX(x) AS max
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| max |
+-----+
| 55  |
+-----+
```

```sql
SELECT x, MAX(x) OVER (PARTITION BY MOD(x, 2)) AS max
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | max  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 8    |
| 4    | 8    |
| 37   | 55   |
| 55   | 55   |
+------+------+
```

### MIN
```sql
MIN(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the minimum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties].

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MIN(x) AS min
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| min |
+-----+
| 4   |
+-----+
```

```sql
SELECT x, MIN(x) OVER (PARTITION BY MOD(x, 2)) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | min  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 4    |
| 4    | 4    |
| 37   | 37   |
| 55   | 37   |
+------+------+
```

### STRING_AGG
```sql
STRING_AGG(
  [DISTINCT]
  expression [, delimiter]
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
[OVER (...)]
```

**Description**

Returns a value (either STRING or
BYTES) obtained by concatenating non-null values.
Returns `NULL` if there are zero input rows or `expression` evaluates to
`NULL` for all rows.

If a `delimiter` is specified, concatenated values are separated by that
delimiter; otherwise, a comma is used as a delimiter.

**Supported Argument Types**

STRING
BYTES

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input strings,
    not the number of characters or bytes in the inputs. An empty string counts
    as 1. A NULL string is not counted.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

STRING
BYTES

**Examples**

```sql
SELECT STRING_AGG(fruit) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+------------------------+
| string_agg             |
+------------------------+
| apple,pear,banana,pear |
+------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| apple & pear & banana & pear |
+------------------------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+-----------------------+
| string_agg            |
+-----------------------+
| apple & pear & banana |
+-----------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| pear & pear & apple & banana |
+------------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+--------------+
| string_agg   |
+--------------+
| apple & pear |
+--------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+---------------+
| string_agg    |
+---------------+
| pear & banana |
+---------------+
```

```sql
SELECT
  fruit,
  STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+--------+------------------------------+
| fruit  | string_agg                   |
+--------+------------------------------+
| NULL   | NULL                         |
| pear   | pear & pear                  |
| pear   | pear & pear                  |
| apple  | pear & pear & apple          |
| banana | pear & pear & apple & banana |
+--------+------------------------------+
```

### SUM
```sql
SUM(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the sum of non-null values.

If the expression is a floating point value, the sum is non-deterministic, which
means you might receive a different result each time you use this function.

**Supported Argument Types**

Any supported numeric data types and INTERVAL.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th><th>INTERVAL</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">INTERVAL</td></tr>
</tbody>

</table>

Special cases:

Returns `NULL` if the input contains only `NULL`s.

Returns `NULL` if the input contains no rows.

Returns `Inf` if the input contains `Inf`.

Returns `-Inf` if the input contains `-Inf`.

Returns `NaN` if the input contains a `NaN`.

Returns `NaN` if the input contains a combination of `Inf` and `-Inf`.

**Examples**

```sql
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 25  |
+-----+
```

```sql
SELECT SUM(DISTINCT x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 15  |
+-----+
```

```sql
SELECT
  x,
  SUM(x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
| x | sum |
+---+-----+
| 3 | 6   |
| 3 | 6   |
| 1 | 10  |
| 4 | 10  |
| 4 | 10  |
| 1 | 10  |
| 2 | 9   |
| 5 | 9   |
| 2 | 9   |
+---+-----+
```

```sql
SELECT
  x,
  SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
| x | sum |
+---+-----+
| 3 | 3   |
| 3 | 3   |
| 1 | 5   |
| 4 | 5   |
| 4 | 5   |
| 1 | 5   |
| 2 | 7   |
| 5 | 7   |
| 2 | 7   |
+---+-----+
```

```sql
SELECT SUM(x) AS sum
FROM UNNEST([]) AS x;

+------+
| sum  |
+------+
| NULL |
+------+
```

### Common clauses

#### HAVING MAX and HAVING MIN clause 
<a id="max_min_clause"></a>

Most aggregate functions support two optional clauses called `HAVING MAX` and
`HAVING MIN`, which restricts the set of rows that a function aggregates to
rows that have a maximal or minimal value in a particular column. The syntax
generally looks like this:

```sql
aggregate_function(expression1 [HAVING {MAX | MIN} expression2])
```

+ `HAVING MAX`: Restricts the set of rows that the
  function aggregates to those having a value for `expression2` equal to the
  maximum value for `expression2` within the group. The  maximum value is
  equal to the result of `MAX(expression2)`.
+ `HAVING MIN` Restricts the set of rows that the
  function aggregates to those having a value for `expression2` equal to the
  minimum value for `expression2` within the group. The minimum value is
  equal to the result of `MIN(expression2)`.

These clauses ignore `NULL` values when computing the maximum or minimum
value unless `expression2` evaluates to `NULL` for all rows.

 These clauses only support
[orderable data types][agg-data-type-properties].

**Example**

In this example, the average rainfall is returned for the most recent year,
2001.

```sql
WITH Precipitation AS
 (SELECT 2001 as year, 'spring' as season, 9 as inches UNION ALL
  SELECT 2001, 'winter', 1 UNION ALL
  SELECT 2000, 'fall', 3 UNION ALL
  SELECT 2000, 'summer', 5 UNION ALL
  SELECT 2000, 'spring', 7 UNION ALL
  SELECT 2000, 'winter', 2)
SELECT AVG(inches HAVING MAX year) as average FROM Precipitation

+---------+
| average |
+---------+
| 5       |
+---------+
```

First, the query gets the rows with the maximum value in the `year` column.
There are two:

```sql
+------+--------+--------+
| year | season | inches |
+------+--------+--------+
| 2001 | spring | 9      |
| 2001 | winter | 1      |
+------+--------+--------+
```

Finally, the query averages the values in the `inches` column (9 and 1) with
this result:

```sql
+---------+
| average |
+---------+
| 5       |
+---------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

<!-- mdlint on -->

