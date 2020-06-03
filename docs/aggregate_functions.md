
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Aggregate functions

An *aggregate function* is a function that performs a calculation on a set of
values. `COUNT`, `MIN` and `MAX` are examples of aggregate functions.

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

The following sections describe the aggregate functions that ZetaSQL
supports.

### ANY_VALUE

```
ANY_VALUE(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
Rows for which `expression` is `NULL` are considered and may be selected.

**Supported Argument Types**

Any

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
ARRAY_AGG([DISTINCT] expression [{IGNORE|RESPECT} NULLS] [HAVING (MAX | MIN) expression2]
          [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
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
    specified,
    the NULL values are excluded from the result. If `RESPECT NULLS` is
    specified or if neither is specified,
    the NULL values are included in the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
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

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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

```
ARRAY_CONCAT_AGG(expression [HAVING (MAX | MIN) expression2]  [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
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

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
AVG([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Returned Data Types**

+ NUMERIC if
  the input type is NUMERIC.
+ DOUBLE

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
```
BIT_AND([DISTINCT] expression [HAVING (MAX | MIN) expression2])
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
BIT_OR([DISTINCT] expression [HAVING (MAX | MIN) expression2])
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
BIT_XOR([DISTINCT] expression [HAVING (MAX | MIN) expression2])
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
COUNT(*)  [OVER (...)]
```

2.
```
COUNT([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

**Supported Argument Types**

`expression` can be any data type.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Types**

INT64

**Examples**

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

### COUNTIF
```
COUNTIF([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the count of `TRUE` values for `expression`. Returns `0` if there are
zero input rows, or if `expression` evaluates to `FALSE` or `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
LOGICAL_AND(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
LOGICAL_OR(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
MAX(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the maximum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any data type except:
`ARRAY`
`STRUCT`
`PROTO`

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
MIN(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the minimum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any data type except:
`ARRAY`
`STRUCT`
`PROTO`

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
STRING_AGG([DISTINCT] expression [, delimiter] [HAVING (MAX | MIN) expression2]  [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
[OVER (...)]
```

**Description**

Returns a value (either STRING or
BYTES) obtained by concatenating non-null values.

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
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
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

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

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
```
SUM([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the sum of non-null values.

If the expression is a floating point value, the sum is non-deterministic, which
means you might receive a different result each time you use this function.

**Supported Argument Types**

Any supported numeric data types.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Types**

+ Returns INT64 if the input is a signed integer.
+ Returns UINT64 if the input is an unsigned integer.
+ Returns
  NUMERIC if the input type is
  NUMERIC.
+ Returns DOUBLE if the input is a floating point
value.

Returns `NULL` if the input contains only `NULL`s.

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

