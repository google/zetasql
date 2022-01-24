

# Numbering functions

The following sections describe the numbering functions that ZetaSQL
supports. Numbering functions are a subset of analytic functions. For an
explanation of how analytic functions work, see
[Analytic Function Concepts][analytic-function-concepts]. For a
description of how numbering functions work, see the
[Numbering Function Concepts][numbering-function-concepts].

`OVER` clause requirements:

+ `PARTITION BY`: Optional.
+ `ORDER BY`: Required, except for `ROW_NUMBER()`.
+ `window_frame_clause`: Disallowed.

### RANK

```sql
RANK()
```

**Description**

Returns the ordinal (1-based) rank of each row within the ordered partition.
All peer rows receive the same rank value. The next row or set of peer rows
receives a rank value which increments by the number of peers with the previous
rank value, instead of `DENSE_RANK`, which always increments by 1.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 4           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

### DENSE_RANK

```sql
DENSE_RANK()
```

**Description**

Returns the ordinal (1-based) rank of each row within the window partition.
All peer rows receive the same rank value, and the subsequent rank value is
incremented by one.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  DENSE_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 3           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

### PERCENT_RANK

```sql
PERCENT_RANK()
```

**Description**

Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
the `RANK` of the row and NR is the number of rows in the partition.
Returns 0 if NR=1.

**Return Type**

`DOUBLE`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  PERCENT_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+---------------------+
| name            | finish_time            | division | finish_rank         |
+-----------------+------------------------+----------+---------------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 0                   |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 0.33333333333333331 |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 0.33333333333333331 |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 1                   |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 0                   |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 0.33333333333333331 |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 0.66666666666666663 |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 1                   |
+-----------------+------------------------+----------+---------------------+
```

### CUME_DIST

```sql
CUME_DIST()
```

**Description**

Return the relative rank of a row defined as NP/NR. NP is defined to be the
number of rows that either precede or are peers with the current row. NR is the
number of rows in the partition.

**Return Type**

`DOUBLE`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  CUME_DIST() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 0.25        |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 0.75        |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 0.75        |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 1           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 0.25        |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 0.5         |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 0.75        |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 1           |
+-----------------+------------------------+----------+-------------+
```

### NTILE

```sql
NTILE(constant_integer_expression)
```

**Description**

This function divides the rows into `constant_integer_expression`
buckets based on row ordering and returns the 1-based bucket number that is
assigned to each row. The number of rows in the buckets can differ by at most 1.
The remainder values (the remainder of number of rows divided by buckets) are
distributed one for each bucket, starting with bucket 1. If
`constant_integer_expression` evaluates to NULL, 0 or negative, an
error is provided.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  NTILE(3) OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 1           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 3           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 1           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 2           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 3           |
+-----------------+------------------------+----------+-------------+
```

### ROW_NUMBER

```sql
ROW_NUMBER()
```

**Description**

Does not require the `ORDER BY` clause. Returns the sequential
row ordinal (1-based) of each row for each ordered partition. If the
`ORDER BY` clause is unspecified then the result is
non-deterministic.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  ROW_NUMBER() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 3           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 4           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[numbering-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#numbering_function_concepts

<!-- mdlint on -->

