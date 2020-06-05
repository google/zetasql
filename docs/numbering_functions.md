

# Numbering Functions

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

**Description**

Returns the ordinal (1-based) rank of each row within the ordered partition.
All peer rows receive the same rank value. The next row or set of peer rows
receives a rank value which increments by the number of peers with the previous
rank value, instead of `DENSE_RANK`, which always increments by 1.

**Supported Argument Types**

INT64

### DENSE_RANK

**Description**

Returns the ordinal (1-based) rank of each row within the window partition.
All peer rows receive the same rank value, and the subsequent rank value is
incremented by one.

**Supported Argument Types**

INT64

### PERCENT_RANK

**Description**

Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
the <code>RANK</code> of the row and NR is the number of rows in the partition.
Returns 0 if NR=1.

**Supported Argument Types**

DOUBLE

### CUME_DIST

**Description**

Return the relative rank of a row defined as NP/NR. NP is defined to be the
number of rows that either precede or are peers with the current row. NR is the
number of rows in the partition.

**Supported Argument Types**

DOUBLE

### NTILE

```
NTILE(constant_integer_expression)
```

**Description**

This function divides the rows into <code>constant_integer_expression</code>
buckets based on row ordering and returns the 1-based bucket number that is
assigned to each row. The number of rows in the buckets can differ by at most 1.
The remainder values (the remainder of number of rows divided by buckets) are
distributed one for each bucket, starting with bucket 1. If
<code>constant_integer_expression</code> evaluates to NULL, 0 or negative, an
error is provided.

**Supported Argument Types**

INT64

### ROW_NUMBER

**Description**

Does not require the <code>ORDER BY</code> clause. Returns the sequential
row ordinal (1-based) of each row for each ordered partition. If the
<code>ORDER BY</code> clause is unspecified then the result is
non-deterministic.

**Supported Argument Types**

INT64

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[numbering-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#numbering_function_concepts

