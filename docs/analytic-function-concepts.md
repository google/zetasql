
<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Analytic Functions Concepts

This topic explains how analytic functions work in ZetaSQL. For a
description of the different analytic functions that ZetaSQL
supports, see the reference topics for
[navigation functions][navigation-functions-reference],
[numbering functions][numbering-functions-reference], and
[aggregate analytic functions][aggregate-analytic-functions-reference].

In databases, an *analytic function* is a function that computes aggregate
values over a group of rows. Unlike [aggregate functions][analytic-functions-link-to-aggregate-functions],
which return a single aggregate value for a group of rows, analytic functions
return a single value for each row by computing the function over a group
of input rows.

Analytic functions are a powerful mechanism for succinctly representing complex
analytic operations, and they enable efficient evaluations that otherwise would
involve expensive self-`JOIN`s or computation outside the SQL query.

Analytic functions are also called "(analytic) window functions" in the SQL
standard and some commercial databases. This is because an analytic function is
evaluated over a group of rows, referred to as a `window` or `window frame`.
In some other databases, they may be referred to as
Online Analytical Processing (OLAP) functions.

Simplified syntax:

```
analytic_function_name ( [ argument_list ] )
  OVER (
    [ PARTITION BY partition_expression_list ]
    [ ORDER BY expression [{ ASC | DESC }] [, ...] ]
    [ window_frame_clause ]
  )
```

An analytic function requires an `OVER` clause, which defines the `window frame`
that the analytic function is evaluated over. The `OVER` clause contains the
following three optional clauses. ZetaSQL evaluates
the sub-clauses of an `OVER` clause in the order in which they are written.

  * A `PARTITION BY` clause divides the input rows into partitions, similar to
    `GROUP BY` but without actually combining rows with the same key.
  * An `ORDER BY` clause specifies the ordering within each partition.
  * A `window_frame_clause` defines the `window frame` within the current
    partition.

The `OVER` clause can also be empty (`OVER()`) in which case the `window frame`
includes all input rows.

Analytic functions are evaluated after aggregation (GROUP BY and non-analytic
aggregate functions).

Example: Consider a company who wants to create a leaderboard for each
department that shows a "seniority ranking" for each employee, i.e. showing
which employees have been there the longest. The table `Employees` contains
columns `Name`, `StartDate`, and `Department`.

The following query calculates the rank of each employee within their
department:

```
SELECT firstname, department, startdate,
  RANK() OVER ( PARTITION BY department ORDER BY startdate ) AS rank
FROM Employees;
```

The conceptual computing process is illustrated in Figure 1.

![Markdown image] (images/analytic-function-illustration.png "Figure 1: Analytic Function Illustration")
**Figure 1: Analytic Function Illustration**

ZetaSQL evaluates the sub-clauses of an `OVER` clause in the order in
which they appear:

  1. `PARTITION BY`: The table is first split into two partitions by
     `department`.
  2. `ORDER BY`: The employee rows in each partition are ordered by `startdate`.
  3. Framing: None. The window frame clause is disallowed for RANK(), as it is
  for all [numbering functions][analytic-functions-link-to-numbering-functions].
  4. `RANK()`: The seniority ranking is computed for each row over the
  `window frame`.

<a name="syntax"></a>
### Analytic Function Syntax

```
analytic_function_name ( [ argument_list ] )
  OVER { window_name | ( [ window_specification ] ) }

window_specification:
  [ window_name ]
  [ PARTITION BY partition_expression_list ]
  [ ORDER BY expression [{ ASC | DESC }] [, ...] ]
  [ window_frame_clause ]

window_frame_clause:
{ ROWS | RANGE }
{
  { UNBOUNDED PRECEDING | numeric_expression PRECEDING | CURRENT ROW }
  |
  { BETWEEN window_frame_boundary_start AND window_frame_boundary_end }
}

window_frame_boundary_start:
{ UNBOUNDED PRECEDING | numeric_expression { PRECEDING | FOLLOWING } | CURRENT ROW }

window_frame_boundary_end:
{ UNBOUNDED FOLLOWING | numeric_expression { PRECEDING | FOLLOWING } | CURRENT ROW }
```

Analytic functions can appear as a scalar expression or a scalar expression
operand in only two places in the query:

+ **The `SELECT` list**. If the analytic function appears in the SELECT list,
its `argument_list` cannot refer to aliases introduced in the same SELECT list.
+ **The `ORDER BY` clause**. If the analytic function appears in the ORDER BY clause
of the query, its `argument_list` can refer to SELECT list aliases.

Additionally, an analytic function cannot refer to another analytic function in
its `argument_list` or its `OVER` clause, even if indirectly through an
alias.

Invalid:

```
SELECT ROW_NUMBER() OVER () AS alias1
FROM Singers
ORDER BY ROW_NUMBER() OVER(PARTITION BY alias1)
```

In the query above, the analytic function `alias1` resolves to an analytic
function: `ROW_NUMBER() OVER()`.

#### OVER Clause

Syntax:

```
OVER { window_name | ( [ window_specification ] ) }

window_specification:
  [ window_name ]
  [ PARTITION BY partition_expression_list ]
  [ ORDER BY sort_specification_list ]
  [ window_frame_clause ]
```

The `OVER` clause has three possible components:

+ `PARTITION BY` clause
+ `ORDER BY` clause
+ A `window_frame_clause` or a `window_name`, which refers to a
  `window_specification` defined in a `WINDOW` clause.

If the `OVER` clause is empty, `OVER()`, the analytic function is computed over
a single partition which contains all input rows, meaning that it will produce
the same result for each output row.

##### PARTITION BY Clause

Syntax:

```
PARTITION BY expression [, ... ]
```

The `PARTITION BY` clause breaks up the input rows into separate partitions,
over which the analytic function is independently evaluated. Multiple
`expressions` are allowed in the `PARTITION BY` clause.

The data type of `expression` must be [groupable](https://github.com/google/zetasql/blob/master/docs/data-types.md#data-type-properties)
and support partitioning. This means the `expression` **cannot** be any of the
following data types:

+ Floating point
+ Protocol buffer

This list is almost identical to the list of data types that `GROUP BY` does not
support, with the additional exclusion of floating point types (see "Groupable"
in the Data Type Properties table at the top of
[ZetaSQL Data Types][analytic-functions-link-to-data-types]).

If no `PARTITION BY` clause is present, ZetaSQL treats the entire
input as a single partition.

##### ORDER BY Clause

Syntax:

```
ORDER BY expression [ ASC | DESC ] [, ... ]
```

The `ORDER BY` clause defines an ordering within each partition. If no
`ORDER BY` clause is present, row ordering within a partition is
non-deterministic. Some analytic functions require `ORDER BY`; this is noted in
the section for each family of analytic functions. Even if an `ORDER BY` clause
is present, some functions are not sensitive to ordering within a `window frame`
(e.g. `COUNT`).

The `ORDER BY` clause within an `OVER` clause is consistent with the normal
[`ORDER BY` clause][analytic-functions-link-to-order-by-clause] in that:

+ There can be multiple `expressions`.
+ `expression` must have a type that supports ordering.
+ An optional `ASC`/`DESC` specification is allowed for each `expression`.
+ NULL values order as the minimum possible value (first for `ASC`,
  last for `DESC`)

Data type support is identical to the normal
[`ORDER BY` clause][analytic-functions-link-to-order-by-clause] in that the following types do **not** support ordering:

+ Array
+ Struct
+ Protocol buffer

If the `OVER` clause contains an `ORDER BY` clause but no `window_frame_clause`,
then the `ORDER BY` implicitly defines `window_frame_clause` as:

```
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

If neither `window_frame_clause` nor the `ORDER BY` clause is present,
the `window frame` defaults to the entire partition.

##### Window Frame Clause

Syntax:

```
{ ROWS | RANGE }
{
  { UNBOUNDED PRECEDING | numeric_expression PRECEDING | CURRENT ROW }
  |
  { BETWEEN window_frame_boundary_start AND window_frame_boundary_end }
}

window_frame_boundary_start:
{ UNBOUNDED PRECEDING | numeric_expression { PRECEDING | FOLLOWING } | CURRENT ROW }

window_frame_boundary_end:
{ UNBOUNDED FOLLOWING | numeric_expression { PRECEDING | FOLLOWING } | CURRENT ROW }
```
`window_frame_clause` defines the `window frame`, around the current row within
a partition, over which the analytic function is evaluated.
`window_frame_clause` allows both physical window frames (defined by `ROWS`) and
logical window frames (defined by `RANGE`). If the `OVER` clause contains an
`ORDER BY` clause but no `window_frame_clause`, then the `ORDER BY` implicitly
defines `window_frame_clause` as:

```
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

If neither `window_frame_clause` nor the `ORDER BY` clause is present,
the `window frame` defaults to the entire partition.

The `numeric_expression` can only be a constant or a query parameter, both
of which must have a non-negative value. Otherwise, ZetaSQL
provides an error.

Examples of window frame clauses:

```
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
```

+ Includes the entire partition.
+ Example use: Compute a grand total over the partition.

```
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

+ Includes all the rows in the partition before or including the current row.
+ Example use: Compute a cumulative sum.

```
ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
```

+ Includes all rows between two before and two after the current row.
+ Example use: Compute a moving average.

If `window_frame_spec` uses the `BETWEEN` clause:

+ `window_frame_boundary_start` must specify a boundary that begins no later
than that of the `window_frame_boundary_end`. This has the following
consequences:
    1.  If `window_frame_boundary_start` contains `CURRENT ROW`,
       `window_frame_boundary_end` cannot contain `PRECEDING`.
    2.  If `window_frame_boundary_start` contains `FOLLOWING`,
       `window_frame_boundary_end` cannot contain `CURRENT ROW` or `PRECEDING`.
+ `window_frame_boundary_start` has no default value.

Otherwise, the specified window_frame_spec boundary represents the start of the window frame and the end of the window frame boundary defaults to 'CURRENT ROW'. Thus,

```
ROWS 10 PRECEDING
```

is equivalent to

```
ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
```

###### ROWS

`ROWS`-based window frames compute the `window frame` based on physical offsets
from the current row. For example, the window frame below defines a window frame
of size five (at most) around the current row.

```
ROWS BETWEEN 2 PRECEDING and 2 FOLLOWING
```

The `numeric_expression` in `window_frame_clause` is interpreted as a number of
rows from the current row, and must be a constant, non-negative integer. It may
also be a query parameter.

If the `window frame` for a given row extends beyond the beginning or end of the
partition, then the `window frame` will only include rows from within that
partition.

Example: Consider the following table with columns `z`, `x`, and `y`.

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>5</td>
<td>AA</td>
</tr>
<tr>
<td>2</td>
<td>2</td>
<td>AA</td>
</tr>
<tr>
<td>3</td>
<td>11</td>
<td>AB</td>
</tr>
<tr>
<td>4</td>
<td>2</td>
<td>AA</td>
</tr>
<tr>
<td>5</td>
<td>8</td>
<td>AC</td>
</tr>
<tr>
<td>6</td>
<td>10</td>
<td>AB</td>
</tr>
<tr>
<td>7</td>
<td>1</td>
<td>AB</td>
</tr>
</tbody>
</table>

Consider the following analytic function:

```
SUM(x) OVER (PARTITION BY y ORDER BY z ROWS BETWEEN 1 PRECEDING AND 1
FOLLOWING)
```

The `PARTITION BY` clause splits the table into 3 partitions based on their `y`
value, and the `ORDER BY` orders the rows within each partition by their `z`
value.

Partition 1 of 3:

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>5</td>
<td>AA</td>
</tr>
<tr>
<td>2</td>
<td>2</td>
<td>AA</td>
</tr>
<tr>
<td>4</td>
<td>2</td>
<td>AA</td>
</tr>
</tbody>
</table>

Partition 2 of 3:

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td>3</td>
<td>11</td>
<td>AB</td>
</tr>
<tr>
<td>6</td>
<td>10</td>
<td>AB</td>
</tr>
<tr>
<td>7</td>
<td>1</td>
<td>AB</td>
</tr>
</tbody>
</table>

Partition 3 of 3:

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>8</td>
<td>AC</td>
</tr>
</tbody>
</table>

In the tables below, **bold** indicates the row currently being evaluated, and
<font color="#D8CEF6">colored</font> cells indicates all the rows in the `window
frame` for that row.

+ For the first row in the y = AA partition, the `window frame` includes only 2
rows since there is no preceding row, even though the `window_frame_spec`
indicates a window size of 3. The result of the analytic function is 7 for the
first row.

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td bgcolor="#D8CEF6"><b>1</b></td>
<td bgcolor="#D8CEF6"><b>5</b></td>
<td bgcolor="#D8CEF6"><b>AA</b></td>
</tr>
<tr>
<td bgcolor="#D8CEF6">2</td>
<td bgcolor="#D8CEF6">2</td>
<td bgcolor="#D8CEF6">AA</td>
</tr>
<tr>
<td>4</td>
<td>2</td>
<td>AA</td>
</tr>
</tbody>
</table>

+ For the second row in the partition, the `window frame` includes all 3 rows.
The result of the analytic function is 9 for the second row.

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td bgcolor="#D8CEF6">1</td>
<td bgcolor="#D8CEF6">5</td>
<td bgcolor="#D8CEF6">AA</td>
</tr>
<tr>
<td bgcolor="#D8CEF6"><b>2</b></td>
<td bgcolor="#D8CEF6"><b>2</b></td>
<td bgcolor="#D8CEF6"><b>AA</b></td>
</tr>
<tr>
<td bgcolor="#D8CEF6">4</td>
<td bgcolor="#D8CEF6">2</td>
<td bgcolor="#D8CEF6">AA</td>
</tr>
</tbody>
</table>

+ For the last row in the partition, the `window frame` includes only 2 rows
since there is no following row.  The result of the analytic function is 4 for
the third row.

<table>
<thead>
<tr>
<th>z</th>
<th>x</th>
<th>y</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>5</td>
<td>AA</td>
</tr>
<tr>
<td bgcolor="#D8CEF6">2</td>
<td bgcolor="#D8CEF6">2</td>
<td bgcolor="#D8CEF6">AA</td>
</tr>
<tr>
<td bgcolor="#D8CEF6"><b>4</b></td>
<td bgcolor="#D8CEF6"><b>2</b></td>
<td bgcolor="#D8CEF6"><b>AA</b></td>
</tr>
</tbody>
</table>

###### RANGE

`RANGE`-based window frames compute the `window frame` based on a logical range
of rows around the current row based on the current row's `ORDER BY` key value.
The provided range value is added or subtracted to the current row's key value
to define a starting or ending range boundary for the `window frame`.

The `ORDER BY` clause must be specified unless the window is:

```
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
```

`numeric_expression` in the `window_frame_clause` is interpreted as an offset
from the current row's value of the `ORDER BY` key. `numeric_expression` must
have numeric type. DATE and TIMESTAMP are not currently supported. In addition,
the `numeric_expression` must be a constant, non-negative integer or a
parameter.

In a `RANGE`-based window frame, there can be at most one `expression` in the
`ORDER BY` clause, and `expression` must have a numeric type.

Example of a `RANGE`-based window frame where there is a single partition:

```
SELECT x, COUNT(*) OVER ( ORDER BY x
  RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING ) AS count_x
FROM T;
```

In the tables below, **bold** indicates the row currently being evaluated, and
<font color="#D8CEF6">colored</font> cells indicates all the rows in the
`window frame` for that row.

+ For row 1, `x` = 5 and therefore `COUNT(*)` will only include rows where
  3 &lt;=`x` &lt;= 7

<table>
<thead>
<tr>
<th>x</th>
<th>count_x</th>
</tr>
</thead>
<tbody>
<tr>
<td bgcolor="#D8CEF6"><b>5</b></td>
<td bgcolor="#D8CEF6"><b>1</b></td>
</tr>
<tr>
<td>2</td>
<td></td>
</tr>
<tr>
<td>11</td>
<td></td>
</tr>
<tr>
<td>2</td>
<td></td>
</tr>
<tr>
<td>8</td>
<td></td>
</tr>
<tr>
<td>10</td>
<td></td>
</tr>
<tr>
<td>1</td>
<td></td>
</tr>
</tbody>
</table>

+ For row 2, `x` = 2 and therefore `COUNT(*)` will only include rows where
  0 &lt;= `x` &lt;= 4

<table>
<thead>
<tr>
<th>x</th>
<th>count_x</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>1</td>
</tr>
<tr>
<td bgcolor="#D8CEF6"><b>2</b></td>
<td bgcolor="#D8CEF6"><b>3</b></td>
</tr>
<tr>
<td>11</td>
<td></td>
</tr>
<tr>
<td bgcolor="#D8CEF6">2</td>
<td></td>
</tr>
<tr>
<td>8</td>
<td></td>
</tr>
<tr>
<td>10</td>
<td></td>
</tr>
<tr>
<td bgcolor="#D8CEF6">1</td>
<td></td>
</tr>
</tbody>
</table>

+ For row 3, `x` = 11 and therefore `COUNT(*)` will only include rows where
  9 &lt;= `x` &lt;= 13

<table>
<thead>
<tr>
<th>x</th>
<th>count_x</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>1</td>
</tr>
<tr>
<td>2</td>
<td>3</td>
</tr>
<tr>
<td bgcolor="#D8CEF6"><b>11</b></td>
<td bgcolor="#D8CEF6"><b>2</b></td>
</tr>
<tr>
<td>2</td>
<td></td>
</tr>
<tr>
<td>8</td>
<td></td>
</tr>
<tr>
<td bgcolor="#D8CEF6">10</td>
<td></td>
</tr>
<tr>
<td>1</td>
<td></td>
</tr>
</tbody>
</table>

### WINDOW Clause

Syntax:

```
WINDOW window_definition [, ...]
window_definition: window_name AS ( window_specification )
```

A `WINDOW` clause defines a list of named windows whose `window_name` can be
referenced in analytic functions in the `SELECT` list. This is useful when you
want to use the same `window_frame_clause` for multiple analytic functions.

The `WINDOW` clause can appear only at the end of a `SELECT` clause, as shown
in [Query Syntax][analytic-functions-link-to-sql-syntax].

#### Named Windows

Once you define a `WINDOW` clause, you can use the named windows in analytic
functions, but only in the `SELECT` list; you cannot use named windows in the
`ORDER BY` clause. Named windows can appear either by themselves or embedded
within an `OVER` clause. Named windows can refer to `SELECT` list aliases.

Examples:

```
SELECT SUM(x) OVER window_name FROM ...
```

```
SELECT SUM(x) OVER (
  window_name
  PARTITION BY...
  ORDER BY...
  window_frame_clause)
FROM ...
```

When embedded within an `OVER` clause, the `window_specification` associated
with `window_name` must be compatible with the `PARTITION BY`, `ORDER BY`, and
`window_frame_clause` that are in the same `OVER` clause.

 The following rules apply to named windows:

+ You can refer to named windows only in the `SELECT` list; you cannot refer to
them in an `ORDER BY` clause, an outer query, or any subquery.
+ A window W1 (named or unnamed) may reference a named window NW2, with the
following rules:
    1. If W1 is a named window, then the referenced named window NW2 must precede
       W1 in the same `WINDOW` clause.
    2. W1 cannot contain a `PARTITION BY` clause
    3. It cannot be true that both W1 and NW2 contain an `ORDER BY` clause
    4. NW2 cannot contain a `window_frame_clause`.
+ If a (named or unnamed) window W1 references a named window NW2, then the
resulting window specification is defined using:
    1. The `PARTITION BY` from NW2, if there is one.
    2.  The `ORDER BY` from W1 or NW2, if either is specified; it is not possible
    for them to both have an `ORDER BY` clause.
    3. The `window_frame_clause` from W1, if there is one.

### Hints

ZetaSQL supports
[hints][analytic-functions-link-to-hints] on both the `PARTITION BY` clause (analogously to `GROUP BY`) and the
`ORDER BY` clause (analogously to the normal `ORDER BY` clause) in an `OVER`
clause.

### Navigation Functions

This topic explains how analytic navigation functions work. For a description of
the analytic navigation functions ZetaSQL supports, see the
[function reference for navigation functions][navigation-functions-reference].

Navigation functions generally compute some `value_expression` over a different
row in the window frame from the current row. The `OVER` clause syntax varies
across navigation functions.

`OVER` clause requirements:

+   `PARTITION BY`: Optional.
+   `ORDER BY`:
    1.  Disallowed for `PERCENTILE_CONT` and `PERCENTILE_DISC`.
    1.   Required for `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD`
    and `LAG`.
+   `window_frame_clause`:
    1.  Disallowed for  `PERCENTILE_CONT`, `PERCENTILE_DISC`, `LEAD` and `LAG`.
    1.  Optional for `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`.

For all navigation functions, the result data type is the same type as
`value_expression`.

### Numbering Functions

This topic explains how analytic numbering functions work. For an explanation of
the analytic numbering functions ZetaSQL supports, see the
[numbering functions reference][numbering-functions-reference].

Numbering functions assign integer values to each row based on their position
within the specified window.

Example of `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`:

```
SELECT x,
  RANK() OVER (ORDER BY x ASC) AS rank,
  DENSE_RANK() OVER (ORDER BY x ASC) AS dense_rank,
  ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS row_num
FROM ...
```

<table>
<thead>
<tr>
<th>x</th>
<th>rank</th>
<th>dense_rank</th>
<th>row_num</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>1</td>
<td>1</td>
<td>1</td>
</tr>
<tr>
<td>2</td>
<td>2</td>
<td>2</td>
<td>2</td>
</tr>
<tr>
<td>2</td>
<td>2</td>
<td>2</td>
<td>3</td>
</tr>
<tr>
<td>5</td>
<td>4</td>
<td>3</td>
<td>4</td>
</tr>
<tr>
<td>8</td>
<td>5</td>
<td>4</td>
<td>5</td>
</tr>
<tr>
<td>10</td>
<td>6</td>
<td>5</td>
<td>6</td>
</tr>
<tr>
<td>10</td>
<td>6</td>
<td>5</td>
<td>7</td>
</tr>
</tbody>
</table>

* `RANK(): `For x=5, `rank` returns 4, since `RANK()` increments by the number
of peers in the previous window ordering group.
* `DENSE_RANK()`: For x=5, `dense_rank` returns 3, since `DENSE_RANK()` always
increments by 1, never skipping a value.
* `ROW_NUMBER(): `For x=5, `row_num` returns 4.

### Aggregate Analytic Functions

ZetaSQL supports certain
[aggregate functions][aggregate-analytic-functions-reference]
as analytic functions.

With these functions, the `OVER` clause is simply appended to the aggregate
function call; the function call syntax remains otherwise unchanged. Like their
aggregate function counterparts, these analytic functions perform aggregations,
but specifically over the relevant window frame for each row. The result data
types of these analytic functions are the same as their aggregate function
counterparts.

For a description of the aggregate analytic functions that ZetaSQL
supports, see the [function reference for aggregate analytic functions][aggregate-analytic-functions-reference].

[navigation-functions-reference]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#navigation-functions
[numbering-functions-reference]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#numbering-functions
[aggregate-analytic-functions-reference]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#aggregate-analytic-functions

[analytic-functions-link-to-aggregate-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

[analytic-functions-link-to-numbering-functions]: #numbering-functions
[analytic-functions-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md
[analytic-functions-link-to-order-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#order_by_clause
[analytic-functions-link-to-sql-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#sql-syntax
[analytic-functions-link-to-hints]: https://github.com/google/zetasql/blob/master/docs/lexical.md#hints
[analytic-functions-link-to-coercion]: #coercion

<!-- END CONTENT -->

