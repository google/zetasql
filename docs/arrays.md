
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Working with Arrays

<!-- BEGIN CONTENT -->

In ZetaSQL, an array is an ordered list consisting of zero or more
values of the same data type. You can construct arrays of simple data types,
such as `INT64`, and complex data types, such as `STRUCT`s. The current
exception to this is the
[`ARRAY`][array-data-type] data
type: arrays of arrays are not supported.

With ZetaSQL, you can construct array literals,
 build arrays from subqueries using the
[`ARRAY`][array-function] function,
 and aggregate values into an array using the
[`ARRAY_AGG`][array-agg-function]
function.

You can combine arrays using functions like
`ARRAY_CONCAT()`, and convert arrays to strings using `ARRAY_TO_STRING()`.

## Constructing arrays

### Using array literals

You can build an array literal in ZetaSQL using brackets (`[` and
`]`). Each element in an array is separated by a comma.

```sql
SELECT [1, 2, 3] as numbers;

SELECT ["apple", "pear", "orange"] as fruit;

SELECT [true, false, true] as booleans;
```

You can also create arrays from any expressions that have compatible types. For
example:

```sql
SELECT [a, b, c]
FROM
  (SELECT 5 AS a,
          37 AS b,
          406 AS c);

SELECT [a, b, c]
FROM
  (SELECT CAST(5 AS INT64) AS a,
          CAST(37 AS DOUBLE) AS b,
          406 AS c);
```

Notice that the second example contains three expressions: one that returns an
`INT64`, one that returns a `DOUBLE`, and one that
declares a literal. This expression works because all three expressions share
`DOUBLE` as a supertype.

To declare a specific data type for an array, use angle
brackets (`<` and `>`). For example:

```sql
SELECT ARRAY<DOUBLE>[1, 2, 3] as floats;
```

Arrays of most data types, such as `INT64` or `STRING`, don't require
that you declare them first.

```sql
SELECT [1, 2, 3] as numbers;
```

You can write an empty array of a specific type using `ARRAY<type>[]`. You can
also write an untyped empty array using `[]`, in which case ZetaSQL
attempts to infer the array type from the surrounding context. If
ZetaSQL cannot infer a type, the default type `ARRAY<INT64>` is used.

### Using generated values

You can also construct an `ARRAY` with generated values.

#### Generating arrays of integers

[`GENERATE_ARRAY`][generate-array-function]
generates an array of values from a starting and ending value and a step value.
For example, the following query generates an array that contains all of the odd
integers from 11 to 33, inclusive:

```sql
SELECT GENERATE_ARRAY(11, 33, 2) AS odds;

+--------------------------------------------------+
| odds                                             |
+--------------------------------------------------+
| [11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33] |
+--------------------------------------------------+
```

You can also generate an array of values in descending order by giving a
negative step value:

```sql
SELECT GENERATE_ARRAY(21, 14, -1) AS countdown;

+----------------------------------+
| countdown                        |
+----------------------------------+
| [21, 20, 19, 18, 17, 16, 15, 14] |
+----------------------------------+
```

#### Generating arrays of dates

[`GENERATE_DATE_ARRAY`][generate-date-array]
generates an array of `DATE`s from a starting and ending `DATE` and a step
`INTERVAL`.

You can generate a set of `DATE` values using `GENERATE_DATE_ARRAY`. For
example, this query returns the current `DATE` and the following
`DATE`s at 1 `WEEK` intervals up to and including a later `DATE`:

```sql
SELECT
  GENERATE_DATE_ARRAY('2017-11-21', '2017-12-31', INTERVAL 1 WEEK)
    AS date_array;

+--------------------------------------------------------------------------+
| date_array                                                               |
+--------------------------------------------------------------------------+
| [2017-11-21, 2017-11-28, 2017-12-05, 2017-12-12, 2017-12-19, 2017-12-26] |
+--------------------------------------------------------------------------+
```

## Casting Arrays

You can use [`CAST`][casting]
to cast arrays from one element type to another. The element types of the input
`ARRAY` must be castable to the element types of the target `ARRAY`. For
example, casting from type `ARRAY<INT32>` to `ARRAY<INT64>` or `ARRAY<STRING>`
is valid; casting from type `ARRAY<INT32>` to `ARRAY<BYTES>` is not valid.

**Example**

```sql
SELECT CAST(int_array AS ARRAY<DOUBLE>) AS double_array
FROM (SELECT ARRAY<INT32>[1, 2, 3] AS int_array);

+--------------+
| double_array |
+--------------+
| [1, 2, 3]    |
+--------------+
```

## Accessing Array Elements

Consider the following table, `sequences`:

```sql
+---------------------+
| some_numbers        |
+---------------------+
| [0, 1, 1, 2, 3, 5]  |
| [2, 4, 8, 16, 32]   |
| [5, 10]             |
+---------------------+
```

This table contains the column `some_numbers` of the `ARRAY` data type.
To access elements from the arrays in this column, you must specify which type
of indexing you want to use: either
[`OFFSET`][offset-and-ordinal],
for zero-based indexes, or
[`ORDINAL`][offset-and-ordinal],
for one-based indexes.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
       some_numbers[OFFSET(1)] AS offset_1,
       some_numbers[ORDINAL(1)] AS ordinal_1
FROM sequences;

+--------------------+----------+-----------+
| some_numbers       | offset_1 | ordinal_1 |
+--------------------+----------+-----------+
| [0, 1, 1, 2, 3, 5] | 1        | 0         |
| [2, 4, 8, 16, 32]  | 4        | 2         |
| [5, 10]            | 10       | 5         |
+--------------------+----------+-----------+
```

You can use this DML statement to insert the example data:

```sql
INSERT sequences
  (some_numbers, id)
VALUES
  ([0, 1, 1, 2, 3, 5], 1),
  ([2, 4, 8, 16, 32], 2),
  ([5, 10], 3);
```

This query shows how to use `OFFSET()` and `ORDINAL()`:

```sql
SELECT some_numbers,
       some_numbers[OFFSET(1)] AS offset_1,
       some_numbers[ORDINAL(1)] AS ordinal_1
FROM sequences;

+---------------+----------+-----------+
| some_numbers  | offset_1 | ordinal_1 |
+---------------+----------+-----------+
| [0,1,1,2,3,5] |        1 |         0 |
+---------------+----------+-----------+
| [2,4,8,16,32] |        4 |         2 |
+---------------+----------+-----------+
| [5,10]        |       10 |         5 |
+---------------+----------+-----------+
```

## Finding Lengths

The `ARRAY_LENGTH()` function returns the length of an array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
       ARRAY_LENGTH(some_numbers) AS len
FROM sequences;

+--------------------+--------+
| some_numbers       | len    |
+--------------------+--------+
| [0, 1, 1, 2, 3, 5] | 6      |
| [2, 4, 8, 16, 32]  | 5      |
| [5, 10]            | 2      |
+--------------------+--------+
```

Here's an example query, assuming the same definition of the `sequences` table
as above, with the same sample rows:

```sql
SELECT some_numbers,
       ARRAY_LENGTH(some_numbers) AS len
FROM sequences;

+---------------+------+
| some_numbers  | len  |
+---------------+------+
| [0,1,1,2,3,5] |    6 |
+---------------+------+
| [2,4,8,16,32] |    5 |
+---------------+------+
| [5,10]        |    2 |
+---------------+------+
```

## Flattening arrays

To convert an `ARRAY` into a set of rows, also known as "flattening," use the
[`UNNEST`][unnest-query]
operator. `UNNEST` takes an `ARRAY` and returns a table with a single row for
each element in the `ARRAY`.

Because `UNNEST` destroys the order of the `ARRAY` elements, you may
wish to restore order to the table. To do so, use the optional `WITH OFFSET`
clause to return an additional column with the offset for each array element,
then use the `ORDER BY` clause to order the rows by their offset.

**Example**

```sql
SELECT *
FROM UNNEST(['foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'])
  AS element
WITH OFFSET AS offset
ORDER BY offset;

+----------+--------+
| element  | offset |
+----------+--------+
| foo      | 0      |
| bar      | 1      |
| baz      | 2      |
| qux      | 3      |
| corge    | 4      |
| garply   | 5      |
| waldo    | 6      |
| fred     | 7      |
+----------+--------+
```

To flatten an entire column of `ARRAY`s while preserving the values
of the other columns in each row, use a
[`CROSS JOIN`][cross-join-query]
to join the table containing the `ARRAY` column to the `UNNEST` output of that
`ARRAY` column.

This is a correlated cross join: the `UNNEST` operator references the column of
`ARRAY`s from each row in the source table, which appears previously in the
`FROM` clause. For each row `N` in the source table, `UNNEST` flattens the
`ARRAY` from row `N` into a set of rows containing the `ARRAY` elements, and
then the `CROSS JOIN` joins this new set of rows with the single row `N` from
the source table.

**Example**

The following example uses [`UNNEST`][unnest-query]
to return a row for each element in the array column. Because of the
`CROSS JOIN`, the `id` column contains the `id` values for the row in
`sequences` that contains each number.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id, flattened_numbers
FROM sequences
CROSS JOIN UNNEST(sequences.some_numbers) AS flattened_numbers;

+------+-------------------+
| id   | flattened_numbers |
+------+-------------------+
|    1 |                 0 |
|    1 |                 1 |
|    1 |                 1 |
|    1 |                 2 |
|    1 |                 3 |
|    1 |                 5 |
|    2 |                 2 |
|    2 |                 4 |
|    2 |                 8 |
|    2 |                16 |
|    2 |                32 |
|    3 |                 5 |
|    3 |                10 |
+------+-------------------+
```

Note that for correlated cross joins the `UNNEST` operator is optional and the
`CROSS JOIN` can be expressed as a comma-join. Using this shorthand notation,
the above example becomes:

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id, flattened_numbers
FROM sequences, sequences.some_numbers AS flattened_numbers;

+------+-------------------+
| id   | flattened_numbers |
+------+-------------------+
|    1 |                 0 |
|    1 |                 1 |
|    1 |                 1 |
|    1 |                 2 |
|    1 |                 3 |
|    1 |                 5 |
|    2 |                 2 |
|    2 |                 4 |
|    2 |                 8 |
|    2 |                16 |
|    2 |                32 |
|    3 |                 5 |
|    3 |                10 |
+------+-------------------+
```

## Querying Nested and Repeated Fields

If a table contains an `ARRAY` of `STRUCT`s or `PROTO`s, you can
[flatten the `ARRAY`][flattening-arrays] to query the fields of the `STRUCT` or
`PROTO`.
You can also flatten `ARRAY` type fields of `STRUCT` values and repeated fields
of `PROTO` values. ZetaSQL treats repeated `PROTO` fields as
`ARRAY`s.

### Querying STRUCT elements in an ARRAY

The following example uses `UNNEST` with `CROSS JOIN` to flatten an `ARRAY` of
`STRUCT`s.

```sql
WITH races AS (
  SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
     STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
     STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
     STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
     STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
     STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
     STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
     STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
       AS participants)
SELECT
  race,
  participant
FROM races r
CROSS JOIN UNNEST(r.participants) as participant;

+------+---------------------------------------+
| race | participant                           |
+------+---------------------------------------+
| 800M | {Rudisha, [23.4, 26.3, 26.4, 26.1]}   |
| 800M | {Makhloufi, [24.5, 25.4, 26.6, 26.1]} |
| 800M | {Murphy, [23.9, 26, 27, 26]}          |
| 800M | {Bosse, [23.6, 26.2, 26.5, 27.1]}     |
| 800M | {Rotich, [24.7, 25.6, 26.9, 26.4]}    |
| 800M | {Lewandowski, [25, 25.7, 26.3, 27.2]} |
| 800M | {Kipketer, [23.2, 26.1, 27.3, 29.4]}  |
| 800M | {Berian, [23.7, 26.1, 27, 29.3]}      |
+------+---------------------------------------+
```

```sql
SELECT race,
       participant.name,
       participant.splits
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r
CROSS JOIN UNNEST(r.participants) AS participant;

+------+-------------+-----------------------+
| race | name        | splits                |
+------+-------------+-----------------------+
| 800M | Rudisha     | [23.4,26.3,26.4,26.1] |
+------+-------------+-----------------------+
| 800M | Makhloufi   | [24.5,25.4,26.6,26.1] |
+------+-------------+-----------------------+
| 800M | Murphy      | [23.9,26,27,26]       |
+------+-------------+-----------------------+
| 800M | Bosse       | [23.6,26.2,26.5,27.1] |
+------+-------------+-----------------------+
| 800M | Rotich      | [24.7,25.6,26.9,26.4] |
+------+-------------+-----------------------+
| 800M | Lewandowski | [25,25.7,26.3,27.2]   |
+------+-------------+-----------------------+
| 800M | Kipketer    | [23.2,26.1,27.3,29.4] |
+------+-------------+-----------------------+
| 800M | Berian      | [23.7,26.1,27,29.3]   |
+------+-------------+-----------------------+
```

You can find specific information from repeated fields. For example, the
following query returns the fastest racer in an 800M race.

<p class="note">This example does not involve flattening an array, but does
represent a common way to get information from a repeated field.</p>

**Example**

```sql
WITH races AS (
  SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
     STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
     STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
     STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
     STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
     STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
     STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
     STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
       AS participants)
SELECT
  race,
  (SELECT name
   FROM UNNEST(participants)
   ORDER BY (
     SELECT SUM(duration)
     FROM UNNEST(splits) AS duration) ASC
   LIMIT 1) AS fastest_racer
FROM races;

+------+---------------+
| race | fastest_racer |
+------+---------------+
| 800M | Rudisha       |
+------+---------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants)
        ORDER BY (
          SELECT SUM(duration)
          FROM UNNEST(splits) AS duration) ASC
          LIMIT 1) AS fastest_racer
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+---------------+
| race | fastest_racer |
+------+---------------+
| 800M | Rudisha       |
+------+---------------+
```

### Querying PROTO elements in an ARRAY

To query the fields of `PROTO` elements in an `ARRAY`, use `UNNEST` and
`CROSS JOIN`.

**Example**

The following query shows the contents of a table where one row contains an
`ARRAY` of `PROTO`s. All of the `PROTO` field values in the `ARRAY` appear in a
single row.

```sql
WITH table AS (
  SELECT
    'Let It Be' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(2 AS rank, 'Oricon' AS chart_name)]
     AS charts
  UNION ALL
  SELECT
    'Rubber Soul' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(24 AS rank, 'Oricon' AS chart_name)]
     AS charts
)
SELECT *
FROM table;

+-------------+---------------------------------+
| album_name  | charts                          |
+-------------+---------------------------------+
| Let It Be   | [chart_name: "US 100", rank: 1, |
|             | chart_name: "UK 40", rank: 1,   |
|             | chart_name: "Oricon" rank: 2]   |
+-------------+---------------------------------+
| Rubber Soul | [chart_name: "US 100", rank: 1, |
|             | chart_name: "UK 40", rank: 1,   |
|             | chart_name: "Oricon" rank: 24]  |
+-------------+---------------------------------+
```

To return the value of the individual fields of the `PROTO`s inside an `ARRAY`,
use `UNNEST` to flatten the `ARRAY`, then use a `CROSS JOIN` to apply the
`UNNEST` operator to each row of the `ARRAY` column. The `CROSS JOIN` also
joins the duplicated values of other columns to the result of `UNNEST`, so you
can query these columns together with the fields of the `PROTO`s in the `ARRAY`.

**Example**

The following example uses `UNNEST` to flatten the `ARRAY` `charts`. The `CROSS
JOIN` applies the `UNNEST` operator to every row in the `charts` column and
joins the duplicated value of `table.album_name` to the `chart` table. This
allows the query to include the `table.album_name` column in the `SELECT` list
together with the `PROTO` fields `chart.chart_name` and `chart.rank`.

```sql
WITH table AS (
  SELECT
    'Let It Be' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(2 AS rank, 'Oricon' AS chart_name)]
     AS charts
  UNION ALL
  SELECT
    'Rubber Soul' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(24 AS rank, 'Oricon' AS chart_name)]
     AS charts
)
SELECT table.album_name, chart.chart_name, chart.rank
FROM table
CROSS JOIN UNNEST(charts) AS chart;

+-------------+------------+------+
| album_name  | chart_name | rank |
+-------------+------------+------+
| Let It Be   | US 100     |    1 |
| Let It Be   | UK 40      |    1 |
| Let It Be   | Oricon     |    2 |
| Rubber Soul | US 100     |    1 |
| Rubber Soul | UK 40      |    1 |
| Rubber Soul | Oricon     |   24 |
+-------------+------------+------+
```

### Querying ARRAY-type fields in a STRUCT

You can also get information from nested repeated fields. For example, the
following statement returns the runner who had the fastest lap in an 800M race.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
    AS participants)
SELECT
race,
(SELECT name
 FROM UNNEST(participants),
   UNNEST(splits) AS duration
 ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM races;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants),
          UNNEST(splits) AS duration
        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

Notice that the preceding query uses the comma operator (`,`) to perform an
implicit `CROSS JOIN`. It is equivalent to the following example, which uses
an explicit `CROSS JOIN`.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
    AS participants)
SELECT
race,
(SELECT name
 FROM UNNEST(participants)
 CROSS JOIN UNNEST(splits) AS duration
 ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM races;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants)
        CROSS JOIN UNNEST(splits) AS duration
        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

Note that flattening arrays with a `CROSS JOIN` excludes rows that have empty
or NULL arrays. If you want to include these rows, use a `LEFT JOIN`.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits),
    STRUCT("Nathan" as name, ARRAY<DOUBLE>[] as splits),
    STRUCT("David" as name, NULL as splits)]
    AS participants)
SELECT
  name, sum(duration) AS finish_time
FROM races, races.participants LEFT JOIN participants.splits duration
GROUP BY name;

+-------------+--------------------+
| name        | finish_time        |
+-------------+--------------------+
| Murphy      | 102.9              |
| Rudisha     | 102.19999999999999 |
| David       | NULL               |
| Rotich      | 103.6              |
| Makhloufi   | 102.6              |
| Berian      | 106.1              |
| Bosse       | 103.4              |
| Kipketer    | 106                |
| Nathan      | NULL               |
| Lewandowski | 104.2              |
+-------------+--------------------+
```

```sql
SELECT
  name, sum(duration) as duration
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" AS name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Nathan" as name, ARRAY<DOUBLE>[] as splits),
     STRUCT("David" as name, NULL as splits)]
     AS participants) AS races,
  races.participants LEFT JOIN participants.splits duration
GROUP BY name;

+-------------+--------------------+
| name        | duration           |
+-------------+--------------------+
| Murphy      | 102.9              |
| Rudisha     | 102.19999999999999 |
| David       | NULL               |
| Rotich      | 103.6              |
| Makhloufi   | 102.6              |
| Bosse       | 103.4              |
| Kipketer    | 106                |
| Nathan      | NULL               |
| Lewandowski | 104.2              |
+-------------+--------------------+
```

### Querying repeated fields

ZetaSQL represents repeated fields of `PROTO`s as `ARRAY`s. You
can query these `ARRAY`s using `UNNEST` and `CROSS JOIN`.

The following example queries a table containing a column of `PROTO`s with the
alias `album` and the repeated field `song`. All values of `song` for each
`album` appear on the same row.

**Example**

```sql
WITH table AS (
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Let It Be' AS album_name,
      ['Across the Universe', 'Get Back', 'Dig It'] AS song
    ) AS album
    UNION ALL
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Rubber Soul' AS album_name,
      ['Drive My Car', 'The Word', 'Michelle'] AS song
    ) AS album
)
SELECT *
FROM table;

+-------------+------------------+---------------------+
| band_name   | album.album_name | album.song          |
+-------------+------------------+---------------------+
| The Beatles | Let It Be        | Across the Universe |
|             |                  | Get Back            |
|             |                  | Dig It              |
| The Beatles | Rubber Soul      | Drive My Car        |
|             |                  | The Word            |
|             |                  | Michelle            |
+-------------+------------------+---------------------+
```

To query the individual values of a repeated field, reference the field name
using dot notation to return an `ARRAY`, and
[flatten the `ARRAY` using `UNNEST`][flattening-arrays]. Use `CROSS JOIN` to
apply the `UNNEST` operator to each row and join the flattened `ARRAY`
to the duplicated value of any non-repeated fields or columns.

**Example**

The following example queries the table from the previous example and returns
the values of the repeated field as an `ARRAY`. The `UNNEST` operator flattens
the `ARRAY` that represents the repeated field `song`. `CROSS JOIN` applies
the `UNNEST` operator to each row and joins the output of `UNNEST` to the
duplicated value of the column `band_name` and the non-repeated field
`album_name`.

```sql
WITH table AS (
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Let It Be' AS album_name,
      ['Across the Universe', 'Get Back', 'Dig It'] AS song
    ) AS album
    UNION ALL
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Rubber Soul' AS album_name,
      ['Drive My Car', 'The Word', 'Michelle'] AS song
    ) AS album
)
SELECT band_name, album.album_name, song_name
FROM table
CROSS JOIN UNNEST(album.song) AS song_name;

+-------------+-------------+---------------------+
| band_name   | album_name  | song_name           |
+-------------+-------------+---------------------+
| The Beatles | Let It Be   | Across the Universe |
| The Beatles | Let It Be   | Get Back            |
| The Beatles | Let It Be   | Dig It              |
| The Beatles | Rubber Soul | Drive My Car        |
| The Beatles | Rubber Soul | The Word            |
| The Beatles | Rubber Soul | Michelle            |
+-------------+-------------+---------------------+

```

## Creating Arrays From Subqueries

A common task when working with arrays is turning a subquery result into an
array. In ZetaSQL, you can accomplish this using the
[`ARRAY()`][array-function] function.

For example, consider the following operation on the `sequences` table:

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
  UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
  UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x) AS doubled
FROM sequences;

+--------------------+---------------------+
| some_numbers       | doubled             |
+--------------------+---------------------+
| [0, 1, 1, 2, 3, 5] | [0, 2, 2, 4, 6, 10] |
| [2, 4, 8, 16, 32]  | [4, 8, 16, 32, 64]  |
| [5, 10]            | [10, 20]            |
+--------------------+---------------------+
```

```sql
SELECT some_numbers,
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x) AS doubled
FROM sequences;

+---------------+----------------+
| some_numbers  | doubled        |
+---------------+----------------+
| [0,1,1,2,3,5] | [0,2,2,4,6,10] |
+---------------+----------------+
| [2,4,8,16,32] | [4,8,16,32,64] |
+---------------+----------------+
| [5,10]        | [10,20]        |
+---------------+----------------+
```

This example starts with a table named sequences. This table contains a column,
`some_numbers`, of type `ARRAY<INT64>`.

The query itself contains a subquery. This subquery selects each row in the
`some_numbers` column and uses
[`UNNEST`][unnest-query] to return the
array as a set of rows. Next, it multiplies each value by two, and then
recombines the rows back into an array using the `ARRAY()` operator.

## Filtering Arrays
The following example uses a `WHERE` clause in the `ARRAY()` operator's subquery
to filter the returned rows.

<p class='note'><b>Note:</b> In the following examples, the resulting rows are
not ordered.</p>

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x
        WHERE x < 5) AS doubled_less_than_five
FROM sequences;

+------------------------+
| doubled_less_than_five |
+------------------------+
| [0, 2, 2, 4, 6]        |
| [4, 8]                 |
| []                     |
+------------------------+
```

```sql
SELECT
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x
        WHERE x < 5) AS doubled_less_than_five
FROM sequences;

+------------------------+
| doubled_less_than_five |
+------------------------+
| [0,2,2,4,6]            |
+------------------------+
| [4,8]                  |
+------------------------+
| []                     |
+------------------------+
```

Notice that the third row contains an empty array, because the elements in the
corresponding original row (`[5, 10]`) did not meet the filter requirement of
`x < 5`.

You can also filter arrays by using `SELECT DISTINCT` to return only
unique elements within an array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers)
SELECT ARRAY(SELECT DISTINCT x
             FROM UNNEST(some_numbers) AS x) AS unique_numbers
FROM sequences;

+-----------------+
| unique_numbers  |
+-----------------+
| [0, 1, 2, 3, 5] |
+-----------------+
```

```sql
SELECT ARRAY(SELECT DISTINCT x
             FROM UNNEST(some_numbers) AS x) AS unique_numbers
FROM sequences
WHERE id = 1;

+----------------+
| unique_numbers |
+----------------+
| [0,1,2,3,5]    |
+----------------+
```

You can also filter rows of arrays by using the
[`IN`][in-operators] keyword. This
keyword filters rows containing arrays by determining if a specific
value matches an element in the array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT
   ARRAY(SELECT x
         FROM UNNEST(some_numbers) AS x
         WHERE 2 IN UNNEST(some_numbers)) AS contains_two
FROM sequences;

+--------------------+
| contains_two       |
+--------------------+
| [0, 1, 1, 2, 3, 5] |
| [2, 4, 8, 16, 32]  |
| []                 |
+--------------------+
```

```sql
SELECT
   ARRAY(SELECT x
         FROM UNNEST(some_numbers) AS x
         WHERE 2 IN UNNEST(some_numbers)) AS contains_two
FROM sequences;

+---------------+
| contains_two  |
+---------------+
| [0,1,1,2,3,5] |
+---------------+
| [2,4,8,16,32] |
+---------------+
| []            |
+---------------+
```

Notice again that the third row contains an empty array, because the array in
the corresponding original row (`[5, 10]`) did not contain `2`.

## Scanning Arrays

To check if an array contains a specific value, use the [`IN`][in-operators]
operator with [`UNNEST`][unnest-query]. To
check if an array contains a value matching a condition, use the [`EXISTS`][expression-subqueries]
function with `UNNEST`.

### Scanning for specific values

To scan an array for a specific value, use the `IN` operator with `UNNEST`.

**Example**

The following example returns `true` if the array contains the number 2.

```sql
SELECT 2 IN UNNEST([0, 1, 1, 2, 3, 5]) AS contains_value;

+----------------+
| contains_value |
+----------------+
| true           |
+----------------+
```

To return the rows of a table where the array column contains a specific value,
filter the results of `IN UNNEST` using the `WHERE` clause.

**Example**

The following example returns the `id` value for the rows where the array
column contains the value 2.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id AS matching_rows
FROM sequences
WHERE 2 IN UNNEST(sequences.some_numbers)
ORDER BY matching_rows;

+---------------+
| matching_rows |
+---------------+
| 1             |
| 2             |
+---------------+
```

### Scanning for values that satisfy a condition

To scan an array for values that match a condition, use `UNNEST` to return a
table of the elements in the array, use `WHERE` to filter the resulting table in
a subquery, and use `EXISTS` to check if the filtered table contains any rows.

**Example**

The following example returns the `id` value for the rows where the array
column contains values greater than 5.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id AS matching_rows FROM sequences
WHERE EXISTS (SELECT *
              FROM UNNEST(some_numbers) AS x
              WHERE x > 5);

+---------------+
| matching_rows |
+---------------+
| 2             |
| 3             |
+---------------+
```

#### Scanning for STRUCT field values that satisfy a condition

To search an array of `STRUCT`s for a field whose value matches a condition, use
`UNNEST` to return a table with a column for each `STRUCT` field, then filter
non-matching rows from the table using `WHERE EXISTS`.

**Example**

The following example returns the rows where the array column contains a
`STRUCT` whose field `b` has a value greater than 3.

```sql
WITH sequences AS
  (SELECT 1 AS id, [STRUCT(0 AS a, 1 AS b)] AS some_numbers
   UNION ALL SELECT 2 AS id, [STRUCT(2 AS a, 4 AS b)] AS some_numbers
   UNION ALL SELECT 3 AS id, [STRUCT(5 AS a, 3 AS b), STRUCT (7 AS a, 4 AS b)]
     AS some_numbers)
SELECT id AS matching_rows
FROM sequences
WHERE EXISTS (SELECT 1
              FROM UNNEST(some_numbers)
              WHERE b > 3);

+---------------+
| matching_rows |
+---------------+
| 2             |
| 3             |
+---------------+
```

## Arrays and Aggregation

With ZetaSQL, you can aggregate values into an array using
`ARRAY_AGG()`.

```sql
WITH fruits AS
  (SELECT "apple" AS fruit
   UNION ALL SELECT "pear" AS fruit
   UNION ALL SELECT "banana" AS fruit)
SELECT ARRAY_AGG(fruit) AS fruit_basket
FROM fruits;

+-----------------------+
| fruit_basket          |
+-----------------------+
| [apple, pear, banana] |
+-----------------------+
```

Consider the following table, `fruits`:

```sql
CREATE TABLE fruits (
  fruit STRING(MAX),
  id INT64 NOT NULL
) PRIMARY KEY(id);

```
Assume the table is populated with the following data:

```sql
+----+--------------+
| id | fruit        |
+----+--------------+
| 1  | "apple"      |
| 2  | "pear"       |
| 3  | "banana"     |
+----+--------------+
```

You can use this DML statement to insert the example data:

```sql
INSERT fruits
  (fruit, id)
VALUES
  ("apple", 1),
  ("pear", 2),
  ("banana", 3);
```

This query shows how to use `ARRAY_AGG()`:

```sql
SELECT ARRAY_AGG(fruit) AS fruit_basket
FROM fruits;

+---------------------+
| fruit_basket        |
+---------------------+
| [apple,pear,banana] |
+---------------------+
```

The array returned by `ARRAY_AGG()` is in an arbitrary order, since the order in
which the function concatenates values is not guaranteed. To order the array
elements, use `ORDER BY`. For example:

```sql
WITH fruits AS
  (SELECT "apple" AS fruit
   UNION ALL SELECT "pear" AS fruit
   UNION ALL SELECT "banana" AS fruit)
SELECT ARRAY_AGG(fruit ORDER BY fruit) AS fruit_basket
FROM fruits;

+-----------------------+
| fruit_basket          |
+-----------------------+
| [apple, banana, pear] |
+-----------------------+
```

You can also apply aggregate functions such as `SUM()` to the elements in an
array. For example, the following query returns the sum of array elements for
each row of the `sequences` table.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
  (SELECT SUM(x)
   FROM UNNEST(s.some_numbers) x) AS sums
FROM sequences s;

+--------------------+------+
| some_numbers       | sums |
+--------------------+------+
| [0, 1, 1, 2, 3, 5] | 12   |
| [2, 4, 8, 16, 32]  | 62   |
| [5, 10]            | 15   |
+--------------------+------+
```

```sql
SELECT some_numbers,
  (SELECT SUM(x)
   FROM UNNEST(s.some_numbers) x) AS sums
FROM sequences s;

+---------------+------+
| some_numbers  | sums |
+---------------+------+
| [0,1,1,2,3,5] |   12 |
+---------------+------+
| [2,4,8,16,32] |   62 |
+---------------+------+
| [5,10]        |   15 |
+---------------+------+
```

ZetaSQL also supports an aggregate function, `ARRAY_CONCAT_AGG()`,
which concatenates the elements of an array column across rows.

```sql
WITH aggregate_example AS
  (SELECT [1,2] AS numbers
   UNION ALL SELECT [3,4] AS numbers
   UNION ALL SELECT [5, 6] AS numbers)
SELECT ARRAY_CONCAT_AGG(numbers) AS count_to_six_agg
FROM aggregate_example;

+--------------------------------------------------+
| count_to_six_agg                                 |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

**Note:** The array returned by `ARRAY_CONCAT_AGG()` is
non-deterministic, since the order in which the function concatenates values is
not guaranteed.

## Converting Arrays to Strings

The `ARRAY_TO_STRING()` function allows you to convert an `ARRAY<STRING>` to a
single `STRING` value or an `ARRAY<BYTES>` to a single `BYTES` value where the
resulting value is the ordered concatenation of the array elements.

The second argument is the separator that the function will insert between
inputs to produce the output; this second argument must be of the same
type as the elements of the first argument.

Example:

```sql
WITH greetings AS
  (SELECT ["Hello", "World"] AS greeting)
SELECT ARRAY_TO_STRING(greeting, " ") AS greetings
FROM greetings;

+-------------+
| greetings   |
+-------------+
| Hello World |
+-------------+
```

The optional third argument takes the place of `NULL` values in the input
array.

+ If you omit this argument, then the function ignores `NULL` array
elements.

+ If you provide an empty string, the function inserts a
separator for `NULL` array elements.

Example:

```sql
SELECT
  ARRAY_TO_STRING(arr, ".", "N") AS non_empty_string,
  ARRAY_TO_STRING(arr, ".", "") AS empty_string,
  ARRAY_TO_STRING(arr, ".") AS omitted
FROM (SELECT ["a", NULL, "b", NULL, "c", NULL] AS arr);

+------------------+--------------+---------+
| non_empty_string | empty_string | omitted |
+------------------+--------------+---------+
| a.N.b.N.c.N      | a..b..c.     | a.b.c   |
+------------------+--------------+---------+
```

## Combining Arrays

In some cases, you might want to combine multiple arrays into a single array.
You can accomplish this using the `ARRAY_CONCAT()` function.

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

+--------------------------------------------------+
| count_to_six                                     |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

## Zipping Arrays

Given two arrays of equal size, you can merge them into a single array
consisting of pairs of elements from input arrays, taken from their
corresponding positions. This operation is sometimes called
[zipping][convolution].

You can zip arrays with `UNNEST` and `WITH OFFSET`. In this example, each
value pair is stored as a `STRUCT` in an array.

```sql
WITH combinations AS (
  SELECT
    ['a', 'b'] AS letters,
    [1, 2, 3] AS numbers
)
SELECT ARRAY_AGG(
  STRUCT(letter, numbers[OFFSET(letters_offset)] AS number)
) AS pairs
FROM combinations, UNNEST(letters) AS letter WITH OFFSET AS letters_offset;

+------------------------------+
| pairs                        |
+------------------------------+
| [{ letter: "a", number: 1 }, |
|  { letter: "b", number: 2 }] |
+------------------------------+
```

You can use input arrays of different lengths as long as the first array
is equal to or less than the length of the second array. The zipped array
will be the length of the shortest input array.

## Building Arrays of Arrays

ZetaSQL does not support building
[arrays of arrays][array-data-type]
directly. Instead, you must create an array of structs, with each struct
containing a field of type `ARRAY`. To illustrate this, consider the following
`points` table:

```sql
+----------+
| point    |
+----------+
| [1, 5]   |
| [2, 8]   |
| [3, 7]   |
| [4, 1]   |
| [5, 7]   |
+----------+
```

Now, let's say you wanted to create an array consisting of each `point` in the
`points` table. To accomplish this, wrap the array returned from each row in a
`STRUCT`, as shown below.

```sql
WITH points AS
  (SELECT [1, 5] as point
   UNION ALL SELECT [2, 8] as point
   UNION ALL SELECT [3, 7] as point
   UNION ALL SELECT [4, 1] as point
   UNION ALL SELECT [5, 7] as point)
SELECT ARRAY(
  SELECT STRUCT(point)
  FROM points)
  AS coordinates;

+-------------------+
| coordinates       |
+-------------------+
| [{point: [1,5]},  |
|  {point: [2,8]},  |
|  {point: [5,7]},  |
|  {point: [3,7]},  |
|  {point: [4,1]}]  |
+--------------------+
```

You can use this DML statement to insert the example data:

```sql
INSERT points
  (point, id)
VALUES
  ([1, 5], 1),
  ([2, 8], 2),
  ([3, 7], 3),
  ([4, 1], 4),
  ([5, 7], 5);
```

```sql
SELECT ARRAY(
  SELECT STRUCT(point)
  FROM points)
  AS coordinates;

+--------------+
| coordinates  |
+--------------+
| point: [1,5] |
| point: [2,8] |
| point: [3,7] |
| point: [4,1] |
| point: [5,7] |
+--------------+
```

[flattening-arrays]: #flattening_arrays
[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types#array_type
[unnest-query]: https://github.com/google/zetasql/blob/master/docs/query-syntax#unnest
[cross-join-query]: https://github.com/google/zetasql/blob/master/docs/query-syntax#cross_join
[convolution]: https://en.wikipedia.org/wiki/Convolution_(computer_science)

[in-operators]: https://github.com/google/zetasql/blob/master/docs/operators#in_operators
[expression-subqueries]: https://github.com/google/zetasql/blob/master/docs/expression_subqueries
[casting]: https://github.com/google/zetasql/blob/master/docs/conversion_rules#casting
[array-function]: https://github.com/google/zetasql/blob/master/docs/array_functions
[array-agg-function]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions#array_agg
[generate-array-function]: https://github.com/google/zetasql/blob/master/docs/array_functions#generate_array
[generate-date-array]: https://github.com/google/zetasql/blob/master/docs/array_functions#generate_date_array
[offset-and-ordinal]: https://github.com/google/zetasql/blob/master/docs/array_functions#offset_and_ordinal

<!-- END CONTENT -->

