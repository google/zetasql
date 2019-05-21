

# Array functions

### ARRAY

```
ARRAY(subquery)
```

**Description**

The `ARRAY` function returns an `ARRAY` with one element for each row in a
[subquery](https://github.com/google/zetasql/blob/master/docs/query-syntax.md#subqueries).

If `subquery` produces a
[SQL table](https://github.com/google/zetasql/blob/master/docs/data-model.md#standard-sql-tables),
the table must have exactly one column. Each element in the output `ARRAY` is
the value of the single column of a row in the table.

If `subquery` produces a
[value table](https://github.com/google/zetasql/blob/master/docs/data-model.md#value-tables), then
each element in the output `ARRAY` is the entire corresponding row of the value
table.

**Constraints**

+ Subqueries are unordered, so the elements of the output `ARRAY` are not
guaranteed to preserve any order in the source table for the subquery. However,
if the subquery includes an `ORDER BY` clause, the `ARRAY` function will return
an `ARRAY` that honors that clause.
+ If the subquery returns more than one column, the `ARRAY` function returns an
error.
+ If the subquery returns an `ARRAY` typed column or `ARRAY` typed rows, the
  `ARRAY` function returns an error: ZetaSQL does not support
  `ARRAY`s with elements of type
  [`ARRAY`](https://github.com/google/zetasql/blob/master/docs/data-types.md#array-type).
+ If the subquery returns zero rows, the `ARRAY` function returns an empty
`ARRAY`. It never returns a `NULL` `ARRAY`.

**Return type**

ARRAY

**Examples**

```
SELECT ARRAY
  (SELECT 1 UNION ALL
   SELECT 2 UNION ALL
   SELECT 3) AS new_array;

+-----------+
| new_array |
+-----------+
| [1, 2, 3] |
+-----------+
```

To construct an `ARRAY` from a subquery that contains multiple
columns, change the subquery to use `SELECT AS STRUCT`. Now
the `ARRAY` function will return an `ARRAY` of `STRUCT`s. The `ARRAY` will
contain one `STRUCT` for each row in the subquery, and each of these `STRUCT`s
will contain a field for each column in that row.

```
SELECT
  ARRAY
    (SELECT AS STRUCT 1, 2, 3
     UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array;

+------------------------+
| new_array              |
+------------------------+
| [{1, 2, 3}, {4, 5, 6}] |
+------------------------+
```

Similarly, to construct an `ARRAY` from a subquery that contains
one or more `ARRAY`s, change the subquery to use `SELECT AS STRUCT`.

```
SELECT ARRAY
  (SELECT AS STRUCT [1, 2, 3] UNION ALL
   SELECT AS STRUCT [4, 5, 6]) AS new_array;

+----------------------------+
| new_array                  |
+----------------------------+
| [{[1, 2, 3]}, {[4, 5, 6]}] |
+----------------------------+
```

### ARRAY_CONCAT

```
ARRAY_CONCAT(array_expression_1 [, array_expression_n])
```

**Description**

Concatenates one or more arrays with the same element type into a single array.

**Return type**

ARRAY

**Examples**

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

+--------------------------------------------------+
| count_to_six                                     |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

### ARRAY_LENGTH

```
ARRAY_LENGTH(array_expression)
```

**Description**

Returns the size of the array. Returns 0 for an empty array. Returns `NULL` if
the `array_expression` is `NULL`.

**Return type**

INT64

**Examples**

```sql

WITH items AS
  (SELECT ["apples", "bananas", NULL, "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list, ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;

+---------------------------------+------+
| list                            | size |
+---------------------------------+------+
| [apples, bananas, NULL, grapes] | 4    |
| [coffee, tea, milk]             | 3    |
| [cake, pie]                     | 2    |
+---------------------------------+------+
```

### ARRAY_TO_STRING

```
ARRAY_TO_STRING(array_expression, delimiter[, null_text])
```

**Description**

Returns a concatenation of the elements in `array_expression`
as a STRING. The value for `array_expression`
can either be an array of STRING or
BYTES data types.

If the `null_text` parameter is used, the function replaces any `NULL` values in
the array with the value of `null_text`.

If the `null_text` parameter is not used, the function omits the `NULL` value
and its preceding delimiter.

**Examples**

```sql

WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| apples--bananas--pears--grapes |
| coffee--tea--milk              |
| cake--pie                      |
+--------------------------------+

WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| apples--bananas--pears--grapes |
| coffee--tea--milk              |
| cake--pie--MISSING             |
+--------------------------------+
```

### GENERATE_ARRAY
```
GENERATE_ARRAY(start_expression, end_expression[, step_expression])
```

**Description**

Returns an array of values. The `start_expression` and `end_expression`
parameters determine the inclusive start and end of the array.

The `GENERATE_ARRAY` function accepts the following data types as inputs:

<ul>
<li>INT64</li><li>UINT64</li><li>NUMERIC</li><li>DOUBLE</li>
</ul>

The `step_expression` parameter determines the increment used to
generate array values. The default value for this parameter is `1`.

This function returns an error if `step_expression` is set to 0, or if any
input is `NaN`.

If any argument is `NULL`, the function will return a `NULL` array.

**Return Data Type**

ARRAY

**Examples**

The following returns an array of integers, with a default step of 1.

```sql
SELECT GENERATE_ARRAY(1, 5) AS example_array;

+-----------------+
| example_array   |
+-----------------+
| [1, 2, 3, 4, 5] |
+-----------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_ARRAY(0, 10, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| [0, 3, 6, 9]  |
+---------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_ARRAY(10, 0, -3) AS example_array;

+---------------+
| example_array |
+---------------+
| [10, 7, 4, 1] |
+---------------+
```

The following returns an array using the same value for the `start_expression`
and `end_expression`.

```sql
SELECT GENERATE_ARRAY(4, 4, 10) AS example_array;

+---------------+
| example_array |
+---------------+
| [4]           |
+---------------+
```

The following returns an empty array, because the `start_expression` is greater
than the `end_expression`, and the `step_expression` value is positive.

```sql
SELECT GENERATE_ARRAY(10, 0, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| []            |
+---------------+
```

The following returns a `NULL` array because `end_expression` is `NULL`.

```sql
SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array;

+---------------+
| example_array |
+---------------+
| NULL          |
+---------------+
```

The following returns multiple arrays.

```sql
SELECT GENERATE_ARRAY(start, 5) AS example_array
FROM UNNEST([3, 4, 5]) AS start;

+---------------+
| example_array |
+---------------+
| [3, 4, 5]     |
| [4, 5]        |
| [5]           |
+---------------+
```

### GENERATE_DATE_ARRAY
```
GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])
```

**Description**

Returns an array of dates. The `start_date` and `end_date`
parameters determine the inclusive start and end of the array.

The `GENERATE_DATE_ARRAY` function accepts the following data types as inputs:

+ `start_date` must be a DATE
+ `end_date` must be a DATE
+ `INT64_expr` must be an INT64
+ `date_part` must be either DAY, WEEK, MONTH, QUARTER, or YEAR.

The `INT64_expr` parameter determines the increment used to generate dates. The
default value for this parameter is 1 day.

This function returns an error if `INT64_expr` is set to 0.

**Return Data Type**

An ARRAY containing 0 or more DATE values.

**Examples**

The following returns an array of dates, with a default step of 1.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;

+--------------------------------------------------+
| example                                          |
+--------------------------------------------------+
| [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
+--------------------------------------------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_DATE_ARRAY(
 '2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example;

+--------------------------------------+
| example                              |
+--------------------------------------+
| [2016-10-05, 2016-10-07, 2016-10-09] |
+--------------------------------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL -3 DAY) AS example;

+--------------------------+
| example                  |
+--------------------------+
| [2016-10-05, 2016-10-02] |
+--------------------------+
```

The following returns an array using the same value for the `start_date`and
`end_date`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-05', INTERVAL 8 DAY) AS example;

+--------------+
| example      |
+--------------+
| [2016-10-05] |
+--------------+

```

The following returns an empty array, because the `start_date` is greater
than the `end_date`, and the `step` value is positive.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL 1 DAY) AS example;

+---------+
| example |
+---------+
| []      |
+---------+
```

The following returns a `NULL` array, because one of its inputs is
`NULL`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example;

+---------+
| example |
+---------+
| NULL    |
+---------+
```

The following returns an array of dates, using MONTH as the `date_part`
interval:

```sql
SELECT GENERATE_DATE_ARRAY('2016-01-01',
  '2016-12-31', INTERVAL 2 MONTH) AS example;

+--------------------------------------------------------------------------+
| example                                                                  |
+--------------------------------------------------------------------------+
| [2016-01-01, 2016-03-01, 2016-05-01, 2016-07-01, 2016-09-01, 2016-11-01] |
+--------------------------------------------------------------------------+
```

The following uses non-constant dates to generate an array.

```sql
WITH StartsAndEnds AS (
  SELECT DATE '2016-01-01' AS date_start, DATE '2016-01-31' AS date_end
  UNION ALL SELECT DATE "2016-04-01", DATE "2016-04-30"
  UNION ALL SELECT DATE "2016-07-01", DATE "2016-07-31"
  UNION ALL SELECT DATE "2016-10-01", DATE "2016-10-31"
)
SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM StartsAndEnds;

+--------------------------------------------------------------+
| date_range                                                   |
+--------------------------------------------------------------+
| [2016-01-01, 2016-01-08, 2016-01-15, 2016-01-22, 2016-01-29] |
| [2016-04-01, 2016-04-08, 2016-04-15, 2016-04-22, 2016-04-29] |
| [2016-07-01, 2016-07-08, 2016-07-15, 2016-07-22, 2016-07-29] |
| [2016-10-01, 2016-10-08, 2016-10-15, 2016-10-22, 2016-10-29] |
+--------------------------------------------------------------+
```

### GENERATE_TIMESTAMP_ARRAY

```
GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp,
                         INTERVAL step_expression date_part)
```

**Description**

Returns an `ARRAY` of `TIMESTAMPS` separated by a given interval. The
`start_timestamp` and `end_timestamp` parameters determine the inclusive
lower and upper bounds of the `ARRAY`.

The `GENERATE_TIMESTAMP_ARRAY` function accepts the following data types as
inputs:

+ `start_timestamp`: `TIMESTAMP`
+ `end_timestamp`: `TIMESTAMP`
+ `step_expression`: `INT64`
+ Allowed `date_part` values are
   `MICROSECOND`,
   `MILLISECOND`,
   `SECOND`, `MINUTE`, `HOUR`, or `DAY`.

The `step_expression` parameter determines the increment used to generate
timestamps.

**Return Data Type**

An `ARRAY` containing 0 or more
`TIMESTAMP` values.

**Examples**

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1 day.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00',
                                INTERVAL 1 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-06 00:00:00+00, 2016-10-07 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1
second.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:02',
                                INTERVAL 1 SECOND) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 00:00:01+00, 2016-10-05 00:00:02+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMPS` with a negative
interval.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-01 00:00:00',
                                INTERVAL -2 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-06 00:00:00+00, 2016-10-04 00:00:00+00, 2016-10-02 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` with a single element, because
`start_timestamp` and `end_timestamp` have the same value.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+--------------------------+
| timestamp_array          |
+--------------------------+
| [2016-10-05 00:00:00+00] |
+--------------------------+
```

The following example returns an empty `ARRAY`, because `start_timestamp` is
later than `end_timestamp`.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| []              |
+-----------------+
```

The following example returns a null `ARRAY`, because one of the inputs is
`NULL`.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', NULL, INTERVAL 1 HOUR)
  AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| NULL            |
+-----------------+
```

The following example generates `ARRAY`s of `TIMESTAMP`s from columns containing
values for `start_timestamp` and `end_timestamp`.

```
SELECT GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp, INTERVAL 1 HOUR)
  AS timestamp_array
FROM
  (SELECT
    TIMESTAMP '2016-10-05 00:00:00' AS start_timestamp,
    TIMESTAMP '2016-10-05 02:00:00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 12:00:00' AS start_timestamp,
    TIMESTAMP '2016-10-05 14:00:00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 23:59:00' AS start_timestamp,
    TIMESTAMP '2016-10-06 01:59:00' AS end_timestamp);

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 01:00:00+00, 2016-10-05 02:00:00+00] |
| [2016-10-05 12:00:00+00, 2016-10-05 13:00:00+00, 2016-10-05 14:00:00+00] |
| [2016-10-05 23:59:00+00, 2016-10-06 00:59:00+00, 2016-10-06 01:59:00+00] |
+--------------------------------------------------------------------------+
```

### OFFSET and ORDINAL

```
array_expression[OFFSET(zero_based_offset)]
array_expression[ORDINAL(one_based_offset)]
```

**Description**

Accesses an ARRAY element by position and returns the
element. `OFFSET` means that the numbering starts at zero, `ORDINAL` means that
the numbering starts at one.

A given array can be interpreted as either 0-based or 1-based. When accessing an
array element, you must preface the array position with `OFFSET` or `ORDINAL`,
respectively; there is no default behavior.

Both `OFFSET` and `ORDINAL` generate an error if the index is out of range.

**Return type**

Varies depending on the elements in the ARRAY.

**Examples**

```sql

WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list, list[OFFSET(1)] as offset_1, list[ORDINAL(1)] as ordinal_1
FROM items;

+----------------------------------+-----------+-----------+
| list                             | offset_1  | ordinal_1 |
+----------------------------------+-----------+-----------+
| [apples, bananas, pears, grapes] | bananas   | apples    |
| [coffee, tea, milk]              | tea       | coffee    |
| [cake, pie]                      | pie       | cake      |
+----------------------------------+-----------+-----------+
```

### ARRAY_REVERSE
```
ARRAY_REVERSE(value)
```

**Description**

Returns the input ARRAY with elements in reverse order.

**Return type**

ARRAY

**Examples**

```sql
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [4, 5] AS arr UNION ALL
  SELECT [] AS arr
)
SELECT
  arr,
  ARRAY_REVERSE(arr) AS reverse_arr
FROM example;

+-----------+-------------+
| arr       | reverse_arr |
+-----------+-------------+
| [1, 2, 3] | [3, 2, 1]   |
| [4, 5]    | [5, 4]      |
| []        | []          |
+-----------+-------------+
```

### SAFE_OFFSET and SAFE_ORDINAL

```
array_expression[SAFE_OFFSET(zero_based_offset)]
array_expression[SAFE_ORDINAL(one_based_offset)]
```

**Description**

Identical to `OFFSET` and `ORDINAL`, except returns `NULL` if the index is out
of range.

**Return type**

Varies depending on the elements in the ARRAY.

**Example**

```sql

WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list,
  list[SAFE_OFFSET(3)] as safe_offset_3,
  list[SAFE_ORDINAL(3)] as safe_ordinal_3
FROM items;

+----------------------------------+---------------+----------------+
| list                             | safe_offset_3 | safe_ordinal_3 |
+----------------------------------+---------------+----------------+
| [apples, bananas, pears, grapes] | grapes        | pears          |
| [coffee, tea, milk]              | NULL          | milk           |
| [cake, pie]                      | NULL          | NULL           |
+----------------------------------+---------------+----------------+
```

