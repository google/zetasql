

# Array functions

### ARRAY

```sql
ARRAY(subquery)
```

**Description**

The `ARRAY` function returns an `ARRAY` with one element for each row in a
[subquery][subqueries].

If `subquery` produces a
[SQL table][datamodel-sql-tables],
the table must have exactly one column. Each element in the output `ARRAY` is
the value of the single column of a row in the table.

If `subquery` produces a
[value table][datamodel-value-tables],
then each element in the output `ARRAY` is the entire corresponding row of the
value table.

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
  [`ARRAY`][array-data-type].
+ If the subquery returns zero rows, the `ARRAY` function returns an empty
`ARRAY`. It never returns a `NULL` `ARRAY`.

**Return type**

ARRAY

**Examples**

```sql
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

```sql
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

```sql
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

```sql
ARRAY_CONCAT(array_expression[, ...])
```

**Description**

Concatenates one or more arrays with the same element type into a single array.

The function returns `NULL` if any input argument is `NULL`.

Note: You can also use the [|| concatenation operator][array-link-to-operators]
to concatenate arrays.

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

### ARRAY_FILTER

```sql
ARRAY_FILTER(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias->boolean_expression
    | (element_alias, index_alias)->boolean_expression
  }
```

**Description**

Takes an array, filters out unwanted elements, and returns the results in a new
array.

+   `array_expression`: The array to filter.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition]. If the expression evaluates to
    `FALSE` or `NULL`, the element is removed from the resulting array.
+   `element_alias`: An alias that represents an array element.
+   `index_alias`: An alias that represents the zero-based offset of the array
    element.
+   `boolean_expression`: The predicate used to filter the array elements.

Returns `NULL` if the `array_expression` is `NULL`.

**Return type**

ARRAY

**Example**

```sql
SELECT
  ARRAY_FILTER([1,2,3], e->e>1) AS a1,
  ARRAY_FILTER([0,2,3], (e, i)->e>i) AS a2;

+-------+-------+
| a1    | a2    |
+-------+-------+
| [2,3] | [2,3] |
+-------+-------+
```

### ARRAY_INCLUDES

+   [Signature 1](#array_includes_signature1): `ARRAY_INCLUDES(array_expression,
    target_element)`
+   [Signature 2](#array_includes_signature2): `ARRAY_INCLUDES(array_expression,
    lambda_expression)`

#### Signature 1 
<a id="array_includes_signature1"></a>

```sql
ARRAY_INCLUDES(array_expression, target_element)
```

**Description**

Takes an array and returns `TRUE` if there is an element in the array that is
equal to the target element.

+   `array_expression`: The array to search.
+   `target_element`: The target element to search for in the array.

Returns `NULL` if `array_expression` or `target_element` is `NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if `0` exists in an
array. Then the query checks to see if `1` exists in an array.

```sql
SELECT
  ARRAY_INCLUDES([1,2,3], 0) AS a1,
  ARRAY_INCLUDES([1,2,3], 1) AS a2;

+-------+------+
| a1    | a2   |
+-------+------+
| false | true |
+-------+------+
```

#### Signature 2 
<a id="array_includes_signature2"></a>

```sql
ARRAY_INCLUDES(array_expression, lambda_expression)

lambda_expression: element_alias->boolean_expression
```

**Description**

Takes an array and returns `TRUE` if the lambda expression evaluates to `TRUE`
for any element in the array.

+   `array_expression`: The array to search.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition].
+   `element_alias`: An alias that represents an array element.
+   `boolean_expression`: The predicate used to evaluate the array elements.

Returns `NULL` if `array_expression` is `NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if any elements that are
greater than 3 exist in an array (`e>3`). Then the query checks to see if any
any elements that are greater than 0 exist in an array (`e>0`).

```sql
SELECT
  ARRAY_INCLUDES([1,2,3], e->e>3) AS a1,
  ARRAY_INCLUDES([1,2,3], e->e>0) AS a2;

+-------+------+
| a1    | a2   |
+-------+------+
| false | true |
+-------+------+
```

### ARRAY_INCLUDES_ANY

```sql
ARRAY_INCLUDES_ANY(source_array_expression, target_array_expression)
```

**Description**

Takes a source and target array. Returns `TRUE` if any elements in the target
array are in the source array, otherwise returns `FALSE`.

+   `source_array_expression`: The array to search.
+   `target_array_expression`: The target array that contains the elements to
    search for in the source array.

Returns `NULL` if `source_array_expression` or `target_array_expression` is
`NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if `3`, `4`, or `5`
exists in an array. Then the query checks to see if `4`, `5`, or `6` exists in
an array.

```sql
SELECT
  ARRAY_INCLUDES_ANY([1,2,3], [3,4,5]) AS a1,
  ARRAY_INCLUDES_ANY([1,2,3], [4,5,6]) AS a2;

+------+-------+
| a1   | a2    |
+------+-------+
| true | false |
+------+-------+
```

### ARRAY_LENGTH

```sql
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
  (SELECT ["coffee", NULL, "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)
SELECT ARRAY_TO_STRING(list, ', ', 'NULL'), ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;

+---------------------------------+------+
| list                            | size |
+---------------------------------+------+
| [coffee, NULL, milk]            | 3    |
| [cake, pie]                     | 2    |
+---------------------------------+------+
```

### ARRAY_TO_STRING

```sql
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
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie                      |
+--------------------------------+
```

```sql
WITH items AS
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie--MISSING             |
+--------------------------------+
```

### ARRAY_TRANSFORM

```sql
ARRAY_TRANSFORM(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias->transform_expression
    | (element_alias, index_alias)->transform_expression
  }
```

**Description**

Takes an array, transforms the elements, and returns the results in a new array.

+   `array_expression`: The array to transform.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition]. The evaluation results are
    returned in a new array.
+   `element_alias`: An alias that represents an array element.
+   `index_alias`: An alias that represents the zero-based offset of the array
    element.
+   `transform_expression`: The expression used to transform the array elements.

Returns `NULL` if the `array_expression` is `NULL`.

**Return type**

ARRAY

**Example**

```sql
SELECT
  ARRAY_TRANSFORM([1,2,3], e->e+1) AS a1,
  ARRAY_TRANSFORM([1,2,3], (e, i)->e+i) AS a2;

+---------+---------+
| a1      | a2      |
+---------+---------+
| [2,3,4] | [1,3,5] |
+---------+---------+
```

### FLATTEN

```sql
FLATTEN(array_elements_field_access_expression)
```

**Description**

Takes a nested array and flattens a specific part of it into a single, flat
array with the
[array elements field access operator][array-el-field-operator].
Returns `NULL` if the input value is `NULL`.
If `NULL` array elements are
encountered, they are added to the resulting array.

There are several ways to flatten nested data into arrays. To learn more, see
[Flattening nested data into an array][flatten-tree-to-array].

**Return type**

`ARRAY`

**Examples**

In the following example, all of the arrays for `v.sales.quantity` are
concatenated in a flattened array.

```sql
WITH t AS (
  SELECT
  [
    STRUCT([STRUCT([1,2,3] AS quantity), STRUCT([4,5,6] AS quantity)] AS sales),
    STRUCT([STRUCT([7,8] AS quantity), STRUCT([] AS quantity)] AS sales)
  ] AS v
)
SELECT FLATTEN(v.sales.quantity) AS all_values
FROM t;

+--------------------------+
| all_values               |
+--------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8] |
+--------------------------+
```

In the following example, `OFFSET` gets the second value in each array and
concatenates them.

```sql
WITH t AS (
  SELECT
  [
    STRUCT([STRUCT([1,2,3] AS quantity), STRUCT([4,5,6] AS quantity)] AS sales),
    STRUCT([STRUCT([7,8,9] AS quantity), STRUCT([10,11,12] AS quantity)] AS sales)
  ] AS v
)
SELECT FLATTEN(v.sales.quantity[OFFSET(1)]) AS second_values
FROM t;

+---------------+
| second_values |
+---------------+
| [2, 5, 8, 11] |
+---------------+
```

In the following example, all values for `v.price` are returned in a
flattened array.

```sql
WITH t AS (
  SELECT
  [
    STRUCT(1 AS price, 2 AS quantity),
    STRUCT(10 AS price, 20 AS quantity)
  ] AS v
)
SELECT FLATTEN(v.price) AS all_prices
FROM t;

+------------+
| all_prices |
+------------+
| [1, 10]    |
+------------+
```

For more examples, including how to use protocol buffers with `FLATTEN`, see the
[array elements field access operator][array-el-field-operator].

### GENERATE_ARRAY

```sql
GENERATE_ARRAY(start_expression, end_expression[, step_expression])
```

**Description**

Returns an array of values. The `start_expression` and `end_expression`
parameters determine the inclusive start and end of the array.

The `GENERATE_ARRAY` function accepts the following data types as inputs:

<ul>
<li>INT64</li><li>UINT64</li><li>NUMERIC</li><li>BIGNUMERIC</li><li>DOUBLE</li>
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

```sql
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
SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM (
  SELECT DATE '2016-01-01' AS date_start, DATE '2016-01-31' AS date_end
  UNION ALL SELECT DATE "2016-04-01", DATE "2016-04-30"
  UNION ALL SELECT DATE "2016-07-01", DATE "2016-07-31"
  UNION ALL SELECT DATE "2016-10-01", DATE "2016-10-31"
) AS items;

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

```sql
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
+ Allowed `date_part` values are:
  `NANOSECOND`
  (if the SQL engine supports it),
  `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, or `DAY`.

The `step_expression` parameter determines the increment used to generate
timestamps.

**Return Data Type**

An `ARRAY` containing 0 or more
`TIMESTAMP` values.

**Examples**

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1 day.

```sql
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

```sql
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

```sql
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

```sql
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

```sql
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

```sql
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

```sql
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

### ARRAY_REVERSE

```sql
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

### ARRAY_IS_DISTINCT

```sql
ARRAY_IS_DISTINCT(value)
```

**Description**

Returns true if the array contains no repeated elements, using the same equality
comparison logic as `SELECT DISTINCT`.

**Return type**

BOOL

**Examples**

```sql
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [1, 1, 1] AS arr UNION ALL
  SELECT [1, 2, NULL] AS arr UNION ALL
  SELECT [1, 1, NULL] AS arr UNION ALL
  SELECT [1, NULL, NULL] AS arr UNION ALL
  SELECT [] AS arr UNION ALL
  SELECT CAST(NULL AS ARRAY<INT64>) AS arr
)
SELECT
  arr,
  ARRAY_IS_DISTINCT(arr) as is_distinct
FROM example;

+-----------------+-------------+
| arr             | is_distinct |
+-----------------+-------------+
| [1, 2, 3]       | true        |
| [1, 1, 1]       | false       |
| [1, 2, NULL]    | true        |
| [1, 1, NULL]    | false       |
| [1, NULL, NULL] | false       |
| []              | true        |
| NULL            | NULL        |
+-----------------+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[array-subscript-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#array_subscript_operator

[subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#subqueries

[datamodel-sql-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#standard_sql_tables

[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#array_type

[flatten-tree-to-array]: https://github.com/google/zetasql/blob/master/docs/arrays.md#flattening_nested_data_into_arrays

[array-el-field-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#array_el_field_operator

[array-link-to-operators]: https://github.com/google/zetasql/blob/master/docs/operators.md

[lambda-definition]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#lambdas

<!-- mdlint on -->

