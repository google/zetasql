

# Operators

Operators are represented by special characters or keywords; they do not use
function call syntax. An operator manipulates any number of data inputs, also
called operands, and returns a result.

Common conventions:

+  Unless otherwise specified, all operators return `NULL` when one of the
   operands is `NULL`.
+  All operators will throw an error if the computation result overflows.
+  For all floating point operations, `+/-inf` and `NaN` may only be returned
   if one of the operands is `+/-inf` or `NaN`. In other cases, an error is
   returned.

### Operator precedence

The following table lists all ZetaSQL operators from highest to
lowest precedence, i.e. the order in which they will be evaluated within a
statement.

<table>
  <thead>
    <tr>
      <th>Order of Precedence</th>
      <th>Operator</th>
      <th>Input Data Types</th>
      <th>Name</th>
      <th>Operator Arity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>Field access operator</td>
      <td><span> JSON</span><br><span> PROTO</span><br><span> STRUCT</span><br></td>
      <td>Field access operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array elements field access operator</td>
      <td>ARRAY</td>
      <td>Field access operator for elements in an array</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array subscript operator</td>
      <td>ARRAY</td>
      <td>Array position. Must be used with OFFSET or ORDINAL&mdash;see
      <a href="https://github.com/google/zetasql/blob/master/docs/array_functions.md">Array Functions</a>

.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>JSON subscript operator</td>
      <td>JSON</td>
      <td>Field name or array position in JSON.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>2</td>
      <td>+</td>
      <td>All numeric types</td>
      <td>Unary plus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>-</td>
      <td>All numeric types</td>
      <td>Unary minus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>~</td>
      <td>Integer or BYTES</td>
      <td>Bitwise not</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>3</td>
      <td>*</td>
      <td>All numeric types</td>
      <td>Multiplication</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>/</td>
      <td>All numeric types</td>
      <td>Division</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>||</td>
      <td>STRING, BYTES, or ARRAY&#60;T&#62;</td>
      <td>Concatenation operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>4</td>
      <td>+</td>
      <td>
        All numeric types, DATE with
        INT64
        , INTERVAL
      </td>
      <td>Addition</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>-</td>
      <td>
        All numeric types, DATE with
        INT64
        , INTERVAL
      </td>
      <td>Subtraction</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>5</td>
      <td>&lt;&lt;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise left-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;&gt;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise right-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>6</td>
      <td>&amp;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise and</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>7</td>
      <td>^</td>
      <td>Integer or BYTES</td>
      <td>Bitwise xor</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>8</td>
      <td>|</td>
      <td>Integer or BYTES</td>
      <td>Bitwise or</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>9 (Comparison Operators)</td>
      <td>=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>!=, &lt;&gt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Not equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] LIKE</td>
      <td>STRING and byte</td>
      <td>Value does [not] match the pattern specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] BETWEEN</td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] within the range specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] IN</td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] in the set of values specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] <code>NULL</code></td>
      <td>All</td>
      <td>Value is [not] <code>NULL</code></td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] TRUE</td>
      <td>BOOL</td>
      <td>Value is [not] TRUE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] FALSE</td>
      <td>BOOL</td>
      <td>Value is [not] FALSE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>10</td>
      <td>NOT</td>
      <td>BOOL</td>
      <td>Logical NOT</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>11</td>
      <td>AND</td>
      <td>BOOL</td>
      <td>Logical AND</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>12</td>
      <td>OR</td>
      <td>BOOL</td>
      <td>Logical OR</td>
      <td>Binary</td>
    </tr>
  </tbody>
</table>

Operators with the same precedence are left associative. This means that those
operators are grouped together starting from the left and moving right. For
example, the expression:

`x AND y AND z`

is interpreted as

`( ( x AND y ) AND z )`

The expression:

```
x * y / z
```

is interpreted as:

```
( ( x * y ) / z )
```

All comparison operators have the same priority, but comparison operators are
not associative. Therefore, parentheses are required in order to resolve
ambiguity. For example:

`(x < y) IS FALSE`

### Field access operator 
<a id="field_access_operator"></a>

```
expression.fieldname[. ...]
```

**Description**

Gets the value of a field. Alternatively known as the dot operator. Can be
used to access nested fields. For example, `expression.fieldname1.fieldname2`.

**Input types**

+ `STRUCT`
+ `PROTO`
+ `JSON`

**Return type**

+ For `STRUCT`: SQL data type of `fieldname`. If a field is not found in
  the struct, an error is thrown.
+ For `PROTO`: SQL data type of `fieldname`. If a field is not found in
  the protocol buffer, an error is thrown.
+ For `JSON`: `JSON`. If a field is not found in a JSON value, a SQL `NULL` is
  returned.

**Example**

In the following example, the expression is `t.customer` and the
field access operations are `.address` and `.country`. An operation is an
application of an operator (`.`) to specific operands (in this case,
`address` and `country`, or more specifically, `t.customer` and `address`,
for the first operation, and `t.customer.address` and `country` for the
second operation).

```sql
WITH orders AS (
  SELECT STRUCT(STRUCT('Yonge Street' AS street, 'Canada' AS country) AS address) AS customer
)
SELECT t.customer.address.country FROM orders AS t;

+---------+
| country |
+---------+
| Canada  |
+---------+
```

### Array subscript operator 
<a id="array_subscript_operator"></a>

```
array_expression[array_subscript_specifier]

array_subscript_specifier:
  position_keyword(index)

position_keyword:
  { OFFSET | SAFE_OFFSET | ORDINAL | SAFE_ORDINAL }
```

Note: The brackets (`[]`) around `array_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

Gets a value from an array at a specific location.

**Input types**

+ `array_expression`: The input array.
+ `position_keyword`: Where the index for the array should start and how
  out-of-range indexes are handled. Your choices are:
  + `OFFSET`: The index starts at zero.
    Produces an error if the index is out of range.
  + `SAFE_OFFSET`: The index starts at
    zero. Returns `NULL` if the index is out of range.
  + `ORDINAL`: The index starts at one.
    Produces an error if the index is out of range.
  + `SAFE_ORDINAL`: The index starts at
    one. Returns `NULL` if the index is out of range.
+ `index`: An integer that represents a specific position in the array.

**Return type**

`T` where `array_expression` is `ARRAY<T>`.

**Examples**

In this example, the array subscript operator is used to return values at
specific locations in `item_array`. This example also shows what happens when
you reference an index (`6`) in an array that is out of range. If the
`SAFE` prefix is included, `NULL` is returned, otherwise an error is produced.

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array,
  item_array[OFFSET(1)] AS item_offset,
  item_array[ORDINAL(1)] AS item_ordinal,
  item_array[SAFE_OFFSET(6)] AS item_safe_offset,
FROM Items

+----------------------------------+--------------+--------------+------------------+
| item_array                       | item_offset  | item_ordinal | item_safe_offset |
+----------------------------------+--------------+--------------+------------------+
| [coffee, tea, milk]              | tea          | coffee       | NULL             |
+----------------------------------+--------------+--------------+------------------+
```

In the following example, when you reference an index in an array that is out of
range and the `SAFE` prefix is not included, an error is produced.

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[OFFSET(6)] AS item_offset
FROM Items

-- Error. OFFSET(6) is out of range.
```

### JSON subscript operator

```
json_expression[array_element_id]
```

```
json_expression[field_name]
```

Note: The brackets (`[]`) around `array_element_id` and `field_name` are part
of the syntax; they do not represent an optional part.

**Description**

Gets a value of an array element or field in a JSON expression. Can be
used to access nested data.

**Input types**

+ `JSON expression`: The `JSON` expression that contains an array element or
  field to return.
+ `[array_element_id]`: An `INT64` expression that represents a zero-based index
  in the array. If a negative value is entered, or the value is greater than
  or equal to the size of the array, or the JSON expression doesn't represent
  a JSON array, a SQL `NULL` is returned.
+ `[field_name]`: A `STRING` expression that represents the name of a field in
  JSON. If the field name is not found, or the JSON expression is not a
  JSON object, a SQL `NULL` is returned.

**Return type**

`JSON`

**Example**

In the following example:

+ `json_value` is a JSON expression.
+ `.class` is a JSON field access.
+ `.students` is a JSON field access.
+ `[0]` is a JSON subscript expression with an element offset that
  accesses the zeroth element of an array in the JSON value.
+ `['name']` is a JSON subscript expression with a field name that
  accesses a field.

```sql
SELECT json_value.class.students[0]['name'] AS first_student
FROM
  UNNEST(
    [
      JSON '{"class" : {"students" : [{"name" : "Jane"}]}}',
      JSON '{"class" : {"students" : []}}',
      JSON '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'])
    AS json_value;

+-----------------+
| first_student   |
+-----------------+
| "Jane"          |
| NULL            |
| "John"          |
+-----------------+
```

### Array elements field access operator 
<a id="array_el_field_operator"></a>

```
array_expression.field_or_element[. ...]

field_or_element:
  { fieldname | array_element }

array_element:
  array_fieldname[array_subscript_specifier]
```

Note: The brackets (`[]`) around `array_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

The array elements field access operation lets you traverse through the
levels of a nested data type inside an array.

**Input types**

+ `array_expression`: An expression that evaluates to an `ARRAY` value.
+ `field_or_element[. ...]`: The field to access. This can also be a position
  in an `ARRAY`-typed field.
+ `fieldname`: The name of the field to access.

  For example, this query returns all values for the `items` field inside of the
  `my_array` array expression:

  ```sql
  WITH T AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items)
  FROM T
  ```
+ `array_element`: If the field to access is an `ARRAY` field (`array_field`),
  you can additionally access a specific position in the field
  with the [array subscript operator][array-subscript-operator]
  (`[array_subscript_specifier]`). This operation returns only elements at a
  selected position, rather than all elements, in the array field.

  For example, this query only returns values at position 0 in the `items`
  array field:

  ```sql
  WITH T AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items[OFFSET(0)])
  FROM T
  ```

**Details**

The array elements field access operation is not a typical expression
that returns a typed value; it represents a concept outside the type system
and can only be interpreted by the following operations:

+  [`FLATTEN` operation][flatten-operation]: Returns an array. For example:

   ```sql
   FLATTEN(x.y.z)
   ```
+  [`UNNEST` operation][operators-link-to-unnest]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `FLATTEN` operator.
   For example, these do the same thing:

   ```sql
   UNNEST(x.y.z)
   ```

   ```sql
   UNNEST(FLATTEN(x.y.z))
   ```
+  [`FROM` operation][operators-link-to-from-clause]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `UNNEST` operator and the `FLATTEN` operator.
   For example, these do the same thing:

   ```sql
   SELECT * FROM T, T.x.y.z;
   ```

   ```sql
   SELECT * FROM T, UNNEST(x.y.z);
   ```

   ```sql
   SELECT * FROM T, UNNEST(FLATTEN(x.y.z));
   ```

If `NULL` array elements are encountered, they are added to the resulting array.

**Common shapes of this operation**

This operation can take several shapes. The right-most value in
the operation determines what type of array is returned. Here are some example
shapes and a description of what they return:

The following shapes extract the final non-array field from each element of
an array expression and return an array of those non-array field values.

+ `array_expression.non_array_field_1`
+ `array_expression.non_array_field_1.array_field.non_array_field_2`

The following shapes extract the final array field from each element of the
array expression and concatenate the array fields together.
An empty array or a `NULL` array contributes no elements to the resulting array.

+ `array_expression.non_array_field_1.array_field_1`
+ `array_expression.non_array_field_1.array_field_1.non_array_field_2.array_field_2`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1`

The following shapes extract the final array field from each element of the
array expression at a specific position. Then they return an array of those
extracted elements. An empty array or a `NULL` array contributes no elements
to the resulting array.

+ `array_expression.non_array_field_1.array_field_1[OFFSET(1)]`
+ `array_expression.non_array_field_1.array_field_1[SAFE_OFFSET(1)]`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1[ORDINAL(2)]`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1[SAFE_ORDINAL(2)]`

**Return Value**

+ `FLATTEN` of an array element access operation returns an `ARRAY`.
+ `UNNEST` of an array element access operation, whether explicit or implicit,
   returns a table.

**Examples**

The next examples in this section reference a table called `T`, that contains
a nested struct in an array called `my_array`:

```sql
WITH
  T AS (
    SELECT
      [
        STRUCT(
          [
            STRUCT([25.0, 75.0] AS prices),
            STRUCT([30.0] AS prices)
          ] AS sales
        )
      ] AS my_array
  )
SELECT * FROM T;

+----------------------------------------------+
| my_array                                     |
+----------------------------------------------+
| [{[{[25, 75] prices}, {[30] prices}] sales}] |
+----------------------------------------------+
```

This is what the array elements field access operator looks like in the
`FLATTEN` operator:

```sql
SELECT FLATTEN(my_array.sales.prices) AS all_prices FROM T;

+--------------+
| all_prices   |
+--------------+
| [25, 75, 30] |
+--------------+
```

This is how you use the array subscript operator to only return values at a
specific index in the `prices` array:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(0)]) AS first_prices FROM T;

+--------------+
| first_prices |
+--------------+
| [25, 30]     |
+--------------+
```

This is an example of an explicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM T, UNNEST(my_array.sales.prices) AS all_prices

+------------+
| all_prices |
+------------+
| 25         |
| 75         |
| 30         |
+------------+
```

This is an example of an implicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM T, T.my_array.sales.prices AS all_prices

+------------+
| all_prices |
+------------+
| 25         |
| 75         |
| 30         |
+------------+
```

This query produces an error because one of the `prices` arrays does not have
an element at index `1` and `OFFSET` is used:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(1)]) AS second_prices FROM T;

-- Error
```

This query is like the previous query, but `SAFE_OFFSET` is used. This
produces a `NULL` value instead of an error.

```sql
SELECT FLATTEN(my_array.sales.prices[SAFE_OFFSET(1)]) AS second_prices FROM T;

+---------------+
| second_prices |
+---------------+
| [75, NULL]    |
+---------------+
```

In this next example, an empty array and a `NULL` field value have been added to
the query. These contribute no elements to the result.

```sql
WITH
  T AS (
    SELECT
      [
        STRUCT(
          [
            STRUCT([25.0, 75.0] AS prices),
            STRUCT([30.0] AS prices),
            STRUCT(ARRAY<DOUBLE>[] AS prices),
            STRUCT(NULL AS prices)
          ] AS sales
        )
      ] AS my_array
  )
SELECT FLATTEN(my_array.sales.prices) AS first_prices FROM T;

+--------------+
| first_prices |
+--------------+
| [25, 75, 30] |
+--------------+
```

The next examples in this section reference a protocol buffer called
`Album` that looks like this:

```proto
message Album {
  optional string album_name = 1;
  repeated string song = 2;
  oneof group_name {
    string solo = 3;
    string duet = 4;
    string band = 5;
  }
}
```

Nested data is common in protocol buffers that have data within repeated
messages. The following example extracts a flattened array of songs from a
table called `AlbumList` that contains a column called `Album` of type `PROTO`.

```sql
WITH
  AlbumList AS (
    SELECT
      [
        NEW Album(
          'OneWay' AS album_name,
          ['North', 'South'] AS song,
          'Crossroads' AS band),
        NEW Album(
          'After Hours' AS album_name,
          ['Snow', 'Ice', 'Water'] AS song,
          'Sunbirds' AS band)]
        AS albums_array
  )
SELECT FLATTEN(albums_array.song) AS songs FROM AlbumList

+------------------------------+
| songs                        |
+------------------------------+
| [North,South,Snow,Ice,Water] |
+------------------------------+
```

The following example extracts a flattened array of album names from a
table called `AlbumList` that contains a proto-typed column called `Album`.

```sql
WITH
  AlbumList AS (
    SELECT
      [
        (
          SELECT
            NEW Album(
              'OneWay' AS album_name,
              ['North', 'South'] AS song,
              'Crossroads' AS band) AS album_col
        ),
        (
          SELECT
            NEW Album(
              'After Hours' AS album_name,
              ['Snow', 'Ice', 'Water'] AS song,
              'Sunbirds' AS band) AS album_col
        )]
        AS albums_array
  )
SELECT names FROM AlbumList, UNNEST(albums_array.album_name) AS names

+----------------------+
| names                |
+----------------------+
| [OneWay,After Hours] |
+----------------------+
```

### Arithmetic operators

All arithmetic operators accept input of numeric type T, and the result type
has type T unless otherwise indicated in the description below:

<table>
  <thead>
    <tr>
    <th>Name</th>
    <th>Syntax</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Addition</td>
      <td>X + Y</td>
    </tr>
    <tr>
      <td>Subtraction</td>
      <td>X - Y</td>
    </tr>
    <tr>
      <td>Multiplication</td>
      <td>X * Y</td>
    </tr>
    <tr>
      <td>Division</td>
      <td>X / Y</td>
    </tr>
    <tr>
      <td>Unary Plus</td>
      <td>+ X</td>
    </tr>
    <tr>
      <td>Unary Minus</td>
      <td>- X</td>
    </tr>
  </tbody>
</table>

NOTE: Divide by zero operations return an error. To return a different result,
consider the IEEE_DIVIDE or SAFE_DIVIDE functions.

Result types for Addition and Multiplication:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Subtraction:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Division:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Unary Plus:

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT32</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Unary Minus:

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### Date arithmetics operators
Operators '+' and '-' can be used for arithmetic operations on dates.

```sql
date_expression + int64_expression
int64_expression + date_expression
date_expression - int64_expression
```

**Description**

Adds or subtracts `int64_expression` days to or from `date_expression`. This is
equivalent to `DATE_ADD` or `DATE_SUB` functions, when interval is expressed in
days.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE "2020-09-22" + 1 AS day_later, DATE "2020-09-22" - 7 AS week_ago

+------------+------------+
| day_later  | week_ago   |
+------------+------------+
| 2020-09-23 | 2020-09-15 |
+------------+------------+
```

### Datetime subtraction

```sql
date_expression - date_expression
timestamp_expression - timestamp_expression
datetime_expression - datetime_expression
```

**Description**

Computes the difference between two datetime values as an interval.

**Return Data Type**

INTERVAL

**Example**

```sql
SELECT
  DATE "2021-05-20" - DATE "2020-04-19" AS date_diff,
  TIMESTAMP "2021-06-01 12:34:56.789" - TIMESTAMP "2021-05-31 00:00:00" AS time_diff

+-------------------+------------------------+
| date_diff         | time_diff              |
+-------------------+------------------------+
| 0-0 396 0:0:0     | 0-0 0 36:34:56.789     |
+-------------------+------------------------+
```

### Interval arithmetic operators

**Addition and subtraction**

```sql
date_expression + interval_expression = DATETIME
date_expression - interval_expression = DATETIME
timestamp_expression + interval_expression = TIMESTAMP
timestamp_expression - interval_expression = TIMESTAMP
datetime_expression + interval_expression = DATETIME
datetime_expression - interval_expression = DATETIME

```

**Description**

Adds an interval to a datetime value or subtracts an interval from a datetime
value.
**Example**

```sql
SELECT
  DATE "2021-04-20" + INTERVAL 25 HOUR AS date_plus,
  TIMESTAMP "2021-05-02 00:01:02.345" - INTERVAL 10 SECOND AS time_minus;

+-------------------------+--------------------------------+
| date_plus               | time_minus                     |
+-------------------------+--------------------------------+
| 2021-04-21 01:00:00     | 2021-05-02 00:00:52.345+00     |
+-------------------------+--------------------------------+
```

**Multiplication and division**

```sql
interval_expression * integer_expression = INTERVAL
interval_expression / integer_expression = INTERVAL

```

**Description**

Multiplies or divides an interval value by an integer.

**Example**

```sql
SELECT
  INTERVAL '1:2:3' HOUR TO SECOND * 10 AS mul1,
  INTERVAL 35 SECOND * 4 AS mul2,
  INTERVAL 10 YEAR / 3 AS div1,
  INTERVAL 1 MONTH / 12 AS div2

+----------------+--------------+-------------+--------------+
| mul1           | mul2         | div1        | div2         |
+----------------+--------------+-------------+--------------+
| 0-0 0 10:20:30 | 0-0 0 0:2:20 | 3-4 0 0:0:0 | 0-0 2 12:0:0 |
+----------------+--------------+-------------+--------------+
```

### Bitwise operators
All bitwise operators return the same type
 and the same length as
the first operand.

<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th style="white-space:nowrap">Input Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Bitwise not</td>
<td>~ X</td>
<td style="white-space:nowrap">Integer or BYTES</td>
<td>Performs logical negation on each bit, forming the ones' complement of the
given binary value.</td>
</tr>
<tr>
<td>Bitwise or</td>
<td>X | Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical inclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise xor</td>
<td style="white-space:nowrap">X ^ Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical exclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise and</td>
<td style="white-space:nowrap">X &amp; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical AND
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Left shift</td>
<td style="white-space:nowrap">X &lt;&lt; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: INT64</td>
<td>Shifts the first operand X to the left.
This operator returns
0 or a byte sequence of b'\x00'
if the second operand Y is greater than or equal to

the bit length of the first operand X (for example, 64 if X has the type INT64).

This operator throws an error if Y is negative.</td>
</tr>
<tr>
<td>Right shift</td>
<td style="white-space:nowrap">X &gt;&gt; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: INT64</td>
<td>Shifts the first operand X to the right. This operator does not do sign bit
extension with a signed type (i.e. it fills vacant bits on the left with 0).
This operator returns
0 or a byte sequence of b'\x00'
if the second operand Y is greater than or equal to

the bit length of the first operand X (for example, 64 if X has the type INT64).

This operator throws an error if Y is negative.</td>
</tr>
</tbody>
</table>

### Logical operators

ZetaSQL supports the `AND`, `OR`, and  `NOT` logical operators.
Logical operators allow only BOOL or `NULL` input
and use [three-valued logic][three-valued-logic]
to produce a result. The result can be `TRUE`, `FALSE`, or `NULL`:

| x       | y       | x AND y | x OR y |
| ------- | ------- | ------- | ------ |
| TRUE    | TRUE    | TRUE    | TRUE   |
| TRUE    | FALSE   | FALSE   | TRUE   |
| TRUE    | NULL    | NULL    | TRUE   |
| FALSE   | TRUE    | FALSE   | TRUE   |
| FALSE   | FALSE   | FALSE   | FALSE  |
| FALSE   | NULL    | FALSE   | NULL   |
| NULL    | TRUE    | NULL    | TRUE   |
| NULL    | FALSE   | FALSE   | NULL   |
| NULL    | NULL    | NULL    | NULL   |

| x       | NOT x   |
| ------- | ------- |
| TRUE    | FALSE   |
| FALSE   | TRUE    |
| NULL    | NULL    |

**Examples**

The examples in this section reference a table called `entry_table`:

```sql
+-------+
| entry |
+-------+
| a     |
| b     |
| c     |
| NULL  |
+-------+
```

```sql
SELECT 'a' FROM entry_table WHERE entry = 'a'

-- a => 'a' = 'a' => TRUE
-- b => 'b' = 'a' => FALSE
-- NULL => NULL = 'a' => NULL

+-------+
| entry |
+-------+
| a     |
+-------+
```

```sql
SELECT entry FROM entry_table WHERE NOT (entry = 'a')

-- a => NOT('a' = 'a') => NOT(TRUE) => FALSE
-- b => NOT('b' = 'a') => NOT(FALSE) => TRUE
-- NULL => NOT(NULL = 'a') => NOT(NULL) => NULL

+-------+
| entry |
+-------+
| b     |
| c     |
+-------+
```

```sql
SELECT entry FROM entry_table WHERE entry IS NULL

-- a => 'a' IS NULL => FALSE
-- b => 'b' IS NULL => FALSE
-- NULL => NULL IS NULL => TRUE

+-------+
| entry |
+-------+
| NULL  |
+-------+
```

### Comparison operators

Comparisons always return BOOL. Comparisons generally
require both operands to be of the same type. If operands are of different
types, and if ZetaSQL can convert the values of those types to a
common type without loss of precision, ZetaSQL will generally coerce
them to that common type for the comparison; ZetaSQL will generally
[coerce literals to the type of non-literals][link-to-coercion], where
present. Comparable data types are defined in
[Data Types][operators-link-to-data-types].

NOTE: ZetaSQL allows comparisons
between signed and unsigned integers.

STRUCTs support only 4 comparison operators: equal
(=), not equal (!= and <>), and IN.

The following rules apply when comparing these data types:

+  Floating point:
   All comparisons with NaN return FALSE,
   except for `!=` and `<>`, which return TRUE.
+  BOOL: FALSE is less than TRUE.
+  STRING: Strings are
   compared codepoint-by-codepoint, which means that canonically equivalent
   strings are only guaranteed to compare as equal if
   they have been normalized first.
+  `NULL`: The convention holds here: any operation with a `NULL` input returns
   `NULL`.

<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Less Than</td>
<td>X &lt; Y</td>
<td>
  Returns TRUE if X is less than Y.
  

</td>
</tr>
<tr>
<td>Less Than or Equal To</td>
<td>X &lt;= Y</td>
<td>
  Returns TRUE if X is less than or equal to Y.
  

</td>
</tr>
<tr>
<td>Greater Than</td>
<td>X &gt; Y</td>
<td>
  Returns TRUE if X is greater than Y.
  

</td>
</tr>
<tr>
<td>Greater Than or Equal To</td>
<td>X &gt;= Y</td>
<td>
  Returns TRUE if X is greater than or equal to Y.
  

</td>
</tr>
<tr>
<td>Equal</td>
<td>X = Y</td>
<td>
  Returns TRUE if X is equal to Y.
  

</td>
</tr>
<tr>
<td>Not Equal</td>
<td>X != Y<br>X &lt;&gt; Y</td>
<td>
  Returns TRUE if X is not equal to Y.
  

</td>
</tr>
<tr>
<td>BETWEEN</td>
<td>X [NOT] BETWEEN Y AND Z</td>
<td>
  <p>
    Returns TRUE if X is [not] within the range specified. The result of "X
    BETWEEN Y AND Z" is equivalent to "Y &lt;= X AND X &lt;= Z" but X is
    evaluated only once in the former.
    

  </p>
</td>
</tr>
<tr>
<td>LIKE</td>
<td>X [NOT] LIKE Y</td>
<td>Checks if the STRING in the first operand X
matches a pattern specified by the second operand Y. Expressions can contain
these characters:
<ul>
<li>A percent sign "%" matches any number of characters or bytes</li>
<li>An underscore "_" matches a single character or byte</li>
<li>You can escape "\", "_", or "%" using two backslashes. For example, <code>
"\\%"</code>. If you are using raw strings, only a single backslash is
required. For example, <code>r"\%"</code>.</li>
</ul>
</td>
</tr>
<tr>
<td>IN</td>
<td>Multiple - see below</td>
<td>
  Returns FALSE if the right operand is empty. Returns <code>NULL</code> if
  the left operand is <code>NULL</code>. Returns TRUE or <code>NULL</code>,
  never FALSE, if the right operand contains <code>NULL</code>. Arguments on
  either side of IN are general expressions. Neither operand is required to be
  a literal, although using a literal on the right is most common. X is
  evaluated only once.
  

</td>
</tr>
</tbody>
</table>

When testing values that have a STRUCT data type for
equality, it's possible that one or more fields are `NULL`. In such cases:

+ If all non-NULL field values are equal, the comparison returns NULL.
+ If any non-NULL field values are not equal, the comparison returns false.

The following table demonstrates how STRUCT data
types are compared when they have fields that are `NULL` valued.

<table>
<thead>
<tr>
<th>Struct1</th>
<th>Struct2</th>
<th>Struct1 = Struct2</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(2, NULL)</code></td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td><code>STRUCT(1,2)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
</tbody>
</table>

### IN operator 
<a id="in_operators"></a>

The `IN` operator supports the following syntax:

```sql
search_value [NOT] IN value_set

value_set:
  {
    (expression[, ...])
    | (subquery)
    | UNNEST(array_expression)
  }
```

**Description**

Checks for an equal value in a set of values.
[Semantic rules][semantic-rules-in] apply, but in general, `IN` returns `TRUE`
if an equal value is found, `FALSE` if an equal value is excluded, otherwise
`NULL`. `NOT IN` returns `FALSE` if an equal value is found, `TRUE` if an
equal value is excluded, otherwise `NULL`.

+ `search_value`: The expression that is compared to a set of values.
+ `value_set`: One or more values to compare to a search value.
   + `(expression[, ...])`: A list of expressions.
   + `(subquery)`: A [subquery][operators-subqueries] that returns
     a single column. The values in that column are the set of values.
     If no rows are produced, the set of values is empty.
   + `UNNEST(array_expression)`: An [UNNEST operator][operators-link-to-unnest]
      that returns a column of values from an array expression. This is
      equivalent to:

      ```sql
      IN (SELECT element FROM UNNEST(array_expression) AS element)
      ```

<a id="semantic_rules_in"></a>
**Semantic rules**

When using the `IN` operator, the following semantics apply in this order:

+ Returns `FALSE` if `value_set` is empty.
+ Returns `NULL` if `search_value` is `NULL`.
+ Returns `TRUE` if `value_set` contains a value equal to `search_value`.
+ Returns `NULL` if `value_set` contains a `NULL`.
+ Returns `FALSE`.

When using the `NOT IN` operator, the following semantics apply in this order:

+ Returns `TRUE` if `value_set` is empty.
+ Returns `NULL` if `search_value` is `NULL`.
+ Returns `FALSE` if `value_set` contains a value equal to `search_value`.
+ Returns `NULL` if `value_set` contains a `NULL`.
+ Returns `TRUE`.

This operator generally supports [collation][link-collation-concepts],
however, `x [NOT] IN UNNEST` is not supported.

[link-collation-concepts]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md

The semantics of:

```
x IN (y, z, ...)
```

are defined as equivalent to:

```
(x = y) OR (x = z) OR ...
```

and the subquery and array forms are defined similarly.

```
x NOT IN ...
```

is equivalent to:

```
NOT(x IN ...)
```

The `UNNEST` form treats an array scan like `UNNEST` in the
[FROM][operators-link-to-from-clause] clause:

```
x [NOT] IN UNNEST(<array expression>)
```

This form is often used with `ARRAY` parameters. For example:

```
x IN UNNEST(@array_parameter)
```

See the [Arrays][operators-link-to-filtering-arrays] topic for more information
on how to use this syntax.

`IN` can be used with multi-part keys by using the struct constructor syntax.
For example:

```
(Key1, Key2) IN ( (12,34), (56,78) )
(Key1, Key2) IN ( SELECT (table.a, table.b) FROM table )
```

See the [Struct Type][operators-link-to-struct-type] for more information.

**Return Data Type**

`BOOL`

**Examples**

You can use these `WITH` clauses to emulate temporary tables for
`Words` and `Items` in the following examples:

```sql
WITH Words AS (
  SELECT 'Intend' as value UNION ALL
  SELECT 'Secure' UNION ALL
  SELECT 'Clarity' UNION ALL
  SELECT 'Peace' UNION ALL
  SELECT 'Intend'
 )
SELECT * FROM Words;

+----------+
| value    |
+----------+
| Intend   |
| Secure   |
| Clarity  |
| Peace    |
| Intend   |
+----------+
```

```sql
WITH
  Items AS (
    SELECT STRUCT('blue' AS color, 'round' AS shape) AS info UNION ALL
    SELECT STRUCT('blue', 'square') UNION ALL
    SELECT STRUCT('red', 'round')
  )
SELECT * FROM Items;

+----------------------------+
| info                       |
+----------------------------+
| {blue color, round shape}  |
| {blue color, square shape} |
| {red color, round shape}   |
+----------------------------+
```

Example with `IN` and an expression:

```sql
SELECT * FROM Words WHERE value IN ('Intend', 'Secure');

+----------+
| value    |
+----------+
| Intend   |
| Secure   |
| Intend   |
+----------+
```

Example with `NOT IN` and an expression:

```sql
SELECT * FROM Words WHERE value NOT IN ('Intend');

+----------+
| value    |
+----------+
| Secure   |
| Clarity  |
| Peace    |
+----------+
```

Example with `IN`, a scalar subquery, and an expression:

```sql
SELECT * FROM Words WHERE value IN ((SELECT 'Intend'), 'Clarity');

+----------+
| value    |
+----------+
| Intend   |
| Clarity  |
| Intend   |
+----------+
```

Example with `IN` and an `UNNEST` operation:

```sql
SELECT * FROM Words WHERE value IN UNNEST(['Secure', 'Clarity']);

+----------+
| value    |
+----------+
| Secure   |
| Clarity  |
+----------+
```

Example with `IN` and a `STRUCT`:

```sql
SELECT
  (SELECT AS STRUCT Items.info) as item
FROM
  Items
WHERE (info.shape, info.color) IN (('round', 'blue'));

+------------------------------------+
| item                               |
+------------------------------------+
| { {blue color, round shape} info } |
+------------------------------------+
```

### IS operators

IS operators return TRUE or FALSE for the condition they are testing. They never
return `NULL`, even for `NULL` inputs, unlike the IS\_INF and IS\_NAN functions
defined in [Mathematical Functions][operators-link-to-math-functions]. If NOT is present,
the output BOOL value is inverted.

<table>
<thead>
<tr>
<th>Function Syntax</th>
<th>Input Data Type</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
  <td><pre>X IS [NOT] NULL</pre></td>
<td>Any value type</td>
<td>BOOL</td>
<td>Returns TRUE if the operand X evaluates to <code>NULL</code>, and returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] TRUE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to TRUE. Returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] FALSE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to FALSE. Returns FALSE
otherwise.</td>
</tr>
</tbody>
</table>

### IS DISTINCT FROM operator 
<a id="is_distinct"></a>

```sql
expression_1 IS [NOT] DISTINCT FROM expression_2
```

**Description**

`IS DISTINCT FROM` returns `TRUE` if the input values are considered to be
distinct from each other by the [`DISTINCT`][operators-distinct] and
[`GROUP BY`][operators-group-by] clauses. Otherwise, returns `FALSE`.

`a IS DISTINCT FROM b` being `TRUE` is equivalent to:

+ `SELECT COUNT(DISTINCT x) FROM UNNEST([a,b]) x` returning `2`.
+ `SELECT * FROM UNNEST([a,b]) x GROUP BY x` returning 2 rows.

`a IS DISTINCT FROM b` is equivalent to `NOT (a = b)`, except for the
following cases:

+ This operator never returns `NULL` so `NULL` values are considered to be
  distinct from non-`NULL` values, not other `NULL` values.
+ `NaN` values are considered to be distinct from non-`NaN` values, but not
  other `NaN` values.

**Input types**

+ `expression_1`: The first value to compare. This can be a groupable data type,
  `NULL` or `NaN`.
+ `expression_2`: The second value to compare. This can be a groupable
  data type, `NULL` or `NaN`.
+ `NOT`: If present, the output `BOOL` value is inverted.

**Return type**

`BOOL`

**Examples**

These return `TRUE`:

```sql
SELECT 1 IS DISTINCT FROM 2
```

```sql
SELECT 1 IS DISTINCT FROM NULL
```

```sql
SELECT 1 IS NOT DISTINCT FROM 1
```

```sql
SELECT NULL IS NOT DISTINCT FROM NULL
```

These return `FALSE`:

```sql
SELECT NULL IS DISTINCT FROM NULL
```

```sql
SELECT 1 IS DISTINCT FROM 1
```

```sql
SELECT 1 IS NOT DISTINCT FROM 2
```

```sql
SELECT 1 IS NOT DISTINCT FROM NULL
```

### Concatenation operator

The concatenation operator combines multiple values into one.

<table>
<thead>
<tr>
<th>Function Syntax</th>
<th>Input Data Type</th>
<th>Result Data Type</th>
</tr>
</thead>
<tbody>
<tr>
  <td><pre>STRING || STRING [ || ... ]</pre></td>
<td>STRING</td>
<td>STRING</td>
</tr>
<tr>
  <td><pre>BYTES || BYTES [ || ... ]</pre></td>
<td>BYTES</td>
<td>STRING</td>
</tr>
<tr>
  <td><pre>ARRAY&#60;T&#62; || ARRAY&#60;T&#62; [ || ... ]</pre></td>
<td>ARRAY&#60;T&#62;</td>
<td>ARRAY&#60;T&#62;</td>
</tr>
</tbody>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[three-valued-logic]: https://en.wikipedia.org/wiki/Three-valued_logic

[semantic-rules-in]: #semantic_rules_in

[array-subscript-operator]: #array_subscript_operator

[operators-link-to-filtering-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering_arrays

[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[operators-link-to-unnest]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unnest_operator

[operators-distinct]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_distinct

[operators-group-by]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[operators-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#about_subqueries

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-math-functions]: https://github.com/google/zetasql/blob/master/docs/mathematical_functions.md

[link-to-coercion]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#coercion

[operators-link-to-array-safeoffset]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#safe-offset-and-safe-ordinal

[flatten-operation]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#flatten

<!-- mdlint on -->

