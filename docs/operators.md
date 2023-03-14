

# Operators

ZetaSQL supports operators.
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
      <td><span><code>JSON</code></span><br><span><code>PROTO</code></span><br><span><code>STRUCT</code></span><br></td>
      <td>Field access operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array elements field access operator</td>
      <td><code>ARRAY</code></td>
      <td>Field access operator for elements in an array</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array subscript operator</td>
      <td><code>ARRAY</code></td>
      <td>Array position. Must be used with <coce>OFFSET</code> or <code>ORDINAL</code>&mdash;see
      <a href="https://github.com/google/zetasql/blob/master/docs/array_functions.md">Array Functions</a>

.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>JSON subscript operator</td>
      <td><code>JSON</code></td>
      <td>Field name or array position in JSON.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>2</td>
      <td><code>+</code></td>
      <td>All numeric types</td>
      <td>Unary plus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>-</code></td>
      <td>All numeric types</td>
      <td>Unary minus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>~</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise not</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>3</td>
      <td><code>*</code></td>
      <td>All numeric types</td>
      <td>Multiplication</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>/</code></td>
      <td>All numeric types</td>
      <td>Division</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td><code>||</code></td>
      <td><code>STRING</code>, <code>BYTES</code>, or <code>ARRAY&#60;T&#62;</code></td>
      <td>Concatenation operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>4</td>
      <td><code>+</code></td>
      <td>
        All numeric types, <code>DATE</code> with
        <code>INT64</code>
        , <code>INTERVAL</code>
      </td>
      <td>Addition</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>-</code></td>
      <td>
        All numeric types, <code>DATE</code> with
        <code>INT64</code>
        , <code>INTERVAL</code>
      </td>
      <td>Subtraction</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>5</td>
      <td><code>&lt;&lt;</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise left-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>&gt;&gt;</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise right-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>6</td>
      <td><code>&amp;</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise and</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>7</td>
      <td><code>^</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise xor</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>8</td>
      <td><code>|</code></td>
      <td>Integer or <code>BYTES</code></td>
      <td>Bitwise or</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>9 (Comparison Operators)</td>
      <td><code>=</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>&lt;</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>&gt;</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>&lt;=</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>&gt;=</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>!=</code>, <code>&lt;&gt;</code></td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Not equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>[NOT] LIKE</code></td>
      <td><code>STRING</code> and <code>BYTES</code></td>
      <td>Value does [not] match the pattern specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>[NOT] BETWEEN</code></td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] within the range specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>[NOT] IN</code></td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] in the set of values specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>IS [NOT] NULL</code></td>
      <td>All</td>
      <td>Value is [not] <code>NULL</code></td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>IS [NOT] TRUE</code></td>
      <td><code>BOOL</code></td>
      <td>Value is [not] <code>TRUE</code>.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td><code>IS [NOT] FALSE</code></td>
      <td><code>BOOL</code></td>
      <td>Value is [not] <code>FALSE</code>.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>10</td>
      <td><code>NOT</code></td>
      <td><code>BOOL</code></td>
      <td>Logical <code>NOT</code></td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>11</td>
      <td><code>AND</code></td>
      <td><code>BOOL</code></td>
      <td>Logical <code>AND</code></td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>12</td>
      <td><code>OR</code></td>
      <td><code>BOOL</code></td>
      <td>Logical <code>OR</code></td>
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

Input values:

+ `STRUCT`
+ `PROTO`
+ `JSON`

Note: If the field to access is within a `STRUCT`, you can use the
[struct subscript operator][struct-subscript-operator] to access the field by
its position within the `STRUCT` instead of by its name. Accessing by
a field by position is useful when fields are un-named or have ambiguous names.

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
  { index | position_keyword(index) }

position_keyword:
  { OFFSET | SAFE_OFFSET | ORDINAL | SAFE_ORDINAL }
```

Note: The brackets (`[]`) around `array_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

Gets a value from an array at a specific position.

Input values:

+ `array_expression`: The input array.
+ `position_keyword(index)`: Determines where the index for the array should
  start and how out-of-range indexes are handled. The index is an integer that
  represents a specific position in the array.
  + `OFFSET(index)`: The index starts at zero. Produces an error if the index is
    out of range. Produces the same
    result as `index` by itself.
  + `SAFE_OFFSET(index)`: The index starts at
    zero. Returns `NULL` if the index is out of range.
  + `ORDINAL(index)`: The index starts at one.
    Produces an error if the index is out of range.
  + `SAFE_ORDINAL(index)`: The index starts at
    one. Returns `NULL` if the index is out of range.
+ `index`: An integer that represents a specific position in the array. If used
  by itself without a position keyword, the index starts at zero and produces
  an error if the index is out of range.

**Return type**

`T` where `array_expression` is `ARRAY<T>`.

**Examples**

In following query, the array subscript operator is used to return values at
specific position in `item_array`. This query also shows what happens when you
reference an index (`6`) in an array that is out of range. If the `SAFE` prefix
is included, `NULL` is returned, otherwise an error is produced.

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array,
  item_array[0] AS item_index,
  item_array[OFFSET(0)] AS item_offset,
  item_array[ORDINAL(1)] AS item_ordinal,
  item_array[SAFE_OFFSET(6)] AS item_safe_offset
FROM Items

+---------------------+------------+-------------+--------------+------------------+
| item_array          | item_index | item_offset | item_ordinal | item_safe_offset |
+---------------------+------------+-------------+--------------+------------------+
| [coffee, tea, milk] | coffee     | coffee      | coffee       | NULL             |
+----------------------------------+-------------+--------------+------------------+
```

When you reference an index that is out of range in an array, and a positional
keyword that begins with `SAFE` is not included, an error is produced.
For example:

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[6] AS item_offset
FROM Items

-- Error. Array index 6 is out of bounds.
```

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[OFFSET(6)] AS item_offset
FROM Items

-- Error. Array index 6 is out of bounds.
```

### Struct subscript operator

```
struct_expression[struct_subscript_specifier]

struct_subscript_specifier:
  { index | position_keyword(index) }

position_keyword:
  { OFFSET | ORDINAL }
```

Note: The brackets (`[]`) around `struct_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

Gets the value of a field at a selected position in a struct.

**Input types**

+ `struct_expression`: The input struct.
+ `position_keyword(index)`: Determines where the index for the struct should
  start and how out-of-range indexes are handled. The index is an
  integer literal or constant that represents a specific position in the struct.
  + `OFFSET(index)`: The index starts at zero. Produces an error if the index is
    out of range. Produces the same
    result as `index` by itself.
  + `ORDINAL(index)`: The index starts at one. Produces an error if the index
    is out of range.
+ `index`: An integer literal or constant that represents a specific position in
  the struct. If used by itself without a position keyword, the index starts at
  zero and produces an error if the index is out of range.

Note: The struct subscript operator doesn't support `SAFE` positional keywords
at this time.

**Examples**

In following query, the struct subscript operator is used to return values at
specific locations in `item_struct` using position keywords. This query also
shows what happens when you reference an index (`6`) in an struct that is out of
range.

```sql
WITH Items AS (SELECT STRUCT<INT64, STRING, BOOL>(23, "tea", FALSE) AS item_struct)
SELECT
  item_struct[0] AS field_index,
  item_struct[OFFSET(0)] AS field_offset,
  item_struct[ORDINAL(1)] AS field_ordinal
FROM Items

+-------------+--------------+---------------+
| field_index | field_offset | field_ordinal |
+-------------+--------------+---------------+
| 23          | 23           | 23            |
+-------------+--------------+---------------+
```

When you reference an index that is out of range in a struct, an error is
produced. For example:

```sql
WITH Items AS (SELECT STRUCT<INT64, STRING, BOOL>(23, "tea", FALSE) AS item_struct)
SELECT
  item_struct[6] AS field_offset
FROM Items

-- Error. Field ordinal 6 is out of bounds in STRUCT
```

```sql
WITH Items AS (SELECT STRUCT<INT64, STRING, BOOL>(23, "tea", FALSE) AS item_struct)
SELECT
  item_struct[OFFSET(6)] AS field_offset
FROM Items

-- Error. Field ordinal 6 is out of bounds in STRUCT
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

Input values:

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

### Protocol buffer map subscript operator 
<a id="proto_subscript_operator"></a>

```sql
proto_map_field_expression[proto_subscript_specifier]

proto_subscript_specifier:
  key_keyword(key_name)

key_keyword:
  { KEY | SAFE_KEY }
```

**Description**

Returns the value in a [protocol buffer map][proto-map] for a
given key.

Input values:

+ `proto_map_field_expression`: A protocol buffer map field.
+ `key_keyword(key_name)`: Determines whether to produce `NULL` or
  an error if the key is not present in the protocol buffer map field.
  + `KEY(key_name)`: Returns an error if the key is not present in the
    protocol buffer map field.
  + `SAFE_KEY(key_name)`: Returns `NULL` if the key is not present in the
    protocol buffer map field.
+ `key_name`: The key in the protocol buffer map field. This operator returns
  `NULL` if the key is `NULL`.

**Return type**

In the input protocol buffer map field, `V` as represented in `map<K,V>`.

**Examples**

To illustrate the use of this function, we use the protocol buffer message
`Item`.

```proto
message Item {
  optional map<string, int64> purchased = 1;
};
```

In the following example, the subscript operator returns the value when the key
is present.

```sql
SELECT
  m.purchased[KEY('A')] AS map_value
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;

+-----------+
| map_value |
+-----------+
| 2         |
+-----------+
```

When the key does not exist in the map field and you use `KEY`, an error is
produced. For example:

```sql
-- ERROR: Key not found in map: 2
SELECT
  m.purchased[KEY('B')] AS value
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;
```

When the key does not exist in the map field and you use `SAFE_KEY`,
the subscript operator returns `NULL`. For example:

```sql
SELECT
  CAST(m.purchased[SAFE_KEY('B')] AS safe_key_missing
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;

+------------------+
| safe_key_missing |
+------------------+
| NULL             |
+------------------+
```

The subscript operator returns `NULL` when the map field or key is `NULL`.
For example:

```sql
SELECT
  CAST(NULL AS Item).purchased[KEY('A')] AS null_map,
  m.purchased[KEY(NULL)] AS null_key
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;

+-----------------------+
| null_map  | null_key  |
+-----------------------+
| NULL      | NULL      |
+-----------------------+
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

Input values:

+ `array_expression`: An expression that evaluates to an array value.
+ `field_or_element[. ...]`: The field to access. This can also be a position
  in an array-typed field.
+ `fieldname`: The name of the field to access.

  For example, this query returns all values for the `items` field inside of the
  `my_array` array expression:

  ```sql
  WITH T AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items)
  FROM T
  ```
+ `array_element`: If the field to access is an array field (`array_field`),
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

Details:

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

+ `FLATTEN` of an array element access operation returns an array.
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
          'One Way' AS album_name,
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

The following example extracts a flattened array of album names, one album name
per row. The data comes from a table called `AlbumList` that contains a
proto-typed column called `Album`.

```sql
WITH
  AlbumList AS (
    SELECT
      [
        (
          SELECT
            NEW Album(
              'One Way' AS album_name,
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
| One Way              |
| After Hours          |
+----------------------+
```

### Arithmetic operators

All arithmetic operators accept input of numeric type `T`, and the result type
has type `T` unless otherwise indicated in the description below:

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
      <td><code>X + Y</code></td>
    </tr>
    <tr>
      <td>Subtraction</td>
      <td><code>X - Y</code></td>
    </tr>
    <tr>
      <td>Multiplication</td>
      <td><code>X * Y</code></td>
    </tr>
    <tr>
      <td>Division</td>
      <td><code>X / Y</code></td>
    </tr>
    <tr>
      <td>Unary Plus</td>
      <td><code>+ X</code></td>
    </tr>
    <tr>
      <td>Unary Minus</td>
      <td><code>- X</code></td>
    </tr>
  </tbody>
</table>

NOTE: Divide by zero operations return an error. To return a different result,
consider the `IEEE_DIVIDE` or `SAFE_DIVIDE` functions.

Result types for Addition and Multiplication:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>INT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>INT64</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT64</code></th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>FLOAT</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>DOUBLE</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

Result types for Subtraction:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>INT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>INT64</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT64</code></th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>FLOAT</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>DOUBLE</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

Result types for Division:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>INT32</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>INT64</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT32</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>UINT64</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>FLOAT</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>DOUBLE</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

Result types for Unary Plus:

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>INT32</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT32</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>FLOAT</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

Result types for Unary Minus:

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>INT32</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>FLOAT</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
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

`DATE`

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

`INTERVAL`

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
<td><code>~ X</code></td>
<td style="white-space:nowrap">Integer or <code>BYTES</code></td>
<td>Performs logical negation on each bit, forming the ones' complement of the
given binary value.</td>
</tr>
<tr>
<td>Bitwise or</td>
<td><code>X | Y</code></td>
<td style="white-space:nowrap">
<code>X</code>: Integer or <code>BYTES</code><br>
<code>Y</code>: Same type as <code>X</code>
</td>
<td>Takes two bit patterns of equal length and performs the logical inclusive
<code>OR</code> operation on each pair of the corresponding bits.

This operator throws an error if <code>X</code> and <code>Y</code> are bytes of
different lengths.

</td>
</tr>
<tr>
<td>Bitwise xor</td>
<td style="white-space:nowrap"><code>X ^ Y</code></td>
<td style="white-space:nowrap">
<code>X</code>: Integer or <code>BYTES</code><br>
<code>Y</code>: Same type as <code>X</code>
</td>
<td>Takes two bit patterns of equal length and performs the
logical exclusive <code>OR</code> operation on each pair of the corresponding
bits.

This operator throws an error if <code>X</code> and <code>Y</code> are bytes of
different lengths.

</td>
</tr>
<tr>
<td>Bitwise and</td>
<td style="white-space:nowrap"><code>X &amp; Y</code></td>
<td style="white-space:nowrap">
<code>X</code>: Integer or <code>BYTES</code><br>
<code>Y</code>: Same type as <code>X</code>
</td>
<td>Takes two bit patterns of equal length and performs the
logical <code>AND</code> operation on each pair of the corresponding bits.

This operator throws an error if <code>X</code> and <code>Y</code> are bytes of
different lengths.

</td>
</tr>
<tr>
<td>Left shift</td>
<td style="white-space:nowrap"><code>X &lt;&lt; Y</code></td>
<td style="white-space:nowrap">
<code>X</code>: Integer or <code>BYTES</code><br>
<code>Y</code>: <code>INT64</code>
</td>
<td>Shifts the first operand <code>X</code> to the left.
This operator returns
<code>0</code> or a byte sequence of <code>b'\x00'</code>
if the second operand <code>Y</code> is greater than or equal to

the bit length of the first operand <code>X</code> (for example, <code>64</code>
if <code>X</code> has the type <code>INT64</code>).

This operator throws an error if <code>Y</code> is negative.</td>
</tr>
<tr>
<td>Right shift</td>
<td style="white-space:nowrap"><code>X &gt;&gt; Y</code></td>
<td style="white-space:nowrap">
<code>X</code>: Integer or <code>BYTES</code><br>
<code>Y</code>: <code>INT64</code></td>
<td>Shifts the first operand <code>X</code> to the right. This operator does not
do sign bit extension with a signed type (i.e. it fills vacant bits on the left
with <code>0</code>). This operator returns
<code>0</code> or a byte sequence of
<code>b'\x00'</code>
if the second operand <code>Y</code> is greater than or equal to

the bit length of the first operand <code>X</code> (for example, <code>64</code>
if <code>X</code> has the type <code>INT64</code>).

This operator throws an error if <code>Y</code> is negative.</td>
</tr>
</tbody>
</table>

### Logical operators

ZetaSQL supports the `AND`, `OR`, and  `NOT` logical operators.
Logical operators allow only `BOOL` or `NULL` input
and use [three-valued logic][three-valued-logic]
to produce a result. The result can be `TRUE`, `FALSE`, or `NULL`:

| `x`     | `y`       | `x AND y` | `x OR y` |
| ------- | --------- | --------- | -------- |
| `TRUE`  | `TRUE`    | `TRUE`    | `TRUE`   |
| `TRUE`  | `FALSE`   | `FALSE`   | `TRUE`   |
| `TRUE`  | `NULL`    | `NULL`    | `TRUE`   |
| `FALSE` | `TRUE`    | `FALSE`   | `TRUE`   |
| `FALSE` | `FALSE`   | `FALSE`   | `FALSE`  |
| `FALSE` | `NULL`    | `FALSE`   | `NULL`   |
| `NULL`  | `TRUE`    | `NULL`    | `TRUE`   |
| `NULL`  | `FALSE`   | `FALSE`   | `NULL`   |
| `NULL`  | `NULL`    | `NULL`    | `NULL`   |

| `x`       | `NOT x`   |
| --------- | --------- |
| `TRUE`    | `FALSE`   |
| `FALSE`   | `TRUE`    |
| `NULL`    | `NULL`    |

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

Comparisons always return `BOOL`. Comparisons generally
require both operands to be of the same type. If operands are of different
types, and if ZetaSQL can convert the values of those types to a
common type without loss of precision, ZetaSQL will generally coerce
them to that common type for the comparison; ZetaSQL will generally
coerce literals to the type of non-literals, where
present. Comparable data types are defined in
[Data Types][operators-link-to-data-types].

NOTE: ZetaSQL allows comparisons
between signed and unsigned integers.

Structs support only these comparison operators: equal
(`=`), not equal (`!=` and `<>`), and `IN`.

The comparison operators in this section cannot be used to compare
`JSON` ZetaSQL literals with other `JSON` ZetaSQL literals.
If you need to compare values inside of `JSON`, convert the values to
SQL values first. For more information, see [`JSON` functions][json-functions].

The following rules apply when comparing these data types:

+  Floating point:
   All comparisons with `NaN` return `FALSE`,
   except for `!=` and `<>`, which return `TRUE`.
+  `BOOL`: `FALSE` is less than `TRUE`.
+  `STRING`: Strings are
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
<td><code>X &lt; Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is less than <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td>Less Than or Equal To</td>
<td><code>X &lt;= Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is less than or equal to
  <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td>Greater Than</td>
<td><code>X &gt; Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is greater than <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td>Greater Than or Equal To</td>
<td><code>X &gt;= Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is greater than or equal to
  <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td>Equal</td>
<td><code>X = Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is equal to <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td>Not Equal</td>
<td><code>X != Y</code><br><code>X &lt;&gt; Y</code></td>
<td>
  Returns <code>TRUE</code> if <code>X</code> is not equal to <code>Y</code>.
  

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

</td>
</tr>
<tr>
<td><code>BETWEEN</code></td>
<td><code>X [NOT] BETWEEN Y AND Z</code></td>
<td>
  <p>
    Returns <code>TRUE</code> if <code>X</code> is [not] within the range
    specified. The result of <code>X BETWEEN Y AND Z</code> is equivalent to
    <code>Y &lt;= X AND X &lt;= Z</code> but <code>X</code> is evaluated only
    once in the former.
    

This operator supports specifying <a href="https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about">collation</a>.

  </p>
</td>
</tr>
<tr>
<td><code>LIKE</code></td>
<td><code>X [NOT] LIKE Y</code></td>
<td>
  See the <a href="#like_operator">`LIKE` operator</a>

  for details.
</td>
</tr>
<tr>
<td><code>IN</code></td>
<td>Multiple</td>
<td>
  See the <a href="#in_operator">`IN` operator</a>

  for details.
</td>
</tr>
</tbody>
</table>

When testing values that have a struct data type for
equality, it's possible that one or more fields are `NULL`. In such cases:

+ If all non-`NULL` field values are equal, the comparison returns `NULL`.
+ If any non-`NULL` field values are not equal, the comparison returns `FALSE`.

The following table demonstrates how struct data
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

### `EXISTS` operator 
<a id="exists_operator"></a>

```sql
EXISTS ( subquery )
```

**Description**

Returns `TRUE` if the subquery produces one or more rows. Returns `FALSE` if
the subquery produces zero rows. Never returns `NULL`. To learn more about
how you can use a subquery with `EXISTS`,
see [`EXISTS` subqueries][exists-subqueries].

**Examples**

In this example, the `EXISTS` operator returns `FALSE` because there are no
rows in `Words` where the direction is `south`:

```sql
WITH Words AS (
  SELECT 'Intend' as value, 'east' as direction UNION ALL
  SELECT 'Secure', 'north' UNION ALL
  SELECT 'Clarity', 'west'
 )
SELECT EXISTS ( SELECT value FROM Words WHERE direction = 'south' ) as result;

+--------+
| result |
+--------+
| FALSE  |
+--------+
```

### `IN` operator 
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

This operator supports [collation][collation], but these limitations apply:

+ `[NOT] IN UNNEST` does not support collation.
+ If collation is used with a list of expressions, there must be at least one
  item in the list.

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
[`FROM`][operators-link-to-from-clause] clause:

```
x [NOT] IN UNNEST(<array expression>)
```

This form is often used with array parameters. For example:

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

See the [Struct Type][operators-link-to-struct-type] topic for more information.

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

Example with `IN` and a struct:

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

### `IS` operators

IS operators return TRUE or FALSE for the condition they are testing. They never
return `NULL`, even for `NULL` inputs, unlike the `IS_INF` and `IS_NAN`
functions defined in [Mathematical Functions][operators-link-to-math-functions].
If `NOT` is present, the output `BOOL` value is
inverted.

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
      <td><code>X IS TRUE</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>TRUE</code> if <code>X</code> evaluates to
        <code>TRUE</code>.
        Otherwise, evaluates to <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS NOT TRUE</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>FALSE</code> if <code>X</code> evaluates to
        <code>TRUE</code>.
        Otherwise, evaluates to <code>TRUE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS FALSE</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>TRUE</code> if <code>X</code> evaluates to
        <code>FALSE</code>.
        Otherwise, evaluates to <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS NOT FALSE</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>FALSE</code> if <code>X</code> evaluates to
        <code>FALSE</code>.
        Otherwise, evaluates to <code>TRUE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS NULL</code></td>
      <td>Any value type</td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>TRUE</code> if <code>X</code> evaluates to
        <code>NULL</code>.
        Otherwise evaluates to <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS NOT NULL</code></td>
      <td>Any value type</td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>FALSE</code> if <code>X</code> evaluates to
        <code>NULL</code>.
        Otherwise evaluates to <code>TRUE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS UNKNOWN</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>TRUE</code> if <code>X</code> evaluates to
        <code>NULL</code>.
        Otherwise evaluates to <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>X IS NOT UNKNOWN</code></td>
      <td><code>BOOL</code></td>
      <td><code>BOOL</code></td>
      <td>
        Evaluates to <code>FALSE</code> if <code>X</code> evaluates to
        <code>NULL</code>.
        Otherwise, evaluates to <code>TRUE</code>.
      </td>
    </tr>
  </tbody>
</table>

### `IS DISTINCT FROM` operator 
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

Input values:

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

### `LIKE` operator 
<a id="like_operator"></a>

```sql
expression_1 IS [NOT] LIKE expression_2
```

**Description**

`IS LIKE` returns `TRUE` if the string in the first operand `expression_1`
matches a pattern specified by the second operand `expression_2`,
otherwise returns `FALSE`.

`IS NOT LIKE` returns `TRUE` if the string in the first operand `expression_1`
does not match a pattern specified by the second operand `expression_2`,
otherwise returns `FALSE`.

Expressions can contain these characters:

+   A percent sign (`%`) matches any number of characters or bytes.
+   An underscore (`_`) matches a single character or byte.
+   You can escape `\ `, `_`, or `%` using two backslashes. For example,
    `\\% `. If you are using raw strings, only a single backslash is
    required. For example, `r'\%'`.

This operator supports [collation][collation], but caveats apply:

+   Each `%` character in `expression_2` represents an
    _arbitrary string specifier_. An arbitrary string specifier can represent
    any sequence of `0` or more characters.
+   When an operand has collation, a character in the expression that is not a
    `_` or `%` character represents itself and is considered a
    _single character specifier_.
+   For a contiguous sequence of single character specifiers, equality
    depends on the collator and its language tags and tailoring.

    +   By default, the `und:ci` collator does not fully normalize a string.
        Some canonically equivalent strings are considered unequal for
        both the `=` and `LIKE` operators.

    +   The `LIKE` operator with collation has the same behavior as the `=` operator
        when there are no wildcards in the strings.

    +   Character sequences with secondary or higher-weighted differences are
        considered unequal. This includes accent differences and some
        special cases.

        For example there are three ways to produce German sharp `ß`:

        +`\u1E9E`
        + `\U00DF`
        + `ss`

        `\u1E9E` and `\U00DF` are considered equal but differ in tertiary.
        They are considered equal with `und:ci` collation but different from
        `ss`, which has secondary differences.

    +   Character sequences with tertiary or lower-weighted differences are
        considered equal. This includes case differences and
        kana subtype differences, which are considered equal.
+   There are [ignorable characters][ignorable-chars] defined in Unicode.
    Ignorable characters are ignored in the pattern matching.
+   An error is returned when `_` is not escaped in `expression_2`.
+   `_` is not allowed when the operands have collation specified and the
    collator is performing a binary comparison.

**Return type**

`BOOL`

**Examples**

The following examples illustrate how you can check to see if the string in the
first operand matches a pattern specified by the second operand.

```sql
-- Returns TRUE
SELECT 'apple' LIKE 'a%';
```

```sql
-- Returns FALSE
SELECT '%a' LIKE 'apple';
```

```sql
-- Returns FALSE
SELECT 'apple' NOT LIKE 'a%';
```

```sql
-- Returns TRUE
SELECT '%a' NOT LIKE 'apple';
```

```sql
-- Produces an error
SELECT NULL LIKE 'a%';
```

```sql
-- Produces an error
SELECT 'apple' LIKE NULL;
```

The following examples illustrate how collation can be used with the `LIKE`
operator.

```sql
-- Returns FALSE
'Foo' LIKE '%foo%'
```

```sql
-- Returns TRUE
COLLATE('Foo', 'und:ci') LIKE COLLATE('%foo%', 'und:ci');
```

```sql
-- Returns TRUE
COLLATE('Foo', 'und:ci') = COLLATE('foo', 'und:ci');
```

```sql
-- Produces an error
COLLATE('Foo', 'und:ci') LIKE COLLATE('%foo%', 'binary');
```

```sql
-- Produces an error
COLLATE('Foo', 'und:ci') LIKE COLLATE('%f_o%', 'und:ci');
```

```sql
-- Returns TRUE
COLLATE('Foo_', 'und:ci') LIKE COLLATE('%foo\\_%', 'und:ci');
```

There are two capital forms of `ß`. We can use either `SS` or `ẞ` as upper
case. While the difference between `ß` and `ẞ` is case difference (tertiary
difference), the difference between sharp `s` and `ss` is secondary and
considered not equal using the `und:ci` collator. For example:

```sql
-- Returns FALSE
'MASSE' LIKE 'Maße';
```

```sql
-- Returns FALSE
COLLATE('MASSE', 'und:ci') LIKE '%Maße%';
```

```sql
-- Returns FALSE
COLLATE('MASSE', 'und:ci') = COLLATE('Maße', 'und:ci');
```

The kana differences in Japanese are considered as tertiary or quaternary
differences, and should be considered as equal in the `und:ci` collator with
secondary strength.

+ `'\u3042'` is `'あ'` (hiragana)
+ `'\u30A2'` is `'ア'` (katakana)

For example:

```sql
-- Returns FALSE
'\u3042' LIKE '%\u30A2%';
```

```sql
-- Returns TRUE
COLLATE('\u3042', 'und:ci') LIKE COLLATE('%\u30A2%', 'und:ci');
```

```sql
-- Returns TRUE
COLLATE('\u3042', 'und:ci') = COLLATE('\u30A2', 'und:ci');
```

When comparing two strings, the `und:ci` collator compares the collation units
based on the specification of the collation. Even though the number of
code points is different, the two strings are considered equal when the
collation units are considered the same.

+ `'\u0041\u030A'` is `'Å'` (two code points)
+ `'\u0061\u030A'` is `'å'` (two code points)
+ `'\u00C5'` is `'Å'` (one code point)

In the following examples, the difference between `'\u0061\u030A'` and
`'\u00C5'` is tertiary.

```sql
-- Returns FALSE
'\u0061\u030A' LIKE '%\u00C5%';
```

```sql
-- Returns TRUE
COLLATE('\u0061\u030A', 'und:ci') LIKE '%\u00C5%';
```

```sql
-- Returns TRUE
COLLATE('\u0061\u030A', 'und:ci') = COLLATE('\u00C5', 'und:ci');
```

In the following example, `'\u0083'` is a `NO BREAK HERE` character and
is ignored.

```sql
-- Returns FALSE
'\u0083' LIKE '';
```

```sql
-- Returns TRUE
COLLATE('\u0083', 'und:ci') LIKE '';
```

### `NEW` operator 
<a id="new_operator"></a>

The `NEW` operator supports only protocol buffers and uses the following syntax:

 + `NEW protocol_buffer {...}`: Creates a
protocol buffer using a map constructor.

  ```sql
  NEW protocol_buffer {
    field_name: literal_or_expression
    field_name { ... }
    repeated_field_name: [literal_or_expression, ... ]
    map_field_name: [{key: literal_or_expression value: literal_or_expression}, ...],
    (extension_name): literal_or_expression
  }
  ```
+   `NEW protocol_buffer (...)`: Creates a protocol buffer using a parenthesized
    list of arguments.

    ```sql
    NEW protocol_buffer(field [AS alias], ...field [AS alias])
    ```

**Examples**

Example with a map constructor:

```sql
NEW Universe {
  name: "Sol"
  closest_planets: ["Mercury", "Venus", "Earth" ]
  star {
    radius_miles: 432,690
    age: 4,603,000,000
  }
  constellations [{
    name: "Libra"
    index: 0
  }, {
    name: "Scorpio"
    index: 1
  }]
  planet_distances: [{
    key: "Mercury"
    distance: 46,507,000
  }, {
    key: "Venus"
    distance: 107,480,000
  }],
  (UniverseExtraInfo.extension) {
    ...
  }
  all_planets: (SELECT planets FROM SolTable)
}
```

Example with a parenthesized list of arguments:

```sql
SELECT
  key,
  name,
  NEW zetasql.examples.music.Chart { rank: 1 chart_name: "2" }
```

To learn more about protocol buffers in ZetaSQL, see [Work with
protocol buffers][protocol-buffers].

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
  <td><code>STRING || STRING [ || ... ]</code></td>
  <td><code>STRING</code></td>
  <td><code>STRING</code></td>
</tr>
<tr>
  <td><code>BYTES || BYTES [ || ... ]</code></td>
  <td><code>BYTES</code></td>
  <td><code>STRING</code></td>
</tr>
<tr>
  <td><code>ARRAY&#60;T&#62; || ARRAY&#60;T&#62; [ || ... ]</code></td>
  <td><code>ARRAY&#60;T&#62;</code></td>
  <td><code>ARRAY&#60;T&#62;</code></td>
</tr>
</tbody>
</table>

### `WITH` operator

```sql
WITH(variable_assignment[, ...], result_expression)

variable_assignment:
  variable_name AS expression
```

**Description**

Create one or more variables. Each variable can be used in subsequent
expressions within the `WITH` operator. Returns the value of
`result_expression`.

+   `variable_assignment`: Introduces a variable. The variable name must be
    unique within a given `WITH` expression. Each expression can reference the
    variables that come before it. For example, if you create variable `a`,
    then follow it with variable `b`, you can reference `a` inside of `b`'s
    expression.
    +   `variable_name`: The name of the variable.
    +   `expression`: The value to assign to the variable.
+   `result_expression`: An expression that is the `WITH` operator's result.
    This expression can use all the variables defined before it.

**Return Type**

+   The type of the `result_expression`.

**Requirements and Caveats**

+   A given variable may only be assigned once in a given `WITH` clause.
+   Variables created during `WITH` may not be used in analytic or
    aggregate function arguments. For example, `WITH(a AS ..., SUM(a))` produces
    an error.
+   Volatile expressions (for example,  `RAND()`) behave as if they are
    evaluated only once.

**Examples**

The following example first concatenates variable `a` with `b`, then variable
`b` with `c`:

```sql
SELECT WITH(a AS '123',               -- a is '123'
            b AS CONCAT(a, '456'),    -- b is '123456
            c AS '789',               -- c is '789'
            CONCAT(b, c)) AS result;  -- b + c is '123456789'

+-------------+
| result      |
+-------------+
| '123456789' |
+-------------+
```

In the following example, the volatile expression `RAND()` behaves as if it is
evaluated only once. This means the value of the result expression will always
be zero:

```sql
SELECT WITH(a AS RAND(), a - a);

+---------+
| result  |
+---------+
| 0.0     |
+---------+
```

Aggregate or analytic function results can be stored in variables. In this
example, an average is computed:

```sql
SELECT WITH(s AS SUM(input), c AS COUNT(input), s/c)
FROM UNNEST([1.0, 2.0, 3.0]) AS input;

+---------+
| result  |
+---------+
| 2.0     |
+---------+
```

Variables cannot be used in aggregate or analytic function call arguments:

```sql
SELECT WITH(diff AS a - b, AVG(diff))
FROM UNNEST([
              STRUCT(1 AS a, 2 AS b),
              STRUCT(3 AS a, 4 AS b),
              STRUCT(5 AS a, 6 AS b),
            ]);

-- ERROR: WITH variables like 'diff' cannot be used in aggregate or analytic
-- function arguments.
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[three-valued-logic]: https://en.wikipedia.org/wiki/Three-valued_logic

[semantic-rules-in]: #semantic_rules_in

[array-subscript-operator]: #array_subscript_operator

[struct-subscript-operator]: #struct_subscript_operator

[operators-link-to-filtering-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering_arrays

[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[operators-link-to-unnest]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unnest_operator

[default-und]: https://github.com/unicode-org/cldr/blob/main/common/collation/root.xml

[ignorable-chars]: https://www.unicode.org/charts/collation/chart_Ignored.html

[protocol-buffers]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md

[operators-distinct]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_distinct

[operators-group-by]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[operators-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#about_subqueries

[exists-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#exists_subquery_concepts

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-math-functions]: https://github.com/google/zetasql/blob/master/docs/mathematical_functions.md

[operators-link-to-array-safeoffset]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#safe-offset-and-safe-ordinal

[flatten-operation]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#flatten

[json-functions]: https://github.com/google/zetasql/blob/master/docs/json_functions.md

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_funcs

[proto-map]: https://developers.google.com/protocol-buffers/docs/proto3#maps

<!-- mdlint on -->

