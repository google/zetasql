

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Functions, operators, and conditionals

This topic is a compilation of functions, operators, and
conditional expressions.

To learn more about how to call functions, function call rules,
the `SAFE` prefix, and special types of arguments,
see [Function calls][function-calls].

---
## OPERATORS AND CONDITIONALS

## Operators

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
      <a href="#array_functions">Array Functions</a>

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

/*---------*
 | country |
 +---------+
 | Canada  |
 *---------*/
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

/*---------------------+------------+-------------+--------------+------------------*
 | item_array          | item_index | item_offset | item_ordinal | item_safe_offset |
 +---------------------+------------+-------------+--------------+------------------+
 | [coffee, tea, milk] | coffee     | coffee      | coffee       | NULL             |
 *----------------------------------+-------------+--------------+------------------*/
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

/*-------------+--------------+---------------*
 | field_index | field_offset | field_ordinal |
 +-------------+--------------+---------------+
 | 23          | 23           | 23            |
 *-------------+--------------+---------------*/
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

/*-----------------*
 | first_student   |
 +-----------------+
 | "Jane"          |
 | NULL            |
 | "John"          |
 *-----------------*/
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

/*-----------*
 | map_value |
 +-----------+
 | 2         |
 *-----------*/
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

/*------------------*
 | safe_key_missing |
 +------------------+
 | NULL             |
 *------------------*/
```

The subscript operator returns `NULL` when the map field or key is `NULL`.
For example:

```sql
SELECT
  CAST(NULL AS Item).purchased[KEY('A')] AS null_map,
  m.purchased[KEY(NULL)] AS null_key
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;

/*-----------------------*
 | null_map  | null_key  |
 +-----------------------+
 | NULL      | NULL      |
 *-----------------------*/
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
  WITH MyTable AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items)
  FROM MyTable
  ```

  These data types have fields:

  
  

  + `STRUCT`
  + `PROTO`
  + `JSON`

  
+ `array_element`: If the field to access is an array field (`array_field`),
  you can additionally access a specific position in the field
  with the [array subscript operator][array-subscript-operator]
  (`[array_subscript_specifier]`). This operation returns only elements at a
  selected position, rather than all elements, in the array field.

  For example, this query only returns values at position 0 in the `items`
  array field:

  ```sql
  WITH MyTable AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items[OFFSET(0)])
  FROM MyTable
  ```

Details:

The array elements field access operation is not a typical expression
that returns a typed value; it represents a concept outside the type system
and can only be interpreted by the following operations:

+  [`FLATTEN` operation][flatten-operation]: Returns an array. For example:

   ```sql
   FLATTEN(my_array.sales.prices)
   ```
+  [`UNNEST` operation][operators-link-to-unnest]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `FLATTEN` operator.
   For example, these do the same thing:

   ```sql
   UNNEST(my_array.sales.prices)
   ```

   ```sql
   UNNEST(FLATTEN(my_array.sales.prices))
   ```
+  [`FROM` clause][operators-link-to-from-clause]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `UNNEST` operator and the `FLATTEN` operator.
   For example, these unnesting operations produce the same values for
   `results`:

   ```sql
   SELECT results FROM SalesTable, SalesTable.my_array.sales.prices AS results;
   ```

   ```sql
   SELECT results FROM SalesTable, UNNEST(my_array.sales.prices) AS results;
   ```

   ```sql
   SELECT results FROM SalesTable, UNNEST(FLATTEN(my_array.sales.prices)) AS results;
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

The next examples in this section reference a table called `SalesTable`, that
contains a nested struct in an array called `my_array`:

```sql
WITH
  SalesTable AS (
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
SELECT * FROM SalesTable;

/*----------------------------------------------*
 | my_array                                     |
 +----------------------------------------------+
 | [{[{[25, 75] prices}, {[30] prices}] sales}] |
 *----------------------------------------------*/
```

This is what the array elements field access operator looks like in the
`FLATTEN` operator:

```sql
SELECT FLATTEN(my_array.sales.prices) AS all_prices FROM SalesTable;

/*--------------*
 | all_prices   |
 +--------------+
 | [25, 75, 30] |
 *--------------*/
```

This is how you use the array subscript operator to only return values at a
specific index in the `prices` array:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(0)]) AS first_prices FROM SalesTable;

/*--------------*
 | first_prices |
 +--------------+
 | [25, 30]     |
 *--------------*/
```

This is an example of an explicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM SalesTable, UNNEST(my_array.sales.prices) AS all_prices

/*------------*
 | all_prices |
 +------------+
 | 25         |
 | 75         |
 | 30         |
 *------------*/
```

This is an example of an implicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM SalesTable, SalesTable.my_array.sales.prices AS all_prices

/*------------*
 | all_prices |
 +------------+
 | 25         |
 | 75         |
 | 30         |
 *------------*/
```

This query produces an error because one of the `prices` arrays does not have
an element at index `1` and `OFFSET` is used:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(1)]) AS second_prices FROM SalesTable;

-- Error
```

This query is like the previous query, but `SAFE_OFFSET` is used. This
produces a `NULL` value instead of an error.

```sql
SELECT FLATTEN(my_array.sales.prices[SAFE_OFFSET(1)]) AS second_prices FROM SalesTable;

/*---------------*
 | second_prices |
 +---------------+
 | [75, NULL]    |
 *---------------*/
```

In this next example, an empty array and a `NULL` field value have been added to
the query. These contribute no elements to the result.

```sql
WITH
  SalesTable AS (
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
SELECT FLATTEN(my_array.sales.prices) AS first_prices FROM SalesTable;

/*--------------*
 | first_prices |
 +--------------+
 | [25, 75, 30] |
 *--------------*/
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

/*------------------------------*
 | songs                        |
 +------------------------------+
 | [North,South,Snow,Ice,Water] |
 *------------------------------*/
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

/*----------------------*
 | names                |
 +----------------------+
 | One Way              |
 | After Hours          |
 *----------------------*/
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

/*------------+------------*
 | day_later  | week_ago   |
 +------------+------------+
 | 2020-09-23 | 2020-09-15 |
 *------------+------------*/
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

/*-------------------+------------------------*
 | date_diff         | time_diff              |
 +-------------------+------------------------+
 | 0-0 396 0:0:0     | 0-0 0 36:34:56.789     |
 *-------------------+------------------------*/
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

/*-------------------------+--------------------------------*
 | date_plus               | time_minus                     |
 +-------------------------+--------------------------------+
 | 2021-04-21 01:00:00     | 2021-05-02 00:00:52.345+00     |
 *-------------------------+--------------------------------*/
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

/*----------------+--------------+-------------+--------------*
 | mul1           | mul2         | div1        | div2         |
 +----------------+--------------+-------------+--------------+
 | 0-0 0 10:20:30 | 0-0 0 0:2:20 | 3-4 0 0:0:0 | 0-0 2 12:0:0 |
 *----------------+--------------+-------------+--------------*/
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
/*-------*
 | entry |
 +-------+
 | a     |
 | b     |
 | c     |
 | NULL  |
 *-------*/
```

```sql
SELECT 'a' FROM entry_table WHERE entry = 'a'

-- a => 'a' = 'a' => TRUE
-- b => 'b' = 'a' => FALSE
-- NULL => NULL = 'a' => NULL

/*-------*
 | entry |
 +-------+
 | a     |
 *-------*/
```

```sql
SELECT entry FROM entry_table WHERE NOT (entry = 'a')

-- a => NOT('a' = 'a') => NOT(TRUE) => FALSE
-- b => NOT('b' = 'a') => NOT(FALSE) => TRUE
-- NULL => NOT(NULL = 'a') => NOT(NULL) => NULL

/*-------*
 | entry |
 +-------+
 | b     |
 | c     |
 *-------*/
```

```sql
SELECT entry FROM entry_table WHERE entry IS NULL

-- a => 'a' IS NULL => FALSE
-- b => 'b' IS NULL => FALSE
-- NULL => NULL IS NULL => TRUE

/*-------*
 | entry |
 +-------+
 | NULL  |
 *-------*/
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

/*--------*
 | result |
 +--------+
 | FALSE  |
 *--------*/
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

/*----------*
 | value    |
 +----------+
 | Intend   |
 | Secure   |
 | Clarity  |
 | Peace    |
 | Intend   |
 *----------*/
```

```sql
WITH
  Items AS (
    SELECT STRUCT('blue' AS color, 'round' AS shape) AS info UNION ALL
    SELECT STRUCT('blue', 'square') UNION ALL
    SELECT STRUCT('red', 'round')
  )
SELECT * FROM Items;

/*----------------------------*
 | info                       |
 +----------------------------+
 | {blue color, round shape}  |
 | {blue color, square shape} |
 | {red color, round shape}   |
 *----------------------------*/
```

Example with `IN` and an expression:

```sql
SELECT * FROM Words WHERE value IN ('Intend', 'Secure');

/*----------*
 | value    |
 +----------+
 | Intend   |
 | Secure   |
 | Intend   |
 *----------*/
```

Example with `NOT IN` and an expression:

```sql
SELECT * FROM Words WHERE value NOT IN ('Intend');

/*----------*
 | value    |
 +----------+
 | Secure   |
 | Clarity  |
 | Peace    |
 *----------*/
```

Example with `IN`, a scalar subquery, and an expression:

```sql
SELECT * FROM Words WHERE value IN ((SELECT 'Intend'), 'Clarity');

/*----------*
 | value    |
 +----------+
 | Intend   |
 | Clarity  |
 | Intend   |
 *----------*/
```

Example with `IN` and an `UNNEST` operation:

```sql
SELECT * FROM Words WHERE value IN UNNEST(['Secure', 'Clarity']);

/*----------*
 | value    |
 +----------+
 | Secure   |
 | Clarity  |
 *----------*/
```

Example with `IN` and a struct:

```sql
SELECT
  (SELECT AS STRUCT Items.info) as item
FROM
  Items
WHERE (info.shape, info.color) IN (('round', 'blue'));

/*------------------------------------*
 | item                               |
 +------------------------------------+
 | { {blue color, round shape} info } |
 *------------------------------------*/
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
  NEW zetasql.examples.music.Chart(key AS rank, name AS chart_name)
FROM
  (SELECT 1 AS key, "2" AS name);
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

/*-------------*
 | result      |
 +-------------+
 | '123456789' |
 *-------------*/
```

In the following example, the volatile expression `RAND()` behaves as if it is
evaluated only once. This means the value of the result expression will always
be zero:

```sql
SELECT WITH(a AS RAND(), a - a);

/*---------*
 | result  |
 +---------+
 | 0.0     |
 *---------*/
```

Aggregate or analytic function results can be stored in variables. In this
example, an average is computed:

```sql
SELECT WITH(s AS SUM(input), c AS COUNT(input), s/c)
FROM UNNEST([1.0, 2.0, 3.0]) AS input;

/*---------*
 | result  |
 +---------+
 | 2.0     |
 *---------*/
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

[field-access-operator]: #field_access_operator

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

[operators-link-to-math-functions]: #mathematical_functions

[operators-link-to-array-safeoffset]: #safe-offset-and-safe-ordinal

[flatten-operation]: #flatten

[json-functions]: #json_functions

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_funcs

[proto-map]: https://developers.google.com/protocol-buffers/docs/proto3#maps

<!-- mdlint on -->

## Conditional expressions

ZetaSQL supports conditional expressions.
Conditional expressions impose constraints on the evaluation order of their
inputs. In essence, they are evaluated left to right, with short-circuiting, and
only evaluate the output value that was chosen. In contrast, all inputs to
regular functions are evaluated before calling the function. Short-circuiting in
conditional expressions can be exploited for error handling or performance
tuning.

### `CASE expr`

```sql
CASE expr
  WHEN expr_to_match THEN result
  [ ... ]
  [ ELSE else_result ]
  END
```

**Description**

Compares `expr` to `expr_to_match` of each successive `WHEN` clause and returns
the first result where this comparison evaluates to `TRUE`. The remaining `WHEN`
clauses and `else_result` aren't evaluated.

If the `expr = expr_to_match` comparison evaluates to `FALSE` or `NULL` for all
`WHEN` clauses, returns the evaluation of `else_result` if present; if
`else_result` isn't present, then returns `NULL`.

Consistent with [equality comparisons][logical-operators] elsewhere, if both
`expr` and `expr_to_match` are `NULL`, then `expr = expr_to_match` evaluates to
`NULL`, which returns `else_result`. If a CASE statement needs to distinguish a
`NULL` value, then the alternate [CASE][case] syntax should be used.

`expr` and `expr_to_match` can be any type. They must be implicitly
coercible to a common [supertype][cond-exp-supertype]; equality comparisons are
done on coerced values. There may be multiple `result` types. `result` and
`else_result` expressions must be coercible to a common supertype.

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```sql
WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 8 UNION ALL
  SELECT 60, 6 UNION ALL
  SELECT 50, 10
)
SELECT
  A,
  B,
  CASE A
    WHEN 90 THEN 'red'
    WHEN 50 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------*
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 8  | blue   |
 | 60 | 6  | green  |
 | 50 | 10 | blue   |
 *------------------*/
```

### `CASE`

```sql
CASE
  WHEN condition THEN result
  [ ... ]
  [ ELSE else_result ]
  END
```

**Description**

Evaluates the condition of each successive `WHEN` clause and returns the
first result where the condition evaluates to `TRUE`; any remaining `WHEN`
clauses and `else_result` aren't evaluated.

If all conditions evaluate to `FALSE` or `NULL`, returns evaluation of
`else_result` if present; if `else_result` isn't present, then returns `NULL`.

For additional rules on how values are evaluated, see the
three-valued logic table in [Logical operators][logical-operators].

`condition` must be a boolean expression. There may be multiple `result` types.
`result` and `else_result` expressions must be implicitly coercible to a common
[supertype][cond-exp-supertype].

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```sql
WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 6 UNION ALL
  SELECT 20, 10
)
SELECT
  A,
  B,
  CASE
    WHEN A > 60 THEN 'red'
    WHEN B = 6 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------*
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 6  | blue   |
 | 20 | 10 | green  |
 *------------------*/
```

### `COALESCE`

```sql
COALESCE(expr[, ...])
```

**Description**

Returns the value of the first non-`NULL` expression, if any, otherwise
`NULL`. The remaining expressions aren't evaluated. An input expression can be
any type. There may be multiple input expression types.
All input expressions must be implicitly coercible to a common
[supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr`[, ...].

**Examples**

```sql
SELECT COALESCE('A', 'B', 'C') as result

/*--------*
 | result |
 +--------+
 | A      |
 *--------*/
```

```sql
SELECT COALESCE(NULL, 'B', 'C') as result

/*--------*
 | result |
 +--------+
 | B      |
 *--------*/
```

### `IF`

```sql
IF(expr, true_result, else_result)
```

**Description**

If `expr` evaluates to `TRUE`, returns `true_result`, else returns the
evaluation for `else_result`. `else_result` isn't evaluated if `expr` evaluates
to `TRUE`. `true_result` isn't evaluated if `expr` evaluates to `FALSE` or
`NULL`.

`expr` must be a boolean expression. `true_result` and `else_result`
must be coercible to a common [supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `true_result` and `else_result`.

**Example**

```sql
WITH Numbers AS (
  SELECT 10 as A, 20 as B UNION ALL
  SELECT 50, 30 UNION ALL
  SELECT 60, 60
)
SELECT
  A,
  B,
  IF(A < B, 'true', 'false') AS result
FROM Numbers

/*------------------*
 | A  | B  | result |
 +------------------+
 | 10 | 20 | true   |
 | 50 | 30 | false  |
 | 60 | 60 | false  |
 *------------------*/
```

### `IFNULL`

```sql
IFNULL(expr, null_result)
```

**Description**

If `expr` evaluates to `NULL`, returns `null_result`. Otherwise, returns
`expr`. If `expr` doesn't evaluate to `NULL`, `null_result` isn't evaluated.

`expr` and `null_result` can be any type and must be implicitly coercible to
a common [supertype][cond-exp-supertype]. Synonym for
`COALESCE(expr, null_result)`.

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` or `null_result`.

**Examples**

```sql
SELECT IFNULL(NULL, 0) as result

/*--------*
 | result |
 +--------+
 | 0      |
 *--------*/
```

```sql
SELECT IFNULL(10, 0) as result

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

### `NULLIF`

```sql
NULLIF(expr, expr_to_match)
```

**Description**

Returns `NULL` if `expr = expr_to_match` evaluates to `TRUE`, otherwise
returns `expr`.

`expr` and `expr_to_match` must be implicitly coercible to a
common [supertype][cond-exp-supertype], and must be comparable.

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` and `expr_to_match`.

**Example**

```sql
SELECT NULLIF(0, 0) as result

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT NULLIF(10, 0) as result

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[cond-exp-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[logical-operators]: #logical_operators

[case]: #case

<!-- mdlint on -->

---
## FUNCTIONS

## Aggregate functions

ZetaSQL supports the following general aggregate functions.
To learn about the syntax for aggregate function calls, see
[Aggregate function calls][agg-function-calls].

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#any_value"><code>ANY_VALUE</code></a>

</td>
  <td>
    Returns an expression for some row.
  </td>
</tr>

<tr>
  <td><a href="#array_agg"><code>ARRAY_AGG</code></a>

</td>
  <td>
    Returns an array of values.
  </td>
</tr>

<tr>
  <td><a href="#array_concat_agg"><code>ARRAY_CONCAT_AGG</code></a>

</td>
  <td>
    Concatenates arrays and returns a single array as a result.
  </td>
</tr>

<tr>
  <td><a href="#avg"><code>AVG</code></a>

</td>
  <td>
    Returns the average of non-<code>NULL</code> values.
  </td>
</tr>

<tr>
  <td><a href="#bit_and"><code>BIT_AND</code></a>

</td>
  <td>
    Performs a bitwise AND operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#bit_or"><code>BIT_OR</code></a>

</td>
  <td>
    Performs a bitwise OR operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#bit_xor"><code>BIT_XOR</code></a>

</td>
  <td>
    Performs a bitwise XOR operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#count"><code>COUNT</code></a>

</td>
  <td>
    Returns the number of rows in the input, or the number of rows with an
    expression evaluated to any value other than <code>NULL</code>.
  </td>
</tr>

<tr>
  <td><a href="#countif"><code>COUNTIF</code></a>

</td>
  <td>
    Returns the count of <code>TRUE</code> values for an expression.
  </td>
</tr>

<tr>
  <td><a href="#logical_and"><code>LOGICAL_AND</code></a>

</td>
  <td>
    Returns the logical AND of all non-<code>NULL</code> expressions.
  </td>
</tr>

<tr>
  <td><a href="#logical_or"><code>LOGICAL_OR</code></a>

</td>
  <td>
    Returns the logical OR of all non-<code>NULL</code> expressions.
  </td>
</tr>

<tr>
  <td><a href="#max"><code>MAX</code></a>

</td>
  <td>
    Returns the maximum non-<code>NULL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#min"><code>MIN</code></a>

</td>
  <td>
    Returns the minimum non-<code>NULL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#string_agg"><code>STRING_AGG</code></a>

</td>
  <td>
    Returns a <code>STRING</code> or <code>BYTES</code> value obtained by
    concatenating non-<code>NULL</code> values.
  </td>
</tr>

<tr>
  <td><a href="#sum"><code>SUM</code></a>

</td>
  <td>
    Returns the sum of non-<code>NULL</code> values.
  </td>
</tr>

  </tbody>
</table>

### `ANY_VALUE`

```sql
ANY_VALUE(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
rows for which `expression` is `NULL` are considered and may be selected.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

Any

**Returned Data Types**

Matches the input data type.

**Examples**

```sql
SELECT ANY_VALUE(fruit) as any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

/*-----------*
 | any_value |
 +-----------+
 | apple     |
 *-----------*/
```

```sql
SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

/*--------+-----------*
 | fruit  | any_value |
 +--------+-----------+
 | pear   | pear      |
 | apple  | pear      |
 | banana | apple     |
 *--------+-----------*/
```

```sql
WITH
  Store AS (
    SELECT 20 AS sold, "apples" AS fruit
    UNION ALL
    SELECT 30 AS sold, "pears" AS fruit
    UNION ALL
    SELECT 30 AS sold, "bananas" AS fruit
    UNION ALL
    SELECT 10 AS sold, "oranges" AS fruit
  )
SELECT ANY_VALUE(fruit HAVING MAX sold) AS a_highest_selling_fruit FROM Store;

/*-------------------------*
 | a_highest_selling_fruit |
 +-------------------------+
 | pears                   |
 *-------------------------*/
```

```sql
WITH
  Store AS (
    SELECT 20 AS sold, "apples" AS fruit
    UNION ALL
    SELECT 30 AS sold, "pears" AS fruit
    UNION ALL
    SELECT 30 AS sold, "bananas" AS fruit
    UNION ALL
    SELECT 10 AS sold, "oranges" AS fruit
  )
SELECT ANY_VALUE(fruit HAVING MIN sold) AS a_lowest_selling_fruit FROM Store;

/*-------------------------*
 | a_lowest_selling_fruit  |
 +-------------------------+
 | oranges                 |
 *-------------------------*/
```

### `ARRAY_AGG`

```sql
ARRAY_AGG(
  [ DISTINCT ]
  expression
  [ { IGNORE | RESPECT } NULLS ]
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns an ARRAY of `expression` values.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

All data types except ARRAY.

**Returned Data Types**

ARRAY

If there are zero input rows, this function returns `NULL`.

**Examples**

```sql
SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST([2, 1,-2, 3, -2, 1, 2]) AS x;

/*-------------------------*
 | array_agg               |
 +-------------------------+
 | [2, 1, -2, 3, -2, 1, 2] |
 *-------------------------*/
```

```sql
SELECT ARRAY_AGG(DISTINCT x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*---------------*
 | array_agg     |
 +---------------+
 | [2, 1, -2, 3] |
 *---------------*/
```

```sql
SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [1, -2, 3, -2, 1] |
 *-------------------*/
```

```sql
SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------------*
 | array_agg               |
 +-------------------------+
 | [1, 1, 2, -2, -2, 2, 3] |
 *-------------------------*/
```

```sql
SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [2, 1, -2, 3, -2] |
 *-------------------*/
```

```sql
WITH vals AS
  (
    SELECT 1 x UNION ALL
    SELECT -2 x UNION ALL
    SELECT 3 x UNION ALL
    SELECT -2 x UNION ALL
    SELECT 1 x
  )
SELECT ARRAY_AGG(DISTINCT x ORDER BY x) as array_agg
FROM vals;

/*------------*
 | array_agg  |
 +------------+
 | [-2, 1, 3] |
 *------------*/
```

```sql
WITH vals AS
  (
    SELECT 1 x, 'a' y UNION ALL
    SELECT 1 x, 'b' y UNION ALL
    SELECT 2 x, 'a' y UNION ALL
    SELECT 2 x, 'c' y
  )
SELECT x, ARRAY_AGG(y) as array_agg
FROM vals
GROUP BY x;

/*---------------*
 | x | array_agg |
 +---------------+
 | 1 | [a, b]    |
 | 2 | [a, c]    |
 *---------------*/
```

```sql
SELECT
  x,
  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*----+-------------------------*
 | x  | array_agg               |
 +----+-------------------------+
 | 1  | [1, 1]                  |
 | 1  | [1, 1]                  |
 | 2  | [1, 1, 2, -2, -2, 2]    |
 | -2 | [1, 1, 2, -2, -2, 2]    |
 | -2 | [1, 1, 2, -2, -2, 2]    |
 | 2  | [1, 1, 2, -2, -2, 2]    |
 | 3  | [1, 1, 2, -2, -2, 2, 3] |
 *----+-------------------------*/
```

### `ARRAY_CONCAT_AGG`

```sql
ARRAY_CONCAT_AGG(
  expression
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
```

**Description**

Concatenates elements from `expression` of type `ARRAY`, returning a single
array as a result.

This function ignores `NULL` input arrays, but respects the `NULL` elements in
non-`NULL` input arrays. Returns `NULL` if there are zero input rows or
`expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`ARRAY`

**Returned Data Types**

`ARRAY`

**Examples**

```sql
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*-----------------------------------*
 | array_concat_agg                  |
 +-----------------------------------+
 | [NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
 *-----------------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x)) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*-----------------------------------*
 | array_concat_agg                  |
 +-----------------------------------+
 | [5, 6, 7, 8, 9, 1, 2, 3, 4]       |
 *-----------------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*--------------------------*
 | array_concat_agg         |
 +--------------------------+
 | [1, 2, 3, 4, 5, 6]       |
 *--------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*------------------*
 | array_concat_agg |
 +------------------+
 | [5, 6, 7, 8, 9]  |
 *------------------*/
```

### `AVG`

```sql
AVG(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the average of non-`NULL` values in an aggregated group.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`AVG` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.
+ If the argument is `[+|-]Infinity` for any row in the group, returns either
  `[+|-]Infinity` or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

+ Any numeric input type
+ `INTERVAL`

**Returned Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT AVG(x) as avg
FROM UNNEST([0, 2, 4, 4, 5]) as x;

/*-----*
 | avg |
 +-----+
 | 3   |
 *-----*/
```

```sql
SELECT AVG(DISTINCT x) AS avg
FROM UNNEST([0, 2, 4, 4, 5]) AS x;

/*------*
 | avg  |
 +------+
 | 2.75 |
 *------*/
```

```sql
SELECT
  x,
  AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x;

/*------+------*
 | x    | avg  |
 +------+------+
 | NULL | NULL |
 | 0    | 0    |
 | 2    | 1    |
 | 4    | 3    |
 | 4    | 4    |
 | 5    | 4.5  |
 *------+------*/
```

[dp-functions]: #aggregate-dp-functions

### `BIT_AND`

```sql
BIT_AND(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise AND operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x;

/*---------*
 | bit_and |
 +---------+
 | 1       |
 *---------*/
```

### `BIT_OR`

```sql
BIT_OR(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise OR operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x;

/*--------*
 | bit_or |
 +--------+
 | 61601  |
 *--------*/
```

### `BIT_XOR`

```sql
BIT_XOR(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise XOR operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 4860    |
 *---------*/
```

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 5678    |
 *---------*/
```

```sql
SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 4860    |
 *---------*/
```

### `COUNT`

1.

```sql
COUNT(*)
[OVER over_clause]
```

2.

```sql
COUNT(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function with DISTINCT supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

`COUNT` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

**Supported Argument Types**

`expression` can be any data type. If
`DISTINCT` is present, `expression` can only be a data type that is
[groupable][agg-data-type-properties].

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

/*------------+--------------*
 | count_star | count_dist_x |
 +------------+--------------+
 | 4          | 3            |
 *------------+--------------*/
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

/*------+------------+--------------*
 | x    | count_star | count_dist_x |
 +------+------------+--------------+
 | 1    | 3          | 2            |
 | 4    | 3          | 2            |
 | 4    | 3          | 2            |
 | 5    | 1          | 1            |
 *------+------------+--------------*/
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(x) OVER (PARTITION BY MOD(x, 3)) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

/*------+------------+---------*
 | x    | count_star | count_x |
 +------+------------+---------+
 | NULL | 1          | 0       |
 | 1    | 3          | 3       |
 | 4    | 3          | 3       |
 | 4    | 3          | 3       |
 | 5    | 1          | 1       |
 *------+------------+---------*/
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

/*-------------------*
 | distinct_positive |
 +-------------------+
 | 3                 |
 *-------------------*/
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

/*------------------------------*
 | distinct_dates_with_failures |
 +------------------------------+
 | 2                            |
 *------------------------------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

[dp-functions]: #aggregate-dp-functions

### `COUNTIF`

```sql
COUNTIF(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

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

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

BOOL

**Return Data Types**

INT64

**Examples**

```sql
SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive
FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x;

/*--------------+--------------*
 | num_negative | num_positive |
 +--------------+--------------+
 | 3            | 4            |
 *--------------+--------------*/
```

```sql
SELECT
  x,
  COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS num_negative
FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x;

/*------+--------------*
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
 *------+--------------*/
```

### `LOGICAL_AND`

```sql
LOGICAL_AND(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the logical AND of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`BOOL`

**Return Data Types**

`BOOL`

**Examples**

`LOGICAL_AND` returns `FALSE` because not all of the values in the array are
less than 3.

```sql
SELECT LOGICAL_AND(x < 3) AS logical_and FROM UNNEST([1, 2, 4]) AS x;

/*-------------*
 | logical_and |
 +-------------+
 | FALSE       |
 *-------------*/
```

### `LOGICAL_OR`

```sql
LOGICAL_OR(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the logical OR of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`BOOL`

**Return Data Types**

`BOOL`

**Examples**

`LOGICAL_OR` returns `TRUE` because at least one of the values in the array is
less than 3.

```sql
SELECT LOGICAL_OR(x < 3) AS logical_or FROM UNNEST([1, 2, 4]) AS x;

/*------------*
 | logical_or |
 +------------+
 | TRUE       |
 *------------*/
```

### `MAX`

```sql
MAX(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the maximum non-`NULL` value in an aggregated group.

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties] except for `ARRAY`.

**Return Data Types**

The data type of the input values.

**Examples**

```sql
SELECT MAX(x) AS max
FROM UNNEST([8, 37, 55, 4]) AS x;

/*-----*
 | max |
 +-----+
 | 55  |
 *-----*/
```

```sql
SELECT x, MAX(x) OVER (PARTITION BY MOD(x, 2)) AS max
FROM UNNEST([8, NULL, 37, 55, NULL, 4]) AS x;

/*------+------*
 | x    | max  |
 +------+------+
 | NULL | NULL |
 | NULL | NULL |
 | 8    | 8    |
 | 4    | 8    |
 | 37   | 55   |
 | 55   | 55   |
 *------+------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `MIN`

```sql
MIN(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the minimum non-`NULL` value in an aggregated group.

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties] except for `ARRAY`.

**Return Data Types**

The data type of the input values.

**Examples**

```sql
SELECT MIN(x) AS min
FROM UNNEST([8, 37, 4, 55]) AS x;

/*-----*
 | min |
 +-----+
 | 4   |
 *-----*/
```

```sql
SELECT x, MIN(x) OVER (PARTITION BY MOD(x, 2)) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

/*------+------*
 | x    | min  |
 +------+------+
 | NULL | NULL |
 | NULL | NULL |
 | 8    | 4    |
 | 4    | 4    |
 | 37   | 37   |
 | 55   | 37   |
 *------+------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `STRING_AGG`

```sql
STRING_AGG(
  [ DISTINCT ]
  expression [, delimiter]
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns a value (either `STRING` or `BYTES`) obtained by concatenating
non-`NULL` values. Returns `NULL` if there are zero input rows or `expression`
evaluates to `NULL` for all rows.

If a `delimiter` is specified, concatenated values are separated by that
delimiter; otherwise, a comma is used as a delimiter.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

Either `STRING` or `BYTES`.

**Return Data Types**

Either `STRING` or `BYTES`.

**Examples**

```sql
SELECT STRING_AGG(fruit) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

/*------------------------*
 | string_agg             |
 +------------------------+
 | apple,pear,banana,pear |
 *------------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*------------------------------*
 | string_agg                   |
 +------------------------------+
 | apple & pear & banana & pear |
 *------------------------------*/
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*-----------------------*
 | string_agg            |
 +-----------------------+
 | apple & pear & banana |
 *-----------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*------------------------------*
 | string_agg                   |
 +------------------------------+
 | pear & pear & apple & banana |
 *------------------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*--------------*
 | string_agg   |
 +--------------+
 | apple & pear |
 *--------------*/
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*---------------*
 | string_agg    |
 +---------------+
 | pear & banana |
 *---------------*/
```

```sql
SELECT
  fruit,
  STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

/*--------+------------------------------*
 | fruit  | string_agg                   |
 +--------+------------------------------+
 | NULL   | NULL                         |
 | pear   | pear & pear                  |
 | pear   | pear & pear                  |
 | apple  | pear & pear & apple          |
 | banana | pear & pear & apple & banana |
 *--------+------------------------------*/
```

### `SUM`

```sql
SUM(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the sum of non-`NULL` values in an aggregated group.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`SUM` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.
+ If the argument is `[+|-]Infinity` for any row in the group, returns either
  `[+|-]Infinity` or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

+ Any supported numeric data type
+ `INTERVAL`

**Return Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*-----*
 | sum |
 +-----+
 | 25  |
 *-----*/
```

```sql
SELECT SUM(DISTINCT x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*-----*
 | sum |
 +-----+
 | 15  |
 *-----*/
```

```sql
SELECT
  x,
  SUM(x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*---+-----*
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
 *---+-----*/
```

```sql
SELECT
  x,
  SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*---+-----*
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
 *---+-----*/
```

```sql
SELECT SUM(x) AS sum
FROM UNNEST([]) AS x;

/*------*
 | sum  |
 +------+
 | NULL |
 *------*/
```

[dp-functions]: #aggregate-dp-functions

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

## Statistical aggregate functions

ZetaSQL supports statistical aggregate functions.
To learn about the syntax for aggregate function calls, see
[Aggregate function calls][agg-function-calls].

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#corr"><code>CORR</code></a>

</td>
  <td>
    Returns the Pearson coefficient of correlation of a set of number pairs.
  </td>
</tr>

<tr>
  <td><a href="#covar_pop"><code>COVAR_POP</code></a>

</td>
  <td>
    Returns the population covariance of a set of number pairs.
  </td>
</tr>

<tr>
  <td><a href="#covar_samp"><code>COVAR_SAMP</code></a>

</td>
  <td>
    Returns the sample covariance of a set of number pairs.
  </td>
</tr>

<tr>
  <td><a href="#stddev_pop"><code>STDDEV_POP</code></a>

</td>
  <td>
    Returns the population (biased) standard deviation of the values.
  </td>
</tr>

<tr>
  <td><a href="#stddev_samp"><code>STDDEV_SAMP</code></a>

</td>
  <td>
    Returns the sample (unbiased) standard deviation of the values.
  </td>
</tr>

<tr>
  <td><a href="#stddev"><code>STDDEV</code></a>

</td>
  <td>
    An alias of the <code>STDDEV_SAMP</code> function.
  </td>
</tr>

<tr>
  <td><a href="#var_pop"><code>VAR_POP</code></a>

</td>
  <td>
    Returns the population (biased) variance of the values.
  </td>
</tr>

<tr>
  <td><a href="#var_samp"><code>VAR_SAMP</code></a>

</td>
  <td>
    Returns the sample (unbiased) variance of the values.
  </td>
</tr>

<tr>
  <td><a href="#variance"><code>VARIANCE</code></a>

</td>
  <td>
    An alias of <code>VAR_SAMP</code>.
  </td>
</tr>

  </tbody>
</table>

### `CORR`

```sql
CORR(
  X1, X2
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the [Pearson coefficient][stat-agg-link-to-pearson-coefficient]
of correlation of a set of number pairs. For each number pair, the first number
is the dependent variable and the second number is the independent variable.
The return result is between `-1` and `1`. A result of `0` indicates no
correlation.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more `NULL` values. If
there are fewer than two input pairs without `NULL` values, this function
returns `NULL`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.
+ The variance of `X1` or `X2` is `0`.
+ The covariance of `X1` and `X2` is `0`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT CORR(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 5.0 AS x),
      (3.0, 9.0),
      (4.0, 7.0)]);

/*--------------------*
 | results            |
 +--------------------+
 | 0.6546536707079772 |
 *--------------------*/
```

```sql
SELECT CORR(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 5.0 AS x),
      (3.0, 9.0),
      (4.0, NULL)]);

/*---------*
 | results |
 +---------+
 | 1       |
 *---------*/
```

```sql
SELECT CORR(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, 3.0)])

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT CORR(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, NULL)])

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT CORR(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 5.0 AS x),
      (3.0, 9.0),
      (4.0, 7.0),
      (5.0, 1.0),
      (7.0, CAST('Infinity' as DOUBLE))])

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

```sql
SELECT CORR(x, y) AS results
FROM
  (
    SELECT 0 AS x, 0 AS y
    UNION ALL
    SELECT 0 AS x, 0 AS y
  )

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

[stat-agg-link-to-pearson-coefficient]: https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient

### `COVAR_POP`

```sql
COVAR_POP(
  X1, X2
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the population [covariance][stat-agg-link-to-covariance] of
a set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more `NULL` values. If
there is no input pair without `NULL` values, this function returns `NULL`.
If there is exactly one input pair without `NULL` values, this function returns
`0`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT COVAR_POP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (9.0, 3.0)])

/*---------------------*
 | results             |
 +---------------------+
 | -1.6800000000000002 |
 *---------------------*/
```

```sql
SELECT COVAR_POP(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, 3.0)])

/*---------*
 | results |
 +---------+
 | 0       |
 *---------*/
```

```sql
SELECT COVAR_POP(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, NULL)])

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT COVAR_POP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (NULL, 3.0)])

/*---------*
 | results |
 +---------+
 | -1      |
 *---------*/
```

```sql
SELECT COVAR_POP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (CAST('Infinity' as DOUBLE), 3.0)])

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

[stat-agg-link-to-covariance]: https://en.wikipedia.org/wiki/Covariance

### `COVAR_SAMP`

```sql
COVAR_SAMP(
  X1, X2
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the sample [covariance][stat-agg-link-to-covariance] of a
set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more `NULL` values. If
there are fewer than two input pairs without `NULL` values, this function
returns `NULL`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT COVAR_SAMP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (9.0, 3.0)])

/*---------*
 | results |
 +---------+
 | -2.1    |
 *---------*/
```

```sql
SELECT COVAR_SAMP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (NULL, 3.0)])

/*----------------------*
 | results              |
 +----------------------+
 | --1.3333333333333333 |
 *----------------------*/
```

```sql
SELECT COVAR_SAMP(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, 3.0)])

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT COVAR_SAMP(y, x) AS results
FROM UNNEST([STRUCT(1.0 AS y, NULL AS x),(9.0, NULL)])

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT COVAR_SAMP(y, x) AS results
FROM
  UNNEST(
    [
      STRUCT(1.0 AS y, 1.0 AS x),
      (2.0, 6.0),
      (9.0, 3.0),
      (2.0, 6.0),
      (CAST('Infinity' as DOUBLE), 3.0)])

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

[stat-agg-link-to-covariance]: https://en.wikipedia.org/wiki/Covariance

### `STDDEV_POP`

```sql
STDDEV_POP(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the population (biased) standard deviation of the values. The return
result is between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any `NULL` inputs. If all inputs are ignored, this
function returns `NULL`. If this function receives a single non-`NULL` input,
it returns `0`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`STDDEV_POP` can be used with differential privacy. To learn more, see
[Differentially private aggregate functions][dp-functions].

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT STDDEV_POP(x) AS results FROM UNNEST([10, 14, 18]) AS x

/*-------------------*
 | results           |
 +-------------------+
 | 3.265986323710904 |
 *-------------------*/
```

```sql
SELECT STDDEV_POP(x) AS results FROM UNNEST([10, 14, NULL]) AS x

/*---------*
 | results |
 +---------+
 | 2       |
 *---------*/
```

```sql
SELECT STDDEV_POP(x) AS results FROM UNNEST([10, NULL]) AS x

/*---------*
 | results |
 +---------+
 | 0       |
 *---------*/
```

```sql
SELECT STDDEV_POP(x) AS results FROM UNNEST([NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT STDDEV_POP(x) AS results FROM UNNEST([10, 14, CAST('Infinity' as DOUBLE)]) AS x

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

[dp-functions]: #aggregate-dp-functions

### `STDDEV_SAMP`

```sql
STDDEV_SAMP(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the sample (unbiased) standard deviation of the values. The return
result is between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any `NULL` inputs. If there are fewer than two non-`NULL`
inputs, this function returns `NULL`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT STDDEV_SAMP(x) AS results FROM UNNEST([10, 14, 18]) AS x

/*---------*
 | results |
 +---------+
 | 4       |
 *---------*/
```

```sql
SELECT STDDEV_SAMP(x) AS results FROM UNNEST([10, 14, NULL]) AS x

/*--------------------*
 | results            |
 +--------------------+
 | 2.8284271247461903 |
 *--------------------*/
```

```sql
SELECT STDDEV_SAMP(x) AS results FROM UNNEST([10, NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT STDDEV_SAMP(x) AS results FROM UNNEST([NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT STDDEV_SAMP(x) AS results FROM UNNEST([10, 14, CAST('Infinity' as DOUBLE)]) AS x

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

### `STDDEV`

```sql
STDDEV(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

An alias of [STDDEV_SAMP][stat-agg-link-to-stddev-samp].

[stat-agg-link-to-stddev-samp]: #stddev_samp

### `VAR_POP`

```sql
VAR_POP(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the population (biased) variance of the values. The return result is
between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any `NULL` inputs. If all inputs are ignored, this
function returns `NULL`. If this function receives a single non-`NULL` input,
it returns `0`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`VAR_POP` can be used with differential privacy. To learn more, see
[Differentially private aggregate functions][dp-functions].

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT VAR_POP(x) AS results FROM UNNEST([10, 14, 18]) AS x

/*--------------------*
 | results            |
 +--------------------+
 | 10.666666666666666 |
 *--------------------*/
```

```sql
SELECT VAR_POP(x) AS results FROM UNNEST([10, 14, NULL]) AS x

/*----------*
 | results |
 +---------+
 | 4       |
 *---------*/
```

```sql
SELECT VAR_POP(x) AS results FROM UNNEST([10, NULL]) AS x

/*----------*
 | results |
 +---------+
 | 0       |
 *---------*/
```

```sql
SELECT VAR_POP(x) AS results FROM UNNEST([NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT VAR_POP(x) AS results FROM UNNEST([10, 14, CAST('Infinity' as DOUBLE)]) AS x

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

[dp-functions]: #aggregate-dp-functions

### `VAR_SAMP`

```sql
VAR_SAMP(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the sample (unbiased) variance of the values. The return result is
between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any `NULL` inputs. If there are fewer than two non-`NULL`
inputs, this function returns `NULL`.

`NaN` is produced if:

+ Any input value is `NaN`
+ Any input value is positive infinity or negative infinity.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

**Examples**

```sql
SELECT VAR_SAMP(x) AS results FROM UNNEST([10, 14, 18]) AS x

/*---------*
 | results |
 +---------+
 | 16      |
 *---------*/
```

```sql
SELECT VAR_SAMP(x) AS results FROM UNNEST([10, 14, NULL]) AS x

/*---------*
 | results |
 +---------+
 | 8       |
 *---------*/
```

```sql
SELECT VAR_SAMP(x) AS results FROM UNNEST([10, NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT VAR_SAMP(x) AS results FROM UNNEST([NULL]) AS x

/*---------*
 | results |
 +---------+
 | NULL    |
 *---------*/
```

```sql
SELECT VAR_SAMP(x) AS results FROM UNNEST([10, 14, CAST('Infinity' as DOUBLE)]) AS x

/*---------*
 | results |
 +---------+
 | NaN     |
 *---------*/
```

### `VARIANCE`

```sql
VARIANCE(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

An alias of [VAR_SAMP][stat-agg-link-to-var-samp].

[stat-agg-link-to-var-samp]: #var_samp

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

## Differentially private aggregate functions 
<a id="aggregate-dp-functions"></a>

ZetaSQL supports differentially private aggregate functions.
For an explanation of how aggregate functions work, see
[Aggregate function calls][agg-function-calls].

You can only use differentially private aggregate functions with
[differentially private queries][dp-guide] in a
[differential privacy clause][dp-syntax].

Note: In this topic, the privacy parameters in the examples are not
recommendations. You should work with your privacy or security officer to
determine the optimal privacy parameters for your dataset and organization.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#anon_avg"><code>ANON_AVG</code></a>

</td>
  <td>
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_count"><code>ANON_COUNT</code></a>

</td>
  <td>
    Signature 1: Gets the differentially-private count of rows in a query
    with an <code>ANONYMIZATION</code> clause.
    <br/>
    <br/>
    Signature 2: Gets the differentially-private count of rows with a
    non-<code>NULL</code> expression in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_percentile_cont"><code>ANON_PERCENTILE_CONT</code></a>

</td>
  <td>
    Computes a differentially-private percentile across privacy unit columns
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_quantiles"><code>ANON_QUANTILES</code></a>

</td>
  <td>
    Produces an array of differentially-private quantile boundaries
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_stddev_pop"><code>ANON_STDDEV_POP</code></a>

</td>
  <td>
    Computes a differentially-private population (biased) standard deviation of
    values in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_sum"><code>ANON_SUM</code></a>

</td>
  <td>
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_var_pop"><code>ANON_VAR_POP</code></a>

</td>
  <td>
    Computes a differentially-private population (biased) variance of values
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_avg"><code>AVG</code> (differential privacy)</a>

</td>
  <td>
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_count"><code>COUNT</code> (differential privacy)</a>

</td>
  <td>
    Signature 1: Gets the differentially-private count of rows in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
    <br/>
    <br/>
    Signature 2: Gets the differentially-private count of rows with a
    non-<code>NULL</code> expression in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_percentile_cont"><code>PERCENTILE_CONT</code> (differential privacy)</a>

</td>
  <td>
    Computes a differentially-private percentile across privacy unit columns
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_sum"><code>SUM</code> (differential privacy)</a>

</td>
  <td>
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_var_pop"><code>VAR_POP</code> (differential privacy)</a>

</td>
  <td>
    Computes the differentially-private population (biased) variance of values
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

  </tbody>
</table>

### `ANON_AVG` 
<a id="anon_avg"></a>

```sql
WITH ANONYMIZATION ...
  ANON_AVG(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per privacy unit column averages.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a view called [`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 *----------+------------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamp-between]: #dp_clamp_between

### `ANON_COUNT` 
<a id="anon_count"></a>

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```sql
WITH ANONYMIZATION ...
  ANON_COUNT(*)
```

**Description**

Returns the number of rows in the
[differentially private][dp-from-clause] `FROM` clause. The final result
is an aggregation across privacy unit columns.
[Input values are clamped implicitly][dp-clamp-implicit]. Clamping is
performed per privacy unit column.

This function must be used with the `ANONYMIZATION` clause.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

#### Signature 2 
<a id="anon_count_signature2"></a>

```sql
WITH ANONYMIZATION ...
  ANON_COUNT(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-clamp-implicit]: #dp_implicit_clamping

[dp-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#dp_from_rules

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamp-between]: #dp_clamp_between

### `ANON_PERCENTILE_CONT` 
<a id="anon_percentile_cont"></a>

```sql
WITH ANONYMIZATION ...
  ANON_PERCENTILE_CONT(expression, percentile [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `percentile`: The percentile to compute. The percentile must be a literal in
  the range [0, 1]
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_PERCENTILE_CONT(quantity, 0.5 CLAMPED BETWEEN 0 AND 100) percentile_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+----------------------*
 | item     | percentile_requested |
 +----------+----------------------+
 | pencil   | 72.00011444091797    |
 | scissors | 8.000175476074219    |
 | pen      | 23.001075744628906   |
 *----------+----------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_QUANTILES` 
<a id="anon_quantiles"></a>

```sql
WITH ANONYMIZATION ...
  ANON_QUANTILES(expression, number CLAMPED BETWEEN lower_bound AND upper_bound)
```

**Description**

Returns an array of differentially private quantile boundaries for values in
`expression`. The first element in the return value is the
minimum quantile boundary and the last element is the maximum quantile boundary.
The returned results are aggregations across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `number`: The number of quantiles to create. This must be an `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`ARRAY`<`DOUBLE`>

**Examples**

The following differentially private query gets the five quantile boundaries of
the four quartiles of the number of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_QUANTILES(quantity, 4 CLAMPED BETWEEN 0 AND 100) quantiles_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+----------------------------------------------------------------------*
 | item     | quantiles_requested                                                  |
 +----------+----------------------------------------------------------------------+
 | pen      | [6.409375,20.647684733072918,41.40625,67.30848524305556,99.80078125] |
 | pencil   | [6.849259,44.010416666666664,62.64204,65.83806818181819,98.59375]    |
 *----------+----------------------------------------------------------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_STDDEV_POP` 
<a id="anon_stddev_pop"></a>

```sql
WITH ANONYMIZATION ...
  ANON_STDDEV_POP(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes the population (biased) standard deviation of
the values in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) standard deviation of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_STDDEV_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_standard_deviation
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------------*
 | item     | pop_standard_deviation |
 +----------+------------------------+
 | pencil   | 25.350871122442054     |
 | scissors | 50                     |
 | pen      | 2                      |
 *----------+------------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_SUM` 
<a id="anon_sum"></a>

```sql
WITH ANONYMIZATION ...
  ANON_SUM(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per privacy unit column.

**Return type**

One of the following [supertypes][dp-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity CLAMPED BETWEEN 0 AND 100) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------*
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 *----------+-----------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+----------*
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 *----------+----------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamp-between]: #dp_clamp_between

### `ANON_VAR_POP` 
<a id="anon_var_pop"></a>

```sql
WITH ANONYMIZATION ...
  ANON_VAR_POP(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][dp-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual entity values.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations might not
be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_VAR_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_variance
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 *----------+-----------------*/
```

[dp-clamp-explicit]: #dp_explicit_clamping

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `AVG` (differential privacy) 
<a id="dp_avg"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  AVG(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a table called [`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 *----------+------------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity) average_quantity
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `COUNT` (differential privacy) 
<a id="dp_count"></a>

+ [Signature 1](#dp_count_signature1): Returns the number of rows in a
  differentially private `FROM` clause.
+ [Signature 2](#dp_count_signature2): Returns the number of non-`NULL`
  values in an expression.

#### Signature 1 
<a id="dp_count_signature1"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(* [, contribution_bounds_per_group => (lower_bound, upper_bound)]))
```

**Description**

Returns the number of rows in the
[differentially private][dp-from-clause] `FROM` clause. The final result
is an aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following argument:

+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a table called
[`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### Signature 2 
<a id="dp_count_signature2"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This expression can be any
  numeric input type, such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a table called
[`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-clamp-implicit]: #dp_implicit_clamping

[dp-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#dp_from

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

[dp-clamped-named]: #dp_clamped_named

### `PERCENTILE_CONT` (differential privacy) 
<a id="dp_percentile_cont"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  PERCENTILE_CONT(expression, percentile, contribution_bounds_per_row => (lower_bound, upper_bound))
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across privacy unit columns.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL` values are always ignored.
+ `percentile`: The percentile to compute. The percentile must be a literal in
  the range `[0, 1]`.
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on the privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    PERCENTILE_CONT(quantity, 0.5, contribution_bounds_per_row => (0,100)) percentile_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
 /*----------+----------------------*
  | item     | percentile_requested |
  +----------+----------------------+
  | pencil   | 72.00011444091797    |
  | scissors | 8.000175476074219    |
  | pen      | 23.001075744628906   |
  *----------+----------------------*/
```

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `SUM` (differential privacy) 
<a id="dp_sum"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  SUM(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across privacy unit columns.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL` values are always ignored.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

One of the following [supertypes][dp-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    SUM(quantity, contribution_bounds_per_group => (0,100)) quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------*
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 *----------+-----------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    SUM(quantity) quantity
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+----------*
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 *----------+----------*/
```

Note: For more information about when and when not to use
noise, see [Use differential privacy][dp-noise].

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `VAR_POP` (differential privacy) 
<a id="dp_var_pop"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  VAR_POP(expression[, contribution_bounds_per_row => (lower_bound, upper_bound)])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][dp-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual user values.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on individual user values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations may not
be included. This query references a view called
[`professors`][dp-example-tables].

```sql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    VAR_POP(quantity, contribution_bounds_per_row => (0,100)) pop_variance
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
/*----------+-----------------*
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 *----------+-----------------*/
```

[dp-clamp-explicit]: #dp_explicit_clamping

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

### Clamp values in a differentially private aggregate function 
<a id="dp_clamping"></a>

In [differentially private queries][dp-syntax],
aggregation clamping is used to limit the contribution of outliers. You can
clamp explicitly or implicitly as follows:

+ [Clamp explicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named].
+ [Clamp implicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named-imp].
+ [Clamp explicitly in the `ANONYMIZATION` clause][dp-clamp-between].
+ [Clamp implicitly in the `ANONYMIZATION` clause][dp-clamp-between-imp].

To learn more about explicit and implicit clamping, see the following:

+ [About implicit clamping][dp-imp-clamp].
+ [About explicit clamping][dp-exp-clamp].

#### Implicitly clamp values in the `DIFFERENTIAL_PRIVACY` clause 
<a id="dp_clamped_named_implicit"></a>

If you don't include the contribution bounds named argument with the
`DIFFERENTIAL_PRIVACY` clause, clamping is [implicit][dp-imp-clamp], which
means bounds are derived from the data itself in a differentially private way.

Implicit bounding works best when computed using large datasets. For more
information, see [Implicit bounding limitations for small datasets][implicit-limits].

**Example**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity) average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 *----------+------------------*/
```

#### Explicitly clamp values in the `DIFFERENTIAL_PRIVACY` clause 
<a id="dp_clamped_named"></a>

```sql
contribution_bounds_per_group => (lower_bound,upper_bound)
```

```sql
contribution_bounds_per_row => (lower_bound,upper_bound)
```

Use the contribution bounds named argument to [explicitly clamp][dp-exp-clamp]
values per group or per row between a lower and upper bound in a
`DIFFERENTIAL_PRIVACY` clause.

Input values:

+ `contribution_bounds_per_row`: Contributions per privacy unit are clamped
  on a per-row (per-record) basis. This means the following:
  +  Upper and lower bounds are applied to column values in individual
    rows produced by the input subquery independently.
  +  The maximum possible contribution per privacy unit (and per grouping set)
    is the product of the per-row contribution limit and `max_groups_contributed`
    differential privacy parameter.
+ `contribution_bounds_per_group`: Contributions per privacy unit are clamped
  on a unique set of entity-specified `GROUP BY` keys. The upper and lower
  bounds are applied to values per group after the values are aggregated per
  privacy unit.
+ `lower_bound`: Numeric literal that represents the smallest value to
  include in an aggregation.
+ `upper_bound`: Numeric literal that represents the largest value to
  include in an aggregation.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.

**Examples**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(0,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```sql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(50,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### Implicitly clamp values in the `ANONYMIZATION` clause 
<a id="dp_clamp_between_implicit"></a>

If you don't include the `CLAMPED BETWEEN` clause with the
`ANONYMIZATION` clause, clamping is [implicit][dp-imp-clamp], which means bounds
are derived from the data itself in a differentially private way.

Implicit bounding works best when computed using large datasets. For more
information, see [Implicit bounding limitations for small datasets][implicit-limits].

**Example**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 *----------+------------------*/
```

#### Explicitly clamp values in the `ANONYMIZATION` clause 
<a id="dp_clamp_between"></a>

```sql
CLAMPED BETWEEN lower_bound AND upper_bound
```

Use the `CLAMPED BETWEEN` clause to [explicitly clamp][dp-exp-clamp] values
between a lower and an upper bound in an `ANONYMIZATION` clause.

Input values:

+ `lower_bound`: Numeric literal that represents the smallest value to
  include in an aggregation.
+ `upper_bound`: Numeric literal that represents the largest value to
  include in an aggregation.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.

Note: This is a legacy feature. If possible, use the `contribution_bounds`
named argument instead.

**Examples**

The following differentially private query clamps each aggregate contribution
for each privacy unit column and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results will be
accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```sql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  ANON_AVG(quantity CLAMPED BETWEEN 50 AND 100) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### About explicit clamping 
<a id="dp_explicit_clamping"></a>

In differentially private aggregate functions, clamping explicitly clamps the
total contribution from each privacy unit column to within a specified
range.

Explicit bounds are uniformly applied to all aggregations.  So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them.  On the other hand, when
[implicit bounds][dp-imp-clamp] are inferred from the data, the bounds applied
to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because the age of most people generally falls within this range.

Important: The results of the query reveal the explicit bounds. Do not use
explicit bounds based on the entity data; explicit bounds should be based on
public information.

#### About implicit clamping 
<a id="dp_implicit_clamping"></a>

In differentially private aggregate functions, explicit clamping is optional.
If you don't include this clause, clamping is implicit,
which means bounds are derived from the data itself in a differentially
private way. The process is somewhat random, so aggregations with identical
ranges can have different bounds.

Implicit bounds are determined for each aggregation. So if some
aggregations have a wide range of values, and others have a narrow range of
values, implicit bounding can identify different bounds for different
aggregations as appropriate. Implicit bounds might be an advantage or a
disadvantage depending on your use case. Different bounds for different
aggregations can result in lower error. Different bounds also means that
different aggregations have different levels of uncertainty, which might not be
directly comparable. [Explicit bounds][dp-exp-clamp], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

[dp-guide]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

[dp-exp-clamp]: #dp_explicit_clamping

[dp-imp-clamp]: #dp_implicit_clamping

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[implicit-limits]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#implicit_limits

[dp-clamp-between]: #dp_clamp_between

[dp-clamp-between-imp]: #dp_clamp_between_implicit

[dp-clamped-named]: #dp_clamped_named

[dp-clamped-named-imp]: #dp_clamped_named_implicit

## Approximate aggregate functions

ZetaSQL supports approximate aggregate functions.
To learn about the syntax for aggregate function calls, see
[Aggregate function calls][agg-function-calls].

Approximate aggregate functions are scalable in terms of memory usage and time,
but produce approximate results instead of exact results. These functions
typically require less memory than [exact aggregation functions][aggregate-functions-reference]
like `COUNT(DISTINCT ...)`, but also introduce statistical uncertainty.
This makes approximate aggregation appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

The approximate aggregate functions in this section work directly on the
input data, rather than an intermediate estimation of the data. These functions
_do not allow_ users to specify the precision for the estimation with
sketches. If you would like to specify precision with sketches, see:

+  [HyperLogLog++ functions][hll-functions] to estimate cardinality.
+  [KLL functions][kll-functions] to estimate quantile values.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#approx_count_distinct"><code>APPROX_COUNT_DISTINCT</code></a>

</td>
  <td>
    Returns the approximate result for <code>COUNT(DISTINCT expression)</code>.
  </td>
</tr>

<tr>
  <td><a href="#approx_quantiles"><code>APPROX_QUANTILES</code></a>

</td>
  <td>
    Returns the approximate quantile boundaries.
  </td>
</tr>

<tr>
  <td><a href="#approx_top_count"><code>APPROX_TOP_COUNT</code></a>

</td>
  <td>
    Returns the approximate top elements and their approximate count.
  </td>
</tr>

<tr>
  <td><a href="#approx_top_sum"><code>APPROX_TOP_SUM</code></a>

</td>
  <td>
    Returns the approximate top elements and sum, based on the approximate sum
    of an assigned weight.
  </td>
</tr>

  </tbody>
</table>

### `APPROX_COUNT_DISTINCT`

```sql
APPROX_COUNT_DISTINCT(
  expression
)
```

**Description**

Returns the approximate result for `COUNT(DISTINCT expression)`. The value
returned is a statistical estimate, not necessarily the actual value.

This function is less accurate than `COUNT(DISTINCT expression)`, but performs
better on huge input.

**Supported Argument Types**

Any data type **except**:

+ `ARRAY`
+ `STRUCT`
+ `PROTO`

**Returned Data Types**

`INT64`

**Examples**

```sql
SELECT APPROX_COUNT_DISTINCT(x) as approx_distinct
FROM UNNEST([0, 1, 1, 2, 3, 5]) as x;

/*-----------------*
 | approx_distinct |
 +-----------------+
 | 5               |
 *-----------------*/
```

### `APPROX_QUANTILES`

```sql
APPROX_QUANTILES(
  [ DISTINCT ]
  expression, number
  [ { IGNORE | RESPECT } NULLS ]
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate boundaries for a group of `expression` values, where
`number` represents the number of quantiles to create. This function returns
an array of `number` + 1 elements, where the first element is the approximate
minimum and the last element is the approximate maximum.

Returns `NULL` if there are zero input rows or `expression` evaluates to
`NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any supported data type **except**:

  + `ARRAY`
  + `STRUCT`
  + `PROTO`
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<T>` where `T` is the type specified by `expression`.

**Examples**

```sql
SELECT APPROX_QUANTILES(x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

/*------------------*
 | approx_quantiles |
 +------------------+
 | [1, 5, 10]       |
 *------------------*/
```

```sql
SELECT APPROX_QUANTILES(x, 100)[OFFSET(90)] AS percentile_90
FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS x;

/*---------------*
 | percentile_90 |
 +---------------+
 | 9             |
 *---------------*/
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

/*------------------*
 | approx_quantiles |
 +------------------+
 | [1, 6, 10]       |
 *------------------*/
```

```sql
SELECT APPROX_QUANTILES(x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

/*------------------*
 | approx_quantiles |
 +------------------+
 | [NULL, 4, 10]    |
 *------------------*/
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

/*------------------*
 | approx_quantiles |
 +------------------+
 | [NULL, 6, 10]    |
 *------------------*/
```

### `APPROX_TOP_COUNT`

```sql
APPROX_TOP_COUNT(
  expression, number
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate top elements of `expression` as an array of `STRUCT`s.
The `number` parameter specifies the number of elements returned.

Each `STRUCT` contains two fields. The first field (named `value`) contains an
input value. The second field (named `count`) contains an `INT64` specifying the
number of times the value was returned.

Returns `NULL` if there are zero input rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any data type that the `GROUP BY` clause supports.
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<STRUCT>`

**Examples**

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST(["apple", "apple", "pear", "pear", "pear", "banana"]) as x;

/*-------------------------*
 | approx_top_count        |
 +-------------------------+
 | [{pear, 3}, {apple, 2}] |
 *-------------------------*/
```

**NULL handling**

`APPROX_TOP_COUNT` does not ignore `NULL`s in the input. For example:

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST([NULL, "pear", "pear", "pear", "apple", NULL]) as x;

/*------------------------*
 | approx_top_count       |
 +------------------------+
 | [{pear, 3}, {NULL, 2}] |
 *------------------------*/
```

### `APPROX_TOP_SUM`

```sql
APPROX_TOP_SUM(
  expression, weight, number
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Returns the approximate top elements of `expression`, based on the sum of an
assigned `weight`. The `number` parameter specifies the number of elements
returned.

If the `weight` input is negative or `NaN`, this function returns an error.

The elements are returned as an array of `STRUCT`s.
Each `STRUCT` contains two fields: `value` and `sum`.
The `value` field contains the value of the input expression. The `sum` field is
the same type as `weight`, and is the approximate sum of the input weight
associated with the `value` field.

Returns `NULL` if there are zero input rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `expression`: Any data type that the `GROUP BY` clause supports.
+ `weight`: One of the following:

  + `INT64`
  + `UINT64`
  + `NUMERIC`
  + `BIGNUMERIC`
  + `DOUBLE`
+ `number`: `INT64` literal or query parameter.

**Returned Data Types**

`ARRAY<STRUCT>`

**Examples**

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([
  STRUCT("apple" AS x, 3 AS weight),
  ("pear", 2),
  ("apple", 0),
  ("banana", 5),
  ("pear", 4)
]);

/*--------------------------*
 | approx_top_sum           |
 +--------------------------+
 | [{pear, 6}, {banana, 5}] |
 *--------------------------*/
```

**NULL handling**

`APPROX_TOP_SUM` does not ignore `NULL` values for the `expression` and `weight`
parameters.

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, NULL AS weight), ("pear", 0), ("pear", NULL)]);

/*----------------------------*
 | approx_top_sum             |
 +----------------------------+
 | [{pear, 0}, {apple, NULL}] |
 *----------------------------*/
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, 2)]);

/*-------------------------*
 | approx_top_sum          |
 +-------------------------+
 | [{NULL, 2}, {apple, 0}] |
 *-------------------------*/
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, NULL)]);

/*----------------------------*
 | approx_top_sum             |
 +----------------------------+
 | [{apple, 0}, {NULL, NULL}] |
 *----------------------------*/
```

[hll-functions]: #hyperloglog_functions

[kll-functions]: #kll_quantile_functions

[aggregate-functions-reference]: #aggregate_functions

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

## HyperLogLog++ functions 
<a id="hll_functions"></a>

The [HyperLogLog++ algorithm (HLL++)][hll-sketches] estimates
[cardinality][cardinality] from [sketches][hll-sketches].

HLL++ functions are approximate aggregate functions.
Approximate aggregation typically requires less
memory than exact aggregation functions,
like [`COUNT(DISTINCT)`][count-distinct], but also introduces statistical error.
This makes HLL++ functions appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

If you do not need materialized sketches, you can alternatively use an
[approximate aggregate function with system-defined precision][approx-functions-reference],
such as [`APPROX_COUNT_DISTINCT`][approx-count-distinct]. However,
`APPROX_COUNT_DISTINCT` does not allow partial aggregations, re-aggregations,
and custom precision.

ZetaSQL supports the following HLL++ functions:

### `HLL_COUNT.EXTRACT`

```
HLL_COUNT.EXTRACT(sketch)
```

**Description**

A scalar function that extracts a cardinality estimate of a single
[HLL++][hll-link-to-research-whitepaper] sketch.

If `sketch` is `NULL`, this function returns a cardinality estimate of `0`.

**Supported input types**

`BYTES`

**Return type**

`INT64`

**Example**

The following query returns the number of distinct users for each country who
have at least one invoice.

```sql
SELECT
  country,
  HLL_COUNT.EXTRACT(HLL_sketch) AS distinct_customers_with_open_invoice
FROM
  (
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
  );

/*---------+--------------------------------------*
 | country | distinct_customers_with_open_invoice |
 +---------+--------------------------------------+
 | UA      |                                    2 |
 | BR      |                                    1 |
 | CZ      |                                    1 |
 *---------+--------------------------------------*/
```

[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html

### `HLL_COUNT.INIT`

```
HLL_COUNT.INIT(input [, precision])
```

**Description**

An aggregate function that takes one or more `input` values and aggregates them
into a [HLL++][hll-link-to-research-whitepaper] sketch. Each sketch
is represented using the `BYTES` data type. You can then merge sketches using
`HLL_COUNT.MERGE` or `HLL_COUNT.MERGE_PARTIAL`. If no merging is needed,
you can extract the final count of distinct values from the sketch using
`HLL_COUNT.EXTRACT`.

This function supports an optional parameter, `precision`. This parameter
defines the accuracy of the estimate at the cost of additional memory required
to process the sketches or store them on disk. The range for this value is
`10` to `24`. The default value is `15`. For more information about precision,
see [Precision for sketches][precision_hll].

If the input is `NULL`, this function returns `NULL`.

For more information, see [HyperLogLog in Practice: Algorithmic Engineering of
a State of The Art Cardinality Estimation Algorithm][hll-link-to-research-whitepaper].

**Supported input types**

+ `INT64`
+ `UINT64`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`
+ `BYTES`

**Return type**

`BYTES`

**Example**

The following query creates HLL++ sketches that count the number of distinct
users with at least one invoice per country.

```sql
SELECT
  country,
  HLL_COUNT.INIT(customer_id, 10)
    AS hll_sketch
FROM
  UNNEST(
    ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
      ('UA', 'customer_id_1', 'invoice_id_11'),
      ('CZ', 'customer_id_2', 'invoice_id_22'),
      ('CZ', 'customer_id_2', 'invoice_id_23'),
      ('BR', 'customer_id_3', 'invoice_id_31'),
      ('UA', 'customer_id_2', 'invoice_id_24')])
GROUP BY country;

/*---------+------------------------------------------------------------------------------------*
 | country | hll_sketch                                                                         |
 +---------+------------------------------------------------------------------------------------+
 | UA      | "\010p\020\002\030\002 \013\202\007\r\020\002\030\n \0172\005\371\344\001\315\010" |
 | CZ      | "\010p\020\002\030\002 \013\202\007\013\020\001\030\n \0172\003\371\344\001"       |
 | BR      | "\010p\020\001\030\002 \013\202\007\013\020\001\030\n \0172\003\202\341\001"       |
 *---------+------------------------------------------------------------------------------------*/
```

[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html

[precision_hll]: https://github.com/google/zetasql/blob/master/docs/sketches.md#precision_hll

### `HLL_COUNT.MERGE_PARTIAL`

```
HLL_COUNT.MERGE_PARTIAL(sketch)
```

**Description**

An aggregate function that takes one or more
[HLL++][hll-link-to-research-whitepaper] `sketch`
inputs and merges them into a new sketch.

Each `sketch` must be initialized on the same type. Attempts to merge sketches
for different types results in an error. For example, you cannot merge a sketch
initialized from `INT64` data with one initialized from `STRING` data.

If the merged sketches were initialized with different precisions, the precision
will be downgraded to the lowest precision involved in the merge. For example,
if `MERGE_PARTIAL` encounters sketches of precision 14 and 15, the returned new
sketch will have precision 14.

This function returns `NULL` if there is no input or all inputs are `NULL`.

**Supported input types**

`BYTES`

**Return type**

`BYTES`

**Example**

The following query returns an HLL++ sketch that counts the number of distinct
users who have at least one invoice across all countries.

```sql
SELECT HLL_COUNT.MERGE_PARTIAL(HLL_sketch) AS distinct_customers_with_open_invoice
FROM
  (
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
  );

/*----------------------------------------------------------------------------------------------*
 | distinct_customers_with_open_invoice                                                         |
 +----------------------------------------------------------------------------------------------+
 | "\010p\020\006\030\002 \013\202\007\020\020\003\030\017 \0242\010\320\2408\352}\244\223\002" |
 *----------------------------------------------------------------------------------------------*/
```

[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html

### `HLL_COUNT.MERGE`

```
HLL_COUNT.MERGE(sketch)
```

**Description**

An aggregate function that returns the cardinality of several
[HLL++][hll-link-to-research-whitepaper] set sketches by computing their union.

Each `sketch` must be initialized on the same type. Attempts to merge sketches
for different types results in an error. For example, you cannot merge a sketch
initialized from `INT64` data with one initialized from `STRING` data.

If the merged sketches were initialized with different precisions, the precision
will be downgraded to the lowest precision involved in the merge.

This function ignores `NULL` values when merging sketches. If the merge happens
over zero rows or only over `NULL` values, the function returns `0`.

**Supported input types**

`BYTES`

**Return type**

`INT64`

**Example**

 The following query counts the number of distinct users across all countries
 who have at least one invoice.

```sql
SELECT HLL_COUNT.MERGE(hll_sketch) AS distinct_customers_with_open_invoice
FROM
  (
    SELECT
      country,
      HLL_COUNT.INIT(customer_id) AS hll_sketch
    FROM
      UNNEST(
        ARRAY<STRUCT<country STRING, customer_id STRING, invoice_id STRING>>[
          ('UA', 'customer_id_1', 'invoice_id_11'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('CZ', 'customer_id_2', 'invoice_id_22'),
          ('CZ', 'customer_id_2', 'invoice_id_23'),
          ('BR', 'customer_id_3', 'invoice_id_31'),
          ('UA', 'customer_id_2', 'invoice_id_24')])
    GROUP BY country
  );

/*--------------------------------------*
 | distinct_customers_with_open_invoice |
 +--------------------------------------+
 |                                    3 |
 *--------------------------------------*/
```

[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html

[hll-sketches]: https://github.com/google/zetasql/blob/master/docs/sketches.md#sketches_hll

[cardinality]: https://en.wikipedia.org/wiki/Cardinality

[count-distinct]: #count

[approx-count-distinct]: #approx-count-distinct

[approx-functions-reference]: #approximate_aggregate_functions

## KLL quantile functions

ZetaSQL supports KLL functions.

The [KLL16 algorithm][kll-sketches] estimates
quantiles from [sketches][kll-sketches]. If you do not want
to work with sketches and do not need customized precision, consider
using [approximate aggregate functions][approx-functions-reference]
with system-defined precision.

KLL functions are approximate aggregate functions.
Approximate aggregation requires significantly less memory than an exact
quantiles computation, but also introduces statistical error.
This makes approximate aggregation appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

Note: While `APPROX_QUANTILES` is also returning approximate quantile results,
the functions from this section allow for partial aggregations and
re-aggregations.

### `KLL_QUANTILES.EXTRACT_INT64`

```sql
KLL_QUANTILES.EXTRACT_INT64(sketch, number)
```

**Description**

Takes a single KLL sketch as `BYTES` and returns a selected `number` of
quantiles. The output is an `ARRAY` containing the exact minimum value from
the input data that you used to initialize the sketch, each approximate
quantile, and the exact maximum value from the initial input data. This is a
scalar function, similar to `KLL_QUANTILES.MERGE_INT64`, but scalar rather than
aggregate.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL quantiles sketch.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `INT64` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<INT64>`

**Example**

The following query initializes a KLL sketch from five rows of data. Then
it returns an `ARRAY` containing the minimum, median, and maximum values in the
input sketch.

```sql
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

/*---------*
 | median  |
 +---------+
 | [1,3,5] |
 *---------*/
```

### `KLL_QUANTILES.EXTRACT_UINT64`

```sql
KLL_QUANTILES.EXTRACT_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64),
but accepts KLL sketches initialized on data of type `UINT64`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `UINT64` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<UINT64>`

### `KLL_QUANTILES.EXTRACT_DOUBLE`

```sql
KLL_QUANTILES.EXTRACT_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64),
but accepts KLL sketches initialized on data of type
`DOUBLE`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on
  `DOUBLE` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<DOUBLE>`

### `KLL_QUANTILES.EXTRACT_POINT_INT64`

```sql
KLL_QUANTILES.EXTRACT_POINT_INT64(sketch, phi)
```

**Description**

Takes a single KLL sketch as `BYTES` and returns a single quantile.
The `phi` argument specifies the quantile to return as a fraction of the total
number of rows in the input, normalized between 0 and 1. This means that the
function will return a value *v* such that approximately Φ * *n* inputs are less
than or equal to *v*, and a (1-Φ) * *n* inputs are greater than or equal to *v*.
This is a scalar function.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL quantiles sketch.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `INT64` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`INT64`

**Example**

The following query initializes a KLL sketch from five rows of data. Then
it returns the value of the eighth decile or 80th percentile of the sketch.

```sql
SELECT KLL_QUANTILES.EXTRACT_POINT_INT64(kll_sketch, .8) AS quintile
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

/*----------*
 | quintile |
 +----------+
 |      4   |
 *----------*/
```

### `KLL_QUANTILES.EXTRACT_POINT_UINT64`

```sql
KLL_QUANTILES.EXTRACT_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts KLL sketches initialized on data of type `UINT64`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `UINT64` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`UINT64`

### `KLL_QUANTILES.EXTRACT_POINT_DOUBLE`

```sql
KLL_QUANTILES.EXTRACT_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts KLL sketches initialized on data of type
`DOUBLE`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on
  `DOUBLE` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`DOUBLE`

### `KLL_QUANTILES.INIT_INT64`

```sql
KLL_QUANTILES.INIT_INT64(input[, precision[, weight => input_weight]])
```

**Description**

Takes one or more `input` values and aggregates them into a
[KLL][kll-sketches] sketch. This function represents the output sketch
using the `BYTES` data type. This is an
aggregate function.

The `precision` argument defines the exactness of the returned approximate
quantile *q*. By default, the rank of the approximate quantile in the input can
be at most ±1/1000 * *n* off from ⌈Φ * *n*⌉, where *n* is the number of rows in
the input and ⌈Φ * *n*⌉ is the rank of the exact quantile. If you provide a
value for `precision`, the rank of the approximate quantile in the input can be
at most ±1/`precision` * *n* off from the rank of the exact quantile. The error
is within this error bound in 99.999% of cases. This error guarantee only
applies to the difference between exact and approximate ranks: the numerical
difference between the exact and approximated value for a quantile can be
arbitrarily large.

By default, values in an initialized KLL sketch are weighted equally as `1`.
If you would you like to weight values differently, use the
mandatory-named argument, `weight`, which assigns weight to each input in the
resulting KLL sketch. `weight` is a multiplier. For example, if you assign a
weight of `3` to an input value, it's as if three instances of the input value
are included in the generation of the KLL sketch.

**Supported Argument Types**

+ `input`: `INT64`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

**Return Type**

KLL sketch as `BYTES`

**Examples**

The following query takes a column of type `INT64` and outputs a sketch as
`BYTES` that allows you to retrieve values whose ranks are within
±1/1000 * 5 = ±1/200 ≈ 0 ranks of their exact quantile.

```sql
SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
FROM (SELECT 1 AS x UNION ALL
      SELECT 2 AS x UNION ALL
      SELECT 3 AS x UNION ALL
      SELECT 4 AS x UNION ALL
      SELECT 5 AS x);

/*----------------------------------------------------------------------*
 | kll_sketch                                                           |
 +----------------------------------------------------------------------+
 | "\010q\020\005 \004\212\007\025\010\200                              |
 | \020\350\007\032\001\001\"\001\005*\007\n\005\001\002\003\004\005"   |
 *----------------------------------------------------------------------*/
```

The following examples illustrate how weight works when you initialize a
KLL sketch. The results are converted to quantiles.

```sql
WITH points AS (
  SELECT 1 AS x, 1 AS y UNION ALL
  SELECT 2 AS x, 1 AS y UNION ALL
  SELECT 3 AS x, 1 AS y UNION ALL
  SELECT 4 AS x, 1 AS y UNION ALL
  SELECT 5 AS x, 1 AS y)
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM
  (
    SELECT KLL_QUANTILES.INIT_INT64(x, 1000,  weight=>y) AS kll_sketch
    FROM points
  );

/*---------*
 | median  |
 +---------+
 | [1,3,5] |
 *---------*/
```

```sql
WITH points AS (
  SELECT 1 AS x, 1 AS y UNION ALL
  SELECT 2 AS x, 3 AS y UNION ALL
  SELECT 3 AS x, 1 AS y UNION ALL
  SELECT 4 AS x, 1 AS y UNION ALL
  SELECT 5 AS x, 1 AS y)
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM
  (
    SELECT KLL_QUANTILES.INIT_INT64(x, 1000,  weight=>y) AS kll_sketch
    FROM points
  );

/*---------*
 | median  |
 +---------+
 | [1,2,5] |
 *---------*/
```

### `KLL_QUANTILES.INIT_UINT64`

```sql
KLL_QUANTILES.INIT_UINT64(input[, precision[, weight => input_weight]])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64),
but accepts `input` of type `UINT64`.

**Supported Argument Types**

+ `input`: `UINT64`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

**Return Type**

KLL sketch as `BYTES`

### `KLL_QUANTILES.INIT_DOUBLE`

```sql
KLL_QUANTILES.INIT_DOUBLE(input[, precision[, weight => input_weight]])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64),
but accepts `input` of type `DOUBLE`.

`KLL_QUANTILES.INIT_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ `input`: `DOUBLE`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

**Return Type**

KLL sketch as `BYTES`

[kll-sketches]: https://github.com/google/zetasql/blob/master/docs/sketches.md#sketches_kll

[sort-order]: https://github.com/google/zetasql/blob/master/docs/data-types.md#comparison_operator_examples

### `KLL_QUANTILES.MERGE_INT64`

```sql
KLL_QUANTILES.MERGE_INT64(sketch, number)
```

**Description**

Takes KLL sketches as `BYTES` and merges them into
a new sketch, then returns the quantiles that divide the input into
`number` equal-sized groups, along with the minimum and maximum values of the
input. The output is an `ARRAY` containing the exact minimum value from
the input data that you used to initialize the sketches, each
approximate quantile, and the exact maximum value from the initial input data.
This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if the input is not a valid KLL quantiles sketch.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `INT64` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<INT64>`

**Example**

The following query initializes two KLL sketches from five rows of data each.
Then it merges these two sketches and returns an `ARRAY` containing the minimum,
median, and maximum values in the input sketches.

```sql
SELECT KLL_QUANTILES.MERGE_INT64(kll_sketch, 2) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

/*---------------*
 | merged_sketch |
 +---------------+
 | [1,5,10]      |
 *---------------*/
```

### `KLL_QUANTILES.MERGE_UINT64`

```sql
KLL_QUANTILES.MERGE_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64),
but accepts KLL sketches initialized on data of type `UINT64`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `UINT64` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<UINT64>`

### `KLL_QUANTILES.MERGE_DOUBLE`

```sql
KLL_QUANTILES.MERGE_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64),
but accepts KLL sketches initialized on data of type
`DOUBLE`.

`KLL_QUANTILES.MERGE_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on
  `DOUBLE` data type
+ `number`: `INT64`

**Return Type**

`ARRAY<DOUBLE>`

[sort-order]: https://github.com/google/zetasql/blob/master/docs/data-types.md#comparison_operator_examples

### `KLL_QUANTILES.MERGE_PARTIAL`

```sql
KLL_QUANTILES.MERGE_PARTIAL(sketch)
```

**Description**

Takes KLL sketches of the same underlying type and merges them to return a new
sketch of the same underlying type. This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if two or more sketches don't have compatible underlying types,
such as one sketch of `INT64` values and another of
`DOUBLE` values.

Returns an error if one or more inputs are not a valid KLL quantiles sketch.

Ignores `NULL` sketches. If the input contains zero rows or only `NULL`
sketches, the function returns `NULL`.

You can initialize sketches with different optional clauses and merge them. For
example, you can initialize a sketch with the `DISTINCT` clause and another
sketch without any optional clauses, and then merge these two sketches.
However, if you initialize sketches with the `DISTINCT` clause and merge them,
the resulting sketch may still contain duplicates.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch

**Return Type**

KLL sketch as `BYTES`

**Example**

The following query initializes two KLL sketches from five rows of data each.
Then it merges these two sketches into a new sketch, also as `BYTES`. Both
input sketches have the same underlying data type and precision.

```sql
SELECT KLL_QUANTILES.MERGE_PARTIAL(kll_sketch) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

/*-----------------------------------------------------------------------------*
 | merged_sketch                                                               |
 +-----------------------------------------------------------------------------+
 | "\010q\020\n \004\212\007\032\010\200 \020\350\007\032\001\001\"\001\n*     |
 | \014\n\n\001\002\003\004\005\006\007\010\t\n"                               |
 *-----------------------------------------------------------------------------*/
```

### `KLL_QUANTILES.MERGE_POINT_INT64`

```sql
KLL_QUANTILES.MERGE_POINT_INT64(sketch, phi)
```

**Description**

Takes KLL sketches as `BYTES` and merges them, then extracts a single
quantile from the merged sketch. The `phi` argument specifies the quantile
to return as a fraction of the total number of rows in the input, normalized
between 0 and 1. This means that the function will return a value *v* such that
approximately Φ * *n* inputs are less than or equal to *v*, and a (1-Φ) / *n*
inputs are greater than or equal to *v*. This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if the input is not a valid KLL quantiles sketch.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `INT64` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`INT64`

**Example**

The following query initializes two KLL sketches from five rows of data each.
Then it merges these two sketches and returns the value of the ninth decile or
90th percentile of the merged sketch.

```sql
SELECT KLL_QUANTILES.MERGE_POINT_INT64(kll_sketch, .9) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

/*---------------*
 | merged_sketch |
 +---------------+
 |             9 |
 *---------------*/
```

### `KLL_QUANTILES.MERGE_POINT_UINT64`

```sql
KLL_QUANTILES.MERGE_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64),
but accepts KLL sketches initialized on data of type `UINT64`.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on `UINT64` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`UINT64`

### `KLL_QUANTILES.MERGE_POINT_DOUBLE`

```sql
KLL_QUANTILES.MERGE_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64),
but accepts KLL sketches initialized on data of type
`DOUBLE`.

`KLL_QUANTILES.MERGE_POINT_DOUBLE` orders values according to the
ZetaSQL [floating point sort order][sort-order]. For example, `NaN`
orders before <code>&#8209;inf</code>.

**Supported Argument Types**

+ `sketch`: `BYTES` KLL sketch initialized on
  `DOUBLE` data type
+ `phi`: `DOUBLE` between 0 and 1

**Return Type**

`DOUBLE`

[sort-order]: https://github.com/google/zetasql/blob/master/docs/data-types.md#comparison_operator_examples

[kll-sketches]: https://github.com/google/zetasql/blob/master/docs/sketches.md#sketches_kll

[approx-functions-reference]: #approximate_aggregate_functions

## Numbering functions

ZetaSQL supports numbering functions.
Numbering functions are a subset of window functions. To create a
window function call and learn about the syntax for window functions,
see [Window function calls][window-function-calls].

Numbering functions assign integer values to each row based on their position
within the specified window. The `OVER` clause syntax varies across
numbering functions.

### `CUME_DIST`

```sql
CUME_DIST()
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Return the relative rank of a row defined as NP/NR. NP is defined to be the
number of rows that either precede or are peers with the current row. NR is the
number of rows in the partition.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

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

/*-----------------+------------------------+----------+-------------*
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
 *-----------------+------------------------+----------+-------------*/
```

### `DENSE_RANK`

```sql
DENSE_RANK()
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Returns the ordinal (1-based) rank of each row within the window partition.
All peer rows receive the same rank value, and the subsequent rank value is
incremented by one.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Type**

`INT64`

**Examples**

```sql
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  DENSE_RANK() OVER (ORDER BY x ASC) AS dense_rank
FROM Numbers

/*-------------------------*
 | x          | dense_rank |
 +-------------------------+
 | 1          | 1          |
 | 2          | 2          |
 | 2          | 2          |
 | 5          | 3          |
 | 8          | 4          |
 | 10         | 5          |
 | 10         | 5          |
 *-------------------------*/
```

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

/*-----------------+------------------------+----------+-------------*
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
 *-----------------+------------------------+----------+-------------*/
```

### `NTILE`

```sql
NTILE(constant_integer_expression)
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

This function divides the rows into `constant_integer_expression`
buckets based on row ordering and returns the 1-based bucket number that is
assigned to each row. The number of rows in the buckets can differ by at most 1.
The remainder values (the remainder of number of rows divided by buckets) are
distributed one for each bucket, starting with bucket 1. If
`constant_integer_expression` evaluates to NULL, 0 or negative, an
error is provided.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

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

/*-----------------+------------------------+----------+-------------*
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
 *-----------------+------------------------+----------+-------------*/
```

### `PERCENT_RANK`

```sql
PERCENT_RANK()
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
the `RANK` of the row and NR is the number of rows in the partition.
Returns 0 if NR=1.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

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

/*-----------------+------------------------+----------+---------------------*
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
 *-----------------+------------------------+----------+---------------------*/
```

### `RANK`

```sql
RANK()
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Returns the ordinal (1-based) rank of each row within the ordered partition.
All peer rows receive the same rank value. The next row or set of peer rows
receives a rank value which increments by the number of peers with the previous
rank value, instead of `DENSE_RANK`, which always increments by 1.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Type**

`INT64`

**Examples**

```sql
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  RANK() OVER (ORDER BY x ASC) AS rank
FROM Numbers

/*-------------------------*
 | x          | rank       |
 +-------------------------+
 | 1          | 1          |
 | 2          | 2          |
 | 2          | 2          |
 | 5          | 4          |
 | 8          | 5          |
 | 10         | 6          |
 | 10         | 6          |
 *-------------------------*/
```

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

/*-----------------+------------------------+----------+-------------*
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
 *-----------------+------------------------+----------+-------------*/
```

### `ROW_NUMBER`

```sql
ROW_NUMBER()
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]

```

**Description**

Does not require the `ORDER BY` clause. Returns the sequential
row ordinal (1-based) of each row for each ordered partition. If the
`ORDER BY` clause is unspecified then the result is
non-deterministic.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Return Type**

`INT64`

**Examples**

```sql
WITH Numbers AS
 (SELECT 1 as x
  UNION ALL SELECT 2
  UNION ALL SELECT 2
  UNION ALL SELECT 5
  UNION ALL SELECT 8
  UNION ALL SELECT 10
  UNION ALL SELECT 10
)
SELECT x,
  ROW_NUMBER() OVER (ORDER BY x) AS row_num
FROM Numbers

/*-------------------------*
 | x          | row_num    |
 +-------------------------+
 | 1          | 1          |
 | 2          | 2          |
 | 2          | 3          |
 | 5          | 4          |
 | 8          | 5          |
 | 10         | 6          |
 | 10         | 7          |
 *-------------------------*/
```

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

/*-----------------+------------------------+----------+-------------*
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
 *-----------------+------------------------+----------+-------------*/
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

## Bit functions

ZetaSQL supports the following bit functions.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#bit_cast_to_int32"><code>BIT_CAST_TO_INT32</code></a>

</td>
  <td>
    Cast bits to an <code>INT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#bit_cast_to_int64"><code>BIT_CAST_TO_INT64</code></a>

</td>
  <td>
    Cast bits to an <code>INT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#bit_cast_to_uint32"><code>BIT_CAST_TO_UINT32</code></a>

</td>
  <td>
    Cast bits to an <code>UINT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#bit_cast_to_uint64"><code>BIT_CAST_TO_UINT64</code></a>

</td>
  <td>
    Cast bits to an <code>UINT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#bit_count"><code>BIT_COUNT</code></a>

</td>
  <td>
    Gets the number of bits that are set in an input expression.
  </td>
</tr>

  </tbody>
</table>

### `BIT_CAST_TO_INT32`

```sql
BIT_CAST_TO_INT32(value)
```

**Description**

ZetaSQL supports bit casting to `INT32`. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

`INT32`

**Examples**

```sql
SELECT BIT_CAST_TO_UINT32(-1) as UINT32_value, BIT_CAST_TO_INT32(BIT_CAST_TO_UINT32(-1)) as bit_cast_value;

/*---------------+----------------------*
 | UINT32_value  | bit_cast_value       |
 +---------------+----------------------+
 | 4294967295    | -1                   |
 *---------------+----------------------*/
```

### `BIT_CAST_TO_INT64`

```sql
BIT_CAST_TO_INT64(value)
```

**Description**

ZetaSQL supports bit casting to `INT64`. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

`INT64`

**Example**

```sql
SELECT BIT_CAST_TO_UINT64(-1) as UINT64_value, BIT_CAST_TO_INT64(BIT_CAST_TO_UINT64(-1)) as bit_cast_value;

/*-----------------------+----------------------*
 | UINT64_value          | bit_cast_value       |
 +-----------------------+----------------------+
 | 18446744073709551615  | -1                   |
 *-----------------------+----------------------*/
```

### `BIT_CAST_TO_UINT32`

```sql
BIT_CAST_TO_UINT32(value)
```

**Description**

ZetaSQL supports bit casting to `UINT32`. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

`UINT32`

**Examples**

```sql
SELECT -1 as UINT32_value, BIT_CAST_TO_UINT32(-1) as bit_cast_value;

/*--------------+----------------------*
 | UINT32_value | bit_cast_value       |
 +--------------+----------------------+
 | -1           | 4294967295           |
 *--------------+----------------------*/
```

### `BIT_CAST_TO_UINT64`

```sql
BIT_CAST_TO_UINT64(value)
```

**Description**

ZetaSQL supports bit casting to `UINT64`. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

`UINT64`

**Example**

```sql
SELECT -1 as INT64_value, BIT_CAST_TO_UINT64(-1) as bit_cast_value;

/*--------------+----------------------*
 | INT64_value  | bit_cast_value       |
 +--------------+----------------------+
 | -1           | 18446744073709551615 |
 *--------------+----------------------*/
```

### `BIT_COUNT`

```sql
BIT_COUNT(expression)
```

**Description**

The input, `expression`, must be an
integer or `BYTES`.

Returns the number of bits that are set in the input `expression`.
For signed integers, this is the number of bits in two's complement form.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT a, BIT_COUNT(a) AS a_bits, FORMAT("%T", b) as b, BIT_COUNT(b) AS b_bits
FROM UNNEST([
  STRUCT(0 AS a, b'' AS b), (0, b'\x00'), (5, b'\x05'), (8, b'\x00\x08'),
  (0xFFFF, b'\xFF\xFF'), (-2, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE'),
  (-1, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
  (NULL, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
]) AS x;

/*-------+--------+---------------------------------------------+--------*
 | a     | a_bits | b                                           | b_bits |
 +-------+--------+---------------------------------------------+--------+
 | 0     | 0      | b""                                         | 0      |
 | 0     | 0      | b"\x00"                                     | 0      |
 | 5     | 2      | b"\x05"                                     | 2      |
 | 8     | 1      | b"\x00\x08"                                 | 1      |
 | 65535 | 16     | b"\xff\xff"                                 | 16     |
 | -2    | 63     | b"\xff\xff\xff\xff\xff\xff\xff\xfe"         | 63     |
 | -1    | 64     | b"\xff\xff\xff\xff\xff\xff\xff\xff"         | 64     |
 | NULL  | NULL   | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" | 80     |
 *-------+--------+---------------------------------------------+--------*/
```

## Conversion functions

ZetaSQL supports conversion functions. These data type
conversions are explicit, but some conversions can happen implicitly. You can
learn more about implicit and explicit conversion [here][conversion-rules].

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#cast"><code>CAST</code></a>

</td>
  <td>
    Convert the results of an expression to the given type.
  </td>
</tr>

<tr>
  <td><a href="#safe_casting"><code>SAFE_CAST</code></a>

</td>
  <td>
    Similar to the <code>CAST</code> function, but returns <code>NULL</code>
    when a runtime error is produced.
  </td>
</tr>

  </tbody>
</table>

### `CAST` 
<a id="cast"></a>

```sql
CAST(expression AS typename [format_clause])
```

**Description**

Cast syntax is used in a query to indicate that the result type of an
expression should be converted to some other type.

When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. If you want to protect your queries from these types of errors, you
can use [SAFE_CAST][con-func-safecast].

Casts between supported types that do not successfully map from the original
value to the target domain produce runtime errors. For example, casting
`BYTES` to `STRING` where the byte sequence is not valid UTF-8 results in a
runtime error.

Other examples include:

+ Casting `INT64` to `INT32` where the value overflows `INT32`.
+ Casting `STRING` to `INT32` where the `STRING` contains non-digit characters.

Some casts can include a [format clause][formatting-syntax], which provides
instructions for how to conduct the
cast. For example, you could
instruct a cast to convert a sequence of bytes to a BASE64-encoded string
instead of a UTF-8-encoded string.

The structure of the format clause is unique to each type of cast and more
information is available in the section for that cast.

**Examples**

The following query results in `"true"` if `x` is `1`, `"false"` for any other
non-`NULL` value, and `NULL` if `x` is `NULL`.

```sql
CAST(x=1 AS STRING)
```

### CAST AS ARRAY

```sql
CAST(expression AS ARRAY<element_type>)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `ARRAY`. The
`expression` parameter can represent an expression for these data types:

+ `ARRAY`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>ARRAY</code></td>
    <td><code>ARRAY</code></td>
    <td>
      
      The element types of the input
      array must be castable to the
      element types of the target array.
      For example, casting from type
      <code>ARRAY&lt;INT64&gt;</code> to
      <code>ARRAY&lt;DOUBLE&gt;</code> or
      <code>ARRAY&lt;STRING&gt;</code> is valid;
      casting from type <code>ARRAY&lt;INT64&gt;</code>
      to <code>ARRAY&lt;BYTES&gt;</code> is not valid.
      
    </td>
  </tr>
</table>

### CAST AS BIGNUMERIC 
<a id="cast_bignumeric"></a>

```sql
CAST(expression AS BIGNUMERIC)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `BIGNUMERIC`. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td><code>BIGNUMERIC</code></td>
    <td>
      The floating point number will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">half away from zero</a>.

      Casting a <code>NaN</code>, <code>+inf</code> or
      <code>-inf</code> will return an error. Casting a value outside the range
      of <code>BIGNUMERIC</code></a> returns an overflow error.
    </td>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>BIGNUMERIC</code></td>
    <td>
      The numeric literal contained in the string must not exceed
      the maximum precision or range of the
      <code>BIGNUMERIC</code> type, or an error will occur. If the number of
      digits after the decimal point exceeds 38, then the resulting
      <code>BIGNUMERIC</code> value will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">half away from zero</a>

      to have 38 digits after the decimal point.
    </td>
  </tr>
</table>

### CAST AS BOOL

```sql
CAST(expression AS BOOL)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `BOOL`. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `BOOL`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Integer</td>
    <td><code>BOOL</code></td>
    <td>
      Returns <code>FALSE</code> if <code>x</code> is <code>0</code>,
      <code>TRUE</code> otherwise.
    </td>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>BOOL</code></td>
    <td>
      Returns <code>TRUE</code> if <code>x</code> is <code>"true"</code> and
      <code>FALSE</code> if <code>x</code> is <code>"false"</code><br />
      All other values of <code>x</code> are invalid and throw an error instead
      of casting to a boolean.<br />
      A string is case-insensitive when converting
      to a boolean.
    </td>
  </tr>
</table>

### CAST AS BYTES

```sql
CAST(expression AS BYTES [format_clause])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `BYTES`. The
`expression` parameter can represent an expression for these data types:

+ `BYTES`
+ `STRING`
+ `PROTO`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as bytes][format-string-as-bytes]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>BYTES</code></td>
    <td>
      Strings are cast to bytes using UTF-8 encoding. For example,
      the string "&copy;", when cast to
      bytes, would become a 2-byte sequence with the
      hex values C2 and A9.
    </td>
  </tr>
  
  <tr>
    <td><code>PROTO</code></td>
    <td><code>BYTES</code></td>
    <td>
      Returns the proto2 wire format bytes
      of <code>x</code>.
    </td>
  </tr>
  
</table>

### CAST AS DATE

```sql
CAST(expression AS DATE [format_clause])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `DATE`. The `expression`
parameter can represent an expression for these data types:

+ `STRING`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>DATE</code></td>
    <td>
      When casting from string to date, the string must conform to
      the supported date literal format, and is independent of time zone. If the
      string expression is invalid or represents a date that is outside of the
      supported min/max range, then an error is produced.
    </td>
  </tr>
  
  <tr>
    <td><code>TIMESTAMP</code></td>
    <td><code>DATE</code></td>
    <td>
      Casting from a timestamp to date effectively truncates the timestamp as
      of the default time zone.
    </td>
  </tr>
  
</table>

### CAST AS DATETIME

```sql
CAST(expression AS DATETIME [format_clause])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `DATETIME`. The
`expression` parameter can represent an expression for these data types:

+ `STRING`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>DATETIME</code></td>
    <td>
      When casting from string to datetime, the string must conform to the
      supported datetime literal format, and is independent of time zone. If
      the string expression is invalid or represents a datetime that is outside
      of the supported min/max range, then an error is produced.
    </td>
  </tr>
  
  <tr>
    <td><code>TIMESTAMP</code></td>
    <td><code>DATETIME</code></td>
    <td>
      Casting from a timestamp to datetime effectively truncates the timestamp
      as of the default time zone.
    </td>
  </tr>
  
</table>

### CAST AS ENUM

```sql
CAST(expression AS ENUM)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `ENUM`. The `expression`
parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `STRING`
+ `ENUM`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>ENUM</code></td>
    <td><code>ENUM</code></td>
    <td>Must have the same enum name.</td>
  </tr>
</table>

### CAST AS Floating Point 
<a id="cast_as_floating_point"></a>

```sql
CAST(expression AS DOUBLE)
```

```sql
CAST(expression AS FLOAT)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to floating point types.
The `expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Integer</td>
    <td>Floating Point</td>
    <td>
      Returns a close but potentially not exact floating point value.
    </td>
  </tr>
  
  <tr>
    <td><code>NUMERIC</code></td>
    <td>Floating Point</td>
    <td>
      <code>NUMERIC</code> will convert to the closest floating point number
      with a possible loss of precision.
    </td>
  </tr>
  
  
  <tr>
    <td><code>BIGNUMERIC</code></td>
    <td>Floating Point</td>
    <td>
      <code>BIGNUMERIC</code> will convert to the closest floating point number
      with a possible loss of precision.
    </td>
  </tr>
  
  <tr>
    <td><code>STRING</code></td>
    <td>Floating Point</td>
    <td>
      Returns <code>x</code> as a floating point value, interpreting it as
      having the same form as a valid floating point literal.
      Also supports casts from <code>"[+,-]inf"</code> to
      <code>[,-]Infinity</code>,
      <code>"[+,-]infinity"</code> to <code>[,-]Infinity</code>, and
      <code>"[+,-]nan"</code> to <code>NaN</code>.
      Conversions are case-insensitive.
    </td>
  </tr>
</table>

### CAST AS Integer 
<a id="cast_as_integer"></a>

```sql
CAST(expression AS INT32)
```

```sql
CAST(expression AS UINT32)
```

```sql
CAST(expression AS INT64)
```

```sql
CAST(expression AS UINT64)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to integer types.
The `expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `ENUM`
+ `BOOL`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  
  <tr>
    <td>
      Floating Point
    </td>
    <td>
      Integer
    </td>
    <td>
      Returns the closest integer value.<br />
      Halfway cases such as 1.5 or -0.5 round away from zero.
    </td>
  </tr>
  <tr>
    <td><code>BOOL</code></td>
    <td>Integer</td>
    <td>
      Returns <code>1</code> if <code>x</code> is <code>TRUE</code>,
      <code>0</code> otherwise.
    </td>
  </tr>
  
  <tr>
    <td><code>STRING</code></td>
    <td>Integer</td>
    <td>
      A hex string can be cast to an integer. For example,
      <code>0x123</code> to <code>291</code> or <code>-0x123</code> to
      <code>-291</code>.
    </td>
  </tr>
  
</table>

**Examples**

If you are working with hex strings (`0x123`), you can cast those strings as
integers:

```sql
SELECT '0x123' as hex_value, CAST('0x123' as INT64) as hex_to_int;

/*-----------+------------*
 | hex_value | hex_to_int |
 +-----------+------------+
 | 0x123     | 291        |
 *-----------+------------*/
```

```sql
SELECT '-0x123' as hex_value, CAST('-0x123' as INT64) as hex_to_int;

/*-----------+------------*
 | hex_value | hex_to_int |
 +-----------+------------+
 | -0x123    | -291       |
 *-----------+------------*/
```

### CAST AS INTERVAL

```sql
CAST(expression AS INTERVAL)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `INTERVAL`. The
`expression` parameter can represent an expression for these data types:

+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>INTERVAL</code></td>
    <td>
      When casting from string to interval, the string must conform to either
      <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO 8601 Duration</a>

      standard or to interval literal
      format 'Y-M D H:M:S.F'. Partial interval literal formats are also accepted
      when they are not ambiguous, for example 'H:M:S'.
      If the string expression is invalid or represents an interval that is
      outside of the supported min/max range, then an error is produced.
    </td>
  </tr>
</table>

**Examples**

```sql
SELECT input, CAST(input AS INTERVAL) AS output
FROM UNNEST([
  '1-2 3 10:20:30.456',
  '1-2',
  '10:20:30',
  'P1Y2M3D',
  'PT10H20M30,456S'
]) input

/*--------------------+--------------------*
 | input              | output             |
 +--------------------+--------------------+
 | 1-2 3 10:20:30.456 | 1-2 3 10:20:30.456 |
 | 1-2                | 1-2 0 0:0:0        |
 | 10:20:30           | 0-0 0 10:20:30     |
 | P1Y2M3D            | 1-2 3 0:0:0        |
 | PT10H20M30,456S    | 0-0 0 10:20:30.456 |
 *--------------------+--------------------*/
```

### CAST AS NUMERIC 
<a id="cast_numeric"></a>

```sql
CAST(expression AS NUMERIC)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `NUMERIC`. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td><code>NUMERIC</code></td>
    <td>
      The floating point number will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">half away from zero</a>.

      Casting a <code>NaN</code>, <code>+inf</code> or
      <code>-inf</code> will return an error. Casting a value outside the range
      of <code>NUMERIC</code> returns an overflow error.
    </td>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>NUMERIC</code></td>
    <td>
      The numeric literal contained in the string must not exceed
      the maximum precision or range of the <code>NUMERIC</code>
      type, or an error will occur. If the number of digits
      after the decimal point exceeds nine, then the resulting
      <code>NUMERIC</code> value will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">half away from zero</a>.

      to have nine digits after the decimal point.
    </td>
  </tr>
</table>

### CAST AS PROTO

```sql
CAST(expression AS PROTO)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `PROTO`. The
`expression` parameter can represent an expression for these data types:

+ `STRING`
+ `BYTES`
+ `PROTO`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>PROTO</code></td>
    <td>
      Returns the protocol buffer that results from parsing
      from proto2 text format.<br />
      Throws an error if parsing fails, e.g. if not all required fields are set.
    </td>
  </tr>
  <tr>
    <td><code>BYTES</code></td>
    <td><code>PROTO</code></td>
    <td>
      Returns the protocol buffer that results from parsing
      <code>x</code> from the proto2 wire format.<br />
      Throws an error if parsing fails, e.g. if not all required fields are set.
    </td>
  </tr>
  <tr>
    <td><code>PROTO</code></td>
    <td><code>PROTO</code></td>
    <td>Must have the same protocol buffer name.</td>
  </tr>
</table>

**Example**

The example in this section references a protocol buffer called `Award`.

```proto
message Award {
  required int32 year = 1;
  optional int32 month = 2;
  repeated Type type = 3;

  message Type {
    optional string award_name = 1;
    optional string category = 2;
  }
}
```

```sql
SELECT
  CAST(
    '''
    year: 2001
    month: 9
    type { award_name: 'Best Artist' category: 'Artist' }
    type { award_name: 'Best Album' category: 'Album' }
    '''
    AS zetasql.examples.music.Award)
  AS award_col

/*---------------------------------------------------------*
 | award_col                                               |
 +---------------------------------------------------------+
 | {                                                       |
 |   year: 2001                                            |
 |   month: 9                                              |
 |   type { award_name: "Best Artist" category: "Artist" } |
 |   type { award_name: "Best Album" category: "Album" }   |
 | }                                                       |
 *---------------------------------------------------------*/
```

### CAST AS STRING 
<a id="cast_as_string"></a>

```sql
CAST(expression AS STRING [format_clause [AT TIME ZONE timezone_expr]])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `STRING`. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `ENUM`
+ `BOOL`
+ `BYTES`
+ `PROTO`
+ `TIME`
+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`
+ `INTERVAL`
+ `STRING`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is one
of these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `BYTES`
+ `TIME`
+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

The format clause for `STRING` has an additional optional clause called
`AT TIME ZONE timezone_expr`, which you can use to specify a specific time zone
to use during formatting of a `TIMESTAMP`. If this optional clause is not
included when formatting a `TIMESTAMP`, your current time zone is used.

For more information, see the following topics:

+ [Format bytes as string][format-bytes-as-string]
+ [Format date and time as string][format-date-time-as-string]
+ [Format numeric type as string][format-numeric-type-as-string]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td><code>STRING</code></td>
    <td>Returns an approximate string representation. A returned
    <code>NaN</code> or <code>0</code> will not be signed.<br />
    </td>
  </tr>
  <tr>
    <td><code>BOOL</code></td>
    <td><code>STRING</code></td>
    <td>
      Returns <code>"true"</code> if <code>x</code> is <code>TRUE</code>,
      <code>"false"</code> otherwise.</td>
  </tr>
  <tr>
    <td><code>BYTES</code></td>
    <td><code>STRING</code></td>
    <td>
      Returns <code>x</code> interpreted as a UTF-8 string.<br />
      For example, the bytes literal
      <code>b'\xc2\xa9'</code>, when cast to a string,
      is interpreted as UTF-8 and becomes the unicode character "&copy;".<br />
      An error occurs if <code>x</code> is not valid UTF-8.</td>
  </tr>
  
  <tr>
    <td><code>ENUM</code></td>
    <td><code>STRING</code></td>
    <td>
      Returns the canonical enum value name of
      <code>x</code>.<br />
      If an enum value has multiple names (aliases),
      the canonical name/alias for that value is used.</td>
  </tr>
  
  
  <tr>
    <td><code>PROTO</code></td>
    <td><code>STRING</code></td>
    <td>Returns the proto2 text format representation of <code>x</code>.</td>
  </tr>
  
  
  <tr>
    <td><code>TIME</code></td>
    <td><code>STRING</code></td>
    <td>
      Casting from a time type to a string is independent of time zone and
      is of the form <code>HH:MM:SS</code>.
    </td>
  </tr>
  
  
  <tr>
    <td><code>DATE</code></td>
    <td><code>STRING</code></td>
    <td>
      Casting from a date type to a string is independent of time zone and is
      of the form <code>YYYY-MM-DD</code>.
    </td>
  </tr>
  
  
  <tr>
    <td><code>DATETIME</code></td>
    <td><code>STRING</code></td>
    <td>
      Casting from a datetime type to a string is independent of time zone and
      is of the form <code>YYYY-MM-DD HH:MM:SS</code>.
    </td>
  </tr>
  
  
  <tr>
    <td><code>TIMESTAMP</code></td>
    <td><code>STRING</code></td>
    <td>
      When casting from timestamp types to string, the timestamp is interpreted
      using the default time zone, which is implementation defined. The number of
      subsecond digits produced depends on the number of trailing zeroes in the
      subsecond part: the CAST function will truncate zero, three, or six
      digits.
    </td>
  </tr>
  
  <tr>
    <td><code>INTERVAL</code></td>
    <td><code>STRING</code></td>
    <td>
      Casting from an interval to a string is of the form
      <code>Y-M D H:M:S</code>.
    </td>
  </tr>
  
</table>

**Examples**

```sql
SELECT CAST(CURRENT_DATE() AS STRING) AS current_date

/*---------------*
 | current_date  |
 +---------------+
 | 2021-03-09    |
 *---------------*/
```

```sql
SELECT CAST(CURRENT_DATE() AS STRING FORMAT 'DAY') AS current_day

/*-------------*
 | current_day |
 +-------------+
 | MONDAY      |
 *-------------*/
```

```sql
SELECT CAST(
  TIMESTAMP '2008-12-25 00:00:00+00:00'
  AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string

-- Results depend upon where this query was executed.
/*------------------------------*
 | date_time_to_string          |
 +------------------------------+
 | 2008-12-24 16:00:00 -08:00   |
 *------------------------------*/
```

```sql
SELECT CAST(
  TIMESTAMP '2008-12-25 00:00:00+00:00'
  AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM'
  AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string

-- Because the time zone is specified, the result is always the same.
/*------------------------------*
 | date_time_to_string          |
 +------------------------------+
 | 2008-12-25 05:30:00 +05:30   |
 *------------------------------*/
```

```sql
SELECT CAST(INTERVAL 3 DAY AS STRING) AS interval_to_string

/*--------------------*
 | interval_to_string |
 +--------------------+
 | 0-0 3 0:0:0        |
 *--------------------*/
```

```sql
SELECT CAST(
  INTERVAL "1-2 3 4:5:6.789" YEAR TO SECOND
  AS STRING) AS interval_to_string

/*--------------------*
 | interval_to_string |
 +--------------------+
 | 1-2 3 4:5:6.789    |
 *--------------------*/
```

### CAST AS STRUCT

```sql
CAST(expression AS STRUCT)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `STRUCT`. The
`expression` parameter can represent an expression for these data types:

+ `STRUCT`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRUCT</code></td>
    <td><code>STRUCT</code></td>
    <td>
      Allowed if the following conditions are met:<br />
      <ol>
        <li>
          The two structs have the same number of
          fields.
        </li>
        <li>
          The original struct field types can be
          explicitly cast to the corresponding target
          struct field types (as defined by field
          order, not field name).
        </li>
      </ol>
    </td>
  </tr>
</table>

### CAST AS TIME

```sql
CAST(expression AS TIME [format_clause])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to TIME. The `expression`
parameter can represent an expression for these data types:

+ `STRING`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>TIME</code></td>
    <td>
      When casting from string to time, the string must conform to
      the supported time literal format, and is independent of time zone. If the
      string expression is invalid or represents a time that is outside of the
      supported min/max range, then an error is produced.
    </td>
  </tr>
</table>

### CAST AS TIMESTAMP

```sql
CAST(expression AS TIMESTAMP [format_clause [AT TIME ZONE timezone_expr]])
```

**Description**

ZetaSQL supports [casting][con-func-cast] to `TIMESTAMP`. The
`expression` parameter can represent an expression for these data types:

+ `STRING`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

The format clause for `TIMESTAMP` has an additional optional clause called
`AT TIME ZONE timezone_expr`, which you can use to specify a specific time zone
to use during formatting. If this optional clause is not included, your
current time zone is used.

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td><code>STRING</code></td>
    <td><code>TIMESTAMP</code></td>
    <td>
      When casting from string to a timestamp, <code>string_expression</code>
      must conform to the supported timestamp literal formats, or else a runtime
      error occurs. The <code>string_expression</code> may itself contain a
      time zone.
      <br /><br />
      If there is a time zone in the <code>string_expression</code>, that
      time zone is used for conversion, otherwise the default time zone,
      which is implementation defined, is used. If the string has fewer than six digits,
      then it is implicitly widened.
      <br /><br />
      An error is produced if the <code>string_expression</code> is invalid,
      has more than six subsecond digits (i.e. precision greater than
      microseconds), or represents a time outside of the supported timestamp
      range.
    </td>
  </tr>
  
  <tr>
    <td><code>DATE</code></td>
    <td><code>TIMESTAMP</code></td>
    <td>
      Casting from a date to a timestamp interprets <code>date_expression</code>
      as of midnight (start of the day) in the default time zone,
      which is implementation defined.
    </td>
  </tr>
  
  
  <tr>
    <td><code>DATETIME</code></td>
    <td><code>TIMESTAMP</code></td>
    <td>
      Casting from a datetime to a timestamp interprets
      <code>datetime_expression</code> in the default time zone,
      which is implementation defined.
      <br /><br />
      Most valid datetime values have exactly one corresponding timestamp
      in each time zone. However, there are certain combinations of valid
      datetime values and time zones that have zero or two corresponding
      timestamp values. This happens in a time zone when clocks are set forward
      or set back, such as for Daylight Savings Time.
      When there are two valid timestamps, the earlier one is used.
      When there is no valid timestamp, the length of the gap in time
      (typically one hour) is added to the datetime.
    </td>
  </tr>
  
</table>

**Examples**

The following example casts a string-formatted timestamp as a timestamp:

```sql
SELECT CAST("2020-06-02 17:00:53.110+00:00" AS TIMESTAMP) AS as_timestamp

-- Results depend upon where this query was executed.
/*----------------------------*
 | as_timestamp               |
 +----------------------------+
 | 2020-06-03 00:00:53.110+00 |
 *----------------------------*/
```

The following examples cast a string-formatted date and time as a timestamp.
These examples return the same output as the previous example.

```sql
SELECT CAST('06/02/2020 17:00:53.110' AS TIMESTAMP FORMAT 'MM/DD/YYYY HH24:MI:SS.FF3' AT TIME ZONE 'UTC') AS as_timestamp
```

```sql
SELECT CAST('06/02/2020 17:00:53.110' AS TIMESTAMP FORMAT 'MM/DD/YYYY HH24:MI:SS.FF3' AT TIME ZONE '+00') AS as_timestamp
```

```sql
SELECT CAST('06/02/2020 17:00:53.110 +00' AS TIMESTAMP FORMAT 'MM/DD/YYYY HH24:MI:SS.FF3 TZH') AS as_timestamp
```

[formatting-syntax]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#formatting_syntax

[format-string-as-bytes]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_string_as_bytes

[format-bytes-as-string]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_bytes_as_string

[format-date-time-as-string]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_date_time_as_string

[format-string-as-date-time]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_string_as_datetime

[format-numeric-type-as-string]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_numeric_type_as_string

[con-func-cast]: #cast

[con-func-safecast]: #safe_casting

### `SAFE_CAST` 
<a id="safe_casting"></a>

<pre class="lang-sql prettyprint">
<code>SAFE_CAST(expression AS typename [format_clause])</code>
</pre>

**Description**

When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. For example, the following query generates an error:

```sql
SELECT CAST("apple" AS INT64) AS not_a_number;
```

If you want to protect your queries from these types of errors, you can use
`SAFE_CAST`. `SAFE_CAST` replaces runtime errors with `NULL`s.  However, during
static analysis, impossible casts between two non-castable types still produce
an error because the query is invalid.

```sql
SELECT SAFE_CAST("apple" AS INT64) AS not_a_number;

/*--------------*
 | not_a_number |
 +--------------+
 | NULL         |
 *--------------*/
```

Some casts can include a [format clause][formatting-syntax], which provides
instructions for how to conduct the
cast. For example, you could
instruct a cast to convert a sequence of bytes to a BASE64-encoded string
instead of a UTF-8-encoded string.

The structure of the format clause is unique to each type of cast and more
information is available in the section for that cast.

If you are casting from bytes to strings, you can also use the
function, [`SAFE_CONVERT_BYTES_TO_STRING`][SC_BTS]. Any invalid UTF-8 characters
are replaced with the unicode replacement character, `U+FFFD`.

[SC_BTS]: #safe_convert_bytes_to_string

[formatting-syntax]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#formatting_syntax

### Other conversion functions 
<a id="other_conv_functions"></a>

You can learn more about these conversion functions elsewhere in the
documentation:

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

Conversion function                    | From               | To
-------                                | --------           | -------
[ARRAY_TO_STRING][ARRAY_STRING]        | ARRAY              | STRING
[BIT_CAST_TO_INT32][BIT_I32]           | UINT32             | INT32
[BIT_CAST_TO_INT64][BIT_I64]           | UINT64             | INT64
[BIT_CAST_TO_UINT32][BIT_U32]          | INT32              | UINT32
[BIT_CAST_TO_UINT64][BIT_U64]          | INT64              | UINT64
[BOOL][JSON_TO_BOOL]                   | JSON               | BOOL
[DATE][T_DATE]                         | Various data types | DATE
[DATETIME][T_DATETIME]                 | Various data types | DATETIME
[FLOAT64][JSON_TO_DOUBLE]              | JSON               | DOUBLE
[FROM_BASE32][F_B32]                   | STRING             | BYTEs
[FROM_BASE64][F_B64]                   | STRING             | BYTES
[FROM_HEX][F_HEX]                      | STRING             | BYTES
[FROM_PROTO][F_PROTO]                  | PROTO value        | Most data types
[INT64][JSON_TO_INT64]                 | JSON               | INT64
[PARSE_DATE][P_DATE]                   | STRING             | DATE
[PARSE_DATETIME][P_DATETIME]           | STRING             | DATETIME
[PARSE_JSON][P_JSON]                   | STRING             | JSON
[PARSE_TIME][P_TIME]                   | STRING             | TIME
[PARSE_TIMESTAMP][P_TIMESTAMP]         | STRING             | TIMESTAMP
[SAFE_CONVERT_BYTES_TO_STRING][SC_BTS] | BYTES              | STRING
[STRING][STRING_TIMESTAMP]             | TIMESTAMP          | STRING
[STRING][JSON_TO_STRING]               | JSON               | STRING
[TIME][T_TIME]                         | Various data types | TIME
[TIMESTAMP][T_TIMESTAMP]               | Various data types | TIMESTAMP
[TO_BASE32][T_B32]                     | BYTES              | STRING
[TO_BASE64][T_B64]                     | BYTES              | STRING
[TO_HEX][T_HEX]                        | BYTES              | STRING
[TO_JSON][T_JSON]                      | All data types     | JSON
[TO_JSON_STRING][T_JSON_STRING]        | All data types     | STRING
[TO_PROTO][T_PROTO]                    | Most data types    | PROTO value

<!-- mdlint on -->

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md

[ARRAY_STRING]: #array_to_string

[BIT_I32]: #bit_cast_to_int32

[BIT_U32]: #bit_cast_to_uint32

[BIT_I64]: #bit_cast_to_int64

[BIT_U64]: #bit_cast_to_uint64

[F_B32]: #from_base32

[F_B64]: #from_base64

[F_HEX]: #from_hex

[F_PROTO]: #from_proto

[P_DATE]: #parse_date

[P_DATETIME]: #parse_datetime

[P_JSON]: #parse_json

[P_TIME]: #parse_time

[P_TIMESTAMP]: #parse_timestamp

[SC_BTS]: #safe_convert_bytes_to_string

[STRING_TIMESTAMP]: #string

[T_B32]: #to_base32

[T_B64]: #to_base64

[T_HEX]: #to_hex

[T_JSON]: #to_json

[T_JSON_STRING]: #to_json_string

[T_PROTO]: #to_proto

[T_DATE]: #date

[T_DATETIME]: #datetime

[T_TIMESTAMP]: #timestamp

[T_TIME]: #time

[JSON_TO_BOOL]: #bool_for_json

[JSON_TO_STRING]: #string_for_json

[JSON_TO_INT64]: #int64_for_json

[JSON_TO_DOUBLE]: #double_for_json

<!-- mdlint on -->

## Mathematical functions

ZetaSQL supports mathematical functions.
All mathematical functions have the following behaviors:

+  They return `NULL` if any of the input parameters is `NULL`.
+  They return `NaN` if any of the arguments is `NaN`.

### `ABS`

```
ABS(X)
```

**Description**

Computes absolute value. Returns an error if the argument is an integer and the
output value cannot be represented as the same type; this happens only for the
largest negative input value, which has no positive representation.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ABS(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>25</td>
      <td>25</td>
    </tr>
    <tr>
      <td>-25</td>
      <td>25</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `ACOS`

```
ACOS(X)
```

**Description**

Computes the principal value of the inverse cosine of X. The return value is in
the range [0,&pi;]. Generates an error if X is a value outside of the
range [-1, 1].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ACOS(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### `ACOSH`

```
ACOSH(X)
```

**Description**

Computes the inverse hyperbolic cosine of X. Generates an error if X is a value
less than 1.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ACOSH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### `ASIN`

```
ASIN(X)
```

**Description**

Computes the principal value of the inverse sine of X. The return value is in
the range [-&pi;/2,&pi;/2]. Generates an error if X is outside of
the range [-1, 1].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ASIN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### `ASINH`

```
ASINH(X)
```

**Description**

Computes the inverse hyperbolic sine of X. Does not fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ASINH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `ATAN`

```
ATAN(X)
```

**Description**

Computes the principal value of the inverse tangent of X. The return value is
in the range [-&pi;/2,&pi;/2]. Does not fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ATAN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td>&pi;/2</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>-&pi;/2</td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `ATAN2`

```
ATAN2(X, Y)
```

**Description**

Calculates the principal value of the inverse tangent of X/Y using the signs of
the two arguments to determine the quadrant. The return value is in the range
[-&pi;,&pi;].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>ATAN2(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>NaN</code></td>
      <td>Any value</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>Any value</td>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>Positive Finite value</td>
      <td><code>-inf</code></td>
      <td>&pi;</td>
    </tr>
    <tr>
      <td>Negative Finite value</td>
      <td><code>-inf</code></td>
      <td>-&pi;</td>
    </tr>
    <tr>
      <td>Finite value</td>
      <td><code>+inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Finite value</td>
      <td>&pi;/2</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Finite value</td>
      <td>-&pi;/2</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>-inf</code></td>
      <td>&frac34;&pi;</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
      <td>-&frac34;&pi;</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
      <td>&pi;/4</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
      <td>-&pi;/4</td>
    </tr>
  </tbody>
</table>

### `ATANH`

```
ATANH(X)
```

**Description**

Computes the inverse hyperbolic tangent of X. Generates an error if X is outside
of the range (-1, 1).

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ATANH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### `CBRT`

```
CBRT(X)
```

**Description**

Computes the cube root of `X`. `X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Supports the `SAFE.` prefix.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>CBRT(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>0</code></td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT CBRT(27) AS cube_root;

/*--------------------*
 | cube_root          |
 +--------------------+
 | 3.0000000000000004 |
 *--------------------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `CEIL`

```
CEIL(X)
```

**Description**

Returns the smallest integral value that is not less than X.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>CEIL(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `CEILING`

```
CEILING(X)
```

**Description**

Synonym of CEIL(X)

### `COS`

```
COS(X)
```

**Description**

Computes the cosine of X where X is specified in radians. Never fails.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COS(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `COSH`

```
COSH(X)
```

**Description**

Computes the hyperbolic cosine of X where X is specified in radians.
Generates an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COSH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `COT`

```
COT(X)
```

**Description**

Computes the cotangent for the angle of `X`, where `X` is specified in radians.
`X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Supports the `SAFE.` prefix.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COT(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>0</code></td>
      <td><code>Error</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT COT(1) AS a, SAFE.COT(0) AS b;

/*---------------------+------*
 | a                   | b    |
 +---------------------+------+
 | 0.64209261593433065 | NULL |
 *---------------------+------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `COTH`

```
COTH(X)
```

**Description**

Computes the hyperbolic cotangent for the angle of `X`, where `X` is specified
in radians. `X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Supports the `SAFE.` prefix.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COTH(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>1</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-1</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>0</code></td>
      <td><code>Error</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT COTH(1) AS a, SAFE.COTH(0) AS b;

/*----------------+------*
 | a              | b    |
 +----------------+------+
 | 1.313035285499 | NULL |
 *----------------+------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `CSC`

```
CSC(X)
```

**Description**

Computes the cosecant of the input angle, which is in radians.
`X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Supports the `SAFE.` prefix.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>CSC(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>0</code></td>
      <td><code>Error</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT CSC(100) AS a, CSC(-1) AS b, SAFE.CSC(0) AS c;

/*----------------+-----------------+------*
 | a              | b               | c    |
 +----------------+-----------------+------+
 | -1.97485753142 | -1.188395105778 | NULL |
 *----------------+-----------------+------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `CSCH`

```
CSCH(X)
```

**Description**

Computes the hyperbolic cosecant of the input angle, which is in radians.
`X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Supports the `SAFE.` prefix.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>CSCH(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>0</code></td>
      <td><code>Error</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT CSCH(0.5) AS a, CSCH(-2) AS b, SAFE.CSCH(0) AS c;

/*----------------+----------------+------*
 | a              | b              | c    |
 +----------------+----------------+------+
 | 1.919034751334 | -0.27572056477 | NULL |
 *----------------+----------------+------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `DIV`

```
DIV(X, Y)
```

**Description**

Returns the result of integer division of X by Y. Division by zero returns
an error. Division by -1 may overflow.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>DIV(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20</td>
      <td>4</td>
      <td>5</td>
    </tr>
    <tr>
      <td>12</td>
      <td>-7</td>
      <td>-1</td>
    </tr>
    <tr>
      <td>20</td>
      <td>3</td>
      <td>6</td>
    </tr>
    <tr>
      <td>0</td>
      <td>20</td>
      <td>0</td>
    </tr>
    <tr>
      <td>20</td>
      <td>0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>INT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>INT64</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>UINT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>UINT64</code></th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
</tbody>

</table>

### `EXP`

```
EXP(X)
```

**Description**

Computes *e* to the power of X, also called the natural exponential function. If
the result underflows, this function returns a zero. Generates an error if the
result overflows.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>EXP(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `FLOOR`

```
FLOOR(X)
```

**Description**

Returns the largest integral value that is not greater than X.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>FLOOR(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `GREATEST`

```
GREATEST(X1,...,XN)
```

**Description**

Returns the greatest value among `X1,...,XN`. If any argument is `NULL`, returns
`NULL`. Otherwise, in the case of floating-point arguments, if any argument is
`NaN`, returns `NaN`. In all other cases, returns the value among `X1,...,XN`
that has the greatest value according to the ordering used by the `ORDER BY`
clause. The arguments `X1, ..., XN` must be coercible to a common supertype, and
the supertype must support ordering.

<table>
  <thead>
    <tr>
      <th>X1,...,XN</th>
      <th>GREATEST(X1,...,XN)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>3,5,1</td>
      <td>5</td>
    </tr>
  </tbody>
</table>

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return Data Types**

Data type of the input values.

### `IEEE_DIVIDE`

```
IEEE_DIVIDE(X, Y)
```

**Description**

Divides X by Y; this function never fails. Returns
`DOUBLE` unless
both X and Y are `FLOAT`, in which case it returns
`FLOAT`. Unlike the division operator (/),
this function does not generate errors for division by zero or overflow.</p>

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>IEEE_DIVIDE(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20.0</td>
      <td>4.0</td>
      <td>5.0</td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>25.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>25.0</td>
      <td>0.0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>-25.0</td>
      <td>0.0</td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>0.0</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td>0.0</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `IS_INF`

```
IS_INF(X)
```

**Description**

Returns `TRUE` if the value is positive or negative infinity.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>IS_INF(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td>25</td>
      <td><code>FALSE</code></td>
    </tr>
  </tbody>
</table>

### `IS_NAN`

```
IS_NAN(X)
```

**Description**

Returns `TRUE` if the value is a `NaN` value.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>IS_NAN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>NaN</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td>25</td>
      <td><code>FALSE</code></td>
    </tr>
  </tbody>
</table>

### `LEAST`

```
LEAST(X1,...,XN)
```

**Description**

Returns the least value among `X1,...,XN`. If any argument is `NULL`, returns
`NULL`. Otherwise, in the case of floating-point arguments, if any argument is
`NaN`, returns `NaN`. In all other cases, returns the value among `X1,...,XN`
that has the least value according to the ordering used by the `ORDER BY`
clause. The arguments `X1, ..., XN` must be coercible to a common supertype, and
the supertype must support ordering.

<table>
  <thead>
    <tr>
      <th>X1,...,XN</th>
      <th>LEAST(X1,...,XN)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>3,5,1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return Data Types**

Data type of the input values.

### `LN`

```
LN(X)
```

**Description**

Computes the natural logarithm of X. Generates an error if X is less than or
equal to zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>LN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>X &lt; 0</code></td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `LOG`

```
LOG(X [, Y])
```

**Description**

If only X is present, `LOG` is a synonym of `LN`. If Y is also present,
`LOG` computes the logarithm of X to base Y.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>LOG(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>100.0</td>
      <td>10.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Any value</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>Any value</td>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>0.0 &lt; Y &lt; 1.0</td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &gt; 1.0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>X &lt;= 0</td>
      <td>Any value</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>Any value</td>
      <td>Y &lt;= 0</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>Any value</td>
      <td>1.0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `LOG10`

```
LOG10(X)
```

**Description**

Similar to `LOG`, but computes logarithm to base 10.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>LOG10(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>100.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>X &lt;= 0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `MOD`

```
MOD(X, Y)
```

**Description**

Modulo function: returns the remainder of the division of X by Y. Returned
value has the same sign as X. An error is generated if Y is 0.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>MOD(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>25</td>
      <td>12</td>
      <td>1</td>
    </tr>
    <tr>
      <td>25</td>
      <td>0</td>
      <td>Error</td>
    </tr>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>INT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>INT64</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>UINT32</code></th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>UINT64</code></th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td></tr>
</tbody>

</table>

### `PI`

```sql
PI()
```

**Description**

Returns the mathematical constant `π` as a `DOUBLE`
value.

**Return type**

`DOUBLE`

**Example**

```sql
SELECT PI() AS pi

/*--------------------*
 | pi                 |
 +--------------------+
 | 3.1415926535897931 |
 *--------------------*/
```

### `PI_BIGNUMERIC`

```sql
PI_BIGNUMERIC()
```

**Description**

Returns the mathematical constant `π` as a `BIGNUMERIC` value.

**Return type**

`BIGNUMERIC`

**Example**

```sql
SELECT PI_BIGNUMERIC() AS pi

/*-----------------------------------------*
 | pi                                      |
 +-----------------------------------------+
 | 3.1415926535897932384626433832795028842 |
 *-----------------------------------------*/
```

### `PI_NUMERIC`

```sql
PI_NUMERIC()
```

**Description**

Returns the mathematical constant `π` as a `NUMERIC` value.

**Return type**

`NUMERIC`

**Example**

```sql
SELECT PI_NUMERIC() AS pi

/*-------------*
 | pi          |
 +-------------+
 | 3.141592654 |
 *-------------*/
```

### `POW`

```
POW(X, Y)
```

**Description**

Returns the value of X raised to the power of Y. If the result underflows and is
not representable, then the function returns a  value of zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>POW(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>3.0</td>
      <td>8.0</td>
    </tr>
    <tr>
      <td>1.0</td>
      <td>Any value including <code>NaN</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>Any value including <code>NaN</code></td>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>-1.0</td>
      <td><code>+inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>-1.0</td>
      <td><code>-inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>ABS(X) &lt; 1</td>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>ABS(X) &gt; 1</td>
      <td><code>-inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>ABS(X) &lt; 1</td>
      <td><code>+inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>ABS(X) &gt; 1</td>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Y &lt; 0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Y &gt; 0</td>
      <td><code>-inf</code> if Y is an odd integer, <code>+inf</code> otherwise</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &lt; 0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &gt; 0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>Finite value &lt; 0</td>
      <td>Non-integer</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>0</td>
      <td>Finite value &lt; 0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.

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

### `POWER`

```
POWER(X, Y)
```

**Description**

Synonym of [`POW(X, Y)`][pow].

[pow]: #pow

### `RAND`

```
RAND()
```

**Description**

Generates a pseudo-random value of type `DOUBLE` in
the range of [0, 1), inclusive of 0 and exclusive of 1.

### `ROUND`

```
ROUND(X [, N])
```

**Description**

If only X is present, rounds X to the nearest integer. If N is present,
rounds X to N decimal places after the decimal point. If N is negative,
rounds off digits to the left of the decimal point. Rounds halfway cases
away from zero. Generates an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>Expression</th>
      <th>Return Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>ROUND(2.0)</code></td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>ROUND(2.3)</code></td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>ROUND(2.8)</code></td>
      <td>3.0</td>
    </tr>
    <tr>
      <td><code>ROUND(2.5)</code></td>
      <td>3.0</td>
    </tr>
    <tr>
      <td><code>ROUND(-2.3)</code></td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td><code>ROUND(-2.8)</code></td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td><code>ROUND(-2.5)</code></td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td><code>ROUND(0)</code></td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>ROUND(+inf)</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>ROUND(-inf)</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>ROUND(NaN)</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>ROUND(123.7, -1)</code></td>
      <td>120.0</td>
    </tr>
    <tr>
      <td><code>ROUND(1.235, 2)</code></td>
      <td>1.24</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `SAFE_ADD`

```
SAFE_ADD(X, Y)
```

**Description**

Equivalent to the addition operator (`+`), but returns
`NULL` if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>SAFE_ADD(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>5</td>
      <td>4</td>
      <td>9</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SAFE_DIVIDE`

```
SAFE_DIVIDE(X, Y)
```

**Description**

Equivalent to the division operator (`X / Y`), but returns
`NULL` if an error occurs, such as a division by zero error.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>SAFE_DIVIDE(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20</td>
      <td>4</td>
      <td>5</td>
    </tr>
    <tr>
      <td>0</td>
      <td>20</td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td>20</td>
      <td>0</td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SAFE_MULTIPLY`

```
SAFE_MULTIPLY(X, Y)
```

**Description**

Equivalent to the multiplication operator (`*`), but returns
`NULL` if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>SAFE_MULTIPLY(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20</td>
      <td>4</td>
      <td>80</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SAFE_NEGATE`

```
SAFE_NEGATE(X)
```

**Description**

Equivalent to the unary minus operator (`-`), but returns
`NULL` if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SAFE_NEGATE(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>+1</td>
      <td>-1</td>
    </tr>
    <tr>
      <td>-1</td>
      <td>+1</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SAFE_SUBTRACT`

```
SAFE_SUBTRACT(X, Y)
```

**Description**

Returns the result of Y subtracted from X.
Equivalent to the subtraction operator (`-`), but returns
`NULL` if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>SAFE_SUBTRACT(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>5</td>
      <td>4</td>
      <td>1</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SEC`

```
SEC(X)
```

**Description**

Computes the secant for the angle of `X`, where `X` is specified in radians.
`X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SEC(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT SEC(100) AS a, SEC(-1) AS b;

/*----------------+---------------*
 | a              | b             |
 +----------------+---------------+
 | 1.159663822905 | 1.85081571768 |
 *----------------+---------------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `SECH`

```
SECH(X)
```

**Description**

Computes the hyperbolic secant for the angle of `X`, where `X` is specified
in radians. `X` can be any data type
that [coerces to `DOUBLE`][conversion-rules].
Never produces an error.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SECH(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>0</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NULL</code></td>
      <td><code>NULL</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

`DOUBLE`

**Example**

```sql
SELECT SECH(0.5) AS a, SECH(-2) AS b, SECH(100) AS c;

/*----------------+----------------+---------------------*
 | a              | b              | c                   |
 +----------------+----------------+---------------------+
 | 0.88681888397  | 0.265802228834 | 7.4401519520417E-44 |
 *----------------+----------------+---------------------*/
```

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#conversion_rules

### `SIGN`

```
SIGN(X)
```

**Description**

Returns `-1`, `0`, or `+1` for negative, zero and positive arguments
respectively. For floating point arguments, this function does not distinguish
between positive and negative zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SIGN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>25</td>
      <td>+1</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td>-25</td>
      <td>-1</td>
    </tr>
    <tr>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

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

### `SIN`

```
SIN(X)
```

**Description**

Computes the sine of X where X is specified in radians. Never fails.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SIN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `SINH`

```
SINH(X)
```

**Description**

Computes the hyperbolic sine of X where X is specified in radians. Generates
an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SINH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `SQRT`

```
SQRT(X)
```

**Description**

Computes the square root of X. Generates an error if X is less than 0.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SQRT(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>25.0</code></td>
      <td><code>5.0</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>X &lt; 0</code></td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

### `TAN`

```
TAN(X)
```

**Description**

Computes the tangent of X where X is specified in radians. Generates an error if
overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TAN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `TANH`

```
TANH(X)
```

**Description**

Computes the hyperbolic tangent of X where X is specified in radians. Does not
fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TANH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>-1.0</td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### `TRUNC`

```
TRUNC(X [, N])
```

**Description**

If only X is present, `TRUNC` rounds X to the nearest integer whose absolute
value is not greater than the absolute value of X. If N is also present, `TRUNC`
behaves like `ROUND(X, N)`, but always rounds towards zero and never overflows.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TRUNC(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

## Navigation functions

ZetaSQL supports navigation functions.
Navigation functions are a subset window functions. To create a
window function call and learn about the syntax for window functions,
see [Window function_calls][window-function-calls].

Navigation functions generally compute some
`value_expression` over a different row in the window frame from the
current row. The `OVER` clause syntax varies across navigation functions.

For all navigation functions, the result data type is the same type as
`value_expression`.

### `FIRST_VALUE`

```sql
FIRST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]
  [ window_frame_clause ]

```

**Description**

Returns the value of the `value_expression` for the first row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the fastest time for each division.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', fastest_time) AS fastest_time,
  TIMESTAMP_DIFF(finish_time, fastest_time, SECOND) AS delta_in_seconds
FROM (
  SELECT name,
  finish_time,
  division,
  FIRST_VALUE(finish_time)
    OVER (PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fastest_time
  FROM finishers);

/*-----------------+-------------+----------+--------------+------------------*
 | name            | finish_time | division | fastest_time | delta_in_seconds |
 +-----------------+-------------+----------+--------------+------------------+
 | Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | 0                |
 | Sophia Liu      | 02:51:45    | F30-34   | 02:51:45     | 0                |
 | Nikki Leith     | 02:59:01    | F30-34   | 02:51:45     | 436              |
 | Jen Edwards     | 03:06:36    | F30-34   | 02:51:45     | 891              |
 | Meghan Lederer  | 03:07:41    | F30-34   | 02:51:45     | 956              |
 | Lauren Reasoner | 03:10:14    | F30-34   | 02:51:45     | 1109             |
 | Lisa Stelzner   | 02:54:11    | F35-39   | 02:54:11     | 0                |
 | Lauren Matthews | 03:01:17    | F35-39   | 02:54:11     | 426              |
 | Desiree Berry   | 03:05:42    | F35-39   | 02:54:11     | 691              |
 | Suzy Slane      | 03:06:24    | F35-39   | 02:54:11     | 733              |
 *-----------------+-------------+----------+--------------+------------------*/
```

### `LAG`

```sql
LAG (value_expression[, offset [, default_expression]])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Returns the value of the `value_expression` on a preceding row. Changing the
`offset` value changes which preceding row is returned; the default value is
`1`, indicating the previous row in the window frame. An error occurs if
`offset` is NULL or a negative value.

The optional `default_expression` is used if there isn't a row in the window
frame at the specified offset. This expression must be a constant expression and
its type must be implicitly coercible to the type of `value_expression`. If left
unspecified, `default_expression` defaults to NULL.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example illustrates a basic use of the `LAG` function.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS preceding_runner
FROM finishers;

/*-----------------+-------------+----------+------------------*
 | name            | finish_time | division | preceding_runner |
 +-----------------+-------------+----------+------------------+
 | Carly Forte     | 03:08:58    | F25-29   | NULL             |
 | Sophia Liu      | 02:51:45    | F30-34   | NULL             |
 | Nikki Leith     | 02:59:01    | F30-34   | Sophia Liu       |
 | Jen Edwards     | 03:06:36    | F30-34   | Nikki Leith      |
 | Meghan Lederer  | 03:07:41    | F30-34   | Jen Edwards      |
 | Lauren Reasoner | 03:10:14    | F30-34   | Meghan Lederer   |
 | Lisa Stelzner   | 02:54:11    | F35-39   | NULL             |
 | Lauren Matthews | 03:01:17    | F35-39   | Lisa Stelzner    |
 | Desiree Berry   | 03:05:42    | F35-39   | Lauren Matthews  |
 | Suzy Slane      | 03:06:24    | F35-39   | Desiree Berry    |
 *-----------------+-------------+----------+------------------*/
```

This next example uses the optional `offset` parameter.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers;

/*-----------------+-------------+----------+-------------------*
 | name            | finish_time | division | two_runners_ahead |
 +-----------------+-------------+----------+-------------------+
 | Carly Forte     | 03:08:58    | F25-29   | NULL              |
 | Sophia Liu      | 02:51:45    | F30-34   | NULL              |
 | Nikki Leith     | 02:59:01    | F30-34   | NULL              |
 | Jen Edwards     | 03:06:36    | F30-34   | Sophia Liu        |
 | Meghan Lederer  | 03:07:41    | F30-34   | Nikki Leith       |
 | Lauren Reasoner | 03:10:14    | F30-34   | Jen Edwards       |
 | Lisa Stelzner   | 02:54:11    | F35-39   | NULL              |
 | Lauren Matthews | 03:01:17    | F35-39   | NULL              |
 | Desiree Berry   | 03:05:42    | F35-39   | Lisa Stelzner     |
 | Suzy Slane      | 03:06:24    | F35-39   | Lauren Matthews   |
 *-----------------+-------------+----------+-------------------*/
```

The following example replaces NULL values with a default value.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2, 'Nobody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers;

/*-----------------+-------------+----------+-------------------*
 | name            | finish_time | division | two_runners_ahead |
 +-----------------+-------------+----------+-------------------+
 | Carly Forte     | 03:08:58    | F25-29   | Nobody            |
 | Sophia Liu      | 02:51:45    | F30-34   | Nobody            |
 | Nikki Leith     | 02:59:01    | F30-34   | Nobody            |
 | Jen Edwards     | 03:06:36    | F30-34   | Sophia Liu        |
 | Meghan Lederer  | 03:07:41    | F30-34   | Nikki Leith       |
 | Lauren Reasoner | 03:10:14    | F30-34   | Jen Edwards       |
 | Lisa Stelzner   | 02:54:11    | F35-39   | Nobody            |
 | Lauren Matthews | 03:01:17    | F35-39   | Nobody            |
 | Desiree Berry   | 03:05:42    | F35-39   | Lisa Stelzner     |
 | Suzy Slane      | 03:06:24    | F35-39   | Lauren Matthews   |
 *-----------------+-------------+----------+-------------------*/
```

### `LAST_VALUE`

```sql
LAST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]
  [ window_frame_clause ]

```

**Description**

Returns the value of the `value_expression` for the last row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the slowest time for each division.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', slowest_time) AS slowest_time,
  TIMESTAMP_DIFF(slowest_time, finish_time, SECOND) AS delta_in_seconds
FROM (
  SELECT name,
  finish_time,
  division,
  LAST_VALUE(finish_time)
    OVER (PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS slowest_time
  FROM finishers);

/*-----------------+-------------+----------+--------------+------------------*
 | name            | finish_time | division | slowest_time | delta_in_seconds |
 +-----------------+-------------+----------+--------------+------------------+
 | Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | 0                |
 | Sophia Liu      | 02:51:45    | F30-34   | 03:10:14     | 1109             |
 | Nikki Leith     | 02:59:01    | F30-34   | 03:10:14     | 673              |
 | Jen Edwards     | 03:06:36    | F30-34   | 03:10:14     | 218              |
 | Meghan Lederer  | 03:07:41    | F30-34   | 03:10:14     | 153              |
 | Lauren Reasoner | 03:10:14    | F30-34   | 03:10:14     | 0                |
 | Lisa Stelzner   | 02:54:11    | F35-39   | 03:06:24     | 733              |
 | Lauren Matthews | 03:01:17    | F35-39   | 03:06:24     | 307              |
 | Desiree Berry   | 03:05:42    | F35-39   | 03:06:24     | 42               |
 | Suzy Slane      | 03:06:24    | F35-39   | 03:06:24     | 0                |
 *-----------------+-------------+----------+--------------+------------------*/
```

### `LEAD`

```sql
LEAD (value_expression[, offset [, default_expression]])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]

```

**Description**

Returns the value of the `value_expression` on a subsequent row. Changing the
`offset` value changes which subsequent row is returned; the default value is
`1`, indicating the next row in the window frame. An error occurs if `offset` is
NULL or a negative value.

The optional `default_expression` is used if there isn't a row in the window
frame at the specified offset. This expression must be a constant expression and
its type must be implicitly coercible to the type of `value_expression`. If left
unspecified, `default_expression` defaults to NULL.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example illustrates a basic use of the `LEAD` function.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS followed_by
FROM finishers;

/*-----------------+-------------+----------+-----------------*
 | name            | finish_time | division | followed_by     |
 +-----------------+-------------+----------+-----------------+
 | Carly Forte     | 03:08:58    | F25-29   | NULL            |
 | Sophia Liu      | 02:51:45    | F30-34   | Nikki Leith     |
 | Nikki Leith     | 02:59:01    | F30-34   | Jen Edwards     |
 | Jen Edwards     | 03:06:36    | F30-34   | Meghan Lederer  |
 | Meghan Lederer  | 03:07:41    | F30-34   | Lauren Reasoner |
 | Lauren Reasoner | 03:10:14    | F30-34   | NULL            |
 | Lisa Stelzner   | 02:54:11    | F35-39   | Lauren Matthews |
 | Lauren Matthews | 03:01:17    | F35-39   | Desiree Berry   |
 | Desiree Berry   | 03:05:42    | F35-39   | Suzy Slane      |
 | Suzy Slane      | 03:06:24    | F35-39   | NULL            |
 *-----------------+-------------+----------+-----------------*/
```

This next example uses the optional `offset` parameter.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers;

/*-----------------+-------------+----------+------------------*
 | name            | finish_time | division | two_runners_back |
 +-----------------+-------------+----------+------------------+
 | Carly Forte     | 03:08:58    | F25-29   | NULL             |
 | Sophia Liu      | 02:51:45    | F30-34   | Jen Edwards      |
 | Nikki Leith     | 02:59:01    | F30-34   | Meghan Lederer   |
 | Jen Edwards     | 03:06:36    | F30-34   | Lauren Reasoner  |
 | Meghan Lederer  | 03:07:41    | F30-34   | NULL             |
 | Lauren Reasoner | 03:10:14    | F30-34   | NULL             |
 | Lisa Stelzner   | 02:54:11    | F35-39   | Desiree Berry    |
 | Lauren Matthews | 03:01:17    | F35-39   | Suzy Slane       |
 | Desiree Berry   | 03:05:42    | F35-39   | NULL             |
 | Suzy Slane      | 03:06:24    | F35-39   | NULL             |
 *-----------------+-------------+----------+------------------*/
```

The following example replaces NULL values with a default value.

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name, 2, 'Nobody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers;

/*-----------------+-------------+----------+------------------*
 | name            | finish_time | division | two_runners_back |
 +-----------------+-------------+----------+------------------+
 | Carly Forte     | 03:08:58    | F25-29   | Nobody           |
 | Sophia Liu      | 02:51:45    | F30-34   | Jen Edwards      |
 | Nikki Leith     | 02:59:01    | F30-34   | Meghan Lederer   |
 | Jen Edwards     | 03:06:36    | F30-34   | Lauren Reasoner  |
 | Meghan Lederer  | 03:07:41    | F30-34   | Nobody           |
 | Lauren Reasoner | 03:10:14    | F30-34   | Nobody           |
 | Lisa Stelzner   | 02:54:11    | F35-39   | Desiree Berry    |
 | Lauren Matthews | 03:01:17    | F35-39   | Suzy Slane       |
 | Desiree Berry   | 03:05:42    | F35-39   | Nobody           |
 | Suzy Slane      | 03:06:24    | F35-39   | Nobody           |
 *-----------------+-------------+----------+------------------*/
```

### `NTH_VALUE`

```sql
NTH_VALUE (value_expression, constant_integer_expression [{RESPECT | IGNORE} NULLS])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  ORDER BY expression [ { ASC | DESC }  ] [, ...]
  [ window_frame_clause ]

```

**Description**

Returns the value of `value_expression` at the Nth row of the current window
frame, where Nth is defined by `constant_integer_expression`. Returns NULL if
there is no such row.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `constant_integer_expression` can be any constant expression that returns an
  integer.

**Return Data Type**

Same type as `value_expression`.

**Examples**

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
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', fastest_time) AS fastest_time,
  FORMAT_TIMESTAMP('%X', second_fastest) AS second_fastest
FROM (
  SELECT name,
  finish_time,
  division,finishers,
  FIRST_VALUE(finish_time)
    OVER w1 AS fastest_time,
  NTH_VALUE(finish_time, 2)
    OVER w1 as second_fastest
  FROM finishers
  WINDOW w1 AS (
    PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING));

/*-----------------+-------------+----------+--------------+----------------*
 | name            | finish_time | division | fastest_time | second_fastest |
 +-----------------+-------------+----------+--------------+----------------+
 | Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | NULL           |
 | Sophia Liu      | 02:51:45    | F30-34   | 02:51:45     | 02:59:01       |
 | Nikki Leith     | 02:59:01    | F30-34   | 02:51:45     | 02:59:01       |
 | Jen Edwards     | 03:06:36    | F30-34   | 02:51:45     | 02:59:01       |
 | Meghan Lederer  | 03:07:41    | F30-34   | 02:51:45     | 02:59:01       |
 | Lauren Reasoner | 03:10:14    | F30-34   | 02:51:45     | 02:59:01       |
 | Lisa Stelzner   | 02:54:11    | F35-39   | 02:54:11     | 03:01:17       |
 | Lauren Matthews | 03:01:17    | F35-39   | 02:54:11     | 03:01:17       |
 | Desiree Berry   | 03:05:42    | F35-39   | 02:54:11     | 03:01:17       |
 | Suzy Slane      | 03:06:24    | F35-39   | 02:54:11     | 03:01:17       |
 *-----------------+-------------+----------+--------------+----------------*/
```

### `PERCENTILE_CONT`

```sql
PERCENTILE_CONT (value_expression, percentile [{RESPECT | IGNORE} NULLS])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]

```

**Description**

Computes the specified percentile value for the value_expression, with linear
interpolation.

This function ignores NULL
values if
`RESPECT NULLS` is absent.  If `RESPECT NULLS` is present:

+ Interpolation between two `NULL` values returns `NULL`.
+ Interpolation between a `NULL` value and a non-`NULL` value returns the
  non-`NULL` value.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`PERCENTILE_CONT` can be used with differential privacy. To learn more, see
[Differentially private aggregate functions][dp-functions].

**Supported Argument Types**

+ `value_expression` and `percentile` must have one of the following types:
   + `NUMERIC`
   + `BIGNUMERIC`
   + `DOUBLE`
+ `percentile` must be a literal in the range `[0, 1]`.

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>DOUBLE</code></th>
</tr>
</thead>
<tbody>
<tr><th><code>NUMERIC</code></th><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>BIGNUMERIC</code></th><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
<tr><th><code>DOUBLE</code></th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td></tr>
</tbody>

</table>

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```sql
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

 /*-----+-------------+--------+--------------+-----*
  | min | percentile1 | median | percentile90 | max |
  +-----+-------------+--------+--------------+-----+
  | 0   | 0.03        | 1.5    | 2.7          | 3   |
  *-----+-------------+--------+--------------+-----+
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```sql
SELECT
  PERCENTILE_CONT(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_CONT(x, 0.01 RESPECT NULLS) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_CONT(x, 0.9 RESPECT NULLS) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

/*------+-------------+--------+--------------+-----*
 | min  | percentile1 | median | percentile90 | max |
 +------+-------------+--------+--------------+-----+
 | NULL | 0           | 1      | 2.6          | 3   |
 *------+-------------+--------+--------------+-----+
```

[dp-functions]: #aggregate-dp-functions

### `PERCENTILE_DISC`

```sql
PERCENTILE_DISC (value_expression, percentile [{RESPECT | IGNORE} NULLS])
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]

```

**Description**

Computes the specified percentile value for a discrete `value_expression`. The
returned value is the first sorted value of `value_expression` with cumulative
distribution greater than or equal to the given `percentile` value.

This function ignores `NULL`
values unless
`RESPECT NULLS` is present.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ `value_expression` can be any orderable type.
+ `percentile` must be a literal in the range `[0, 1]`, with one of the
  following types:
   + `NUMERIC`
   + `BIGNUMERIC`
   + `DOUBLE`

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```sql
SELECT
  x,
  PERCENTILE_DISC(x, 0) OVER() AS min,
  PERCENTILE_DISC(x, 0.5) OVER() AS median,
  PERCENTILE_DISC(x, 1) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

/*------+-----+--------+-----*
 | x    | min | median | max |
 +------+-----+--------+-----+
 | c    | a   | b      | c   |
 | NULL | a   | b      | c   |
 | b    | a   | b      | c   |
 | a    | a   | b      | c   |
 *------+-----+--------+-----*/
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```sql
SELECT
  x,
  PERCENTILE_DISC(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_DISC(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_DISC(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

/*------+------+--------+-----*
 | x    | min  | median | max |
 +------+------+--------+-----+
 | c    | NULL | a      | c   |
 | NULL | NULL | a      | c   |
 | b    | NULL | a      | c   |
 | a    | NULL | a      | c   |
 *------+------+--------+-----*/

```

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

## Hash functions

ZetaSQL supports the following hash functions.

### `FARM_FINGERPRINT`

```
FARM_FINGERPRINT(value)
```

**Description**

Computes the fingerprint of the `STRING` or `BYTES` input using the
`Fingerprint64` function from the
[open-source FarmHash library][hash-link-to-farmhash-github]. The output
of this function for a particular input will never change.

**Return type**

INT64

**Examples**

```sql
WITH example AS (
  SELECT 1 AS x, "foo" AS y, true AS z UNION ALL
  SELECT 2 AS x, "apple" AS y, false AS z UNION ALL
  SELECT 3 AS x, "" AS y, true AS z
)
SELECT
  *,
  FARM_FINGERPRINT(CONCAT(CAST(x AS STRING), y, CAST(z AS STRING)))
    AS row_fingerprint
FROM example;
/*---+-------+-------+----------------------*
 | x | y     | z     | row_fingerprint      |
 +---+-------+-------+----------------------+
 | 1 | foo   | true  | -1541654101129638711 |
 | 2 | apple | false | 2794438866806483259  |
 | 3 |       | true  | -4880158226897771312 |
 *---+-------+-------+----------------------*/
```

[hash-link-to-farmhash-github]: https://github.com/google/farmhash

### `FINGERPRINT` (DEPRECATED) 
<a id="fingerprint"></a>

```
FINGERPRINT(input)
```

**Description**

Computes the fingerprint of the `STRING`
or `BYTES` input using Fingerprint.

This function is deprecated. For better hash quality, use another fingerprint
hashing function.

**Return type**

UINT64

**Examples**

```sql
SELECT FINGERPRINT("Hello World") as fingerprint;

/*----------------------*
 | fingerprint          |
 +----------------------+
 | 4584092443788135411  |
 *----------------------*/
```

### `MD5`

```
MD5(input)
```

**Description**

Computes the hash of the input using the
[MD5 algorithm][hash-link-to-md5-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 16 bytes.

Warning: MD5 is no longer considered secure.
For increased security use another hashing function.

**Return type**

`BYTES`

**Example**

```sql
SELECT MD5("Hello World") as md5;

/*-------------------------------------------------*
 | md5                                             |
 +-------------------------------------------------+
 | \xb1\n\x8d\xb1d\xe0uA\x05\xb7\xa9\x9b\xe7.?\xe5 |
 *-------------------------------------------------*/
```

[hash-link-to-md5-wikipedia]: https://en.wikipedia.org/wiki/MD5

### `SHA1`

```
SHA1(input)
```

**Description**

Computes the hash of the input using the
[SHA-1 algorithm][hash-link-to-sha-1-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 20 bytes.

Warning: SHA1 is no longer considered secure.
For increased security, use another hashing function.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA1("Hello World") as sha1;

/*-----------------------------------------------------------*
 | sha1                                                      |
 +-----------------------------------------------------------+
 | \nMU\xa8\xd7x\xe5\x02/\xabp\x19w\xc5\xd8@\xbb\xc4\x86\xd0 |
 *-----------------------------------------------------------*/
```

[hash-link-to-sha-1-wikipedia]: https://en.wikipedia.org/wiki/SHA-1

### `SHA256`

```
SHA256(input)
```

**Description**

Computes the hash of the input using the
[SHA-256 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 32 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA256("Hello World") as sha256;
```

[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

### `SHA512`

```
SHA512(input)
```

**Description**

Computes the hash of the input using the
[SHA-512 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 64 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA512("Hello World") as sha512;
```

[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

## String functions

ZetaSQL supports string functions.
These string functions work on two different values:
`STRING` and `BYTES` data types. `STRING` values must be well-formed UTF-8.

Functions that return position values, such as [STRPOS][string-link-to-strpos],
encode those positions as `INT64`. The value `1`
refers to the first character (or byte), `2` refers to the second, and so on.
The value `0` indicates an invalid position. When working on `STRING` types, the
returned positions refer to character positions.

All string comparisons are done byte-by-byte, without regard to Unicode
canonical equivalence.

### `ASCII`

```sql
ASCII(value)
```

**Description**

Returns the ASCII code for the first character or byte in `value`. Returns
`0` if `value` is empty or the ASCII code is `0` for the first character
or byte.

**Return type**

`INT64`

**Examples**

```sql
SELECT ASCII('abcd') as A, ASCII('a') as B, ASCII('') as C, ASCII(NULL) as D;

/*-------+-------+-------+-------*
 | A     | B     | C     | D     |
 +-------+-------+-------+-------+
 | 97    | 97    | 0     | NULL  |
 *-------+-------+-------+-------*/
```

### `BYTE_LENGTH`

```sql
BYTE_LENGTH(value)
```

**Description**

Returns the length of the `STRING` or `BYTES` value in `BYTES`,
regardless of whether the type of the value is `STRING` or `BYTES`.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters, b'абвгд' AS bytes)

SELECT
  characters,
  BYTE_LENGTH(characters) AS string_example,
  bytes,
  BYTE_LENGTH(bytes) AS bytes_example
FROM example;

/*------------+----------------+-------+---------------*
 | characters | string_example | bytes | bytes_example |
 +------------+----------------+-------+---------------+
 | абвгд      | 10             | абвгд | 10            |
 *------------+----------------+-------+---------------*/
```

### `CHAR_LENGTH`

```sql
CHAR_LENGTH(value)
```

**Description**

Returns the length of the `STRING` in characters.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  CHAR_LENGTH(characters) AS char_length_example
FROM example;

/*------------+---------------------*
 | characters | char_length_example |
 +------------+---------------------+
 | абвгд      |                   5 |
 *------------+---------------------*/
```

### `CHARACTER_LENGTH`

```sql
CHARACTER_LENGTH(value)
```

**Description**

Synonym for [CHAR_LENGTH][string-link-to-char-length].

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  CHARACTER_LENGTH(characters) AS char_length_example
FROM example;

/*------------+---------------------*
 | characters | char_length_example |
 +------------+---------------------+
 | абвгд      |                   5 |
 *------------+---------------------*/
```

[string-link-to-char-length]: #char_length

### `CHR`

```sql
CHR(value)
```

**Description**

Takes a Unicode [code point][string-link-to-code-points-wikipedia] and returns
the character that matches the code point. Each valid code point should fall
within the range of [0, 0xD7FF] and [0xE000, 0x10FFFF]. Returns an empty string
if the code point is `0`. If an invalid Unicode code point is specified, an
error is returned.

To work with an array of Unicode code points, see
[`CODE_POINTS_TO_STRING`][string-link-to-codepoints-to-string]

**Return type**

`STRING`

**Examples**

```sql
SELECT CHR(65) AS A, CHR(255) AS B, CHR(513) AS C, CHR(1024)  AS D;

/*-------+-------+-------+-------*
 | A     | B     | C     | D     |
 +-------+-------+-------+-------+
 | A     | ÿ     | ȁ     | Ѐ     |
 *-------+-------+-------+-------*/
```

```sql
SELECT CHR(97) AS A, CHR(0xF9B5) AS B, CHR(0) AS C, CHR(NULL) AS D;

/*-------+-------+-------+-------*
 | A     | B     | C     | D     |
 +-------+-------+-------+-------+
 | a     | 例    |       | NULL  |
 *-------+-------+-------+-------*/
```

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-codepoints-to-string]: #code_points_to_string

### `CODE_POINTS_TO_BYTES`

```sql
CODE_POINTS_TO_BYTES(ascii_code_points)
```

**Description**

Takes an array of extended ASCII
[code points][string-link-to-code-points-wikipedia]
as `ARRAY<INT64>` and returns `BYTES`.

To convert from `BYTES` to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`BYTES`

**Examples**

The following is a basic example using `CODE_POINTS_TO_BYTES`.

```sql
SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]) AS bytes;

/*----------*
 | bytes    |
 +----------+
 | AbCd     |
 *----------*/
```

The following example uses a rotate-by-13 places (ROT13) algorithm to encode a
string.

```sql
SELECT CODE_POINTS_TO_BYTES(ARRAY_AGG(
  (SELECT
      CASE
        WHEN chr BETWEEN b'a' and b'z'
          THEN TO_CODE_POINTS(b'a')[offset(0)] +
            MOD(code+13-TO_CODE_POINTS(b'a')[offset(0)],26)
        WHEN chr BETWEEN b'A' and b'Z'
          THEN TO_CODE_POINTS(b'A')[offset(0)] +
            MOD(code+13-TO_CODE_POINTS(b'A')[offset(0)],26)
        ELSE code
      END
   FROM
     (SELECT code, CODE_POINTS_TO_BYTES([code]) chr)
  ) ORDER BY OFFSET)) AS encoded_string
FROM UNNEST(TO_CODE_POINTS(b'Test String!')) code WITH OFFSET;

/*------------------*
 | encoded_string   |
 +------------------+
 | Grfg Fgevat!     |
 *------------------*/
```

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-code-points]: #to_code_points

### `CODE_POINTS_TO_STRING`

```sql
CODE_POINTS_TO_STRING(unicode_code_points)
```

**Description**

Takes an array of Unicode [code points][string-link-to-code-points-wikipedia]
as `ARRAY<INT64>` and returns a `STRING`.

To convert from a string to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`STRING`

**Examples**

The following are basic examples using `CODE_POINTS_TO_STRING`.

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024]) AS string;

/*--------*
 | string |
 +--------+
 | AÿȁЀ   |
 *--------*/
```

```sql
SELECT CODE_POINTS_TO_STRING([97, 0, 0xF9B5]) AS string;

/*--------*
 | string |
 +--------+
 | a例    |
 *--------*/
```

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, NULL, 1024]) AS string;

/*--------*
 | string |
 +--------+
 | NULL   |
 *--------*/
```

The following example computes the frequency of letters in a set of words.

```sql
WITH Words AS (
  SELECT word
  FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama']) AS word
)
SELECT
  CODE_POINTS_TO_STRING([code_point]) AS letter,
  COUNT(*) AS letter_count
FROM Words,
  UNNEST(TO_CODE_POINTS(word)) AS code_point
GROUP BY 1
ORDER BY 2 DESC;

/*--------+--------------*
 | letter | letter_count |
 +--------+--------------+
 | a      | 5            |
 | f      | 3            |
 | r      | 2            |
 | b      | 2            |
 | l      | 2            |
 | o      | 2            |
 | g      | 1            |
 | z      | 1            |
 | e      | 1            |
 | m      | 1            |
 | i      | 1            |
 *--------+--------------*/
```

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-code-points]: #to_code_points

### `COLLATE`

```sql
COLLATE(value, collate_specification)
```

Takes a `STRING` and a [collation specification][link-collation-spec]. Returns
a `STRING` with a collation specification. If `collate_specification` is empty,
returns a value with collation removed from the `STRING`.

The collation specification defines how the resulting `STRING` can be compared
and sorted. To learn more, see
[Working with collation][link-collation-concepts].

+ `collation_specification` must be a string literal, otherwise an error is
  thrown.
+ Returns `NULL` if `value` is `NULL`.

**Return type**

`STRING`

**Examples**

In this example, the weight of `a` is less than the weight of `Z`. This
is because the collate specification, `und:ci` assigns more weight to `Z`.

```sql
WITH Words AS (
  SELECT
    COLLATE('a', 'und:ci') AS char1,
    COLLATE('Z', 'und:ci') AS char2
)
SELECT ( Words.char1 < Words.char2 ) AS a_less_than_Z
FROM Words;

/*----------------*
 | a_less_than_Z  |
 +----------------+
 | TRUE           |
 *----------------*/
```

In this example, the weight of `a` is greater than the weight of `Z`. This
is because the default collate specification assigns more weight to `a`.

```sql
WITH Words AS (
  SELECT
    'a' AS char1,
    'Z' AS char2
)
SELECT ( Words.char1 < Words.char2 ) AS a_less_than_Z
FROM Words;

/*----------------*
 | a_less_than_Z  |
 +----------------+
 | FALSE          |
 *----------------*/
```

[link-collation-spec]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_spec_details

[link-collation-concepts]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#working_with_collation

### `CONCAT`

```sql
CONCAT(value1[, ...])
```

**Description**

Concatenates one or more values into a single result. All values must be
`BYTES` or data types that can be cast to `STRING`.

The function returns `NULL` if any input argument is `NULL`.

Note: You can also use the
[|| concatenation operator][string-link-to-operators] to concatenate
values into a string.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT CONCAT('T.P.', ' ', 'Bar') as author;

/*---------------------*
 | author              |
 +---------------------+
 | T.P. Bar            |
 *---------------------*/
```

```sql
SELECT CONCAT('Summer', ' ', 1923) as release_date;

/*---------------------*
 | release_date        |
 +---------------------+
 | Summer 1923         |
 *---------------------*/
```

```sql

With Employees AS
  (SELECT
    'John' AS first_name,
    'Doe' AS last_name
  UNION ALL
  SELECT
    'Jane' AS first_name,
    'Smith' AS last_name
  UNION ALL
  SELECT
    'Joe' AS first_name,
    'Jackson' AS last_name)

SELECT
  CONCAT(first_name, ' ', last_name)
  AS full_name
FROM Employees;

/*---------------------*
 | full_name           |
 +---------------------+
 | John Doe            |
 | Jane Smith          |
 | Joe Jackson         |
 *---------------------*/
```

[string-link-to-operators]: #operators

### `ENDS_WITH`

```sql
ENDS_WITH(value, suffix)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if `suffix`
is a suffix of `value`.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return type**

`BOOL`

**Examples**

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  ENDS_WITH(item, 'e') as example
FROM items;

/*---------*
 | example |
 +---------+
 |    True |
 |   False |
 |    True |
 *---------*/
```

### `FORMAT` 
<a id="format_string"></a>

```sql
FORMAT(format_string_expression, data_type_expression[, ...])
```

**Description**

`FORMAT` formats a data type expression as a string.

+ `format_string_expression`: Can contain zero or more
  [format specifiers][format-specifiers]. Each format specifier is introduced
  by the `%` symbol, and must map to one or more of the remaining arguments.
  In general, this is a one-to-one mapping, except when the `*` specifier is
  present. For example, `%.*i` maps to two arguments&mdash;a length argument
  and a signed integer argument.  If the number of arguments related to the
  format specifiers is not the same as the number of arguments, an error occurs.
+ `data_type_expression`: The value to format as a string. This can be any
  ZetaSQL data type.

**Return type**

`STRING`

**Examples**

<table>
<tr>
<th>Description</th>
<th>Statement</th>
<th>Result</th>
</tr>
<tr>
<td>Simple integer</td>
<td>FORMAT('%d', 10)</td>
<td>10</td>
</tr>
<tr>
<td>Integer with left blank padding</td>
<td>FORMAT('|%10d|', 11)</td>
<td>|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;11|</td>
</tr>
<tr>
<td>Integer with left zero padding</td>
<td>FORMAT('+%010d+', 12)</td>
<td>+0000000012+</td>
</tr>
<tr>
<td>Integer with commas</td>
<td>FORMAT("%'d", 123456789)</td>
<td>123,456,789</td>
</tr>
<tr>
<td>STRING</td>
<td>FORMAT('-%s-', 'abcd efg')</td>
<td>-abcd efg-</td>
</tr>
<tr>
<td>DOUBLE</td>
<td>FORMAT('%f %E', 1.1, 2.2)</td>
<td>1.100000&nbsp;2.200000E+00</td>
</tr>

<tr>
<td>DATE</td>
<td>FORMAT('%t', date '2015-09-01')</td>
<td>2015-09-01</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td>FORMAT('%t', timestamp '2015-09-01 12:34:56
America/Los_Angeles')</td>
<td>2015&#8209;09&#8209;01&nbsp;19:34:56+00</td>
</tr>
</table>

The `FORMAT()` function does not provide fully customizable formatting for all
types and values, nor formatting that is sensitive to locale.

If custom formatting is necessary for a type, you must first format it using
type-specific format functions, such as `FORMAT_DATE()` or `FORMAT_TIMESTAMP()`.
For example:

```sql
SELECT FORMAT('date: %s!', FORMAT_DATE('%B %d, %Y', date '2015-01-02'));
```

Returns

```
date: January 02, 2015!
```

#### Supported format specifiers 
<a id="format_specifiers"></a>

```
%[flags][width][.precision]specifier
```

A [format specifier][format-specifier-list] adds formatting when casting a
value to a string. It can optionally contain these sub-specifiers:

+ [Flags][flags]
+ [Width][width]
+ [Precision][precision]

Additional information about format specifiers:

+ [%g and %G behavior][g-and-g-behavior]
+ [%p and %P behavior][p-and-p-behavior]
+ [%t and %T behavior][t-and-t-behavior]
+ [Error conditions][error-format-specifiers]
+ [NULL argument handling][null-format-specifiers]
+ [Additional semantic rules][rules-format-specifiers]

##### Format specifiers 
<a id="format_specifier_list"></a>

<table>
 <tr>
    <td>Specifier</td>
    <td>Description</td>
    <td width="200px">Examples</td>
    <td>Types</td>
 </tr>
 <tr>
    <td><code>d</code> or <code>i</code></td>
    <td>Decimal integer</td>
    <td>392</td>
    <td>
    <span>INT32</span><br><span>INT64</span><br><span>UINT32</span><br><span>UINT64</span><br>
    </td>
 </tr>

 <tr>
    <td><code>u</code></td>
    <td>Unsigned integer</td>
    <td>7235</td>
    <td>
    <span>UINT32</span><br><span>UINT64</span><br>
    </td>
 </tr>

 <tr>
    <td><code>o</code></td>
    <td>Octal</td>
    <td>610</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>x</code></td>
    <td>Hexadecimal integer</td>
    <td>7fa</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>X</code></td>
    <td>Hexadecimal integer (uppercase)</td>
    <td>7FA</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>f</code></td>
    <td>Decimal notation, in [-](integer part).(fractional part) for finite
        values, and in lowercase for non-finite values</td>
    <td>392.650000<br/>
    inf<br/>
    nan</td>
    <td>
    <span> NUMERIC</span><br><span> BIGNUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>F</code></td>
    <td>Decimal notation, in [-](integer part).(fractional part) for finite
        values, and in uppercase for non-finite values</td>
    <td>392.650000<br/>
    INF<br/>
    NAN</td>
    <td>
    <span> NUMERIC</span><br><span> BIGNUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>e</code></td>
    <td>Scientific notation (mantissa/exponent), lowercase</td>
    <td>3.926500e+02<br/>
    inf<br/>
    nan</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>E</code></td>
    <td>Scientific notation (mantissa/exponent), uppercase</td>
    <td>3.926500E+02<br/>
    INF<br/>
    NAN</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>g</code></td>
    <td>Either decimal notation or scientific notation, depending on the input
        value's exponent and the specified precision. Lowercase.
        See <a href="#g_and_g_behavior">%g and %G behavior</a> for details.</td>
    <td>392.65<br/>
      3.9265e+07<br/>
    inf<br/>
    nan</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>G</code></td>
    <td>
      Either decimal notation or scientific notation, depending on the input
      value's exponent and the specified precision. Uppercase.
      See <a href="#g_and_g_behavior">%g and %G behavior</a> for details.
    </td>
    <td>
      392.65<br/>
      3.9265E+07<br/>
      INF<br/>
      NAN
    </td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>

 <tr>
    <td><code>p</code></td>
    <td>
      
      Produces a one-line printable string representing a protocol buffer
      or JSON.
      
      See <a href="#p_and_p_behavior">%p and %P behavior</a>.
    </td>
    <td>
      
<pre>year: 2019 month: 10</pre>
      
      
<pre>{"month":10,"year":2019}</pre>
      
    </td>
    <td>
      PROTO
      <br />
      JSON
    </td>
 </tr>

  <tr>
    <td><code>P</code></td>
    <td>
      
      Produces a multi-line printable string representing a protocol buffer
      or JSON.
      
      See <a href="#p_and_p_behavior">%p and %P behavior</a>.
    </td>
    <td>
      
<pre>
year: 2019
month: 10
</pre>
      
      
<pre>
{
  "month": 10,
  "year": 2019
}
</pre>
      
    </td>
    <td>
      PROTO
      <br />
      JSON
    </td>
  </tr>

 <tr>
    <td><code>s</code></td>
    <td>String of characters</td>
    <td>sample</td>
    <td>STRING</td>
 </tr>
 <tr>
    <td><code>t</code></td>
    <td>
      Returns a printable string representing the value. Often looks
      similar to casting the argument to <code>STRING</code>.
      See <a href="#t_and_t_behavior">%t and %T behavior</a>.
    </td>
    <td>
      sample<br/>
      2014&#8209;01&#8209;01
    </td>
    <td>&lt;any&gt;</td>
 </tr>
 <tr>
    <td><code>T</code></td>
    <td>
      Produces a string that is a valid ZetaSQL constant with a
      similar type to the value's type (maybe wider, or maybe string).
      See <a href="#t_and_t_behavior">%t and %T behavior</a>.
    </td>
    <td>
      'sample'<br/>
      b'bytes&nbsp;sample'<br/>
      1234<br/>
      2.3<br/>
      date&nbsp;'2014&#8209;01&#8209;01'
    </td>
    <td>&lt;any&gt;</td>
 </tr>
 <tr>
    <td><code>%</code></td>
    <td>'%%' produces a single '%'</td>
    <td>%</td>
    <td>n/a</td>
 </tr>
</table>

<a id="oxX"></a><sup>*</sup>The specifiers `%o`, `%x`, and `%X` raise an
error if negative values are used.

The format specifier can optionally contain the sub-specifiers identified above
in the specifier prototype.

These sub-specifiers must comply with the following specifications.

##### Flags 
<a id="flags"></a>

<table>
 <tr>
    <td>Flags</td>
    <td>Description</td>
 </tr>
 <tr>
    <td><code>-</code></td>
    <td>Left-justify within the given field width; Right justification is the
default (see width sub-specifier)</td>
</tr>
 <tr>
    <td><code>+</code></td>
    <td>Forces to precede the result with a plus or minus sign (<code>+</code>
or <code>-</code>) even for positive numbers. By default, only negative numbers
are preceded with a <code>-</code> sign</td>
</tr>
 <tr>
    <td>&lt;space&gt;</td>
    <td>If no sign is going to be written, a blank space is inserted before the
value</td>
</tr>
 <tr>
    <td><code>#</code></td>
    <td><ul>
      <li>For `%o`, `%x`, and `%X`, this flag means to precede the
          value with 0, 0x or 0X respectively for values different than zero.</li>
      <li>For `%f`, `%F`, `%e`, and `%E`, this flag means to add the decimal
          point even when there is no fractional part, unless the value
          is non-finite.</li>
      <li>For `%g` and `%G`, this flag means to add the decimal point even
          when there is no fractional part unless the value is non-finite, and
          never remove the trailing zeros after the decimal point.</li>
      </ul>
   </td>
 </tr>
 <tr>
    <td><code>0</code></td>
    <td>
      Left-pads the number with zeroes (0) instead of spaces when padding is
      specified (see width sub-specifier)</td>
 </tr>
 <tr>
  <td><code>'</code></td>
  <td>
    <p>Formats integers using the appropriating grouping character.
       For example:</p>
    <ul>
      <li><code>FORMAT("%'d", 12345678)</code> returns <code>12,345,678</code></li>
      <li><code>FORMAT("%'x", 12345678)</code> returns <code>bc:614e</code></li>
      <li><code>FORMAT("%'o", 55555)</code> returns <code>15,4403</code></li>
      <p>This flag is only relevant for decimal, hex, and octal values.</p>
    </ul>
  </td>
 </tr>
</table>

Flags may be specified in any order. Duplicate flags are not an error. When
flags are not relevant for some element type, they are ignored.

##### Width 
<a id="width"></a>

<table>
  <tr>
    <td>Width</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>&lt;number&gt;</td>
    <td>
      Minimum number of characters to be printed. If the value to be printed
      is shorter than this number, the result is padded with blank spaces.
      The value is not truncated even if the result is larger
    </td>
  </tr>
  <tr>
    <td><code>*</code></td>
    <td>
      The width is not specified in the format string, but as an additional
      integer value argument preceding the argument that has to be formatted
    </td>
  </tr>
</table>

##### Precision 
<a id="precision"></a>

<table>
 <tr>
    <td>Precision</td>
    <td>Description</td>
 </tr>
 <tr>
    <td><code>.</code>&lt;number&gt;</td>
    <td>
      <ul>
      <li>For integer specifiers `%d`, `%i`, `%o`, `%u`, `%x`, and `%X`:
          precision specifies the
          minimum number of digits to be written. If the value to be written is
          shorter than this number, the result is padded with trailing zeros.
          The value is not truncated even if the result is longer. A precision
          of 0 means that no character is written for the value 0.</li>
      <li>For specifiers `%a`, `%A`, `%e`, `%E`, `%f`, and `%F`: this is the
          number of digits to be printed after the decimal point. The default
          value is 6.</li>
      <li>For specifiers `%g` and `%G`: this is the number of significant digits
          to be printed, before the removal of the trailing zeros after the
          decimal point. The default value is 6.</li>
      </ul>
   </td>
</tr>
 <tr>
    <td><code>.*</code></td>
    <td>
      The precision is not specified in the format string, but as an
      additional integer value argument preceding the argument that has to be
      formatted
   </td>
  </tr>
</table>

##### %g and %G behavior 
<a id="g_and_g_behavior"></a>
The `%g` and `%G` format specifiers choose either the decimal notation (like
the `%f` and `%F` specifiers) or the scientific notation (like the `%e` and `%E`
specifiers), depending on the input value's exponent and the specified
[precision](#precision).

Let p stand for the specified [precision](#precision) (defaults to 6; 1 if the
specified precision is less than 1). The input value is first converted to
scientific notation with precision = (p - 1). If the resulting exponent part x
is less than -4 or no less than p, the scientific notation with precision =
(p - 1) is used; otherwise the decimal notation with precision = (p - 1 - x) is
used.

Unless [`#` flag](#flags) is present, the trailing zeros after the decimal point
are removed, and the decimal point is also removed if there is no digit after
it.

##### %p and %P behavior 
<a id="p_and_p_behavior"></a>

The `%p` format specifier produces a one-line printable string. The `%P`
format specifier produces a multi-line printable string. You can use these
format specifiers with the following data types:

<table>
  <tr>
    <td><strong>Type</strong></td>
    <td><strong>%p</strong></td>
    <td><strong>%P</strong></td>
  </tr>
 
  <tr>
    <td>PROTO</td>
    <td valign="top">
      <p>PROTO input:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
      <p>Produces a one-line printable string representing a protocol buffer:</p>
      <pre>year: 2019 month: 10</pre>
    </td>
    <td valign="top">
      <p>PROTO input:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
      <p>Produces a multi-line printable string representing a protocol buffer:</p>
<pre>
year: 2019
month: 10
</pre>
    </td>
  </tr>
 
 
  <tr>
    <td>JSON</td>
    <td valign="top">
      <p>JSON input:</p>
<pre>
JSON '
{
  "month": 10,
  "year": 2019
}
'</pre>
      <p>Produces a one-line printable string representing JSON:</p>
      <pre>{"month":10,"year":2019}</pre>
    </td>
    <td valign="top">
      <p>JSON input:</p>
<pre>
JSON '
{
  "month": 10,
  "year": 2019
}
'</pre>
      <p>Produces a multi-line printable string representing JSON:</p>
<pre>
{
  "month": 10,
  "year": 2019
}
</pre>
    </td>
  </tr>
 
</table>

##### %t and %T behavior 
<a id="t_and_t_behavior"></a>

The `%t` and `%T` format specifiers are defined for all types. The
[width](#width), [precision](#precision), and [flags](#flags) act as they do
for `%s`: the [width](#width) is the minimum width and the `STRING` will be
padded to that size, and [precision](#precision) is the maximum width
of content to show and the `STRING` will be truncated to that size, prior to
padding to width.

The `%t` specifier is always meant to be a readable form of the value.

The `%T` specifier is always a valid SQL literal of a similar type, such as a
wider numeric type.
The literal will not include casts or a type name, except for the special case
of non-finite floating point values.

The `STRING` is formatted as follows:

<table>
  <tr>
    <td><strong>Type</strong></td>
    <td><strong>%t</strong></td>
    <td><strong>%T</strong></td>
  </tr>
  <tr>
    <td><code>NULL</code> of any type</td>
    <td>NULL</td>
    <td>NULL</td>
  </tr>
  <tr>
    <td><span> INT32</span><br><span> INT64</span><br><span> UINT32</span><br><span> UINT64</span><br></td>
    <td>123</td>
    <td>123</td>
  </tr>

  <tr>
    <td>NUMERIC</td>
    <td>123.0  <em>(always with .0)</em>
    <td>NUMERIC "123.0"</td>
  </tr>
  <tr>

    <td>FLOAT, DOUBLE</td>

    <td>
      123.0  <em>(always with .0)</em><br/>
      123e+10<br><code>inf</code><br><code>-inf</code><br><code>NaN</code>
    </td>
    <td>
      123.0  <em>(always with .0)</em><br/>
      123e+10<br/>
      CAST("inf" AS &lt;type&gt;)<br/>
      CAST("-inf" AS &lt;type&gt;)<br/>
      CAST("nan" AS &lt;type&gt;)
    </td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>unquoted string value</td>
    <td>quoted string literal</td>
  </tr>
  <tr>
    <td>BYTES</td>
    <td>
      unquoted escaped bytes<br/>
      e.g. abc\x01\x02
    </td>
    <td>
      quoted bytes literal<br/>
      e.g. b"abc\x01\x02"
    </td>
  </tr>
  <tr>
    <td>BOOL</td>
    <td>boolean value</td>
    <td>boolean value</td>
  </tr>

  <tr>
    <td>ENUM</td>
    <td>EnumName</td>
    <td>"EnumName"</td>
  </tr>
 
 
  <tr>
    <td>DATE</td>
    <td>2011-02-03</td>
    <td>DATE "2011-02-03"</td>
  </tr>
 
 
  <tr>
    <td>TIMESTAMP</td>
    <td>2011-02-03 04:05:06+00</td>
    <td>TIMESTAMP "2011-02-03 04:05:06+00"</td>
  </tr>

 
  <tr>
    <td>INTERVAL</td>
    <td>1-2 3 4:5:6.789</td>
    <td>INTERVAL "1-2 3 4:5:6.789" YEAR TO SECOND</td>
  </tr>
 
 
  <tr>
    <td>PROTO</td>
    <td>
      one-line printable string representing a protocol buffer.
    </td>
    <td>
      quoted string literal with one-line printable string representing a
      protocol buffer.
    </td>
  </tr>
 
  <tr>
    <td>ARRAY</td>
    <td>[value, value, ...]<br/>
    where values are formatted with %t</td>
    <td>[value, value, ...]<br/>
    where values are formatted with %T</td>
  </tr>
 
  <tr>
    <td>STRUCT</td>
    <td>(value, value, ...)<br/>
    where fields are formatted with %t</td>
    <td>(value, value, ...)<br/>
    where fields are formatted with %T<br/>
    <br/>
    Special cases:<br/>
    Zero fields: STRUCT()<br/>
    One field: STRUCT(value)</td>
  </tr>
  
 
  <tr>
    <td>JSON</td>
    <td>
      one-line printable string representing JSON.<br />
      <pre class="lang-json prettyprint">{"name":"apple","stock":3}</pre>
    </td>
    <td>
      one-line printable string representing a JSON literal.<br />
      <pre class="lang-sql prettyprint">JSON '{"name":"apple","stock":3}'</pre>
    </td>
  </tr>
 
</table>

##### Error conditions 
<a id="error_format_specifiers"></a>

If a format specifier is invalid, or is not compatible with the related
argument type, or the wrong number or arguments are provided, then an error is
produced.  For example, the following `<format_string>` expressions are invalid:

```sql
FORMAT('%s', 1)
```

```sql
FORMAT('%')
```

##### NULL argument handling 
<a id="null_format_specifiers"></a>

A `NULL` format string results in a `NULL` output `STRING`. Any other arguments
are ignored in this case.

The function generally produces a `NULL` value if a `NULL` argument is present.
For example, `FORMAT('%i', NULL_expression)` produces a `NULL STRING` as
output.

However, there are some exceptions: if the format specifier is %t or %T
(both of which produce `STRING`s that effectively match CAST and literal value
semantics), a `NULL` value produces 'NULL' (without the quotes) in the result
`STRING`. For example, the function:

```sql
FORMAT('00-%t-00', NULL_expression);
```

Returns

```sql
00-NULL-00
```

##### Additional semantic rules 
<a id="rules_format_specifiers"></a>

`DOUBLE` and
`FLOAT` values can be `+/-inf` or `NaN`.
When an argument has one of those values, the result of the format specifiers
`%f`, `%F`, `%e`, `%E`, `%g`, `%G`, and `%t` are `inf`, `-inf`, or `nan`
(or the same in uppercase) as appropriate.  This is consistent with how
ZetaSQL casts these values to `STRING`.  For `%T`,
ZetaSQL returns quoted strings for
`DOUBLE` values that don't have non-string literal
representations.

[format-specifiers]: #format_specifiers

[format-specifier-list]: #format_specifier_list

[flags]: #flags

[width]: #width

[precision]: #precision

[g-and-g-behavior]: #g_and_g_behavior

[p-and-p-behavior]: #p_and_p_behavior

[t-and-t-behavior]: #t_and_t_behavior

[error-format-specifiers]: #error_format_specifiers

[null-format-specifiers]: #null_format_specifiers

[rules-format-specifiers]: #rules_format_specifiers

### `FROM_BASE32`

```sql
FROM_BASE32(string_expr)
```

**Description**

Converts the base32-encoded input `string_expr` into `BYTES` format. To convert
`BYTES` to a base32-encoded `STRING`, use [TO_BASE32][string-link-to-base32].

**Return type**

`BYTES`

**Example**

```sql
SELECT FROM_BASE32('MFRGGZDF74======') AS byte_data;

/*-----------*
 | byte_data |
 +-----------+
 | abcde\xff |
 *-----------*/
```

[string-link-to-base32]: #to_base32

### `FROM_BASE64`

```sql
FROM_BASE64(string_expr)
```

**Description**

Converts the base64-encoded input `string_expr` into
`BYTES` format. To convert
`BYTES` to a base64-encoded `STRING`,
use [TO_BASE64][string-link-to-base64].

There are several base64 encodings in common use that vary in exactly which
alphabet of 65 ASCII characters are used to encode the 64 digits and padding.
See [RFC 4648][RFC-4648] for details. This
function expects the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`BYTES`

**Example**

```sql
SELECT FROM_BASE64('/+A=') AS byte_data;

/*-----------*
 | byte_data |
 +-----------+
 | \377\340  |
 *-----------*/
```

To work with an encoding using a different base64 alphabet, you might need to
compose `FROM_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To decode a
`base64url`-encoded string, replace `-` and `_` with `+` and `/` respectively.

```sql
SELECT FROM_BASE64(REPLACE(REPLACE('_-A=', '-', '+'), '_', '/')) AS binary;

/*-----------*
 | binary    |
 +-----------+
 | \377\340  |
 *-----------*/
```

[RFC-4648]: https://tools.ietf.org/html/rfc4648#section-4

[string-link-to-from-base64]: #from_base64

### `FROM_HEX`

```sql
FROM_HEX(string)
```

**Description**

Converts a hexadecimal-encoded `STRING` into `BYTES` format. Returns an error
if the input `STRING` contains characters outside the range
`(0..9, A..F, a..f)`. The lettercase of the characters does not matter. If the
input `STRING` has an odd number of characters, the function acts as if the
input has an additional leading `0`. To convert `BYTES` to a hexadecimal-encoded
`STRING`, use [TO_HEX][string-link-to-to-hex].

**Return type**

`BYTES`

**Example**

```sql
WITH Input AS (
  SELECT '00010203aaeeefff' AS hex_str UNION ALL
  SELECT '0AF' UNION ALL
  SELECT '666f6f626172'
)
SELECT hex_str, FROM_HEX(hex_str) AS bytes_str
FROM Input;

/*------------------+----------------------------------*
 | hex_str          | bytes_str                        |
 +------------------+----------------------------------+
 | 0AF              | \x00\xaf                         |
 | 00010203aaeeefff | \x00\x01\x02\x03\xaa\xee\xef\xff |
 | 666f6f626172     | foobar                           |
 *------------------+----------------------------------*/
```

[string-link-to-to-hex]: #to_hex

### `INITCAP`

```sql
INITCAP(value[, delimiters])
```

**Description**

Takes a `STRING` and returns it with the first character in each word in
uppercase and all other characters in lowercase. Non-alphabetic characters
remain the same.

`delimiters` is an optional string argument that is used to override the default
set of characters used to separate words. If `delimiters` is not specified, it
defaults to the following characters: \
`<whitespace> [ ] ( ) { } / | \ < > ! ? @ " ^ # $ & ~ _ , . : ; * % + -`

If `value` or `delimiters` is `NULL`, the function returns `NULL`.

**Return type**

`STRING`

**Examples**

```sql
WITH example AS
(
  SELECT 'Hello World-everyone!' AS value UNION ALL
  SELECT 'tHe dog BARKS loudly+friendly' AS value UNION ALL
  SELECT 'apples&oranges;&pears' AS value UNION ALL
  SELECT 'καθίσματα ταινιών' AS value
)
SELECT value, INITCAP(value) AS initcap_value FROM example

/*-------------------------------+-------------------------------*
 | value                         | initcap_value                 |
 +-------------------------------+-------------------------------+
 | Hello World-everyone!         | Hello World-Everyone!         |
 | tHe dog BARKS loudly+friendly | The Dog Barks Loudly+Friendly |
 | apples&oranges;&pears         | Apples&Oranges;&Pears         |
 | καθίσματα ταινιών             | Καθίσματα Ταινιών             |
 *-------------------------------+-------------------------------*/

WITH example AS
(
  SELECT 'hello WORLD!' AS value, '' AS delimiters UNION ALL
  SELECT 'καθίσματα ταιντιώ@ν' AS value, 'τ@' AS delimiters UNION ALL
  SELECT 'Apples1oranges2pears' AS value, '12' AS delimiters UNION ALL
  SELECT 'tHisEisEaESentence' AS value, 'E' AS delimiters
)
SELECT value, delimiters, INITCAP(value, delimiters) AS initcap_value FROM example;

/*----------------------+------------+----------------------*
 | value                | delimiters | initcap_value        |
 +----------------------+------------+----------------------+
 | hello WORLD!         |            | Hello world!         |
 | καθίσματα ταιντιώ@ν  | τ@         | ΚαθίσματΑ τΑιντΙώ@Ν  |
 | Apples1oranges2pears | 12         | Apples1Oranges2Pears |
 | tHisEisEaESentence   | E          | ThisEIsEAESentence   |
 *----------------------+------------+----------------------*/
```

### `INSTR`

```sql
INSTR(value, subvalue[, position[, occurrence]])
```

**Description**

Returns the lowest 1-based position of `subvalue` in `value`.
`value` and `subvalue` must be the same type, either
`STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`value`, otherwise it starts at `1`, which is the beginning of
`value`. If `position` is negative, the function searches backwards
from the end of `value`, with `-1` indicating the last character.
`position` is of type `INT64` and cannot be `0`.

If `occurrence` is specified, the search returns the position of a specific
instance of `subvalue` in `value`. If not specified, `occurrence`
defaults to `1` and returns the position of the first occurrence.
For `occurrence` > `1`, the function includes overlapping occurrences.
`occurrence` is of type `INT64` and must be positive.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

Returns `0` if:

+ No match is found.
+ If `occurrence` is greater than the number of matches found.
+ If `position` is greater than the length of `value`.

Returns `NULL` if:

+ Any input argument is `NULL`.

Returns an error if:

+ `position` is `0`.
+ `occurrence` is `0` or negative.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
(SELECT 'banana' as value, 'an' as subvalue, 1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as value, 'an' as subvalue, 1 as position, 2 as
occurrence UNION ALL
SELECT 'banana' as value, 'an' as subvalue, 1 as position, 3 as
occurrence UNION ALL
SELECT 'banana' as value, 'an' as subvalue, 3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as value, 'an' as subvalue, -1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as value, 'an' as subvalue, -3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as value, 'ann' as subvalue, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as value, 'oo' as subvalue, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as value, 'oo' as subvalue, 1 as position, 2 as
occurrence
)
SELECT value, subvalue, position, occurrence, INSTR(value,
subvalue, position, occurrence) AS instr
FROM example;

/*--------------+--------------+----------+------------+-------*
 | value        | subvalue     | position | occurrence | instr |
 +--------------+--------------+----------+------------+-------+
 | banana       | an           | 1        | 1          | 2     |
 | banana       | an           | 1        | 2          | 4     |
 | banana       | an           | 1        | 3          | 0     |
 | banana       | an           | 3        | 1          | 4     |
 | banana       | an           | -1       | 1          | 4     |
 | banana       | an           | -3       | 1          | 4     |
 | banana       | ann          | 1        | 1          | 0     |
 | helloooo     | oo           | 1        | 1          | 5     |
 | helloooo     | oo           | 1        | 2          | 6     |
 *--------------+--------------+----------+------------+-------*/
```

### `LEFT`

```sql
LEFT(value, length)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of the specified
number of leftmost characters or bytes from `value`. The `length` is an
`INT64` that specifies the length of the returned
value. If `value` is of type `BYTES`, `length` is the number of leftmost bytes
to return. If `value` is `STRING`, `length` is the number of leftmost characters
to return.

If `length` is 0, an empty `STRING` or `BYTES` value will be
returned. If `length` is negative, an error will be returned. If `length`
exceeds the number of characters or bytes from `value`, the original `value`
will be returned.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH examples AS
(SELECT 'apple' as example
UNION ALL
SELECT 'banana' as example
UNION ALL
SELECT 'абвгд' as example
)
SELECT example, LEFT(example, 3) AS left_example
FROM examples;

/*---------+--------------*
 | example | left_example |
 +---------+--------------+
 | apple   | app          |
 | banana  | ban          |
 | абвгд   | абв          |
 *---------+--------------*/
```

```sql
WITH examples AS
(SELECT b'apple' as example
UNION ALL
SELECT b'banana' as example
UNION ALL
SELECT b'\xab\xcd\xef\xaa\xbb' as example
)
SELECT example, LEFT(example, 3) AS left_example
FROM examples;

/*----------------------+--------------*
 | example              | left_example |
 +----------------------+--------------+
 | apple                | app          |
 | banana               | ban          |
 | \xab\xcd\xef\xaa\xbb | \xab\xcd\xef |
 *----------------------+--------------*/
```

### `LENGTH`

```sql
LENGTH(value)
```

**Description**

Returns the length of the `STRING` or `BYTES` value. The returned
value is in characters for `STRING` arguments and in bytes for the `BYTES`
argument.

**Return type**

`INT64`

**Examples**

```sql

WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  LENGTH(characters) AS string_example,
  LENGTH(CAST(characters AS BYTES)) AS bytes_example
FROM example;

/*------------+----------------+---------------*
 | characters | string_example | bytes_example |
 +------------+----------------+---------------+
 | абвгд      |              5 |            10 |
 *------------+----------------+---------------*/
```

### `LOWER`

```sql
LOWER(value)
```

**Description**

For `STRING` arguments, returns the original string with all alphabetic
characters in lowercase. Mapping between lowercase and uppercase is done
according to the
[Unicode Character Database][string-link-to-unicode-character-definitions]
without taking into account language-specific mappings.

For `BYTES` arguments, the argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql

WITH items AS
  (SELECT
    'FOO' as item
  UNION ALL
  SELECT
    'BAR' as item
  UNION ALL
  SELECT
    'BAZ' as item)

SELECT
  LOWER(item) AS example
FROM items;

/*---------*
 | example |
 +---------+
 | foo     |
 | bar     |
 | baz     |
 *---------*/
```

[string-link-to-unicode-character-definitions]: http://unicode.org/ucd/

### `LPAD`

```sql
LPAD(original_value, return_length[, pattern])
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value` prepended
with `pattern`. The `return_length` is an `INT64` that
specifies the length of the returned value. If `original_value` is of type
`BYTES`, `return_length` is the number of bytes. If `original_value` is
of type `STRING`, `return_length` is the number of characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `LPAD('hello world', 7);` returns `'hello w'`.

If `original_value`, `return_length`, or `pattern` is `NULL`, this function
returns `NULL`.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, len, FORMAT('%T', LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);

/*------+-----+----------*
 | t    | len | LPAD     |
 |------|-----|----------|
 | abc  | 5   | "  abc"  |
 | abc  | 2   | "ab"     |
 | 例子  | 4   | "  例子" |
 *------+-----+----------*/
```

```sql
SELECT t, len, pattern, FORMAT('%T', LPAD(t, len, pattern)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

/*------+-----+---------+--------------*
 | t    | len | pattern | LPAD         |
 |------|-----|---------|--------------|
 | abc  | 8   | def     | "defdeabc"   |
 | abc  | 5   | -       | "--abc"      |
 | 例子  | 5   | 中文    | "中文中例子"   |
 *------+-----+---------+--------------*/
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

/*-----------------+-----+------------------*
 | t               | len | LPAD             |
 |-----------------|-----|------------------|
 | b"abc"          | 5   | b"  abc"         |
 | b"abc"          | 2   | b"ab"            |
 | b"\xab\xcd\xef" | 4   | b" \xab\xcd\xef" |
 *-----------------+-----+------------------*/
```

```sql
SELECT
  FORMAT('%T', t) AS t,
  len,
  FORMAT('%T', pattern) AS pattern,
  FORMAT('%T', LPAD(t, len, pattern)) AS LPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);

/*-----------------+-----+---------+-------------------------*
 | t               | len | pattern | LPAD                    |
 |-----------------|-----|---------|-------------------------|
 | b"abc"          | 8   | b"def"  | b"defdeabc"             |
 | b"abc"          | 5   | b"-"    | b"--abc"                |
 | b"\xab\xcd\xef" | 5   | b"\x00" | b"\x00\x00\xab\xcd\xef" |
 *-----------------+-----+---------+-------------------------*/
```

### `LTRIM`

```sql
LTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes leading characters.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT '   apple   ' as item
  UNION ALL
  SELECT '   banana   ' as item
  UNION ALL
  SELECT '   orange   ' as item)

SELECT
  CONCAT('#', LTRIM(item), '#') as example
FROM items;

/*-------------*
 | example     |
 +-------------+
 | #apple   #  |
 | #banana   # |
 | #orange   # |
 *-------------*/
```

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  LTRIM(item, '*') as example
FROM items;

/*-----------*
 | example   |
 +-----------+
 | apple***  |
 | banana*** |
 | orange*** |
 *-----------*/
```

```sql
WITH items AS
  (SELECT 'xxxapplexxx' as item
  UNION ALL
  SELECT 'yyybananayyy' as item
  UNION ALL
  SELECT 'zzzorangezzz' as item
  UNION ALL
  SELECT 'xyzpearxyz' as item)
```

```sql
SELECT
  LTRIM(item, 'xyz') as example
FROM items;

/*-----------*
 | example   |
 +-----------+
 | applexxx  |
 | bananayyy |
 | orangezzz |
 | pearxyz   |
 *-----------*/
```

[string-link-to-trim]: #trim

### `NORMALIZE_AND_CASEFOLD`

```sql
NORMALIZE_AND_CASEFOLD(value[, normalization_mode])
```

**Description**

Takes a string value and returns it as a normalized string. If you do not
provide a normalization mode, `NFC` is used.

[Normalization][string-link-to-normalization-wikipedia] is used to ensure that
two strings are equivalent. Normalization is often used in situations in which
two strings render the same on the screen but have different Unicode code
points.

[Case folding][string-link-to-case-folding-wikipedia] is used for the caseless
comparison of strings. If you need to compare strings and case should not be
considered, use `NORMALIZE_AND_CASEFOLD`, otherwise use
[`NORMALIZE`][string-link-to-normalize].

`NORMALIZE_AND_CASEFOLD` supports four optional normalization modes:

| Value   | Name                                           | Description|
|---------|------------------------------------------------|------------|
| `NFC`   | Normalization Form Canonical Composition       | Decomposes and recomposes characters by canonical equivalence.|
| `NFKC`  | Normalization Form Compatibility Composition   | Decomposes characters by compatibility, then recomposes them by canonical equivalence.|
| `NFD`   | Normalization Form Canonical Decomposition     | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.|
| `NFKD`  | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

**Return type**

`STRING`

**Examples**

```sql
SELECT
  a, b,
  NORMALIZE(a) = NORMALIZE(b) as normalized,
  NORMALIZE_AND_CASEFOLD(a) = NORMALIZE_AND_CASEFOLD(b) as normalized_with_case_folding
FROM (SELECT 'The red barn' AS a, 'The Red Barn' AS b);

/*--------------+--------------+------------+------------------------------*
 | a            | b            | normalized | normalized_with_case_folding |
 +--------------+--------------+------------+------------------------------+
 | The red barn | The Red Barn | false      | true                         |
 *--------------+--------------+------------+------------------------------*/
```

```sql
WITH Strings AS (
  SELECT '\u2168' AS a, 'IX' AS b UNION ALL
  SELECT '\u0041\u030A', '\u00C5'
)
SELECT a, b,
  NORMALIZE_AND_CASEFOLD(a, NFD)=NORMALIZE_AND_CASEFOLD(b, NFD) AS nfd,
  NORMALIZE_AND_CASEFOLD(a, NFC)=NORMALIZE_AND_CASEFOLD(b, NFC) AS nfc,
  NORMALIZE_AND_CASEFOLD(a, NFKD)=NORMALIZE_AND_CASEFOLD(b, NFKD) AS nkfd,
  NORMALIZE_AND_CASEFOLD(a, NFKC)=NORMALIZE_AND_CASEFOLD(b, NFKC) AS nkfc
FROM Strings;

/*---+----+-------+-------+------+------*
 | a | b  | nfd   | nfc   | nkfd | nkfc |
 +---+----+-------+-------+------+------+
 | Ⅸ | IX | false | false | true | true |
 | Å | Å  | true  | true  | true | true |
 *---+----+-------+-------+------+------*/
```

[string-link-to-normalization-wikipedia]: https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization

[string-link-to-case-folding-wikipedia]: https://en.wikipedia.org/wiki/Letter_case#Case_folding

[string-link-to-normalize]: #normalize

### `NORMALIZE`

```sql
NORMALIZE(value[, normalization_mode])
```

**Description**

Takes a string value and returns it as a normalized string. If you do not
provide a normalization mode, `NFC` is used.

[Normalization][string-link-to-normalization-wikipedia] is used to ensure that
two strings are equivalent. Normalization is often used in situations in which
two strings render the same on the screen but have different Unicode code
points.

`NORMALIZE` supports four optional normalization modes:

| Value   | Name                                           | Description|
|---------|------------------------------------------------|------------|
| `NFC`   | Normalization Form Canonical Composition       | Decomposes and recomposes characters by canonical equivalence.|
| `NFKC`  | Normalization Form Compatibility Composition   | Decomposes characters by compatibility, then recomposes them by canonical equivalence.|
| `NFD`   | Normalization Form Canonical Decomposition     | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.|
| `NFKD`  | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

**Return type**

`STRING`

**Examples**

```sql
SELECT a, b, a = b as normalized
FROM (SELECT NORMALIZE('\u00ea') as a, NORMALIZE('\u0065\u0302') as b);

/*---+---+------------*
 | a | b | normalized |
 +---+---+------------+
 | ê | ê | true       |
 *---+---+------------*/
```
The following example normalizes different space characters.

```sql
WITH EquivalentNames AS (
  SELECT name
  FROM UNNEST([
      'Jane\u2004Doe',
      'John\u2004Smith',
      'Jane\u2005Doe',
      'Jane\u2006Doe',
      'John Smith']) AS name
)
SELECT
  NORMALIZE(name, NFKC) AS normalized_name,
  COUNT(*) AS name_count
FROM EquivalentNames
GROUP BY 1;

/*-----------------+------------*
 | normalized_name | name_count |
 +-----------------+------------+
 | John Smith      | 2          |
 | Jane Doe        | 3          |
 *-----------------+------------*/
```

[string-link-to-normalization-wikipedia]: https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization

### `OCTET_LENGTH`

```sql
OCTET_LENGTH(value)
```

Alias for [`BYTE_LENGTH`][byte-length].

[byte-length]: #byte_length

### `REGEXP_CONTAINS`

```sql
REGEXP_CONTAINS(value, regexp)
```

**Description**

Returns `TRUE` if `value` is a partial match for the regular expression,
`regexp`.

If the `regexp` argument is invalid, the function returns an error.

You can search for a full match by using `^` (beginning of text) and `$` (end of
text). Due to regular expression operator precedence, it is good practice to use
parentheses around everything between `^` and `$`.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`BOOL`

**Examples**

```sql
SELECT
  email,
  REGEXP_CONTAINS(email, r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+') AS is_valid
FROM
  (SELECT
    ['foo@example.com', 'bar@example.org', 'www.example.net']
    AS addresses),
  UNNEST(addresses) AS email;

/*-----------------+----------*
 | email           | is_valid |
 +-----------------+----------+
 | foo@example.com | true     |
 | bar@example.org | true     |
 | www.example.net | false    |
 *-----------------+----------*/

-- Performs a full match, using ^ and $. Due to regular expression operator
-- precedence, it is good practice to use parentheses around everything between ^
-- and $.
SELECT
  email,
  REGEXP_CONTAINS(email, r'^([\w.+-]+@foo\.com|[\w.+-]+@bar\.org)$')
    AS valid_email_address,
  REGEXP_CONTAINS(email, r'^[\w.+-]+@foo\.com|[\w.+-]+@bar\.org$')
    AS without_parentheses
FROM
  (SELECT
    ['a@foo.com', 'a@foo.computer', 'b@bar.org', '!b@bar.org', 'c@buz.net']
    AS addresses),
  UNNEST(addresses) AS email;

/*----------------+---------------------+---------------------*
 | email          | valid_email_address | without_parentheses |
 +----------------+---------------------+---------------------+
 | a@foo.com      | true                | true                |
 | a@foo.computer | false               | true                |
 | b@bar.org      | true                | true                |
 | !b@bar.org     | false               | true                |
 | c@buz.net      | false               | false               |
 *----------------+---------------------+---------------------*/
```

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

### `REGEXP_EXTRACT_ALL`

```sql
REGEXP_EXTRACT_ALL(value, regexp)
```

**Description**

Returns an array of all substrings of `value` that match the
[re2 regular expression][string-link-to-re2], `regexp`. Returns an empty array
if there is no match.

If the regular expression contains a capturing group (`(...)`), and there is a
match for that capturing group, that match is added to the results. If there
are multiple matches for a capturing group, the last match is added to the
results.

The `REGEXP_EXTRACT_ALL` function only returns non-overlapping matches. For
example, using this function to extract `ana` from `banana` returns only one
substring, not two.

Returns an error if:

+ The regular expression is invalid
+ The regular expression has more than one capturing group

**Return type**

`ARRAY<STRING>` or `ARRAY<BYTES>`

**Examples**

```sql
WITH code_markdown AS
  (SELECT 'Try `function(x)` or `function(y)`' as code)

SELECT
  REGEXP_EXTRACT_ALL(code, '`(.+?)`') AS example
FROM code_markdown;

/*----------------------------*
 | example                    |
 +----------------------------+
 | [function(x), function(y)] |
 *----------------------------*/
```

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

### `REGEXP_EXTRACT`

```sql
REGEXP_EXTRACT(value, regexp)
```

**Description**

Returns the first substring in `value` that matches the
[re2 regular expression][string-link-to-re2],
`regexp`. Returns `NULL` if there is no match.

If the regular expression contains a capturing group (`(...)`), and there is a
match for that capturing group, that match is returned. If there
are multiple matches for a capturing group, the last match is returned.

Returns an error if:

+ The regular expression is invalid
+ The regular expression has more than one capturing group

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'baz@example.net' as email)

SELECT
  REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+')
  AS user_name
FROM email_addresses;

/*-----------*
 | user_name |
 +-----------+
 | foo       |
 | bar       |
 | baz       |
 *-----------*/
```

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'baz@example.net' as email)

SELECT
  REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)')
  AS top_level_domain
FROM email_addresses;

/*------------------*
 | top_level_domain |
 +------------------+
 | com              |
 | org              |
 | net              |
 *------------------*/
```

```sql
WITH
  characters AS (
    SELECT 'ab' AS value, '.b' AS regex UNION ALL
    SELECT 'ab' AS value, '(.)b' AS regex UNION ALL
    SELECT 'xyztb' AS value, '(.)+b' AS regex UNION ALL
    SELECT 'ab' AS value, '(z)?b' AS regex
  )
SELECT value, regex, REGEXP_EXTRACT(value, regex) AS result FROM characters;

/*-------+---------+----------*
 | value | regex   | result   |
 +-------+---------+----------+
 | ab    | .b      | ab       |
 | ab    | (.)b    | a        |
 | xyztb | (.)+b   | t        |
 | ab    | (z)?b   | NULL     |
 *-------+---------+----------*/
```

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

### `REGEXP_INSTR`

```sql
REGEXP_INSTR(source_value, regexp [, position[, occurrence, [occurrence_position]]])
```

**Description**

Returns the lowest 1-based position of a regular expression, `regexp`, in
`source_value`. `source_value` and `regexp` must be the same type, either
`STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`source_value`, otherwise it starts at `1`, which is the beginning of
`source_value`. `position` is of type `INT64` and must be positive.

If `occurrence` is specified, the search returns the position of a specific
instance of `regexp` in `source_value`. If not specified, `occurrence` defaults
to `1` and returns the position of the first occurrence.  For `occurrence` > 1,
the function searches for the next, non-overlapping occurrence.
`occurrence` is of type `INT64` and must be positive.

You can optionally use `occurrence_position` to specify where a position
in relation to an `occurrence` starts. Your choices are:

+  `0`: Returns the start position of `occurrence`.
+  `1`: Returns the end position of `occurrence` + `1`. If the
   end of the occurrence is at the end of `source_value `,
   `LENGTH(source_value) + 1` is returned.

Returns `0` if:

+ No match is found.
+ If `occurrence` is greater than the number of matches found.
+ If `position` is greater than the length of `source_value`.
+ The regular expression is empty.

Returns `NULL` if:

+ `position` is `NULL`.
+ `occurrence` is `NULL`.

Returns an error if:

+ `position` is `0` or negative.
+ `occurrence` is `0` or negative.
+ `occurrence_position` is neither `0` nor `1`.
+ The regular expression is invalid.
+ The regular expression has more than one capturing group.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS (
  SELECT 'ab@cd-ef' AS source_value, '@[^-]*' AS regexp UNION ALL
  SELECT 'ab@d-ef', '@[^-]*' UNION ALL
  SELECT 'abc@cd-ef', '@[^-]*' UNION ALL
  SELECT 'abc-ef', '@[^-]*')
SELECT source_value, regexp, REGEXP_INSTR(source_value, regexp) AS instr
FROM example;

/*--------------+--------+-------*
 | source_value | regexp | instr |
 +--------------+--------+-------+
 | ab@cd-ef     | @[^-]* | 3     |
 | ab@d-ef      | @[^-]* | 3     |
 | abc@cd-ef    | @[^-]* | 4     |
 | abc-ef       | @[^-]* | 0     |
 *--------------+--------+-------*/
```

```sql
WITH example AS (
  SELECT 'a@cd-ef b@cd-ef' AS source_value, '@[^-]*' AS regexp, 1 AS position UNION ALL
  SELECT 'a@cd-ef b@cd-ef', '@[^-]*', 2 UNION ALL
  SELECT 'a@cd-ef b@cd-ef', '@[^-]*', 3 UNION ALL
  SELECT 'a@cd-ef b@cd-ef', '@[^-]*', 4)
SELECT
  source_value, regexp, position,
  REGEXP_INSTR(source_value, regexp, position) AS instr
FROM example;

/*-----------------+--------+----------+-------*
 | source_value    | regexp | position | instr |
 +-----------------+--------+----------+-------+
 | a@cd-ef b@cd-ef | @[^-]* | 1        | 2     |
 | a@cd-ef b@cd-ef | @[^-]* | 2        | 2     |
 | a@cd-ef b@cd-ef | @[^-]* | 3        | 10    |
 | a@cd-ef b@cd-ef | @[^-]* | 4        | 10    |
 *-----------------+--------+----------+-------*/
```

```sql
WITH example AS (
  SELECT 'a@cd-ef b@cd-ef c@cd-ef' AS source_value,
         '@[^-]*' AS regexp, 1 AS position, 1 AS occurrence UNION ALL
  SELECT 'a@cd-ef b@cd-ef c@cd-ef', '@[^-]*', 1, 2 UNION ALL
  SELECT 'a@cd-ef b@cd-ef c@cd-ef', '@[^-]*', 1, 3)
SELECT
  source_value, regexp, position, occurrence,
  REGEXP_INSTR(source_value, regexp, position, occurrence) AS instr
FROM example;

/*-------------------------+--------+----------+------------+-------*
 | source_value            | regexp | position | occurrence | instr |
 +-------------------------+--------+----------+------------+-------+
 | a@cd-ef b@cd-ef c@cd-ef | @[^-]* | 1        | 1          | 2     |
 | a@cd-ef b@cd-ef c@cd-ef | @[^-]* | 1        | 2          | 10    |
 | a@cd-ef b@cd-ef c@cd-ef | @[^-]* | 1        | 3          | 18    |
 *-------------------------+--------+----------+------------+-------*/
```

```sql
WITH example AS (
  SELECT 'a@cd-ef' AS source_value, '@[^-]*' AS regexp,
         1 AS position, 1 AS occurrence, 0 AS o_position UNION ALL
  SELECT 'a@cd-ef', '@[^-]*', 1, 1, 1)
SELECT
  source_value, regexp, position, occurrence, o_position,
  REGEXP_INSTR(source_value, regexp, position, occurrence, o_position) AS instr
FROM example;

/*--------------+--------+----------+------------+------------+-------*
 | source_value | regexp | position | occurrence | o_position | instr |
 +--------------+--------+----------+------------+------------+-------+
 | a@cd-ef      | @[^-]* | 1        | 1          | 0          | 2     |
 | a@cd-ef      | @[^-]* | 1        | 1          | 1          | 5     |
 *--------------+--------+----------+------------+------------+-------*/
```

### `REGEXP_MATCH`

<p class="caution"><strong>Deprecated.</strong> Use <a href="#regexp_contains">REGEXP_CONTAINS</a>.</p>

```sql
REGEXP_MATCH(value, regexp)
```

**Description**

Returns `TRUE` if `value` is a full match for the regular expression, `regexp`.

If the `regexp` argument is invalid, the function returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`BOOL`

**Examples**

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'notavalidemailaddress' as email)

SELECT
  email,
  REGEXP_MATCH(email,
               r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
               AS valid_email_address
FROM email_addresses;

/*-----------------------+---------------------*
 | email                 | valid_email_address |
 +-----------------------+---------------------+
 | foo@example.com       | true                |
 | bar@example.org       | true                |
 | notavalidemailaddress | false               |
 *-----------------------+---------------------*/
```

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

### `REGEXP_REPLACE`

```sql
REGEXP_REPLACE(value, regexp, replacement)
```

**Description**

Returns a `STRING` where all substrings of `value` that
match regular expression `regexp` are replaced with `replacement`.

You can use backslashed-escaped digits (\1 to \9) within the `replacement`
argument to insert text matching the corresponding parenthesized group in the
`regexp` pattern. Use \0 to refer to the entire matching text.

To add a backslash in your regular expression, you must first escape it. For
example, `SELECT REGEXP_REPLACE('abc', 'b(.)', 'X\\1');` returns `aXc`. You can
also use [raw strings][string-link-to-lexical-literals] to remove one layer of
escaping, for example `SELECT REGEXP_REPLACE('abc', 'b(.)', r'X\1');`.

The `REGEXP_REPLACE` function only replaces non-overlapping matches. For
example, replacing `ana` within `banana` results in only one replacement, not
two.

If the `regexp` argument is not a valid regular expression, this function
returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH markdown AS
  (SELECT '# Heading' as heading
  UNION ALL
  SELECT '# Another heading' as heading)

SELECT
  REGEXP_REPLACE(heading, r'^# ([a-zA-Z0-9\s]+$)', '<h1>\\1</h1>')
  AS html
FROM markdown;

/*--------------------------*
 | html                     |
 +--------------------------+
 | <h1>Heading</h1>         |
 | <h1>Another heading</h1> |
 *--------------------------*/
```

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

[string-link-to-lexical-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#string_and_bytes_literals

### `REPEAT`

```sql
REPEAT(original_value, repetitions)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value`, repeated.
The `repetitions` parameter specifies the number of times to repeat
`original_value`. Returns `NULL` if either `original_value` or `repetitions`
are `NULL`.

This function returns an error if the `repetitions` value is negative.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, n, REPEAT(t, n) AS REPEAT FROM UNNEST([
  STRUCT('abc' AS t, 3 AS n),
  ('例子', 2),
  ('abc', null),
  (null, 3)
]);

/*------+------+-----------*
 | t    | n    | REPEAT    |
 |------|------|-----------|
 | abc  | 3    | abcabcabc |
 | 例子 | 2    | 例子例子  |
 | abc  | NULL | NULL      |
 | NULL | 3    | NULL      |
 *------+------+-----------*/
```

### `REPLACE`

```sql
REPLACE(original_value, from_value, to_value)
```

**Description**

Replaces all occurrences of `from_value` with `to_value` in `original_value`.
If `from_value` is empty, no replacement is made.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH desserts AS
  (SELECT 'apple pie' as dessert
  UNION ALL
  SELECT 'blackberry pie' as dessert
  UNION ALL
  SELECT 'cherry pie' as dessert)

SELECT
  REPLACE (dessert, 'pie', 'cobbler') as example
FROM desserts;

/*--------------------*
 | example            |
 +--------------------+
 | apple cobbler      |
 | blackberry cobbler |
 | cherry cobbler     |
 *--------------------*/
```

### `REVERSE`

```sql
REVERSE(value)
```

**Description**

Returns the reverse of the input `STRING` or `BYTES`.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH example AS (
  SELECT 'foo' AS sample_string, b'bar' AS sample_bytes UNION ALL
  SELECT 'абвгд' AS sample_string, b'123' AS sample_bytes
)
SELECT
  sample_string,
  REVERSE(sample_string) AS reverse_string,
  sample_bytes,
  REVERSE(sample_bytes) AS reverse_bytes
FROM example;

/*---------------+----------------+--------------+---------------*
 | sample_string | reverse_string | sample_bytes | reverse_bytes |
 +---------------+----------------+--------------+---------------+
 | foo           | oof            | bar          | rab           |
 | абвгд         | дгвба          | 123          | 321           |
 *---------------+----------------+--------------+---------------*/
```

### `RIGHT`

```sql
RIGHT(value, length)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of the specified
number of rightmost characters or bytes from `value`. The `length` is an
`INT64` that specifies the length of the returned
value. If `value` is `BYTES`, `length` is the number of rightmost bytes to
return. If `value` is `STRING`, `length` is the number of rightmost characters
to return.

If `length` is 0, an empty `STRING` or `BYTES` value will be
returned. If `length` is negative, an error will be returned. If `length`
exceeds the number of characters or bytes from `value`, the original `value`
will be returned.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH examples AS
(SELECT 'apple' as example
UNION ALL
SELECT 'banana' as example
UNION ALL
SELECT 'абвгд' as example
)
SELECT example, RIGHT(example, 3) AS right_example
FROM examples;

/*---------+---------------*
 | example | right_example |
 +---------+---------------+
 | apple   | ple           |
 | banana  | ana           |
 | абвгд   | вгд           |
 *---------+---------------*/
```

```sql
WITH examples AS
(SELECT b'apple' as example
UNION ALL
SELECT b'banana' as example
UNION ALL
SELECT b'\xab\xcd\xef\xaa\xbb' as example
)
SELECT example, RIGHT(example, 3) AS right_example
FROM examples;

/*----------------------+---------------*
 | example              | right_example |
 +----------------------+---------------+
 | apple                | ple           |
 | banana               | ana           |
 | \xab\xcd\xef\xaa\xbb | \xef\xaa\xbb  |
 *----------------------+---------------*
```

### `RPAD`

```sql
RPAD(original_value, return_length[, pattern])
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value` appended
with `pattern`. The `return_length` parameter is an
`INT64` that specifies the length of the
returned value. If `original_value` is `BYTES`,
`return_length` is the number of bytes. If `original_value` is `STRING`,
`return_length` is the number of characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `RPAD('hello world', 7);` returns `'hello w'`.

If `original_value`, `return_length`, or `pattern` is `NULL`, this function
returns `NULL`.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, len, FORMAT('%T', RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);

/*------+-----+----------*
 | t    | len | RPAD     |
 +------+-----+----------+
 | abc  | 5   | "abc  "  |
 | abc  | 2   | "ab"     |
 | 例子  | 4   | "例子  " |
 *------+-----+----------*/
```

```sql
SELECT t, len, pattern, FORMAT('%T', RPAD(t, len, pattern)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

/*------+-----+---------+--------------*
 | t    | len | pattern | RPAD         |
 +------+-----+---------+--------------+
 | abc  | 8   | def     | "abcdefde"   |
 | abc  | 5   | -       | "abc--"      |
 | 例子  | 5   | 中文     | "例子中文中"  |
 *------+-----+---------+--------------*/
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

/*-----------------+-----+------------------*
 | t               | len | RPAD             |
 +-----------------+-----+------------------+
 | b"abc"          | 5   | b"abc  "         |
 | b"abc"          | 2   | b"ab"            |
 | b"\xab\xcd\xef" | 4   | b"\xab\xcd\xef " |
 *-----------------+-----+------------------*/
```

```sql
SELECT
  FORMAT('%T', t) AS t,
  len,
  FORMAT('%T', pattern) AS pattern,
  FORMAT('%T', RPAD(t, len, pattern)) AS RPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);

/*-----------------+-----+---------+-------------------------*
 | t               | len | pattern | RPAD                    |
 +-----------------+-----+---------+-------------------------+
 | b"abc"          | 8   | b"def"  | b"abcdefde"             |
 | b"abc"          | 5   | b"-"    | b"abc--"                |
 | b"\xab\xcd\xef" | 5   | b"\x00" | b"\xab\xcd\xef\x00\x00" |
 *-----------------+-----+---------+-------------------------*/
```

### `RTRIM`

```sql
RTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes trailing characters.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  RTRIM(item, '*') as example
FROM items;

/*-----------*
 | example   |
 +-----------+
 | ***apple  |
 | ***banana |
 | ***orange |
 *-----------*/
```

```sql
WITH items AS
  (SELECT 'applexxx' as item
  UNION ALL
  SELECT 'bananayyy' as item
  UNION ALL
  SELECT 'orangezzz' as item
  UNION ALL
  SELECT 'pearxyz' as item)

SELECT
  RTRIM(item, 'xyz') as example
FROM items;

/*---------*
 | example |
 +---------+
 | apple   |
 | banana  |
 | orange  |
 | pear    |
 *---------*/
```

[string-link-to-trim]: #trim

### `SAFE_CONVERT_BYTES_TO_STRING`

```sql
SAFE_CONVERT_BYTES_TO_STRING(value)
```

**Description**

Converts a sequence of `BYTES` to a `STRING`. Any invalid UTF-8 characters are
replaced with the Unicode replacement character, `U+FFFD`.

**Return type**

`STRING`

**Examples**

The following statement returns the Unicode replacement character, &#65533;.

```sql
SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\xc2') as safe_convert;
```

### `SOUNDEX`

```sql
SOUNDEX(value)
```

**Description**

Returns a `STRING` that represents the
[Soundex][string-link-to-soundex-wikipedia] code for `value`.

SOUNDEX produces a phonetic representation of a string. It indexes words by
sound, as pronounced in English. It is typically used to help determine whether
two strings, such as the family names _Levine_ and _Lavine_, or the words _to_
and _too_, have similar English-language pronunciation.

The result of the SOUNDEX consists of a letter followed by 3 digits. Non-latin
characters are ignored. If the remaining string is empty after removing
non-Latin characters, an empty `STRING` is returned.

**Return type**

`STRING`

**Examples**

```sql
WITH example AS (
  SELECT 'Ashcraft' AS value UNION ALL
  SELECT 'Raven' AS value UNION ALL
  SELECT 'Ribbon' AS value UNION ALL
  SELECT 'apple' AS value UNION ALL
  SELECT 'Hello world!' AS value UNION ALL
  SELECT '  H3##!@llo w00orld!' AS value UNION ALL
  SELECT '#1' AS value UNION ALL
  SELECT NULL AS value
)
SELECT value, SOUNDEX(value) AS soundex
FROM example;

/*----------------------+---------*
 | value                | soundex |
 +----------------------+---------+
 | Ashcraft             | A261    |
 | Raven                | R150    |
 | Ribbon               | R150    |
 | apple                | a140    |
 | Hello world!         | H464    |
 |   H3##!@llo w00orld! | H464    |
 | #1                   |         |
 | NULL                 | NULL    |
 *----------------------+---------*/
```

[string-link-to-soundex-wikipedia]: https://en.wikipedia.org/wiki/Soundex

### `SPLIT`

```sql
SPLIT(value[, delimiter])
```

**Description**

Splits `value` using the `delimiter` argument.

For `STRING`, the default delimiter is the comma `,`.

For `BYTES`, you must specify a delimiter.

Splitting on an empty delimiter produces an array of UTF-8 characters for
`STRING` values, and an array of `BYTES` for `BYTES` values.

Splitting an empty `STRING` returns an
`ARRAY` with a single empty
`STRING`.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return type**

`ARRAY<STRING>` or `ARRAY<BYTES>`

**Examples**

```sql
WITH letters AS
  (SELECT '' as letter_group
  UNION ALL
  SELECT 'a' as letter_group
  UNION ALL
  SELECT 'b c d' as letter_group)

SELECT SPLIT(letter_group, ' ') as example
FROM letters;

/*----------------------*
 | example              |
 +----------------------+
 | []                   |
 | [a]                  |
 | [b, c, d]            |
 *----------------------*/
```

### `STARTS_WITH`

```sql
STARTS_WITH(value, prefix)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if `prefix` is a
prefix of `value`.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return type**

`BOOL`

**Examples**

```sql
WITH items AS
  (SELECT 'foo' as item
  UNION ALL
  SELECT 'bar' as item
  UNION ALL
  SELECT 'baz' as item)

SELECT
  STARTS_WITH(item, 'b') as example
FROM items;

/*---------*
 | example |
 +---------+
 |   False |
 |    True |
 |    True |
 *---------*/
```

### `STRPOS`

```sql
STRPOS(value, subvalue)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns the 1-based position of the first
occurrence of `subvalue` inside `value`. Returns `0` if `subvalue` is not found.

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Return type**

`INT64`

**Examples**

```sql
WITH email_addresses AS
  (SELECT
    'foo@example.com' AS email_address
  UNION ALL
  SELECT
    'foobar@example.com' AS email_address
  UNION ALL
  SELECT
    'foobarbaz@example.com' AS email_address
  UNION ALL
  SELECT
    'quxexample.com' AS email_address)

SELECT
  STRPOS(email_address, '@') AS example
FROM email_addresses;

/*---------*
 | example |
 +---------+
 |       4 |
 |       7 |
 |      10 |
 |       0 |
 *---------*/
```

### `SUBSTR`

```sql
SUBSTR(value, position[, length])
```

**Description**

Returns a substring of the supplied `STRING` or `BYTES` value.

The `position` argument is an integer specifying the starting position of the
substring.

+ If `position` is `1`, the substring starts from the first character or byte.
+ If `position` is `0` or less than `-LENGTH(value)`, `position` is set to `1`,
  and the substring starts from the first character or byte.
+ If `position` is greater than the length of `value`, the function produces
  an empty substring.
+ If `position` is negative, the function counts from the end of `value`,
  with `-1` indicating the last character or byte.

The `length` argument specifies the maximum number of characters or bytes to
return.

+ If `length` is not specified, the function produces a substring that starts
  at the specified position and ends at the last character or byte of `value`.
+ If `length` is `0`, the function produces an empty substring.
+ If `length` is negative, the function produces an error.
+ The returned substring may be shorter than `length`, for example, when
  `length` exceeds the length of `value`, or when the starting position of the
  substring plus `length` is greater than the length of `value`.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 2) as example
FROM items;

/*---------*
 | example |
 +---------+
 | pple    |
 | anana   |
 | range   |
 *---------*/
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 2, 2) as example
FROM items;

/*---------*
 | example |
 +---------+
 | pp      |
 | an      |
 | ra      |
 *---------*/
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, -2) as example
FROM items;

/*---------*
 | example |
 +---------+
 | le      |
 | na      |
 | ge      |
 *---------*/
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 1, 123) as example
FROM items;

/*---------*
 | example |
 +---------+
 | apple   |
 | banana  |
 | orange  |
 *---------*/
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 123) as example
FROM items;

/*---------*
 | example |
 +---------+
 |         |
 |         |
 |         |
 *---------*/
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 123, 5) as example
FROM items;

/*---------*
 | example |
 +---------+
 |         |
 |         |
 |         |
 *---------*/
```

### `SUBSTRING`

```sql
SUBSTRING(value, position[, length])
```

Alias for [`SUBSTR`][substr].

[substr]: #substr

### `TO_BASE32`

```sql
TO_BASE32(bytes_expr)
```

**Description**

Converts a sequence of `BYTES` into a base32-encoded `STRING`. To convert a
base32-encoded `STRING` into `BYTES`, use [FROM_BASE32][string-link-to-from-base32].

**Return type**

`STRING`

**Example**

```sql
SELECT TO_BASE32(b'abcde\xFF') AS base32_string;

/*------------------*
 | base32_string    |
 +------------------+
 | MFRGGZDF74====== |
 *------------------*/
```

[string-link-to-from-base32]: #from_base32

### `TO_BASE64`

```sql
TO_BASE64(bytes_expr)
```

**Description**

Converts a sequence of `BYTES` into a base64-encoded `STRING`. To convert a
base64-encoded `STRING` into `BYTES`, use [FROM_BASE64][string-link-to-from-base64].

There are several base64 encodings in common use that vary in exactly which
alphabet of 65 ASCII characters are used to encode the 64 digits and padding.
See [RFC 4648][RFC-4648] for details. This
function adds padding and uses the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`STRING`

**Example**

```sql
SELECT TO_BASE64(b'\377\340') AS base64_string;

/*---------------*
 | base64_string |
 +---------------+
 | /+A=          |
 *---------------*/
```

To work with an encoding using a different base64 alphabet, you might need to
compose `TO_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To encode a
`base64url`-encoded string, replace `+` and `/` with `-` and `_` respectively.

```sql
SELECT REPLACE(REPLACE(TO_BASE64(b'\377\340'), '+', '-'), '/', '_') as websafe_base64;

/*----------------*
 | websafe_base64 |
 +----------------+
 | _-A=           |
 *----------------*/
```

[string-link-to-from-base64]: #from_base64

[RFC-4648]: https://tools.ietf.org/html/rfc4648#section-4

### `TO_CODE_POINTS`

```sql
TO_CODE_POINTS(value)
```

**Description**

Takes a `STRING` or `BYTES` value and returns an array of
`INT64`.

+ If `value` is a `STRING`, each element in the returned array represents a
  [code point][string-link-to-code-points-wikipedia]. Each code point falls
  within the range of [0, 0xD7FF] and [0xE000, 0x10FFFF].
+ If `value` is `BYTES`, each element in the array is an extended ASCII
  character value in the range of [0, 255].

To convert from an array of code points to a `STRING` or `BYTES`, see
[CODE_POINTS_TO_STRING][string-link-to-codepoints-to-string] or
[CODE_POINTS_TO_BYTES][string-link-to-codepoints-to-bytes].

**Return type**

`ARRAY<INT64>`

**Examples**

The following example gets the code points for each element in an array of
words.

```sql
SELECT word, TO_CODE_POINTS(word) AS code_points
FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama']) AS word;

/*---------+------------------------------------*
 | word    | code_points                        |
 +---------+------------------------------------+
 | foo     | [102, 111, 111]                    |
 | bar     | [98, 97, 114]                      |
 | baz     | [98, 97, 122]                      |
 | giraffe | [103, 105, 114, 97, 102, 102, 101] |
 | llama   | [108, 108, 97, 109, 97]            |
 *---------+------------------------------------*/
```

The following example converts integer representations of `BYTES` to their
corresponding ASCII character values.

```sql
SELECT word, TO_CODE_POINTS(word) AS bytes_value_as_integer
FROM UNNEST([b'\x00\x01\x10\xff', b'\x66\x6f\x6f']) AS word;

/*------------------+------------------------*
 | word             | bytes_value_as_integer |
 +------------------+------------------------+
 | \x00\x01\x10\xff | [0, 1, 16, 255]        |
 | foo              | [102, 111, 111]        |
 *------------------+------------------------*/
```

The following example demonstrates the difference between a `BYTES` result and a
`STRING` result.

```sql
SELECT TO_CODE_POINTS(b'Ā') AS b_result, TO_CODE_POINTS('Ā') AS s_result;

/*------------+----------*
 | b_result   | s_result |
 +------------+----------+
 | [196, 128] | [256]    |
 *------------+----------*/
```

Notice that the character, Ā, is represented as a two-byte Unicode sequence. As
a result, the `BYTES` version of `TO_CODE_POINTS` returns an array with two
elements, while the `STRING` version returns an array with a single element.

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-codepoints-to-string]: #code_points_to_string

[string-link-to-codepoints-to-bytes]: #code_points_to_bytes

### `TO_HEX`

```sql
TO_HEX(bytes)
```

**Description**

Converts a sequence of `BYTES` into a hexadecimal `STRING`. Converts each byte
in the `STRING` as two hexadecimal characters in the range
`(0..9, a..f)`. To convert a hexadecimal-encoded
`STRING` to `BYTES`, use [FROM_HEX][string-link-to-from-hex].

**Return type**

`STRING`

**Example**

```sql
WITH Input AS (
  SELECT b'\x00\x01\x02\x03\xAA\xEE\xEF\xFF' AS byte_str UNION ALL
  SELECT b'foobar'
)
SELECT byte_str, TO_HEX(byte_str) AS hex_str
FROM Input;

/*----------------------------------+------------------*
 | byte_string                      | hex_string       |
 +----------------------------------+------------------+
 | \x00\x01\x02\x03\xaa\xee\xef\xff | 00010203aaeeefff |
 | foobar                           | 666f6f626172     |
 *----------------------------------+------------------*/
```

[string-link-to-from-hex]: #from_hex

### `TRANSLATE`

```sql
TRANSLATE(expression, source_characters, target_characters)
```

**Description**

In `expression`, replaces each character in `source_characters` with the
corresponding character in `target_characters`. All inputs must be the same
type, either `STRING` or `BYTES`.

+ Each character in `expression` is translated at most once.
+ A character in `expression` that is not present in `source_characters` is left
  unchanged in `expression`.
+ A character in `source_characters` without a corresponding character in
  `target_characters` is omitted from the result.
+ A duplicate character in `source_characters` results in an error.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH example AS (
  SELECT 'This is a cookie' AS expression, 'sco' AS source_characters, 'zku' AS
  target_characters UNION ALL
  SELECT 'A coaster' AS expression, 'co' AS source_characters, 'k' as
  target_characters
)
SELECT expression, source_characters, target_characters, TRANSLATE(expression,
source_characters, target_characters) AS translate
FROM example;

/*------------------+-------------------+-------------------+------------------*
 | expression       | source_characters | target_characters | translate        |
 +------------------+-------------------+-------------------+------------------+
 | This is a cookie | sco               | zku               | Thiz iz a kuukie |
 | A coaster        | co                | k                 | A kaster         |
 *------------------+-------------------+-------------------+------------------*/
```

### `TRIM`

```sql
TRIM(value_to_trim[, set_of_characters_to_remove])
```

**Description**

Takes a `STRING` or `BYTES` value to trim.

If the value to trim is a `STRING`, removes from this value all leading and
trailing Unicode code points in `set_of_characters_to_remove`.
The set of code points is optional. If it is not specified, all
whitespace characters are removed from the beginning and end of the
value to trim.

If the value to trim is `BYTES`, removes from this value all leading and
trailing bytes in `set_of_characters_to_remove`. The set of bytes is required.

**Return type**

+ `STRING` if `value_to_trim` is a `STRING` value.
+ `BYTES` if `value_to_trim` is a `BYTES` value.

**Examples**

In the following example, all leading and trailing whitespace characters are
removed from `item` because `set_of_characters_to_remove` is not specified.

```sql
WITH items AS
  (SELECT '   apple   ' as item
  UNION ALL
  SELECT '   banana   ' as item
  UNION ALL
  SELECT '   orange   ' as item)

SELECT
  CONCAT('#', TRIM(item), '#') as example
FROM items;

/*----------*
 | example  |
 +----------+
 | #apple#  |
 | #banana# |
 | #orange# |
 *----------*/
```

In the following example, all leading and trailing `*` characters are removed
from `item`.

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  TRIM(item, '*') as example
FROM items;

/*---------*
 | example |
 +---------+
 | apple   |
 | banana  |
 | orange  |
 *---------*/
```

In the following example, all leading and trailing `x`, `y`, and `z` characters
are removed from `item`.

```sql
WITH items AS
  (SELECT 'xxxapplexxx' as item
  UNION ALL
  SELECT 'yyybananayyy' as item
  UNION ALL
  SELECT 'zzzorangezzz' as item
  UNION ALL
  SELECT 'xyzpearxyz' as item)

SELECT
  TRIM(item, 'xyz') as example
FROM items;

/*---------*
 | example |
 +---------+
 | apple   |
 | banana  |
 | orange  |
 | pear    |
 *---------*/
```

In the following example, examine how `TRIM` interprets characters as
Unicode code-points. If your trailing character set contains a combining
diacritic mark over a particular letter, `TRIM` might strip the
same diacritic mark from a different letter.

```sql
SELECT
  TRIM('abaW̊', 'Y̊') AS a,
  TRIM('W̊aba', 'Y̊') AS b,
  TRIM('abaŪ̊', 'Y̊') AS c,
  TRIM('Ū̊aba', 'Y̊') AS d;

/*------+------+------+------*
 | a    | b    | c    | d    |
 +------+------+------+------+
 | abaW | W̊aba | abaŪ | Ūaba |
 *------+------+------+------*/
```

In the following example, all leading and trailing `b'n'`, `b'a'`, `b'\xab'`
bytes are removed from `item`.

```sql
WITH items AS
(
  SELECT b'apple' as item UNION ALL
  SELECT b'banana' as item UNION ALL
  SELECT b'\xab\xcd\xef\xaa\xbb' as item
)
SELECT item, TRIM(item, b'na\xab') AS examples
FROM items;

/*----------------------+------------------*
 | item                 | example          |
 +----------------------+------------------+
 | apple                | pple             |
 | banana               | b                |
 | \xab\xcd\xef\xaa\xbb | \xcd\xef\xaa\xbb |
 *----------------------+------------------*/
```

### `UNICODE`

```sql
UNICODE(value)
```

**Description**

Returns the Unicode [code point][string-code-point] for the first character in
`value`. Returns `0` if `value` is empty, or if the resulting Unicode code
point is `0`.

**Return type**

`INT64`

**Examples**

```sql
SELECT UNICODE('âbcd') as A, UNICODE('â') as B, UNICODE('') as C, UNICODE(NULL) as D;

/*-------+-------+-------+-------*
 | A     | B     | C     | D     |
 +-------+-------+-------+-------+
 | 226   | 226   | 0     | NULL  |
 *-------+-------+-------+-------*/
```

[string-code-point]: https://en.wikipedia.org/wiki/Code_point

### `UPPER`

```sql
UPPER(value)
```

**Description**

For `STRING` arguments, returns the original string with all alphabetic
characters in uppercase. Mapping between uppercase and lowercase is done
according to the
[Unicode Character Database][string-link-to-unicode-character-definitions]
without taking into account language-specific mappings.

For `BYTES` arguments, the argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT
    'foo' as item
  UNION ALL
  SELECT
    'bar' as item
  UNION ALL
  SELECT
    'baz' as item)

SELECT
  UPPER(item) AS example
FROM items;

/*---------*
 | example |
 +---------+
 | FOO     |
 | BAR     |
 | BAZ     |
 *---------*/
```

[string-link-to-unicode-character-definitions]: http://unicode.org/ucd/

[string-link-to-strpos]: #strpos

## JSON functions

ZetaSQL supports the following functions, which can retrieve and
transform JSON data.

### Function overview

#### Standard JSON extraction functions (recommended)

The following functions use double quotes to escape invalid
[JSONPath][JSONPath-format] characters: <code>"a.b"</code>.

This behavior is consistent with the ANSI standard.

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#json_query"><code>JSON_QUERY</code></a></td>
      <td>
        Extracts a JSON value, such as an array or object, or a JSON
        scalar value, such as a string, number, or boolean.
      </td>
      <td>
        JSON-formatted <code>STRING</code>
         or
        <code>JSON</code>
        
      </td>
    </tr>
    <tr>
      <td><a href="#json_value"><code>JSON_VALUE</code></a></td>
      <td>
        Extracts a scalar value.
        A scalar value can represent a string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if a non-scalar value is selected.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#json_query_array"><code>JSON_QUERY_ARRAY</code></a></td>
      <td>
        Extracts an array of JSON values, such as arrays or objects, and
        JSON scalar values, such as strings, numbers, and booleans.
      </td>
      <td>
        <code>ARRAY&lt;JSON-formatted STRING&gt;</code>
         or
        <code>ARRAY&lt;JSON&gt;</code>
        
      </td>
    </tr>
    
    
    <tr>
      <td><a href="#json_value_array"><code>JSON_VALUE_ARRAY</code></a></td>
      <td>
        Extracts an array of scalar values. A scalar value can represent a
        string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if the selected value is not an array or
        not an array containing only scalar values.
      </td>
      <td><code>ARRAY&lt;STRING&gt;</code></td>
    </tr>
    
  </tbody>
</table>

#### Legacy JSON extraction functions

The following functions use single quotes and brackets to escape invalid
[JSONPath][JSONPath-format] characters: <code>['a.b']</code></td>.

While these functions are supported by ZetaSQL, we recommend using
the functions in the previous table.

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#json_extract"><code>JSON_EXTRACT</code></a></td>
      <td>
        Extracts a JSON value, such as an array or object, or a JSON
        scalar value, such as a string, number, or boolean.
      </td>
      <td>
        JSON-formatted <code>STRING</code>
         or
        <code>JSON</code>
        
      </td>
    </tr>
    <tr>
      <td><a href="#json_extract_scalar"><code>JSON_EXTRACT_SCALAR</code></a></td>
      <td>
        Extracts a scalar value.
        A scalar value can represent a string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if a non-scalar value is selected.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    
  </tbody>
</table>

#### Other JSON functions

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#parse_json"><code>PARSE_JSON</code></a></td>
      <td>
        Takes a JSON-formatted string and returns a JSON value.
      </td>
      <td><code>JSON</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#to_json"><code>TO_JSON</code></a></td>
      <td>
        Takes a SQL value and returns a JSON value.
      </td>
      <td><code>JSON</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#to_json_string"><code>TO_JSON_STRING</code></a></td>
      <td>
        Takes a SQL value and returns a JSON-formatted string
        representation of the value.
      </td>
      <td>JSON-formatted <code>STRING</code></td>
    </tr>
    
    
    
    <tr>
      <td><a href="#string_for_json"><code>STRING</code></a></td>
      <td>
        Extracts a string from JSON.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#bool_for_json"><code>BOOL</code></a></td>
      <td>
        Extracts a boolean from JSON.
      </td>
      <td><code>BOOL</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#int64_for_json"><code>INT64</code></a></td>
      <td>
        Extracts a 64-bit integer from JSON.
      </td>
      <td><code>INT64</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#double_for_json"><code>DOUBLE</code></a></td>
      <td>
        Extracts a 64-bit floating-point number from JSON.
      </td>
      <td><code>DOUBLE</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#json_type"><code>JSON_TYPE</code></a></td>
      <td>
        Returns the type of the outermost JSON value as a string.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
  </tbody>
</table>

### `BOOL` 
<a id="bool_for_json"></a>

```sql
BOOL(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON boolean, and returns that value as a SQL
`BOOL`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON value is not a boolean, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`BOOL`

**Examples**

```sql
SELECT BOOL(JSON 'true') AS vacancy;

/*---------*
 | vacancy |
 +---------+
 | true    |
 *---------*/
```

```sql
SELECT BOOL(JSON_QUERY(JSON '{"hotel class": "5-star", "vacancy": true}', "$.vacancy")) AS vacancy;

/*---------*
 | vacancy |
 +---------+
 | true    |
 *---------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type bool.
SELECT BOOL(JSON '123') AS result; -- Throws an error
SELECT BOOL(JSON 'null') AS result; -- Throw an error
SELECT SAFE.BOOL(JSON '123') AS result; -- Returns a SQL NULL
```

### `DOUBLE` 
<a id="double_for_json"></a>

```sql
DOUBLE(json_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Takes a JSON expression, extracts a JSON number and returns that value as a SQL
`DOUBLE`. If the expression is SQL `NULL`, the
function returns SQL `NULL`. If the extracted JSON value is not a number, an
error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

This function supports an optional mandatory-named argument called
`wide_number_mode` which defines what happens with a number that cannot be
represented as a DOUBLE without loss of precision.

This argument accepts one of the two case-sensitive values:

+   ‘exact’: The function fails if the result cannot be represented as a
    `DOUBLE` without loss of precision.
+   ‘round’: The numeric value stored in JSON will be rounded to
    `DOUBLE`. If such rounding is not possible, the
    function fails. This is the default value if the argument is not specified.

**Return type**

`DOUBLE`

**Examples**

```sql
SELECT DOUBLE(JSON '9.8') AS velocity;

/*----------*
 | velocity |
 +----------+
 | 9.8      |
 *----------*/
```

```sql
SELECT DOUBLE(JSON_QUERY(JSON '{"vo2_max": 39.1, "age": 18}', "$.vo2_max")) AS vo2_max;

/*---------*
 | vo2_max |
 +---------+
 | 39.1    |
 *---------*/
```

```sql
SELECT DOUBLE(JSON '18446744073709551615', wide_number_mode=>'round') as result;

/*------------------------*
 | result                 |
 +------------------------+
 | 1.8446744073709552e+19 |
 *------------------------*/
```

```sql
SELECT DOUBLE(JSON '18446744073709551615') as result;

/*------------------------*
 | result                 |
 +------------------------+
 | 1.8446744073709552e+19 |
 *------------------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type DOUBLE.
SELECT DOUBLE(JSON '"strawberry"') AS result;
SELECT DOUBLE(JSON 'null') AS result;

-- An error is thrown because `wide_number_mode` is case-sensitive and not "exact" or "round".
SELECT DOUBLE(JSON '123.4', wide_number_mode=>'EXACT') as result;
SELECT DOUBLE(JSON '123.4', wide_number_mode=>'exac') as result;

-- An error is thrown because the number cannot be converted to DOUBLE without loss of precision
SELECT DOUBLE(JSON '18446744073709551615', wide_number_mode=>'exact') as result;

-- Returns a SQL NULL
SELECT SAFE.DOUBLE(JSON '"strawberry"') AS result;
```

### `INT64` 
<a id="int64_for_json"></a>

```sql
INT64(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON number and returns that value as a SQL
`INT64`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON number has a fractional part or is outside of the
INT64 domain, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`INT64`

**Examples**

```sql
SELECT INT64(JSON '2005') AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT64(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT64(JSON '10.0') AS score;

/*-------*
 | score |
 +-------+
 | 10    |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT INT64(JSON '10.1') AS result;  -- Throws an error
SELECT INT64(JSON '"strawberry"') AS result; -- Throws an error
SELECT INT64(JSON 'null') AS result; -- Throws an error
SELECT SAFE.INT64(JSON '"strawberry"') AS result;  -- Returns a SQL NULL
```

### `JSON_EXTRACT_SCALAR`

Note: This function is deprecated. Consider using [JSON_VALUE][json-value].

```sql
JSON_EXTRACT_SCALAR(json_string_expr[, json_path])
```

```sql
JSON_EXTRACT_SCALAR(json_expr[, json_path])
```

**Description**

Extracts a scalar value and then returns it as a string. A scalar value can
represent a string, number, or boolean. Removes the outermost quotes and
unescapes the return values. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using single quotes and brackets.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

`STRING`

**Examples**

In the following example, `age` is extracted.

```sql
SELECT JSON_EXTRACT_SCALAR(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

/*------------*
 | scalar_age |
 +------------+
 | 6          |
 *------------*/
```

The following example compares how results are returned for the `JSON_EXTRACT`
and `JSON_EXTRACT_SCALAR` functions.

```sql
SELECT JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.name') AS json_name,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.name') AS scalar_name,
  JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.age') AS json_age,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

/*-----------+-------------+----------+------------*
 | json_name | scalar_name | json_age | scalar_age |
 +-----------+-------------+----------+------------+
 | "Jakob"   | Jakob       | "6"      | 6          |
 *-----------+-------------+----------+------------*/
```

```sql
SELECT JSON_EXTRACT('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract,
  JSON_EXTRACT_SCALAR('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract_scalar;

/*--------------------+---------------------*
 | json_extract       | json_extract_scalar |
 +--------------------+---------------------+
 | ["apple","banana"] | NULL                |
 *--------------------+---------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c") AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

[json-value]: #json_value

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_EXTRACT`

Note: This function is deprecated. Consider using [JSON_QUERY][json-query].

```sql
JSON_EXTRACT(json_string_expr, json_path)
```

```sql
JSON_EXTRACT(json_expr, json_path)
```

**Description**

Extracts a JSON value, such as an array or object, or a JSON scalar
value, such as a string, number, or boolean. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using single quotes and brackets.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string `null` is encountered.
    For example:

    ```sql
    SELECT JSON_EXTRACT("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_EXTRACT(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_EXTRACT(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')
  AS json_data;

/*-----------------------------------*
 | json_data                         |
 +-----------------------------------+
 | {"students":[{"id":5},{"id":12}]} |
 *-----------------------------------*/
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT JSON_EXTRACT(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"Jane"}]}}                  |
 | {"class":{"students":[]}}                                 |
 | {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"Jane"} |
 | NULL            |
 | {"name":"John"} |
 *-----------------*/
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-------------------*
 | second_student    |
 +-------------------+
 | NULL              |
 | NULL              |
 | NULL              |
 | "Jamie"           |
 *-------------------*/
```

```sql
SELECT JSON_EXTRACT(json_text, "$.class['students']") AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"Jane"}]                  |
 | []                                 |
 | [{"name":"John"},{"name":"Jamie"}] |
 *------------------------------------*/
```

```sql
SELECT JSON_EXTRACT('{"a":null}', "$.a"); -- Returns a SQL NULL
SELECT JSON_EXTRACT('{"a":null}', "$.b"); -- Returns a SQL NULL
```

```sql
SELECT JSON_EXTRACT(JSON '{"a":null}', "$.a"); -- Returns a JSON 'null'
SELECT JSON_EXTRACT(JSON '{"a":null}', "$.b"); -- Returns a SQL NULL
```

[json-query]: #json_query

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_QUERY_ARRAY`

```sql
JSON_QUERY_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_QUERY_ARRAY(json_expr[, json_path])
```

**Description**

Extracts an array of JSON values, such as arrays or objects, and
JSON scalar values, such as strings, numbers, and booleans.
If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: `ARRAY<JSON-formatted STRING>`
+ `json_expr`: `ARRAY<JSON>`

**Examples**

This extracts items in JSON to an array of `JSON` values:

```sql
SELECT JSON_QUERY_ARRAY(
  JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits'
  ) AS json_array;

/*---------------------------------*
 | json_array                      |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
SELECT JSON_QUERY_ARRAY('[1,2,3]') AS string_array;

/*--------------*
 | string_array |
 +--------------+
 | [1, 2, 3]    |
 *--------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_QUERY_ARRAY('[1,2,3]','$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

This extracts string values in a JSON-formatted string to an array:

```sql
-- Doesn't strip the double quotes
SELECT JSON_QUERY_ARRAY('["apples","oranges","grapes"]', '$') AS string_array;

/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/

-- Strips the double quotes
SELECT ARRAY(
  SELECT JSON_VALUE(string_element, '$')
  FROM UNNEST(JSON_QUERY_ARRAY('["apples","oranges","grapes"]','$')) AS string_element
) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

This extracts only the items in the `fruit` property to an array:

```sql
SELECT JSON_QUERY_ARRAY(
  '{"fruit":[{"apples":5,"oranges":10},{"apples":2,"oranges":4}],"vegetables":[{"lettuce":7,"kale": 8}]}',
  '$.fruit'
) AS string_array;

/*-------------------------------------------------------*
 | string_array                                          |
 +-------------------------------------------------------+
 | [{"apples":5,"oranges":10}, {"apples":2,"oranges":4}] |
 *-------------------------------------------------------*/
```

These are equivalent:

```sql
SELECT JSON_QUERY_ARRAY('{"fruits":["apples","oranges","grapes"]}','$.fruits') AS string_array;

SELECT JSON_QUERY_ARRAY('{"fruits":["apples","oranges","grapes"]}','$."fruits"') AS string_array;

-- The queries above produce the following result:
/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_QUERY_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

/*-----------*
 | hello     |
 +-----------+
 | ["world"] |
 *-----------*/
```

The following examples show how invalid requests and empty arrays are handled:

```sql
-- An error is returned if you provide an invalid JSONPath.
SELECT JSON_QUERY_ARRAY('["foo","bar","baz"]','INVALID_JSONPath') AS result;

-- If the JSONPath does not refer to an array, then NULL is returned.
SELECT JSON_QUERY_ARRAY('{"a":"foo"}','$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a key that does not exist is specified, then the result is NULL.
SELECT JSON_QUERY_ARRAY('{"a":"foo"}','$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- Empty arrays in JSON-formatted strings are supported.
SELECT JSON_QUERY_ARRAY('{"a":"foo","b":[]}','$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_QUERY`

```sql
JSON_QUERY(json_string_expr, json_path)
```

```sql
JSON_QUERY(json_expr, json_path)
```

**Description**

Extracts a JSON value, such as an array or object, or a JSON scalar
value, such as a string, number, or boolean. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string `null` is encountered.
    For example:

    ```sql
    SELECT JSON_QUERY("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_QUERY(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_QUERY(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')
  AS json_data;

/*-----------------------------------*
 | json_data                         |
 +-----------------------------------+
 | {"students":[{"id":5},{"id":12}]} |
 *-----------------------------------*/
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT JSON_QUERY(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"Jane"}]}}                  |
 | {"class":{"students":[]}}                                 |
 | {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"Jane"} |
 | NULL            |
 | {"name":"John"} |
 *-----------------*/
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*-------------------*
 | second_student    |
 +-------------------+
 | NULL              |
 | NULL              |
 | NULL              |
 | "Jamie"           |
 *-------------------*/
```

```sql
SELECT JSON_QUERY(json_text, '$.class."students"') AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"Jane"}]                  |
 | []                                 |
 | [{"name":"John"},{"name":"Jamie"}] |
 *------------------------------------*/
```

```sql
SELECT JSON_QUERY('{"a":null}', "$.a"); -- Returns a SQL NULL
SELECT JSON_QUERY('{"a":null}', "$.b"); -- Returns a SQL NULL
```

```sql
SELECT JSON_QUERY(JSON '{"a":null}', "$.a"); -- Returns a JSON 'null'
SELECT JSON_QUERY(JSON '{"a":null}', "$.b"); -- Returns a SQL NULL
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_TYPE` 
<a id="json_type"></a>

```sql
JSON_TYPE(json_expr)
```

**Description**

Takes a JSON expression and returns the type of the outermost JSON value as a
SQL `STRING`. The names of these JSON types can be returned:

+ `object`
+ `array`
+ `string`
+ `number`
+ `boolean`
+ `null`

If the expression is SQL `NULL`, the function returns SQL `NULL`. If the
extracted JSON value is not a valid JSON type, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`STRING`

**Examples**

```sql
SELECT json_val, JSON_TYPE(json_val) AS type
FROM
  UNNEST(
    [
      JSON '"apple"',
      JSON '10',
      JSON '3.14',
      JSON 'null',
      JSON '{"city": "New York", "State": "NY"}',
      JSON '["apple", "banana"]',
      JSON 'false'
    ]
  ) AS json_val;

/*----------------------------------+---------*
 | json_val                         | type    |
 +----------------------------------+---------+
 | "apple"                          | string  |
 | 10                               | number  |
 | 3.14                             | number  |
 | null                             | null    |
 | {"State":"NY","city":"New York"} | object  |
 | ["apple","banana"]               | array   |
 | false                            | boolean |
 *----------------------------------+---------*/
```

### `JSON_VALUE`

```sql
JSON_VALUE(json_string_expr[, json_path])
```

```sql
JSON_VALUE(json_expr[, json_path])
```

**Description**

Extracts a scalar value and then returns it as a string. A scalar value can
represent a string, number, or boolean. Removes the outermost quotes and
unescapes the return values. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

`STRING`

**Examples**

In the following example, JSON data is extracted and returned as a scalar value.

```sql
SELECT JSON_VALUE(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

/*------------*
 | scalar_age |
 +------------+
 | 6          |
 *------------*/
```

The following example compares how results are returned for the `JSON_QUERY`
and `JSON_VALUE` functions.

```sql
SELECT JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.name') AS json_name,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.name') AS scalar_name,
  JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.age') AS json_age,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

/*-----------+-------------+----------+------------*
 | json_name | scalar_name | json_age | scalar_age |
 +-----------+-------------+----------+------------+
 | "Jakob"   | Jakob       | "6"      | 6          |
 *-----------+-------------+----------+------------*/
```

```sql
SELECT JSON_QUERY('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_query,
  JSON_VALUE('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_value;

/*--------------------+------------*
 | json_query         | json_value |
 +--------------------+------------+
 | ["apple","banana"] | NULL       |
 *--------------------+------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes. For example:

```sql
SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c') AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_VALUE_ARRAY`

```sql
JSON_VALUE_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_VALUE_ARRAY(json_expr[, json_path])
```

**Description**

Extracts an array of scalar values and returns an array of string-formatted
scalar values. A scalar value can represent a string, number, or boolean.
If a JSON key uses invalid [JSONPath][JSONPath-format] characters, you can
escape those characters using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

Caveats:

+ A JSON `null` in the input array produces a SQL `NULL` as the output for that
  JSON `null`.
+ If a JSONPath matches an array that contains scalar objects and a JSON `null`,
  then the output is an array of the scalar objects and a SQL `NULL`.

**Return type**

`ARRAY<STRING>`

**Examples**

This extracts items in JSON to a string array:

```sql
SELECT JSON_VALUE_ARRAY(
  JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits'
  ) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

The following example compares how results are returned for the
`JSON_QUERY_ARRAY` and `JSON_VALUE_ARRAY` functions.

```sql
SELECT JSON_QUERY_ARRAY('["apples","oranges"]') AS json_array,
       JSON_VALUE_ARRAY('["apples","oranges"]') AS string_array;

/*-----------------------+-------------------*
 | json_array            | string_array      |
 +-----------------------+-------------------+
 | ["apples", "oranges"] | [apples, oranges] |
 *-----------------------+-------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
-- Strips the double quotes
SELECT JSON_VALUE_ARRAY('["foo","bar","baz"]','$') AS string_array;

/*-----------------*
 | string_array    |
 +-----------------+
 | [foo, bar, baz] |
 *-----------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_VALUE_ARRAY('[1,2,3]','$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

These are equivalent:

```sql
SELECT JSON_VALUE_ARRAY('{"fruits":["apples","oranges","grapes"]}','$.fruits') AS string_array;
SELECT JSON_VALUE_ARRAY('{"fruits":["apples","oranges","grapes"]}','$."fruits"') AS string_array;

-- The queries above produce the following result:
/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_VALUE_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

/*---------*
 | hello   |
 +---------+
 | [world] |
 *---------*/
```

The following examples explore how invalid requests and empty arrays are
handled:

```sql
-- An error is thrown if you provide an invalid JSONPath.
SELECT JSON_VALUE_ARRAY('["foo","bar","baz"]','INVALID_JSONPath') AS result;

-- If the JSON-formatted string is invalid, then NULL is returned.
SELECT JSON_VALUE_ARRAY('}}','$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If the JSON document is NULL, then NULL is returned.
SELECT JSON_VALUE_ARRAY(NULL,'$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath does not match anything, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":["foo","bar","baz"]}','$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an object that is not an array, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":"foo"}','$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of non-scalar objects, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":[{"b":"foo","c":1},{"b":"bar","c":2}],"d":"baz"}','$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of mixed scalar and non-scalar objects,
-- then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":[10, {"b": 20}]','$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an empty JSON array, then the output is an empty array instead of NULL.
SELECT JSON_VALUE_ARRAY('{"a":"foo","b":[]}','$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/

-- In the following query, the JSON null input is returned as a
-- SQL NULL in the output.
SELECT JSON_VALUE_ARRAY('["world", null, 1]') AS result;

/*------------------*
 | result           |
 +------------------+
 | [world, NULL, 1] |
 *------------------*/

```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `PARSE_JSON`

```sql
PARSE_JSON(json_string_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Takes a SQL `STRING` value and returns a SQL `JSON` value.
The `STRING` value represents a string-formatted JSON value.

This function supports an optional mandatory-named argument called
`wide_number_mode` that determines how to handle numbers that cannot be stored
in a `JSON` value without the loss of precision. If used,
`wide_number_mode` must include one of these values:

+ `exact`: Only accept numbers that can be stored without loss of precision. If
  a number that cannot be stored without loss of precision is encountered,
  the function throws an error.
+ `round`: If a number that cannot be stored without loss of precision is
  encountered, attempt to round it to a number that can be stored without
  loss of precision. If the number cannot be rounded, the function throws an
  error.

If `wide_number_mode` is not used, the function implicitly includes
`wide_number_mode=>'exact'`. If a number appears in a JSON object or array,
the `wide_number_mode` argument is applied to the number in the object or array.

Numbers from the following domains can be stored in JSON without loss of
precision:

+ 64-bit signed/unsigned integers, such as `INT64`
+ `DOUBLE`

**Return type**

`JSON`

**Examples**

In the following example, a JSON-formatted string is converted to `JSON`.

```sql
SELECT PARSE_JSON('{"coordinates":[10,20],"id":1}') AS json_data;

/*--------------------------------*
 | json_data                      |
 +--------------------------------+
 | {"coordinates":[10,20],"id":1} |
 *--------------------------------*/
```

The following queries fail because:

+ The number that was passed in cannot be stored without loss of precision.
+ `wide_number_mode=>'exact'` is used implicitly in the first query and
  explicitly in the second query.

```sql
SELECT PARSE_JSON('{"id":922337203685477580701}') AS json_data; -- fails
SELECT PARSE_JSON('{"id":922337203685477580701}', wide_number_mode=>'exact') AS json_data; -- fails
```

The following query rounds the number to a number that can be stored in JSON.

```sql
SELECT PARSE_JSON('{"id":922337203685477580701}', wide_number_mode=>'round') AS json_data;

/*--------------------------------*
 | json_data                      |
 +--------------------------------+
 | {"id":9.223372036854776e+20}   |
 *--------------------------------*/
```

### `STRING` 
<a id="string_for_json"></a>

```sql
STRING(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON string, and returns that value as a SQL
`STRING`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON value is not a string, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`STRING`

**Examples**

```sql
SELECT STRING(JSON '"purple"') AS color;

/*--------*
 | color  |
 +--------+
 | purple |
 *--------*/
```

```sql
SELECT STRING(JSON_QUERY(JSON '{"name": "sky", "color": "blue"}', "$.color")) AS color;

/*-------*
 | color |
 +-------+
 | blue  |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not of type string.
SELECT STRING(JSON '123') AS result; -- Throws an error
SELECT STRING(JSON 'null') AS result; -- Throws an error
SELECT SAFE.STRING(JSON '123') AS result; -- Returns a SQL NULL
```

### `TO_JSON_STRING`

```sql
TO_JSON_STRING(value[, pretty_print])
```

**Description**

Takes a SQL value and returns a JSON-formatted string
representation of the value. The value must be a supported ZetaSQL
data type. You can review the ZetaSQL data types that this function
supports and their JSON encodings [here][json-encodings].

This function supports an optional boolean parameter called `pretty_print`.
If `pretty_print` is `true`, the returned value is formatted for easy
readability.

**Return type**

A JSON-formatted `STRING`

**Examples**

Convert rows in a table to JSON-formatted strings.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t) AS json_data
FROM CoordinatesTable AS t;

/*----+-------------+--------------------------------*
 | id | coordinates | json_data                      |
 +----+-------------+--------------------------------+
 | 1  | [10, 20]    | {"id":1,"coordinates":[10,20]} |
 | 2  | [30, 40]    | {"id":2,"coordinates":[30,40]} |
 | 3  | [50, 60]    | {"id":3,"coordinates":[50,60]} |
 *----+-------------+--------------------------------*/
```

Convert rows in a table to JSON-formatted strings that are easy to read.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t, true) AS json_data
FROM CoordinatesTable AS t;

/*----+-------------+--------------------*
 | id | coordinates | json_data          |
 +----+-------------+--------------------+
 | 1  | [10, 20]    | {                  |
 |    |             |   "id": 1,         |
 |    |             |   "coordinates": [ |
 |    |             |     10,            |
 |    |             |     20             |
 |    |             |   ]                |
 |    |             | }                  |
 +----+-------------+--------------------+
 | 2  | [30, 40]    | {                  |
 |    |             |   "id": 2,         |
 |    |             |   "coordinates": [ |
 |    |             |     30,            |
 |    |             |     40             |
 |    |             |   ]                |
 |    |             | }                  |
 *----+-------------+--------------------*/
```

[json-encodings]: #json_encodings

### `TO_JSON`

```sql
TO_JSON(sql_value[, stringify_wide_numbers=>{ TRUE | FALSE }])
```

**Description**

Takes a SQL value and returns a JSON value. The value
must be a supported ZetaSQL data type. You can review the
ZetaSQL data types that this function supports and their
JSON encodings [here][json-encodings].

This function supports an optional mandatory-named argument called
`stringify_wide_numbers`.

+ If this argument is `TRUE`, numeric values outside
  of the `DOUBLE` type domain are encoded as strings.
+ If this argument is not used or is `FALSE`, numeric values outside
  of the `DOUBLE` type domain are not encoded
  as strings, but are stored as JSON numbers. If a numerical value cannot be
  stored in JSON without loss of precision, an error is thrown.

The following numerical data types are affected by the
`stringify_wide_numbers` argument:

+ `INT64`
+ `UINT64`
+ `NUMERIC`
+ `BIGNUMERIC`

If one of these numerical data types appears in a container data type
such as an `ARRAY` or `STRUCT`, the `stringify_wide_numbers` argument is
applied to the numerical data types in the container data type.

**Return type**

`JSON`

**Examples**

In the following example, the query converts rows in a table to JSON values.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT TO_JSON(t) AS json_objects
FROM CoordinatesTable AS t;

/*--------------------------------*
 | json_objects                   |
 +--------------------------------+
 | {"coordinates":[10,20],"id":1} |
 | {"coordinates":[30,40],"id":2} |
 | {"coordinates":[50,60],"id":3} |
 *--------------------------------*/
```

In the following example, the query returns a large numerical value as a
JSON string.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>TRUE) as stringify_on

/*--------------------*
 | stringify_on       |
 +--------------------+
 | "9007199254740993" |
 *--------------------*/
```

In the following example, both queries return a large numerical value as a
JSON number.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>FALSE) as stringify_off
SELECT TO_JSON(9007199254740993) as stringify_off

/*------------------*
 | stringify_off    |
 +------------------+
 | 9007199254740993 |
 *------------------*/
```

In the following example, only large numeric values are converted to
JSON strings.

```sql
With T1 AS (
  (SELECT 9007199254740993 AS id) UNION ALL
  (SELECT 2 AS id))
SELECT TO_JSON(t, stringify_wide_numbers=>TRUE) AS json_objects
FROM T1 AS t;

/*---------------------------*
 | json_objects              |
 +---------------------------+
 | {"id":"9007199254740993"} |
 | {"id":2}                  |
 *---------------------------*/
```

In this example, the values `9007199254740993` (`INT64`)
and `2.1` (`DOUBLE`) are converted
to the common supertype `DOUBLE`, which is not
affected by the `stringify_wide_numbers` argument.

```sql
With T1 AS (
  (SELECT 9007199254740993 AS id) UNION ALL
  (SELECT 2.1 AS id))
SELECT TO_JSON(t, stringify_wide_numbers=>TRUE) AS json_objects
FROM T1 AS t;

/*------------------------------*
 | json_objects                 |
 +------------------------------+
 | {"id":9.007199254740992e+15} |
 | {"id":2.1}                   |
 *------------------------------*/
```

[json-encodings]: #json_encodings

### JSON encodings 
<a id="json_encodings"></a>

The following table includes common encodings that are used when a
SQL value is encoded as JSON value with
the `TO_JSON_STRING`
or `TO_JSON` function.

<table>
  <thead>
    <tr>
      <th>From SQL</th>
      <th width='400px'>To JSON</th>
      <th>Examples</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>NULL</td>
      <td>
        <p>null</p>
      </td>
      <td>
        SQL input: <code>NULL</code><br />
        JSON output: <code>null</code>
      </td>
    </tr>
    
    <tr>
      <td>BOOL</td>
      <td>boolean</td>
      <td>
        SQL input: <code>TRUE</code><br />
        JSON output: <code>true</code><br />
        <hr />
        SQL input: <code>FALSE</code><br />
        JSON output: <code>false</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>
        INT32<br/>
        UINT32
      </td>
      <td>integer</td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>12345678901</code><br />
        JSON output: <code>12345678901</code><br />
      </td>
    </tr>
    
    
    
    <tr>
      <td>
        INT64
        <br />UINT64
      </td>
      <td>
        <p>(TO_JSON_STRING only)</p>
        <p>number or string</p>
        <p>
          Encoded as a number when the value is in the range of
          [-2<sup>53</sup>, 2<sup>53</sup>], which is the range of
          integers that can be represented losslessly as IEEE 754
          double-precision floating point numbers. A value outside of this range
          is encoded as a string.
        </p>
      <td>
        SQL input: <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        INT64
        <br />UINT64
      </td>
      <td>
        <p>(TO_JSON only)</p>
        <p>number or string</p>
        <p>
          If the <code>stringify_wide_numbers</code> argument
          is <code>TRUE</code> and the value is outside of the
          DOUBLE type domain, the value is
          encoded as a string. If the value cannot be stored in
          JSON without loss of precision, the function fails.
          Otherwise, the value is encoded as a number.
        </p>
        <p>
          If the <code>stringify_wide_numbers</code> is not used or is
          <code>FALSE</code>, numeric values outside of the
          `DOUBLE` type domain are not
          encoded as strings, but are stored as JSON numbers. If a
          numerical value cannot be stored in JSON without loss of precision,
          an error is thrown.
        </p>
      <td>
        SQL input: <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE:
        <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE:
        <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        NUMERIC
        <br/>BIGNUMERIC
      </td>
      <td>
        <p>(TO_JSON_STRING only)</p>
        <p>number or string</p>
        <p>
          Encoded as a number when the value is in the range of
          [-2<sup>53</sup>, 2<sup>53</sup>] and has no fractional
          part. A value outside of this range is encoded as a string.
        </p>
      </td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
        <hr />
        SQL input: <code>123.56</code><br />
        JSON output: <code>"123.56"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        NUMERIC
        <br/>BIGNUMERIC
      </td>
      <td>
        <p>(TO_JSON only)</p>
        <p>number or string</p>
        <p>
          If the <code>stringify_wide_numbers</code> argument
          is <code>TRUE</code> and the value is outside of the
          DOUBLE type domain, it is
          encoded as a string. Otherwise, it's encoded as a number.
        </p>
      </td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input: <code>123.56</code><br />
        JSON output: <code>123.56</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE: <code>123.56</code><br />
        JSON output: <code>123.56</code><br />
      </td>
    </tr>
    
    
    
    <tr>
      <td>
        FLOAT<br />
        DOUBLE
      </td>
      <td>
        <p>number or string</p>
        <p>
          <code>+/-inf</code> and <code>NaN</code> are encoded as
          <code>Infinity</code>, <code>-Infinity</code>, and <code>NaN</code>.
          Otherwise, this value is encoded as a number.
        </p>
      </td>
      <td>
        SQL input: <code>1.0</code><br />
        JSON output: <code>1</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input: <code>"+inf"</code><br />
        JSON output: <code>"Infinity"</code><br />
        <hr />
        SQL input: <code>"-inf"</code><br />
        JSON output: <code>"-Infinity"</code><br />
        <hr />
        SQL input: <code>"NaN"</code><br />
        JSON output: <code>"NaN"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>STRING</td>
      <td>
        <p>string</p>
        <p>
          Encoded as a string, escaped according to the JSON standard.
          Specifically, <code>"</code>, <code>\</code>, and the control
          characters from <code>U+0000</code> to <code>U+001F</code> are
          escaped.
        </p>
      </td>
      <td>
        SQL input: <code>"abc"</code><br />
        JSON output: <code>"abc"</code><br />
        <hr />
        SQL input: <code>"\"abc\""</code><br />
        JSON output: <code>"\"abc\""</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>BYTES</td>
      <td>
        <p>string</p>
        <p>Uses RFC 4648 Base64 data encoding.</p>
      </td>
      <td>
        SQL input: <code>b"Google"</code><br />
        JSON output: <code>"R29vZ2xl"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>ENUM</td>
      <td>
        <p>string</p>
        <p>
          Invalid enum values are encoded as their number, such as 0 or 42.
        </p>
      </td>
      <td>
        SQL input: <code>Color.Red</code><br />
        JSON output: <code>"Red"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>DATE</td>
      <td>string</td>
      <td>
        SQL input: <code>DATE '2017-03-06'</code><br />
        JSON output: <code>"2017-03-06"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>TIMESTAMP</td>
      <td>
        <p>string</p>
        <p>
          Encoded as ISO 8601 date and time, where T separates the date and
          time and Z (Zulu/UTC) represents the time zone.
        </p>
      </td>
      <td>
        SQL input: <code>TIMESTAMP '2017-03-06 12:34:56.789012'</code><br />
        JSON output: <code>"2017-03-06T12:34:56.789012Z"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>DATETIME</td>
      <td>
        <p>string</p>
        <p>
          Encoded as ISO 8601 date and time, where T separates the date and
          time.
        </p>
      </td>
      <td>
        SQL input: <code>DATETIME '2017-03-06 12:34:56.789012'</code><br />
        JSON output: <code>"2017-03-06T12:34:56.789012"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>TIME</td>
      <td>
        <p>string</p>
        <p>Encoded as ISO 8601 time.</p>
      </td>
      <td>
        SQL input: <code>TIME '12:34:56.789012'</code><br />
        JSON output: <code>"12:34:56.789012"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>JSON</td>
      <td>
        <p>data of the input JSON</p>
      </td>
      <td>
        SQL input: <code>JSON '{"item": "pen", "price": 10}'</code><br />
        JSON output: <code>{"item":"pen", "price":10}</code><br />
        <hr />
        SQL input:<code>[1, 2, 3]</code><br />
        JSON output:<code>[1, 2, 3]</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>ARRAY</td>
      <td>
        <p>array</p>
        <p>
          Can contain zero or more elements.
        </p>
      </td>
      <td>
        SQL input: <code>["red", "blue", "green"]</code><br />
        JSON output: <code>["red","blue","green"]</code><br />
        <hr />
        SQL input:<code>[1, 2, 3]</code><br />
        JSON output:<code>[1,2,3]</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>STRUCT</td>
      <td>
        <p>object</p>
        <p>
          The object can contain zero or more key/value pairs.
          Each value is formatted according to its type.
        </p>
        <p>
          For <code>TO_JSON</code>, a field is
          included in the output string and any duplicates of this field are
          omitted.
          For <code>TO_JSON_STRING</code>,
          a field and any duplicates of this field are included in the
          output string.
        </p>
        <p>
          Anonymous fields are represented with <code>""</code>.
        </p>
        <p>
          Invalid UTF-8 field names might result in unparseable JSON. String
          values are escaped according to the JSON standard. Specifically,
          <code>"</code>, <code>\</code>, and the control characters from
          <code>U+0000</code> to <code>U+001F</code> are escaped.
        </p>
      </td>
      <td>
        SQL input: <code>STRUCT(12 AS purchases, TRUE AS inStock)</code><br />
        JSON output: <code>{"inStock": true,"purchases":12}</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>PROTO</td>
      <td>
        <p>object</p>
        <p>
          The object can contain zero or more key/value pairs.
          Each value is formatted according to its type.
        </p>
        <p>
          Field names with underscores are converted to camel-case in accordance
          with
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. Field values are formatted according to
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. If a <code>field_value</code> is a
          non-empty repeated field or submessage, elements/fields are indented
          to the appropriate level.
        </p>
        <ul>
          <li>
            Field names that are not valid UTF-8 might result in unparseable
            JSON.
          </li>
          <li>Field annotations are ignored.</li>
          <li>Repeated fields are represented as arrays.</li>
          <li>Submessages are formatted as values of PROTO type.</li>
          <li>
            Extension fields are included in the output, where the extension
            field name is enclosed in brackets and prefixed with the full name
            of the extension type.
          </li>
          
        </ul>
      </td>
      <td>
        SQL input: <code>NEW Item(12 AS purchases,TRUE AS in_Stock)</code><br />
        JSON output: <code>{"purchases":12,"inStock": true}</code><br />
      </td>
    </tr>
    
  </tbody>
</table>

### JSONPath format 
<a id="JSONPath_format"></a>

With the JSONPath format, you can identify the values you want to
obtain from a JSON-formatted string. The JSONPath format supports these
operators:

<table>
  <thead>
    <tr>
      <th>Operator</th>
      <th width='300px'>Description</th>
      <th>Examples</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>$</code></td>
      <td>
        Root object or element. The JSONPath format must start with this
        operator, which refers to the outermost level of the
        JSON-formatted string.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$"</code>
        </p>
        <p>
          JSON result:<br />
          <code>{"class":{"students":[{"name":"Jane"}]}}</code><br />
        </p>
      </td>
    </tr>
    <tr>
      <td><code>.</code></td>
      <td>
        Child operator. You can identify child values using dot-notation.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$.class.students"</code>
        </p>
        <p>
          JSON result:<br />
          <code>[{"name":"Jane"}]</code>
        </p>
      </td>
    </tr>
    <tr>
      <td><code>[]</code></td>
      <td>
        Subscript operator. If the JSON object is an array, you can use
        brackets to specify the array index.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$.class.students[0]"</code>
        </p>
        <p>
          JSON result:<br />
          <code>{"name":"Jane"}</code>
        </p>
      </td>
    </tr>
  </tbody>
</table>

If a key in a JSON functions contains a JSON format operator, refer to each
JSON function for how to escape them.

A JSON function returns `NULL` if the JSONPath format does not match a value in
a JSON-formatted string. If the selected value for a scalar function is not
scalar, such as an object or an array, the function returns `NULL`. If the
JSONPath format is invalid, an error is produced.

### Differences between the JSON and JSON-formatted STRING types 
<a id="differences_json_and_string"></a>

Many JSON functions accept two input types:

+  [`JSON`][JSON-type] type
+  `STRING` type

The `STRING` version of the extraction functions behaves differently than the
`JSON` version, mainly because `JSON` type values are always validated whereas
JSON-formatted `STRING` type values are not.

#### Non-validation of `STRING` inputs

The following `STRING` is invalid JSON because it is missing a trailing `}`:

```
{"hello": "world"
```

The JSON function reads the input from the beginning and stops as soon as the
field to extract is found, without reading the remainder of the input. A parsing
error is not produced.

With the `JSON` type, however, `JSON '{"hello": "world"'` returns a parsing
error.

For example:

```sql
SELECT JSON_VALUE('{"hello": "world"', "$.hello") AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

```sql
SELECT JSON_VALUE(JSON '{"hello": "world"', "$.hello") AS hello;
-- An error is returned: Invalid JSON literal: syntax error while parsing
-- object - unexpected end of input; expected '}'
```

#### No strict validation of extracted values

In the following examples, duplicated keys are not removed when using a
JSON-formatted string. Similarly, keys order is preserved. For the `JSON`
type, `JSON '{"key": 1, "key": 2}'` will result in `JSON '{"key":1}'` during
parsing.

```sql
SELECT JSON_QUERY('{"key": 1, "key": 2}', "$") AS string;

/*-------------------*
 | string            |
 +-------------------+
 | {"key":1,"key":2} |
 *-------------------*/
```

```sql
SELECT JSON_QUERY(JSON '{"key": 1, "key": 2}', "$") AS json;

/*-----------*
 | json      |
 +-----------+
 | {"key":1} |
 *-----------*/
```

#### JSON `null`

When using a JSON-formatted `STRING` type in a JSON function, a JSON `null`
value is extracted as a SQL `NULL` value.

When using a JSON type in a JSON function, a JSON `null` value returns a JSON
`null` value.

```sql
WITH t AS (
  SELECT '{"name": null}' AS json_string, JSON '{"name": null}' AS json)
SELECT JSON_QUERY(json_string, "$.name") AS name_string,
  JSON_QUERY(json_string, "$.name") IS NULL AS name_string_is_null,
  JSON_QUERY(json, "$.name") AS name_json,
  JSON_QUERY(json, "$.name") IS NULL AS name_json_is_null
FROM t;

/*-------------+---------------------+-----------+-------------------*
 | name_string | name_string_is_null | name_json | name_json_is_null |
 +-------------+---------------------+-----------+-------------------+
 | NULL        | true                | null      | false             |
 *-------------+---------------------+-----------+-------------------*/
```

[JSONPath-format]: #JSONPath_format

[JSON-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#json_type

## Array functions

ZetaSQL supports the following array functions.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#array"><code>ARRAY</code></a>

</td>
  <td>
    Produces an array with one element for each row in a subquery.
  </td>
</tr>

<tr>
  <td><a href="#array_avg"><code>ARRAY_AVG</code></a>

</td>
  <td>
    Gets the average of non-<code>NULL</code> values in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_concat"><code>ARRAY_CONCAT</code></a>

</td>
  <td>
    Concatenates one or more arrays with the same element type into a
    single array.
  </td>
</tr>

<tr>
  <td><a href="#array_filter"><code>ARRAY_FILTER</code></a>

</td>
  <td>
    Takes an array, filters out unwanted elements, and returns the results
    in a new array.
  </td>
</tr>

<tr>
  <td><a href="#array_first"><code>ARRAY_FIRST</code></a>

</td>
  <td>
    Gets the first element in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_includes"><code>ARRAY_INCLUDES</code></a>

</td>
  <td>
    Checks to see if there is an element in the array that is
    equal to a search value.
  </td>
</tr>

<tr>
  <td><a href="#array_includes_all"><code>ARRAY_INCLUDES_ALL</code></a>

</td>
  <td>
    Checks to see if all search values are in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_includes_any"><code>ARRAY_INCLUDES_ANY</code></a>

</td>
  <td>
    Checks to see if any search values are in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_is_distinct"><code>ARRAY_IS_DISTINCT</code></a>

</td>
  <td>
    Checks to see if an array contains no repeated elements.
  </td>
</tr>

<tr>
  <td><a href="#array_last"><code>ARRAY_LAST</code></a>

</td>
  <td>
    Gets the last element in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_length"><code>ARRAY_LENGTH</code></a>

</td>
  <td>
    Gets the number of elements in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_max"><code>ARRAY_MAX</code></a>

</td>
  <td>
    Gets the maximum non-<code>NULL</code> value in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_min"><code>ARRAY_MIN</code></a>

</td>
  <td>
    Gets the minimum non-<code>NULL</code> value in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_reverse"><code>ARRAY_REVERSE</code></a>

</td>
  <td>
    Reverses the order of elements in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_slice"><code>ARRAY_SLICE</code></a>

</td>
  <td>
    Returns an array containing zero or more consecutive elements from an
    input array.
  </td>
</tr>

<tr>
  <td><a href="#array_sum"><code>ARRAY_SUM</code></a>

</td>
  <td>
    Gets the sum of non-<code>NULL</code> values in an array.
  </td>
</tr>

<tr>
  <td><a href="#array_to_string"><code>ARRAY_TO_STRING</code></a>

</td>
  <td>
    Produces a concatenation of the elements in an array as a
    <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#array_transform"><code>ARRAY_TRANSFORM</code></a>

</td>
  <td>
    Transforms the elements of an array, and returns the results in a new
    array.
  </td>
</tr>

<tr>
  <td><a href="#flatten"><code>FLATTEN</code></a>

</td>
  <td>
    Flattens arrays of nested data to create a single flat array.
  </td>
</tr>

<tr>
  <td><a href="#generate_array"><code>GENERATE_ARRAY</code></a>

</td>
  <td>
    Generates an array of values in a range.
  </td>
</tr>

<tr>
  <td><a href="#generate_date_array"><code>GENERATE_DATE_ARRAY</code></a>

</td>
  <td>
    Generates an array of dates in a range.
  </td>
</tr>

<tr>
  <td><a href="#generate_timestamp_array"><code>GENERATE_TIMESTAMP_ARRAY</code></a>

</td>
  <td>
    Generates an array of timestamps in a range.
  </td>
</tr>

  </tbody>
</table>

### `ARRAY`

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
  `ARRAY` function returns an error that ZetaSQL does not support
  `ARRAY`s with elements of type
  [`ARRAY`][array-data-type].
+ If the subquery returns zero rows, the `ARRAY` function returns an empty
`ARRAY`. It never returns a `NULL` `ARRAY`.

**Return type**

`ARRAY`

**Examples**

```sql
SELECT ARRAY
  (SELECT 1 UNION ALL
   SELECT 2 UNION ALL
   SELECT 3) AS new_array;

/*-----------*
 | new_array |
 +-----------+
 | [1, 2, 3] |
 *-----------*/
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

/*------------------------*
 | new_array              |
 +------------------------+
 | [{1, 2, 3}, {4, 5, 6}] |
 *------------------------*/
```

Similarly, to construct an `ARRAY` from a subquery that contains
one or more `ARRAY`s, change the subquery to use `SELECT AS STRUCT`.

```sql
SELECT ARRAY
  (SELECT AS STRUCT [1, 2, 3] UNION ALL
   SELECT AS STRUCT [4, 5, 6]) AS new_array;

/*----------------------------*
 | new_array                  |
 +----------------------------+
 | [{[1, 2, 3]}, {[4, 5, 6]}] |
 *----------------------------*/
```

[subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#subqueries

[datamodel-sql-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#standard_sql_tables

[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#array_type

### `ARRAY_AVG`

```sql
ARRAY_AVG(input_array)
```

**Description**

Returns the average of non-`NULL` values in an array.

Caveats:

+ If the array is `NULL`, empty, or contains only `NULL`s, returns
  `NULL`.
+ If the array contains `NaN`, returns `NaN`.
+ If the array contains `[+|-]Infinity`, returns either `[+|-]Infinity`
  or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

In the input array, `ARRAY<T>`, `T` can represent one of the following
data types:

+ Any numeric input type
+ `INTERVAL`

**Return type**

The return type depends upon `T` in the input array:

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT ARRAY_AVG([0, 2, NULL, 4, 4, 5]) as avg

/*-----*
 | avg |
 +-----+
 | 3   |
 *-----*/
```

### `ARRAY_CONCAT`

```sql
ARRAY_CONCAT(array_expression[, ...])
```

**Description**

Concatenates one or more arrays with the same element type into a single array.

The function returns `NULL` if any input argument is `NULL`.

Note: You can also use the [|| concatenation operator][array-link-to-operators]
to concatenate arrays.

**Return type**

`ARRAY`

**Examples**

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

/*--------------------------------------------------*
 | count_to_six                                     |
 +--------------------------------------------------+
 | [1, 2, 3, 4, 5, 6]                               |
 *--------------------------------------------------*/
```

[array-link-to-operators]: #operators

### `ARRAY_FILTER`

```sql
ARRAY_FILTER(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias -> boolean_expression
    | (element_alias, index_alias) -> boolean_expression
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
  ARRAY_FILTER([1 ,2, 3], e -> e > 1) AS a1,
  ARRAY_FILTER([0, 2, 3], (e, i) -> e > i) AS a2;

/*-------+-------*
 | a1    | a2    |
 +-------+-------+
 | [2,3] | [2,3] |
 *-------+-------*/
```

[lambda-definition]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#lambdas

### `ARRAY_FIRST`

```sql
ARRAY_FIRST(array_expression)
```

**Description**

Takes an array and returns the first element in the array.

Produces an error if the array is empty.

Returns `NULL` if `array_expression` is `NULL`.

Note: To get the last element in an array, see [`ARRAY_LAST`][array-last].

**Return type**

Matches the data type of elements in `array_expression`.

**Example**

```sql
SELECT ARRAY_FIRST(['a','b','c','d']) as first_element

/*---------------*
 | first_element |
 +---------------+
 | a             |
 *---------------*/
```

[array-last]: #array_last

### `ARRAY_INCLUDES`

+   [Signature 1](#array_includes_signature1):
    `ARRAY_INCLUDES(array_to_search, search_value)`
+   [Signature 2](#array_includes_signature2):
    `ARRAY_INCLUDES(array_to_search, lambda_expression)`

#### Signature 1 
<a id="array_includes_signature1"></a>

```sql
ARRAY_INCLUDES(array_to_search, search_value)
```

**Description**

Takes an array and returns `TRUE` if there is an element in the array that is
equal to the search_value.

+   `array_to_search`: The array to search.
+   `search_value`: The element to search for in the array.

Returns `NULL` if `array_to_search` or `search_value` is `NULL`.

**Return type**

`BOOL`

**Example**

In the following example, the query first checks to see if `0` exists in an
array. Then the query checks to see if `1` exists in an array.

```sql
SELECT
  ARRAY_INCLUDES([1, 2, 3], 0) AS a1,
  ARRAY_INCLUDES([1, 2, 3], 1) AS a2;

/*-------+------*
 | a1    | a2   |
 +-------+------+
 | false | true |
 *-------+------*/
```

#### Signature 2 
<a id="array_includes_signature2"></a>

```sql
ARRAY_INCLUDES(array_to_search, lambda_expression)

lambda_expression: element_alias -> boolean_expression
```

**Description**

Takes an array and returns `TRUE` if the lambda expression evaluates to `TRUE`
for any element in the array.

+   `array_to_search`: The array to search.
+   `lambda_expression`: Each element in `array_to_search` is evaluated against
    the [lambda expression][lambda-definition].
+   `element_alias`: An alias that represents an array element.
+   `boolean_expression`: The predicate used to evaluate the array elements.

Returns `NULL` if `array_to_search` is `NULL`.

**Return type**

`BOOL`

**Example**

In the following example, the query first checks to see if any elements that are
greater than 3 exist in an array (`e > 3`). Then the query checks to see if any
any elements that are greater than 0 exist in an array (`e > 0`).

```sql
SELECT
  ARRAY_INCLUDES([1, 2, 3], e -> e > 3) AS a1,
  ARRAY_INCLUDES([1, 2, 3], e -> e > 0) AS a2;

/*-------+------*
 | a1    | a2   |
 +-------+------+
 | false | true |
 *-------+------*/
```

[lambda-definition]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#lambdas

### `ARRAY_INCLUDES_ALL`

```sql
ARRAY_INCLUDES_ALL(array_to_search, search_values)
```

**Description**

Takes an array to search and an array of search values. Returns `TRUE` if all
search values are in the array to search, otherwise returns `FALSE`.

+   `array_to_search`: The array to search.
+   `search_values`: The array that contains the elements to search for.

Returns `NULL` if `array_to_search` or `search_values` is
`NULL`.

**Return type**

`BOOL`

**Example**

In the following example, the query first checks to see if `3`, `4`, and `5`
exists in an array. Then the query checks to see if `4`, `5`, and `6` exists in
an array.

```sql
SELECT
  ARRAY_INCLUDES_ALL([1,2,3,4,5], [3,4,5]) AS a1,
  ARRAY_INCLUDES_ALL([1,2,3,4,5], [4,5,6]) AS a2;

/*------+-------*
 | a1   | a2    |
 +------+-------+
 | true | false |
 *------+-------*/
```

### `ARRAY_INCLUDES_ANY`

```sql
ARRAY_INCLUDES_ANY(array_to_search, search_values)
```

**Description**

Takes an array to search and an array of search values. Returns `TRUE` if any
search values are in the array to search, otherwise returns `FALSE`.

+   `array_to_search`: The array to search.
+   `search_values`: The array that contains the elements to search for.

Returns `NULL` if `array_to_search` or `search_values` is
`NULL`.

**Return type**

`BOOL`

**Example**

In the following example, the query first checks to see if `3`, `4`, or `5`
exists in an array. Then the query checks to see if `4`, `5`, or `6` exists in
an array.

```sql
SELECT
  ARRAY_INCLUDES_ANY([1,2,3], [3,4,5]) AS a1,
  ARRAY_INCLUDES_ANY([1,2,3], [4,5,6]) AS a2;

/*------+-------*
 | a1   | a2    |
 +------+-------+
 | true | false |
 *------+-------*/
```

### `ARRAY_IS_DISTINCT`

```sql
ARRAY_IS_DISTINCT(value)
```

**Description**

Returns `TRUE` if the array contains no repeated elements, using the same
equality comparison logic as `SELECT DISTINCT`.

**Return type**

`BOOL`

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

/*-----------------+-------------*
 | arr             | is_distinct |
 +-----------------+-------------+
 | [1, 2, 3]       | TRUE        |
 | [1, 1, 1]       | FALSE       |
 | [1, 2, NULL]    | TRUE        |
 | [1, 1, NULL]    | FALSE       |
 | [1, NULL, NULL] | FALSE       |
 | []              | TRUE        |
 | NULL            | NULL        |
 *-----------------+-------------*/
```

### `ARRAY_LAST`

```sql
ARRAY_LAST(array_expression)
```

**Description**

Takes an array and returns the last element in the array.

Produces an error if the array is empty.

Returns `NULL` if `array_expression` is `NULL`.

Note: To get the first element in an array, see [`ARRAY_FIRST`][array-first].

**Return type**

Matches the data type of elements in `array_expression`.

**Example**

```sql
SELECT ARRAY_LAST(['a','b','c','d']) as last_element

/*---------------*
 | last_element  |
 +---------------+
 | d             |
 *---------------*/
```

[array-first]: #array_first

### `ARRAY_LENGTH`

```sql
ARRAY_LENGTH(array_expression)
```

**Description**

Returns the size of the array. Returns 0 for an empty array. Returns `NULL` if
the `array_expression` is `NULL`.

**Return type**

`INT64`

**Examples**

```sql
WITH items AS
  (SELECT ["coffee", NULL, "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)
SELECT ARRAY_TO_STRING(list, ', ', 'NULL'), ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;

/*--------------------+------*
 | list               | size |
 +--------------------+------+
 | coffee, NULL, milk | 3    |
 | cake, pie          | 2    |
 *--------------------+------*/
```

### `ARRAY_MAX`

```sql
ARRAY_MAX(input_array)
```

**Description**

Returns the maximum non-`NULL` value in an array.

Caveats:

+ If the array is `NULL`, empty, or contains only `NULL`s, returns
  `NULL`.
+ If the array contains `NaN`, returns `NaN`.

**Supported Argument Types**

In the input array, `ARRAY<T>`, `T` can be an
[orderable data type][data-type-properties].

**Return type**

The same data type as `T` in the input array.

**Examples**

```sql
SELECT ARRAY_MAX([8, 37, NULL, 55, 4]) as max

/*-----*
 | max |
 +-----+
 | 55  |
 *-----*/
```

[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `ARRAY_MIN`

```sql
ARRAY_MIN(input_array)
```

**Description**

Returns the minimum non-`NULL` value in an array.

Caveats:

+ If the array is `NULL`, empty, or contains only `NULL`s, returns
  `NULL`.
+ If the array contains `NaN`, returns `NaN`.

**Supported Argument Types**

In the input array, `ARRAY<T>`, `T` can be an
[orderable data type][data-type-properties].

**Return type**

The same data type as `T` in the input array.

**Examples**

```sql
SELECT ARRAY_MIN([8, 37, NULL, 4, 55]) as min

/*-----*
 | min |
 +-----+
 | 4   |
 *-----*/
```

[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `ARRAY_REVERSE`

```sql
ARRAY_REVERSE(value)
```

**Description**

Returns the input `ARRAY` with elements in reverse order.

**Return type**

`ARRAY`

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

/*-----------+-------------*
 | arr       | reverse_arr |
 +-----------+-------------+
 | [1, 2, 3] | [3, 2, 1]   |
 | [4, 5]    | [5, 4]      |
 | []        | []          |
 *-----------+-------------*/
```

### `ARRAY_SLICE`

```sql
ARRAY_SLICE(array_to_slice, start_offset, end_offset)
```

**Description**

Returns an array containing zero or more consecutive elements from the
input array.

+ `array_to_slice`: The array that contains the elements you want to slice.
+ `start_offset`: The inclusive starting offset.
+ `end_offset`: The inclusive ending offset.

An offset can be positive or negative. A positive offset starts from the
beginning of the input array and is 0-based. A negative offset starts from
the end of the input array. Out-of-bounds offsets are supported. Here are some
examples:

  <table>
  <thead>
    <tr>
      <th width="150px">Input offset</th>
      <th width="200px">Final offset in array</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>[<b>'a'</b>, 'b', 'c', 'd']</td>
      <td>The final offset is <code>0</code>.</td>
    </tr>
    <tr>
      <td>3</td>
      <td>['a', 'b', 'c', <b>'d'</b>]</td>
      <td>The final offset is <code>3</code>.</td>
    </tr>
    <tr>
      <td>5</td>
      <td>['a', 'b', 'c', <b>'d'</b>]</td>
      <td>
        Because the input offset is out of bounds,
        the final offset is <code>3</code> (<code>array length - 1</code>).
      </td>
    </tr>
    <tr>
      <td>-1</td>
      <td>['a', 'b', 'c', <b>'d'</b>]</td>
      <td>
        Because a negative offset is used, the offset starts at the end of the
        array. The final offset is <code>3</code>
        (<code>array length - 1</code>).
      </td>
    </tr>
    <tr>
      <td>-2</td>
      <td>['a', 'b', <b>'c'</b>, 'd']</td>
      <td>
        Because a negative offset is used, the offset starts at the end of the
        array. The final offset is <code>2</code>
        (<code>array length - 2</code>).
      </td>
    </tr>
    <tr>
      <td>-4</td>
      <td>[<b>'a'</b>, 'b', 'c', 'd']</td>
      <td>
        Because a negative offset is used, the offset starts at the end of the
        array. The final offset is <code>0</code>
        (<code>array length - 4</code>).
      </td>
    </tr>
    <tr>
      <td>-5</td>
      <td>[<b>'a'</b>, 'b', 'c', 'd']</td>
      <td>
        Because the offset is negative and out of bounds, the final offset is
        <code>0</code> (<code>array length - array length</code>).
      </td>
    </tr>
  </tbody>
</table>

Additional details:

+ The input array can contain `NULL` elements. `NULL` elements are included
  in the resulting array.
+ Returns `NULL` if `array_to_slice`,  `start_offset`, or `end_offset` is
  `NULL`.
+ Returns an empty array if `array_to_slice` is empty.
+ Returns an empty array if the position of the `start_offset` in the array is
  after the position of the `end_offset`.

**Return type**

`ARRAY`

**Examples**

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, 3) AS result

/*-----------*
 | result    |
 +-----------+
 | [b, c, d] |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -1, 3) AS result

/*-----------*
 | result    |
 +-----------+
 | []        |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, -3) AS result

/*--------*
 | result |
 +--------+
 | [b, c] |
 *--------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -1, -3) AS result

/*-----------*
 | result    |
 +-----------+
 | []        |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -3, -1) AS result

/*-----------*
 | result    |
 +-----------+
 | [c, d, e] |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 3, 3) AS result

/*--------*
 | result |
 +--------+
 | [d]    |
 *--------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -3, -3) AS result

/*--------*
 | result |
 +--------+
 | [c]    |
 *--------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, 30) AS result

/*--------------*
 | result       |
 +--------------+
 | [b, c, d, e] |
 *--------------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, -30) AS result

/*-----------*
 | result    |
 +-----------+
 | []        |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -30, 30) AS result

/*-----------------*
 | result          |
 +-----------------+
 | [a, b, c, d, e] |
 *-----------------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], -30, -5) AS result

/*--------*
 | result |
 +--------+
 | [a]    |
 *--------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 5, 30) AS result

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, NULL) AS result

/*-----------*
 | result    |
 +-----------+
 | NULL      |
 *-----------*/
```

```sql
SELECT ARRAY_SLICE(['a', 'b', NULL, 'd', 'e'], 1, 3) AS result

/*--------------*
 | result       |
 +--------------+
 | [b, NULL, d] |
 *--------------*/
```

### `ARRAY_SUM`

```sql
ARRAY_SUM(input_array)
```

**Description**

Returns the sum of non-`NULL` values in an array.

Caveats:

+ If the array is `NULL`, empty, or contains only `NULL`s, returns
  `NULL`.
+ If the array contains `NaN`, returns `NaN`.
+ If the array contains `[+|-]Infinity`, returns either `[+|-]Infinity`
  or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

In the input array, `ARRAY<T>`, `T` can represent:

+ Any supported numeric data type
+ `INTERVAL`

**Return type**

The return type depends upon `T` in the input array:

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT ARRAY_SUM([1, 2, 3, 4, 5, NULL, 4, 3, 2, 1]) as sum

/*-----*
 | sum |
 +-----+
 | 25  |
 *-----*/
```

### `ARRAY_TO_STRING`

```sql
ARRAY_TO_STRING(array_expression, delimiter[, null_text])
```

**Description**

Returns a concatenation of the elements in `array_expression`
as a `STRING`. The value for `array_expression`
can either be an array of `STRING` or
`BYTES` data types.

If the `null_text` parameter is used, the function replaces any `NULL` values in
the array with the value of `null_text`.

If the `null_text` parameter is not used, the function omits the `NULL` value
and its preceding delimiter.

**Return type**

`STRING`

**Examples**

```sql
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)

SELECT ARRAY_TO_STRING(list, '--') AS text
FROM items;

/*--------------------------------*
 | text                           |
 +--------------------------------+
 | coffee--tea--milk              |
 | cake--pie                      |
 *--------------------------------*/
```

```sql
WITH items AS
  (SELECT ['coffee', 'tea', 'milk' ] as list
  UNION ALL
  SELECT ['cake', 'pie', NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text
FROM items;

/*--------------------------------*
 | text                           |
 +--------------------------------+
 | coffee--tea--milk              |
 | cake--pie--MISSING             |
 *--------------------------------*/
```

### `ARRAY_TRANSFORM`

```sql
ARRAY_TRANSFORM(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias -> transform_expression
    | (element_alias, index_alias) -> transform_expression
  }
```

**Description**

Takes an array, transforms the elements, and returns the results in a new array.
The output array always has the same length as the input array.

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

`ARRAY`

**Example**

```sql
SELECT
  ARRAY_TRANSFORM([1, 2, 3], e -> e + 1) AS a1,
  ARRAY_TRANSFORM([1, 2, 3], (e, i) -> e + i) AS a2;

/*---------+---------*
 | a1      | a2      |
 +---------+---------+
 | [2,3,4] | [1,3,5] |
 *---------+---------*/
```

[lambda-definition]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#lambdas

### `FLATTEN`

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

/*--------------------------*
 | all_values               |
 +--------------------------+
 | [1, 2, 3, 4, 5, 6, 7, 8] |
 *--------------------------*/
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

/*---------------*
 | second_values |
 +---------------+
 | [2, 5, 8, 11] |
 *---------------*/
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

/*------------*
 | all_prices |
 +------------+
 | [1, 10]    |
 *------------*/
```

For more examples, including how to use protocol buffers with `FLATTEN`, see the
[array elements field access operator][array-el-field-operator].

[flatten-tree-to-array]: https://github.com/google/zetasql/blob/master/docs/arrays.md#flattening_nested_data_into_arrays

[array-el-field-operator]: #array_el_field_operator

### `GENERATE_ARRAY`

```sql
GENERATE_ARRAY(start_expression, end_expression[, step_expression])
```

**Description**

Returns an array of values. The `start_expression` and `end_expression`
parameters determine the inclusive start and end of the array.

The `GENERATE_ARRAY` function accepts the following data types as inputs:

+ `INT64`
+ `UINT64`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `DOUBLE`

The `step_expression` parameter determines the increment used to
generate array values. The default value for this parameter is `1`.

This function returns an error if `step_expression` is set to 0, or if any
input is `NaN`.

If any argument is `NULL`, the function will return a `NULL` array.

**Return Data Type**

`ARRAY`

**Examples**

The following returns an array of integers, with a default step of 1.

```sql
SELECT GENERATE_ARRAY(1, 5) AS example_array;

/*-----------------*
 | example_array   |
 +-----------------+
 | [1, 2, 3, 4, 5] |
 *-----------------*/
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_ARRAY(0, 10, 3) AS example_array;

/*---------------*
 | example_array |
 +---------------+
 | [0, 3, 6, 9]  |
 *---------------*/
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_ARRAY(10, 0, -3) AS example_array;

/*---------------*
 | example_array |
 +---------------+
 | [10, 7, 4, 1] |
 *---------------*/
```

The following returns an array using the same value for the `start_expression`
and `end_expression`.

```sql
SELECT GENERATE_ARRAY(4, 4, 10) AS example_array;

/*---------------*
 | example_array |
 +---------------+
 | [4]           |
 *---------------*/
```

The following returns an empty array, because the `start_expression` is greater
than the `end_expression`, and the `step_expression` value is positive.

```sql
SELECT GENERATE_ARRAY(10, 0, 3) AS example_array;

/*---------------*
 | example_array |
 +---------------+
 | []            |
 *---------------*/
```

The following returns a `NULL` array because `end_expression` is `NULL`.

```sql
SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array;

/*---------------*
 | example_array |
 +---------------+
 | NULL          |
 *---------------*/
```

The following returns multiple arrays.

```sql
SELECT GENERATE_ARRAY(start, 5) AS example_array
FROM UNNEST([3, 4, 5]) AS start;

/*---------------*
 | example_array |
 +---------------+
 | [3, 4, 5]     |
 | [4, 5]        |
 | [5]           |
 +---------------*/
```

### `GENERATE_DATE_ARRAY`

```sql
GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])
```

**Description**

Returns an array of dates. The `start_date` and `end_date`
parameters determine the inclusive start and end of the array.

The `GENERATE_DATE_ARRAY` function accepts the following data types as inputs:

+ `start_date` must be a `DATE`.
+ `end_date` must be a `DATE`.
+ `INT64_expr` must be an `INT64`.
+ `date_part` must be either DAY, WEEK, MONTH, QUARTER, or YEAR.

The `INT64_expr` parameter determines the increment used to generate dates. The
default value for this parameter is 1 day.

This function returns an error if `INT64_expr` is set to 0.

**Return Data Type**

`ARRAY` containing 0 or more `DATE` values.

**Examples**

The following returns an array of dates, with a default step of 1.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;

/*--------------------------------------------------*
 | example                                          |
 +--------------------------------------------------+
 | [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
 *--------------------------------------------------*/
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_DATE_ARRAY(
 '2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example;

/*--------------------------------------*
 | example                              |
 +--------------------------------------+
 | [2016-10-05, 2016-10-07, 2016-10-09] |
 *--------------------------------------*/
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL -3 DAY) AS example;

/*--------------------------*
 | example                  |
 +--------------------------+
 | [2016-10-05, 2016-10-02] |
 *--------------------------*/
```

The following returns an array using the same value for the `start_date`and
`end_date`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-05', INTERVAL 8 DAY) AS example;

/*--------------*
 | example      |
 +--------------+
 | [2016-10-05] |
 *--------------*/
```

The following returns an empty array, because the `start_date` is greater
than the `end_date`, and the `step` value is positive.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL 1 DAY) AS example;

/*---------*
 | example |
 +---------+
 | []      |
 *---------*/
```

The following returns a `NULL` array, because one of its inputs is
`NULL`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example;

/*---------*
 | example |
 +---------+
 | NULL    |
 *---------*/
```

The following returns an array of dates, using MONTH as the `date_part`
interval:

```sql
SELECT GENERATE_DATE_ARRAY('2016-01-01',
  '2016-12-31', INTERVAL 2 MONTH) AS example;

/*--------------------------------------------------------------------------*
 | example                                                                  |
 +--------------------------------------------------------------------------+
 | [2016-01-01, 2016-03-01, 2016-05-01, 2016-07-01, 2016-09-01, 2016-11-01] |
 *--------------------------------------------------------------------------*/
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

/*--------------------------------------------------------------*
 | date_range                                                   |
 +--------------------------------------------------------------+
 | [2016-01-01, 2016-01-08, 2016-01-15, 2016-01-22, 2016-01-29] |
 | [2016-04-01, 2016-04-08, 2016-04-15, 2016-04-22, 2016-04-29] |
 | [2016-07-01, 2016-07-08, 2016-07-15, 2016-07-22, 2016-07-29] |
 | [2016-10-01, 2016-10-08, 2016-10-15, 2016-10-22, 2016-10-29] |
 *--------------------------------------------------------------*/
```

### `GENERATE_TIMESTAMP_ARRAY`

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

An `ARRAY` containing 0 or more `TIMESTAMP` values.

**Examples**

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1 day.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00',
                                INTERVAL 1 DAY) AS timestamp_array;

/*--------------------------------------------------------------------------*
 | timestamp_array                                                          |
 +--------------------------------------------------------------------------+
 | [2016-10-05 00:00:00+00, 2016-10-06 00:00:00+00, 2016-10-07 00:00:00+00] |
 *--------------------------------------------------------------------------*/
```

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1
second.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:02',
                                INTERVAL 1 SECOND) AS timestamp_array;

/*--------------------------------------------------------------------------*
 | timestamp_array                                                          |
 +--------------------------------------------------------------------------+
 | [2016-10-05 00:00:00+00, 2016-10-05 00:00:01+00, 2016-10-05 00:00:02+00] |
 *--------------------------------------------------------------------------*/
```

The following example returns an `ARRAY` of `TIMESTAMPS` with a negative
interval.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-01 00:00:00',
                                INTERVAL -2 DAY) AS timestamp_array;

/*--------------------------------------------------------------------------*
 | timestamp_array                                                          |
 +--------------------------------------------------------------------------+
 | [2016-10-06 00:00:00+00, 2016-10-04 00:00:00+00, 2016-10-02 00:00:00+00] |
 *--------------------------------------------------------------------------*/
```

The following example returns an `ARRAY` with a single element, because
`start_timestamp` and `end_timestamp` have the same value.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

/*--------------------------*
 | timestamp_array          |
 +--------------------------+
 | [2016-10-05 00:00:00+00] |
 *--------------------------*/
```

The following example returns an empty `ARRAY`, because `start_timestamp` is
later than `end_timestamp`.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

/*-----------------*
 | timestamp_array |
 +-----------------+
 | []              |
 *-----------------*/
```

The following example returns a null `ARRAY`, because one of the inputs is
`NULL`.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', NULL, INTERVAL 1 HOUR)
  AS timestamp_array;

/*-----------------*
 | timestamp_array |
 +-----------------+
 | NULL            |
 *-----------------*/
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

/*--------------------------------------------------------------------------*
 | timestamp_array                                                          |
 +--------------------------------------------------------------------------+
 | [2016-10-05 00:00:00+00, 2016-10-05 01:00:00+00, 2016-10-05 02:00:00+00] |
 | [2016-10-05 12:00:00+00, 2016-10-05 13:00:00+00, 2016-10-05 14:00:00+00] |
 | [2016-10-05 23:59:00+00, 2016-10-06 00:59:00+00, 2016-10-06 01:59:00+00] |
 *--------------------------------------------------------------------------*/
```

### OFFSET and ORDINAL

For information about using `OFFSET` and `ORDINAL` with arrays, see
[Array subscript operator][array-subscript-operator] and [Accessing array
elements][accessing-array-elements].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[array-subscript-operator]: #array_subscript_operator

[accessing-array-elements]: https://github.com/google/zetasql/blob/master/docs/arrays.md#accessing_array_elements

<!-- mdlint on -->

## Date functions

ZetaSQL supports the following date functions.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#current_date"><code>CURRENT_DATE</code></a>

</td>
  <td>
    Returns the current date as a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#date"><code>DATE</code></a>

</td>
  <td>
    Constructs a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#date_add"><code>DATE_ADD</code></a>

</td>
  <td>
    Adds a specified time interval to a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#date_diff"><code>DATE_DIFF</code></a>

</td>
  <td>
    Gets the number of intervals between two <code>DATE</code> values.
  </td>
</tr>

<tr>
  <td><a href="#date_from_unix_date"><code>DATE_FROM_UNIX_DATE</code></a>

</td>
  <td>
    Interprets an <code>INT64</code> expression as the number of days
    since 1970-01-01.
  </td>
</tr>

<tr>
  <td><a href="#date_sub"><code>DATE_SUB</code></a>

</td>
  <td>
    Subtracts a specified time interval from a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#date_trunc"><code>DATE_TRUNC</code></a>

</td>
  <td>
    Truncates a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#extract"><code>EXTRACT</code></a>

</td>
  <td>
    Extracts part of a date from a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#format_date"><code>FORMAT_DATE</code></a>

</td>
  <td>
    Formats a <code>DATE</code> value according to a specified format string.
  </td>
</tr>

<tr>
  <td><a href="#last_day"><code>LAST_DAY</code></a>

</td>
  <td>
    Gets the last day in a specified time period that contains a
    <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#parse_date"><code>PARSE_DATE</code></a>

</td>
  <td>
    Converts a <code>STRING</code> value to a <code>DATE</code> value.
  </td>
</tr>

<tr>
  <td><a href="#unix_date"><code>UNIX_DATE</code></a>

</td>
  <td>
    Converts a <code>DATE</code> value to the number of days since 1970-01-01.
  </td>
</tr>

  </tbody>
</table>

### `CURRENT_DATE`

```sql
CURRENT_DATE()
```

```sql
CURRENT_DATE(time_zone_expression)
```

```sql
CURRENT_DATE
```

**Description**

Returns the current date as a `DATE` object. Parentheses are optional when
called with no arguments.

This function supports the following arguments:

+ `time_zone_expression`: A `STRING` expression that represents a
  [time zone][date-timezone-definitions]. If no time zone is specified, the
  default time zone, which is implementation defined, is used. If this expression is
  used and it evaluates to `NULL`, this function returns `NULL`.

The current date is recorded at the start of the query
statement which contains this function, not when this specific function is
evaluated.

**Return Data Type**

`DATE`

**Examples**

The following query produces the current date in the default time zone:

```sql
SELECT CURRENT_DATE() AS the_date;

/*--------------*
 | the_date     |
 +--------------+
 | 2016-12-25   |
 *--------------*/
```

The following queries produce the current date in a specified time zone:

```sql
SELECT CURRENT_DATE('America/Los_Angeles') AS the_date;

/*--------------*
 | the_date     |
 +--------------+
 | 2016-12-25   |
 *--------------*/
```

```sql
SELECT CURRENT_DATE('-08') AS the_date;

/*--------------*
 | the_date     |
 +--------------+
 | 2016-12-25   |
 *--------------*/
```

The following query produces the current date in the default time zone.
Parentheses are not needed if the function has no arguments.

```sql
SELECT CURRENT_DATE AS the_date;

/*--------------*
 | the_date     |
 +--------------+
 | 2016-12-25   |
 *--------------*/
```

When a column named `current_date` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][date-range-variables]. For example, the
following query will select the function in the `the_date` column and the table
column in the `current_date` column.

```sql
WITH t AS (SELECT 'column value' AS `current_date`)
SELECT current_date() AS the_date, t.current_date FROM t;

/*------------+--------------*
 | the_date   | current_date |
 +------------+--------------+
 | 2016-12-25 | column value |
 *------------+--------------*/
```

[date-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[date-timezone-definitions]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time_zones

### `DATE`

```sql
DATE(year, month, day)
```

```sql
DATE(timestamp_expression)
```

```sql
DATE(timestamp_expression, time_zone_expression)
```

```
DATE(datetime_expression)
```

**Description**

Constructs or extracts a date.

This function supports the following arguments:

+ `year`: The `INT64` value for year.
+ `month`: The `INT64` value for month.
+ `day`: The `INT64` value for day.
+ `timestamp_expression`: A `TIMESTAMP` expression that contains the date.
+ `time_zone_expression`: A `STRING` expression that represents a
  [time zone][date-timezone-definitions]. If no time zone is specified with
  `timestamp_expression`, the default time zone, which is implementation defined, is
  used.
+ `datetime_expression`: A `DATETIME` expression that contains the date.

**Return Data Type**

`DATE`

**Example**

```sql
SELECT
  DATE(2016, 12, 25) AS date_ymd,
  DATE(DATETIME '2016-12-25 23:59:59') AS date_dt,
  DATE(TIMESTAMP '2016-12-25 05:30:00+07', 'America/Los_Angeles') AS date_tstz;

/*------------+------------+------------*
 | date_ymd   | date_dt    | date_tstz  |
 +------------+------------+------------+
 | 2016-12-25 | 2016-12-25 | 2016-12-24 |
 *------------+------------+------------*/
```

[date-timezone-definitions]: #timezone_definitions

### `DATE_ADD`

```sql
DATE_ADD(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds a specified time interval to a DATE.

`DATE_ADD` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_ADD(DATE '2008-12-25', INTERVAL 5 DAY) AS five_days_later;

/*--------------------*
 | five_days_later    |
 +--------------------+
 | 2008-12-30         |
 *--------------------*/
```

### `DATE_DIFF`

```sql
DATE_DIFF(date_expression_a, date_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
`DATE` objects (`date_expression_a` - `date_expression_b`).
If the first `DATE` is earlier than the second one,
the output is negative.

`DATE_DIFF` supports the following `date_part` values:

+  `DAY`
+  `WEEK` This date part begins on Sunday.
+  `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
   `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
   boundaries. ISO weeks begin on Monday.
+  `MONTH`, except when the first two arguments are `TIMESTAMP` objects.
+  `QUARTER`
+  `YEAR`
+  `ISOYEAR`: Uses the [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

INT64

**Example**

```sql
SELECT DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY) AS days_diff;

/*-----------*
 | days_diff |
 +-----------+
 | 559       |
 *-----------*/
```

```sql
SELECT
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', DAY) AS days_diff,
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', WEEK) AS weeks_diff;

/*-----------+------------*
 | days_diff | weeks_diff |
 +-----------+------------+
 | 1         | 1          |
 *-----------+------------*/
```

The example above shows the result of `DATE_DIFF` for two days in succession.
`DATE_DIFF` with the date part `WEEK` returns 1 because `DATE_DIFF` counts the
number of date part boundaries in this range of dates. Each `WEEK` begins on
Sunday, so there is one date part boundary between Saturday, 2017-10-14
and Sunday, 2017-10-15.

The following example shows the result of `DATE_DIFF` for two dates in different
years. `DATE_DIFF` with the date part `YEAR` returns 3 because it counts the
number of Gregorian calendar year boundaries between the two dates. `DATE_DIFF`
with the date part `ISOYEAR` returns 2 because the second date belongs to the
ISO year 2015. The first Thursday of the 2015 calendar year was 2015-01-01, so
the ISO year 2015 begins on the preceding Monday, 2014-12-29.

```sql
SELECT
  DATE_DIFF('2017-12-30', '2014-12-30', YEAR) AS year_diff,
  DATE_DIFF('2017-12-30', '2014-12-30', ISOYEAR) AS isoyear_diff;

/*-----------+--------------*
 | year_diff | isoyear_diff |
 +-----------+--------------+
 | 3         | 2            |
 *-----------+--------------*/
```

The following example shows the result of `DATE_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATE_DIFF` with the date part `WEEK` returns 0 because this date part
uses weeks that begin on Sunday. `DATE_DIFF` with the date part `WEEK(MONDAY)`
returns 1. `DATE_DIFF` with the date part `ISOWEEK` also returns 1 because
ISO weeks begin on Monday.

```sql
SELECT
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

/*-----------+-------------------+--------------*
 | week_diff | week_weekday_diff | isoweek_diff |
 +-----------+-------------------+--------------+
 | 0         | 1                 | 1            |
 *-----------+-------------------+--------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `DATE_FROM_UNIX_DATE`

```sql
DATE_FROM_UNIX_DATE(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of days since 1970-01-01.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_FROM_UNIX_DATE(14238) AS date_from_epoch;

/*-----------------*
 | date_from_epoch |
 +-----------------+
 | 2008-12-25      |
 *-----------------+*/
```

### `DATE_SUB`

```sql
DATE_SUB(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts a specified time interval from a DATE.

`DATE_SUB` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_SUB(DATE '2008-12-25', INTERVAL 5 DAY) AS five_days_ago;

/*---------------*
 | five_days_ago |
 +---------------+
 | 2008-12-20    |
 *---------------*/
```

### `DATE_TRUNC`

```sql
DATE_TRUNC(date_expression, date_part)
```

**Description**

Truncates a `DATE` value to the granularity of `date_part`. The `DATE` value
is always rounded to the beginning of `date_part`, which can be one of the
following:

+ `DAY`: The day in the Gregorian calendar year that contains the
  `DATE` value.
+ `WEEK`: The first day of the week in the week that contains the
  `DATE` value. Weeks begin on Sundays. `WEEK` is equivalent to
  `WEEK(SUNDAY)`.
+ `WEEK(WEEKDAY)`: The first day of the week in the week that contains the
  `DATE` value. Weeks begin on `WEEKDAY`. `WEEKDAY` must be one of the
   following: `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`,
   or `SATURDAY`.
+ `ISOWEEK`: The first day of the [ISO 8601 week][ISO-8601-week] in the
  ISO week that contains the `DATE` value. The ISO week begins on
  Monday. The first ISO week of each ISO year contains the first Thursday of the
  corresponding Gregorian calendar year.
+ `MONTH`: The first day of the month in the month that contains the
  `DATE` value.
+ `QUARTER`: The first day of the quarter in the quarter that contains the
  `DATE` value.
+ `YEAR`: The first day of the year in the year that contains the
  `DATE` value.
+ `ISOYEAR`: The first day of the [ISO 8601][ISO-8601] week-numbering year
  in the ISO year that contains the `DATE` value. The ISO year is the
  Monday of the first week whose Thursday belongs to the corresponding
  Gregorian calendar year.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

<!-- mdlint on -->

**Return Data Type**

DATE

**Examples**

```sql
SELECT DATE_TRUNC(DATE '2008-12-25', MONTH) AS month;

/*------------*
 | month      |
 +------------+
 | 2008-12-01 |
 *------------*/
```

In the following example, the original date falls on a Sunday. Because
the `date_part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATE` for the
preceding Monday.

```sql
SELECT date AS original, DATE_TRUNC(date, WEEK(MONDAY)) AS truncated
FROM (SELECT DATE('2017-11-05') AS date);

/*------------+------------*
 | original   | truncated  |
 +------------+------------+
 | 2017-11-05 | 2017-10-30 |
 *------------+------------*/
```

In the following example, the original `date_expression` is in the Gregorian
calendar year 2015. However, `DATE_TRUNC` with the `ISOYEAR` date part
truncates the `date_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `date_expression` 2015-06-15 is
2014-12-29.

```sql
SELECT
  DATE_TRUNC('2015-06-15', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATE '2015-06-15') AS isoyear_number;

/*------------------+----------------*
 | isoyear_boundary | isoyear_number |
 +------------------+----------------+
 | 2014-12-29       | 2015           |
 *------------------+----------------*/
```

### `EXTRACT`

```sql
EXTRACT(part FROM date_expression)
```

**Description**

Returns the value corresponding to the specified date part. The `part` must
be one of:

+   `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day
    of the week.
+   `DAY`
+   `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53]. Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of the date in the range [0, 53].
  Weeks begin on `WEEKDAY`. Dates prior to
  the first `WEEKDAY` of the year are in week 0. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `date_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+   `MONTH`
+   `QUARTER`: Returns values in the range [1,4].
+   `YEAR`
+   `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
    week-numbering year, which is the Gregorian calendar year containing the
    Thursday of the week to which `date_expression` belongs.

**Return Data Type**

INT64

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
date part.

```sql
SELECT EXTRACT(DAY FROM DATE '2013-12-25') AS the_day;

/*---------*
 | the_day |
 +---------+
 | 25      |
 *---------*/
```

In the following example, `EXTRACT` returns values corresponding to different
date parts from a column of dates near the end of the year.

```sql
SELECT
  date,
  EXTRACT(ISOYEAR FROM date) AS isoyear,
  EXTRACT(ISOWEEK FROM date) AS isoweek,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(WEEK FROM date) AS week
FROM UNNEST(GENERATE_DATE_ARRAY('2015-12-23', '2016-01-09')) AS date
ORDER BY date;

/*------------+---------+---------+------+------*
 | date       | isoyear | isoweek | year | week |
 +------------+---------+---------+------+------+
 | 2015-12-23 | 2015    | 52      | 2015 | 51   |
 | 2015-12-24 | 2015    | 52      | 2015 | 51   |
 | 2015-12-25 | 2015    | 52      | 2015 | 51   |
 | 2015-12-26 | 2015    | 52      | 2015 | 51   |
 | 2015-12-27 | 2015    | 52      | 2015 | 52   |
 | 2015-12-28 | 2015    | 53      | 2015 | 52   |
 | 2015-12-29 | 2015    | 53      | 2015 | 52   |
 | 2015-12-30 | 2015    | 53      | 2015 | 52   |
 | 2015-12-31 | 2015    | 53      | 2015 | 52   |
 | 2016-01-01 | 2015    | 53      | 2016 | 0    |
 | 2016-01-02 | 2015    | 53      | 2016 | 0    |
 | 2016-01-03 | 2015    | 53      | 2016 | 1    |
 | 2016-01-04 | 2016    | 1       | 2016 | 1    |
 | 2016-01-05 | 2016    | 1       | 2016 | 1    |
 | 2016-01-06 | 2016    | 1       | 2016 | 1    |
 | 2016-01-07 | 2016    | 1       | 2016 | 1    |
 | 2016-01-08 | 2016    | 1       | 2016 | 1    |
 | 2016-01-09 | 2016    | 1       | 2016 | 1    |
 *------------+---------+---------+------+------*/
```

In the following example, `date_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT DATE('2017-11-05') AS date)
SELECT
  date,
  EXTRACT(WEEK(SUNDAY) FROM date) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM date) AS week_monday FROM table;

/*------------+-------------+-------------*
 | date       | week_sunday | week_monday |
 +------------+-------------+-------------+
 | 2017-11-05 | 45          | 44          |
 *------------+-------------+-------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `FORMAT_DATE`

```sql
FORMAT_DATE(format_string, date_expr)
```

**Description**

Formats the `date_expr` according to the specified `format_string`.

See [Supported Format Elements For DATE][date-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Examples**

```sql
SELECT FORMAT_DATE('%x', DATE '2008-12-25') AS US_format;

/*------------*
 | US_format  |
 +------------+
 | 12/25/08   |
 *------------*/
```

```sql
SELECT FORMAT_DATE('%b-%d-%Y', DATE '2008-12-25') AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec-25-2008 |
 *-------------*/
```

```sql
SELECT FORMAT_DATE('%b %Y', DATE '2008-12-25') AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec 2008    |
 *-------------*/
```

[date-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `LAST_DAY`

```sql
LAST_DAY(date_expression[, date_part])
```

**Description**

Returns the last day from a date expression. This is commonly used to return
the last day of the month.

You can optionally specify the date part for which the last day is returned.
If this parameter is not used, the default value is `MONTH`.
`LAST_DAY` supports the following values for `date_part`:

+  `YEAR`
+  `QUARTER`
+  `MONTH`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `WEEK(<WEEKDAY>)`. `<WEEKDAY>` represents the starting day of the week.
   Valid values are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`. Uses [ISO 8601][ISO-8601-week] week boundaries. ISO weeks begin
   on Monday.
+  `ISOYEAR`. Uses the [ISO 8601][ISO-8601] week-numbering year boundary.
   The ISO year boundary is the Monday of the first week whose Thursday belongs
   to the corresponding Gregorian calendar year.

**Return Data Type**

`DATE`

**Example**

These both return the last day of the month:

```sql
SELECT LAST_DAY(DATE '2008-11-25', MONTH) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-30 |
 *------------*/
```

```sql
SELECT LAST_DAY(DATE '2008-11-25') AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-30 |
 *------------*/
```

This returns the last day of the year:

```sql
SELECT LAST_DAY(DATE '2008-11-25', YEAR) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-12-31 |
 *------------*/
```

This returns the last day of the week for a week that starts on a Sunday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(SUNDAY)) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-15 |
 *------------*/
```

This returns the last day of the week for a week that starts on a Monday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(MONDAY)) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-16 |
 *------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `PARSE_DATE`

```sql
PARSE_DATE(format_string, date_string)
```

**Description**

Converts a [string representation of date][date-format] to a
`DATE` object.

`format_string` contains the [format elements][date-format-elements]
that define how `date_string` is formatted. Each element in
`date_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `date_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008')

-- This produces an error because the year element is in different locations.
SELECT PARSE_DATE('%Y %A %b %e', 'Thursday Dec 25 2008')

-- This produces an error because one of the year elements is missing.
SELECT PARSE_DATE('%A %b %e', 'Thursday Dec 25 2008')

-- This works because %F can find all matching elements in date_string.
SELECT PARSE_DATE('%F', '2000-12-30')
```

When using `PARSE_DATE`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01`.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the date string. In
  addition, leading and trailing white spaces in the date string are always
  allowed -- even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones.

**Return Data Type**

DATE

**Examples**

This example converts a `MM/DD/YY` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE('%x', '12/25/08') AS parsed;

/*------------*
 | parsed     |
 +------------+
 | 2008-12-25 |
 *------------*/
```

This example converts a `YYYYMMDD` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE('%Y%m%d', '20081225') AS parsed;

/*------------*
 | parsed     |
 +------------+
 | 2008-12-25 |
 *------------*/
```

[date-format]: #format_date

[date-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `UNIX_DATE`

```sql
UNIX_DATE(date_expression)
```

**Description**

Returns the number of days since `1970-01-01`.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_DATE(DATE '2008-12-25') AS days_from_epoch;

/*-----------------*
 | days_from_epoch |
 +-----------------+
 | 14238           |
 *-----------------*/
```

## Datetime functions

ZetaSQL supports the following datetime functions.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#current_datetime"><code>CURRENT_DATETIME</code></a>

</td>
  <td>
    Returns the current date and time as a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#datetime"><code>DATETIME</code></a>

</td>
  <td>
    Constructs a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#datetime_add"><code>DATETIME_ADD</code></a>

</td>
  <td>
    Adds a specified time interval to a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#datetime_diff"><code>DATETIME_DIFF</code></a>

</td>
  <td>
    Gets the number of intervals between two <code>DATETIME</code> values.
  </td>
</tr>

<tr>
  <td><a href="#datetime_sub"><code>DATETIME_SUB</code></a>

</td>
  <td>
    Subtracts a specified time interval from a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#datetime_trunc"><code>DATETIME_TRUNC</code></a>

</td>
  <td>
    Truncates a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#extract"><code>EXTRACT</code></a>

</td>
  <td>
    Extracts part of a date and time from a <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#format_datetime"><code>FORMAT_DATETIME</code></a>

</td>
  <td>
    Formats a <code>DATETIME</code> value according to a specified
    format string.
  </td>
</tr>

<tr>
  <td><a href="#last_day"><code>LAST_DAY</code></a>

</td>
  <td>
    Gets the last day in a specified time period that contains a
    <code>DATETIME</code> value.
  </td>
</tr>

<tr>
  <td><a href="#parse_datetime"><code>PARSE_DATETIME</code></a>

</td>
  <td>
    Converts a <code>STRING</code> value to a <code>DATETIME</code> value.
  </td>
</tr>

  </tbody>
</table>

### `CURRENT_DATETIME`

```sql
CURRENT_DATETIME([time_zone])
```

```sql
CURRENT_DATETIME
```

**Description**

Returns the current time as a `DATETIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `time_zone` parameter.
See [Time zone definitions][datetime-timezone-definitions] for
information on how to specify a time zone.

The current date and time is recorded at the start of the query
statement which contains this function, not when this specific function is
evaluated.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT CURRENT_DATETIME() as now;

/*----------------------------*
 | now                        |
 +----------------------------+
 | 2016-05-19 10:38:47.046465 |
 *----------------------------*/
```

When a column named `current_datetime` is present, the column name and the
function call without parentheses are ambiguous. To ensure the function call,
add parentheses; to ensure the column name, qualify it with its
[range variable][datetime-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_datetime` column.

```sql
WITH t AS (SELECT 'column value' AS `current_datetime`)
SELECT current_datetime() as now, t.current_datetime FROM t;

/*----------------------------+------------------*
 | now                        | current_datetime |
 +----------------------------+------------------+
 | 2016-05-19 10:38:47.046465 | column value     |
 *----------------------------+------------------*/
```

[datetime-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[datetime-timezone-definitions]: #timezone_definitions

### `DATETIME`

```sql
1. DATETIME(year, month, day, hour, minute, second)
2. DATETIME(date_expression[, time_expression])
3. DATETIME(timestamp_expression [, time_zone])
```

**Description**

1. Constructs a `DATETIME` object using `INT64` values
   representing the year, month, day, hour, minute, and second.
2. Constructs a `DATETIME` object using a DATE object and an optional `TIME`
   object.
3. Constructs a `DATETIME` object using a `TIMESTAMP` object. It supports an
   optional parameter to
   [specify a time zone][datetime-timezone-definitions].
   If no time zone is specified, the default time zone, which is implementation defined,
   is used.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME(2008, 12, 25, 05, 30, 00) as datetime_ymdhms,
  DATETIME(TIMESTAMP "2008-12-25 05:30:00+00", "America/Los_Angeles") as datetime_tstz;

/*---------------------+---------------------*
 | datetime_ymdhms     | datetime_tstz       |
 +---------------------+---------------------+
 | 2008-12-25 05:30:00 | 2008-12-24 21:30:00 |
 *---------------------+---------------------*/
```

[datetime-timezone-definitions]: #timezone_definitions

### `DATETIME_ADD`

```sql
DATETIME_ADD(datetime_expression, INTERVAL int64_expression part)
```

**Description**

Adds `int64_expression` units of `part` to the `DATETIME` object.

`DATETIME_ADD` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`. Equivalent to 7 `DAY`s.
+ `MONTH`
+ `QUARTER`
+ `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original DATETIME's day, then the result day is the last day of
the new month.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as later;

/*-----------------------------+------------------------*
 | original_date               | later                  |
 +-----------------------------+------------------------+
 | 2008-12-25 15:30:00         | 2008-12-25 15:40:00    |
 *-----------------------------+------------------------*/
```

### `DATETIME_DIFF`

```sql
DATETIME_DIFF(datetime_expression_a, datetime_expression_b, part)
```

**Description**

Returns the whole number of specified `part` intervals between two
`DATETIME` objects (`datetime_expression_a` - `datetime_expression_b`).
If the first `DATETIME` is earlier than the second one,
the output is negative. Throws an error if the computation overflows the
result type, such as if the difference in
nanoseconds
between the two `DATETIME` objects would overflow an
`INT64` value.

`DATETIME_DIFF` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`: This date part begins on Sunday.
+ `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
  `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
  `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
  boundaries. ISO weeks begin on Monday.
+ `MONTH`, except when the first two arguments are `TIMESTAMP` objects.
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Uses the [ISO 8601][ISO-8601]
  week-numbering year boundary. The ISO year boundary is the Monday of the
  first week whose Thursday belongs to the corresponding Gregorian calendar
  year.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  DATETIME "2010-07-07 10:20:00" as first_datetime,
  DATETIME "2008-12-25 15:30:00" as second_datetime,
  DATETIME_DIFF(DATETIME "2010-07-07 10:20:00",
    DATETIME "2008-12-25 15:30:00", DAY) as difference;

/*----------------------------+------------------------+------------------------*
 | first_datetime             | second_datetime        | difference             |
 +----------------------------+------------------------+------------------------+
 | 2010-07-07 10:20:00        | 2008-12-25 15:30:00    | 559                    |
 *----------------------------+------------------------+------------------------*/
```

```sql
SELECT
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', DAY) as days_diff,
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', WEEK) as weeks_diff;

/*-----------+------------*
 | days_diff | weeks_diff |
 +-----------+------------+
 | 1         | 1          |
 *-----------+------------*/
```

The example above shows the result of `DATETIME_DIFF` for two `DATETIME`s that
are 24 hours apart. `DATETIME_DIFF` with the part `WEEK` returns 1 because
`DATETIME_DIFF` counts the number of part boundaries in this range of
`DATETIME`s. Each `WEEK` begins on Sunday, so there is one part boundary between
Saturday, `2017-10-14 00:00:00` and Sunday, `2017-10-15 00:00:00`.

The following example shows the result of `DATETIME_DIFF` for two dates in
different years. `DATETIME_DIFF` with the date part `YEAR` returns 3 because it
counts the number of Gregorian calendar year boundaries between the two
`DATETIME`s. `DATETIME_DIFF` with the date part `ISOYEAR` returns 2 because the
second `DATETIME` belongs to the ISO year 2015. The first Thursday of the 2015
calendar year was 2015-01-01, so the ISO year 2015 begins on the preceding
Monday, 2014-12-29.

```sql
SELECT
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', YEAR) AS year_diff,
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', ISOYEAR) AS isoyear_diff;

/*-----------+--------------*
 | year_diff | isoyear_diff |
 +-----------+--------------+
 | 3         | 2            |
 *-----------+--------------*/
```

The following example shows the result of `DATETIME_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATETIME_DIFF` with the date part `WEEK` returns 0 because this time
part uses weeks that begin on Sunday. `DATETIME_DIFF` with the date part
`WEEK(MONDAY)` returns 1. `DATETIME_DIFF` with the date part
`ISOWEEK` also returns 1 because ISO weeks begin on Monday.

```sql
SELECT
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

/*-----------+-------------------+--------------*
 | week_diff | week_weekday_diff | isoweek_diff |
 +-----------+-------------------+--------------+
 | 0         | 1                 | 1            |
 *-----------+-------------------+--------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `DATETIME_SUB`

```sql
DATETIME_SUB(datetime_expression, INTERVAL int64_expression part)
```

**Description**

Subtracts `int64_expression` units of `part` from the `DATETIME`.

`DATETIME_SUB` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`. Equivalent to 7 `DAY`s.
+ `MONTH`
+ `QUARTER`
+ `YEAR`

Special handling is required for `MONTH`, `QUARTER`, and `YEAR` parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original `DATETIME`'s day, then the result day is the last day of
the new month.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as earlier;

/*-----------------------------+------------------------*
 | original_date               | earlier                |
 +-----------------------------+------------------------+
 | 2008-12-25 15:30:00         | 2008-12-25 15:20:00    |
 *-----------------------------+------------------------*/
```

### `DATETIME_TRUNC`

```sql
DATETIME_TRUNC(datetime_expression, date_time_part)
```

**Description**

Truncates a `DATETIME` value to the granularity of `date_time_part`.
The `DATETIME` value is always rounded to the beginning of `date_time_part`,
which can be one of the following:

+ `NANOSECOND`: If used, nothing is truncated from the value.
+ `MICROSECOND`: The nearest lessor or equal microsecond.
+ `MILLISECOND`: The nearest lessor or equal millisecond.
+ `SECOND`: The nearest lessor or equal second.
+ `MINUTE`: The nearest lessor or equal minute.
+ `HOUR`: The nearest lessor or equal hour.
+ `DAY`: The day in the Gregorian calendar year that contains the
  `DATETIME` value.
+ `WEEK`: The first day of the week in the week that contains the
  `DATETIME` value. Weeks begin on Sundays. `WEEK` is equivalent to
  `WEEK(SUNDAY)`.
+ `WEEK(WEEKDAY)`: The first day of the week in the week that contains the
  `DATETIME` value. Weeks begin on `WEEKDAY`. `WEEKDAY` must be one of the
   following: `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`,
   or `SATURDAY`.
+ `ISOWEEK`: The first day of the [ISO 8601 week][ISO-8601-week] in the
  ISO week that contains the `DATETIME` value. The ISO week begins on
  Monday. The first ISO week of each ISO year contains the first Thursday of the
  corresponding Gregorian calendar year.
+ `MONTH`: The first day of the month in the month that contains the
  `DATETIME` value.
+ `QUARTER`: The first day of the quarter in the quarter that contains the
  `DATETIME` value.
+ `YEAR`: The first day of the year in the year that contains the
  `DATETIME` value.
+ `ISOYEAR`: The first day of the [ISO 8601][ISO-8601] week-numbering year
  in the ISO year that contains the `DATETIME` value. The ISO year is the
  Monday of the first week whose Thursday belongs to the corresponding
  Gregorian calendar year.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

<!-- mdlint on -->

**Return Data Type**

`DATETIME`

**Examples**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original,
  DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY) as truncated;

/*----------------------------+------------------------*
 | original                   | truncated              |
 +----------------------------+------------------------+
 | 2008-12-25 15:30:00        | 2008-12-25 00:00:00    |
 *----------------------------+------------------------*/
```

In the following example, the original `DATETIME` falls on a Sunday. Because the
`part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATETIME` for the
preceding Monday.

```sql
SELECT
 datetime AS original,
 DATETIME_TRUNC(datetime, WEEK(MONDAY)) AS truncated
FROM (SELECT DATETIME(TIMESTAMP "2017-11-05 00:00:00+00", "UTC") AS datetime);

/*---------------------+---------------------*
 | original            | truncated           |
 +---------------------+---------------------+
 | 2017-11-05 00:00:00 | 2017-10-30 00:00:00 |
 *---------------------+---------------------*/
```

In the following example, the original `datetime_expression` is in the Gregorian
calendar year 2015. However, `DATETIME_TRUNC` with the `ISOYEAR` date part
truncates the `datetime_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `datetime_expression`
2015-06-15 00:00:00 is 2014-12-29.

```sql
SELECT
  DATETIME_TRUNC('2015-06-15 00:00:00', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATETIME '2015-06-15 00:00:00') AS isoyear_number;

/*---------------------+----------------*
 | isoyear_boundary    | isoyear_number |
 +---------------------+----------------+
 | 2014-12-29 00:00:00 | 2015           |
 *---------------------+----------------*/
```

### `EXTRACT`

```sql
EXTRACT(part FROM datetime_expression)
```

**Description**

Returns a value that corresponds to the
specified `part` from a supplied `datetime_expression`.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day of
   of the week.
+ `DAY`
+ `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53].  Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of `datetime_expression` in the
  range [0, 53]. Weeks begin on `WEEKDAY`.
  `datetime`s prior to the first `WEEKDAY` of the year are in week 0. Valid
  values for `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`,
  `THURSDAY`, `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `datetime_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
  week-numbering year, which is the Gregorian calendar year containing the
  Thursday of the week to which `date_expression` belongs.
+ `DATE`
+ `TIME`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`, except in the following cases:

+ If `part` is `DATE`, returns a `DATE` object.
+ If `part` is `TIME`, returns a `TIME` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00)) as hour;

/*------------------*
 | hour             |
 +------------------+
 | 15               |
 *------------------*/
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of datetimes.

```sql
WITH Datetimes AS (
  SELECT DATETIME '2005-01-03 12:34:56' AS datetime UNION ALL
  SELECT DATETIME '2007-12-31' UNION ALL
  SELECT DATETIME '2009-01-01' UNION ALL
  SELECT DATETIME '2009-12-31' UNION ALL
  SELECT DATETIME '2017-01-02' UNION ALL
  SELECT DATETIME '2017-05-26'
)
SELECT
  datetime,
  EXTRACT(ISOYEAR FROM datetime) AS isoyear,
  EXTRACT(ISOWEEK FROM datetime) AS isoweek,
  EXTRACT(YEAR FROM datetime) AS year,
  EXTRACT(WEEK FROM datetime) AS week
FROM Datetimes
ORDER BY datetime;

/*---------------------+---------+---------+------+------*
 | datetime            | isoyear | isoweek | year | week |
 +---------------------+---------+---------+------+------+
 | 2005-01-03 12:34:56 | 2005    | 1       | 2005 | 1    |
 | 2007-12-31 00:00:00 | 2008    | 1       | 2007 | 52   |
 | 2009-01-01 00:00:00 | 2009    | 1       | 2009 | 0    |
 | 2009-12-31 00:00:00 | 2009    | 53      | 2009 | 52   |
 | 2017-01-02 00:00:00 | 2017    | 1       | 2017 | 1    |
 | 2017-05-26 00:00:00 | 2017    | 21      | 2017 | 21   |
 *---------------------+---------+---------+------+------*/
```

In the following example, `datetime_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT DATETIME(TIMESTAMP "2017-11-05 00:00:00+00", "UTC") AS datetime)
SELECT
  datetime,
  EXTRACT(WEEK(SUNDAY) FROM datetime) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM datetime) AS week_monday
FROM table;

/*---------------------+-------------+---------------*
 | datetime            | week_sunday | week_monday   |
 +---------------------+-------------+---------------+
 | 2017-11-05 00:00:00 | 45          | 44            |
 *---------------------+-------------+---------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `FORMAT_DATETIME`

```sql
FORMAT_DATETIME(format_string, datetime_expression)
```

**Description**

Formats a `DATETIME` object according to the specified `format_string`. See
[Supported Format Elements For DATETIME][datetime-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Examples**

```sql
SELECT
  FORMAT_DATETIME("%c", DATETIME "2008-12-25 15:30:00")
  AS formatted;

/*--------------------------*
 | formatted                |
 +--------------------------+
 | Thu Dec 25 15:30:00 2008 |
 *--------------------------*/
```

```sql
SELECT
  FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec-25-2008 |
 *-------------*/
```

```sql
SELECT
  FORMAT_DATETIME("%b %Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec 2008    |
 *-------------*/
```

[datetime-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `LAST_DAY`

```sql
LAST_DAY(datetime_expression[, date_part])
```

**Description**

Returns the last day from a datetime expression that contains the date.
This is commonly used to return the last day of the month.

You can optionally specify the date part for which the last day is returned.
If this parameter is not used, the default value is `MONTH`.
`LAST_DAY` supports the following values for `date_part`:

+  `YEAR`
+  `QUARTER`
+  `MONTH`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `WEEK(<WEEKDAY>)`. `<WEEKDAY>` represents the starting day of the week.
   Valid values are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`. Uses [ISO 8601][ISO-8601-week] week boundaries. ISO weeks begin
   on Monday.
+  `ISOYEAR`. Uses the [ISO 8601][ISO-8601] week-numbering year boundary.
   The ISO year boundary is the Monday of the first week whose Thursday belongs
   to the corresponding Gregorian calendar year.

**Return Data Type**

`DATE`

**Example**

These both return the last day of the month:

```sql
SELECT LAST_DAY(DATETIME '2008-11-25', MONTH) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-30 |
 *------------*/
```

```sql
SELECT LAST_DAY(DATETIME '2008-11-25') AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-30 |
 *------------*/
```

This returns the last day of the year:

```sql
SELECT LAST_DAY(DATETIME '2008-11-25 15:30:00', YEAR) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-12-31 |
 *------------*/
```

This returns the last day of the week for a week that starts on a Sunday:

```sql
SELECT LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(SUNDAY)) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-15 |
 *------------*/
```

This returns the last day of the week for a week that starts on a Monday:

```sql
SELECT LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(MONDAY)) AS last_day

/*------------*
 | last_day   |
 +------------+
 | 2008-11-16 |
 *------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

### `PARSE_DATETIME`

```sql
PARSE_DATETIME(format_string, datetime_string)
```
**Description**

Converts a [string representation of a datetime][datetime-format] to a
`DATETIME` object.

`format_string` contains the [format elements][datetime-format-elements]
that define how `datetime_string` is formatted. Each element in
`datetime_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `datetime_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")

-- This produces an error because the year element is in different locations.
SELECT PARSE_DATETIME("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This produces an error because one of the year elements is missing.
SELECT PARSE_DATETIME("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This works because %c can find all matching elements in datetime_string.
SELECT PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008")
```

`PARSE_DATETIME` parses `string` according to the following rules:

+ **Unspecified fields.** Any unspecified field is initialized from
  `1970-01-01 00:00:00.0`. For example, if the year is unspecified then it
  defaults to `1970`.
+ **Case insensitivity.** Names, such as `Monday` and `February`,
  are case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the
  `DATETIME` string. Leading and trailing
  white spaces in the `DATETIME` string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two or more format elements have overlapping
  information, the last one generally overrides any earlier ones, with some
  exceptions. For example, both `%F` and `%Y` affect the year, so the earlier
  element overrides the later. See the descriptions
  of `%s`, `%C`, and `%y` in
  [Supported Format Elements For DATETIME][datetime-format-elements].
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`DATETIME`

**Examples**

The following examples parse a `STRING` literal as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1998-10-18 13:45:55') AS datetime;

/*---------------------*
 | datetime            |
 +---------------------+
 | 1998-10-18 13:45:55 |
 *---------------------*/
```

```sql
SELECT PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', '8/30/2018 2:23:38 pm') AS datetime

/*---------------------*
 | datetime            |
 +---------------------+
 | 2018-08-30 14:23:38 |
 *---------------------*/
```

The following example parses a `STRING` literal
containing a date in a natural language format as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%A, %B %e, %Y','Wednesday, December 19, 2018')
  AS datetime;

/*---------------------*
 | datetime            |
 +---------------------+
 | 2018-12-19 00:00:00 |
 *---------------------*/
```

[datetime-format]: #format_datetime

[datetime-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

## Time functions

ZetaSQL supports the following time functions.

### `CURRENT_TIME`

```sql
CURRENT_TIME([time_zone])
```

```sql
CURRENT_TIME
```

**Description**

Returns the current time as a `TIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `time_zone` parameter.
See [Time zone definitions][time-link-to-timezone-definitions] for information
on how to specify a time zone.

The current time is recorded at the start of the query
statement which contains this function, not when this specific function is
evaluated.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT CURRENT_TIME() as now;

/*----------------------------*
 | now                        |
 +----------------------------+
 | 15:31:38.776361            |
 *----------------------------*/
```

When a column named `current_time` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][time-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_time` column.

```sql
WITH t AS (SELECT 'column value' AS `current_time`)
SELECT current_time() as now, t.current_time FROM t;

/*-----------------+--------------*
 | now             | current_time |
 +-----------------+--------------+
 | 15:31:38.776361 | column value |
 *-----------------+--------------*/
```

[time-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[time-link-to-timezone-definitions]: #timezone_definitions

### `EXTRACT`

```sql
EXTRACT(part FROM time_expression)
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `time_expression`.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`

**Example**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM TIME "15:30:00") as hour;

/*------------------*
 | hour             |
 +------------------+
 | 15               |
 *------------------*/
```

### `FORMAT_TIME`

```sql
FORMAT_TIME(format_string, time_object)
```

**Description**
Formats a `TIME` object according to the specified `format_string`. See
[Supported Format Elements For TIME][time-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIME("%R", TIME "15:30:00") as formatted_time;

/*----------------*
 | formatted_time |
 +----------------+
 | 15:30          |
 *----------------*/
```

[time-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `PARSE_TIME`

```sql
PARSE_TIME(format_string, time_string)
```

**Description**

Converts a [string representation of time][time-format] to a
`TIME` object.

`format_string` contains the [format elements][time-format-elements]
that define how `time_string` is formatted. Each element in
`time_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `time_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIME("%I:%M:%S", "07:30:00")

-- This produces an error because the seconds element is in different locations.
SELECT PARSE_TIME("%S:%I:%M", "07:30:00")

-- This produces an error because one of the seconds elements is missing.
SELECT PARSE_TIME("%I:%M", "07:30:00")

-- This works because %T can find all matching elements in time_string.
SELECT PARSE_TIME("%T", "07:30:00")
```

When using `PARSE_TIME`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from
  `00:00:00.0`. For instance, if `seconds` is unspecified then it
  defaults to `00`, and so on.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the `TIME` string. In
  addition, leading and trailing white spaces in the `TIME` string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information, the last one generally overrides any earlier ones.
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT PARSE_TIME("%H", "15") as parsed_time;

/*-------------*
 | parsed_time |
 +-------------+
 | 15:00:00    |
 *-------------*/
```

```sql
SELECT PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') AS parsed_time

/*-------------*
 | parsed_time |
 +-------------+
 | 14:23:38    |
 *-------------*/
```

[time-format]: #format_time

[time-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `TIME`

```sql
1. TIME(hour, minute, second)
2. TIME(timestamp, [time_zone])
3. TIME(datetime)
```

**Description**

1. Constructs a `TIME` object using `INT64`
   values representing the hour, minute, and second.
2. Constructs a `TIME` object using a `TIMESTAMP` object. It supports an
   optional
   parameter to [specify a time zone][time-link-to-timezone-definitions]. If no
   time zone is specified, the default time zone, which is implementation defined, is
   used.
3. Constructs a `TIME` object using a
   `DATETIME` object.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME(15, 30, 00) as time_hms,
  TIME(TIMESTAMP "2008-12-25 15:30:00+08", "America/Los_Angeles") as time_tstz;

/*----------+-----------*
 | time_hms | time_tstz |
 +----------+-----------+
 | 15:30:00 | 23:30:00  |
 *----------+-----------*/
```

```sql
SELECT TIME(DATETIME "2008-12-25 15:30:00.000000") AS time_dt;

/*----------*
 | time_dt  |
 +----------+
 | 15:30:00 |
 *----------*/
```

[time-link-to-timezone-definitions]: #timezone_definitions

### `TIME_ADD`

```sql
TIME_ADD(time_expression, INTERVAL int64_expression part)
```

**Description**

Adds `int64_expression` units of `part` to the `TIME` object.

`TIME_ADD` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you add an hour to `23:30:00`, the returned
value is `00:30:00`.

**Return Data Types**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original_time,
  TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE) as later;

/*-----------------------------+------------------------*
 | original_time               | later                  |
 +-----------------------------+------------------------+
 | 15:30:00                    | 15:40:00               |
 *-----------------------------+------------------------*/
```

### `TIME_DIFF`

```sql
TIME_DIFF(time_expression_a, time_expression_b, part)
```

**Description**

Returns the whole number of specified `part` intervals between two
`TIME` objects (`time_expression_a` - `time_expression_b`). If the first
`TIME` is earlier than the second one, the output is negative. Throws an error
if the computation overflows the result type, such as if the difference in
nanoseconds
between the two `TIME` objects would overflow an
`INT64` value.

`TIME_DIFF` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIME "15:30:00" as first_time,
  TIME "14:35:00" as second_time,
  TIME_DIFF(TIME "15:30:00", TIME "14:35:00", MINUTE) as difference;

/*----------------------------+------------------------+------------------------*
 | first_time                 | second_time            | difference             |
 +----------------------------+------------------------+------------------------+
 | 15:30:00                   | 14:35:00               | 55                     |
 *----------------------------+------------------------+------------------------*/
```

### `TIME_SUB`

```sql
TIME_SUB(time_expression, INTERVAL int64_expression part)
```

**Description**

Subtracts `int64_expression` units of `part` from the `TIME` object.

`TIME_SUB` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you subtract an hour from `00:30:00`, the
returned value is `23:30:00`.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original_date,
  TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE) as earlier;

/*-----------------------------+------------------------*
 | original_date               | earlier                |
 +-----------------------------+------------------------+
 | 15:30:00                    | 15:20:00               |
 *-----------------------------+------------------------*/
```

### `TIME_TRUNC`

```sql
TIME_TRUNC(time_expression, time_part)
```

**Description**

Truncates a `TIME` value to the granularity of `time_part`. The `TIME` value
is always rounded to the beginning of `time_part`, which can be one of the
following:

+ `NANOSECOND`: If used, nothing is truncated from the value.
+ `MICROSECOND`: The nearest lessor or equal microsecond.
+ `MILLISECOND`: The nearest lessor or equal millisecond.
+ `SECOND`: The nearest lessor or equal second.
+ `MINUTE`: The nearest lessor or equal minute.
+ `HOUR`: The nearest lessor or equal hour.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original,
  TIME_TRUNC(TIME "15:30:00", HOUR) as truncated;

/*----------------------------+------------------------*
 | original                   | truncated              |
 +----------------------------+------------------------+
 | 15:30:00                   | 15:00:00               |
 *----------------------------+------------------------*/
```

[time-to-string]: #cast

## Timestamp functions

ZetaSQL supports the following timestamp functions.

IMPORTANT: Before working with these functions, you need to understand
the difference between the formats in which timestamps are stored and displayed,
and how time zones are used for the conversion between these formats.
To learn more, see
[How time zones work with timestamp functions][timestamp-link-to-timezone-definitions].

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined [`DATE` range][data-types-link-to-date_type]
and [`TIMESTAMP` range][data-types-link-to-timestamp_type].

### `CURRENT_TIMESTAMP`

```sql
CURRENT_TIMESTAMP()
```

```sql
CURRENT_TIMESTAMP
```

**Description**

Returns the current date and time as a timestamp object. The timestamp is
continuous, non-ambiguous, has exactly 60 seconds per minute and does not repeat
values over the leap second. Parentheses are optional.

This function handles leap seconds by smearing them across a window of 20 hours
around the inserted leap second.

The current date and time is recorded at the start of the query
statement which contains this function, not when this specific function is
evaluated.

**Supported Input Types**

Not applicable

**Result Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT CURRENT_TIMESTAMP() AS now;

/*---------------------------------------------*
 | now                                         |
 +---------------------------------------------+
 | 2020-06-02 17:00:53.110 America/Los_Angeles |
 *---------------------------------------------*/
```

When a column named `current_timestamp` is present, the column name and the
function call without parentheses are ambiguous. To ensure the function call,
add parentheses; to ensure the column name, qualify it with its
[range variable][timestamp-functions-link-to-range-variables]. For example, the
following query selects the function in the `now` column and the table
column in the `current_timestamp` column.

```sql
WITH t AS (SELECT 'column value' AS `current_timestamp`)
SELECT current_timestamp() AS now, t.current_timestamp FROM t;

/*---------------------------------------------+-------------------*
 | now                                         | current_timestamp |
 +---------------------------------------------+-------------------+
 | 2020-06-02 17:00:53.110 America/Los_Angeles | column value      |
 *---------------------------------------------+-------------------*/
```

[timestamp-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

### `EXTRACT`

```sql
EXTRACT(part FROM timestamp_expression [AT TIME ZONE time_zone])
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `timestamp_expression`. This function supports an optional
`time_zone` parameter. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day of
   of the week.
+ `DAY`
+ `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53].  Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of `timestamp_expression` in the
  range [0, 53]. Weeks begin on `WEEKDAY`. `datetime`s prior to the first
  `WEEKDAY` of the year are in week 0. Valid values for `WEEKDAY` are `SUNDAY`,
  `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `datetime_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
  week-numbering year, which is the Gregorian calendar year containing the
  Thursday of the week to which `date_expression` belongs.
+ `DATE`
+ <code>DATETIME</code>
+ <code>TIME</code>

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`, except in the following cases:

+ If `part` is `DATE`, the function returns a `DATE` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
time part.

```sql
WITH Input AS (SELECT TIMESTAMP("2008-12-25 05:30:00+00") AS timestamp_value)
SELECT
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "UTC") AS the_day_utc,
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "America/Los_Angeles") AS the_day_california
FROM Input

/*-------------+--------------------*
 | the_day_utc | the_day_california |
 +-------------+--------------------+
 | 25          | 24                 |
 *-------------+--------------------*/
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of type `TIMESTAMP`.

```sql
WITH Timestamps AS (
  SELECT TIMESTAMP("2005-01-03 12:34:56+00") AS timestamp_value UNION ALL
  SELECT TIMESTAMP("2007-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-01-01 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-01-02 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-05-26 12:00:00+00")
)
SELECT
  timestamp_value,
  EXTRACT(ISOYEAR FROM timestamp_value) AS isoyear,
  EXTRACT(ISOWEEK FROM timestamp_value) AS isoweek,
  EXTRACT(YEAR FROM timestamp_value) AS year,
  EXTRACT(WEEK FROM timestamp_value) AS week
FROM Timestamps
ORDER BY timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------+---------+------+------*
 | timestamp_value                             | isoyear | isoweek | year | week |
 +---------------------------------------------+---------+---------+------+------+
 | 2005-01-03 04:34:56.000 America/Los_Angeles | 2005    | 1       | 2005 | 1    |
 | 2007-12-31 04:00:00.000 America/Los_Angeles | 2008    | 1       | 2007 | 52   |
 | 2009-01-01 04:00:00.000 America/Los_Angeles | 2009    | 1       | 2009 | 0    |
 | 2009-12-31 04:00:00.000 America/Los_Angeles | 2009    | 53      | 2009 | 52   |
 | 2017-01-02 04:00:00.000 America/Los_Angeles | 2017    | 1       | 2017 | 1    |
 | 2017-05-26 05:00:00.000 America/Los_Angeles | 2017    | 21      | 2017 | 21   |
 *---------------------------------------------+---------+---------+------+------*/
```

In the following example, `timestamp_expression` falls on a Monday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT TIMESTAMP("2017-11-06 00:00:00+00") AS timestamp_value)
SELECT
  timestamp_value,
  EXTRACT(WEEK(SUNDAY) FROM timestamp_value) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM timestamp_value) AS week_monday
FROM table;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+-------------+---------------*
 | timestamp_value                             | week_sunday | week_monday   |
 +---------------------------------------------+-------------+---------------+
 | 2017-11-05 16:00:00.000 America/Los_Angeles | 45          | 44            |
 *---------------------------------------------+-------------+---------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `FORMAT_TIMESTAMP`

```sql
FORMAT_TIMESTAMP(format_string, timestamp[, time_zone])
```

**Description**

Formats a timestamp according to the specified `format_string`.

See [Format elements for date and time parts][timestamp-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS formatted;

/*--------------------------*
 | formatted                |
 +--------------------------+
 | Thu Dec 25 15:30:00 2008 |
 *--------------------------*/
```

```sql
SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00+00") AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec-25-2008 |
 *-------------*/
```

```sql
SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2008-12-25 15:30:00+00")
  AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec 2008    |
 *-------------*/
```

[timestamp-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `PARSE_TIMESTAMP`

```sql
PARSE_TIMESTAMP(format_string, timestamp_string[, time_zone])
```

**Description**

Converts a [string representation of a timestamp][timestamp-format] to a
`TIMESTAMP` object.

`format_string` contains the [format elements][timestamp-format-elements]
that define how `timestamp_string` is formatted. Each element in
`timestamp_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `timestamp_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")

-- This produces an error because the year element is in different locations.
SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This produces an error because one of the year elements is missing.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This works because %c can find all matching elements in timestamp_string.
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008")
```

When using `PARSE_TIMESTAMP`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01
  00:00:00.0`. This initialization value uses the time zone specified by the
  function's time zone argument, if present. If not, the initialization value
  uses the default time zone, which is implementation defined.  For instance, if the year
  is unspecified then it defaults to `1970`, and so on.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the timestamp string. In
  addition, leading and trailing white spaces in the timestamp string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones, with some exceptions (see the
  descriptions of `%s`, `%C`, and `%y`).
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008") AS parsed;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | parsed                                      |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

[timestamp-format]: #format_timestamp

[timestamp-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `STRING`

```sql
STRING(timestamp_expression[, time_zone])
```

**Description**

Converts a timestamp to a string. Supports an optional
parameter to specify a time zone. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS string;

/*-------------------------------*
 | string                        |
 +-------------------------------+
 | 2008-12-25 15:30:00+00        |
 *-------------------------------*/
```

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `TIMESTAMP`

```sql
TIMESTAMP(string_expression[, time_zone])
TIMESTAMP(date_expression[, time_zone])
TIMESTAMP(datetime_expression[, time_zone])
```

**Description**

+  `string_expression[, time_zone]`: Converts a string to a
   timestamp. `string_expression` must include a
   timestamp literal.
   If `string_expression` includes a time zone in the timestamp literal, do
   not include an explicit `time_zone`
   argument.
+  `date_expression[, time_zone]`: Converts a date to a timestamp.
   The value returned is the earliest timestamp that falls within
   the given date.
+  `datetime_expression[, time_zone]`: Converts a
   datetime to a timestamp.

This function supports an optional
parameter to [specify a time zone][timestamp-link-to-timezone-definitions]. If
no time zone is specified, the default time zone, which is implementation defined,
is used.

**Return Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00+00") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 15:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00 UTC") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP(DATETIME "2008-12-25 15:30:00") AS timestamp_datetime;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_datetime                          |
 +---------------------------------------------+
 | 2008-12-25 15:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP(DATE "2008-12-25") AS timestamp_date;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_date                              |
 +---------------------------------------------+
 | 2008-12-25 00:00:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

[timestamp-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#timestamp_literals

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `TIMESTAMP_ADD`

```sql
TIMESTAMP_ADD(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds `int64_expression` units of `date_part` to the timestamp, independent of
any time zone.

`TIMESTAMP_ADD` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE` parts.
+ `DAY`. Equivalent to 24 `HOUR` parts.

**Return Data Types**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS later;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | original                                    | later                                       |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:40:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

### `TIMESTAMP_DIFF`

```sql
TIMESTAMP_DIFF(timestamp_expression_a, timestamp_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
timestamps (`timestamp_expression_a` - `timestamp_expression_b`).
If the first timestamp is earlier than the second one,
the output is negative. Produces an error if the computation overflows the
result type, such as if the difference in
nanoseconds
between the two timestamps would overflow an
`INT64` value.

`TIMESTAMP_DIFF` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIMESTAMP("2010-07-07 10:20:00+00") AS later_timestamp,
  TIMESTAMP("2008-12-25 15:30:00+00") AS earlier_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00+00", TIMESTAMP "2008-12-25 15:30:00+00", HOUR) AS hours;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------+-------*
 | later_timestamp                             | earlier_timestamp                           | hours |
 +---------------------------------------------+---------------------------------------------+-------+
 | 2010-07-07 03:20:00.000 America/Los_Angeles | 2008-12-25 07:30:00.000 America/Los_Angeles | 13410 |
 *---------------------------------------------+---------------------------------------------+-------*/
```

In the following example, the first timestamp occurs before the
second timestamp, resulting in a negative output.

```sql
SELECT TIMESTAMP_DIFF(TIMESTAMP "2018-08-14", TIMESTAMP "2018-10-14", DAY) AS negative_diff;

/*---------------*
 | negative_diff |
 +---------------+
 | -61           |
 *---------------+
```

In this example, the result is 0 because only the number of whole specified
`HOUR` intervals are included.

```sql
SELECT TIMESTAMP_DIFF("2001-02-01 01:00:00", "2001-02-01 00:00:01", HOUR) AS diff;

/*---------------*
 | diff          |
 +---------------+
 | 0             |
 *---------------+
```

### `TIMESTAMP_FROM_UNIX_MICROS`

```sql
TIMESTAMP_FROM_UNIX_MICROS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MICROS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_FROM_UNIX_MILLIS`

```sql
TIMESTAMP_FROM_UNIX_MILLIS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MILLIS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_FROM_UNIX_SECONDS`

```sql
TIMESTAMP_FROM_UNIX_SECONDS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_SECONDS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_MICROS`

```sql
TIMESTAMP_MICROS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since 1970-01-01
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_MILLIS`

```sql
TIMESTAMP_MILLIS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since 1970-01-01
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_SECONDS`

```sql
TIMESTAMP_SECONDS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since 1970-01-01 00:00:00
UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_SUB`

```sql
TIMESTAMP_SUB(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts `int64_expression` units of `date_part` from the timestamp,
independent of any time zone.

`TIMESTAMP_SUB` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE` parts.
+ `DAY`. Equivalent to 24 `HOUR` parts.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS earlier;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | original                                    | earlier                                     |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:20:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

### `TIMESTAMP_TRUNC`

```sql
TIMESTAMP_TRUNC(timestamp_expression, date_time_part[, time_zone])
```

**Description**

Truncates a timestamp to the granularity of `date_time_part`.
The timestamp is always rounded to the beginning of `date_time_part`,
which can be one of the following:

+ `NANOSECOND`: If used, nothing is truncated from the value.
+ `MICROSECOND`: The nearest lessor or equal microsecond.
+ `MILLISECOND`: The nearest lessor or equal millisecond.
+ `SECOND`: The nearest lessor or equal second.
+ `MINUTE`: The nearest lessor or equal minute.
+ `HOUR`: The nearest lessor or equal hour.
+ `DAY`: The day in the Gregorian calendar year that contains the
  `TIMESTAMP` value.
+ `WEEK`: The first day of the week in the week that contains the
  `TIMESTAMP` value. Weeks begin on Sundays. `WEEK` is equivalent to
  `WEEK(SUNDAY)`.
+ `WEEK(WEEKDAY)`: The first day of the week in the week that contains the
  `TIMESTAMP` value. Weeks begin on `WEEKDAY`. `WEEKDAY` must be one of the
   following: `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`,
   or `SATURDAY`.
+ `ISOWEEK`: The first day of the [ISO 8601 week][ISO-8601-week] in the
  ISO week that contains the `TIMESTAMP` value. The ISO week begins on
  Monday. The first ISO week of each ISO year contains the first Thursday of the
  corresponding Gregorian calendar year.
+ `MONTH`: The first day of the month in the month that contains the
  `TIMESTAMP` value.
+ `QUARTER`: The first day of the quarter in the quarter that contains the
  `TIMESTAMP` value.
+ `YEAR`: The first day of the year in the year that contains the
  `TIMESTAMP` value.
+ `ISOYEAR`: The first day of the [ISO 8601][ISO-8601] week-numbering year
  in the ISO year that contains the `TIMESTAMP` value. The ISO year is the
  Monday of the first week whose Thursday belongs to the corresponding
  Gregorian calendar year.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

<!-- mdlint on -->

`TIMESTAMP_TRUNC` function supports an optional `time_zone` parameter. This
parameter applies to the following `date_time_part`:

+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`
+ `ISOWEEK`
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`

Use this parameter if you want to use a time zone other than the
default time zone, which is implementation defined, as part of the
truncate operation.

When truncating a timestamp to `MINUTE`
or`HOUR` parts, `TIMESTAMP_TRUNC` determines the civil time of the
timestamp in the specified (or default) time zone
and subtracts the minutes and seconds (when truncating to `HOUR`) or the seconds
(when truncating to `MINUTE`) from that timestamp.
While this provides intuitive results in most cases, the result is
non-intuitive near daylight savings transitions that are not hour-aligned.

**Return Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC") AS utc,
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "America/Los_Angeles") AS la;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | utc                                         | la                                          |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-24 16:00:00.000 America/Los_Angeles | 2008-12-25 00:00:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

In the following example, `timestamp_expression` has a time zone offset of +12.
The first column shows the `timestamp_expression` in UTC time. The second
column shows the output of `TIMESTAMP_TRUNC` using weeks that start on Monday.
Because the `timestamp_expression` falls on a Sunday in UTC, `TIMESTAMP_TRUNC`
truncates it to the preceding Monday. The third column shows the same function
with the optional [Time zone definition][timestamp-link-to-timezone-definitions]
argument 'Pacific/Auckland'. Here, the function truncates the
`timestamp_expression` using New Zealand Daylight Time, where it falls on a
Monday.

```sql
SELECT
  timestamp_value AS timestamp_value,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "UTC") AS utc_truncated,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "Pacific/Auckland") AS nzdt_truncated
FROM (SELECT TIMESTAMP("2017-11-06 00:00:00+12") AS timestamp_value);

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------+---------------------------------------------*
 | timestamp_value                             | utc_truncated                               | nzdt_truncated                              |
 +---------------------------------------------+---------------------------------------------+---------------------------------------------+
 | 2017-11-05 04:00:00.000 America/Los_Angeles | 2017-10-29 17:00:00.000 America/Los_Angeles | 2017-11-05 03:00:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------+---------------------------------------------*/
```

In the following example, the original `timestamp_expression` is in the
Gregorian calendar year 2015. However, `TIMESTAMP_TRUNC` with the `ISOYEAR` date
part truncates the `timestamp_expression` to the beginning of the ISO year, not
the Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `timestamp_expression`
2015-06-15 00:00:00+00 is 2014-12-29.

```sql
SELECT
  TIMESTAMP_TRUNC("2015-06-15 00:00:00+00", ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM TIMESTAMP "2015-06-15 00:00:00+00") AS isoyear_number;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+----------------*
 | isoyear_boundary                            | isoyear_number |
 +---------------------------------------------+----------------+
 | 2014-12-29 00:00:00.000 America/Los_Angeles | 2015           |
 *---------------------------------------------+----------------*/
```

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `UNIX_MICROS`

```sql
UNIX_MICROS(timestamp_expression)
```

**Description**

Returns the number of microseconds since `1970-01-01 00:00:00 UTC`.
Truncates higher levels of precision by
rounding down to the beginning of the microsecond.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00") AS micros;

/*------------------*
 | micros           |
 +------------------+
 | 1230219000000000 |
 *------------------*/
```

```sql
SELECT UNIX_MICROS(TIMESTAMP "1970-01-01 00:00:00.0000018+00") AS micros;

/*------------------*
 | micros           |
 +------------------+
 | 1                |
 *------------------*/
```

### `UNIX_MILLIS`

```sql
UNIX_MILLIS(timestamp_expression)
```

**Description**

Returns the number of milliseconds since `1970-01-01 00:00:00 UTC`. Truncates
higher levels of precision by rounding down to the beginning of the millisecond.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00") AS millis;

/*---------------*
 | millis        |
 +---------------+
 | 1230219000000 |
 *---------------*/
```

```sql
SELECT UNIX_MILLIS(TIMESTAMP "1970-01-01 00:00:00.0018+00") AS millis;

/*---------------*
 | millis        |
 +---------------+
 | 1             |
 *---------------*/
```

### `UNIX_SECONDS`

```sql
UNIX_SECONDS(timestamp_expression)
```

**Description**

Returns the number of seconds since `1970-01-01 00:00:00 UTC`. Truncates higher
levels of precision by rounding down to the beginning of the second.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00") AS seconds;

/*------------*
 | seconds    |
 +------------+
 | 1230219000 |
 *------------*/
```

```sql
SELECT UNIX_SECONDS(TIMESTAMP "1970-01-01 00:00:01.8+00") AS seconds;

/*------------*
 | seconds    |
 +------------+
 | 1          |
 *------------*/
```

### How time zones work with timestamp functions 
<a id="timezone_definitions"></a>

A timestamp represents an absolute point in time, independent of any time
zone. However, when a timestamp value is displayed, it is usually converted to
a human-readable format consisting of a civil date and time
(YYYY-MM-DD HH:MM:SS)
and a time zone. This is not the internal representation of the
`TIMESTAMP`; it is only a human-understandable way to describe the point in time
that the timestamp represents.

Some timestamp functions have a time zone argument. A time zone is needed to
convert between civil time (YYYY-MM-DD HH:MM:SS) and the absolute time
represented by a timestamp.
A function like `PARSE_TIMESTAMP` takes an input string that represents a
civil time and returns a timestamp that represents an absolute time. A
time zone is needed for this conversion. A function like `EXTRACT` takes an
input timestamp (absolute time) and converts it to civil time in order to
extract a part of that civil time. This conversion requires a time zone.
If no time zone is specified, the default time zone, which is implementation defined,
is used.

Certain date and timestamp functions allow you to override the default time zone
and specify a different one. You can specify a time zone by either supplying
the time zone name (for example, `America/Los_Angeles`)
or time zone offset from UTC (for example, -08).

To learn more about how time zones work with the `TIMESTAMP` type, see
[Time zones][data-types-timezones].

[timezone-by-name]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[data-types-timezones]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time_zones

[timestamp-link-to-timezone-definitions]: #timezone_definitions

[data-types-link-to-date_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#date_type

[data-types-link-to-timestamp_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#timestamp_type

## Interval functions

ZetaSQL supports the following interval functions.

### `EXTRACT`

```sql
EXTRACT(part FROM interval_expression)
```

**Description**

Returns the value corresponding to the specified date part. The `part` must be
one of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND` or
`MICROSECOND`.

**Return Data Type**

`INTERVAL`

**Examples**

In the following example, different parts of two intervals are extracted.

```sql
SELECT
  EXTRACT(YEAR FROM i) AS year,
  EXTRACT(MONTH FROM i) AS month,
  EXTRACT(DAY FROM i) AS day,
  EXTRACT(HOUR FROM i) AS hour,
  EXTRACT(MINUTE FROM i) AS minute,
  EXTRACT(SECOND FROM i) AS second,
  EXTRACT(MILLISECOND FROM i) AS milli,
  EXTRACT(MICROSECOND FROM i) AS micro
FROM
  UNNEST([INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND,
          INTERVAL '0-13 370 48:61:61' YEAR TO SECOND]) AS i

/*------+-------+-----+------+--------+--------+-------+--------*
 | year | month | day | hour | minute | second | milli | micro  |
 +------+-------+-----+------+--------+--------+-------+--------+
 | 1    | 2     | 3   | 4    | 5      | 6      | 789   | 789999 |
 | 1    | 1     | 370 | 49   | 2      | 1      | 0     | 0      |
 *------+-------+-----+------+--------+--------+-------+--------*/
```

When a negative sign precedes the time part in an interval, the negative sign
distributes over the hours, minutes, and seconds. For example:

```sql
SELECT
  EXTRACT(HOUR FROM i) AS hour,
  EXTRACT(MINUTE FROM i) AS minute
FROM
  UNNEST([INTERVAL '10 -12:30' DAY TO MINUTE]) AS i

/*------+--------*
 | hour | minute |
 +------+--------+
 | -12  | -30    |
 *------+--------*/
```

When a negative sign precedes the year and month part in an interval, the
negative sign distributes over the years and months. For example:

```sql
SELECT
  EXTRACT(YEAR FROM i) AS year,
  EXTRACT(MONTH FROM i) AS month
FROM
  UNNEST([INTERVAL '-22-6 10 -12:30' YEAR TO MINUTE]) AS i

/*------+--------*
 | year | month  |
 +------+--------+
 | -22  | -6     |
 *------+--------*/
```

### `JUSTIFY_DAYS`

```sql
JUSTIFY_DAYS(interval_expression)
```

**Description**

Normalizes the day part of the interval to the range from -29 to 29 by
incrementing/decrementing the month or year part of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  JUSTIFY_DAYS(INTERVAL 29 DAY) AS i1,
  JUSTIFY_DAYS(INTERVAL -30 DAY) AS i2,
  JUSTIFY_DAYS(INTERVAL 31 DAY) AS i3,
  JUSTIFY_DAYS(INTERVAL -65 DAY) AS i4,
  JUSTIFY_DAYS(INTERVAL 370 DAY) AS i5

/*--------------+--------------+-------------+---------------+--------------*
 | i1           | i2           | i3          | i4            | i5           |
 +--------------+--------------+-------------+---------------+--------------+
 | 0-0 29 0:0:0 | -0-1 0 0:0:0 | 0-1 1 0:0:0 | -0-2 -5 0:0:0 | 1-0 10 0:0:0 |
 *--------------+--------------+-------------+---------------+--------------*/
```

### `JUSTIFY_HOURS`

```sql
JUSTIFY_HOURS(interval_expression)
```

**Description**

Normalizes the time part of the interval to the range from -23:59:59.999999 to
23:59:59.999999 by incrementing/decrementing the day part of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  JUSTIFY_HOURS(INTERVAL 23 HOUR) AS i1,
  JUSTIFY_HOURS(INTERVAL -24 HOUR) AS i2,
  JUSTIFY_HOURS(INTERVAL 47 HOUR) AS i3,
  JUSTIFY_HOURS(INTERVAL -12345 MINUTE) AS i4

/*--------------+--------------+--------------+-----------------*
 | i1           | i2           | i3           | i4              |
 +--------------+--------------+--------------+-----------------+
 | 0-0 0 23:0:0 | 0-0 -1 0:0:0 | 0-0 1 23:0:0 | 0-0 -8 -13:45:0 |
 *--------------+--------------+--------------+-----------------*/
```

### `JUSTIFY_INTERVAL`

```sql
JUSTIFY_INTERVAL(interval_expression)
```

**Description**

Normalizes the days and time parts of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT JUSTIFY_INTERVAL(INTERVAL '29 49:00:00' DAY TO SECOND) AS i

/*-------------*
 | i           |
 +-------------+
 | 0-1 1 1:0:0 |
 *-------------*/
```

### `MAKE_INTERVAL`

```sql
MAKE_INTERVAL([year][, month][, day][, hour][, minute][, second])
```

**Description**

Constructs an [`INTERVAL`][interval-type] object using `INT64` values
representing the year, month, day, hour, minute, and second. All arguments are
optional, `0` by default, and can be [named arguments][named-arguments].

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  MAKE_INTERVAL(1, 6, 15) AS i1,
  MAKE_INTERVAL(hour => 10, second => 20) AS i2,
  MAKE_INTERVAL(1, minute => 5, day => 2) AS i3

/*--------------+---------------+-------------*
 | i1           | i2            | i3          |
 +--------------+---------------+-------------+
 | 1-6 15 0:0:0 | 0-0 0 10:0:20 | 1-0 2 0:5:0 |
 *--------------+---------------+-------------*/
```

[interval-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#interval_type

[named-arguments]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#named_arguments

## Geography functions

ZetaSQL supports geography functions.
Geography functions operate on or generate ZetaSQL
`GEOGRAPHY` values. The signature of most geography
functions starts with `ST_`. ZetaSQL supports the following functions
that can be used to analyze geographical data, determine spatial relationships
between geographical features, and construct or manipulate
`GEOGRAPHY`s.

All ZetaSQL geography functions return `NULL` if any input argument
is `NULL`.

### Categories

The geography functions are grouped into the following categories based on their
behavior:

<table>
  <thead>
    <tr>
      <td>Category</td>
      <td width='300px'>Functions</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Constructors</td>
      <td>
        <a href="#st_geogpoint"><code>ST_GEOGPOINT</code></a><br>
        <a href="#st_makeline"><code>ST_MAKELINE</code></a><br>
        <a href="#st_makepolygon"><code>ST_MAKEPOLYGON</code></a><br>
        <a href="#st_makepolygonoriented"><code>ST_MAKEPOLYGONORIENTED</code></a>
      </td>
      <td>
        Functions that build new
        geography values from coordinates
        or existing geographies.
      </td>
    </tr>
    <tr>
      <td>Parsers</td>
      <td>
        <a href="#st_geogfrom"><code>ST_GEOGFROM</code></a><br>
        <a href="#st_geogfromgeojson"><code>ST_GEOGFROMGEOJSON</code></a><br>
        <a href="#st_geogfromkml"><code>ST_GEOGFROMKML</code></a><br>
        <a href="#st_geogfromtext"><code>ST_GEOGFROMTEXT</code></a><br>
        <a href="#st_geogfromwkb"><code>ST_GEOGFROMWKB</code></a><br>
        <a href="#st_geogpointfromgeohash"><code>ST_GEOGPOINTFROMGEOHASH</code></a><br>
      </td>
      <td>
        Functions that create geographies
        from an external format such as
        <a href="https://en.wikipedia.org/wiki/Well-known_text">WKT</a> and
        <a href="https://en.wikipedia.org/wiki/GeoJSON">GeoJSON</a>.
      </td>
    </tr>
    <tr>
      <td>Formatters</td>
      <td>
        <a href="#st_asbinary"><code>ST_ASBINARY</code></a><br>
        <a href="#st_asgeojson"><code>ST_ASGEOJSON</code></a><br>
        <a href="#st_askml"><code>ST_ASKML</code></a><br>
        <a href="#st_astext"><code>ST_ASTEXT</code></a><br>
        <a href="#st_geohash"><code>ST_GEOHASH</code></a>
      </td>
      <td>
        Functions that export geographies
        to an external format such as WKT.
      </td>
    </tr>
    <tr>
      <td>Transformations</td>
      <td>
        <a href="#st_accum"><code>ST_ACCUM</code></a> (Aggregate)<br>
        <a href="#st_boundary"><code>ST_BOUNDARY</code></a><br>
        <a href="#st_buffer"><code>ST_BUFFER</code></a><br>
        <a href="#st_bufferwithtolerance"><code>ST_BUFFERWITHTOLERANCE</code></a><br>
        <a href="#st_centroid"><code>ST_CENTROID</code></a><br>
        <a href="#st_centroid_agg"><code>ST_CENTROID_AGG</code></a> (Aggregate)<br>
        <a href="#st_closestpoint"><code>ST_CLOSESTPOINT</code></a><br>
        <a href="#st_convexhull"><code>ST_CONVEXHULL</code></a><br>
        <a href="#st_difference"><code>ST_DIFFERENCE</code></a><br>
        <a href="#st_exteriorring"><code>ST_EXTERIORRING</code></a><br>
        <a href="#st_interiorrings"><code>ST_INTERIORRINGS</code></a><br>
        <a href="#st_intersection"><code>ST_INTERSECTION</code></a><br>
        <a href="#st_simplify"><code>ST_SIMPLIFY</code></a><br>
        <a href="#st_snaptogrid"><code>ST_SNAPTOGRID</code></a><br>
        <a href="#st_union"><code>ST_UNION</code></a><br>
        <a href="#st_union_agg"><code>ST_UNION_AGG</code></a> (Aggregate)<br>
      </td>
      <td>
        Functions that generate a new
        geography based on input.
      </td>
    </tr>
    <tr>
      <td>Accessors</td>
      <td>
        <a href="#st_dimension"><code>ST_DIMENSION</code></a><br>
        <a href="#st_dump"><code>ST_DUMP</code></a><br>
        <a href="#st_dumppoints"><code>ST_DUMPPOINTS</code></a><br>
        <a href="#st_endpoint"><code>ST_ENDPOINT</code></a><br>
        <a href="#st_geometrytype"><code>ST_GEOMETRYTYPE</code></a><br>
        <a href="#st_isclosed"><code>ST_ISCLOSED</code></a><br>
        <a href="#st_iscollection"><code>ST_ISCOLLECTION</code></a><br>
        <a href="#st_isempty"><code>ST_ISEMPTY</code></a><br>
        <a href="#st_isring"><code>ST_ISRING</code></a><br>
        <a href="#st_npoints"><code>ST_NPOINTS</code></a><br>
        <a href="#st_numgeometries"><code>ST_NUMGEOMETRIES</code></a><br>
        <a href="#st_numpoints"><code>ST_NUMPOINTS</code></a><br>
        <a href="#st_pointn"><code>ST_POINTN</code></a><br>
        <a href="#st_startpoint"><code>ST_STARTPOINT</code></a><br>
        <a href="#st_x"><code>ST_X</code></a><br>
        <a href="#st_y"><code>ST_Y</code></a><br>
      </td>
      <td>
        Functions that provide access to
        properties of a geography without
        side-effects.
      </td>
    </tr>
    <tr>
      <td>Predicates</td>
      <td>
        <a href="#st_contains"><code>ST_CONTAINS</code></a><br>
        <a href="#st_coveredby"><code>ST_COVEREDBY</code></a><br>
        <a href="#st_covers"><code>ST_COVERS</code></a><br>
        <a href="#st_disjoint"><code>ST_DISJOINT</code></a><br>
        <a href="#st_dwithin"><code>ST_DWITHIN</code></a><br>
        <a href="#st_equals"><code>ST_EQUALS</code></a><br>
        <a href="#st_intersects"><code>ST_INTERSECTS</code></a><br>
        <a href="#st_intersectsbox"><code>ST_INTERSECTSBOX</code></a><br>
        <a href="#st_touches"><code>ST_TOUCHES</code></a><br>
        <a href="#st_within"><code>ST_WITHIN</code></a><br>
      </td>
      <td>
        Functions that return <code>TRUE</code> or
        <code>FALSE</code> for some spatial
        relationship between two
        geographies or some property of
        a geography. These functions
        are commonly used in filter
        clauses.
      </td>
    </tr>
    <tr>
      <td>Measures</td>
      <td>
        <a href="#st_angle"><code>ST_ANGLE</code></a><br>
        <a href="#st_area"><code>ST_AREA</code></a><br>
        <a href="#st_azimuth"><code>ST_AZIMUTH</code></a><br>
        <a href="#st_boundingbox"><code>ST_BOUNDINGBOX</code></a><br>
        <a href="#st_distance"><code>ST_DISTANCE</code></a><br>
        <a href="#st_extent"><code>ST_EXTENT</code></a> (Aggregate)<br>
        <a href="#st_linelocatepoint"><code>ST_LINELOCATEPOINT</code></a><br>
        <a href="#st_length"><code>ST_LENGTH</code></a><br>
        <a href="#st_maxdistance"><code>ST_MAXDISTANCE</code></a><br>
        <a href="#st_perimeter"><code>ST_PERIMETER</code></a><br>
      </td>
      <td>
        Functions that compute measurements
        of one or more geographies.
      </td>
    </tr>
    
    <tr>
      <td>Clustering</td>
      <td>
        <a href="#st_clusterdbscan"><code>ST_CLUSTERDBSCAN</code></a>
      </td>
      <td>
        Functions that perform clustering on geographies.
      </td>
    </tr>
    
    
  </tbody>
</table>

### `ST_ACCUM`

```sql
ST_ACCUM(geography)
```

**Description**

Takes a `GEOGRAPHY` and returns an array of
`GEOGRAPHY` elements.
This function is identical to [ARRAY_AGG][geography-link-array-agg],
but only applies to `GEOGRAPHY` objects.

**Return type**

`ARRAY<GEOGRAPHY>`

[geography-link-array-agg]: #array_agg

### `ST_ANGLE`

```sql
ST_ANGLE(point_geography_1, point_geography_2, point_geography_3)
```

**Description**

Takes three point `GEOGRAPHY` values, which represent two intersecting lines.
Returns the angle between these lines. Point 2 and point 1 represent the first
line and point 2 and point 3 represent the second line. The angle between
these lines is in radians, in the range `[0, 2pi)`. The angle is measured
clockwise from the first line to the second line.

`ST_ANGLE` has the following edge cases:

+ If points 2 and 3 are the same, returns `NULL`.
+ If points 2 and 1 are the same, returns `NULL`.
+ If points 2 and 3 are exactly antipodal, returns `NULL`.
+ If points 2 and 1 are exactly antipodal, returns `NULL`.
+ If any of the input geographies are not single points or are the empty
  geography, then throws an error.

**Return type**

`DOUBLE`

**Example**

```sql
WITH geos AS (
  SELECT 1 id, ST_GEOGPOINT(1, 0) geo1, ST_GEOGPOINT(0, 0) geo2, ST_GEOGPOINT(0, 1) geo3 UNION ALL
  SELECT 2 id, ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0), ST_GEOGPOINT(0, 1) UNION ALL
  SELECT 3 id, ST_GEOGPOINT(1, 0), ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0) UNION ALL
  SELECT 4 id, ST_GEOGPOINT(1, 0) geo1, ST_GEOGPOINT(0, 0) geo2, ST_GEOGPOINT(0, 0) geo3 UNION ALL
  SELECT 5 id, ST_GEOGPOINT(0, 0), ST_GEOGPOINT(-30, 0), ST_GEOGPOINT(150, 0) UNION ALL
  SELECT 6 id, ST_GEOGPOINT(0, 0), NULL, NULL UNION ALL
  SELECT 7 id, NULL, ST_GEOGPOINT(0, 0), NULL UNION ALL
  SELECT 8 id, NULL, NULL, ST_GEOGPOINT(0, 0))
SELECT ST_ANGLE(geo1,geo2,geo3) AS angle FROM geos ORDER BY id;

/*---------------------*
 | angle               |
 +---------------------+
 | 4.71238898038469    |
 | 0.78547432161873854 |
 | 0                   |
 | NULL                |
 | NULL                |
 | NULL                |
 | NULL                |
 | NULL                |
 *---------------------*/
```

### `ST_AREA`

```sql
ST_AREA(geography_expression[, use_spheroid])
```

**Description**

Returns the area in square meters covered by the polygons in the input
`GEOGRAPHY`.

If `geography_expression` is a point or a line, returns zero. If
`geography_expression` is a collection, returns the area of the polygons in the
collection; if the collection does not contain polygons, returns zero.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

`DOUBLE`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_ASBINARY`

```sql
ST_ASBINARY(geography_expression)
```

**Description**

Returns the [WKB][wkb-link] representation of an input
`GEOGRAPHY`.

See [`ST_GEOGFROMWKB`][st-geogfromwkb] to construct a
`GEOGRAPHY` from WKB.

**Return type**

`BYTES`

[wkb-link]: https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary

[st-geogfromwkb]: #st_geogfromwkb

### `ST_ASGEOJSON`

```sql
ST_ASGEOJSON(geography_expression)
```

**Description**

Returns the [RFC 7946][GeoJSON-spec-link] compliant [GeoJSON][geojson-link]
representation of the input `GEOGRAPHY`.

A ZetaSQL `GEOGRAPHY` has spherical
geodesic edges, whereas a GeoJSON `Geometry` object explicitly has planar edges.
To convert between these two types of edges, ZetaSQL adds additional
points to the line where necessary so that the resulting sequence of edges
remains within 10 meters of the original edge.

See [`ST_GEOGFROMGEOJSON`][st-geogfromgeojson] to construct a
`GEOGRAPHY` from GeoJSON.

**Return type**

`STRING`

[geojson-spec-link]: https://tools.ietf.org/html/rfc7946

[geojson-link]: https://en.wikipedia.org/wiki/GeoJSON

[st-geogfromgeojson]: #st_geogfromgeojson

### `ST_ASKML`

```sql
ST_ASKML(geography)
```

**Description**

Takes a `GEOGRAPHY` and returns a `STRING` [KML geometry][kml-geometry-link].
Coordinates are formatted with as few digits as possible without loss
of precision.

**Return type**

`STRING`

[kml-geometry-link]: https://developers.google.com/kml/documentation/kmlreference#geometry

### `ST_ASTEXT`

```sql
ST_ASTEXT(geography_expression)
```

**Description**

Returns the [WKT][wkt-link] representation of an input
`GEOGRAPHY`.

See [`ST_GEOGFROMTEXT`][st-geogfromtext] to construct a
`GEOGRAPHY` from WKT.

**Return type**

`STRING`

[wkt-link]: https://en.wikipedia.org/wiki/Well-known_text

[st-geogfromtext]: #st_geogfromtext

### `ST_AZIMUTH`

```sql
ST_AZIMUTH(point_geography_1, point_geography_2)
```

**Description**

Takes two point `GEOGRAPHY` values, and returns the azimuth of the line segment
formed by points 1 and 2. The azimuth is the angle in radians measured between
the line from point 1 facing true North to the line segment from point 1 to
point 2.

The positive angle is measured clockwise on the surface of a sphere. For
example, the azimuth for a line segment:

+   Pointing North is `0`
+   Pointing East is `PI/2`
+   Pointing South is `PI`
+   Pointing West is `3PI/2`

`ST_AZIMUTH` has the following edge cases:

+   If the two input points are the same, returns `NULL`.
+   If the two input points are exactly antipodal, returns `NULL`.
+   If either of the input geographies are not single points or are the empty
    geography, throws an error.

**Return type**

`DOUBLE`

**Example**

```sql
WITH geos AS (
  SELECT 1 id, ST_GEOGPOINT(1, 0) AS geo1, ST_GEOGPOINT(0, 0) AS geo2 UNION ALL
  SELECT 2, ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0) UNION ALL
  SELECT 3, ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1) UNION ALL
  -- identical
  SELECT 4, ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 0) UNION ALL
  -- antipode
  SELECT 5, ST_GEOGPOINT(-30, 0), ST_GEOGPOINT(150, 0) UNION ALL
  -- nulls
  SELECT 6, ST_GEOGPOINT(0, 0), NULL UNION ALL
  SELECT 7, NULL, ST_GEOGPOINT(0, 0))
SELECT ST_AZIMUTH(geo1, geo2) AS azimuth FROM geos ORDER BY id;

/*--------------------*
 | azimuth            |
 +--------------------+
 | 4.71238898038469   |
 | 1.5707963267948966 |
 | 0                  |
 | NULL               |
 | NULL               |
 | NULL               |
 | NULL               |
 *--------------------*/
```

### `ST_BOUNDARY`

```sql
ST_BOUNDARY(geography_expression)
```

**Description**

Returns a single `GEOGRAPHY` that contains the union
of the boundaries of each component in the given input
`GEOGRAPHY`.

The boundary of each component of a `GEOGRAPHY` is
defined as follows:

+   The boundary of a point is empty.
+   The boundary of a linestring consists of the endpoints of the linestring.
+   The boundary of a polygon consists of the linestrings that form the polygon
    shell and each of the polygon's holes.

**Return type**

`GEOGRAPHY`

### `ST_BOUNDINGBOX`

```sql
ST_BOUNDINGBOX(geography_expression)
```

**Description**

Returns a `STRUCT` that represents the bounding box for the specified geography.
The bounding box is the minimal rectangle that encloses the geography. The edges
of the rectangle follow constant lines of longitude and latitude.

Caveats:

+ Returns `NULL` if the input is `NULL` or an empty geography.
+ The bounding box might cross the antimeridian if this allows for a smaller
  rectangle. In this case, the bounding box has one of its longitudinal bounds
  outside of the [-180, 180] range, so that `xmin` is smaller than the eastmost
  value `xmax`.

**Return type**

`STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>`.

Bounding box parts:

+ `xmin`: The westmost constant longitude line that bounds the rectangle.
+ `xmax`: The eastmost constant longitude line that bounds the rectangle.
+ `ymin`: The minimum constant latitude line that bounds the rectangle.
+ `ymax`: The maximum constant latitude line that bounds the rectangle.

**Example**

```sql
WITH data AS (
  SELECT 1 id, ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))') g
  UNION ALL
  SELECT 2 id, ST_GEOGFROMTEXT('POLYGON((172 53, -130 55, -141 70, 172 53))') g
  UNION ALL
  SELECT 3 id, ST_GEOGFROMTEXT('POINT EMPTY') g
  UNION ALL
  SELECT 4 id, ST_GEOGFROMTEXT('POLYGON((172 53, -141 70, -130 55, 172 53))', oriented => TRUE)
)
SELECT id, ST_BOUNDINGBOX(g) AS box
FROM data

/*----+------------------------------------------*
 | id | box                                      |
 +----+------------------------------------------+
 | 1  | {xmin:-125, ymin:46, xmax:-117, ymax:49} |
 | 2  | {xmin:172, ymin:53, xmax:230, ymax:70}   |
 | 3  | NULL                                     |
 | 4  | {xmin:-180, ymin:-90, xmax:180, ymax:90} |
 *----+------------------------------------------*/
```

See [`ST_EXTENT`][st-extent] for the aggregate version of `ST_BOUNDINGBOX`.

[st-extent]: #st_extent

### `ST_BUFFER`

```sql
ST_BUFFER(
    geography,
    buffer_radius
    [, num_seg_quarter_circle => num_segments]
    [, use_spheroid => boolean_expression]
    [, endcap => endcap_style]
    [, side => line_side])
```

**Description**

Returns a `GEOGRAPHY` that represents the buffer around the input `GEOGRAPHY`.
This function is similar to [`ST_BUFFERWITHTOLERANCE`][st-bufferwithtolerance],
but you specify the number of segments instead of providing tolerance to
determine how much the resulting geography can deviate from the ideal
buffer radius.

+   `geography`: The input `GEOGRAPHY` to encircle with the buffer radius.
+   `buffer_radius`: `DOUBLE` that represents the radius of the buffer
    around the input geography. The radius is in meters. Note that polygons
    contract when buffered with a negative `buffer_radius`. Polygon shells and
    holes that are contracted to a point are discarded.
+   `num_seg_quarter_circle`: (Optional) `DOUBLE` specifies the number of
    segments that are used to approximate a quarter circle. The default value is
    `8.0`. Naming this argument is optional.
+   `endcap`: (Optional) `STRING` allows you to specify one of two endcap
    styles: `ROUND` and `FLAT`. The default value is `ROUND`. This option only
    affects the endcaps of buffered linestrings.
+   `side`: (Optional) `STRING` allows you to specify one of three possibilities
    for lines: `BOTH`, `LEFT`, and `RIGHT`. The default is `BOTH`. This option
    only affects how linestrings are buffered.
+   `use_spheroid`: (Optional) `BOOL` determines how this function measures
    distance. If `use_spheroid` is `FALSE`, the function measures distance on
    the surface of a perfect sphere. The `use_spheroid` parameter
    currently only supports the value `FALSE`. The default value of
    `use_spheroid` is `FALSE`.

**Return type**

Polygon `GEOGRAPHY`

**Example**

The following example shows the result of `ST_BUFFER` on a point. A buffered
point is an approximated circle. When `num_seg_quarter_circle = 2`, there are
two line segments in a quarter circle, and therefore the buffered circle has
eight sides and [`ST_NUMPOINTS`][st-numpoints] returns nine vertices. When
`num_seg_quarter_circle = 8`, there are eight line segments in a quarter circle,
and therefore the buffered circle has thirty-two sides and
[`ST_NUMPOINTS`][st-numpoints] returns thirty-three vertices.

```sql
SELECT
  -- num_seg_quarter_circle=2
  ST_NUMPOINTS(ST_BUFFER(ST_GEOGFROMTEXT('POINT(1 2)'), 50, 2)) AS eight_sides,
  -- num_seg_quarter_circle=8, since 8 is the default
  ST_NUMPOINTS(ST_BUFFER(ST_GEOGFROMTEXT('POINT(100 2)'), 50)) AS thirty_two_sides;

/*-------------+------------------*
 | eight_sides | thirty_two_sides |
 +-------------+------------------+
 | 9           | 33               |
 *-------------+------------------*/
```

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

[st-bufferwithtolerance]: #st_bufferwithtolerance

[st-numpoints]: #st_numpoints

### `ST_BUFFERWITHTOLERANCE`

```sql
ST_BUFFERWITHTOLERANCE(
    geography,
    buffer_radius,
    tolerance_meters => tolernace
    [, use_spheroid => boolean_expression]
    [, endcap => endcap_style]
    [, side => line_side])
```

Returns a `GEOGRAPHY` that represents the buffer around the input `GEOGRAPHY`.
This function is similar to [`ST_BUFFER`][st-buffer],
but you provide tolerance instead of segments to determine how much the
resulting geography can deviate from the ideal buffer radius.

+   `geography`: The input `GEOGRAPHY` to encircle with the buffer radius.
+   `buffer_radius`: `DOUBLE` that represents the radius of the buffer
    around the input geography. The radius is in meters. Note that polygons
    contract when buffered with a negative `buffer_radius`. Polygon shells
    and holes that are contracted to a point are discarded.
+   `tolerance_meters`: `DOUBLE` specifies a tolerance in meters with
    which the shape is approximated. Tolerance determines how much a polygon can
    deviate from the ideal radius. Naming this argument is optional.
+   `endcap`: (Optional) `STRING` allows you to specify one of two endcap
    styles: `ROUND` and `FLAT`. The default value is `ROUND`. This option only
    affects the endcaps of buffered linestrings.
+   `side`: (Optional) `STRING` allows you to specify one of three possible line
    styles: `BOTH`, `LEFT`, and `RIGHT`. The default is `BOTH`. This option only
    affects the endcaps of buffered linestrings.
+   `use_spheroid`: (Optional) `BOOL` determines how this function measures
    distance. If `use_spheroid` is `FALSE`, the function measures distance on
    the surface of a perfect sphere. The `use_spheroid` parameter
    currently only supports the value `FALSE`. The default value of
    `use_spheroid` is `FALSE`.

**Return type**

Polygon `GEOGRAPHY`

**Example**

The following example shows the results of `ST_BUFFERWITHTOLERANCE` on a point,
given two different values for tolerance but with the same buffer radius of
`100`. A buffered point is an approximated circle. When `tolerance_meters=25`,
the tolerance is a large percentage of the buffer radius, and therefore only
five segments are used to approximate a circle around the input point. When
`tolerance_meters=1`, the tolerance is a much smaller percentage of the buffer
radius, and therefore twenty-four edges are used to approximate a circle around
the input point.

```sql
SELECT
  -- tolerance_meters=25, or 25% of the buffer radius.
  ST_NumPoints(ST_BUFFERWITHTOLERANCE(ST_GEOGFROMTEXT('POINT(1 2)'), 100, 25)) AS five_sides,
  -- tolerance_meters=1, or 1% of the buffer radius.
  st_NumPoints(ST_BUFFERWITHTOLERANCE(ST_GEOGFROMTEXT('POINT(100 2)'), 100, 1)) AS twenty_four_sides;

/*------------+-------------------*
 | five_sides | twenty_four_sides |
 +------------+-------------------+
 | 6          | 24                |
 *------------+-------------------*/
```

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

[st-buffer]: #st_buffer

### `ST_CENTROID`

```sql
ST_CENTROID(geography_expression)
```

**Description**

Returns the _centroid_ of the input `GEOGRAPHY` as a single point `GEOGRAPHY`.

The _centroid_ of a `GEOGRAPHY` is the weighted average of the centroids of the
highest-dimensional components in the `GEOGRAPHY`. The centroid for components
in each dimension is defined as follows:

+   The centroid of points is the arithmetic mean of the input coordinates.
+   The centroid of linestrings is the centroid of all the edges weighted by
    length. The centroid of each edge is the geodesic midpoint of the edge.
+   The centroid of a polygon is its center of mass.

If the input `GEOGRAPHY` is empty, an empty `GEOGRAPHY` is returned.

**Constraints**

In the unlikely event that the centroid of a `GEOGRAPHY` cannot be defined by a
single point on the surface of the Earth, a deterministic but otherwise
arbitrary point is returned. This can only happen if the centroid is exactly at
the center of the Earth, such as the centroid for a pair of antipodal points,
and the likelihood of this happening is vanishingly small.

**Return type**

Point `GEOGRAPHY`

### `ST_CLOSESTPOINT`

```sql
ST_CLOSESTPOINT(geography_1, geography_2[, use_spheroid])
```

**Description**

Returns a `GEOGRAPHY` containing a point on
`geography_1` with the smallest possible distance to `geography_2`. This implies
that the distance between the point returned by `ST_CLOSESTPOINT` and
`geography_2` is less than or equal to the distance between any other point on
`geography_1` and `geography_2`.

If either of the input `GEOGRAPHY`s is empty, `ST_CLOSESTPOINT` returns `NULL`.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

Point `GEOGRAPHY`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_CLUSTERDBSCAN`

```sql
ST_CLUSTERDBSCAN(geography_column, epsilon, minimum_geographies)
OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]

```

Performs [DBSCAN clustering][dbscan-link] on a column of geographies. Returns a
0-based cluster number.

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Input parameters**

+   `geography_column`: A column of `GEOGRAPHY`s that
    is clustered.
+   `epsilon`: The epsilon that specifies the radius, measured in meters, around
    a core value. Non-negative `DOUBLE` value.
+   `minimum_geographies`: Specifies the minimum number of geographies in a
    single cluster. Only dense input forms a cluster, otherwise it is classified
    as noise. Non-negative `INT64` value.

**Geography types and the DBSCAN algorithm**

The DBSCAN algorithm identifies high-density clusters of data and marks outliers
in low-density areas of noise. Geographies passed in through `geography_column`
are classified in one of three ways by the DBSCAN algorithm:

+   Core value: A geography is a core value if it is within `epsilon` distance
    of `minimum_geographies` geographies, including itself. The core value
    starts a new cluster, or is added to the same cluster as a core value within
    `epsilon` distance. Core values are grouped in a cluster together with all
    other core and border values that are within `epsilon` distance.
+   Border value: A geography is a border value if it is within epsilon distance
    of a core value. It is added to the same cluster as a core value within
    `epsilon` distance. A border value may be within `epsilon` distance of more
    than one cluster. In this case, it may be arbitrarily assigned to either
    cluster and the function will produce the same result in subsequent calls.
+   Noise: A geography is noise if it is neither a core nor a border value.
    Noise values are assigned to a `NULL` cluster. An empty
    `GEOGRAPHY` is always classified as noise.

**Constraints**

+   The argument `minimum_geographies` is a non-negative
    `INT64`and `epsilon` is a non-negative
    `DOUBLE`.
+   An empty geography cannot join any cluster.
+   Multiple clustering assignments could be possible for a border value. If a
    geography is a border value, `ST_CLUSTERDBSCAN` will assign it to an
    arbitrary valid cluster.

**Return type**

`INT64` for each geography in the geography column.

**Examples**

This example performs DBSCAN clustering with a radius of 100,000 meters with a
`minimum_geographies` argument of 1. The geographies being analyzed are a
mixture of points, lines, and polygons.

```sql
WITH Geos as
  (SELECT 1 as row_id, ST_GEOGFROMTEXT('POINT EMPTY') as geo UNION ALL
    SELECT 2, ST_GEOGFROMTEXT('MULTIPOINT(1 1, 2 2, 4 4, 5 2)') UNION ALL
    SELECT 3, ST_GEOGFROMTEXT('POINT(14 15)') UNION ALL
    SELECT 4, ST_GEOGFROMTEXT('LINESTRING(40 1, 42 34, 44 39)') UNION ALL
    SELECT 5, ST_GEOGFROMTEXT('POLYGON((40 2, 40 1, 41 2, 40 2))'))
SELECT row_id, geo, ST_CLUSTERDBSCAN(geo, 1e5, 1) OVER () AS cluster_num FROM
Geos ORDER BY row_id

/*--------+-----------------------------------+-------------*
 | row_id |                geo                | cluster_num |
 +--------+-----------------------------------+-------------+
 | 1      | GEOMETRYCOLLECTION EMPTY          | NULL        |
 | 2      | MULTIPOINT(1 1, 2 2, 5 2, 4 4)    | 0           |
 | 3      | POINT(14 15)                      | 1           |
 | 4      | LINESTRING(40 1, 42 34, 44 39)    | 2           |
 | 5      | POLYGON((40 2, 40 1, 41 2, 40 2)) | 2           |
 *--------+-----------------------------------+-------------*/
```

[dbscan-link]: https://en.wikipedia.org/wiki/DBSCAN

### `ST_CONTAINS`

```sql
ST_CONTAINS(geography_1, geography_2)
```

**Description**

Returns `TRUE` if no point of `geography_2` is outside `geography_1`, and
the interiors intersect; returns `FALSE` otherwise.

NOTE: A `GEOGRAPHY` *does not* contain its own
boundary. Compare with [`ST_COVERS`][st_covers].

**Return type**

`BOOL`

**Example**

The following query tests whether the polygon `POLYGON((1 1, 20 1, 10 20, 1 1))`
contains each of the three points `(0, 0)`, `(1, 1)`, and `(10, 10)`, which lie
on the exterior, the boundary, and the interior of the polygon respectively.

```sql
SELECT
  ST_GEOGPOINT(i, i) AS p,
  ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((1 1, 20 1, 10 20, 1 1))'),
              ST_GEOGPOINT(i, i)) AS `contains`
FROM UNNEST([0, 1, 10]) AS i;

/*--------------+----------*
 | p            | contains |
 +--------------+----------+
 | POINT(0 0)   | FALSE    |
 | POINT(1 1)   | FALSE    |
 | POINT(10 10) | TRUE     |
 *--------------+----------*/
```

[st_covers]: #st_covers

### `ST_CONVEXHULL`

```sql
ST_CONVEXHULL(geography_expression)
```

**Description**

Returns the convex hull for the input `GEOGRAPHY`. The convex hull is the
smallest convex `GEOGRAPHY` that covers the input. A `GEOGRAPHY` is convex if
for every pair of points in the `GEOGRAPHY`, the geodesic edge connecting the
points are also contained in the same `GEOGRAPHY`.

In most cases, the convex hull consists of a single polygon. Notable edge cases
include the following:

*   The convex hull of a single point is also a point.
*   The convex hull of two or more collinear points is a linestring as long as
    that linestring is convex.
*   If the input `GEOGRAPHY` spans more than a
    hemisphere, the convex hull is the full globe. This includes any input that
    contains a pair of antipodal points.
*   `ST_CONVEXHULL` returns `NULL` if the input is either `NULL` or the empty
    `GEOGRAPHY`.

**Return type**

`GEOGRAPHY`

**Examples**

The convex hull returned by `ST_CONVEXHULL` can be a point, linestring, or a
polygon, depending on the input.

```sql
WITH Geographies AS
 (SELECT ST_GEOGFROMTEXT('POINT(1 1)') AS g UNION ALL
  SELECT ST_GEOGFROMTEXT('LINESTRING(1 1, 2 2)') AS g UNION ALL
  SELECT ST_GEOGFROMTEXT('MULTIPOINT(2 11, 4 12, 0 15, 1 9, 1 12)') AS g)
SELECT
  g AS input_geography,
  ST_CONVEXHULL(g) AS convex_hull
FROM Geographies;

/*-----------------------------------------+--------------------------------------------------------*
 |             input_geography             |                      convex_hull                       |
 +-----------------------------------------+--------------------------------------------------------+
 | POINT(1 1)                              | POINT(0.999999999999943 1)                             |
 | LINESTRING(1 1, 2 2)                    | LINESTRING(2 2, 1.49988573656168 1.5000570914792, 1 1) |
 | MULTIPOINT(1 9, 4 12, 2 11, 1 12, 0 15) | POLYGON((1 9, 4 12, 0 15, 1 9))                        |
 *-----------------------------------------+--------------------------------------------------------*/
```

### `ST_COVEREDBY`

```sql
ST_COVEREDBY(geography_1, geography_2)
```

**Description**

Returns `FALSE` if `geography_1` or `geography_2` is empty. Returns `TRUE` if no
points of `geography_1` lie in the exterior of `geography_2`.

Given two `GEOGRAPHY`s `a` and `b`,
`ST_COVEREDBY(a, b)` returns the same result as
[`ST_COVERS`][st-covers]`(b, a)`. Note the opposite order of arguments.

**Return type**

`BOOL`

[st-covers]: #st_covers

### `ST_COVERS`

```sql
ST_COVERS(geography_1, geography_2)
```

**Description**

Returns `FALSE` if `geography_1` or `geography_2` is empty.
Returns `TRUE` if no points of `geography_2` lie in the exterior of
`geography_1`.

**Return type**

`BOOL`

**Example**

The following query tests whether the polygon `POLYGON((1 1, 20 1, 10 20, 1 1))`
covers each of the three points `(0, 0)`, `(1, 1)`, and `(10, 10)`, which lie
on the exterior, the boundary, and the interior of the polygon respectively.

```sql
SELECT
  ST_GEOGPOINT(i, i) AS p,
  ST_COVERS(ST_GEOGFROMTEXT('POLYGON((1 1, 20 1, 10 20, 1 1))'),
            ST_GEOGPOINT(i, i)) AS `covers`
FROM UNNEST([0, 1, 10]) AS i;

/*--------------+--------*
 | p            | covers |
 +--------------+--------+
 | POINT(0 0)   | FALSE  |
 | POINT(1 1)   | TRUE   |
 | POINT(10 10) | TRUE   |
 *--------------+--------*/
```

### `ST_DIFFERENCE`

```sql
ST_DIFFERENCE(geography_1, geography_2)
```

**Description**

Returns a `GEOGRAPHY` that represents the point set
difference of `geography_1` and `geography_2`. Therefore, the result consists of
the part of `geography_1` that does not intersect with `geography_2`.

If `geometry_1` is completely contained in `geometry_2`, then `ST_DIFFERENCE`
returns an empty `GEOGRAPHY`.

**Constraints**

The underlying geometric objects that a ZetaSQL
`GEOGRAPHY` represents correspond to a *closed* point
set. Therefore, `ST_DIFFERENCE` is the closure of the point set difference of
`geography_1` and `geography_2`. This implies that if `geography_1` and
`geography_2` intersect, then a portion of the boundary of `geography_2` could
be in the difference.

**Return type**

`GEOGRAPHY`

**Example**

The following query illustrates the difference between `geog1`, a larger polygon
`POLYGON((0 0, 10 0, 10 10, 0 0))` and `geog1`, a smaller polygon
`POLYGON((4 2, 6 2, 8 6, 4 2))` that intersects with `geog1`. The result is
`geog1` with a hole where `geog2` intersects with it.

```sql
SELECT
  ST_DIFFERENCE(
      ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 0))'),
      ST_GEOGFROMTEXT('POLYGON((4 2, 6 2, 8 6, 4 2))')
  );

/*--------------------------------------------------------*
 | difference_of_geog1_and_geog2                          |
 +--------------------------------------------------------+
 | POLYGON((0 0, 10 0, 10 10, 0 0), (8 6, 6 2, 4 2, 8 6)) |
 *--------------------------------------------------------*/
```

### `ST_DIMENSION`

```sql
ST_DIMENSION(geography_expression)
```

**Description**

Returns the dimension of the highest-dimensional element in the input
`GEOGRAPHY`.

The dimension of each possible element is as follows:

+   The dimension of a point is `0`.
+   The dimension of a linestring is `1`.
+   The dimension of a polygon is `2`.

If the input `GEOGRAPHY` is empty, `ST_DIMENSION`
returns `-1`.

**Return type**

`INT64`

### `ST_DISJOINT`

```sql
ST_DISJOINT(geography_1, geography_2)
```

**Description**

Returns `TRUE` if the intersection of `geography_1` and `geography_2` is empty,
that is, no point in `geography_1` also appears in `geography_2`.

`ST_DISJOINT` is the logical negation of [`ST_INTERSECTS`][st-intersects].

**Return type**

`BOOL`

[st-intersects]: #st_intersects

### `ST_DISTANCE`

```
ST_DISTANCE(geography_1, geography_2[, use_spheroid])
```

**Description**

Returns the shortest distance in meters between two non-empty
`GEOGRAPHY`s.

If either of the input `GEOGRAPHY`s is empty,
`ST_DISTANCE` returns `NULL`.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere. If `use_spheroid` is `TRUE`, the function measures
distance on the surface of the [WGS84][wgs84-link] spheroid. The default value
of `use_spheroid` is `FALSE`.

**Return type**

`DOUBLE`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_DUMP`

```sql
ST_DUMP(geography[, dimension])
```

**Description**

Returns an `ARRAY` of simple
`GEOGRAPHY`s where each element is a component of
the input `GEOGRAPHY`. A simple
`GEOGRAPHY` consists of a single point, linestring,
or polygon. If the input `GEOGRAPHY` is simple, the
result is a single element. When the input
`GEOGRAPHY` is a collection, `ST_DUMP` returns an
`ARRAY` with one simple
`GEOGRAPHY` for each component in the collection.

If `dimension` is provided, the function only returns
`GEOGRAPHY`s of the corresponding dimension. A
dimension of -1 is equivalent to omitting `dimension`.

**Return Type**

`ARRAY<GEOGRAPHY>`

**Examples**

The following example shows how `ST_DUMP` returns the simple geographies within
a complex geography.

```sql
WITH example AS (
  SELECT ST_GEOGFROMTEXT('POINT(0 0)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 1)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))'))
SELECT
  geography AS original_geography,
  ST_DUMP(geography) AS dumped_geographies
FROM example

/*-------------------------------------+------------------------------------*
 |         original_geographies        |      dumped_geographies            |
 +-------------------------------------+------------------------------------+
 | POINT(0 0)                          | [POINT(0 0)]                       |
 | MULTIPOINT(0 0, 1 1)                | [POINT(0 0), POINT(1 1)]           |
 | GEOMETRYCOLLECTION(POINT(0 0),      | [POINT(0 0), LINESTRING(1 2, 2 1)] |
 |   LINESTRING(1 2, 2 1))             |                                    |
 *-------------------------------------+------------------------------------*/
 ```

The following example shows how `ST_DUMP` with the dimension argument only
returns simple geographies of the given dimension.

```sql
WITH example AS (
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))') AS geography)
SELECT
  geography AS original_geography,
  ST_DUMP(geography, 1) AS dumped_geographies
FROM example

/*-------------------------------------+------------------------------*
 |         original_geographies        |      dumped_geographies      |
 +-------------------------------------+------------------------------+
 | GEOMETRYCOLLECTION(POINT(0 0),      | [LINESTRING(1 2, 2 1)]       |
 |   LINESTRING(1 2, 2 1))             |                              |
 *-------------------------------------+------------------------------*/
```

### `ST_DUMPPOINTS`

```sql
ST_DUMPPOINTS(geography)
```

**Description**

Takes an input geography and returns all of its points, line vertices, and
polygon vertices as an array of point geographies.

**Return Type**

`ARRAY<Point GEOGRAPHY>`

**Examples**

```sql
WITH example AS (
  SELECT ST_GEOGFROMTEXT('POINT(0 0)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 1)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))'))
SELECT
  geography AS original_geography,
  ST_DUMPPOINTS(geography) AS dumped_points_geographies
FROM example

/*-------------------------------------+------------------------------------*
 | original_geographies                | dumped_points_geographies          |
 +-------------------------------------+------------------------------------+
 | POINT(0 0)                          | [POINT(0 0)]                       |
 | MULTIPOINT(0 0, 1 1)                | [POINT(0 0),POINT(1 1)]            |
 | GEOMETRYCOLLECTION(POINT(0 0),      | [POINT(0 0),POINT(1 2),POINT(2 1)] |
 |   LINESTRING(1 2, 2 1))             |                                    |
 *-------------------------------------+------------------------------------*/
```

### `ST_DWITHIN`

```sql
ST_DWITHIN(geography_1, geography_2, distance[, use_spheroid])
```

**Description**

Returns `TRUE` if the distance between at least one point in `geography_1` and
one point in `geography_2` is less than or equal to the distance given by the
`distance` argument; otherwise, returns `FALSE`. If either input
`GEOGRAPHY` is empty, `ST_DWithin` returns `FALSE`. The
given `distance` is in meters on the surface of the Earth.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

`BOOL`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_ENDPOINT`

```sql
ST_ENDPOINT(linestring_geography)
```

**Description**

Returns the last point of a linestring geography as a point geography. Returns
an error if the input is not a linestring or if the input is empty. Use the
`SAFE` prefix to obtain `NULL` for invalid input instead of an error.

**Return Type**

Point `GEOGRAPHY`

**Example**

```sql
SELECT ST_ENDPOINT(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)')) last

/*--------------*
 | last         |
 +--------------+
 | POINT(3 3)   |
 *--------------*/
```

### `ST_EQUALS`

```sql
ST_EQUALS(geography_1, geography_2)
```

**Description**

Returns `TRUE` if `geography_1` and `geography_2` represent the same

`GEOGRAPHY` value. More precisely, this means that
one of the following conditions holds:
+   `ST_COVERS(geography_1, geography_2) = TRUE` and `ST_COVERS(geography_2,
    geography_1) = TRUE`
+   Both `geography_1` and `geography_2` are empty.

Therefore, two `GEOGRAPHY`s may be equal even if the
ordering of points or vertices differ, as long as they still represent the same
geometric structure.

**Constraints**

`ST_EQUALS` is not guaranteed to be a transitive function.

**Return type**

`BOOL`

### `ST_EXTENT`

```sql
ST_EXTENT(geography_expression)
```

**Description**

Returns a `STRUCT` that represents the bounding box for the set of input
`GEOGRAPHY` values. The bounding box is the minimal rectangle that encloses the
geography. The edges of the rectangle follow constant lines of longitude and
latitude.

Caveats:

+ Returns `NULL` if all the inputs are `NULL` or empty geographies.
+ The bounding box might cross the antimeridian if this allows for a smaller
  rectangle. In this case, the bounding box has one of its longitudinal bounds
  outside of the [-180, 180] range, so that `xmin` is smaller than the eastmost
  value `xmax`.
+ If the longitude span of the bounding box is larger than or equal to 180
  degrees, the function returns the bounding box with the longitude range of
  [-180, 180].

**Return type**

`STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>`.

Bounding box parts:

+ `xmin`: The westmost constant longitude line that bounds the rectangle.
+ `xmax`: The eastmost constant longitude line that bounds the rectangle.
+ `ymin`: The minimum constant latitude line that bounds the rectangle.
+ `ymax`: The maximum constant latitude line that bounds the rectangle.

**Example**

```sql
WITH data AS (
  SELECT 1 id, ST_GEOGFROMTEXT('POLYGON((-125 48, -124 46, -117 46, -117 49, -125 48))') g
  UNION ALL
  SELECT 2 id, ST_GEOGFROMTEXT('POLYGON((172 53, -130 55, -141 70, 172 53))') g
  UNION ALL
  SELECT 3 id, ST_GEOGFROMTEXT('POINT EMPTY') g
)
SELECT ST_EXTENT(g) AS box
FROM data

/*----------------------------------------------*
 | box                                          |
 +----------------------------------------------+
 | {xmin:172, ymin:46, xmax:243, ymax:70}       |
 *----------------------------------------------*/
```

[`ST_BOUNDINGBOX`][st-boundingbox] for the non-aggregate version of `ST_EXTENT`.

[st-boundingbox]: #st_boundingbox

### `ST_EXTERIORRING`

```sql
ST_EXTERIORRING(polygon_geography)
```

**Description**

Returns a linestring geography that corresponds to the outermost ring of a
polygon geography.

+   If the input geography is a polygon, gets the outermost ring of the polygon
    geography and returns the corresponding linestring.
+   If the input is the full `GEOGRAPHY`, returns an empty geography.
+   Returns an error if the input is not a single polygon.

Use the `SAFE` prefix to return `NULL` for invalid input instead of an error.

**Return type**

+ Linestring `GEOGRAPHY`
+ Empty `GEOGRAPHY`

**Examples**

```sql
WITH geo as
 (SELECT ST_GEOGFROMTEXT('POLYGON((0 0, 1 4, 2 2, 0 0))') AS g UNION ALL
  SELECT ST_GEOGFROMTEXT('''POLYGON((1 1, 1 10, 5 10, 5 1, 1 1),
                                  (2 2, 3 4, 2 4, 2 2))''') as g)
SELECT ST_EXTERIORRING(g) AS ring FROM geo;

/*---------------------------------------*
 | ring                                  |
 +---------------------------------------+
 | LINESTRING(2 2, 1 4, 0 0, 2 2)        |
 | LINESTRING(5 1, 5 10, 1 10, 1 1, 5 1) |
 *---------------------------------------*/
```

### `ST_GEOGFROM`

```sql
ST_GEOGFROM(expression)
```

**Description**

Converts an expression for a `STRING` or `BYTES` value into a
`GEOGRAPHY` value.

If `expression` represents a `STRING` value, it must be a valid
`GEOGRAPHY` representation in one of the following formats:

+ WKT format. To learn more about this format and the requirements to use it,
  see [ST_GEOGFROMTEXT][st-geogfromtext].
+ WKB in hexadecimal text format. To learn more about this format and the
  requirements to use it, see [ST_GEOGFROMWKB][st-geogfromwkb].
+ GeoJSON format. To learn more about this format and the
  requirements to use it, see [ST_GEOGFROMGEOJSON][st-geogfromgeojson].

If `expression` represents a `BYTES` value, it must be a valid `GEOGRAPHY`
binary expression in WKB format. To learn more about this format and the
requirements to use it, see [ST_GEOGFROMWKB][st-geogfromwkb].

If `expression` is `NULL`, the output is `NULL`.

**Return type**

`GEOGRAPHY`

**Examples**

This takes a WKT-formatted string and returns a `GEOGRAPHY` polygon:

```sql
SELECT ST_GEOGFROM('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))') AS WKT_format

/*------------------------------------*
 | WKT_format                         |
 +------------------------------------+
 | POLYGON((2 0, 2 2, 0 2, 0 0, 2 0)) |
 *------------------------------------*/
```

This takes a WKB-formatted hexadecimal-encoded string and returns a
`GEOGRAPHY` point:

```sql
SELECT ST_GEOGFROM(FROM_HEX('010100000000000000000000400000000000001040')) AS WKB_format

/*----------------*
 | WKB_format     |
 +----------------+
 | POINT(2 4)     |
 *----------------*/
```

This takes WKB-formatted bytes and returns a `GEOGRAPHY` point:

```sql
SELECT ST_GEOGFROM('010100000000000000000000400000000000001040')-AS WKB_format

/*----------------*
 | WKB_format     |
 +----------------+
 | POINT(2 4)     |
 *----------------*/
```

This takes a GeoJSON-formatted string and returns a `GEOGRAPHY` polygon:

```sql
SELECT ST_GEOGFROM(
  '{ "type": "Polygon", "coordinates": [ [ [2, 0], [2, 2], [1, 2], [0, 2], [0, 0], [2, 0] ] ] }'
) AS GEOJSON_format

/*-----------------------------------------*
 | GEOJSON_format                          |
 +-----------------------------------------+
 | POLYGON((2 0, 2 2, 1 2, 0 2, 0 0, 2 0)) |
 *-----------------------------------------*/
```

[st-geogfromtext]: #st_geogfromtext

[st-geogfromwkb]: #st_geogfromwkb

[st-geogfromgeojson]: #st_geogfromgeojson

### `ST_GEOGFROMGEOJSON`

```sql
ST_GEOGFROMGEOJSON(geojson_string [, make_valid => constant_expression])
```

**Description**

Returns a `GEOGRAPHY` value that corresponds to the
input [GeoJSON][geojson-link] representation.

`ST_GEOGFROMGEOJSON` accepts input that is [RFC 7946][geojson-spec-link]
compliant.

If the parameter `make_valid` is set to `TRUE`, the function attempts to repair
polygons that don't conform to [Open Geospatial Consortium][ogc-link] semantics.
This parameter uses named argument syntax, and should be specified using
`make_valid => argument_value` syntax.

A ZetaSQL `GEOGRAPHY` has spherical
geodesic edges, whereas a GeoJSON `Geometry` object explicitly has planar edges.
To convert between these two types of edges, ZetaSQL adds additional
points to the line where necessary so that the resulting sequence of edges
remains within 10 meters of the original edge.

See [`ST_ASGEOJSON`][st-asgeojson] to format a
`GEOGRAPHY` as GeoJSON.

**Constraints**

The JSON input is subject to the following constraints:

+   `ST_GEOGFROMGEOJSON` only accepts JSON geometry fragments and cannot be used
    to ingest a whole JSON document.
+   The input JSON fragment must consist of a GeoJSON geometry type, which
    includes `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`,
    `MultiPolygon`, and `GeometryCollection`. Any other GeoJSON type such as
    `Feature` or `FeatureCollection` will result in an error.
+   A position in the `coordinates` member of a GeoJSON geometry type must
    consist of exactly two elements. The first is the longitude and the second
    is the latitude. Therefore, `ST_GEOGFROMGEOJSON` does not support the
    optional third element for a position in the `coordinates` member.

**Return type**

`GEOGRAPHY`

[geojson-link]: https://en.wikipedia.org/wiki/GeoJSON

[geojson-spec-link]: https://tools.ietf.org/html/rfc7946

[ogc-link]: https://www.ogc.org/standards/sfa

[st-asgeojson]: #st_asgeojson

### `ST_GEOGFROMKML`

```sql
ST_GEOGFROMKML(kml_geometry)
```

Takes a `STRING` [KML geometry][kml-geometry-link] and returns a
`GEOGRAPHY`. The KML geomentry can include:

+  Point with coordinates element only
+  Linestring with coordinates element only
+  Polygon with boundary elements only
+  Multigeometry

[kml-geometry-link]: https://developers.google.com/kml/documentation/kmlreference#geometry

### `ST_GEOGFROMTEXT`

+ [Signature 1](#st_geogfromtext_signature1)
+ [Signature 2](#st_geogfromtext_signature2)

#### Signature 1 
<a id="st_geogfromtext_signature1"></a>

```sql
ST_GEOGFROMTEXT(wkt_string[, oriented])
```

**Description**

Returns a `GEOGRAPHY` value that corresponds to the
input [WKT][wkt-link] representation.

This function supports an optional parameter of type
`BOOL`, `oriented`. If this parameter is set to
`TRUE`, any polygons in the input are assumed to be oriented as follows:
if someone walks along the boundary of the polygon in the order of
the input vertices, the interior of the polygon is on the left. This allows
WKT to represent polygons larger than a hemisphere. If `oriented` is `FALSE` or
omitted, this function returns the polygon with the smaller area.
See also [`ST_MAKEPOLYGONORIENTED`][st-makepolygonoriented] which is similar
to `ST_GEOGFROMTEXT` with `oriented=TRUE`.

To format `GEOGRAPHY` as WKT, use
[`ST_ASTEXT`][st-astext].

**Constraints**

*   All input edges are assumed to be spherical geodesics, and *not* planar
    straight lines. For reading data in a planar projection, consider using
    [`ST_GEOGFROMGEOJSON`][st-geogfromgeojson].
*   The function does not support three-dimensional geometries that have a `Z`
    suffix, nor does it support linear referencing system geometries with an `M`
    suffix.
*   The function only supports geometry primitives and multipart geometries. In
    particular it supports only point, multipoint, linestring, multilinestring,
    polygon, multipolygon, and geometry collection.

**Return type**

`GEOGRAPHY`

**Example**

The following query reads the WKT string `POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))`
both as a non-oriented polygon and as an oriented polygon, and checks whether
each result contains the point `(1, 1)`.

```sql
WITH polygon AS (SELECT 'POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))' AS p)
SELECT
  ST_CONTAINS(ST_GEOGFROMTEXT(p), ST_GEOGPOINT(1, 1)) AS fromtext_default,
  ST_CONTAINS(ST_GEOGFROMTEXT(p, FALSE), ST_GEOGPOINT(1, 1)) AS non_oriented,
  ST_CONTAINS(ST_GEOGFROMTEXT(p, TRUE),  ST_GEOGPOINT(1, 1)) AS oriented
FROM polygon;

/*-------------------+---------------+-----------*
 | fromtext_default  | non_oriented  | oriented  |
 +-------------------+---------------+-----------+
 | TRUE              | TRUE          | FALSE     |
 *-------------------+---------------+-----------*/
```

#### Signature 2 
<a id="st_geogfromtext_signature2"></a>

```sql
ST_GEOGFROMTEXT(wkt_string[, oriented => boolean_constant_1]
    [, planar => boolean_constant_2] [, make_valid => boolean_constant_3])
```

**Description**

Returns a `GEOGRAPHY` value that corresponds to the
input [WKT][wkt-link] representation.

This function supports three optional parameters  of type
`BOOL`: `oriented`, `planar`, and `make_valid`.
This signature uses named arguments syntax, and the parameters should be
specified using `parameter_name => parameter_value` syntax, in any order.

If the `oriented` parameter is set to
`TRUE`, any polygons in the input are assumed to be oriented as follows:
if someone walks along the boundary of the polygon in the order of
the input vertices, the interior of the polygon is on the left. This allows
WKT to represent polygons larger than a hemisphere. If `oriented` is `FALSE` or
omitted, this function returns the polygon with the smaller area.
See also [`ST_MAKEPOLYGONORIENTED`][st-makepolygonoriented] which is similar
to `ST_GEOGFROMTEXT` with `oriented=TRUE`.

If the parameter `planar` is set to `TRUE`, the edges of the line strings and
polygons are assumed to use planar map semantics, rather than ZetaSQL
default spherical geodesics semantics.

If the parameter `make_valid` is set to `TRUE`, the function attempts to repair
polygons that don't conform to [Open Geospatial Consortium][ogc-link] semantics.

To format `GEOGRAPHY` as WKT, use
[`ST_ASTEXT`][st-astext].

**Constraints**

*   All input edges are assumed to be spherical geodesics by default, and *not*
    planar straight lines. For reading data in a planar projection,
    pass `planar => TRUE` argument, or consider using
    [`ST_GEOGFROMGEOJSON`][st-geogfromgeojson].
*   The function does not support three-dimensional geometries that have a `Z`
    suffix, nor does it support linear referencing system geometries with an `M`
    suffix.
*   The function only supports geometry primitives and multipart geometries. In
    particular it supports only point, multipoint, linestring, multilinestring,
    polygon, multipolygon, and geometry collection.
*   `oriented` and `planar` cannot be equal to `TRUE` at the same time.
*   `oriented` and `make_valid` cannot be equal to `TRUE` at the same time.

**Example**

The following query reads the WKT string `POLYGON((0 0, 0 2, 2 2, 0 2, 0 0))`
both as a non-oriented polygon and as an oriented polygon, and checks whether
each result contains the point `(1, 1)`.

```sql
WITH polygon AS (SELECT 'POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))' AS p)
SELECT
  ST_CONTAINS(ST_GEOGFROMTEXT(p), ST_GEOGPOINT(1, 1)) AS fromtext_default,
  ST_CONTAINS(ST_GEOGFROMTEXT(p, oriented => FALSE), ST_GEOGPOINT(1, 1)) AS non_oriented,
  ST_CONTAINS(ST_GEOGFROMTEXT(p, oriented => TRUE),  ST_GEOGPOINT(1, 1)) AS oriented
FROM polygon;

/*-------------------+---------------+-----------*
 | fromtext_default  | non_oriented  | oriented  |
 +-------------------+---------------+-----------+
 | TRUE              | TRUE          | FALSE     |
 *-------------------+---------------+-----------*/
```

The following query converts a WKT string with an invalid polygon to
`GEOGRAPHY`. The WKT string violates two properties
of a valid polygon - the loop describing the polygon is not closed, and it
contains self-intersection. With the `make_valid` option, `ST_GEOGFROMTEXT`
successfully converts it to a multipolygon shape.

```sql
WITH data AS (
  SELECT 'POLYGON((0 -1, 2 1, 2 -1, 0 1))' wkt)
SELECT
  SAFE.ST_GEOGFROMTEXT(wkt) as geom,
  SAFE.ST_GEOGFROMTEXT(wkt, make_valid => TRUE) as valid_geom
FROM data

/*------+-----------------------------------------------------------------*
 | geom | valid_geom                                                      |
 +------+-----------------------------------------------------------------+
 | NULL | MULTIPOLYGON(((0 -1, 1 0, 0 1, 0 -1)), ((1 0, 2 -1, 2 1, 1 0))) |
 *------+-----------------------------------------------------------------*/
```

[ogc-link]: https://www.ogc.org/standards/sfa

[wkt-link]: https://en.wikipedia.org/wiki/Well-known_text

[st-makepolygonoriented]: #st_makepolygonoriented

[st-astext]: #st_astext

[st-geogfromgeojson]: #st_geogfromgeojson

### `ST_GEOGFROMWKB`

```sql
ST_GEOGFROMWKB(wkb_bytes_expression)
```

```sql
ST_GEOGFROMWKB(wkb_hex_string_expression)
```

**Description**

Converts an expression for a hexadecimal-text `STRING` or `BYTES`
value into a `GEOGRAPHY` value. The expression must be in
[WKB][wkb-link] format.

To format `GEOGRAPHY` as WKB, use
[`ST_ASBINARY`][st-asbinary].

**Constraints**

All input edges are assumed to be spherical geodesics, and *not* planar straight
lines. For reading data in a planar projection, consider using
[`ST_GEOGFROMGEOJSON`][st-geogfromgeojson].

**Return type**

`GEOGRAPHY`

[wkb-link]: https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary

[st-asbinary]: #st_asbinary

[st-geogfromgeojson]: #st_geogfromgeojson

### `ST_GEOGPOINT`

```sql
ST_GEOGPOINT(longitude, latitude)
```

**Description**

Creates a `GEOGRAPHY` with a single point. `ST_GEOGPOINT` creates a point from
the specified `DOUBLE` longitude (in degrees,
negative west of the Prime Meridian, positive east) and latitude (in degrees,
positive north of the Equator, negative south) parameters and returns that point
in a `GEOGRAPHY` value.

NOTE: Some systems present latitude first; take care with argument order.

**Constraints**

+   Longitudes outside the range \[-180, 180\] are allowed; `ST_GEOGPOINT` uses
    the input longitude modulo 360 to obtain a longitude within \[-180, 180\].
+   Latitudes must be in the range \[-90, 90\]. Latitudes outside this range
    will result in an error.

**Return type**

Point `GEOGRAPHY`

### `ST_GEOGPOINTFROMGEOHASH`

```sql
ST_GEOGPOINTFROMGEOHASH(geohash)
```

**Description**

Returns a `GEOGRAPHY` value that corresponds to a
point in the middle of a bounding box defined in the [GeoHash][geohash-link].

**Return type**

Point `GEOGRAPHY`

[geohash-link]: https://en.wikipedia.org/wiki/Geohash

### `ST_GEOHASH`

```sql
ST_GEOHASH(geography_expression[, maxchars])
```

**Description**

Takes a single-point `GEOGRAPHY` and returns a [GeoHash][geohash-link]
representation of that `GEOGRAPHY` object.

+   `geography_expression`: Represents a `GEOGRAPHY` object. Only a `GEOGRAPHY`
    object that represents a single point is supported. If `ST_GEOHASH` is used
    over an empty `GEOGRAPHY` object, returns `NULL`.
+   `maxchars`: This optional `INT64` parameter specifies the maximum number of
    characters the hash will contain. Fewer characters corresponds to lower
    precision (or, described differently, to a bigger bounding box). `maxchars`
    defaults to 20 if not explicitly specified. A valid `maxchars` value is 1
    to 20. Any value below or above is considered unspecified and the default of
    20 is used.

**Return type**

`STRING`

**Example**

Returns a GeoHash of the Seattle Center with 10 characters of precision.

```sql
SELECT ST_GEOHASH(ST_GEOGPOINT(-122.35, 47.62), 10) geohash

/*--------------*
 | geohash      |
 +--------------+
 | c22yzugqw7   |
 *--------------*/
```

[geohash-link]: https://en.wikipedia.org/wiki/Geohash

### `ST_GEOMETRYTYPE`

```sql
ST_GEOMETRYTYPE(geography_expression)
```

**Description**

Returns the [Open Geospatial Consortium][ogc-link] (OGC) geometry type that
describes the input `GEOGRAPHY` as a `STRING`. The OGC geometry type matches the
types that are used in [WKT][wkt-link] and [GeoJSON][geojson-link] formats and
printed for [ST_ASTEXT][st-astext] and [ST_ASGEOJSON][st-asgeojson].
`ST_GEOMETRYTYPE` returns the OGC geometry type with the "ST_" prefix.

`ST_GEOMETRYTYPE` returns the following given the type on the input:

+   Single point geography: Returns `ST_Point`.
+   Collection of only points: Returns `ST_MultiPoint`.
+   Single linestring geography: Returns `ST_LineString`.
+   Collection of only linestrings: Returns `ST_MultiLineString`.
+   Single polygon geography: Returns `ST_Polygon`.
+   Collection of only polygons: Returns `ST_MultiPolygon`.
+   Collection with elements of different dimensions, or the input is the empty
    geography: Returns `ST_GeometryCollection`.

**Return type**

`STRING`

**Example**

The following example shows how `ST_GEOMETRYTYPE` takes geographies and returns
the names of their OGC geometry types.

```sql
WITH example AS(
  SELECT ST_GEOGFROMTEXT('POINT(0 1)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('MULTILINESTRING((2 2, 3 4), (5 6, 7 7))')
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(MULTIPOINT(-1 2, 0 12), LINESTRING(-2 4, 0 6))')
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))
SELECT
  geography AS WKT,
  ST_GEOMETRYTYPE(geography) AS geometry_type_name
FROM example;

/*-------------------------------------------------------------------+-----------------------*
 | WKT                                                               | geometry_type_name    |
 +-------------------------------------------------------------------+-----------------------+
 | POINT(0 1)                                                        | ST_Point              |
 | MULTILINESTRING((2 2, 3 4), (5 6, 7 7))                           | ST_MultiLineString    |
 | GEOMETRYCOLLECTION(MULTIPOINT(-1 2, 0 12), LINESTRING(-2 4, 0 6)) | ST_GeometryCollection |
 | GEOMETRYCOLLECTION EMPTY                                          | ST_GeometryCollection |
 *-------------------------------------------------------------------+-----------------------*/
```

[ogc-link]: https://www.ogc.org/standards/sfa

[wkt-link]: https://en.wikipedia.org/wiki/Well-known_text

[geojson-link]: https://en.wikipedia.org/wiki/GeoJSON

[st-astext]: #st_astext

[st-asgeojson]: #st_asgeojson

### `ST_INTERIORRINGS`

```sql
ST_INTERIORRINGS(polygon_geography)
```

**Description**

Returns an array of linestring geographies that corresponds to the interior
rings of a polygon geography. Each interior ring is the border of a hole within
the input polygon.

+   If the input geography is a polygon, excludes the outermost ring of the
    polygon geography and returns the linestrings corresponding to the interior
    rings.
+   If the input is the full `GEOGRAPHY`, returns an empty array.
+   If the input polygon has no holes, returns an empty array.
+   Returns an error if the input is not a single polygon.

Use the `SAFE` prefix to return `NULL` for invalid input instead of an error.

**Return type**

`ARRAY<LineString GEOGRAPHY>`

**Examples**

```sql
WITH geo AS (
  SELECT ST_GEOGFROMTEXT('POLYGON((0 0, 1 1, 1 2, 0 0))') AS g UNION ALL
  SELECT ST_GEOGFROMTEXT('POLYGON((1 1, 1 10, 5 10, 5 1, 1 1), (2 2, 3 4, 2 4, 2 2))') UNION ALL
  SELECT ST_GEOGFROMTEXT('POLYGON((1 1, 1 10, 5 10, 5 1, 1 1), (2 2.5, 3.5 3, 2.5 2, 2 2.5), (3.5 7, 4 6, 3 3, 3.5 7))') UNION ALL
  SELECT ST_GEOGFROMTEXT('fullglobe') UNION ALL
  SELECT NULL)
SELECT ST_INTERIORRINGS(g) AS rings FROM geo;

/*----------------------------------------------------------------------------*
 | rings                                                                      |
 +----------------------------------------------------------------------------+
 | []                                                                         |
 | [LINESTRING(2 2, 3 4, 2 4, 2 2)]                                           |
 | [LINESTRING(2.5 2, 3.5 3, 2 2.5, 2.5 2), LINESTRING(3 3, 4 6, 3.5 7, 3 3)] |
 | []                                                                         |
 | NULL                                                                       |
 *----------------------------------------------------------------------------*/
```

### `ST_INTERSECTION`

```sql
ST_INTERSECTION(geography_1, geography_2)
```

**Description**

Returns a `GEOGRAPHY` that represents the point set
intersection of the two input `GEOGRAPHY`s. Thus,
every point in the intersection appears in both `geography_1` and `geography_2`.

If the two input `GEOGRAPHY`s are disjoint, that is,
there are no points that appear in both input `geometry_1` and `geometry_2`,
then an empty `GEOGRAPHY` is returned.

See [ST_INTERSECTS][st-intersects], [ST_DISJOINT][st-disjoint] for related
predicate functions.

**Return type**

`GEOGRAPHY`

[st-intersects]: #st_intersects

[st-disjoint]: #st_disjoint

### `ST_INTERSECTS`

```sql
ST_INTERSECTS(geography_1, geography_2)
```

**Description**

Returns `TRUE` if the point set intersection of `geography_1` and `geography_2`
is non-empty. Thus, this function returns `TRUE` if there is at least one point
that appears in both input `GEOGRAPHY`s.

If `ST_INTERSECTS` returns `TRUE`, it implies that [`ST_DISJOINT`][st-disjoint]
returns `FALSE`.

**Return type**

`BOOL`

[st-disjoint]: #st_disjoint

### `ST_INTERSECTSBOX`

```sql
ST_INTERSECTSBOX(geography, lng1, lat1, lng2, lat2)
```

**Description**

Returns `TRUE` if `geography` intersects the rectangle between `[lng1, lng2]`
and `[lat1, lat2]`. The edges of the rectangle follow constant lines of
longitude and latitude. `lng1` and `lng2` specify the westmost and eastmost
constant longitude lines that bound the rectangle, and `lat1` and `lat2` specify
the minimum and maximum constant latitude lines that bound the rectangle.

Specify all longitude and latitude arguments in degrees.

**Constraints**

The input arguments are subject to the following constraints:

+   Latitudes should be in the `[-90, 90]` degree range.
+   Longitudes should follow either of the following rules:
    +   Both longitudes are in the `[-180, 180]` degree range.
    +   One of the longitudes is in the `[-180, 180]` degree range, and
        `lng2 - lng1` is in the `[0, 360]` interval.

**Return type**

`BOOL`

**Example**

```sql
SELECT p, ST_INTERSECTSBOX(p, -90, 0, 90, 20) AS box1,
       ST_INTERSECTSBOX(p, 90, 0, -90, 20) AS box2
FROM UNNEST([ST_GEOGPOINT(10, 10), ST_GEOGPOINT(170, 10),
             ST_GEOGPOINT(30, 30)]) p

/*----------------+--------------+--------------*
 | p              | box1         | box2         |
 +----------------+--------------+--------------+
 | POINT(10 10)   | TRUE         | FALSE        |
 | POINT(170 10)  | FALSE        | TRUE         |
 | POINT(30 30)   | FALSE        | FALSE        |
 *----------------+--------------+--------------*/
```

### `ST_ISCLOSED`

```sql
ST_ISCLOSED(geography_expression)
```

**Description**

Returns `TRUE` for a non-empty Geography, where each element in the Geography
has an empty boundary. The boundary for each element can be defined with
[`ST_BOUNDARY`][st-boundary].

+   A point is closed.
+   A linestring is closed if the start and end points of the linestring are
    the same.
+   A polygon is closed only if it is a full polygon.
+   A collection is closed if and only if every element in the collection is
    closed.

An empty `GEOGRAPHY` is not closed.

**Return type**

`BOOL`

**Example**

```sql
WITH example AS(
  SELECT ST_GEOGFROMTEXT('POINT(5 0)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('LINESTRING(0 1, 4 3, 2 6, 0 1)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('LINESTRING(2 6, 1 3, 3 9)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))
SELECT
  geography,
  ST_ISCLOSED(geography) AS is_closed,
FROM example;

/*------------------------------------------------------+-----------*
 | geography                                            | is_closed |
 +------------------------------------------------------+-----------+
 | POINT(5 0)                                           | TRUE      |
 | LINESTRING(0 1, 4 3, 2 6, 0 1)                       | TRUE      |
 | LINESTRING(2 6, 1 3, 3 9)                            | FALSE     |
 | GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1)) | FALSE     |
 | GEOMETRYCOLLECTION EMPTY                             | FALSE     |
 *------------------------------------------------------+-----------*/
```

[st-boundary]: #st_boundary

### `ST_ISCOLLECTION`

```sql
ST_ISCOLLECTION(geography_expression)
```

**Description**

Returns `TRUE` if the total number of points, linestrings, and polygons is
greater than one.

An empty `GEOGRAPHY` is not a collection.

**Return type**

`BOOL`

### `ST_ISEMPTY`

```sql
ST_ISEMPTY(geography_expression)
```

**Description**

Returns `TRUE` if the given `GEOGRAPHY` is empty; that is, the `GEOGRAPHY` does
not contain any points, lines, or polygons.

NOTE: An empty `GEOGRAPHY` is not associated with a particular geometry shape.
For example, the results of expressions `ST_GEOGFROMTEXT('POINT EMPTY')` and
`ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY')` are identical.

**Return type**

`BOOL`

### `ST_ISRING`

```sql
ST_ISRING(geography_expression)
```

**Description**

Returns `TRUE` if the input `GEOGRAPHY` is a linestring and if the
linestring is both [`ST_ISCLOSED`][st-isclosed] and
simple. A linestring is considered simple if it does not pass through the
same point twice (with the exception of the start and endpoint, which may
overlap to form a ring).

An empty `GEOGRAPHY` is not a ring.

**Return type**

`BOOL`

[st-isclosed]: #st_isclosed

### `ST_LENGTH`

```sql
ST_LENGTH(geography_expression[, use_spheroid])
```

**Description**

Returns the total length in meters of the lines in the input
`GEOGRAPHY`.

If `geography_expression` is a point or a polygon, returns zero. If
`geography_expression` is a collection, returns the length of the lines in the
collection; if the collection does not contain lines, returns zero.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

`DOUBLE`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_LINELOCATEPOINT`

```sql
ST_LINELOCATEPOINT(linestring_geography, point_geography)
```

**Description**

Gets a section of a linestring between the start point and a selected point (a
point on the linestring closest to the `point_geography` argument). Returns the
percentage that this section represents in the linestring.

Details:

+   To select a point on the linestring `GEOGRAPHY` (`linestring_geography`),
    this function takes a point `GEOGRAPHY` (`point_geography`) and finds the
    [closest point][st-closestpoint] to it on the linestring.
+   If two points on `linestring_geography` are an equal distance away from
    `point_geography`, it is not guaranteed which one will be selected.
+   The return value is an inclusive value between 0 and 1 (0-100%).
+   If the selected point is the start point on the linestring, function returns
    0 (0%).
+   If the selected point is the end point on the linestring, function returns 1
    (100%).

`NULL` and error handling:

+   Returns `NULL` if any input argument is `NULL`.
+   Returns an error if `linestring_geography` is not a linestring or if
    `point_geography` is not a point. Use the `SAFE` prefix
    to obtain `NULL` for invalid input instead of an error.

**Return Type**

`DOUBLE`

**Examples**

```sql
WITH geos AS (
    SELECT ST_GEOGPOINT(0, 0) AS point UNION ALL
    SELECT ST_GEOGPOINT(1, 0) UNION ALL
    SELECT ST_GEOGPOINT(1, 1) UNION ALL
    SELECT ST_GEOGPOINT(2, 2) UNION ALL
    SELECT ST_GEOGPOINT(3, 3) UNION ALL
    SELECT ST_GEOGPOINT(4, 4) UNION ALL
    SELECT ST_GEOGPOINT(5, 5) UNION ALL
    SELECT ST_GEOGPOINT(6, 5) UNION ALL
    SELECT NULL
  )
SELECT
  point AS input_point,
  ST_LINELOCATEPOINT(ST_GEOGFROMTEXT('LINESTRING(1 1, 5 5)'), point)
    AS percentage_from_beginning
FROM geos

/*-------------+---------------------------*
 | input_point | percentage_from_beginning |
 +-------------+---------------------------+
 | POINT(0 0)  | 0                         |
 | POINT(1 0)  | 0                         |
 | POINT(1 1)  | 0                         |
 | POINT(2 2)  | 0.25015214685147907       |
 | POINT(3 3)  | 0.5002284283637185        |
 | POINT(4 4)  | 0.7501905913884388        |
 | POINT(5 5)  | 1                         |
 | POINT(6 5)  | 1                         |
 | NULL        | NULL                      |
 *-------------+---------------------------*/
```

[st-closestpoint]: #st_closestpoint

### `ST_MAKELINE`

```sql
ST_MAKELINE(geography_1, geography_2)
```

```sql
ST_MAKELINE(array_of_geography)
```

**Description**

Creates a `GEOGRAPHY` with a single linestring by
concatenating the point or line vertices of each of the input
`GEOGRAPHY`s in the order they are given.

`ST_MAKELINE` comes in two variants. For the first variant, input must be two
`GEOGRAPHY`s. For the second, input must be an `ARRAY` of type `GEOGRAPHY`. In
either variant, each input `GEOGRAPHY` must consist of one of the following
values:

+   Exactly one point.
+   Exactly one linestring.

For the first variant of `ST_MAKELINE`, if either input `GEOGRAPHY` is `NULL`,
`ST_MAKELINE` returns `NULL`. For the second variant, if input `ARRAY` or any
element in the input `ARRAY` is `NULL`, `ST_MAKELINE` returns `NULL`.

**Constraints**

Every edge must span strictly less than 180 degrees.

NOTE: The ZetaSQL snapping process may discard sufficiently short
edges and snap the two endpoints together. For instance, if two input
`GEOGRAPHY`s each contain a point and the two points are separated by a distance
less than the snap radius, the points will be snapped together. In such a case
the result will be a `GEOGRAPHY` with exactly one point.

**Return type**

LineString `GEOGRAPHY`

### `ST_MAKEPOLYGON`

```sql
ST_MAKEPOLYGON(geography_expression[, array_of_geography])
```

**Description**

Creates a `GEOGRAPHY` containing a single polygon
from linestring inputs, where each input linestring is used to construct a
polygon ring.

`ST_MAKEPOLYGON` comes in two variants. For the first variant, the input
linestring is provided by a single `GEOGRAPHY` containing exactly one
linestring. For the second variant, the input consists of a single `GEOGRAPHY`
and an array of `GEOGRAPHY`s, each containing exactly one linestring.

The first `GEOGRAPHY` in either variant is used to construct the polygon shell.
Additional `GEOGRAPHY`s provided in the input `ARRAY` specify a polygon hole.
For every input `GEOGRAPHY` containing exactly one linestring, the following
must be true:

+   The linestring must consist of at least three distinct vertices.
+   The linestring must be closed: that is, the first and last vertex have to be
    the same. If the first and last vertex differ, the function constructs a
    final edge from the first vertex to the last.

For the first variant of `ST_MAKEPOLYGON`, if either input `GEOGRAPHY` is
`NULL`, `ST_MAKEPOLYGON` returns `NULL`. For the second variant, if
input `ARRAY` or any element in the `ARRAY` is `NULL`, `ST_MAKEPOLYGON` returns
`NULL`.

NOTE: `ST_MAKEPOLYGON` accepts an empty `GEOGRAPHY` as input. `ST_MAKEPOLYGON`
interprets an empty `GEOGRAPHY` as having an empty linestring, which will
create a full loop: that is, a polygon that covers the entire Earth.

**Constraints**

Together, the input rings must form a valid polygon:

+   The polygon shell must cover each of the polygon holes.
+   There can be only one polygon shell (which has to be the first input ring).
    This implies that polygon holes cannot be nested.
+   Polygon rings may only intersect in a vertex on the boundary of both rings.

Every edge must span strictly less than 180 degrees.

Each polygon ring divides the sphere into two regions. The first input linesting
to `ST_MAKEPOLYGON` forms the polygon shell, and the interior is chosen to be
the smaller of the two regions. Each subsequent input linestring specifies a
polygon hole, so the interior of the polygon is already well-defined. In order
to define a polygon shell such that the interior of the polygon is the larger of
the two regions, see [`ST_MAKEPOLYGONORIENTED`][st-makepolygonoriented].

NOTE: The ZetaSQL snapping process may discard sufficiently
short edges and snap the two endpoints together. Hence, when vertices are
snapped together, it is possible that a polygon hole that is sufficiently small
may disappear, or the output `GEOGRAPHY` may contain only a line or a
point.

**Return type**

`GEOGRAPHY`

[st-makepolygonoriented]: #st_makepolygonoriented

### `ST_MAKEPOLYGONORIENTED`

```sql
ST_MAKEPOLYGONORIENTED(array_of_geography)
```

**Description**

Like `ST_MAKEPOLYGON`, but the vertex ordering of each input linestring
determines the orientation of each polygon ring. The orientation of a polygon
ring defines the interior of the polygon as follows: if someone walks along the
boundary of the polygon in the order of the input vertices, the interior of the
polygon is on the left. This applies for each polygon ring provided.

This variant of the polygon constructor is more flexible since
`ST_MAKEPOLYGONORIENTED` can construct a polygon such that the interior is on
either side of the polygon ring. However, proper orientation of polygon rings is
critical in order to construct the desired polygon.

If the input `ARRAY` or any element in the `ARRAY` is `NULL`,
`ST_MAKEPOLYGONORIENTED` returns `NULL`.

NOTE: The input argument for `ST_MAKEPOLYGONORIENTED` may contain an empty
`GEOGRAPHY`. `ST_MAKEPOLYGONORIENTED` interprets an empty `GEOGRAPHY` as having
an empty linestring, which will create a full loop: that is, a polygon that
covers the entire Earth.

**Constraints**

Together, the input rings must form a valid polygon:

+   The polygon shell must cover each of the polygon holes.
+   There must be only one polygon shell, which must to be the first input ring.
    This implies that polygon holes cannot be nested.
+   Polygon rings may only intersect in a vertex on the boundary of both rings.

Every edge must span strictly less than 180 degrees.

`ST_MAKEPOLYGONORIENTED` relies on the ordering of the input vertices of each
linestring to determine the orientation of the polygon. This applies to the
polygon shell and any polygon holes. `ST_MAKEPOLYGONORIENTED` expects all
polygon holes to have the opposite orientation of the shell. See
[`ST_MAKEPOLYGON`][st-makepolygon] for an alternate polygon constructor, and
other constraints on building a valid polygon.

NOTE: Due to the ZetaSQL snapping process, edges with a sufficiently
short length will be discarded and the two endpoints will be snapped to a single
point. Therefore, it is possible that vertices in a linestring may be snapped
together such that one or more edge disappears. Hence, it is possible that a
polygon hole that is sufficiently small may disappear, or the resulting
`GEOGRAPHY` may contain only a line or a point.

**Return type**

`GEOGRAPHY`

[st-makepolygon]: #st_makepolygon

### `ST_MAXDISTANCE`

```sql
ST_MAXDISTANCE(geography_1, geography_2[, use_spheroid])
```

Returns the longest distance in meters between two non-empty
`GEOGRAPHY`s; that is, the distance between two
vertices where the first vertex is in the first
`GEOGRAPHY`, and the second vertex is in the second
`GEOGRAPHY`. If `geography_1` and `geography_2` are the
same `GEOGRAPHY`, the function returns the distance
between the two most distant vertices in that
`GEOGRAPHY`.

If either of the input `GEOGRAPHY`s is empty,
`ST_MAXDISTANCE` returns `NULL`.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

`DOUBLE`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_NPOINTS`

```sql
ST_NPOINTS(geography_expression)
```

**Description**

An alias of [ST_NUMPOINTS][st-numpoints].

[st-numpoints]: #st_numpoints

### `ST_NUMGEOMETRIES`

```
ST_NUMGEOMETRIES(geography_expression)
```

**Description**

Returns the number of geometries in the input `GEOGRAPHY`. For a single point,
linestring, or polygon, `ST_NUMGEOMETRIES` returns `1`. For any collection of
geometries, `ST_NUMGEOMETRIES` returns the number of geometries making up the
collection. `ST_NUMGEOMETRIES` returns `0` if the input is the empty
`GEOGRAPHY`.

**Return type**

`INT64`

**Example**

The following example computes `ST_NUMGEOMETRIES` for a single point geography,
two collections, and an empty geography.

```sql
WITH example AS(
  SELECT ST_GEOGFROMTEXT('POINT(5 0)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('MULTIPOINT(0 1, 4 3, 2 6)') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))') AS geography
  UNION ALL
  SELECT ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))
SELECT
  geography,
  ST_NUMGEOMETRIES(geography) AS num_geometries,
FROM example;

/*------------------------------------------------------+----------------*
 | geography                                            | num_geometries |
 +------------------------------------------------------+----------------+
 | POINT(5 0)                                           | 1              |
 | MULTIPOINT(0 1, 4 3, 2 6)                            | 3              |
 | GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1)) | 2              |
 | GEOMETRYCOLLECTION EMPTY                             | 0              |
 *------------------------------------------------------+----------------*/
```

### `ST_NUMPOINTS`

```sql
ST_NUMPOINTS(geography_expression)
```

**Description**

Returns the number of vertices in the input
`GEOGRAPHY`. This includes the number of points, the
number of linestring vertices, and the number of polygon vertices.

NOTE: The first and last vertex of a polygon ring are counted as distinct
vertices.

**Return type**

`INT64`

### `ST_PERIMETER`

```sql
ST_PERIMETER(geography_expression[, use_spheroid])
```

**Description**

Returns the length in meters of the boundary of the polygons in the input
`GEOGRAPHY`.

If `geography_expression` is a point or a line, returns zero. If
`geography_expression` is a collection, returns the perimeter of the polygons
in the collection; if the collection does not contain polygons, returns zero.

The optional `use_spheroid` parameter determines how this function measures
distance. If `use_spheroid` is `FALSE`, the function measures distance on the
surface of a perfect sphere.

The `use_spheroid` parameter currently only supports
the value `FALSE`. The default value of `use_spheroid` is `FALSE`.

**Return type**

`DOUBLE`

[wgs84-link]: https://en.wikipedia.org/wiki/World_Geodetic_System

### `ST_POINTN`

```sql
ST_POINTN(linestring_geography, index)
```

**Description**

Returns the Nth point of a linestring geography as a point geography, where N is
the index. The index is 1-based. Negative values are counted backwards from the
end of the linestring, so that -1 is the last point. Returns an error if the
input is not a linestring, if the input is empty, or if there is no vertex at
the given index. Use the `SAFE` prefix to obtain `NULL` for invalid input
instead of an error.

**Return Type**

Point `GEOGRAPHY`

**Example**

The following example uses `ST_POINTN`, [`ST_STARTPOINT`][st-startpoint] and
[`ST_ENDPOINT`][st-endpoint] to extract points from a linestring.

```sql
WITH linestring AS (
    SELECT ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)') g
)
SELECT ST_POINTN(g, 1) AS first, ST_POINTN(g, -1) AS last,
    ST_POINTN(g, 2) AS second, ST_POINTN(g, -2) AS second_to_last
FROM linestring;

/*--------------+--------------+--------------+----------------*
 | first        | last         | second       | second_to_last |
 +--------------+--------------+--------------+----------------+
 | POINT(1 1)   | POINT(3 3)   | POINT(2 1)   | POINT(3 2)     |
 *--------------+--------------+--------------+----------------*/
```

[st-startpoint]: #st_startpoint

[st-endpoint]: #st_endpoint

### `ST_SIMPLIFY`

```sql
ST_SIMPLIFY(geography, tolerance_meters)
```

**Description**

Returns a simplified version of `geography`, the given input
`GEOGRAPHY`. The input `GEOGRAPHY` is simplified by replacing nearly straight
chains of short edges with a single long edge. The input `geography` will not
change by more than the tolerance specified by `tolerance_meters`. Thus,
simplified edges are guaranteed to pass within `tolerance_meters` of the
*original* positions of all vertices that were removed from that edge. The given
`tolerance_meters` is in meters on the surface of the Earth.

Note that `ST_SIMPLIFY` preserves topological relationships, which means that
no new crossing edges will be created and the output will be valid. For a large
enough tolerance, adjacent shapes may collapse into a single object, or a shape
could be simplified to a shape with a smaller dimension.

**Constraints**

For `ST_SIMPLIFY` to have any effect, `tolerance_meters` must be non-zero.

`ST_SIMPLIFY` returns an error if the tolerance specified by `tolerance_meters`
is one of the following:

+ A negative tolerance.
+ Greater than ~7800 kilometers.

**Return type**

`GEOGRAPHY`

**Examples**

The following example shows how `ST_SIMPLIFY` simplifies the input line
`GEOGRAPHY` by removing intermediate vertices.

```sql
WITH example AS
 (SELECT ST_GEOGFROMTEXT('LINESTRING(0 0, 0.05 0, 0.1 0, 0.15 0, 2 0)') AS line)
SELECT
   line AS original_line,
   ST_SIMPLIFY(line, 1) AS simplified_line
FROM example;

/*---------------------------------------------+----------------------*
 |                original_line                |   simplified_line    |
 +---------------------------------------------+----------------------+
 | LINESTRING(0 0, 0.05 0, 0.1 0, 0.15 0, 2 0) | LINESTRING(0 0, 2 0) |
 *---------------------------------------------+----------------------*/
```

The following example illustrates how the result of `ST_SIMPLIFY` can have a
lower dimension than the original shape.

```sql
WITH example AS
 (SELECT
    ST_GEOGFROMTEXT('POLYGON((0 0, 0.1 0, 0.1 0.1, 0 0))') AS polygon,
    t AS tolerance
  FROM UNNEST([1000, 10000, 100000]) AS t)
SELECT
  polygon AS original_triangle,
  tolerance AS tolerance_meters,
  ST_SIMPLIFY(polygon, tolerance) AS simplified_result
FROM example

/*-------------------------------------+------------------+-------------------------------------*
 |          original_triangle          | tolerance_meters |          simplified_result          |
 +-------------------------------------+------------------+-------------------------------------+
 | POLYGON((0 0, 0.1 0, 0.1 0.1, 0 0)) |             1000 | POLYGON((0 0, 0.1 0, 0.1 0.1, 0 0)) |
 | POLYGON((0 0, 0.1 0, 0.1 0.1, 0 0)) |            10000 |            LINESTRING(0 0, 0.1 0.1) |
 | POLYGON((0 0, 0.1 0, 0.1 0.1, 0 0)) |           100000 |                          POINT(0 0) |
 *-------------------------------------+------------------+-------------------------------------*/
```

### `ST_SNAPTOGRID`

```sql
ST_SNAPTOGRID(geography_expression, grid_size)
```

**Description**

Returns the input `GEOGRAPHY`, where each vertex has
been snapped to a longitude/latitude grid. The grid size is determined by the
`grid_size` parameter which is given in degrees.

**Constraints**

Arbitrary grid sizes are not supported. The `grid_size` parameter is rounded so
that it is of the form `10^n`, where `-10 < n < 0`.

**Return type**

`GEOGRAPHY`

### `ST_STARTPOINT`

```sql
ST_STARTPOINT(linestring_geography)
```

**Description**

Returns the first point of a linestring geography as a point geography. Returns
an error if the input is not a linestring or if the input is empty. Use the
`SAFE` prefix to obtain `NULL` for invalid input instead of an error.

**Return Type**

Point `GEOGRAPHY`

**Example**

```sql
SELECT ST_STARTPOINT(ST_GEOGFROMTEXT('LINESTRING(1 1, 2 1, 3 2, 3 3)')) first

/*--------------*
 | first        |
 +--------------+
 | POINT(1 1)   |
 *--------------*/
```

### `ST_TOUCHES`

```sql
ST_TOUCHES(geography_1, geography_2)
```

**Description**

Returns `TRUE` provided the following two conditions are satisfied:

1.  `geography_1` intersects `geography_2`.
1.  The interior of `geography_1` and the interior of `geography_2` are
    disjoint.

**Return type**

`BOOL`

### `ST_UNION_AGG`

```sql
ST_UNION_AGG(geography)
```

**Description**

Returns a `GEOGRAPHY` that represents the point set
union of all input `GEOGRAPHY`s.

`ST_UNION_AGG` ignores `NULL` input `GEOGRAPHY` values.

See [`ST_UNION`][st-union] for the non-aggregate version of `ST_UNION_AGG`.

**Return type**

`GEOGRAPHY`

[st-union]: #st_union

### `ST_UNION`

```sql
ST_UNION(geography_1, geography_2)
```

```sql
ST_UNION(array_of_geography)
```

**Description**

Returns a `GEOGRAPHY` that represents the point set
union of all input `GEOGRAPHY`s.

`ST_UNION` comes in two variants. For the first variant, input must be two
`GEOGRAPHY`s. For the second, the input is an
`ARRAY` of type `GEOGRAPHY`.

For the first variant of `ST_UNION`, if an input
`GEOGRAPHY` is `NULL`, `ST_UNION` returns `NULL`.
For the second variant, if the input `ARRAY` value
is `NULL`, `ST_UNION` returns `NULL`.
For a non-`NULL` input `ARRAY`, the union is computed
and `NULL` elements are ignored so that they do not affect the output.

See [`ST_UNION_AGG`][st-union-agg] for the aggregate version of `ST_UNION`.

**Return type**

`GEOGRAPHY`

[st-union-agg]: #st_union_agg

### `ST_WITHIN`

```sql
ST_WITHIN(geography_1, geography_2)
```

**Description**

Returns `TRUE` if no point of `geography_1` is outside of `geography_2` and
the interiors of `geography_1` and `geography_2` intersect.

Given two geographies `a` and `b`, `ST_WITHIN(a, b)` returns the same result
as [`ST_CONTAINS`][st-contains]`(b, a)`. Note the opposite order of arguments.

**Return type**

`BOOL`

[st-contains]: #st_contains

### `ST_X`

```sql
ST_X(geography_expression)
```

**Description**

Returns the longitude in degrees of the single-point input
`GEOGRAPHY`.

For any input `GEOGRAPHY` that is not a single point,
including an empty `GEOGRAPHY`, `ST_X` returns an
error. Use the `SAFE.` prefix to obtain `NULL`.

**Return type**

`DOUBLE`

**Example**

The following example uses `ST_X` and `ST_Y` to extract coordinates from
single-point geographies.

```sql
WITH points AS
   (SELECT ST_GEOGPOINT(i, i + 1) AS p FROM UNNEST([0, 5, 12]) AS i)
 SELECT
   p,
   ST_X(p) as longitude,
   ST_Y(p) as latitude
FROM points;

/*--------------+-----------+----------*
 | p            | longitude | latitude |
 +--------------+-----------+----------+
 | POINT(0 1)   | 0.0       | 1.0      |
 | POINT(5 6)   | 5.0       | 6.0      |
 | POINT(12 13) | 12.0      | 13.0     |
 *--------------+-----------+----------*/
```

### `ST_Y`

```sql
ST_Y(geography_expression)
```

**Description**

Returns the latitude in degrees of the single-point input
`GEOGRAPHY`.

For any input `GEOGRAPHY` that is not a single point,
including an empty `GEOGRAPHY`, `ST_Y` returns an
error. Use the `SAFE.` prefix to return `NULL` instead.

**Return type**

`DOUBLE`

**Example**

See [`ST_X`][st-x] for example usage.

[st-x]: #st_x

## Protocol buffer functions

ZetaSQL supports the following protocol buffer functions.

### `CONTAINS_KEY`

```sql
CONTAINS_KEY(proto_map_field_expression, key)
```

**Description**

Returns whether a [protocol buffer map field][proto-map] contains a given key.

Input values:

+ `proto_map_field_expression`: A protocol buffer map field.
+ `key`: A key in the protocol buffer map field.

`NULL` handling:

+ If `map_field` is `NULL`, returns `NULL`.
+ If `key` is `NULL`, returns `FALSE`.

**Return type**

`BOOL`

**Examples**

To illustrate the use of this function, consider the protocol buffer message
`Item`:

```proto
message Item {
  optional map<string, int64> purchased = 1;
};
```

In the following example, the function returns `TRUE` when the key is
present, `FALSE` otherwise.

```sql
SELECT
  CONTAINS_KEY(m.purchased, 'A') AS contains_a,
  CONTAINS_KEY(m.purchased, 'B') AS contains_b
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 }" AS Item)) AS m;

/*------------+------------*
 | contains_a | contains_b |
 +------------+------------+
 | TRUE       | FALSE      |
 *------------+------------*/
```

[proto-map]: https://developers.google.com/protocol-buffers/docs/proto3#maps

### `EXTRACT` 
<a id="proto_extract"></a>

```sql
EXTRACT( extraction_type (proto_field) FROM proto_expression )

extraction_type:
  { FIELD | RAW | HAS | ONEOF_CASE }
```

**Description**

Extracts a value from a protocol buffer. `proto_expression` represents the
expression that returns a protocol buffer, `proto_field` represents the field
of the protocol buffer to extract from, and `extraction_type` determines the
type of data to return. `EXTRACT` can be used to get values of ambiguous fields.
An alternative to `EXTRACT` is the [dot operator][querying-protocol-buffers].

**Extraction Types**

You can choose the type of information to get with `EXTRACT`. Your choices are:

+  `FIELD`: Extract a value from a protocol buffer field.
+  `RAW`: Extract an uninterpreted value from a
    protocol buffer field. Raw values
    ignore any ZetaSQL type annotations.
+  `HAS`: Returns `TRUE` if a protocol buffer field is set in a proto message;
   otherwise, `FALSE`. Returns an error if this is used with a scalar proto3
   field. Alternatively, use [`has_x`][has-value], to perform this task.
+  `ONEOF_CASE`: Returns the name of the set protocol buffer field in a Oneof.
   If no field is set, returns an empty string.

**Return Type**

The return type depends upon the extraction type in the query.

+  `FIELD`: Protocol buffer field type.
+  `RAW`: Protocol buffer field
    type. Format annotations are
    ignored.
+  `HAS`: `BOOL`
+  `ONEOF_CASE`: `STRING`

**Examples**

The examples in this section reference two protocol buffers called `Album` and
`Chart`, and one table called `AlbumList`.

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

```proto
message Chart {
  optional int64 date = 1 [(zetasql.format) = DATE];
  optional string chart_name = 2;
  optional int64 rank = 3;
}
```

```sql
WITH AlbumList AS (
  SELECT
    NEW Album(
      'Alana Yah' AS solo,
      'New Moon' AS album_name,
      ['Sandstorm','Wait'] AS song) AS album_col,
    NEW Chart(
      'Billboard' AS chart_name,
      '2016-04-23' AS date,
      1 AS rank) AS chart_col
    UNION ALL
  SELECT
    NEW Album(
      'The Roadlands' AS band,
      'Grit' AS album_name,
      ['The Way', 'Awake', 'Lost Things'] AS song) AS album_col,
    NEW Chart(
      'Billboard' AS chart_name,
      1 as rank) AS chart_col
)
SELECT * FROM AlbumList
```

The following example extracts the album names from a table called `AlbumList`
that contains a proto-typed column called `Album`.

```sql
SELECT EXTRACT(FIELD(album_name) FROM album_col) AS name_of_album
FROM AlbumList

/*------------------*
 | name_of_album    |
 +------------------+
 | New Moon         |
 | Grit             |
 *------------------*/
```

A table called `AlbumList` contains a proto-typed column called `Album`.
`Album` contains a field called `date`, which can store an integer. The
`date` field has an annotated format called `DATE` assigned to it, which means
that when you extract the value in this field, it returns a `DATE`, not an
`INT64`.

If you would like to return the value for `date` as an `INT64`, not
as a `DATE`, use the `RAW` extraction type in your query. For example:

```sql
SELECT
  EXTRACT(RAW(date) FROM chart_col) AS raw_date,
  EXTRACT(FIELD(date) FROM chart_col) AS formatted_date
FROM AlbumList

/*----------+----------------*
 | raw_date | formatted_date |
 +----------+----------------+
 | 16914    | 2016-04-23     |
 | 0        | 1970-01-01     |
 *----------+----------------*/
```

The following example checks to see if release dates exist in a table called
`AlbumList` that contains a protocol buffer called `Chart`.

```sql
SELECT EXTRACT(HAS(date) FROM chart_col) AS has_release_date
FROM AlbumList

/*------------------*
 | has_release_date |
 +------------------+
 | TRUE             |
 | FALSE            |
 *------------------*/
```

The following example extracts the group name that is assigned to an artist in
a table called `AlbumList`. The group name is set for exactly one
protocol buffer field inside of the `group_name` Oneof. The `group_name` Oneof
exists inside the `Chart` protocol buffer.

```sql
SELECT EXTRACT(ONEOF_CASE(group_name) FROM album_col) AS artist_type
FROM AlbumList;

/*-------------*
 | artist_type |
 +-------------+
 | solo        |
 | band        |
 *-------------*/
```

[querying-protocol-buffers]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#querying_protocol_buffers

[has-value]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#checking_if_a_field_has_a_value

### `FILTER_FIELDS`

```sql
FILTER_FIELDS(proto_expression, proto_field_list)

proto_field_list:
  {+|-}proto_field_path[, ...]
```

**Description**

Takes a protocol buffer and a list of its fields to include or exclude.
Returns a version of that protocol buffer with unwanted fields removed.
Returns `NULL` if the protocol buffer is `NULL`.

Input values:

+ `proto_expression`: The protocol buffer to filter.
+ `proto_field_list`: The fields to exclude or include in the resulting
  protocol buffer.
+ `+`: Include a protocol buffer field and its children in the results.
+ `-`: Exclude a protocol buffer field and its children in the results.
+ `proto_field_path`: The protocol buffer field to include or exclude.
  If the field represents an [extension][querying-proto-extensions], you can use
  syntax for that extension in the path.

Protocol buffer field expression behavior:

+ The first field in `proto_field_list` determines the default
  inclusion/exclusion. By default, when you include the first field, all other
  fields are excluded. Or by default, when you exclude the first field, all
  other fields are included.
+ A required field in the protocol buffer cannot be excluded explicitly or
  implicitly.
+ If a field is included, its child fields and descendants are implicitly
  included in the results.
+ If a field is excluded, its child fields and descendants are
  implicitly excluded in the results.
+ A child field must be listed after its parent field in the argument list,
  but does not need to come right after the parent field.

Caveats:

+ If you attempt to exclude/include a field that already has been
  implicitly excluded/included, an error is produced.
+ If you attempt to explicitly include/exclude a field that has already
  implicitly been included/excluded, an error is produced.

**Return type**

Type of `proto_expression`

**Examples**

The examples in this section reference a protocol buffer called `Award` and
a table called `MusicAwards`.

```proto
message Award {
  required int32 year = 1;
  optional int32 month = 2;
  repeated Type type = 3;

  message Type {
    optional string award_name = 1;
    optional string category = 2;
  }
}
```

```sql
WITH
  MusicAwards AS (
    SELECT
      CAST(
        '''
        year: 2001
        month: 9
        type { award_name: 'Best Artist' category: 'Artist' }
        type { award_name: 'Best Album' category: 'Album' }
        '''
        AS zetasql.examples.music.Award) AS award_col
    UNION ALL
    SELECT
      CAST(
        '''
        year: 2001
        month: 12
        type { award_name: 'Best Song' category: 'Song' }
        '''
        AS zetasql.examples.music.Award) AS award_col
  )
SELECT *
FROM MusicAwards

/*---------------------------------------------------------*
 | award_col                                               |
 +---------------------------------------------------------+
 | {                                                       |
 |   year: 2001                                            |
 |   month: 9                                              |
 |   type { award_name: "Best Artist" category: "Artist" } |
 |   type { award_name: "Best Album" category: "Album" }   |
 | }                                                       |
 | {                                                       |
 |   year: 2001                                            |
 |   month: 12                                             |
 |   type { award_name: "Best Song" category: "Song" }     |
 | }                                                       |
 *---------------------------------------------------------*/
```

The following example returns protocol buffers that only include the `year`
field.

```sql
SELECT FILTER_FIELDS(award_col, +year) AS filtered_fields
FROM MusicAwards

/*-----------------*
 | filtered_fields |
 +-----------------+
 | {year: 2001}    |
 | {year: 2001}    |
 *-----------------*/
```

The following example returns protocol buffers that include all but the `type`
field.

```sql
SELECT FILTER_FIELDS(award_col, -type) AS filtered_fields
FROM MusicAwards

/*------------------------*
 | filtered_fields        |
 +------------------------+
 | {year: 2001 month: 9}  |
 | {year: 2001 month: 12} |
 *------------------------*/
```

The following example returns protocol buffers that only include the `year` and
`type.award_name` fields.

```sql
SELECT FILTER_FIELDS(award_col, +year, +type.award_name) AS filtered_fields
FROM MusicAwards

/*--------------------------------------*
 | filtered_fields                      |
 +--------------------------------------+
 | {                                    |
 |   year: 2001                         |
 |   type { award_name: "Best Artist" } |
 |   type { award_name: "Best Album" }  |
 | }                                    |
 | {                                    |
 |   year: 2001                         |
 |   type { award_name: "Best Song" }   |
 | }                                    |
 *--------------------------------------*/
```

The following example returns the `year` and `type` fields, but excludes the
`award_name` field in the `type` field.

```sql
SELECT FILTER_FIELDS(award_col, +year, +type, -type.award_name) AS filtered_fields
FROM MusicAwards

/*---------------------------------*
 | filtered_fields                 |
 +---------------------------------+
 | {                               |
 |   year: 2001                    |
 |   type { category: "Artist" }   |
 |   type { category: "Album" }    |
 | }                               |
 | {                               |
 |   year: 2001                    |
 |   type { category: "Song" }     |
 | }                               |
 *---------------------------------*/
```

The following example produces an error because `year` is a required field
and cannot be excluded explicitly or implicitly from the results.

```sql
SELECT FILTER_FIELDS(award_col, -year) AS filtered_fields
FROM MusicAwards

-- Error
```

The following example produces an error because when `year` was included,
`month` was implicitly excluded. You cannot explicitly exclude a field that
has already been implicitly excluded.

```sql
SELECT FILTER_FIELDS(award_col, +year, -month) AS filtered_fields
FROM MusicAwards

-- Error
```

[querying-proto-extensions]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#extensions

### `FROM_PROTO`

```sql
FROM_PROTO(expression)
```

**Description**

Returns a ZetaSQL value. The valid `expression` types are defined
in the table below, along with the return types that they produce.
Other input `expression` types are invalid. If `expression` cannot be converted
to a valid value, an error is returned.

<table width="100%">
  <thead>
    <tr>
      <th width="50%"><code>expression</code> type</th>
      <th width="50%">Return type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <ul>
        <li>INT32</li>
        <li>google.protobuf.Int32Value</li>
        </ul>
      </td>
      <td>INT32</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT32</li>
        <li>google.protobuf.UInt32Value</li>
        </ul>
      </td>
      <td>UINT32</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>INT64</li>
        <li>google.protobuf.Int64Value</li>
        </ul>
      </td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT64</li>
        <li>google.protobuf.UInt64Value</li>
        </ul>
      </td>
      <td>UINT64</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>FLOAT</li>
        <li>google.protobuf.FloatValue</li>
        </ul>
      </td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DOUBLE</li>
        <li>google.protobuf.DoubleValue</li>
        </ul>
      </td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BOOL</li>
        <li>google.protobuf.BoolValue</li>
        </ul>
      </td>
      <td>BOOL</td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>STRING</li>
          <li>
            google.protobuf.StringValue
            <p>
            Note: The <code>StringValue</code>
            value field must be
            UTF-8 encoded.
            </p>
          </li>
        </ul>
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BYTES</li>
        <li>google.protobuf.BytesValue</li>
        </ul>
      </td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DATE</li>
        <li>google.type.Date</li>
        </ul>
      </td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIME</li>
        <li>
          google.type.TimeOfDay

          

          

        </li>
        </ul>
      </td>
      <td>TIME</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIMESTAMP</li>
        <li>
          google.protobuf.Timestamp

          

          

        </li>
        </ul>
      </td>
      <td>TIMESTAMP</td>
    </tr>
  </tbody>
</table>

**Return Type**

The return type depends upon the `expression` type. See the return types
in the table above.

**Examples**

Convert a `google.type.Date` type into a `DATE` type.

```sql
SELECT FROM_PROTO(
  new google.type.Date(
    2019 as year,
    10 as month,
    30 as day
  )
)

/*------------*
 | $col1      |
 +------------+
 | 2019-10-30 |
 *------------*/
```

Pass in and return a `DATE` type.

```sql
SELECT FROM_PROTO(DATE '2019-10-30')

/*------------*
 | $col1      |
 +------------+
 | 2019-10-30 |
 *------------*/
```

### `MODIFY_MAP`

```sql
MODIFY_MAP(proto_map_field_expression, key_value_pair[, ...])

key_value_pair:
  key, value
```

**Description**

Modifies a [protocol buffer map field][proto-map] and returns the modified map
field.

Input values:

+ `proto_map_field_expression`: A protocol buffer map field.
+ `key_value_pair`: A key-value pair in the protocol buffer map field.

Modification behavior:

+ If the key is not already in the map field, adds the key and its value to the
  map field.
+ If the key is already in the map field, replaces its value.
+ If the key is in the map field and the value is `NULL`, removes the key and
  its value from the map field.

`NULL` handling:

+ If `key` is `NULL`, produces an error.
+ If the same `key` appears more than once, produces an error.
+ If `map` is `NULL`, `map` is treated as empty.

**Return type**

In the input protocol buffer map field, `V` as represented in `map<K,V>`.

**Examples**

To illustrate the use of this function, consider the protocol buffer message
`Item`:

```proto
message Item {
  optional map<string, int64> purchased = 1;
};
```

In the following example, the query deletes key `A`, replaces `B`, and adds
`C` in a map field called `purchased`.

```sql
SELECT
  MODIFY_MAP(m.purchased, 'A', NULL, 'B', 4, 'C', 6) AS result_map
FROM
  (SELECT AS VALUE CAST("purchased { key: 'A' value: 2 } purchased { key: 'B' value: 3}" AS Item)) AS m;

/*---------------------------------------------*
 | result_map                                  |
 +---------------------------------------------+
 | { key: 'B' value: 4 } { key: 'C' value: 6 } |
 *---------------------------------------------*/
```

[proto-map]: https://developers.google.com/protocol-buffers/docs/proto3#maps

### `PROTO_DEFAULT_IF_NULL`

```sql
PROTO_DEFAULT_IF_NULL(proto_field_expression)
```

**Description**

Evaluates any expression that results in a proto field access.
If the `proto_field_expression` evaluates to `NULL`, returns the default
value for the field. Otherwise, returns the field value.

Stipulations:

+ The expression cannot resolve to a required field.
+ The expression cannot resolve to a message field.
+ The expression must resolve to a regular proto field access, not
  a virtual field.
+ The expression cannot access a field with
  `zetasql.use_defaults=false`.

**Return Type**

Type of `proto_field_expression`.

**Example**

In the following example, each book in a library has a country of origin. If
the country is not set, the country defaults to unknown.

In this statement, table `library_books` contains a column named `book`,
whose type is `Book`.

```sql
SELECT PROTO_DEFAULT_IF_NULL(book.country) AS origin FROM library_books;
```

`Book` is a type that contains a field called `country`.

```proto
message Book {
  optional string country = 4 [default = 'Unknown'];
}
```

This is the result if `book.country` evaluates to `Canada`.

```sql
/*-----------------*
 | origin          |
 +-----------------+
 | Canada          |
 *-----------------*/
```

This is the result if `book` is `NULL`. Since `book` is `NULL`,
`book.country` evaluates to `NULL` and therefore the function result is the
default value for `country`.

```sql
/*-----------------*
 | origin          |
 +-----------------+
 | Unknown         |
 *-----------------*/
```

### `REPLACE_FIELDS`

```sql
REPLACE_FIELDS(proto_expression, value AS field_path [, ... ])
```

**Description**

Returns a copy of a protocol buffer, replacing the values in one or more fields.
`field_path` is a delimited path to the protocol buffer field to be replaced.

+ If `value` is `NULL`, it un-sets `field_path` or returns an error if the last
  component of `field_path` is a required field.
+ Replacing subfields will succeed only if the message containing the field is
  set.
+ Replacing subfields of repeated field is not allowed.
+ A repeated field can be replaced with an `ARRAY` value.

**Return type**

Type of `proto_expression`

**Examples**

To illustrate the usage of this function, we use protocol buffer messages
`Book` and `BookDetails`.

```
message Book {
  required string title = 1;
  repeated string reviews = 2;
  optional BookDetails details = 3;
};

message BookDetails {
  optional string author = 1;
  optional int32 chapters = 2;
};
```

This statement replaces value of field `title` and subfield `chapters`
of proto type `Book`. Note that field `details` must be set for the statement
to succeed.

```sql
SELECT REPLACE_FIELDS(
  NEW Book(
    "The Hummingbird" AS title,
    NEW BookDetails(10 AS chapters) AS details),
  "The Hummingbird II" AS title,
  11 AS details.chapters)
AS proto;

/*-----------------------------------------------------------------------------*
 | proto                                                                       |
 +-----------------------------------------------------------------------------+
 |{title: "The Hummingbird II" details: {chapters: 11 }}                       |
 *-----------------------------------------------------------------------------*/
```

The function can replace value of repeated fields.

```sql
SELECT REPLACE_FIELDS(
  NEW Book("The Hummingbird" AS title,
    NEW BookDetails(10 AS chapters) AS details),
  ["A good read!", "Highly recommended."] AS reviews)
AS proto;

/*-----------------------------------------------------------------------------*
 | proto                                                                       |
 +-----------------------------------------------------------------------------+
 |{title: "The Hummingbird" review: "A good read" review: "Highly recommended."|
 | details: {chapters: 10 }}                                                   |
 *-----------------------------------------------------------------------------*/
```

It can set a field to `NULL`.

```sql
SELECT REPLACE_FIELDS(
  NEW Book("The Hummingbird" AS title,
    NEW BookDetails(10 AS chapters) AS details),
  NULL AS details)
AS proto;

/*-----------------------------------------------------------------------------*
 | proto                                                                       |
 +-----------------------------------------------------------------------------+
 |{title: "The Hummingbird" }                                                  |
 *-----------------------------------------------------------------------------*/
```

### `TO_PROTO`

```
TO_PROTO(expression)
```

**Description**

Returns a PROTO value. The valid `expression` types are defined in the
table below, along with the return types that they produce. Other input
`expression` types are invalid.

<table width="100%">
  <thead>
    <tr>
      <th width="50%"><code>expression</code> type</th>
      <th width="50%">Return type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <ul>
        <li>INT32</li>
        <li>google.protobuf.Int32Value</li>
        </ul>
      </td>
      <td>google.protobuf.Int32Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT32</li>
        <li>google.protobuf.UInt32Value</li>
        </ul>
      </td>
      <td>google.protobuf.UInt32Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>INT64</li>
        <li>google.protobuf.Int64Value</li>
        </ul>
      </td>
      <td>google.protobuf.Int64Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT64</li>
        <li>google.protobuf.UInt64Value</li>
        </ul>
      </td>
      <td>google.protobuf.UInt64Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>FLOAT</li>
        <li>google.protobuf.FloatValue</li>
        </ul>
      </td>
      <td>google.protobuf.FloatValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DOUBLE</li>
        <li>google.protobuf.DoubleValue</li>
        </ul>
      </td>
      <td>google.protobuf.DoubleValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BOOL</li>
        <li>google.protobuf.BoolValue</li>
        </ul>
      </td>
      <td>google.protobuf.BoolValue</td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>STRING</li>
          <li>google.protobuf.StringValue</li>
        </ul>
      </td>
      <td>google.protobuf.StringValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BYTES</li>
        <li>google.protobuf.BytesValue</li>
        </ul>
      </td>
      <td>google.protobuf.BytesValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DATE</li>
        <li>google.type.Date</li>
        </ul>
      </td>
      <td>google.type.Date</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIME</li>
        <li>google.type.TimeOfDay</li>
        </ul>
      </td>
      <td>google.type.TimeOfDay</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIMESTAMP</li>
        <li>google.protobuf.Timestamp</li>
        </ul>
      </td>
      <td>google.protobuf.Timestamp</td>
    </tr>
  </tbody>
</table>

**Return Type**

The return type depends upon the `expression` type. See the return types
in the table above.

**Examples**

Convert a `DATE` type into a `google.type.Date` type.

```sql
SELECT TO_PROTO(DATE '2019-10-30')

/*--------------------------------*
 | $col1                          |
 +--------------------------------+
 | {year: 2019 month: 10 day: 30} |
 *--------------------------------*/
```

Pass in and return a `google.type.Date` type.

```sql
SELECT TO_PROTO(
  new google.type.Date(
    2019 as year,
    10 as month,
    30 as day
  )
)

/*--------------------------------*
 | $col1                          |
 +--------------------------------+
 | {year: 2019 month: 10 day: 30} |
 *--------------------------------*/
```

## Security functions

ZetaSQL supports the following security functions.

### `SESSION_USER`

```
SESSION_USER()
```

**Description**

For first-party users, returns the email address of the user that is running the
query.
For third-party users, returns the
[principal identifier](https://cloud.google.com/iam/docs/principal-identifiers)
of the user that is running the query.
For more information about identities, see
[Principals](https://cloud.google.com/docs/authentication#principal).

**Return Data Type**

`STRING`

**Example**

```sql
SELECT SESSION_USER() as user;

/*----------------------*
 | user                 |
 +----------------------+
 | jdoe@example.com     |
 *----------------------*/
```

## Net functions

ZetaSQL supports the following Net functions.

### `NET.FORMAT_IP` (DEPRECATED) 
<a id="net_format_ip_dep"></a>

```
NET.FORMAT_IP(integer)
```

**Description**

This function is deprecated. It is the same as
[`NET.IP_TO_STRING`][net-link-to-ip-to-string]`(`[`NET.IPV4_FROM_INT64`][net-link-to-ipv4-from-int64]`(integer))`,
except that this function does not allow negative input values.

**Return Data Type**

STRING

[net-link-to-ip-to-string]: #netip_to_string

[net-link-to-ipv4-from-int64]: #netipv4_from_int64

### `NET.FORMAT_PACKED_IP` (DEPRECATED) 
<a id="net_format_packed_ip_dep"></a>

```
NET.FORMAT_PACKED_IP(bytes_value)
```

**Description**

This function is deprecated. It is the same as [`NET.IP_TO_STRING`][net-link-to-ip-to-string].

**Return Data Type**

STRING

[net-link-to-ip-to-string]: #netip_to_string

### `NET.HOST`

```
NET.HOST(url)
```

**Description**

Takes a URL as a `STRING` value and returns the host. For best results, URL
values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply
with RFC 3986 formatting, this function makes a best effort to parse the input
and return a relevant result. If the function cannot parse the input, it
returns `NULL`.

Note: The function does not perform any normalization.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                               | description                                                                   | host               | suffix  | domain         |
|---------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                  | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                    | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                 | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                  | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                              | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"    |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;" | non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK"|
| "mailto:?to=&subject=&body="                                        | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

[net-link-to-rfc-3986-appendix-a]: https://tools.ietf.org/html/rfc3986#appendix-A

### `NET.IP_FROM_STRING`

```
NET.IP_FROM_STRING(addr_str)
```

**Description**

Converts an IPv4 or IPv6 address from text (STRING) format to binary (BYTES)
format in network byte order.

This function supports the following formats for `addr_str`:

+ IPv4: Dotted-quad format. For example, `10.1.2.3`.
+ IPv6: Colon-separated format. For example,
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. For more examples, see the
  [IP Version 6 Addressing Architecture][net-link-to-ipv6-rfc].

This function does not support [CIDR notation][net-link-to-cidr-notation], such as `10.1.2.3/32`.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  addr_str, FORMAT("%T", NET.IP_FROM_STRING(addr_str)) AS ip_from_string
FROM UNNEST([
  '48.49.50.51',
  '::1',
  '3031:3233:3435:3637:3839:4041:4243:4445',
  '::ffff:192.0.2.128'
]) AS addr_str;

/*---------------------------------------------------------------------------------------------------------------*
 | addr_str                                | ip_from_string                                                      |
 +---------------------------------------------------------------------------------------------------------------+
 | 48.49.50.51                             | b"0123"                                                             |
 | ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
 | 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
 | ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |
 *---------------------------------------------------------------------------------------------------------------*/
```

[net-link-to-ipv6-rfc]: http://www.ietf.org/rfc/rfc2373.txt

[net-link-to-cidr-notation]: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing

### `NET.IP_IN_NET`

```
NET.IP_IN_NET(address, subnet)
```

**Description**

Takes an IP address and a subnet CIDR as STRING and returns true if the IP
address is contained in the subnet.

This function supports the following formats for `address` and `subnet`:

+ IPv4: Dotted-quad format. For example, `10.1.2.3`.
+ IPv6: Colon-separated format. For example,
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. For more examples, see the
  [IP Version 6 Addressing Architecture][net-link-to-ipv6-rfc].
+ CIDR (IPv4): Dotted-quad format. For example, `10.1.2.0/24`
+ CIDR (IPv6): Colon-separated format. For example, `1:2::/48`.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

BOOL

[net-link-to-ipv6-rfc]: http://www.ietf.org/rfc/rfc2373.txt

### `NET.IP_NET_MASK`

```
NET.IP_NET_MASK(num_output_bytes, prefix_length)
```

**Description**

Returns a network mask: a byte sequence with length equal to `num_output_bytes`,
where the first `prefix_length` bits are set to 1 and the other bits are set to
0. `num_output_bytes` and `prefix_length` are INT64.
This function throws an error if `num_output_bytes` is not 4 (for IPv4) or 16
(for IPv6). It also throws an error if `prefix_length` is negative or greater
than `8 * num_output_bytes`.

**Return Data Type**

BYTES

**Example**

```sql
SELECT x, y, FORMAT("%T", NET.IP_NET_MASK(x, y)) AS ip_net_mask
FROM UNNEST([
  STRUCT(4 as x, 0 as y),
  (4, 20),
  (4, 32),
  (16, 0),
  (16, 1),
  (16, 128)
]);

/*--------------------------------------------------------------------------------*
 | x  | y   | ip_net_mask                                                         |
 +--------------------------------------------------------------------------------+
 | 4  | 0   | b"\x00\x00\x00\x00"                                                 |
 | 4  | 20  | b"\xff\xff\xf0\x00"                                                 |
 | 4  | 32  | b"\xff\xff\xff\xff"                                                 |
 | 16 | 0   | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
 | 16 | 1   | b"\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
 | 16 | 128 | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" |
 *--------------------------------------------------------------------------------*/
```

### `NET.IP_TO_STRING`

```
NET.IP_TO_STRING(addr_bin)
```

**Description**
Converts an IPv4 or IPv6 address from binary (BYTES) format in network byte
order to text (STRING) format.

If the input is 4 bytes, this function returns an IPv4 address as a STRING. If
the input is 16 bytes, it returns an IPv6 address as a STRING.

If this function receives a `NULL` input, it returns `NULL`. If the input has
a length different from 4 or 16, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

STRING

**Example**

```sql
SELECT FORMAT("%T", x) AS addr_bin, NET.IP_TO_STRING(x) AS ip_to_string
FROM UNNEST([
  b"0123",
  b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
  b"0123456789@ABCDE",
  b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80"
]) AS x;

/*---------------------------------------------------------------------------------------------------------------*
 | addr_bin                                                            | ip_to_string                            |
 +---------------------------------------------------------------------------------------------------------------+
 | b"0123"                                                             | 48.49.50.51                             |
 | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" | ::1                                     |
 | b"0123456789@ABCDE"                                                 | 3031:3233:3435:3637:3839:4041:4243:4445 |
 | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" | ::ffff:192.0.2.128                      |
 *---------------------------------------------------------------------------------------------------------------*/
```

### `NET.IP_TRUNC`

```
NET.IP_TRUNC(addr_bin, prefix_length)
```

**Description**
Takes `addr_bin`, an IPv4 or IPv6 address in binary (BYTES) format in network
byte order, and returns a subnet address in the same format. The result has the
same length as `addr_bin`, where the first `prefix_length` bits are equal to
those in `addr_bin` and the remaining bits are 0.

This function throws an error if `LENGTH(addr_bin)` is not 4 or 16, or if
`prefix_len` is negative or greater than `LENGTH(addr_bin) * 8`.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  FORMAT("%T", x) as addr_bin, prefix_length,
  FORMAT("%T", NET.IP_TRUNC(x, prefix_length)) AS ip_trunc
FROM UNNEST([
  STRUCT(b"\xAA\xBB\xCC\xDD" as x, 0 as prefix_length),
  (b"\xAA\xBB\xCC\xDD", 11), (b"\xAA\xBB\xCC\xDD", 12),
  (b"\xAA\xBB\xCC\xDD", 24), (b"\xAA\xBB\xCC\xDD", 32),
  (b'0123456789@ABCDE', 80)
]);

/*-----------------------------------------------------------------------------*
 | addr_bin            | prefix_length | ip_trunc                              |
 +-----------------------------------------------------------------------------+
 | b"\xaa\xbb\xcc\xdd" | 0             | b"\x00\x00\x00\x00"                   |
 | b"\xaa\xbb\xcc\xdd" | 11            | b"\xaa\xa0\x00\x00"                   |
 | b"\xaa\xbb\xcc\xdd" | 12            | b"\xaa\xb0\x00\x00"                   |
 | b"\xaa\xbb\xcc\xdd" | 24            | b"\xaa\xbb\xcc\x00"                   |
 | b"\xaa\xbb\xcc\xdd" | 32            | b"\xaa\xbb\xcc\xdd"                   |
 | b"0123456789@ABCDE" | 80            | b"0123456789\x00\x00\x00\x00\x00\x00" |
 *-----------------------------------------------------------------------------*/
```

### `NET.IPV4_FROM_INT64`

```
NET.IPV4_FROM_INT64(integer_value)
```

**Description**

Converts an IPv4 address from integer format to binary (BYTES) format in network
byte order. In the integer input, the least significant bit of the IP address is
stored in the least significant bit of the integer, regardless of host or client
architecture. For example, `1` means `0.0.0.1`, and `0x1FF` means `0.0.1.255`.

This function checks that either all the most significant 32 bits are 0, or all
the most significant 33 bits are 1 (sign-extended from a 32-bit integer).
In other words, the input should be in the range `[-0x80000000, 0xFFFFFFFF]`;
otherwise, this function throws an error.

This function does not support IPv6.

**Return Data Type**

BYTES

**Example**

```sql
SELECT x, x_hex, FORMAT("%T", NET.IPV4_FROM_INT64(x)) AS ipv4_from_int64
FROM (
  SELECT CAST(x_hex AS INT64) x, x_hex
  FROM UNNEST(["0x0", "0xABCDEF", "0xFFFFFFFF", "-0x1", "-0x2"]) AS x_hex
);

/*-----------------------------------------------*
 | x          | x_hex      | ipv4_from_int64     |
 +-----------------------------------------------+
 | 0          | 0x0        | b"\x00\x00\x00\x00" |
 | 11259375   | 0xABCDEF   | b"\x00\xab\xcd\xef" |
 | 4294967295 | 0xFFFFFFFF | b"\xff\xff\xff\xff" |
 | -1         | -0x1       | b"\xff\xff\xff\xff" |
 | -2         | -0x2       | b"\xff\xff\xff\xfe" |
 *-----------------------------------------------*/
```

### `NET.IPV4_TO_INT64`

```
NET.IPV4_TO_INT64(addr_bin)
```

**Description**

Converts an IPv4 address from binary (BYTES) format in network byte order to
integer format. In the integer output, the least significant bit of the IP
address is stored in the least significant bit of the integer, regardless of
host or client architecture. For example, `1` means `0.0.0.1`, and `0x1FF` means
`0.0.1.255`. The output is in the range `[0, 0xFFFFFFFF]`.

If the input length is not 4, this function throws an error.

This function does not support IPv6.

**Return Data Type**

INT64

**Example**

```sql
SELECT
  FORMAT("%T", x) AS addr_bin,
  FORMAT("0x%X", NET.IPV4_TO_INT64(x)) AS ipv4_to_int64
FROM
UNNEST([b"\x00\x00\x00\x00", b"\x00\xab\xcd\xef", b"\xff\xff\xff\xff"]) AS x;

/*-------------------------------------*
 | addr_bin            | ipv4_to_int64 |
 +-------------------------------------+
 | b"\x00\x00\x00\x00" | 0x0           |
 | b"\x00\xab\xcd\xef" | 0xABCDEF      |
 | b"\xff\xff\xff\xff" | 0xFFFFFFFF    |
 *-------------------------------------*/
```

### `NET.MAKE_NET`

```
NET.MAKE_NET(address, prefix_length)
```

**Description**

Takes an IPv4 or IPv6 address as STRING and an integer representing the prefix
length (the number of leading 1-bits in the network mask). Returns a
STRING representing the [CIDR subnet][net-link-to-cidr-notation] with the given prefix length.

The value of `prefix_length` must be greater than or equal to 0. A smaller value
means a bigger subnet, covering more IP addresses. The result CIDR subnet must
be no smaller than `address`, meaning that the value of `prefix_length` must be
less than or equal to the prefix length in `address`. See the effective upper
bound below.

This function supports the following formats for `address`:

+ IPv4: Dotted-quad format, such as `10.1.2.3`. The value of `prefix_length`
  must be less than or equal to 32.
+ IPv6: Colon-separated format, such as
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. The value of `prefix_length` must
  be less than or equal to 128.
+ CIDR (IPv4): Dotted-quad format, such as `10.1.2.0/24`.
  The value of `prefix_length` must be less than or equal to the number after
  the slash in `address` (24 in the example), which must be less than or equal
  to 32.
+ CIDR (IPv6): Colon-separated format, such as `1:2::/48`.
  The value of `prefix_length` must be less than or equal to the number after
  the slash in `address` (48 in the example), which must be less than or equal
  to 128.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

STRING

[net-link-to-cidr-notation]: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing

### `NET.PARSE_IP` (DEPRECATED) 
<a id="net_parse_ip_dep"></a>

```
NET.PARSE_IP(addr_str)
```

**Description**

This function is deprecated. It is the same as
[`NET.IPV4_TO_INT64`][net-link-to-ipv4-to-int64]`(`[`NET.IP_FROM_STRING`][net-link-to-ip-from-string]`(addr_str))`,
except that this function truncates the input at the first `'\x00'` character,
if any, while `NET.IP_FROM_STRING` treats `'\x00'` as invalid.

**Return Data Type**

INT64

[net-link-to-ip-to-string]: #netip_to_string

[net-link-to-ipv4-to-int64]: #netipv4_to_int64

### `NET.PARSE_PACKED_IP` (DEPRECATED) 
<a id="net_parse_packed_ip_dep"></a>

```
NET.PARSE_PACKED_IP(addr_str)
```

**Description**

This function is deprecated. It is the same as
[`NET.IP_FROM_STRING`][net-link-to-ip-from-string], except that this function truncates
the input at the first `'\x00'` character, if any, while `NET.IP_FROM_STRING`
treats `'\x00'` as invalid.

**Return Data Type**

BYTES

[net-link-to-ip-from-string]: #netip_from_string

### `NET.PUBLIC_SUFFIX`

```
NET.PUBLIC_SUFFIX(url)
```

**Description**

Takes a URL as a `STRING` value and returns the public suffix (such as `com`,
`org`, or `net`). A public suffix is an ICANN domain registered at
[publicsuffix.org][net-link-to-public-suffix]. For best results, URL values
should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply
with RFC 3986 formatting, this function makes a best effort to parse the input
and return a relevant result.

This function returns `NULL` if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle
  (not leading or trailing);
+ The parsed host does not contain any public suffix.

Before looking up the public suffix, this function temporarily normalizes the
host by converting uppercase English letters to lowercase and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode].
The function then returns the public suffix as part of the original host instead
of the normalized host.

Note: The function does not perform
[Unicode normalization][unicode-normalization].

Note: The public suffix data at
[publicsuffix.org][net-link-to-public-suffix] also contains
private domains. This function ignores the private domains.

Note: The public suffix data may change over time. Consequently, input that
produces a `NULL` result now may produce a non-`NULL` value in the future.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                              | description                                                                   | host               | suffix  | domain         |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                 | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                   | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                 | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                            | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"     |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;"| non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK |
| "mailto:?to=&subject=&body="                                       | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

[unicode-normalization]: https://en.wikipedia.org/wiki/Unicode_equivalence

[net-link-to-punycode]: https://en.wikipedia.org/wiki/Punycode

[net-link-to-public-suffix]: https://publicsuffix.org/list/

[net-link-to-rfc-3986-appendix-a]: https://tools.ietf.org/html/rfc3986#appendix-A

### `NET.REG_DOMAIN`

```
NET.REG_DOMAIN(url)
```

**Description**

Takes a URL as a string and returns the registered or registrable domain (the
[public suffix](#netpublic_suffix) plus one preceding label), as a
string. For best results, URL values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply
with RFC 3986 formatting, this function makes a best effort to parse the input
and return a relevant result.

This function returns `NULL` if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle
  (not leading or trailing);
+ The parsed host does not contain any public suffix;
+ The parsed host contains only a public suffix without any preceding label.

Before looking up the public suffix, this function temporarily normalizes the
host by converting uppercase English letters to lowercase and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode]. The function then
returns the registered or registerable domain as part of the original host
instead of the normalized host.

Note: The function does not perform
[Unicode normalization][unicode-normalization].

Note: The public suffix data at
[publicsuffix.org][net-link-to-public-suffix] also contains
private domains. This function does not treat a private domain as a public
suffix. For example, if `us.com` is a private domain in the public suffix data,
`NET.REG_DOMAIN("foo.us.com")` returns `us.com` (the public suffix `com` plus
the preceding label `us`) rather than `foo.us.com` (the private domain `us.com`
plus the preceding label `foo`).

Note: The public suffix data may change over time.
Consequently, input that produces a `NULL` result now may produce a non-`NULL`
value in the future.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                              | description                                                                   | host               | suffix  | domain         |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                 | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                   | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                 | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                            | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"  |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;"| non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK"|
| "mailto:?to=&subject=&body="                                       | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

[unicode-normalization]: https://en.wikipedia.org/wiki/Unicode_equivalence

[net-link-to-public-suffix]: https://publicsuffix.org/list/

[net-link-to-punycode]: https://en.wikipedia.org/wiki/Punycode

[net-link-to-rfc-3986-appendix-a]: https://tools.ietf.org/html/rfc3986#appendix-A

### `NET.SAFE_IP_FROM_STRING`

```
NET.SAFE_IP_FROM_STRING(addr_str)
```

**Description**

Similar to [`NET.IP_FROM_STRING`][net-link-to-ip-from-string], but returns `NULL`
instead of throwing an error if the input is invalid.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  addr_str,
  FORMAT("%T", NET.SAFE_IP_FROM_STRING(addr_str)) AS safe_ip_from_string
FROM UNNEST([
  '48.49.50.51',
  '::1',
  '3031:3233:3435:3637:3839:4041:4243:4445',
  '::ffff:192.0.2.128',
  '48.49.50.51/32',
  '48.49.50',
  '::wxyz'
]) AS addr_str;

/*---------------------------------------------------------------------------------------------------------------*
 | addr_str                                | safe_ip_from_string                                                 |
 +---------------------------------------------------------------------------------------------------------------+
 | 48.49.50.51                             | b"0123"                                                             |
 | ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
 | 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
 | ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |
 | 48.49.50.51/32                          | NULL                                                                |
 | 48.49.50                                | NULL                                                                |
 | ::wxyz                                  | NULL                                                                |
 *---------------------------------------------------------------------------------------------------------------*/
```

[net-link-to-ip-from-string]: #netip_from_string

## Debugging functions

ZetaSQL supports the following debugging functions.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#error"><code>ERROR</code></a>

</td>
  <td>
    Produces an error with a custom error message.
  </td>
</tr>

<tr>
  <td><a href="#iferror"><code>IFERROR</code></a>

</td>
  <td>
    Evaluates a try expression, and if an evaluation error is produced, returns
    the result of a catch expression.
  </td>
</tr>

<tr>
  <td><a href="#iserror"><code>ISERROR</code></a>

</td>
  <td>
    Evaluates a try expression, and if an evaluation error is produced, returns
    <code>TRUE</code>.
  </td>
</tr>

<tr>
  <td><a href="#nulliferror"><code>NULLIFERROR</code></a>

</td>
  <td>
    Evaluates a try expression, and if an evaluation error is produced, returns
    <code>NULL</code>.
  </td>
</tr>

  </tbody>
</table>

### `ERROR`

```sql
ERROR(error_message)
```

**Description**

Returns an error. The `error_message` argument is a `STRING`.

ZetaSQL treats `ERROR` in the same way as any expression that may
result in an error: there is no special guarantee of evaluation order.

**Return Data Type**

ZetaSQL infers the return type in context.

**Examples**

In the following example, the query returns an error message if the value of the
row does not match one of two defined values.

```sql
SELECT
  CASE
    WHEN value = 'foo' THEN 'Value is foo.'
    WHEN value = 'bar' THEN 'Value is bar.'
    ELSE ERROR(CONCAT('Found unexpected value: ', value))
  END AS new_value
FROM (
  SELECT 'foo' AS value UNION ALL
  SELECT 'bar' AS value UNION ALL
  SELECT 'baz' AS value);

-- Found unexpected value: baz
```

In the following example, ZetaSQL may evaluate the `ERROR` function
before or after the <nobr>`x > 0`</nobr> condition, because ZetaSQL
generally provides no ordering guarantees between `WHERE` clause conditions and
there are no special guarantees for the `ERROR` function.

```sql
SELECT *
FROM (SELECT -1 AS x)
WHERE x > 0 AND ERROR('Example error');
```

In the next example, the `WHERE` clause evaluates an `IF` condition, which
ensures that ZetaSQL only evaluates the `ERROR` function if the
condition fails.

```sql
SELECT *
FROM (SELECT -1 AS x)
WHERE IF(x > 0, true, ERROR(FORMAT('Error: x must be positive but is %t', x)));

-- Error: x must be positive but is -1
```

### `IFERROR`

```sql
IFERROR(try_expression, catch_expression)
```

**Description**

Evaluates `try_expression`.

When `try_expression` is evaluated:

+ If the evaluation of `try_expression` does not produce an error, then
  `IFERROR` returns the result of `try_expression` without evaluating
  `catch_expression`.
+ If the evaluation of `try_expression` produces a system error, then `IFERROR`
  produces that system error.
+ If the evaluation of `try_expression` produces an evaluation error, then
  `IFERROR` suppresses that evaluation error and evaluates `catch_expression`.

If `catch_expression` is evaluated:

+ If the evaluation of `catch_expression` does not produce an error, then
  `IFERROR` returns the result of `catch_expression`.
+ If the evaluation of `catch_expression` produces any error, then `IFERROR`
  produces that error.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.
+ `catch_expression`: An expression that returns a scalar value.

The results of `try_expression` and `catch_expression` must share a
[supertype][supertype].

**Return Data Type**

The [supertype][supertype] for `try_expression` and
`catch_expression`.

**Example**

In the following examples, the query successfully evaluates `try_expression`.

```sql
SELECT IFERROR('a', 'b') AS result

/*--------*
 | result |
 +--------+
 | a      |
 *--------*/
```

```sql
SELECT IFERROR((SELECT [1,2,3][OFFSET(0)]), -1) AS result

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

In the following examples, `IFERROR` catches an evaluation error in the
`try_expression` and successfully evaluates `catch_expression`.

```sql
SELECT IFERROR(ERROR('a'), 'b') AS result

/*--------*
 | result |
 +--------+
 | b      |
 *--------*/
```

```sql
SELECT IFERROR((SELECT [1,2,3][OFFSET(9)]), -1) AS result

/*--------*
 | result |
 +--------+
 | -1     |
 *--------*/
```

In the following query, the error is handled by the innermost `IFERROR`
operation, `IFERROR(ERROR('a'), 'b')`.

```sql
SELECT IFERROR(IFERROR(ERROR('a'), 'b'), 'c') AS result

/*--------*
 | result |
 +--------+
 | b      |
 *--------*/
```

In the following query, the error is handled by the outermost `IFERROR`
operation, `IFERROR(..., 'c')`.

```sql
SELECT IFERROR(IFERROR(ERROR('a'), ERROR('b')), 'c') AS result

/*--------*
 | result |
 +--------+
 | c      |
 *--------*/
```

In the following example, an evaluation error is produced because the subquery
passed in as the `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT IFERROR((SELECT e FROM UNNEST([1, 2]) AS e), 3) AS result

/*--------*
 | result |
 +--------+
 | 3      |
 *--------*/
```

In the following example, `IFERROR` catches an evaluation error in `ERROR('a')`
and then evaluates `ERROR('b')`. Because there is also an evaluation error in
`ERROR('b')`, `IFERROR` produces an evaluation error for `ERROR('b')`.

```sql
SELECT IFERROR(ERROR('a'), ERROR('b')) AS result

--ERROR: OUT_OF_RANGE 'b'
```

[supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

### `ISERROR`

```sql
ISERROR(try_expression)
```

**Description**

Evaluates `try_expression`.

+ If the evaluation of `try_expression` does not produce an error, then
  `ISERROR` returns `FALSE`.
+ If the evaluation of `try_expression` produces a system error, then `ISERROR`
  produces that system error.
+ If the evaluation of `try_expression` produces an evaluation error, then
  `ISERROR` returns `TRUE`.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.

**Return Data Type**

`BOOL`

**Example**

In the following examples, `ISERROR` successfully evaluates `try_expression`.

```sql
SELECT ISERROR('a') AS is_error

/*----------*
 | is_error |
 +----------+
 | false    |
 *----------*/
```

```sql
SELECT ISERROR(2/1) AS is_error

/*----------*
 | is_error |
 +----------+
 | false    |
 *----------*/
```

```sql
SELECT ISERROR((SELECT [1,2,3][OFFSET(0)])) AS is_error

/*----------*
 | is_error |
 +----------+
 | false    |
 *----------*/
```

In the following examples, `ISERROR` catches an evaluation error in
`try_expression`.

```sql
SELECT ISERROR(ERROR('a')) AS is_error

/*----------*
 | is_error |
 +----------+
 | true     |
 *----------*/
```

```sql
SELECT ISERROR(2/0) AS is_error

/*----------*
 | is_error |
 +----------+
 | true     |
 *----------*/
```

```sql
SELECT ISERROR((SELECT [1,2,3][OFFSET(9)])) AS is_error

/*----------*
 | is_error |
 +----------+
 | true     |
 *----------*/
```

In the following example, an evaluation error is produced because the subquery
passed in as `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT ISERROR((SELECT e FROM UNNEST([1, 2]) AS e)) AS is_error

/*----------*
 | is_error |
 +----------+
 | true     |
 *----------*/
```

### `NULLIFERROR`

```sql
NULLIFERROR(try_expression)
```
**Description**

Evaluates `try_expression`.

+ If the evaluation of `try_expression` does not produce an error, then
  `NULLIFERROR` returns the result of `try_expression`.
+ If the evaluation of `try_expression` produces a system error, then
 `NULLIFERROR` produces that system error.

+ If the evaluation of `try_expression` produces an evaluation error, then
  `NULLIFERROR` returns `NULL`.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.

**Return Data Type**

The data type for `try_expression` or `NULL`

**Example**

In the following examples, `NULLIFERROR` successfully evaluates
`try_expression`.

```sql
SELECT NULLIFERROR('a') AS result

/*--------*
 | result |
 +--------+
 | a      |
 *--------*/
```

```sql
SELECT NULLIFERROR((SELECT [1,2,3][OFFSET(0)])) AS result

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

In the following examples, `NULLIFERROR` catches an evaluation error in
`try_expression`.

```sql
SELECT NULLIFERROR(ERROR('a')) AS result

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT NULLIFERROR((SELECT [1,2,3][OFFSET(9)])) AS result

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

In the following example, an evaluation error is produced because the subquery
passed in as `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT NULLIFERROR((SELECT e FROM UNNEST([1, 2]) AS e)) AS result

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md

[function-calls]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md

<!-- mdlint on -->

