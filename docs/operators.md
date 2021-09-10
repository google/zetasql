

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
      <td>.</td>
      <td><span> JSON</span><br><span> PROTO</span><br><span> STRUCT</span><br></td>
      <td>Field access operator</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>Array subscript operator</td>
      <td>ARRAY</td>
      <td>Array position. Must be used with OFFSET or ORDINAL&mdash;see
      

<a href="https://github.com/google/zetasql/blob/master/docs/array_functions.md#array_functions">

Array Functions
</a>
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
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;</td>
      <td>Any comparable type. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Less than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;</td>
      <td>Any comparable type. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Greater than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;=</td>
      <td>Any comparable type. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Less than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;=</td>
      <td>Any comparable type. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Greater than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>!=, &lt;&gt;</td>
      <td>Any comparable type. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

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
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

      for a complete list.</td>
      <td>Value is [not] within the range specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] IN</td>
      <td>Any comparable types. See
      

<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md#data_types">

Data Types
</a>

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

### Array subscript operator

```
array_expression [position_keyword (array_element_id)]
```

**Description**

Get a value in an array at a specific location.
Supported by some array functions.

**Input types**

+ `position_keyword`: `OFFSET` or `ORDINAL`.
  To learn more, see [OFFSET and ORDINAL][operators-link-to-array-offset]
+ `array_element_id`: An integer that represents an index in the array.

**Return type**

Type `T` stored at the index in an array.

### JSON subscript operator

```
json_expression[array_element_id]
```

```
json_expression[field_name]
```

**Description**

Gets a value of an array element or field in a JSON expression. Can be
used to access nested data. For example:

```sql
(JSON '["apple", "orange", "pear"]')[1] -- Returns JSON 'orange'
(JSON '{"apple": "10", "pear": "5"}')['pear'] -- Returns JSON '5'
(JSON '[ {"fruit": "apple"}, {"fruit": "pear"}]')[0]['fruit'] -- Returns JSON 'apple'
(JSON '[ {"fruit": "apple"}, {"fruit": "pear"}]')[1]['fruit'] -- Returns JSON 'pear'
```

**Input data types**

+ `JSON expression`: The `JSON` expression that contains an array element or
  field to return.
+ `[array_element_id]`: An `INT64` expression that represents a zero-based index
  in the array. If a negative value is entered, or the value is greater than
  or equal to the size of the array, or the JSON expression doesn't represent
  a JSON array, a SQL `NULL` is returned.
+ `[field_name]`: A `STRING` expression that represents the name of a field in
  JSON. If the field name is not found, or the JSON expression is not a
  JSON object, a SQL `NULL` is returned.

**Result data type**

`JSON`

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
and use [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic)
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
  SELECT 'Intend' UNION ALL
 )
SELECT * FROM Words;

+----------+
| value    |
+----------+
| Intend   |
| Secure   |
| Clarity  |
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

[semantic-rules-in]: #semantic_rules_in
[operators-link-to-filtering-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering-arrays
[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md
[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause
[operators-link-to-unnest]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unnest_operator
[operators-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#about-subqueries
[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-math-functions]: https://github.com/google/zetasql/blob/master/docs/mathematical_functions.md
[link-to-coercion]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#coercion
[operators-link-to-array-offset]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#offset-and-ordinal

