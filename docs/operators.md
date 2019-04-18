

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
<td><span> PROTO<span><br><span> STRUCT<span><br></td>
<td>Member field access operator</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>[ ]</td>
<td>ARRAY</td>
<td>Array position. Must be used with OFFSET or ORDINAL&mdash;see <a
href="#array_functions">ARRAY Functions</a>.</td>
<td>Binary</td>
</tr>
<tr>
<td>2</td>
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
<td>4</td>
<td>+</td>
<td>All numeric types</td>
<td>Addition</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>-</td>
<td>All numeric types</td>
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
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
<td>Equal</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>&lt;</td>
<td>Any comparable type. See
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
<td>Less than</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>&gt;</td>
<td>Any comparable type. See
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
<td>Greater than</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>&lt;=</td>
<td>Any comparable type. See
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
<td>Less than or equal to</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>&gt;=</td>
<td>Any comparable type. See
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
<td>Greater than or equal to</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>!=, &lt;&gt;</td>
<td>Any comparable type. See
<a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a> for
a complete list.</td>
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
<td>Any comparable types. See Data Types for list.</td>
<td>Value is [not] within the range specified</td>
<td>Binary</td>
</tr>
<tr>
<td>&nbsp;</td>
<td>[NOT] IN</td>
<td>Any comparable types. See Data Types for list.</td>
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

All comparison operators have the same priority and are grouped using left
associativity. However, comparison operators are not associative. As a result,
it is recommended that you use parentheses to improve readability and ensure
expressions are resolved as desired. For example:

`(x < y) IS FALSE`

is recommended over:

`x < y IS FALSE`

### Element access operators

<table>
<thead>
<tr>
<th>Operator</th>
<th>Syntax</th>
<th>Input Data Types</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>.</td>
<td>expression.fieldname1...</td>
<td><span> PROTO<span><br><span> STRUCT<span><br></td>
<td>Type T stored in fieldname1</td>
<td>Dot operator. Can be used to access nested fields,
e.g.expression.fieldname1.fieldname2...</td>
</tr>
<tr>
<td>[ ]</td>
<td>array_expression [position_keyword (int_expression ) ]</td>
<td>See ARRAY Functions.</td>
<td>Type T stored in ARRAY</td>
<td>position_keyword is either OFFSET or ORDINAL. See <a
href="#array_functions">ARRAY Functions</a> for the
two functions that use this operator.</td>
</tr>
</tbody>
</table>

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
<td>Unary Minus</td>
<td>- X</td>
</tr>
</tbody>
</table>

NOTE: Divide by zero operations return an error. To return a different result,
consider the IEEE_DIVIDE or SAFE_DIVIDE functions.

Result types for Addition and Multiplication:

<table>
<thead>
<tr><th>&nbsp;</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr>
</thead>
<tbody><tr><td>INT32</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>INT64</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT32</td><td>INT64</td><td>INT64</td><td>UINT64</td><td>UINT64</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT64</td><td>ERROR</td><td>ERROR</td><td>UINT64</td><td>UINT64</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>FLOAT</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr></tbody>
</table>

Result types for Subtraction:

<table>
<thead>
<tr><th>&nbsp;</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr>
</thead>
<tbody><tr><td>INT32</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>INT64</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT32</td><td>INT64</td><td>INT64</td><td>INT64</td><td>INT64</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT64</td><td>ERROR</td><td>ERROR</td><td>INT64</td><td>INT64</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>FLOAT</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr></tbody>
</table>

Result types for Division:

<table>
<thead>
<tr><th>&nbsp;</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr>
</thead>
<tbody><tr><td>INT32</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>INT64</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT32</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>UINT64</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>NUMERIC</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>FLOAT</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr><tr><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td><td>DOUBLE</td></tr></tbody>
</table>

Result types for Unary Minus:

<table>
<thead>
<tr>
<th>Input Data Type</th>
<th>Result Data Type</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT32</td>
<td>INT32</td>
</tr>

<tr>
<td>INT64</td>
<td>INT64</td>
</tr>

<tr>
<td>NUMERIC</td>
<td>NUMERIC</td>
</tr>

<tr>
<td>FLOAT</td>
<td>FLOAT</td>
</tr>

<tr>
<td>DOUBLE</td>
<td>DOUBLE</td>
</tr>

</tbody>
</table>

### Bitwise operators
All bitwise operators return the same type
 and the same length as
the first operand.

<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th nowrap>Input Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Bitwise not</td>
<td>~ X</td>
<td nowrap>Integer or BYTES</td>
<td>Performs logical negation on each bit, forming the ones' complement of the
given binary value.</td>
</tr>
<tr>
<td>Bitwise or</td>
<td>X | Y</td>
<td nowrap>X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical inclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise xor</td>
<td nowrap>X ^ Y</td>
<td nowrap>X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical exclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise and</td>
<td nowrap>X &amp; Y</td>
<td nowrap>X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical AND
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Left shift</td>
<td nowrap>X &lt;&lt; Y</td>
<td nowrap>X: Integer or BYTES
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
<td nowrap>X &gt;&gt; Y</td>
<td nowrap>X: Integer or BYTES
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

All logical operators allow only BOOL input.

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
<td>Logical NOT</td>
<td nowrap>NOT X</td>
<td>Returns FALSE if input is TRUE. Returns TRUE if input is FALSE. Returns <code>NULL</code>
otherwise.</td>
</tr>
<tr>
<td>Logical AND</td>
<td nowrap>X AND Y</td>
<td>Returns FALSE if at least one input is FALSE. Returns TRUE if both X and Y
are TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
<tr>
<td>Logical OR</td>
<td nowrap>X OR Y</td>
<td>Returns FALSE if both X and Y are FALSE. Returns TRUE if at least one input
is TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
</tbody>
</table>

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

+  Floating point: All comparisons with NaN return FALSE,
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
<td>Returns TRUE if X is less than Y.</td>
</tr>
<tr>
<td>Less Than or Equal To</td>
<td>X &lt;= Y</td>
<td>Returns TRUE if X is less than or equal to Y.</td>
</tr>
<tr>
<td>Greater Than</td>
<td>X &gt; Y</td>
<td>Returns TRUE if X is greater than Y.</td>
</tr>
<tr>
<td>Greater Than or Equal To</td>
<td>X &gt;= Y</td>
<td>Returns TRUE if X is greater than or equal to Y.</td>
</tr>
<tr>
<td>Equal</td>
<td>X = Y</td>
<td>Returns TRUE if X is equal to Y.</td>
</tr>
<tr>
<td>Not Equal</td>
<td>X != Y<br>X &lt;&gt; Y</td>
<td>Returns TRUE if X is not equal to Y.</td>
</tr>
<tr>
<td>BETWEEN</td>
<td>X [NOT] BETWEEN Y AND Z</td>
<td>Returns TRUE if X is [not] within the range specified. The result of "X
BETWEEN Y AND Z" is equivalent to "Y &lt;= X AND X &lt;= Z" but X is evaluated
only once in the former.</td>
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
required. For example, <code>r"\%".</li>
</ul>
</td>
</tr>
<tr>
<td>IN</td>
<td>Multiple - see below</td>
<td>Returns FALSE if the right operand is empty. Returns <code>NULL</code> if the left
operand is <code>NULL</code>. Returns TRUE or <code>NULL</code>, never FALSE, if the right operand
contains <code>NULL</code>. Arguments on either side of IN are general expressions. Neither
operand is required to be a literal, although using a literal on the right is
most common. X is evaluated only once.</td>
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
<td><code>STRUCT(1, NULL)</code</td>
<td><code>NULL</code></td>
</tr>
</tbody>
</table>

### IN operators

The `IN` operator supports the following syntaxes:

```
x [NOT] IN (y, z, ... ) # Requires at least one element
x [NOT] IN (<subquery>)
x [NOT] IN UNNEST(<array expression>) # analysis error if the expression
                                      # does not return an ARRAY type.
```

Arguments on either side of the `IN` operator  are general expressions.
It is common to use literals on the right side expression; however, this is not
required.

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

The UNNEST form treats an array scan like `UNNEST` in the
[FROM][operators-link-to-from-clause] clause:

```
x [NOT] IN UNNEST(<array expression>)
```

This form is often used with ARRAY parameters. For example:

```
x IN UNNEST(@array_parameter)
```

**Note:** A `NULL` ARRAY will be treated equivalently to an empty ARRAY.

See the [Arrays][operators-link-to-arrays-topic] topic for more information on
how to use this syntax.

When using the `IN` operator, the following semantics apply:

+ `IN` with an empty right side expression is always FALSE
+ `IN` with a `NULL` left side expression and a non-empty right side expression is
  always `NULL`
+ `IN` with a `NULL` in the `IN`-list can only return TRUE or `NULL`, never FALSE
+ `NULL IN (NULL)` returns `NULL`
+ `IN UNNEST(<NULL array>)` returns FALSE (not `NULL`)

`IN` can be used with multi-part keys by using the struct constructor syntax.
For example:

```
(Key1, Key2) IN ( (12,34), (56,78) )
(Key1, Key2) IN ( SELECT (table.a, table.b) FROM table )
```

See the [Struct Type][operators-link-to-struct-type] section of the Data Types topic for more
information on this syntax.

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

[operators-link-to-arrays-topic]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering-arrays
[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md
[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from-clause
[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct-type
[operators-link-to-math-functions]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#mathematical-functions
[link-to-coercion]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#coercion

