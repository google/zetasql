

# Conversion rules

"Conversion" includes, but is not limited to, casting and coercion.

+ Casting is explicit conversion and uses the
  [`CAST()`][con-rules-link-to-cast] function.
+ Coercion is implicit conversion, which ZetaSQL performs
  automatically under the conditions described below.

There are also conversions that have their own function names, such as
`PARSE_DATE()`. To learn more about these functions, see
[Conversion functions][con-rules-link-to-conversion-functions-other]

### Comparison chart

The table below summarizes all possible `CAST` and coercion possibilities for
ZetaSQL data types. "Coercion To" applies to all *expressions* of a
given data type, (for example, a
column), but literals
and parameters can also be coerced. See [Literal Coercion][con-rules-link-to-literal-coercion] and
[Parameter Coercion][con-rules-link-to-parameter-coercion] for details.

<table>
<thead>
<tr>
<th>From Type</th>
<th>CAST to</th>
<th>Coercion To</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT32</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>INT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>INT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>UINT32</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>INT64</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>UINT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>NUMERIC</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>BIGNUMERIC</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>FLOAT</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>DOUBLE</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BOOL</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>STRING</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>BYTES</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /><span>ENUM</span><br /><span>PROTO</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BYTES</td>
<td><span>STRING</span><br /><span>BYTES</span><br /><span>PROTO</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>DATE</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIMESTAMP</span><br /></td>
<td><span>DATETIME</span><br /></td>
</tr>

<tr>
<td>DATETIME</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIME</td>
<td><span>STRING</span><br /><span>TIME</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>ENUM</td>
<td><span>
ENUM
(with the same ENUM name)
</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>STRING</span><br /></td>
<td>ENUM
(with the same ENUM name)</td>
</tr>

<tr>
<td>STRUCT</td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>PROTO</td>
<td><span>
PROTO
(with the same PROTO name)
</span><br /><span>STRING</span><br /><span>BYTES</span><br /></td>
<td>PROTO
(with the same PROTO name)</td>
</tr>

</tbody>
</table>

### Casting

Most data types can be cast from one type to another with the `CAST` function.
When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. If you want to protect your queries from these types of errors, you
can use `SAFE_CAST`. To learn more about the rules for `CAST`, `SAFE_CAST` and
other casting functions, see
[Conversion functions][con-rules-link-to-conversion-functions].

### Coercion

ZetaSQL coerces the result type of an argument expression to another
type if needed to match function signatures. For example, if function func() is
defined to take a single argument of type DOUBLE and
an expression is used as an argument that has a result type of
INT64, then the result of the expression will be
coerced to DOUBLE type before func() is computed.

#### Literal coercion

ZetaSQL supports the following literal coercions:

<table>
<thead>
<tr>
<th>Input Data Type</th>
<th>Result Data Type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>

<tr>
<td>Integer literal</td>
<td><span> INT32</span><br /><span> UINT32</span><br /><span> UINT64</span><br /><span> ENUM</span><br /></td>
<td>

Integer literals will implicitly coerce to ENUM type when necessary, or can
be explicitly CAST to a specific ENUM type name.

</td>
</tr>

<tr>
<td>DOUBLE literal</td>
<td>

<span> NUMERIC</span><br />

<span> FLOAT</span><br />

</td>
<td>Coercion may not be exact, and returns a close value.</td>
</tr>

<tr>
<td>STRING literal</td>
<td><span> DATE</span><br /><span> DATETIME</span><br /><span> TIME</span><br /><span> TIMESTAMP</span><br /><span> ENUM</span><br /><span> PROTO</span><br /></td>
<td>

String literals will implicitly coerce to PROTO
or ENUM type when necessary, or can
be explicitly CAST to a specific PROTO or
ENUM type name.

</td>
</tr>

<tr>
<td>BYTES literal</td>
<td>PROTO</td>
<td>&nbsp;</td>
</tr>

</tbody>
</table>

Literal coercion is needed when the actual literal type is different from the
type expected by the function in question. For
example, if function `func()` takes a DATE argument, then the expression
`func("2014-09-27")` is valid because the STRING literal `"2014-09-27"`
is coerced to DATE.

Literal conversion is evaluated at analysis time, and gives an error if the
input literal cannot be converted successfully to the target type.

**Note:** String literals do not coerce to numeric types.

#### Parameter coercion

ZetaSQL supports the following parameter coercions:

<table>
<thead>
<tr>
<th>Input Data Type</th>
<th>Result Data Type</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT32 parameter</td>
<td>ENUM</td>
</tr>

<tr>
<td>INT64 parameter</td>
<td>ENUM</td>
</tr>

<tr>
<td>STRING parameter</td>
<td><span> PROTO</span><br /></td>
</tr>

<tr>
<td>BYTES parameter</td>
<td>PROTO</td>
</tr>

</tbody>
</table>

If the parameter value cannot be coerced successfully to the target type, an
error is provided.

[conversion-rules-table]: #conversion_rules
[con-rules-link-to-literal-coercion]: #literal_coercion
[con-rules-link-to-parameter-coercion]: #parameter_coercion
[con-rules-link-to-time-zones]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time_zones

[con-rules-link-to-safe-convert-bytes-to-string]: https://github.com/google/zetasql/blob/master/docs/string_functions.md#safe_convert_bytes_to_string
[con-rules-link-to-date-functions]: https://github.com/google/zetasql/blob/master/docs/date_functions.md
[con-rules-link-to-datetime-functions]: https://github.com/google/zetasql/blob/master/docs/datetime_functions.md
[con-rules-link-to-time-functions]: https://github.com/google/zetasql/blob/master/docs/time_functions.md
[con-rules-link-to-timestamp-functions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md
[con-rules-link-to-conversion-functions]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md
[con-rules-link-to-cast]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#cast
[con-rules-link-to-conversion-functions-other]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#other_conv_functions

