

# Conversion rules

"Conversion" includes, but is not limited to, casting and coercion.

+ Casting is explicit conversion and uses the `CAST()` function.
+ Coercion is implicit conversion, which ZetaSQL performs
  automatically under the conditions described below.
+ There is a third group of conversion
functions that have their own function
  names, such as `UNIX_DATE()`.

The table below summarizes all possible `CAST` and coercion possibilities for
ZetaSQL data types. "Coercion To" applies to all *expressions* of a
given data type (e.g. a column)
, but literals
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
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>DOUBLE</span><br /><span>INT64</span><br /><span>NUMERIC</span><br /></td>
</tr>

<tr>
<td>INT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>DOUBLE</span><br /><span>NUMERIC</span><br /></td>
</tr>

<tr>
<td>UINT32</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>INT64</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>UINT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>DOUBLE</span><br /><span>NUMERIC</span><br /></td>
</tr>

<tr>
<td>NUMERIC</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>FLOAT</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>DOUBLE</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BOOL</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>STRING</span><br /><span>UINT32</span><br /><span>UINT64</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>STRING</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>BYTES</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /><span>ENUM</span><br /><span>PROTO</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BYTES</td>
<td><span>BYTES</span><br /><span>PROTO</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>DATE</td>
<td><span>DATE</span><br /><span>DATETIME</span><br /><span>STRING</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>DATETIME</td>
<td><span>DATE</span><br /><span>DATETIME</span><br /><span>STRING</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIME</td>
<td><span>STRING</span><br /><span>TIME</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td><span>DATE</span><br /><span>DATETIME</span><br /><span>STRING</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
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
</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>STRING</span><br /><span>UINT32</span><br /><span>UINT64</span><br /></td>
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
</span><br /><span>BYTES</span><br /><span>STRING</span><br /></td>
<td>PROTO
(with the same PROTO name)</td>
</tr>

</tbody>
</table>

### Casting

Syntax:

```
CAST(expr AS typename)
```

Cast syntax is used in a query to indicate that the result type of an
expression should be converted to some other type.

Example:

```
CAST(x=1 AS STRING)
```

This results in `"true"` if `x` is `1`, `"false"` for any other non-`NULL`
value, and `NULL` if `x` is `NULL`.

Casts between supported types that do not successfully map from the original
value to the target domain produce runtime errors. For example, casting
BYTES to STRING where the
byte sequence is not valid UTF-8 results in a runtime error.

Other examples include:

+ Casting INT64 to INT32 where the value overflows INT32.
+ Casting STRING to INT32 where the STRING contains non-digit characters.

When casting an expression `x` of the following types, these rules apply:

<table>
<tr>
<th>From</th>
<th>To</th>
<th>Rule(s) when casting <code>x</code></th>
</tr>
<tr>
<td>Integer</td>
<td>Floating Point</td>
<td>Returns a close but potentially not exact
Floating Point
value.</td>
</tr>
<tr>
<td>Integer</td>
<td>BOOL</td>
<td>Returns <code>FALSE</code> if <code>x</code> is <code>0</code>, <code>TRUE</code> otherwise.</td>
</tr>
<tr>
<td>NUMERIC</td>
<td>Floating Point</td>
<td>NUMERIC will convert to the closest floating point number with a possible
loss of precision.
</td>
</tr>
<tr>
<td>Floating Point</td>
<td>Integer</td>
<td>Returns the closest integer value.<br />
Halfway cases such as 1.5 or -0.5 round away from zero.</td>
</tr>
<tr>
<td>Floating Point</td>
<td>STRING</td>
<td>Returns an approximate string representation.<br />
</td>
</tr>
<tr>
<td>Floating Point</td>
<td>NUMERIC</td>
<td>If the floating point number has more than nine digits after the decimal
  point, it will round
  <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">
  half away from zero</a>. Casting a <code>NaN</code>, <code>+inf</code> or
  <code>-inf</code> will return an error. Casting a value
  <a href="data-types#numeric-type">outside the range of NUMERIC</a> will
  return an overflow error.
</td>
</tr>
<tr>
<td>BOOL</td>
<td>Integer</td>
<td>Returns <code>1</code> if <code>x</code> is <code>TRUE</code>, <code>0</code> otherwise.</td>
</tr>
<tr>
<td>BOOL</td>
<td>STRING</td>
<td>Returns <code>"true"</code> if <code>x</code> is <code>TRUE</code>, <code>"false"</code> otherwise.</td>
</tr>
<tr>
<td>STRING</td>
<td>Floating Point</td>
<td>Returns <code>x</code> as a
Floating Point
value, interpreting it as having the same form as a valid
Floating Point
literal.<br />
Also supports casts from <code>"inf"</code>, <code>"+inf"</code>, <code>"-inf"</code>, and <code>"nan"</code>.<br />
Conversions are case-insensitive.
</td>
</tr>
<tr>
<td>STRING</td>
<td>NUMERIC</td>
<td>The numeric literal contained in the <code>STRING</code> must not exceed the
  <a href="data-types#numeric-type">maximum precision or range of the
<code>NUMERIC</code> type</a>, or an error will occur. If the number of digits
after the decimal point exceeds nine, then the resulting <code>NUMERIC</code>
value will round
<a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">half
away from zero</a> to have nine digits after the decimal point.
</td>
</tr>
<tr>
<td>STRING</td>
<td>BOOL</td>
<td>Returns <code>TRUE</code> if <code>x</code> is <code>"true"</code> and
<code>FALSE</code> if <code>x</code> is <code>"false"</code><br />
All other values of <code>x</code> are invalid and throw an error instead of
casting to BOOL.<br />
STRINGs are case-insensitive when converting to BOOL.</td>
</tr>
<tr>
<td>STRING</td>
<td>BYTES</td>
<td>STRINGs are cast to BYTES using UTF-8 encoding. For example, the STRING "&copy;",
when cast to BYTES, would become a 2-byte sequence with the hex values C2 and
A9.</td>
</tr>

<tr>
<td>STRING</td>
<td>PROTO</td>
<td>Returns the PROTO that results from parsing from proto2 text format.<br />
Throws an error if parsing fails, e.g. if not all required fields are set.
</td>
</tr>

<tr>
<td>BYTES</td>
<td>STRING</td>
<td>Returns <code>x</code> interpreted as a UTF-8 STRING.<br />
For example, the BYTES literal
<code>b'\xc2\xa9'</code>, when cast to STRING, is interpreted as UTF-8 and
becomes the unicode character "&copy;".<br />
An error occurs if <code>x</code> is not valid UTF-8.</td>
</tr>

<tr>
<td>BYTES</td>
<td>PROTO</td>
<td>Returns the PROTO that results from parsing
<code>x</code> from the proto2 wire format.<br />
Throws an error if parsing fails, e.g. if not all required fields are set.
</td>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>The element types of the input
 <code>ARRAY</code> must be castable to the element
  types of the target <code>ARRAY</code>.

  For example, casting from type
  <code>ARRAY&lt;INT64&gt;</code> to
  <code>ARRAY&lt;DOUBLE&gt;</code> or
  <code>ARRAY&lt;STRING&gt;</code> is valid;
  casting from type <code>ARRAY&lt;INT64&gt;</code> to
  <code>ARRAY&lt;BYTES&gt;</code> is not valid.
</td>
</tr>

<tr>
<td>ENUM</td>
<td>STRING</td>
<td>Returns the canonical ENUM value name of <code>x</code>.<br />
If an ENUM value has multiple names (aliases),
the canonical name/alias for that value is used.</td>
</tr>
<tr>
<td>ENUM</td>
<td>ENUM</td>
<td>Must have the same ENUM name.</td>
</tr>

<tr>
<td>STRUCT</td>
<td>STRUCT</td>
<td>Allowed if the following conditions are met:<br />
<ol>
<li>The two STRUCTs have the same number of fields.</li>
<li>The original STRUCT field types can be explicitly cast to the corresponding
target STRUCT field types (as defined by field order, not field name).</li>
</ol>
</td>
</tr>

<tr>
<td>PROTO</td>
<td>STRING</td>
<td>Returns the proto2 text format representation of <code>x</code>.</td>
</tr>
<tr>
<td>PROTO</td>
<td>BYTES</td>
<td>Returns the proto2 wire format BYTES of <code>x</code>.</td>
</tr>
<tr>
<td>PROTO</td>
<td>PROTO</td>
<td>Must have the same PROTO name.</td>
</tr>

</table>

#### Safe casting

When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. For example, the following query generates an error:

```
SELECT CAST("apple" AS INT64) AS not_a_number;
```

If you want to protect your queries from these types of errors, you can use
`SAFE_CAST`. `SAFE_CAST` is identical to `CAST`, except it returns NULL instead
of raising an error.

```
SELECT SAFE_CAST("apple" AS INT64) AS not_a_number;

+--------------+
| not_a_number |
+--------------+
| NULL         |
+--------------+
```

If you are casting from bytes to strings, you can also use the
function, `SAFE_CONVERT_BYTES_TO_STRING`. Any invalid UTF-8 characters are
replaced with the unicode replacement character, `U+FFFD`. See
[SAFE_CONVERT_BYTES_TO_STRING][con-rules-link-to-safe-convert-bytes-to-string] for more
information.

#### Casting hex strings to integers

If you are working with hex strings (`0x123`), you can cast those strings as
integers:

```sql
SELECT '0x123' as hex_value, CAST('0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| 0x123     | 291        |
+-----------+------------+

SELECT '0x123' as hex_value, CAST('-0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| 0x123     | -291       |
+-----------+------------+
```

#### Casting date types

ZetaSQL supports casting date types to/from strings as follows:

```
CAST(date_expression AS STRING)
CAST(string_expression AS DATE)
```

Casting from a date type to a string is independent of time zone and is of the
form `YYYY-MM-DD`.  When casting from string to date, the string must conform to
the supported date literal format, and is independent of time zone. If the string
expression is invalid or represents a date that is outside of the supported
min/max range, then an error is produced.

#### Casting timestamp types

ZetaSQL supports casting timestamp types to/from strings as follows:

```
CAST(timestamp_expression AS STRING)
CAST(string_expression AS TIMESTAMP)
```

When casting from timestamp types to string, the timestamp is interpreted using
the default time zone, which is implementation defined. The number of subsecond digits
produced depends on the number of trailing zeroes in the subsecond part: the
CAST function will truncate zero, three, or six digits.

When casting from string to a timestamp, `string_expression` must conform to
the supported timestamp literal formats, or else a runtime error
occurs.  The `string_expression` may itself contain a `time_zone`&mdash;see
[time zones][con-rules-link-to-time-zones].
If there is a time zone in the `string_expression`, that time zone is used for
conversion, otherwise the default time zone, which is implementation defined, is used.
If the string has fewer than six digits, then it is implicitly widened.

An error is produced if the `string_expression` is invalid, has more than six
subsecond digits (i.e. precision greater than microseconds), or represents a
time outside of the supported timestamp range.

#### Casting between date and timestamp types

ZetaSQL supports casting between date and timestamp types as follows:

```
CAST(date_expression AS TIMESTAMP)
CAST(timestamp_expression AS DATE)
```

Casting from a date to a timestamp interprets `date_expression` as of midnight
(start of the day) in the default time zone, which is implementation defined. Casting
from a timestamp to date effectively truncates the timestamp as of the default
time zone.

#### Bit casting

ZetaSQL supports bit casting functions between signed and unsigned
integers. A bit cast is a cast in which the order of bits is preserved instead
of the value those bytes represent.

```sql
SELECT -1 as INT64_value, BIT_CAST_TO_UINT64(-1) as bit_cast_value;

+-------------+----------------------+
| INT64_value | bit_cast_value       |
+-------------+----------------------+
| -1          | 18446744073709551615 |
+-------------+----------------------+
```

The following is a list of bit casting functions:

+ <a id="bit_cast_to_uint64"></a>`BIT_CAST_TO_UINT64(int64_value)`
+ `BIT_CAST_TO_UINT64(uint64_value)`
+ <a id="bit_cast_to_int64"></a>`BIT_CAST_TO_INT64(int64_value)`
+ `BIT_CAST_TO_INT64(uint64_value)`
+ <a id="bit_cast_to_uint32"></a>`BIT_CAST_TO_UINT32(int32_value)`
+ `BIT_CAST_TO_UINT32(uint32_value)`
+ <a id="bit_cast_to_int32"></a>`BIT_CAST_TO_INT32(int32_value)`
+ `BIT_CAST_TO_INT32(uint32_value)`

### Coercion

ZetaSQL coerces the result type of an expression to another type if
needed to match function signatures.  For example, if function func() is defined
to take a single argument of type INT64  and an
 expression is used as an argument that has a result type of
DOUBLE, then the result of the expression will be
coerced to INT64 type before func() is computed.

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
<td><span> ENUM</span><br /><span> INT32</span><br /><span> UINT32</span><br /><span> UINT64</span><br /></td>
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
<td><span> DATE</span><br /><span> ENUM</span><br /><span> PROTO</span><br /><span> TIMESTAMP</span><br /></td>
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
<td><span> PROTO<span><br /></td>
</tr>

<tr>
<td>BYTES parameter</td>
<td>PROTO</td>
</tr>

</tbody>
</table>

If the parameter value cannot be coerced successfully to the target type, an
error is provided.

<a id="additional_date_and_timestamp_conversion_functions"></a>
### Additional conversion functions

ZetaSQL provides the following additional conversion functions:

+ [DATE functions][con-rules-link-to-date-functions]
+ [DATETIME functions][con-rules-link-to-datetime-functions]
+ [TIME functions][con-rules-link-to-time-functions]
+ [TIMESTAMP functions][con-rules-link-to-timestamp-functions]

[con-rules-link-to-literal-coercion]: #literal-coercion
[con-rules-link-to-parameter-coercion]: #parameter-coercion
[con-rules-link-to-time-zones]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time-zones

[con-rules-link-to-safe-convert-bytes-to-string]: https://github.com/google/zetasql/blob/master/docs/string_functions.md#safe_convert_bytes_to_string
[con-rules-link-to-date-functions]: https://github.com/google/zetasql/blob/master/docs/date_functions.md
[con-rules-link-to-datetime-functions]: https://github.com/google/zetasql/blob/master/docs/datetime_functions.md
[con-rules-link-to-time-functions]: https://github.com/google/zetasql/blob/master/docs/time_functions.md
[con-rules-link-to-timestamp-functions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md

