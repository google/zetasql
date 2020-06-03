

# String functions

<a name="string_values"></a>These string functions work on two different values:
STRING and BYTES data
types. STRING values must be well-formed UTF-8.

Functions that return position values, such as [STRPOS][string-link-to-strpos], encode those
positions as INT64. The value `1` refers to the first
character (or byte), `2` refers to the second, and so on. The value `0`
indicates an invalid index. When working on
STRING types, the returned positions refer to
character positions.

All string comparisons are done byte-by-byte, without regard to Unicode
canonical equivalence.

### BYTE_LENGTH

```
BYTE_LENGTH(value)
```

**Description**

Returns the length of the [value][string-link-to-string-values] in bytes, regardless of
whether the type of the value is STRING or
BYTES.

**Return type**

INT64

**Examples**

```sql

WITH example AS
  (SELECT "абвгд" AS characters, b"абвгд" AS bytes)

SELECT
  characters,
  BYTE_LENGTH(characters) AS string_example,
  bytes,
  BYTE_LENGTH(bytes) AS bytes_example
FROM example;

+------------+----------------+-------+---------------+
| characters | string_example | bytes | bytes_example |
+------------+----------------+-------+---------------+
| абвгд      | 10             | абвгд | 10            |
+------------+----------------+-------+---------------+
```

### CHAR_LENGTH

```
CHAR_LENGTH(value)
```

**Description**

Returns the length of the STRING in characters.

**Return type**

INT64

**Examples**

```sql

WITH example AS
  (SELECT "абвгд" AS characters)

SELECT
  characters,
  CHAR_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+

```
### CHARACTER_LENGTH
```
CHARACTER_LENGTH(value)
```

**Description**

Synonym for [CHAR_LENGTH][string-link-to-char-length].

**Return type**

INT64

**Examples**

```sql

WITH example AS
  (SELECT "абвгд" AS characters)

SELECT
  characters,
  CHARACTER_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+

```

### CODE_POINTS_TO_BYTES
```
CODE_POINTS_TO_BYTES(ascii_values)
```

**Description**

Takes an array of extended ASCII
[code points][string-link-to-code-points-wikipedia]
(ARRAY of INT64) and
returns BYTES.

To convert from BYTES to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

BYTES

**Examples**

The following is a basic example using `CODE_POINTS_TO_BYTES`.

```sql
SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]) AS bytes;

+-------+
| bytes |
+-------+
| AbCd  |
+-------+
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

+----------------+
| encoded_string |
+----------------+
| Grfg Fgevat!   |
+----------------+
```

### CODE_POINTS_TO_STRING
```
CODE_POINTS_TO_STRING(value)
```

**Description**

Takes an array of Unicode [code points][string-link-to-code-points-wikipedia]
(ARRAY of INT64) and
returns a STRING.

To convert from a string to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

STRING

**Example**

The following is a basic example using `CODE_POINTS_TO_STRING`.

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024]) AS string;

+--------+
| string |
+--------+
| AÿȁЀ   |
+--------+
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

+--------+--------------+
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
+--------+--------------+
```

### CONCAT
```
CONCAT(value1[, ...])
```

**Description**

Concatenates one or more values into a single result. All values must be
`BYTES` or data types that can be cast to `STRING`.

Note: You can also use the
[|| concatenation operator][string-link-to-operators] to concatenate
values into a string.

**Return type**

STRING or BYTES

**Examples**

```sql
SELECT CONCAT("T.P.", " ", "Bar") as author;

+---------------------+
| author              |
+---------------------+
| T.P. Bar            |
+---------------------+
```

```sql
SELECT CONCAT("Summer", " ", 1923) as release_date;

+---------------------+
| release_date        |
+---------------------+
| Summer 1923         |
+---------------------+
```

```sql

With Employees AS
  (SELECT
    "John" AS first_name,
    "Doe" AS last_name
  UNION ALL
  SELECT
    "Jane" AS first_name,
    "Smith" AS last_name
  UNION ALL
  SELECT
    "Joe" AS first_name,
    "Jackson" AS last_name)

SELECT
  CONCAT(first_name, " ", last_name)
  AS full_name
FROM Employees;

+---------------------+
| full_name           |
+---------------------+
| John Doe            |
| Jane Smith          |
| Joe Jackson         |
+---------------------+
```

### ENDS_WITH
```
ENDS_WITH(value1, value2)
```

**Description**

Takes two [values][string-link-to-string-values]. Returns TRUE if the second value is a
suffix of the first.

**Return type**

BOOL

**Examples**

```sql

WITH items AS
  (SELECT "apple" as item
  UNION ALL
  SELECT "banana" as item
  UNION ALL
  SELECT "orange" as item)

SELECT
  ENDS_WITH(item, "e") as example
FROM items;

+---------+
| example |
+---------+
|    True |
|   False |
|    True |
+---------+

```

### FORMAT {: #format_string }

ZetaSQL supports a `FORMAT()` function for formatting strings. This
function is similar to the C `printf` function. It produces a
STRING from a format string that contains zero or
more format specifiers, along with a variable length list of additional
arguments that matches the format specifiers. Here are some examples:

<table>
<tr>
<th>Description</th>
<th>Statement</th>
<th>Result</th>
</tr>
<tr>
<td>Simple integer</td>
<td>FORMAT("%d", 10)</td>
<td>10</td>
</tr>
<tr>
<td>Integer with left blank padding</td>
<td>FORMAT("|%10d|", 11)</td>
<td>|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;11|</td>
</tr>
<tr>
<td>Integer with left zero padding</td>
<td>FORMAT("+%010d+", 12)</td>
<td>+0000000012+</td>
</tr>
<tr>
<td>Integer with commas</td>
<td>FORMAT("%'d", 123456789)</td>
<td>123,456,789</td>
</tr>
<tr>
<td>STRING</td>
<td>FORMAT("-%s-", 'abcd efg')</td>
<td>-abcd efg-</td>
</tr>
<tr>
<td>DOUBLE</td>
<td>FORMAT("%f %E", 1.1, 2.2)</td>
<td>1.100000&nbsp;2.200000E+00</td>
</tr>

<tr>
<td>DATE</td>
<td>FORMAT("%t", date "2015-09-01")</td>
<td>2015-09-01</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td>FORMAT("%t", timestamp "2015-09-01 12:34:56
America/Los_Angeles")</td>
<td>2015&#8209;09&#8209;01&nbsp;19:34:56+00</td>
</tr>
</table>

The `FORMAT()` function does not provide fully customizable formatting for all
types and values, nor formatting that is sensitive to locale.

If custom formatting is necessary for a type, you must first format it using
type-specific format functions, such as `FORMAT_DATE()` or `FORMAT_TIMESTAMP()`.
For example:

```sql
SELECT FORMAT("date: %s!", FORMAT_DATE("%B %d, %Y", date '2015-01-02'));
```

Returns

```
date: January 02, 2015!
```

#### Syntax

The `FORMAT()` syntax takes a format string and variable length list of
arguments and produces a STRING result:

```
FORMAT(<format_string>, ...)
```

The `<format_string>` expression can contain zero or more format specifiers.
Each format specifier is introduced by the `%` symbol, and must map to one or
more of the remaining arguments.  For the most part, this is a one-to-one
mapping, except when the `*` specifier is present. For example, `%.*i` maps to
two arguments&mdash;a length argument and a signed integer argument.  If the
number of arguments related to the format specifiers is not the same as the
number of arguments, an error occurs.

#### Supported format specifiers

The `FORMAT()` function format specifier follows this prototype:

```
%[flags][width][.precision]specifier
```

The supported format specifiers are identified in the following table.
Deviations from printf() are identified in <em>italics</em>.

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
    <span> NUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
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
    <span> NUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>e</code></td>
    <td>Scientific notation (mantissa/exponent), lowercase</td>
    <td>3.926500e+02<br/>
    inf<br/>
    nan</td>
    <td>
    <span> NUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>E</code></td>
    <td>Scientific notation (mantissa/exponent), uppercase</td>
    <td>3.926500E+02<br/>
    INF<br/>
    NAN</td>
    <td>
    <span> NUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
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
    <span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>G</code></td>
    <td>Either decimal notation or scientific notation, depending on the input
        value's exponent and the specified precision. Uppercase.
        See <a href="#g_and_g_behavior">%g and %G behavior</a> for details.</td>
    <td>392.65<br/>
      3.9265E+07<br/>
    INF<br/>
    NAN</td>
    <td>
    <span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>

 <tr>
    <td><em><code>p</code></em></td>
    <td><em>
      <p>Produces a one-line printable string representing a protocol buffer.</p>
      <p>This protocol buffer generates the example to the right:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
    </em></td>
    <td><em>year: 2019 month: 10</em></td>
    <td><em>ShortDebugString</em></td>
 </tr>
 <tr>
    <td><em><code>P</code></em></td>
    <td><em>
      <p>Produces a multi-line printable string representing a protocol buffer.</p>
      <p>This protocol buffer generates the example to the right:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
    </em></td>
    <td><em>
      year: 2019<br/>
      month: 10
      </em></td>
    <td><em>DebugString</em></td>
 </tr>

 <tr>
    <td><code>s</code></td>
    <td>String of characters</td>
    <td>sample</td>
    <td>STRING</td>
 </tr>
 <tr>
    <td><em><code>t</code></em></td>
    <td><em>Returns a printable string representing the value. Often looks
similar to casting the argument to STRING. See <a
href="#t_and_t_behavior">%t and %T behavior</a>.</em></td>
    <td><em>sample</em><br/>
    <em>2014&#8209;01&#8209;01</em></td>
    <td><em>&lt;any&gt;</em></td>
 </tr>
 <tr>
    <td><em><code>T</code></em></td>
    <td><em>Produces a string that is a valid ZetaSQL constant with a
similar type to the value's type (maybe wider, or maybe string). See <a
href="#t_and_t_behavior">%t and %T behavior</a>.</em></td>
    <td><em>'sample'</em><br/>
        <em>b'bytes&nbsp;sample'</em><br/>
        <em>1234</em><br/>
        <em>2.3</em><br/>
        <em>date&nbsp;'2014&#8209;01&#8209;01'</em></td>
    <td><em>&lt;any&gt;</em></td>
 </tr>
 <tr>
    <td><code>%</code></td>
    <td>'%%' produces a single '%'</td>
    <td>%</td>
    <td>n/a</td>
 </tr>
</table>

<i><a id="oxX"></a><sup>*</sup>The specifiers `%o`, `%x`, and `%X` raise an error if
negative values are used.</i>

The format specifier can optionally contain the sub-specifiers identified above
in the specifier prototype.

These sub-specifiers must comply with the following specifications.

##### Flags

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
    <td>Left-pads the number with zeroes (0) instead of spaces when padding is
specified (see width sub-specifier)</td>
 </tr>
 <tr>
  <td><code>'</code></td>
  <td><p>Formats integers using the appropriating grouping character.
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

Flags may be specified in any order.  Duplicate flags are not an error.  When
flags are not relevant for some element type, they are ignored.

##### Width

<table>
 <tr>
    <td>Width</td>
    <td>Description</td>
 </tr>
 <tr>
    <td>&lt;number&gt;</td>
    <td>Minimum number of characters to be printed. If the value to be printed
is shorter than this number, the result is padded with blank spaces. The value
is not truncated even if the result is larger</td>
</tr>
 <tr>
    <td><code>*</code></td>
    <td>The width is not specified in the format string, but as an additional
integer value argument preceding the argument that has to be formatted</td>
</tr> </table>

##### Precision

<table>
 <tr>
    <td>Precision</td>
    <td>Description</td>
 </tr>
 <tr>
    <td><code>.</code>&lt;number&gt;</td>
    <td>
      <ul>
      <li>For integer specifiers `%d`, `%i`, `%o`, `%u`, `%x`, and `%X`: precision specifies the
          minimum number of digits to be written. If the value to be written is
          shorter than this number, the result is padded with trailing zeros.
          The value is not truncated even if the result is longer. A precision
          of 0 means that no character is written for the value 0.</li>
      <li>For specifiers `%a`, `%A`, `%e`, `%E`, `%f`, and `%F`: this is the number of digits to be
          printed after the decimal point. The default value is 6.</li>
      <li>For specifiers `%g` and `%G`: this is the number of significant digits to be
          printed, before the removal of the trailing zeros after the decimal
          point. The default value is 6.</li>
      </ul>
   </td>
</tr>
 <tr>
    <td><code>.*</code></td>
    <td>The precision is not specified in the format string, but as an
additional integer value argument preceding the argument that has to be
formatted</td>
</tr>
</table>

<a name="g_and_g_behavior"></a>
#### %g and %G behavior
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

<a name="t_and_t_behavior"></a>
#### %t and %T behavior

The `%t` and `%T` format specifiers are defined for all types.  The [width](#width),
[precision](#precision), and [flags](#flags) act as they do for `%s`: the [width](#width) is the minimum width
and the STRING will be padded to that size, and [precision](#precision) is the maximum width
of content to show and the STRING will be truncated to that size, prior to
padding to width.

The `%t` specifier is always meant to be a readable form of the value.

The `%T` specifier is always a valid SQL literal of a similar type, such as a wider numeric
type.
The literal will not include casts or a type name, except for the special case
of non-finite floating point values.

The STRING is formatted as follows:

<table>
 <tr>
    <td><strong>Type</strong></td>
    <td><strong>%t</strong></td>
    <td><strong>%T</strong></td>
 </tr>
 <tr>
    <td><code>NULL</code> of any type</td>
    <td><code>NULL</code></td>
    <td><code>NULL</code></td>
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
<td>123.0  <em>(always with .0)</em><br/>
        123e+10<br><code>inf</code><br><code>-inf</code><br><code>NaN</code></td>
    <td>123.0  <em>(always with .0)</em><br/>
        123e+10<br/>
        CAST("inf" AS &lt;type&gt;)<br/>
        CAST("-inf" AS &lt;type&gt;)<br/>
        CAST("nan" AS &lt;type&gt;)</td>
 </tr>
 <tr>
    <td>STRING</td>
    <td>unquoted string value</td>
    <td>quoted string literal</td>
 </tr>
 <tr>
    <td>BYTES</td>
    <td>unquoted escaped bytes<br/>
    e.g. abc\x01\x02</td>
    <td>quoted bytes literal<br/>
    e.g. b"abc\x01\x02"</td>
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
    <td>PROTO</td>
    <td>proto ShortDebugString</td>
    <td>quoted string literal with proto<br/>
    ShortDebugString</td>
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
</table>

#### Error conditions

If a format specifier is invalid, or is not compatible with the related
argument type, or the wrong number or arguments are provided, then an error is
produced.  For example, the following `<format_string>` expressions are invalid:

```
FORMAT('%s', 1)
```

```
FORMAT('%')
```

#### NULL argument handling

A `NULL` format string results in a `NULL` output STRING.  Any other arguments are
ignored in this case.

The function generally produces a `NULL` value if a `NULL` argument is present. For
example, `FORMAT('%i', <NULL expression>)` produces a `NULL`
STRING as output.

However, there are some exceptions: if the format specifier is %t or %T (both of
which produce STRINGs that effectively match CAST and
literal value semantics), a `NULL` value produces 'NULL' (without the quotes) in
the result STRING. For example, the function:

```
FORMAT('00-%t-00', <NULL expression>);
```

Returns

```
00-NULL-00
```

#### Additional semantic rules

DOUBLE and
FLOAT values can be `+/-inf` or `NaN`.  When an argument has one of
those values, the result of the format specifiers `%f`, `%F`, `%e`, `%E`, `%g`,
`%G`, and `%t` are `inf`, `-inf`, or `nan` (or the same in uppercase) as
appropriate.  This is consistent with how ZetaSQL casts these values
to STRING.  For `%T`, ZetaSQL returns quoted strings for
DOUBLE values that don't have non-string literal
representations.

### FROM_BASE32

```
FROM_BASE32(string_expr)
```

**Description**

Converts the base32-encoded input `string_expr` into BYTES format. To convert
BYTES to a base32-encoded STRING, use [TO_BASE32][string-link-to-base32].

**Return type**

BYTES

**Example**

```sql
SELECT FROM_BASE32('MFRGGZDF74======') AS byte_data;

+-----------+
| byte_data |
+-----------+
| abcde\xff |
+-----------+
```

### FROM_BASE64

```
FROM_BASE64(string_expr)
```

**Description**

Converts the base64-encoded input `string_expr` into BYTES format. To convert
BYTES to a base64-encoded STRING, use [TO_BASE64][string-link-to-base64].

**Return type**

BYTES

**Example**

```sql
SELECT FROM_BASE64('3q2+7w==') AS byte_data;

+------------------+
| byte_data        |
+------------------+
| \xde\xad\xbe\xef |
+------------------+
```

### FROM_HEX
```
FROM_HEX(string)
```

**Description**

Converts a hexadecimal-encoded STRING into BYTES format. Returns an error if the
input STRING contains characters outside the range
`(0..9, A..F, a..f)`. The lettercase of the characters does not matter. If the
input STRING has an odd number of characters, the function acts as if the input
has an additional leading `0`. To convert BYTES to a
hexadecimal-encoded STRING, use
[TO_HEX][string-link-to-to-hex].

**Return type**

BYTES

**Example**

```sql
WITH Input AS (
  SELECT '00010203aaeeefff' AS hex_str UNION ALL
  SELECT '0AF' UNION ALL
  SELECT '666f6f626172'
)
SELECT hex_str, FROM_HEX(hex_str) AS bytes_str
FROM Input;
+------------------+----------------------------------+
| hex_str          | bytes_str                        |
+------------------+----------------------------------+
| 0AF              | \x00\xaf                         |
| 00010203aaeeefff | \x00\x01\x02\x03\xaa\xee\xef\xff |
| 666f6f626172     | foobar                           |
+------------------+----------------------------------+
```

### LENGTH
```
LENGTH(value)
```

**Description**

Returns the length of the [value][string-link-to-string-values]. The returned value
is in characters for STRING arguments and in bytes
for the BYTES argument.

**Return type**

INT64

**Examples**

```sql

WITH example AS
  (SELECT "абвгд" AS characters)

SELECT
  characters,
  LENGTH(characters) AS string_example,
  LENGTH(CAST(characters AS BYTES)) AS bytes_example
FROM example;

+------------+----------------+---------------+
| characters | string_example | bytes_example |
+------------+----------------+---------------+
| абвгд      |              5 |            10 |
+------------+----------------+---------------+
```

### LPAD
```
LPAD(original_value, return_length[, pattern])
```

**Description**

Returns a [value][string-link-to-string-values] that consists of `original_value` prepended
with `pattern`. The `return_length` is an INT64 that specifies the length of the
returned value. If `original_value` is BYTES, `return_length` is the number of
bytes. If `original_value` is STRING, `return_length` is the number of
characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `LPAD("hello world", 7);` returns `"hello w"`.

If `original_value`, `return_length`, or `pattern` is NULL, this function
returns NULL.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

STRING or BYTES

**Examples**

```sql
SELECT t, len, FORMAT("%T", LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);
```
| t    | len | LPAD     |
|------|-----|----------|
| abc  | 5   | <code>"&nbsp;&nbsp;abc"</code>  |
| abc  | 2   | <code>"ab"</code>     |
| 例子 | 4   | <code>"&nbsp;&nbsp;例子"</code> |

```sql
SELECT t, len, pattern, FORMAT("%T", LPAD(t, len, pattern)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);
```
| t    | len | pattern | LPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | <code>"defdeabc"</code>   |
| abc  | 5   | -       | <code>"--abc"</code>      |
| 例子 | 5   | 中文    | <code>"中文中例子"</code> |

```sql
SELECT FORMAT("%T", t) AS t, len, FORMAT("%T", LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);
```
| t               | len | LPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | <code>b"&nbsp;&nbsp;abc"</code>         |
| b"abc"          | 2   | <code>b"ab"</code>            |
| b"\xab\xcd\xef" | 4   | <code>b" \xab\xcd\xef"</code> |

```sql
SELECT
  FORMAT("%T", t) AS t,
  len,
  FORMAT("%T", pattern) AS pattern,
  FORMAT("%T", LPAD(t, len, pattern)) AS LPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);
```
| t               | len | pattern | LPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | <code>b"defdeabc"</code>             |
| b"abc"          | 5   | b"-"    | <code>b"--abc"</code>                |
| b"\xab\xcd\xef" | 5   | b"\x00" | <code>b"\x00\x00\xab\xcd\xef"</code> |

### LOWER
```
LOWER(value)
```

**Description**

For STRING arguments, returns the original
string with all alphabetic characters in lowercase.
Mapping between lowercase and uppercase is done according to the
[Unicode Character Database][string-link-to-unicode-character-definitions] without taking into
account language-specific mappings.

For BYTES arguments, the
argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT
    "FOO" as item
  UNION ALL
  SELECT
    "BAR" as item
  UNION ALL
  SELECT
    "BAZ" as item)

SELECT
  LOWER(item) AS example
FROM items;

+---------+
| example |
+---------+
| foo     |
| bar     |
| baz     |
+---------+
```

### LTRIM
```
LTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes leading characters.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT "   apple   " as item
  UNION ALL
  SELECT "   banana   " as item
  UNION ALL
  SELECT "   orange   " as item)

SELECT
  CONCAT("#", LTRIM(item), "#") as example
FROM items;

+-------------+
| example     |
+-------------+
| #apple   #  |
| #banana   # |
| #orange   # |
+-------------+

WITH items AS
  (SELECT "***apple***" as item
  UNION ALL
  SELECT "***banana***" as item
  UNION ALL
  SELECT "***orange***" as item)

SELECT
  LTRIM(item, "*") as example
FROM items;

+-----------+
| example   |
+-----------+
| apple***  |
| banana*** |
| orange*** |
+-----------+

WITH items AS
  (SELECT "xxxapplexxx" as item
  UNION ALL
  SELECT "yyybananayyy" as item
  UNION ALL
  SELECT "zzzorangezzz" as item
  UNION ALL
  SELECT "xyzpearxyz" as item)

SELECT
  LTRIM(item, "xyz") as example
FROM items;

+-----------+
| example   |
+-----------+
| applexxx  |
| bananayyy |
| orangezzz |
| pearxyz   |
+-----------+
```

### NORMALIZE
```
NORMALIZE(value[, normalization_mode])
```

**Description**

Takes a STRING, `value`, and returns it as a normalized string.

[Normalization][string-link-to-normalization-wikipedia] is used to ensure that
two strings are equivalent. Normalization is often used in situations in which
two strings render the same on the screen but have different Unicode code
points.

`NORMALIZE` supports four optional normalization modes:

| Value | Name | Description|
|-------|------|------------|
| NFC | Normalization Form Canonical Composition | Decomposes and recomposes characters by canonical equivalence.
| NFKC | Normalization Form Compatibility Composition | Decomposes characters by compatibility, then recomposes them by canonical equivalence. |
| NFD   | Normalization Form Canonical Decomposition | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.
| NFKD | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

The default normalization mode is `NFC`.

**Return type**

STRING

**Examples**

```sql
SELECT a, b, a = b as normalized
FROM (SELECT NORMALIZE('\u00ea') as a, NORMALIZE('\u0065\u0302') as b)
AS normalize_example;

+---+---+------------+
| a | b | normalized |
+---+---+------------+
| ê | ê | true       |
+---+---+------------+
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

+-----------------+------------+
| normalized_name | name_count |
+-----------------+------------+
| John Smith      | 2          |
| Jane Doe        | 3          |
+-----------------+------------+
```

### NORMALIZE_AND_CASEFOLD
```
NORMALIZE_AND_CASEFOLD(value[, normalization_mode])
```

**Description**

Takes a STRING, `value`, and performs the same actions as
[`NORMALIZE`][string-link-to-normalize], as well as
[casefolding][string-link-to-case-folding-wikipedia] for
case-insensitive operations.

`NORMALIZE_AND_CASEFOLD` supports four optional normalization modes:

| Value | Name | Description|
|-------|------|------------|
| NFC | Normalization Form Canonical Composition | Decomposes and recomposes characters by canonical equivalence.
| NFKC | Normalization Form Compatibility Composition | Decomposes characters by compatibility, then recomposes them by canonical equivalence. |
| NFD   | Normalization Form Canonical Decomposition | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.
| NFKD | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

The default normalization mode is `NFC`.

**Return type**

STRING

**Example**

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

+---+----+-------+-------+------+------+
| a | b  | nfd   | nfc   | nkfd | nkfc |
+---+----+-------+-------+------+------+
| Ⅸ | IX | false | false | true | true |
| Å | Å  | true  | true  | true | true |
+---+----+-------+-------+------+------+
```

### REGEXP_CONTAINS

```
REGEXP_CONTAINS(value, regexp)
```

**Description**

Returns TRUE if `value` is a partial match for the regular expression, `regexp`.

If the `regexp` argument is invalid, the function returns an error.

You can search for a full match by using `^` (beginning of text) and `$` (end of
text). Due to regular expression operator precedence, it is good practice to use
parentheses around everything between `^` and `$`.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

BOOL

**Examples**

```sql
SELECT
  email,
  REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") AS is_valid
FROM
  (SELECT
    ["foo@example.com", "bar@example.org", "www.example.net"]
    AS addresses),
  UNNEST(addresses) AS email;

+-----------------+----------+
| email           | is_valid |
+-----------------+----------+
| foo@example.com | true     |
| bar@example.org | true     |
| www.example.net | false    |
+-----------------+----------+

# Performs a full match, using ^ and $. Due to regular expression operator
# precedence, it is good practice to use parentheses around everything between ^
# and $.
SELECT
  email,
  REGEXP_CONTAINS(email, r"^([\w.+-]+@foo\.com|[\w.+-]+@bar\.org)$")
    AS valid_email_address,
  REGEXP_CONTAINS(email, r"^[\w.+-]+@foo\.com|[\w.+-]+@bar\.org$")
    AS without_parentheses
FROM
  (SELECT
    ["a@foo.com", "a@foo.computer", "b@bar.org", "!b@bar.org", "c@buz.net"]
    AS addresses),
  UNNEST(addresses) AS email;

+----------------+---------------------+---------------------+
| email          | valid_email_address | without_parentheses |
+----------------+---------------------+---------------------+
| a@foo.com      | true                | true                |
| a@foo.computer | false               | true                |
| b@bar.org      | true                | true                |
| !b@bar.org     | false               | true                |
| c@buz.net      | false               | false               |
+----------------+---------------------+---------------------+
```

### REGEXP_EXTRACT

```
REGEXP_EXTRACT(value, regexp)
```

**Description**

Returns the first substring in `value` that matches the regular expression,
`regexp`. Returns `NULL` if there is no match.

If the regular expression contains a capturing group, the function returns the
substring that is matched by that capturing group. If the expression does not
contain a capturing group, the function returns the entire matching substring.

Returns an error if:

+ The regular expression is invalid
+ The regular expression has more than one capturing group

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH email_addresses AS
  (SELECT "foo@example.com" as email
  UNION ALL
  SELECT "bar@example.org" as email
  UNION ALL
  SELECT "baz@example.net" as email)

SELECT
  REGEXP_EXTRACT(email, r"^[a-zA-Z0-9_.+-]+")
  AS user_name
FROM email_addresses;

+-----------+
| user_name |
+-----------+
| foo       |
| bar       |
| baz       |
+-----------+

WITH email_addresses AS
  (SELECT "foo@example.com" as email
  UNION ALL
  SELECT "bar@example.org" as email
  UNION ALL
  SELECT "baz@example.net" as email)

SELECT
  REGEXP_EXTRACT(email, r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)")
  AS top_level_domain
FROM email_addresses;

+------------------+
| top_level_domain |
+------------------+
| com              |
| org              |
| net              |
+------------------+
```

### REGEXP_EXTRACT_ALL

```
REGEXP_EXTRACT_ALL(value, regexp)
```

**Description**

Returns an array of all substrings of `value` that match the regular expression,
`regexp`.

The `REGEXP_EXTRACT_ALL` function only returns non-overlapping matches. For
example, using this function to extract `ana` from `banana` returns only one
substring, not two.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

An ARRAY of either STRINGs
or BYTES

**Examples**

```sql

WITH code_markdown AS
  (SELECT "Try `function(x)` or `function(y)`" as code)

SELECT
  REGEXP_EXTRACT_ALL(code, "`(.+?)`") AS example
FROM code_markdown;

+----------------------------+
| example                    |
+----------------------------+
| [function(x), function(y)] |
+----------------------------+
```

### REGEXP_MATCH

<p class="caution"><strong>Deprecated.</strong> Use <a href="#regexp_contains">REGEXP_CONTAINS</a>.</p>

```
REGEXP_MATCH(value, regexp)
```

**Description**

Returns TRUE if `value` is a full match for the regular expression, `regexp`.

If the `regexp` argument is invalid, the function returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

BOOL

**Examples**

```sql

WITH email_addresses AS
  (SELECT "foo@example.com" as email
  UNION ALL
  SELECT "bar@example.org" as email
  UNION ALL
  SELECT "notavalidemailaddress" as email)

SELECT
  email,
  REGEXP_MATCH(email,
               r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
               AS valid_email_address
FROM email_addresses;

+-----------------------+---------------------+
| email                 | valid_email_address |
+-----------------------+---------------------+
| foo@example.com       | true                |
| bar@example.org       | true                |
| notavalidemailaddress | false               |
+-----------------------+---------------------+
```

### REGEXP_REPLACE

```
REGEXP_REPLACE(value, regexp, replacement)
```

**Description**

Returns a STRING where all substrings of `value` that
match regular expression `regexp` are replaced with `replacement`.

You can use backslashed-escaped digits (\1 to \9) within the `replacement`
argument to insert text matching the corresponding parenthesized group in the
`regexp` pattern. Use \0 to refer to the entire matching text.

Note: To add a backslash in your regular expression, you must first escape it.
For example, `SELECT REGEXP_REPLACE("abc", "b(.)", "X\\1");` returns `aXc`.

The `REGEXP_REPLACE` function only replaces non-overlapping matches. For
example, replacing `ana` within `banana` results in only one replacement, not
two.

If the `regexp` argument is not a valid regular expression, this function
returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH markdown AS
  (SELECT "# Heading" as heading
  UNION ALL
  SELECT "# Another heading" as heading)

SELECT
  REGEXP_REPLACE(heading, r"^# ([a-zA-Z0-9\s]+$)", "<h1>\\1</h1>")
  AS html
FROM markdown;

+--------------------------+
| html                     |
+--------------------------+
| <h1>Heading</h1>         |
| <h1>Another heading</h1> |
+--------------------------+
```

### REPLACE
```
REPLACE(original_value, from_value, to_value)
```

**Description**

Replaces all occurrences of `from_value` with `to_value` in `original_value`.
If `from_value` is empty, no replacement is made.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH desserts AS
  (SELECT "apple pie" as dessert
  UNION ALL
  SELECT "blackberry pie" as dessert
  UNION ALL
  SELECT "cherry pie" as dessert)

SELECT
  REPLACE (dessert, "pie", "cobbler") as example
FROM desserts;

+--------------------+
| example            |
+--------------------+
| apple cobbler      |
| blackberry cobbler |
| cherry cobbler     |
+--------------------+
```

### REPEAT
```
REPEAT(original_value, repetitions)
```

**Description**

Returns a [value][string-link-to-string-values] that consists of `original_value`, repeated.
The `repetitions` parameter specifies the number of times to repeat
`original_value`. Returns NULL if either `original_value` or `repetitions` are
NULL.

This function return an error if the `repetitions` value is negative.

**Return type**

STRING or BYTES

**Examples**

```sql
SELECT t, n, REPEAT(t, n) AS REPEAT FROM UNNEST([
  STRUCT('abc' AS t, 3 AS n),
  ('例子', 2),
  ('abc', null),
  (null, 3)
]);
```
| t    | n    | REPEAT    |
|------|------|-----------|
| abc  | 3    | abcabcabc |
| 例子 | 2    | 例子例子  |
| abc  | NULL | NULL      |
| NULL | 3    | NULL      |

### REVERSE
```
REVERSE(value)
```

**Description**

Returns the reverse of the input STRING or BYTES.

**Return type**

STRING or BYTES

**Examples**

```sql
WITH example AS (
  SELECT "foo" AS sample_string, b"bar" AS sample_bytes UNION ALL
  SELECT "абвгд" AS sample_string, b"123" AS sample_bytes
)
SELECT
  sample_string,
  REVERSE(sample_string) AS reverse_string,
  sample_bytes,
  REVERSE(sample_bytes) AS reverse_bytes
FROM example;

+---------------+----------------+--------------+---------------+
| sample_string | reverse_string | sample_bytes | reverse_bytes |
+---------------+----------------+--------------+---------------+
| foo           | oof            | bar          | rab           |
| абвгд         | дгвба          | 123          | 321           |
+---------------+----------------+--------------+---------------+
```

### RPAD
```
RPAD(original_value, return_length[, pattern])
```

**Description**

Returns a [value][string-link-to-string-values] that consists of `original_value` appended
with `pattern`. The `return_length` is an INT64 that specifies the length of the
returned value. If `original_value` is BYTES, `return_length` is the number of
bytes. If `original_value` is STRING, `return_length` is the number of
characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `RPAD("hello world", 7);` returns `"hello w"`.

If `original_value`, `return_length`, or `pattern` is NULL, this function
returns NULL.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

STRING or BYTES

**Examples**

```sql
SELECT t, len, FORMAT("%T", RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);
```

| t    | len | RPAD     |
|------|-----|----------|
| abc  | 5   | <code>"abc&nbsp;&nbsp;"</code>  |
| abc  | 2   | <code>"ab"</code>     |
| 例子 | 4   | <code>"例子&nbsp;&nbsp;"</code> |

```sql
SELECT t, len, pattern, FORMAT("%T", RPAD(t, len, pattern)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);
```

| t    | len | pattern | RPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | <code>"abcdefde"</code>    |
| abc  | 5   | -       | <code>"abc--"</code>       |
| 例子 | 5   | 中文    | <code>"例子中文中"</code>  |

```sql
SELECT FORMAT("%T", t) AS t, len, FORMAT("%T", RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);
```

| t               | len | RPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | <code>b"abc&nbsp;&nbsp;"</code>         |
| b"abc"          | 2   | <code>b"ab"</code>             |
| b"\xab\xcd\xef" | 4   | <code>b"\xab\xcd\xef "</code>  |

```sql
SELECT
  FORMAT("%T", t) AS t,
  len,
  FORMAT("%T", pattern) AS pattern,
  FORMAT("%T", RPAD(t, len, pattern)) AS RPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);
```
| t               | len | pattern | RPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | <code>b"abcdefde"</code>             |
| b"abc"          | 5   | b"-"    | <code>b"abc--"</code>                |
| b"\xab\xcd\xef" | 5   | b"\x00" | <code>b"\xab\xcd\xef\x00\x00"</code> |

### RTRIM
```
RTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes trailing characters.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT "***apple***" as item
  UNION ALL
  SELECT "***banana***" as item
  UNION ALL
  SELECT "***orange***" as item)

SELECT
  RTRIM(item, "*") as example
FROM items;

+-----------+
| example   |
+-----------+
| ***apple  |
| ***banana |
| ***orange |
+-----------+

WITH items AS
  (SELECT "applexxx" as item
  UNION ALL
  SELECT "bananayyy" as item
  UNION ALL
  SELECT "orangezzz" as item
  UNION ALL
  SELECT "pearxyz" as item)

SELECT
  RTRIM(item, "xyz") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```

### SAFE_CONVERT_BYTES_TO_STRING

```
SAFE_CONVERT_BYTES_TO_STRING(value)
```

**Description**

Converts a sequence of bytes to a string. Any invalid UTF-8 characters are
replaced with the Unicode replacement character, `U+FFFD`.

**Return type**

STRING

**Examples**

The following statement returns the Unicode replacement character, &#65533;.

```sql
SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\xc2') as safe_convert;
```

### SPLIT

```
SPLIT(value[, delimiter])
```

**Description**

Splits `value` using the `delimiter` argument.

For STRING, the default delimiter is the comma `,`.

For BYTES, you must specify a delimiter.

Splitting on an empty delimiter produces an array of UTF-8 characters for
STRING values, and an array of
BYTES for BYTES values.

Splitting an empty STRING returns an
ARRAY with a single empty
STRING.

**Return type**

ARRAY of type STRING or
ARRAY of type BYTES

**Examples**

```sql

WITH letters AS
  (SELECT "" as letter_group
  UNION ALL
  SELECT "a" as letter_group
  UNION ALL
  SELECT "b c d" as letter_group)

SELECT SPLIT(letter_group, " ") as example
FROM letters;

+----------------------+
| example              |
+----------------------+
| []                   |
| [a]                  |
| [b, c, d]            |
+----------------------+
```

### STARTS_WITH
```
STARTS_WITH(value1, value2)
```

**Description**

Takes two [values][string-link-to-string-values]. Returns TRUE if the second value is a prefix
of the first.

**Return type**

BOOL

**Examples**

```sql

WITH items AS
  (SELECT "foo" as item
  UNION ALL
  SELECT "bar" as item
  UNION ALL
  SELECT "baz" as item)

SELECT
  STARTS_WITH(item, "b") as example
FROM items;

+---------+
| example |
+---------+
|   False |
|    True |
|    True |
+---------+
```

### STRPOS
```
STRPOS(string, substring)
```

**Description**

Returns the 1-based index of the first occurrence of `substring` inside
`string`. Returns `0` if `substring` is not found.

**Return type**

INT64

**Examples**

```sql

WITH email_addresses AS
  (SELECT
    "foo@example.com" AS email_address
  UNION ALL
  SELECT
    "foobar@example.com" AS email_address
  UNION ALL
  SELECT
    "foobarbaz@example.com" AS email_address
  UNION ALL
  SELECT
    "quxexample.com" AS email_address)

SELECT
  STRPOS(email_address, "@") AS example
FROM email_addresses;

+---------+
| example |
+---------+
|       4 |
|       7 |
|      10 |
|       0 |
+---------+
```

### SUBSTR
```
SUBSTR(value, position[, length])
```

**Description**

Returns a substring of the supplied [value][string-link-to-string-values]. The `position`
argument is an integer specifying the starting position of the substring, with
position = 1 indicating the first character or byte. The `length`
argument is the maximum number of characters for
STRING arguments, or bytes for
BYTES arguments.

If `position` is negative, the function counts from the end of `value`,
with -1 indicating the last character.

If `position` is a position off the left end of the
STRING (`position` = 0 or
`position` &lt; `-LENGTH(value)`), the function starts
from position = 1. If `length` exceeds the length of
`value`, returns fewer than `length` characters.

If `length` is less than 0, the function returns an error.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT "apple" as item
  UNION ALL
  SELECT "banana" as item
  UNION ALL
  SELECT "orange" as item)

SELECT
  SUBSTR(item, 2) as example
FROM items;

+---------+
| example |
+---------+
| pple    |
| anana   |
| range   |
+---------+

WITH items AS
  (SELECT "apple" as item
  UNION ALL
  SELECT "banana" as item
  UNION ALL
  SELECT "orange" as item)

SELECT
  SUBSTR(item, 2, 2) as example
FROM items;

+---------+
| example |
+---------+
| pp      |
| an      |
| ra      |
+---------+

WITH items AS
  (SELECT "apple" as item
  UNION ALL
  SELECT "banana" as item
  UNION ALL
  SELECT "orange" as item)

SELECT
  SUBSTR(item, -2) as example
FROM items;

+---------+
| example |
+---------+
| le      |
| na      |
| ge      |
+---------+
```

### TO_BASE32

```
TO_BASE32(bytes_expr)
```

**Description**

Converts a sequence of BYTES into a base32-encoded STRING. To convert a
base32-encoded STRING into BYTES, use [FROM_BASE32][string-link-to-from-base32].

**Return type**

STRING

**Example**

```sql
SELECT TO_BASE32(b'abcde\xFF') AS base32_string;

+------------------+
| base32_string    |
+------------------+
| MFRGGZDF74====== |
+------------------+
```

### TO_BASE64

```
TO_BASE64(bytes_expr)
```

**Description**

Converts a sequence of BYTES into a base64-encoded STRING. To convert a
base64-encoded STRING into BYTES, use [FROM_BASE64][string-link-to-from-base64].

**Return type**

STRING

**Example**

```sql
SELECT TO_BASE64(b'\xde\xad\xbe\xef') AS base64_string;

+---------------+
| base64_string |
+---------------+
| 3q2+7w==      |
+---------------+
```

### TO_CODE_POINTS
```
TO_CODE_POINTS(value)
```

**Description**

Takes a [value](#string_values) and returns an array of INT64.

+ If `value` is a STRING, each element in the returned array represents a
  [code point][string-link-to-code-points-wikipedia]. Each code point falls
  within the range of [0, 0xD7FF] and [0xE000, 0x10FFFF].
+ If `value` is BYTES, each element in the array is an extended ASCII
  character value in the range of [0, 255].

To convert from an array of code points to a STRING or BYTES, see
[CODE_POINTS_TO_STRING][string-link-to-codepoints-to-string] or
[CODE_POINTS_TO_BYTES][string-link-to-codepoints-to-bytes].

**Return type**

ARRAY of INT64

**Examples**

The following example gets the code points for each element in an array of
words.

```sql
SELECT word, TO_CODE_POINTS(word) AS code_points
FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama']) AS word;

+---------+------------------------------------+
| word    | code_points                        |
+---------+------------------------------------+
| foo     | [102, 111, 111]                    |
| bar     | [98, 97, 114]                      |
| baz     | [98, 97, 122]                      |
| giraffe | [103, 105, 114, 97, 102, 102, 101] |
| llama   | [108, 108, 97, 109, 97]            |
+---------+------------------------------------+
```

The following example converts integer representations of BYTES to their
corresponding ASCII character values.

```sql
SELECT word, TO_CODE_POINTS(word) AS bytes_value_as_integer
FROM UNNEST([b'\x00\x01\x10\xff', b'\x66\x6f\x6f']) AS word;

+------------------+------------------------+
| word             | bytes_value_as_integer |
+------------------+------------------------+
| \x00\x01\x10\xff | [0, 1, 16, 255]        |
| foo              | [102, 111, 111]        |
+------------------+------------------------+
```

The following example demonstrates the difference between a BYTES result and a
STRING result.

```sql
SELECT TO_CODE_POINTS(b'Ā') AS b_result, TO_CODE_POINTS('Ā') AS s_result;

+------------+----------+
| b_result   | s_result |
+------------+----------+
| [196, 128] | [256]    |
+------------+----------+
```

Notice that the character, Ā, is represented as a two-byte Unicode sequence. As
a result, the BYTES version of `TO_CODE_POINTS` returns an array with two
elements, while the STRING version returns an array with a single element.

### TO_HEX
```
TO_HEX(bytes)
```

**Description**

Converts a sequence of BYTES into a hexadecimal
STRING. Converts each byte in
the STRING as two hexadecimal characters in the range
`(0..9, a..f)`. To convert a hexadecimal-encoded
STRING
to BYTES, use [FROM_HEX][string-link-to-from-hex].

**Return type**

STRING

**Example**

```sql
WITH Input AS (
  SELECT b'\x00\x01\x02\x03\xAA\xEE\xEF\xFF' AS byte_str UNION ALL
  SELECT b'foobar'
)
SELECT byte_str, TO_HEX(byte_str) AS hex_str
FROM Input;
+----------------------------------+------------------+
| byte_string                      | hex_string       |
+----------------------------------+------------------+
| foobar                           | 666f6f626172     |
| \x00\x01\x02\x03\xaa\xee\xef\xff | 00010203aaeeefff |
+----------------------------------+------------------+
```

### TRIM
```
TRIM(value1[, value2])
```

**Description**

Removes all leading and trailing characters that match `value2`. If
`value2` is not specified, all leading and trailing whitespace characters (as
defined by the Unicode standard) are removed. If the first argument is of type
BYTES, the second argument is required.

If `value2` contains more than one character or byte, the function removes all
leading or trailing characters or bytes contained in `value2`.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT "   apple   " as item
  UNION ALL
  SELECT "   banana   " as item
  UNION ALL
  SELECT "   orange   " as item)

SELECT
  CONCAT("#", TRIM(item), "#") as example
FROM items;

+----------+
| example  |
+----------+
| #apple#  |
| #banana# |
| #orange# |
+----------+

WITH items AS
  (SELECT "***apple***" as item
  UNION ALL
  SELECT "***banana***" as item
  UNION ALL
  SELECT "***orange***" as item)

SELECT
  TRIM(item, "*") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
+---------+

WITH items AS
  (SELECT "xxxapplexxx" as item
  UNION ALL
  SELECT "yyybananayyy" as item
  UNION ALL
  SELECT "zzzorangezzz" as item
  UNION ALL
  SELECT "xyzpearxyz" as item)

SELECT
  TRIM(item, "xyz") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```

### UPPER
```
UPPER(value)
```

**Description**

For STRING arguments, returns the original
string with all alphabetic characters in uppercase.
Mapping between uppercase and lowercase is done according to the
[Unicode Character Database][string-link-to-unicode-character-definitions] without taking into
account language-specific mappings.

For BYTES arguments, the
argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

STRING or BYTES

**Examples**

```sql

WITH items AS
  (SELECT
    "foo" as item
  UNION ALL
  SELECT
    "bar" as item
  UNION ALL
  SELECT
    "baz" as item)

SELECT
  UPPER(item) AS example
FROM items;

+---------+
| example |
+---------+
| FOO     |
| BAR     |
| BAZ     |
+---------+
```

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point
[string-link-to-unicode-character-definitions]: http://unicode.org/ucd/
[string-link-to-normalization-wikipedia]: https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization
[string-link-to-case-folding-wikipedia]: https://en.wikipedia.org/wiki/Letter_case#Case_folding
[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax
[string-link-to-strpos]: #strpos
[string-link-to-string-values]: #string_values
[string-link-to-char-length]: #char_length
[string-link-to-code-points]: #to_code_points
[string-link-to-base64]: #to_base64
[string-link-to-trim]: #trim
[string-link-to-normalize]: #normalize
[string-link-to-from-base64]: #from_base64
[string-link-to-codepoints-to-string]: #code_points_to_string
[string-link-to-codepoints-to-bytes]: #code_points_to_bytes
[string-link-to-base32]: #to_base32
[string-link-to-from-base32]: #from_base32
[string-link-to-from-hex]: #from_hex
[string-link-to-to-hex]: #to_hex

[string-link-to-operators]: https://github.com/google/zetasql/blob/master/docs/operators

