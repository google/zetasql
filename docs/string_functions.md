

# String functions

These string functions work on two different values:
`STRING` and `BYTES` data types. `STRING` values must be well-formed UTF-8.

Functions that return position values, such as [STRPOS][string-link-to-strpos],
encode those positions as `INT64`. The value `1`
refers to the first character (or byte), `2` refers to the second, and so on.
The value `0` indicates an invalid index. When working on `STRING` types, the
returned positions refer to character positions.

All string comparisons are done byte-by-byte, without regard to Unicode
canonical equivalence.

### ASCII

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

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| 97    | 97    | 0     | NULL  |
+-------+-------+-------+-------+
```

### BYTE_LENGTH

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

+------------+----------------+-------+---------------+
| characters | string_example | bytes | bytes_example |
+------------+----------------+-------+---------------+
| абвгд      | 10             | абвгд | 10            |
+------------+----------------+-------+---------------+
```

### CHAR_LENGTH

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

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+
```

### CHARACTER_LENGTH

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

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+
```

### CHR

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

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| A     | ÿ     | ȁ     | Ѐ     |
+-------+-------+-------+-------+
```

```sql
SELECT CHR(97) AS A, CHR(0xF9B5) AS B, CHR(0) AS C, CHR(NULL) AS D;

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| a     | 例    |       | NULL  |
+-------+-------+-------+-------+
```

### CODE_POINTS_TO_BYTES

```sql
CODE_POINTS_TO_BYTES(ascii_values)
```

**Description**

Takes an array of extended ASCII
[code points][string-link-to-code-points-wikipedia]
(`ARRAY` of `INT64`) and returns `BYTES`.

To convert from `BYTES` to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`BYTES`

**Examples**

The following is a basic example using `CODE_POINTS_TO_BYTES`.

```sql
SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]) AS bytes;

+----------+
| bytes    |
+----------+
| AbCd     |
+----------+
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

+------------------+
| encoded_string   |
+------------------+
| Grfg Fgevat!     |
+------------------+
```

### CODE_POINTS_TO_STRING

```sql
CODE_POINTS_TO_STRING(value)
```

**Description**

Takes an array of Unicode [code points][string-link-to-code-points-wikipedia]
(`ARRAY` of `INT64`) and
returns a `STRING`. If a code point is 0, does not return a character for it
in the `STRING`.

To convert from a string to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`STRING`

**Examples**

The following are basic examples using `CODE_POINTS_TO_STRING`.

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024]) AS string;

+--------+
| string |
+--------+
| AÿȁЀ   |
+--------+
```

```sql
SELECT CODE_POINTS_TO_STRING([97, 0, 0xF9B5]) AS string;

+--------+
| string |
+--------+
| a例    |
+--------+
```

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, NULL, 1024]) AS string;

+--------+
| string |
+--------+
| NULL   |
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

+---------------------+
| author              |
+---------------------+
| T.P. Bar            |
+---------------------+
```

```sql
SELECT CONCAT('Summer', ' ', 1923) as release_date;

+---------------------+
| release_date        |
+---------------------+
| Summer 1923         |
+---------------------+
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

+---------------------+
| full_name           |
+---------------------+
| John Doe            |
| Jane Smith          |
| Joe Jackson         |
+---------------------+
```

### ENDS_WITH

```sql
ENDS_WITH(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if the second
value is a suffix of the first.

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

+---------+
| example |
+---------+
|    True |
|   False |
|    True |
+---------+
```

### FORMAT 
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

### FROM_BASE32

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

+-----------+
| byte_data |
+-----------+
| abcde\xff |
+-----------+
```

### FROM_BASE64

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
See [RFC 4648](https://tools.ietf.org/html/rfc4648#section-4) for details. This
function expects the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`BYTES`

**Example**

```sql
SELECT FROM_BASE64('/+A=') AS byte_data;

+------------+
| byte_data |
+-----------+
| \377\340  |
+-----------+
```

To work with an encoding using a different base64 alphabet, you might need to
compose `FROM_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To decode a
`base64url`-encoded string, replace `-` and `_` with `+` and `/` respectively.

```sql
SELECT FROM_BASE64(REPLACE(REPLACE('_-A=', '-', '+'), '_', '/')) AS binary;

+-----------+
| binary    |
+-----------+
| \377\340  |
+-----------+
```

### FROM_HEX

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

+------------------+----------------------------------+
| hex_str          | bytes_str                        |
+------------------+----------------------------------+
| 0AF              | \x00\xaf                         |
| 00010203aaeeefff | \x00\x01\x02\x03\xaa\xee\xef\xff |
| 666f6f626172     | foobar                           |
+------------------+----------------------------------+
```

### INITCAP

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

+-------------------------------+-------------------------------+
| value                         | initcap_value                 |
+-------------------------------+-------------------------------+
| Hello World-everyone!         | Hello World-Everyone!         |
| tHe dog BARKS loudly+friendly | The Dog Barks Loudly+Friendly |
| apples&oranges;&pears         | Apples&Oranges;&Pears         |
| καθίσματα ταινιών             | Καθίσματα Ταινιών             |
+-------------------------------+-------------------------------+

WITH example AS
(
  SELECT 'hello WORLD!' AS value, '' AS delimiters UNION ALL
  SELECT 'καθίσματα ταιντιώ@ν' AS value, 'τ@' AS delimiters UNION ALL
  SELECT 'Apples1oranges2pears' AS value, '12' AS delimiters UNION ALL
  SELECT 'tHisEisEaESentence' AS value, 'E' AS delimiters
)
SELECT value, delimiters, INITCAP(value, delimiters) AS initcap_value FROM example;

+----------------------+------------+----------------------+
| value                | delimiters | initcap_value        |
+----------------------+------------+----------------------+
| hello WORLD!         |            | Hello world!         |
| καθίσματα ταιντιώ@ν  | τ@         | ΚαθίσματΑ τΑιντΙώ@Ν  |
| Apples1oranges2pears | 12         | Apples1Oranges2Pears |
| tHisEisEaESentence   | E          | ThisEIsEAESentence   |
+----------------------+------------+----------------------+
```

### INSTR

```sql
INSTR(source_value, search_value[, position[, occurrence]])
```

**Description**

Returns the lowest 1-based index of `search_value` in `source_value`. 0 is
returned when no match is found. `source_value` and `search_value` must be the
same type, either `STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`source_value`, otherwise it starts at the beginning of `source_value`. If
`position` is negative, the function searches backwards from the end of
`source_value`, with -1 indicating the last character. `position` cannot be 0.

If `occurrence` is specified, the search returns the position of a specific
instance of `search_value` in `source_value`, otherwise it returns the index of
the first occurrence. If `occurrence` is greater than the number of matches
found, 0 is returned. For `occurrence` > 1, the function searches for
overlapping occurrences, in other words, the function searches for additional
occurrences beginning with the second character in the previous occurrence.
`occurrence` cannot be 0 or negative.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
(SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 2 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 3 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, -1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, -3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'ann' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 2 as
occurrence
)
SELECT source_value, search_value, position, occurrence, INSTR(source_value,
search_value, position, occurrence) AS instr
FROM example;

+--------------+--------------+----------+------------+-------+
| source_value | search_value | position | occurrence | instr |
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
+--------------+--------------+----------+------------+-------+
```

### LEFT

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

+---------+--------------+
| example | left_example |
+---------+--------------+
| apple   | app          |
| banana  | ban          |
| абвгд   | абв          |
+---------+--------------+
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

+----------------------+--------------+
| example              | left_example |
+----------------------+--------------+
| apple                | app          |
| banana               | ban          |
| \xab\xcd\xef\xaa\xbb | \xab\xcd\xef |
+----------------------+--------------+
```

### LENGTH

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

+------------+----------------+---------------+
| characters | string_example | bytes_example |
+------------+----------------+---------------+
| абвгд      |              5 |            10 |
+------------+----------------+---------------+
```

### LPAD

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

+------+-----+----------+
| t    | len | LPAD     |
|------|-----|----------|
| abc  | 5   | "  abc"  |
| abc  | 2   | "ab"     |
| 例子  | 4   | "  例子" |
+------+-----+----------+
```

```sql
SELECT t, len, pattern, FORMAT('%T', LPAD(t, len, pattern)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

+------+-----+---------+--------------+
| t    | len | pattern | LPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | "defdeabc"   |
| abc  | 5   | -       | "--abc"      |
| 例子  | 5   | 中文    | "中文中例子"   |
+------+-----+---------+--------------+
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

+-----------------+-----+------------------+
| t               | len | LPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | b"  abc"         |
| b"abc"          | 2   | b"ab"            |
| b"\xab\xcd\xef" | 4   | b" \xab\xcd\xef" |
+-----------------+-----+------------------+
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

+-----------------+-----+---------+-------------------------+
| t               | len | pattern | LPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | b"defdeabc"             |
| b"abc"          | 5   | b"-"    | b"--abc"                |
| b"\xab\xcd\xef" | 5   | b"\x00" | b"\x00\x00\xab\xcd\xef" |
+-----------------+-----+---------+-------------------------+
```

### LOWER

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

+---------+
| example |
+---------+
| foo     |
| bar     |
| baz     |
+---------+
```

### LTRIM

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

+-------------+
| example     |
+-------------+
| #apple   #  |
| #banana   # |
| #orange   # |
+-------------+
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

+-----------+
| example   |
+-----------+
| apple***  |
| banana*** |
| orange*** |
+-----------+
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

```sql
NORMALIZE_AND_CASEFOLD(value[, normalization_mode])
```

**Description**

Takes a string value and returns it as a normalized string with
normalization.

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

+--------------+--------------+------------+------------------------------+
| a            | b            | normalized | normalized_with_case_folding |
+--------------+--------------+------------+------------------------------+
| The red barn | The Red Barn | false      | true                         |
+--------------+--------------+------------+------------------------------+
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

+---+----+-------+-------+------+------+
| a | b  | nfd   | nfc   | nkfd | nkfc |
+---+----+-------+-------+------+------+
| Ⅸ | IX | false | false | true | true |
| Å | Å  | true  | true  | true | true |
+---+----+-------+-------+------+------+
```

### OCTET_LENGTH

```sql
OCTET_LENGTH(value)
```

Alias for [`BYTE_LENGTH`](#byte-length).

### REGEXP_CONTAINS

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

+-----------------+----------+
| email           | is_valid |
+-----------------+----------+
| foo@example.com | true     |
| bar@example.org | true     |
| www.example.net | false    |
+-----------------+----------+

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

```sql
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

+-----------+
| user_name |
+-----------+
| foo       |
| bar       |
| baz       |
+-----------+
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

+------------------+
| top_level_domain |
+------------------+
| com              |
| org              |
| net              |
+------------------+
```

### REGEXP_EXTRACT_ALL

```sql
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

An `ARRAY` of either `STRING`s or `BYTES`

**Examples**

```sql
WITH code_markdown AS
  (SELECT 'Try `function(x)` or `function(y)`' as code)

SELECT
  REGEXP_EXTRACT_ALL(code, '`(.+?)`') AS example
FROM code_markdown;

+----------------------------+
| example                    |
+----------------------------+
| [function(x), function(y)] |
+----------------------------+
```

### REGEXP_INSTR

```sql
REGEXP_INSTR(source_value, regexp [, position[, occurrence, [occurrence_position]]])
```

**Description**

Returns the lowest 1-based index of a regular expression, `regexp`, in
`source_value`. Returns `0` when no match is found or the regular expression
is empty. Returns an error if the regular expression is invalid or has more than
one capturing group. `source_value` and `regexp` must be the same type, either
`STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`source_value`, otherwise it starts at the beginning of `source_value`. If
`position` is negative, the function searches backwards from the end of
`source_value`, with -1 indicating the last character. `position` cannot be 0.

If `occurrence` is specified, the search returns the position of a specific
instance of `regexp` in `source_value`, otherwise it returns the index of
the first occurrence. If `occurrence` is greater than the number of matches
found, 0 is returned. For `occurrence` > 1, the function searches for
overlapping occurrences, in other words, the function searches for additional
occurrences beginning with the second character in the previous occurrence.
`occurrence` cannot be 0 or negative.

You can optionally use `occurrence_position` to specify where a position
in relation to an `occurrence` starts. Your choices are:
+  `0`: Returns the beginning position of the occurrence.
+  `1`: Returns the first position following the end of the occurrence. If the
   end of the occurrence is also the end of the input, one off the
   end of the occurrence is returned. For example, length of a string + 1.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS (
  SELECT 'ab@gmail.com' AS source_value, '@[^.]*' AS regexp UNION ALL
  SELECT 'ab@mail.com', '@[^.]*' UNION ALL
  SELECT 'abc@gmail.com', '@[^.]*' UNION ALL
  SELECT 'abc.com', '@[^.]*')
SELECT source_value, regexp, REGEXP_INSTR(source_value, regexp) AS instr
FROM example;

+---------------+--------+-------+
| source_value  | regexp | instr |
+---------------+--------+-------+
| ab@gmail.com  | @[^.]* | 3     |
| ab@mail.com   | @[^.]* | 3     |
| abc@gmail.com | @[^.]* | 4     |
| abc.com       | @[^.]* | 0     |
+---------------+--------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com' AS source_value, '@[^.]*' AS regexp, 1 AS position UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 3 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 4)
SELECT
  source_value, regexp, position,
  REGEXP_INSTR(source_value, regexp, position) AS instr
FROM example;

+-------------------------+--------+----------+-------+
| source_value            | regexp | position | instr |
+-------------------------+--------+----------+-------+
| a@gmail.com b@gmail.com | @[^.]* | 1        | 2     |
| a@gmail.com b@gmail.com | @[^.]* | 2        | 2     |
| a@gmail.com b@gmail.com | @[^.]* | 3        | 14    |
| a@gmail.com b@gmail.com | @[^.]* | 4        | 14    |
+-------------------------+--------+----------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com c@gmail.com' AS source_value,
         '@[^.]*' AS regexp, 1 AS position, 1 AS occurrence UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 3)
SELECT
  source_value, regexp, position, occurrence,
  REGEXP_INSTR(source_value, regexp, position, occurrence) AS instr
FROM example;

+-------------------------------------+--------+----------+------------+-------+
| source_value                        | regexp | position | occurrence | instr |
+-------------------------------------+--------+----------+------------+-------+
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 1          | 2     |
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 2          | 14    |
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 3          | 26    |
+-------------------------------------+--------+----------+------------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com' AS source_value, '@[^.]*' AS regexp,
         1 AS position, 1 AS occurrence, 0 AS o_position UNION ALL
  SELECT 'a@gmail.com', '@[^.]*', 1, 1, 1)
SELECT
  source_value, regexp, position, occurrence, o_position,
  REGEXP_INSTR(source_value, regexp, position, occurrence, o_position) AS instr
FROM example;

+--------------+--------+----------+------------+------------+-------+
| source_value | regexp | position | occurrence | o_position | instr |
+--------------+--------+----------+------------+------------+-------+
| a@gmail.com  | @[^.]* | 1        | 1          | 0          | 2     |
| a@gmail.com  | @[^.]* | 1        | 1          | 1          | 8     |
+--------------+--------+----------+------------+------------+-------+
```

### REGEXP_MATCH

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

+-----------------------+---------------------+
| email                 | valid_email_address |
+-----------------------+---------------------+
| foo@example.com       | true                |
| bar@example.org       | true                |
| notavalidemailaddress | false               |
+-----------------------+---------------------+
```

### REGEXP_REPLACE

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

+--------------------------+
| html                     |
+--------------------------+
| <h1>Heading</h1>         |
| <h1>Another heading</h1> |
+--------------------------+
```

### REPLACE

```sql
REPLACE(original_value, from_value, to_value)
```

**Description**

Replaces all occurrences of `from_value` with `to_value` in `original_value`.
If `from_value` is empty, no replacement is made.

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

+--------------------+
| example            |
+--------------------+
| apple cobbler      |
| blackberry cobbler |
| cherry cobbler     |
+--------------------+
```

### REPEAT

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

+------+------+-----------+
| t    | n    | REPEAT    |
|------|------|-----------|
| abc  | 3    | abcabcabc |
| 例子 | 2    | 例子例子  |
| abc  | NULL | NULL      |
| NULL | 3    | NULL      |
+------+------+-----------+
```

### REVERSE

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

+---------------+----------------+--------------+---------------+
| sample_string | reverse_string | sample_bytes | reverse_bytes |
+---------------+----------------+--------------+---------------+
| foo           | oof            | bar          | rab           |
| абвгд         | дгвба          | 123          | 321           |
+---------------+----------------+--------------+---------------+
```

### RIGHT

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

+---------+---------------+
| example | right_example |
+---------+---------------+
| apple   | ple           |
| banana  | ana           |
| абвгд   | вгд           |
+---------+---------------+
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

+----------------------+---------------+
| example              | right_example |
+----------------------+---------------+
| apple                | ple           |
| banana               | ana           |
| \xab\xcd\xef\xaa\xbb | \xef\xaa\xbb  |
+----------------------+---------------+
```

### RPAD

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

+------+-----+----------+
| t    | len | RPAD     |
|------|-----|----------|
| abc  | 5   | "abc  "  |
| abc  | 2   | "ab"     |
| 例子  | 4   | "例子  " |
+------+-----+----------+
```

```sql
SELECT t, len, pattern, FORMAT('%T', RPAD(t, len, pattern)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

+------+-----+---------+--------------+
| t    | len | pattern | RPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | "abcdefde"   |
| abc  | 5   | -       | "abc--"      |
| 例子  | 5   | 中文     | "例子中文中"  |
+------+-----+---------+--------------+
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

+-----------------+-----+------------------+
| t               | len | RPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | b"abc  "         |
| b"abc"          | 2   | b"ab"            |
| b"\xab\xcd\xef" | 4   | b"\xab\xcd\xef " |
+-----------------+-----+------------------+
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

+-----------------+-----+---------+-------------------------+
| t               | len | pattern | RPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | b"abcdefde"             |
| b"abc"          | 5   | b"-"    | b"abc--"                |
| b"\xab\xcd\xef" | 5   | b"\x00" | b"\xab\xcd\xef\x00\x00" |
+-----------------+-----+---------+-------------------------+
```

### RTRIM

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

+-----------+
| example   |
+-----------+
| ***apple  |
| ***banana |
| ***orange |
+-----------+
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

### SOUNDEX

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

+----------------------+---------+
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
+----------------------+---------+
```

### SPLIT

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

**Return type**

`ARRAY` of type `STRING` or
`ARRAY` of type `BYTES`

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

+----------------------+
| example              |
+----------------------+
| []                   |
| [a]                  |
| [b, c, d]            |
+----------------------+
```

### STARTS_WITH

```sql
STARTS_WITH(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if the second value is a
prefix of the first.

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

+---------+
| example |
+---------+
|   False |
|    True |
|    True |
+---------+
```

### STRPOS

```sql
STRPOS(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns the 1-based index of the first
occurrence of `value2` inside `value1`. Returns `0` if `value2` is not found.

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

```sql
SUBSTR(value, position[, length])
```

**Description**

Returns a substring of the supplied `STRING` or `BYTES` value. The
`position` argument is an integer specifying the starting position of the
substring, with position = 1 indicating the first character or byte. The
`length` argument is the maximum number of characters for `STRING` arguments,
or bytes for `BYTES` arguments.

If `position` is negative, the function counts from the end of `value`,
with -1 indicating the last character.

If `position` is a position off the left end of the
`STRING` (`position` = 0 or `position` &lt; `-LENGTH(value)`), the function
starts from position = 1. If `length` exceeds the length of `value`, the
function returns fewer than `length` characters.

If `length` is less than 0, the function returns an error.

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

+---------+
| example |
+---------+
| pple    |
| anana   |
| range   |
+---------+
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

+---------+
| example |
+---------+
| pp      |
| an      |
| ra      |
+---------+
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

+---------+
| example |
+---------+
| le      |
| na      |
| ge      |
+---------+
```

### SUBSTRING

```sql
SUBSTRING(value, position[, length])
```

Alias for [`SUBSTR`](#substr).

### TO_BASE32

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

+------------------+
| base32_string    |
+------------------+
| MFRGGZDF74====== |
+------------------+
```

### TO_BASE64

```sql
TO_BASE64(bytes_expr)
```

**Description**

Converts a sequence of `BYTES` into a base64-encoded `STRING`. To convert a
base64-encoded `STRING` into `BYTES`, use [FROM_BASE64][string-link-to-from-base64].

There are several base64 encodings in common use that vary in exactly which
alphabet of 65 ASCII characters are used to encode the 64 digits and padding.
See [RFC 4648](https://tools.ietf.org/html/rfc4648#section-4) for details. This
function adds padding and uses the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`STRING`

**Example**

```sql
SELECT TO_BASE64(b'\377\340') AS base64_string;

+---------------+
| base64_string |
+---------------+
| /+A=          |
+---------------+
```

To work with an encoding using a different base64 alphabet, you might need to
compose `TO_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To encode a
`base64url`-encoded string, replace `+` and `/` with `-` and `_` respectively.

```sql
SELECT REPLACE(REPLACE(TO_BASE64(b'\377\340'), '+', '-'), '/', '_') as websafe_base64;

+----------------+
| websafe_base64 |
+----------------+
| _-A=           |
+----------------+
```

### TO_CODE_POINTS

```sql
TO_CODE_POINTS(value)
```

**Description**

Takes a [value](#string_values) and returns an array of
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

`ARRAY` of `INT64`

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

The following example converts integer representations of `BYTES` to their
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

The following example demonstrates the difference between a `BYTES` result and a
`STRING` result.

```sql
SELECT TO_CODE_POINTS(b'Ā') AS b_result, TO_CODE_POINTS('Ā') AS s_result;

+------------+----------+
| b_result   | s_result |
+------------+----------+
| [196, 128] | [256]    |
+------------+----------+
```

Notice that the character, Ā, is represented as a two-byte Unicode sequence. As
a result, the `BYTES` version of `TO_CODE_POINTS` returns an array with two
elements, while the `STRING` version returns an array with a single element.

### TO_HEX

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

+----------------------------------+------------------+
| byte_string                      | hex_string       |
+----------------------------------+------------------+
| \x00\x01\x02\x03\xaa\xee\xef\xff | 00010203aaeeefff |
| foobar                           | 666f6f626172     |
+----------------------------------+------------------+
```

### TRANSLATE

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

+------------------+-------------------+-------------------+------------------+
| expression       | source_characters | target_characters | translate        |
+------------------+-------------------+-------------------+------------------+
| This is a cookie | sco               | zku               | Thiz iz a kuukie |
| A coaster        | co                | k                 | A kaster         |
+------------------+-------------------+-------------------+------------------+
```

### TRIM

```sql
TRIM(value1[, value2])
```

**Description**

Removes all leading and trailing characters that match `value2`. If
`value2` is not specified, all leading and trailing whitespace characters (as
defined by the Unicode standard) are removed. If the first argument is of type
`BYTES`, the second argument is required.

If `value2` contains more than one character or byte, the function removes all
leading or trailing characters or bytes contained in `value2`.

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
  CONCAT('#', TRIM(item), '#') as example
FROM items;

+----------+
| example  |
+----------+
| #apple#  |
| #banana# |
| #orange# |
+----------+
```

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

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
+---------+
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

SELECT
  TRIM(item, 'xyz') as example
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

### UNICODE

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

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| 226   | 226   | 0     | NULL  |
+-------+-------+-------+-------+
```

### UPPER

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

+---------+
| example |
+---------+
| FOO     |
| BAR     |
| BAZ     |
+---------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-unicode-character-definitions]: http://unicode.org/ucd/

[string-link-to-normalization-wikipedia]: https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization

[string-link-to-case-folding-wikipedia]: https://en.wikipedia.org/wiki/Letter_case#Case_folding

[string-link-to-soundex-wikipedia]: https://en.wikipedia.org/wiki/Soundex

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

[string-code-point]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-strpos]: #strpos

[string-link-to-char-length]: #char_length

[string-link-to-code-points]: #to_code_points

[string-link-to-base64]: #to_base64

[string-link-to-trim]: #trim

[string-link-to-normalize]: #normalize

[string-link-to-normalize-casefold]: #normalize_and_casefold

[string-link-to-from-base64]: #from_base64

[string-link-to-codepoints-to-string]: #code_points_to_string

[string-link-to-codepoints-to-bytes]: #code_points_to_bytes

[string-link-to-base32]: #to_base32

[string-link-to-from-base32]: #from_base32

[string-link-to-from-hex]: #from_hex

[string-link-to-to-hex]: #to_hex

[string-link-to-lexical-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#string_and_bytes_literals

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

[string-link-to-operators]: https://github.com/google/zetasql/blob/master/docs/operators.md

<!-- mdlint on -->

