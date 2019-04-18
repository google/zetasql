
<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Lexical Structure and Syntax

A ZetaSQL statement comprises a series of tokens. Tokens include
*identifiers,* *quoted identifiers, literals*, *keywords*, *operators*, and
*special characters*. Tokens can be separated by whitespace (space, backspace,
tab, newline) or comments.

<a id=identifiers></a>
## Identifiers

Identifiers are names that are associated with columns, tables, and other
database objects.

Identifiers must begin with a letter or an underscore.
Subsequent characters can be letters, numbers, or underscores. Quoted
identifiers are identifiers enclosed by backtick (`) characters and can
contain any character, such as spaces or symbols. However, quoted identifiers
cannot be empty. [Reserved Keywords](#reserved_keywords) can only be used as
identifiers if enclosed by backticks.

Syntax (defined here as a regular expression):

<code>[A-Za-z\_][A-Za-z\_0-9]\*</code>

Examples:

```
Customers5
_dataField1
ADGROUP
```

Invalid examples:

```
5Customers
_dataField!
GROUP
```

`5Customers` begins with a number, not a letter or underscore. `_dataField!`
contains the special character "!" which is not a letter, number, or underscore.
`GROUP` is a reserved keyword, and therefore cannot be used as an identifier
without being enclosed by backtick characters.

Both identifiers and quoted identifiers are case insensitive, with some
nuances. See [Case Sensitivity](#case_sensitivity) for further details.

Quoted identifiers have the same escape sequences as string literals,
defined below.

<a id=literals></a>
## Literals

A literal represents a constant value of a built-in data type. Some, but not
all, data types can be expressed as literals.

<a id=string_and_bytes_literals></a>
### String and Bytes Literals

Both string and bytes literals must be *quoted*, either with single (`'`) or
double (`"`) quotation marks, or *triple-quoted* with groups of three single
(`'''`) or three double (`"""`) quotation marks.

**Quoted literals:**

<table>
<thead>
<tr>
<th>Literal</th>
<th>Examples</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Quoted string</td>
<td><ul><li><code>"abc"</code></li><li><code>"it's"</code></li><li><code>'it\'s'</code></li><li><code>'Title: "Boy"'</code></li></ul></td>
<td>Quoted strings enclosed by single (<code>'</code>) quotes can contain unescaped double (<code>"</code>) quotes, and vice versa. <br>Backslashes (<code>\</code>) introduce escape sequences. See Escape Sequences table below.<br>Quoted strings cannot contain newlines, even when preceded by a backslash (<code>\</code>).</td>
</tr>
<tr>
<td>Triple-quoted string</td>
<td><ul><li><code>"""abc"""</code></li><li><code>'''it's'''</code></li><li><code>'''Title:"Boy"'''</code></li><li><code>'''two<br>lines'''</code></li><li><code>'''why\?'''</code></li></ul></td>
<td>Embedded newlines and quotes are allowed without escaping - see fourth example.<br>Backslashes (<code>\</code>) introduce escape sequences. See Escape Sequences table below.<br>A trailing unescaped backslash (<code>\</code>) at the end of a line is not allowed.<br>Three unescaped quotes in a row which match the starting quotes will end the string.</td>
</tr>
<tr>
<td>Raw string</td>
<td><ul><li><code>R"abc+"</code></li><li> <code>r'''abc+'''</code></li><li> <code>R"""abc+"""</code></li><li><code>r'f\(abc,(.*),def\)'</code></li></ul></td>
<td>Quoted or triple-quoted literals that have the raw string literal prefix (<code>r</code> or <code>R</code>) are interpreted as raw/regex strings.<br>Backslash characters (<code>\</code>) do not act as escape characters. If a backslash followed by another character occurs inside the string literal, both characters are preserved.<br>A raw string cannot end with an odd number of backslashes.<br>Raw strings are useful for constructing regular expressions.</td>
</tr>
</tbody>
</table>

Prefix characters (`r`, `R`, `b`, `B)` are optional for quoted or triple-quoted strings, and indicate that the string is a raw/regex string or a byte sequence, respectively. For
example, `b'abc'` and `b'''abc'''` are both interpreted as type bytes. Prefix characters are case insensitive.

**Quoted literals with prefixes:**

<table>
<thead>
<tr>
<th>Literal</th>
<th>Example</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Bytes</td>
<td><ul><li><code>B"abc"</code></li><li><code>B'''abc'''</code></li><li><code>b"""abc"""</code></li></ul></td>
<td>Quoted or triple-quoted literals that have the bytes literal prefix (<code>b</code> or <code>B</code>) are interpreted as bytes.</td>
</tr>
<tr>
<td>Raw bytes</td>
<td><ul><li><code>br'abc+'</code></li><li><code>RB"abc+"</code></li><li><code>RB'''abc'''</code></li></ul></td>
<td>The <code>r</code> and <code>b</code> prefixes can be combined in any order. For example, <code>rb'abc*'</code> is equivalent to <code>br'abc*'</code>.</td>
</tr>
</tbody>
</table>

The table below lists all valid escape sequences for representing non-alphanumeric characters in string and byte literals.
Any sequence not in this table produces an error.

<table>
<thead>
<tr>
<th>Escape Sequence</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>\a</code></td>
<td>Bell</td>
</tr>
<tr>
<td><code>\b</code></td>
<td>Backspace</td>
</tr>
<tr>
<td><code>\f</code></td>
<td>Formfeed</td>
</tr>
<tr>
<td><code>\n</code></td>
<td>Newline</td>
</tr>
<tr>
<td><code>\r</code></td>
<td>Carriage Return</td>
</tr>
<tr>
<td><code>\t</code></td>
<td>Tab</td>
</tr>
<tr>
<td><code>\v</code></td>
<td>Vertical Tab</td>
</tr>
<tr>
<td><code>\\</code></td>
<td>Backslash (<code>\</code>)</td>
</tr>
<tr>
<td><code>\?</code></td>
<td>Question Mark (<code>?</code>)</td>
</tr>
<tr>
<td><code>\"</code></td>
<td>Double Quote (<code>"</code>)</td>
</tr>
<tr>
<td><code>\'</code></td>
<td>Single Quote (<code>'</code>)</td>
</tr>
<tr>
<td><code>\`</code></td>
<td>Backtick (<code>`</code>)</td>
</tr>
<tr>
<td><code>\ooo</code></td>
<td>Octal escape, with exactly three digits (in the range 0-7). Decodes to a single Unicode character (in string literals) or byte (in bytes literals).</td>
</tr>
<tr>
<td><code>\xhh</code> or <code>\Xhh</code></td>
<td>Hex escape, with exactly two hex digits (0-9 or A-F or a-f). Decodes to a single Unicode character (in string literals) or byte (in bytes literals). Examples:<ul style="list-style-type:none"><li><code>'\x41'</code> == <code>'A'</code></li><li><code>'\x41B'</code> is <code>'AB'</code></li><li><code>'\x4'</code> is an error</li></ul></td>
</tr>
<tr>
<td><code>\uhhhh</code></td>
<td>Unicode escape, with lowercase 'u' and exactly four hex digits. Valid only in string literals or identifiers.<br/>Note that the range D800-DFFF is not allowed, as these are surrogate unicode values.</td>
</tr>
<tr>
<td><code>\Uhhhhhhhh</code></td>
<td>Unicode escape, with uppercase 'U' and exactly eight hex digits. Valid only in string literals or identifiers.<br/>Note that the range D800-DFFF is not allowed, as these are surrogate unicode values. Also, values greater than 10FFFF are not allowed.</td>
</tr>
</tbody>
</table>

<a id=integer_literals></a>
### Integer Literals

Integer literals are either a sequence of decimal digits (0 through
9) or a hexadecimal value that is prefixed with "`0x`". Integers can be prefixed
by "`+`" or "`-`" to represent positive and negative values, respectively.

Examples:

```
123
0xABC
-123
```

An integer literal is interpreted as an `INT64`.

Coercion (implicit casting) of integer literals to other integer types can occur
if casting does not result in truncation. For example, if the integer 55 of type
`INT32` is compared to the integer literal 77, the
literal value 77 is coerced into type `INT32` because
`77` can be represented by the `INT32` type.

### NUMERIC Literals

You can construct NUMERIC literals using the
`NUMERIC` keyword followed by a floating point value in quotes.

Examples:

```
SELECT NUMERIC '0';
SELECT NUMERIC '123456';
SELECT NUMERIC '-3.14';
SELECT NUMERIC '-0.54321';
SELECT NUMERIC '1.23456e05';
SELECT NUMERIC '-9.876e-3';
```

<a id=floating_point_literals></a>
### Floating Point Literals

Syntax options:

```
[+-]DIGITS.[DIGITS][e[+-]DIGITS]
[DIGITS].DIGITS[e[+-]DIGITS]
DIGITSe[+-]DIGITS
```

`DIGITS` represents one or more decimal numbers (0 through 9) and `e` represents the exponent marker (e or E).

Examples:

```
123.456e-67
.1E4
58.
4e2
```

Numeric literals that contain
either a decimal point or an exponent marker are presumed to be type double.

Implicit coercion of floating point literals to float type is possible if the
value is within the valid float range.

There is no literal
representation of NaN or infinity, but the following case-insensitive strings
can be explicitly cast to float:

 + "NaN"
 + "inf" or "+inf"
 + "-inf"

<a id=array_literals></a>
### Array Literals

Array literals are a comma-separated lists of elements
enclosed in square brackets. The `ARRAY` keyword is optional, and an explicit
element type T is also optional.

You can write an empty array of a specific type using `ARRAY<type>[]`. You can
also write an untyped empty array using `[]`, in which case ZetaSQL
attempts to infer the array type from the surrounding context. If
ZetaSQL cannot infer a type, the default type `ARRAY<INT64>` is used.

Examples:

```
[1, 2, 3]
['x', 'y', 'xy']
ARRAY[1, 2, 3]
ARRAY<string>['x', 'y', 'xy']
ARRAY<int64>[]
[]
```

### Struct Literals

Syntax:

```
(elem[, elem...])
```

where `elem` is an element in the struct. `elem` must be a literal data type, not an expression or column name.

The output type is an anonymous struct type (structs are not named types) with anonymous fields with types
matching the types of the input expressions.

<table>
<thead>
<tr>
<th>Example</th>
<th>Output Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>(1, 2, 3)</code></td>
<td><code>STRUCT&lt;int64,int64,int64&gt;</code></td>
</tr>
<tr>
<td><code>(1, 'abc')</code></td>
<td><code>STRUCT&lt;int64,string&gt;</code></td>
</tr>
</tbody>
</table>

### Date Literals

Syntax:

```
DATE 'YYYY-M[M]-D[D]'
```

Date literals contain the `DATE` keyword followed by a string literal that conforms to the canonical date format, enclosed in single quotation marks. Date literals support a range between the
years 1 and 9999, inclusive. Dates outside of this range are invalid.

For example, the following date literal represents September 27, 2014:

```
DATE '2014-09-27'
```

String literals in canonical date format also implicitly coerce to DATE type
when used where a DATE-type expression is expected. For example, in the query

```
SELECT * FROM foo WHERE date_col = "2014-09-27"
```

the string literal `"2014-09-27"` will be coerced to a date literal.

 

### Time Literals
Syntax:

```
TIME '[H]H:[M]M:[S]S[.DDDDDD]]'
```

TIME literals contain the `TIME` keyword and a string literal that conforms to
the canonical time format, enclosed in single quotation marks.

For example, the following time represents 12:30 p.m.:

```
TIME '12:30:00.45'
```

### DATETIME Literals
Syntax:

```
DATETIME 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD]]'
```

DATETIME literals contain the `DATETIME` keyword and a string literal that
conforms to the canonical DATETIME format, enclosed in single quotation marks.

For example, the following DATETIME represents 12:30 p.m. on September 27,
2014:

```
DATETIME '2014-09-27 12:30:00.45'
```

DATETIME literals support a range between the years 1 and 9999, inclusive.
DATETIMEs outside of this range are invalid.

String literals with the canonical DATETIME format implicitly coerce to a
DATETIME literal when used where a DATETIME expression is expected.

For example:

```
SELECT * FROM foo
WHERE datetime_col = "2014-09-27 12:30:00.45"
```

In this query, the string literal `"2014-09-27 12:30:00.45"` is coerced to a
DATETIME literal.

### Timestamp literals

Syntax:

```
TIMESTAMP 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD]] [timezone]'
```

Timestamp literals contain the `TIMESTAMP` keyword and a string literal that
conforms to the canonical timestamp format, enclosed in single quotation marks.

Timestamp literals support a range between the years 1 and 9999, inclusive.
Timestamps outside of this range are invalid.

A timestamp literal can include a numerical suffix to indicate the timezone:

```
TIMESTAMP '2014-09-27 12:30:00.45-08'
```

If this suffix is absent, the default timezone, which is implementation defined, is used.

For example, the following timestamp represents 12:30 p.m. on September 27,
2014, using the timezone, which is implementation defined:

```
TIMESTAMP '2014-09-27 12:30:00.45'
```

For more information on timezones, see [Timezone](#timezone).

String literals with the canonical timestamp format, including those with
timezone names, implicitly coerce to a timestamp literal when used where a
timestamp expression is expected.  For example, in the following query, the
string literal `"2014-09-27 12:30:00.45 America/Los_Angeles"` is coerced
to a timestamp literal.

```
SELECT * FROM foo
WHERE timestamp_col = "2014-09-27 12:30:00.45 America/Los_Angeles"
```

#### Timezone

Since timestamp literals must be mapped to a specific point in time, a timezone
is necessary to correctly interpret a literal. If a timezone is not specified
as part of the literal itself, then the default timezone value, which is set by
the ZetaSQL implementation, is used.

Timezones are represented by strings in the following canonical format, which
represents the offset from Coordinated Universal Time (UTC).

Format:

```
(+|-)H[H][:M[M]]
```

Examples:

```
'-08:00'
'-8:15'
'+3:00'
'+07:30'
'-7'
```

Timezones can also be expressed using string timezone names from the
[tz database](http://www.iana.org/time-zones). For a less comprehensive but
simpler reference, see the
[List of tz database timezones](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
on Wikipedia. Canonical timezone names have the format
`<continent/[region/]city>`, such as `America/Los_Angeles`.

Note that not all timezone names are interchangeable even if they do happen to
report the same time during a given part of the year. For example, `America/Los_Angeles` reports the same time as `UTC-7:00` during Daylight Savings Time, but reports the same time as `UTC-8:00` outside of Daylight Savings Time.

Example:

```
TIMESTAMP '2014-09-27 12:30:00 America/Los_Angeles'
TIMESTAMP '2014-09-27 12:30:00 America/Argentina/Buenos_Aires'
```

<a id=enum_literals></a>
### Enum Literals

There is no syntax for enum literals, but integer or string literals will coerce to enum type when necessary, or can be explicitly CAST to a specific enum type name. See "Literal Coercion" in [Expressions, Functions, and Operators](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md).

<a id=case_sensitivity></a>
## Case Sensitivity

ZetaSQL follows these rules for case sensitivity:

<table>
<thead>
<tr>
<th>Category</th>
<th>Case Sensitive?</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td>Keywords</td>
<td>No</td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Function names</td>
<td>No</td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Table names</td>

<td>See Notes</td>
<td>Table names are usually case insensitive, but may be case sensitive when querying a database that uses case sensitive table names.</td>

</tr>
<tr>
<td>Column names</td>
<td>No</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>All type names except for protocol buffer type names</td>
<td>No</td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Protocol buffer type names</td>
<td>Yes</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>String values</td>
<td>Yes</td>
<td>Includes enum value strings</td>
</tr>
<tr>
<td>String comparisons</td>
<td>Yes</td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Aliases within a query</td>
<td>No</td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Regular expression matching</td>
<td>See Notes</td>
<td>Regular expression matching is case sensitive by default, unless the expression itself specifies that it should be case insensitive.</td>
</tr>
<tr>
<td><code>LIKE</code> matching</td>
<td>Yes</td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

<a id=reserved_keywords></a>
## Reserved Keywords

Keywords are a group of tokens that have special meaning in the ZetaSQL
language, and  have the following characteristics:

 + Keywords cannot be used as identifiers unless enclosed by backtick (`) characters.
 + Keywords are case insensitive.

ZetaSQL has the following reserved keywords.

<table style="table-layout: fixed; width: 110%">
<tbody>
<tr>
<td>
ALL<br />
AND<br />
ANY<br />
ARRAY<br />
AS<br />
ASC<br />
ASSERT_ROWS_MODIFIED<br />
AT<br />
BETWEEN<br />
BY<br />
CASE<br />
CAST<br />
COLLATE<br />
CONTAINS<br />
CREATE<br />
CROSS<br />
CUBE<br />
CURRENT<br />
DEFAULT<br />
DEFINE<br />
DESC<br />
DISTINCT<br />
ELSE<br />
END<br />
</td>
<td>
ENUM<br />
ESCAPE<br />
EXCEPT<br />
EXCLUDE<br />
EXISTS<br />
EXTRACT<br />
FALSE<br />
FETCH<br />
FOLLOWING<br />
FOR<br />
FROM<br />
FULL<br />
GROUP<br />
GROUPING<br />
GROUPS<br />
HASH<br />
HAVING<br />
IF<br />
IGNORE<br />
IN<br />
INNER<br />
INTERSECT<br />
INTERVAL<br />
INTO<br />
</td>
<td>
IS<br />
JOIN<br />
LATERAL<br />
LEFT<br />
LIKE<br />
LIMIT<br />
LOOKUP<br />
MERGE<br />
NATURAL<br />
NEW<br />
NO<br />
NOT<br />
NULL<br />
NULLS<br />
OF<br />
ON<br />
OR<br />
ORDER<br />
OUTER<br />
OVER<br />
PARTITION<br />
PRECEDING<br />
PROTO<br />
RANGE<br />
</td>
<td>
RECURSIVE<br />
RESPECT<br />
RIGHT<br />
ROLLUP<br />
ROWS<br />
SELECT<br />
SET<br />
SOME<br />
STRUCT<br />
TABLESAMPLE<br />
THEN<br />
TO<br />
TREAT<br />
TRUE<br />
UNBOUNDED<br />
UNION<br />
UNNEST<br />
USING<br />
WHEN<br />
WHERE<br />
WINDOW<br />
WITH<br />
WITHIN<br />
</td>
</tr>
</tbody>
</table>

<a id=terminating_semicolons></a>
## Terminating Semicolons

Statements can optionally use a terminating semicolon (`;`) in the context of a query string submitted through an Application Programming Interface (API). Some interactive tools require statements to have a terminating semicolon.
In a request containing multiple statements, statements must be separated by semicolons, but the semicolon is optional for the final statement.

<a id=query_parameters></a>
## Query Parameters

Syntax:

```
@param
```

Query parameters are denoted using identifiers preceded by the @ character. The user defines the parameter outside of the query statement.

Query parameters can be used in substitution of arbitrary expressions. They
cannot, however, be used in substitution of identifiers, column names, table
names, or other parts of the query itself.

Client APIs allow the binding of parameter names to values; the parameter will be
substituted with the bound value at execution time. Some client APIs allow queries with parameters that are not bound to specific values,
but the parameter type must be defined at query analysis time.

Example:

```
SELECT * FROM Roster WHERE LastName = @myparam
```

returns all rows where `LastName` is equal to the value of query parameter `myparam`.

<a id=hints></a>
## Hints

Syntax:

```
@{ [engine_name.]hint_name = value, ... }
```

Hint syntax requires the @ character followed by curly braces. The optional `engine_name.` prefix allows for multiple engines to define hints with the same `hint_name`.

Query hints suggest different engine-specific execution
strategies. Query hints generally do not affect query semantics, but may have
performance implications. Different engines may support different hints.

## Comments

Comments are sequences of characters that are ignored by the parser. ZetaSQL
supports the following types of comments.

### Single line comments

Single line comments are supported by prepending `#` or `--` before the
comment.

**Examples**

`SELECT x FROM T; # x is a field and T is a table`

Comment includes all characters from the '#' character to the end of the line.

`SELECT x FROM T; --x is a field and T is a table`

Comment includes all characters from the '`--`' sequence to the end of the line. You can optionally add a space after the '`--`'.

### Multiline comments

Multiline comments are supported by enclosing the comment using `/* <comment> */`.

**Example:**

```
SELECT x FROM T /* x is a field and T is a table */
WHERE x = 3;
```

**Invalid example:**

```
SELECT x FROM T /* comment starts here
                /* comment ends on this line */
                this line is not considered a comment */
WHERE x = 3;
```

Comment includes all characters, including newlines, enclosed by the first
occurrence of '`/*`' and the first subsequent occurrence of '`*/`'. Nested
comments are not supported. The second example contains a nested comment that
renders the query invalid.

<!-- END CONTENT -->

