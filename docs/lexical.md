
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Lexical Structure and Syntax

<!-- BEGIN CONTENT -->
A ZetaSQL statement comprises a series of tokens. Tokens include
*identifiers*, *quoted identifiers*, *literals*, *keywords*, *operators*, and
*special characters*. You can separate tokens with whitespace (for example, space, backspace,
tab, newline) or comments.

<a id=identifiers></a>
## Identifiers

Identifiers are names that are associated with columns, tables, and other
database objects. They can be unquoted or quoted.

+  Identifiers can be used in path expressions that return a
   `STRUCT`
   or `PROTO`.
+  Some identifiers are case-sensitive and some are not.
   For details, see [Case Sensitivity][case-sensitivity].
+  Unquoted identifiers must begin with a letter or an underscore character.
   Subsequent characters can be letters, numbers, or underscores.
+  Quoted identifiers must be enclosed by backtick (`) characters.
   +  Quoted identifiers can contain any character, such as spaces or symbols.
   +  Quoted identifiers cannot be empty.
   +  Quoted identifiers have the same escape sequences as
      [string literals][string-literals].
   +  A [reserved keyword](#reserved_keywords) must be a quoted identifier
      if it is a standalone keyword or the first component of a path expression.
      It may be unquoted as the second or later component of a
      path expression.
+  Table name identifiers have additional syntax to support dashes (-)
   when referenced in `FROM` and `TABLE` clauses.

**Examples**

These are valid identifiers:

```
Customers5
`5Customers`
dataField
_dataField1
ADGROUP
`tableName~`
`GROUP`
```

These path expressions contain valid identifiers:

```
foo.`GROUP`
foo.GROUP
foo().dataField
(foo).dataField
list[OFFSET(3)].dataField
list[ORDINAL(3)].dataField
@parameter.dataField
```

These are invalid identifiers:

```
5Customers
_dataField!
GROUP
```

`5Customers` begins with a number, not a letter or underscore. `_dataField!`
contains the special character "!" which is not a letter, number, or underscore.
`GROUP` is a reserved keyword, and therefore cannot be used as an identifier
without being enclosed by backtick characters.

You do not need to enclose table names that include dashes
with backticks. These are equivalent:

```sql
SELECT * FROM data-customers-287.mydatabase.mytable
```

```sql
SELECT * FROM `data-customers-287`.mydatabase.mytable
```

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
<td>Quoted strings enclosed by single (<code>'</code>) quotes can contain unescaped double (<code>"</code>) quotes, as well as the inverse. <br>Backslashes (<code>\</code>) introduce escape sequences. See the Escape Sequences table below.<br>Quoted strings cannot contain newlines, even when preceded by a backslash (<code>\</code>).</td>
</tr>
<tr>
<td>Triple-quoted string</td>
<td><ul><li><code>"""abc"""</code></li><li><code>'''it's'''</code></li><li><code>'''Title:"Boy"'''</code></li><li><code>'''two<br>lines'''</code></li><li><code>'''why\?'''</code></li></ul></td>
<td>Embedded newlines and quotes are allowed without escaping - see fourth example.<br>Backslashes (<code>\</code>) introduce escape sequences. See Escape Sequences table below.<br>A trailing unescaped backslash (<code>\</code>) at the end of a line is not allowed.<br>End the string with three unescaped quotes in a row that match the starting quotes.</td>
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
<td>Octal escape, with exactly 3 digits (in the range 0–7). Decodes to a single Unicode character (in string literals) or byte (in bytes literals).</td>
</tr>
<tr>
<td><code>\xhh</code> or <code>\Xhh</code></td>
<td>Hex escape, with exactly 2 hex digits (0–9 or A–F or a–f). Decodes to a single Unicode character (in string literals) or byte (in bytes literals). Examples:<ul style="list-style-type:none"><li><code>'\x41'</code> == <code>'A'</code></li><li><code>'\x41B'</code> is <code>'AB'</code></li><li><code>'\x4'</code> is an error</li></ul></td>
</tr>
<tr>
<td><code>\uhhhh</code></td>
<td>Unicode escape, with lowercase 'u' and exactly 4 hex digits. Valid only in string literals or identifiers.<br/>Note that the range D800-DFFF is not allowed, as these are surrogate unicode values.</td>
</tr>
<tr>
<td><code>\Uhhhhhhhh</code></td>
<td>Unicode escape, with uppercase 'U' and exactly 8 hex digits. Valid only in string literals or identifiers.<br/>The range D800-DFFF is not allowed, as these values are surrogate unicode values. Also, values greater than 10FFFF are not allowed.</td>
</tr>
</tbody>
</table>

<a id=integer_literals></a>
### Integer Literals

Integer literals are either a sequence of decimal digits (0–9) or a hexadecimal
value that is prefixed with "`0x`" or "`0X`". Integers can be prefixed by "`+`"
or "`-`" to represent positive and negative values, respectively.
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

```sql
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

```sql
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

Array literals are comma-separated lists of elements
enclosed in square brackets. The `ARRAY` keyword is optional, and an explicit
element type T is also optional.

You can write an empty array of a specific type using `ARRAY<type>[]`. You can
also write an untyped empty array using `[]`, in which case ZetaSQL
attempts to infer the array type from the surrounding context. If
ZetaSQL cannot infer a type, the default type `ARRAY<INT64>` is used.

Examples:

```sql
[1, 2, 3]
['x', 'y', 'xy']
ARRAY[1, 2, 3]
ARRAY<string>['x', 'y', 'xy']
ARRAY<int64>[]
[]
```

### Struct Literals

Syntax:

```sql
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

```sql
DATE 'YYYY-M[M]-D[D]'
```

Date literals contain the `DATE` keyword followed by a string literal that conforms to the canonical date format, enclosed in single quotation marks. Date literals support a range between the
years 1 and 9999, inclusive. Dates outside of this range are invalid.

For example, the following date literal represents September 27, 2014:

```sql
DATE '2014-09-27'
```

String literals in canonical date format also implicitly coerce to DATE type
when used where a DATE-type expression is expected. For example, in the query

```sql
SELECT * FROM foo WHERE date_col = "2014-09-27"
```

the string literal `"2014-09-27"` will be coerced to a date literal.

 

### Time Literals
Syntax:

```sql
TIME '[H]H:[M]M:[S]S[.DDDDDD]]'
```

Time literals contain the `TIME` keyword and a string literal that conforms to
the canonical time format, enclosed in single quotation marks.

For example, the following time represents 12:30 p.m.:

```sql
TIME '12:30:00.45'
```

### Datetime Literals
Syntax:

```sql
DATETIME 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD]]'
```

Datetime literals contain the `DATETIME` keyword and a string literal that
conforms to the canonical datetime format, enclosed in single quotation marks.

For example, the following datetime represents 12:30 p.m. on September 27,
2014:

```sql
DATETIME '2014-09-27 12:30:00.45'
```

Datetime literals support a range between the years 1 and 9999, inclusive.
Datetimes outside of this range are invalid.

String literals with the canonical datetime format implicitly coerce to a
datetime literal when used where a datetime expression is expected.

For example:

```sql
SELECT * FROM foo
WHERE datetime_col = "2014-09-27 12:30:00.45"
```

In the query above, the string literal `"2014-09-27 12:30:00.45"` is coerced to
a datetime literal.

A datetime literal can also include the optional character `T` or `t`. This
is a flag for time and is used as a separator between the date and time. If
you use this character, a space can't be included before or after it.
These are valid:

```sql
DATETIME '2014-09-27T12:30:00.45'
DATETIME '2014-09-27t12:30:00.45'
```

### Timestamp literals

Syntax:

```sql
TIMESTAMP 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD] [timezone]]`
```

Timestamp literals contain the `TIMESTAMP` keyword and a string literal that
conforms to the canonical timestamp format, enclosed in single quotation marks.

Timestamp literals support a range between the years 1 and 9999, inclusive.
Timestamps outside of this range are invalid.

A timestamp literal can include a numerical suffix to indicate the time zone:

```sql
TIMESTAMP '2014-09-27 12:30:00.45-08'
```

If this suffix is absent, the default time zone,
which is implementation defined, is used.

For example, the following timestamp represents 12:30 p.m. on September 27,
2014, using the which is implementation defined time zone:

```sql
TIMESTAMP '2014-09-27 12:30:00.45'
```

For more information about time zones, see [Time zone][time-zone].

String literals with the canonical timestamp format, including those with
time zone names, implicitly coerce to a timestamp literal when used where a
timestamp expression is expected.  For example, in the following query, the
string literal `"2014-09-27 12:30:00.45 America/Los_Angeles"` is coerced
to a timestamp literal.

```sql
SELECT * FROM foo
WHERE timestamp_col = "2014-09-27 12:30:00.45 America/Los_Angeles"
```

A timestamp literal can include these optional characters:

+  `T` or `t`: A flag for time. Use as a separator between the date and time.
+  `Z` or `z`: A flag for the default timezone. This cannot be used with
   `[timezone]`.

If you use one of these characters, a space can't be included before or after it.
These are valid:

```sql
TIMESTAMP '2017-01-18T12:34:56.123456Z'
TIMESTAMP '2017-01-18t12:34:56.123456'
TIMESTAMP '2017-01-18 12:34:56.123456z'
TIMESTAMP '2017-01-18 12:34:56.123456Z'
```

<a id=timezone></a>
#### Time zone

Since timestamp literals must be mapped to a specific point in time, a time zone
is necessary to correctly interpret a literal. If a time zone is not specified
as part of the literal itself, then ZetaSQL uses the default time zone
value, which the ZetaSQL implementation sets.

ZetaSQL represents time zones using strings in the following canonical
format, which represents the offset from Coordinated Universal Time (UTC).

Format:

```sql
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

Time zones can also be expressed using string time zone names from the
[tz database][tz-database]{: class=external target=_blank }. For a less
comprehensive but simpler reference, see the
[List of tz database time zones][tz-database-time-zones]{: class=external target=_blank }
on Wikipedia. Canonical time zone names have the format
`<continent/[region/]city>`, such as `America/Los_Angeles`.

Note: Not all time zone names are interchangeable even if they do happen to
report the same time during a given part of the year. For example, `America/Los_Angeles` reports the same time as `UTC-7:00` during Daylight Savings Time, but reports the same time as `UTC-8:00` outside of Daylight Savings Time.

Example:

```sql
TIMESTAMP '2014-09-27 12:30:00 America/Los_Angeles'
TIMESTAMP '2014-09-27 12:30:00 America/Argentina/Buenos_Aires'
```

<a id=enum_literals></a>
### Enum Literals

There is no syntax for enum literals, but integer or string literals will coerce to enum type when necessary, or can be explicitly CAST to a specific enum type name. See "Literal Coercion" in [Expressions, Functions, and Operators][functions-reference].

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
<td>Table names are usually case insensitive, but may be case sensitive when querying a database that uses case-sensitive table names.</td>

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

You can optionally use a terminating semicolon (`;`) when you submit a query
string statement through an Application Programming Interface (API).

In a request containing multiple statements, you must separate statements with
semicolons, but the semicolon is generally optional after the final statement.
Some interactive tools require statements to have a terminating semicolon.

<a id=trailing_commas></a>
## Trailing Commas

You can optionally use a trailing comma (`,`) at the end of a list in a `SELECT`
statement.

**Example**

```
SELECT name, release_date, FROM Books
```

<a id=query_parameters></a>
## Query Parameters

You can use query parameters to substitute arbitrary expressions.
However, query parameters cannot be used to substitute identifiers,
column names, table names, or other parts of the query itself.
Query parameters are defined outside of the query statement.

Client APIs allow the binding of parameter names to values; the query engine
substitutes a bound value for a parameter at execution time.

Query parameters cannot be used in the SQL body of these statements:
`CREATE FUNCTION`, `CREATE TABLE FUNCTION`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, and `CREATE PROCEDURE`.

### Named Query Parameters

Syntax:

```sql
@parameter_name
```

A named query parameter is denoted using an identifier preceded by the
`@` character. Named query parameters cannot
be used alongside [positional query parameters][positional-query-parameters].

**Example:**

This example returns all rows where `LastName` is equal to the value of the
named query parameter `myparam`.

```sql
SELECT * FROM Roster WHERE LastName = @myparam
```

### Positional Query Parameters

Positional query parameters are denoted using the `?` character.
Positional parameters are evaluated by the order in which they are passed in.
Positional query parameters cannot be used
alongside [named query parameters][named-query-parameters].

**Example:**

This query returns all rows where `LastName` and `FirstName` are equal to the
values passed into this query. The order in which these values are passed in
matters. If the last name is passed in first, followed by the first name, the
expected results will not be returned.

```sql
SELECT * FROM Roster WHERE FirstName = ? and LastName = ?
```

<a id=hints></a>
## Hints

```sql
@{ hint [, ...] }

hint:
  [engine_name.]hint_name = value
```

The purpose of a hint is to modify the execution strategy for a query
without changing the result of the query. Hints generally do not affect query
semantics, but may have performance implications.

Hint syntax requires the `@` character followed by curly braces.
You can create one hint or a group of hints. The optional `engine_name.`
prefix allows for multiple engines to define hints with the same `hint_name`.
This is important if you need to suggest different engine-specific
execution strategies or different engines support different hints.

You can assign [identifiers][lexical-identifiers] and
[literals][lexical-literals] to hints.

+  Identifiers are useful for hints that are meant to act like enums.
   You can use an identifier to avoid using a quoted string.
   In the resolved AST, identifier hints are represented as string literals,
   so `@{hint="abc"}` is the same as `@{hint=abc}`. Identifier hints can also
   be used for hints that take a table name or column
   name as a single identifier.
+  NULL literals are allowed and are inferred as integers.

Hints are meant to apply only to the node they are attached to,
and not to a larger scope.

**Examples**

In this example, a literal is assigned to a hint. This hint is only used
with two database engines called `database_engine_a` and `database_engine_b`.
The value for the hint is different for each database engine.

```sql
@{ database_engine_a.file_count=23, database_engine_b.file_count=10 }
```

## Comments

Comments are sequences of characters that the parser ignores.
ZetaSQL supports the following types of comments.

<a name="single-line-comments"></a>
### Single-line comments

Use a single-line comment if you want the comment to appear on a line by itself.

**Examples**

```sql
# this is a single-line comment
SELECT book FROM library;
```

```sql
-- this is a single-line comment
SELECT book FROM library;
```

```sql
/* this is a single-line comment */
SELECT book FROM library;
```

```sql
SELECT book FROM library
/* this is a single-line comment */
WHERE book = "Ulysses";
```

### Inline comments

Use an inline comment if you want the comment to appear on the same line as
a statement. A comment that is prepended with `#` or `--` must appear to the
right of a statement.

**Examples**

```sql
SELECT book FROM library; # this is an inline comment
```

```sql
SELECT book FROM library; -- this is an inline comment
```

```sql
SELECT book FROM library; /* this is an inline comment */
```

```sql
SELECT book FROM library /* this is an inline comment */ WHERE book = "Ulysses";
```

### Multiline comments

Use a multiline comment if you need the comment to span multiple lines.
Nested multiline comments are not supported.

**Examples**

```sql
SELECT book FROM library
/*
  This is a multiline comment
  on multiple lines
*/
WHERE book = "Ulysses";
```

```sql
SELECT book FROM library
/* this is a multiline comment
on two lines */
WHERE book = "Ulysses";
```

[tz-database]: http://www.iana.org/time-zones
[tz-database-time-zones]: http://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[lexical-identifiers]: #identifiers
[lexical-literals]: #literals
[case-sensitivity]: #case_sensitivity
[time-zone]: #timezone
[string-literals]: #string_and_bytes_literals
[named-query-parameters]: #named_query_parameters
[positional-query-parameters]: #positional_query_parameters
[query-reference]: https://github.com/google/zetasql/blob/master/docs/query-syntax
[lexical-udfs-reference]: https://github.com/google/zetasql/blob/master/docs/user-defined-functions

[functions-reference]: https://github.com/google/zetasql/blob/master/docs/functions-reference

<!-- END CONTENT -->

