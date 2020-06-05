

# ZetaSQL Documentation (One-Page)

<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

<!-- Product Overview -->

<!-- Quickstarts -->

<!-- Concepts -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Lexical Structure and Syntax

<!-- BEGIN CONTENT -->
A ZetaSQL statement comprises a series of tokens. Tokens include
*identifiers*, *quoted identifiers*, *literals*, *keywords*, *operators*, and
*special characters*. You can separate tokens with whitespace (for example, space, backspace,
tab, newline) or comments.

<a id=identifiers></a>
### Identifiers

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
### Literals

A literal represents a constant value of a built-in data type. Some, but not
all, data types can be expressed as literals.

<a id=string_and_bytes_literals></a>
#### String and Bytes Literals

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
#### Integer Literals

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

#### NUMERIC Literals

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
#### Floating Point Literals

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
#### Array Literals

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

#### Struct Literals

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

#### Date Literals

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

 

#### Time Literals
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

#### Datetime Literals
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

#### Timestamp literals

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
##### Time zone

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
#### Enum Literals

There is no syntax for enum literals, but integer or string literals will coerce to enum type when necessary, or can be explicitly CAST to a specific enum type name. See "Literal Coercion" in [Expressions, Functions, and Operators][functions-reference].

<a id=case_sensitivity></a>
### Case Sensitivity

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
### Reserved Keywords

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
### Terminating Semicolons

You can optionally use a terminating semicolon (`;`) when you submit a query
string statement through an Application Programming Interface (API).

In a request containing multiple statements, you must separate statements with
semicolons, but the semicolon is generally optional after the final statement.
Some interactive tools require statements to have a terminating semicolon.

<a id=trailing_commas></a>
### Trailing Commas

You can optionally use a trailing comma (`,`) at the end of a list in a `SELECT`
statement.

**Example**

```
SELECT name, release_date, FROM Books
```

<a id=query_parameters></a>
### Query Parameters

You can use query parameters to substitute arbitrary expressions.
However, query parameters cannot be used to substitute identifiers,
column names, table names, or other parts of the query itself.
Query parameters are defined outside of the query statement.

Client APIs allow the binding of parameter names to values; the query engine
substitutes a bound value for a parameter at execution time.

Query parameters cannot be used in the SQL body of these statements:
`CREATE FUNCTION`, `CREATE TABLE FUNCTION`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, and `CREATE PROCEDURE`.

#### Named Query Parameters

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

#### Positional Query Parameters

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
### Hints

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

### Comments

Comments are sequences of characters that the parser ignores.
ZetaSQL supports the following types of comments.

<a name="single-line-comments"></a>
#### Single-line comments

Use a single-line comment if you want the comment to appear on a line by itself.

**Examples**

```sql
## this is a single-line comment
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

#### Inline comments

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

#### Multiline comments

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

[functions-reference]: #function-reference
[query-reference]: #query-syntax
[lexical-udfs-reference]: #user-defined-functions

<!-- END CONTENT -->

## Conversion rules

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

#### Casting

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
  <code>-inf</code> will return an error. Casting a value outside the range of
  <a href="#numeric_type"><code>NUMERIC</code></a>
  will return an overflow error.
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
maximum precision or range of the
<a href="#numeric_type"><code>NUMERIC</code></a>
type, or an error will occur. If the number of digits
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

##### Safe casting

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

##### Casting hex strings to integers

If you are working with hex strings (`0x123`), you can cast those strings as
integers:

```sql
SELECT '0x123' as hex_value, CAST('0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| 0x123     | 291        |
+-----------+------------+

SELECT '-0x123' as hex_value, CAST('-0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| -0x123    | -291       |
+-----------+------------+
```

##### Casting date types

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

##### Casting timestamp types

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

##### Casting between date and timestamp types

ZetaSQL supports casting between date and timestamp types as follows:

```
CAST(date_expression AS TIMESTAMP)
CAST(timestamp_expression AS DATE)
```

Casting from a date to a timestamp interprets `date_expression` as of midnight
(start of the day) in the default time zone, which is implementation defined. Casting
from a timestamp to date effectively truncates the timestamp as of the default
time zone.

##### Bit casting

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

#### Coercion

ZetaSQL coerces the result type of an expression to another type if
needed to match function signatures.  For example, if function func() is defined
to take a single argument of type INT64  and an
 expression is used as an argument that has a result type of
DOUBLE, then the result of the expression will be
coerced to INT64 type before func() is computed.

##### Literal coercion

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

##### Parameter coercion

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
#### Additional conversion functions

ZetaSQL provides the following additional conversion functions:

+ [DATE functions][con-rules-link-to-date-functions]
+ [DATETIME functions][con-rules-link-to-datetime-functions]
+ [TIME functions][con-rules-link-to-time-functions]
+ [TIMESTAMP functions][con-rules-link-to-timestamp-functions]

[con-rules-link-to-literal-coercion]: #literal_coercion
[con-rules-link-to-parameter-coercion]: #parameter_coercion
[con-rules-link-to-time-zones]: https://github.com/google/zetasql/blob/master/docs/data-types#time_zones

[con-rules-link-to-time-zones]: #time-zones
[con-rules-link-to-safe-convert-bytes-to-string]: #safe_convert_bytes_to_string
[con-rules-link-to-date-functions]: #date_functions
[con-rules-link-to-datetime-functions]: #datetime_functions
[con-rules-link-to-time-functions]: #time_functions
[con-rules-link-to-timestamp-functions]: #timestamp_functions

## Operators

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
<td>Array position. Must be used with OFFSET or ORDINAL&mdash;see

<a href="#array_functions">

Array Functions
</a>
.</td>
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
<td>&nbsp;</td>
<td>||</td>
<td>STRING, BYTES, or ARRAY&#60;T&#62;</td>
<td>Concatenation operator</td>
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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

<a href="#data_types">

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

#### Element access operators

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
<td>position_keyword is either OFFSET or ORDINAL. See

<a href="#array_functions">

Array Functions
</a>

for the two functions that use this operator.</td>
</tr>
</tbody>
</table>

#### Arithmetic operators

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

#### Bitwise operators
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

#### Logical operators

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
<td style="white-space:nowrap">NOT X</td>
<td>Returns FALSE if input is TRUE. Returns TRUE if input is FALSE. Returns <code>NULL</code>
otherwise.</td>
</tr>
<tr>
<td>Logical AND</td>
<td style="white-space:nowrap">X AND Y</td>
<td>Returns FALSE if at least one input is FALSE. Returns TRUE if both X and Y
are TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
<tr>
<td>Logical OR</td>
<td style="white-space:nowrap">X OR Y</td>
<td>Returns FALSE if both X and Y are FALSE. Returns TRUE if at least one input
is TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
</tbody>
</table>

#### Comparison operators

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
required. For example, <code>r"\%"</code>.</li>
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

#### IN operators

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

See the [Arrays][operators-link-to-filtering-arrays] topic for more information on
how to use this syntax.

When using the `IN` operator, the following semantics apply:

+ `IN` with an empty right side expression is always FALSE
+ `IN` with a `NULL` left side expression and a non-empty right side expression is
  always `NULL`
+ `IN` with a `NULL` in the `IN`-list can only return TRUE or `NULL`, never FALSE
+ `NULL IN (NULL)` returns `NULL`
+ `IN UNNEST(<NULL array>)` returns FALSE (not `NULL`)
+ `NOT IN` with a `NULL` in the `IN`-list can only return FALSE or `NULL`, never
   TRUE

`IN` can be used with multi-part keys by using the struct constructor syntax.
For example:

```
(Key1, Key2) IN ( (12,34), (56,78) )
(Key1, Key2) IN ( SELECT (table.a, table.b) FROM table )
```

See the [Struct Type][operators-link-to-struct-type] section of the Data Types topic for more
information on this syntax.

#### IS operators

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

#### Concatenation operator

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

[operators-link-to-filtering-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering-arrays
[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types
[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax#from_clause
[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types#struct_type

[operators-link-to-filtering-arrays]: #filtering-arrays
[operators-link-to-data-types]: #data-types
[operators-link-to-from-clause]: #from-clause
[operators-link-to-struct-type]: #struct-type
[operators-link-to-math-functions]: #mathematical_functions
[link-to-coercion]: #coercion

## Conditional expressions

Conditional expressions impose constraints on the evaluation order of their
inputs. In essence, they are evaluated left to right, with short-circuiting, and
only evaluate the output value that was chosen. In contrast, all inputs to
regular functions are evaluated before calling the function. Short-circuiting in
conditional expressions can be exploited for error handling or performance
tuning.

<table>
<thead>
<tr>
<th>Syntax</th>
<th>Input Data Types</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
  <td><pre>CASE expr
  WHEN value THEN result
  [WHEN ...]
  [ELSE else_result]
  END</pre></td>
<td><code>expr</code> and <code>value</code>: Any type</td>
<td><code>result</code> and <code>else_result</code>: Supertype of input
types.</td>
<td>Compares <code>expr</code> to value of each successive <code>WHEN</code>
clause and returns the first result where this comparison returns true. The
remaining <code>WHEN</code> clauses and <code>else_result</code> are not
evaluated. If the
<code>expr = value</code> comparison returns false or <code>NULL</code> for
all <code>WHEN</code> clauses, returns
<code>else_result</code> if present; if not present, returns <code>NULL</code>.
<code>expr</code> and <code>value</code> expressions
must be implicitly coercible to a common supertype; equality comparisons are
done on coerced values. <code>result</code> and <code>else_result</code>
expressions must be coercible to a common supertype.</td>
</tr>

<tr>
  <td><pre>CASE
  WHEN cond1 THEN result
  [WHEN cond2...]
  [ELSE else_result]
  END</pre></td>
<td><code>cond</code>: BOOL</td>
<td><code>result</code> and <code>else_result</code>: Supertype of input
types.</td>
<td>Evaluates condition <code>cond</code> of each successive <code>WHEN</code>
clause and returns the first result where the condition is true; any remaining
<code>WHEN</code> clauses and <code>else_result</code> are not evaluated. If all
conditions are false or <code>NULL</code>, returns
<code>else_result</code> if present; if not present, returns
<code>NULL</code>. <code>result</code> and <code>else_result</code>
expressions must be implicitly coercible to a common supertype. </td>
</tr>

<tr>
<td><a id="coalesce"></a>COALESCE(expr1, ..., exprN)</td>
<td>Any type</td>
<td>Supertype of input types</td>
<td>Returns the value of the first non-null expression. The remaining
expressions are not evaluated. All input expressions must be implicitly
coercible to a common supertype.</td>
</tr>
<tr>
<td><a id="if"></a>IF(cond, true_result, else_result)</td>
<td><code>cond</code>: BOOL</td>
<td><code>true_result</code> and <code>else_result</code>: Any type.</td>
<td>If <code>cond</code> is true, returns <code>true_result</code>, else returns
<code>else_result</code>. <code>else_result</code> is not evaluated if
<code>cond</code> is true. <code>true_result</code> is not evaluated if
<code>cond</code> is false or <code>NULL</code>. <code>true_result</code> and
<code>else_result</code> must be coercible to a common supertype.</td>
</tr>
<tr>
<td><a id="ifnull"></a>IFNULL(expr, null_result)</td>
<td>Any type</td>
<td>Any type or supertype of input types.</td>
<td>If <code>expr</code> is <code>NULL</code>, return <code>null_result</code>. Otherwise,
return <code>expr</code>. If <code>expr</code> is not <code>NULL</code>,
<code>null_result</code> is not evaluated. <code>expr</code> and
<code>null_result</code> must be implicitly coercible to a common
supertype. Synonym for <code>COALESCE(expr, null_result)</code>.</td>
</tr>
<tr>
<td><a id="nullif"></a>NULLIF(expression, expression_to_match)</td>
<td>Any type T or subtype of T</td>
<td>Any type T or subtype of T</td>
<td>Returns <code>NULL</code> if <code>expression = expression_to_match</code>
is true, otherwise returns <code>expression</code>. <code>expression</code> and
<code>expression_to_match</code> must be implicitly coercible to a common
supertype; equality comparison is done on coerced values.</td>
</tr>
</tbody>
</table>

## Expression subqueries

There are four types of expression subqueries, i.e. subqueries that are used as
expressions.  Expression subqueries return `NULL` or a single value, as opposed to
a column or table, and must be surrounded by parentheses. For a fuller
discussion of subqueries, see
[Subqueries][exp-sub-link-to-subqueries].

<table>
<thead>
<tr>
<th>Type of Subquery</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Scalar</td>
<td>Any type T</td>
<td>A subquery in parentheses inside an expression (e.g. in the
<code>SELECT</code> list or <code>WHERE</code> clause) is interpreted as a
scalar subquery. The <code>SELECT</code> list in a scalar subquery must have
exactly one field. If the subquery returns exactly one row, that single value is
the scalar subquery result. If the subquery returns zero rows, the scalar
subquery value is <code>NULL</code>. If the subquery returns more than one row, the query
fails with a runtime error. When the subquery is written with <code>SELECT AS
STRUCT</code>  or <code>SELECT AS ProtoName</code>, it can include multiple
columns, and the returned value is the constructed STRUCT or PROTO. Selecting
multiple columns without using <code>SELECT AS</code> is an error.</td>
</tr>
<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>Can use <code>SELECT AS STRUCT</code> or <code>SELECT AS ProtoName</code> to
build arrays of structs or PROTOs, and conversely, selecting multiple columns
without using <code>SELECT AS</code> is an error. Returns an empty ARRAY if the
subquery returns zero rows. Never returns a <code>NULL</code> ARRAY.</td>
</tr>

<tr>
<td>IN</td>
<td>BOOL</td>
<td>Occurs in an expression following the IN operator. The subquery must produce
a single column whose type is equality-compatible with the expression on the
left side of the IN operator. Returns FALSE if the subquery returns zero rows.
<code>x IN ()</code> is equivalent to <code>x IN (value, value, ...)</code>
See the <code>IN</code> operator in

<a href="#comparison_operators">

Comparison Operators
</a>

for full semantics.</td>

</tr>

<tr>
<td>EXISTS</td>
<td>BOOL</td>
<td>Returns TRUE if the subquery produced one or more rows. Returns FALSE if the
subquery produces zero rows. Never returns <code>NULL</code>. Unlike all other expression
subqueries, there are no rules about the column list. Any number of columns may
be selected and it will not affect the query result.</td>

</tr>
</tbody>
</table>

**Examples**

The following examples of expression subqueries assume that `t.int_array` has
type `ARRAY<INT64>`.

<table>
<thead>
<tr>
<th>Type</th>
<th>Subquery</th>
<th>Result Data Type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td rowspan="8" style="vertical-align:top">Scalar</td>
<td><code>(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>INT64</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT DISTINCT i FROM t.int_array i)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT i FROM t.int_array i WHERE i=5)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT ARRAY_AGG(i) FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Uses the ARRAY_AGG aggregation function to return an ARRAY.</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a)</code></td>
<td>STRING</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a, 123 b)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a, 123 b)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="7" style="vertical-align:top">ARRAY</td>
<td><code>ARRAY(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>ARRAY of size 1</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT x FROM t)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT i FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Makes an ARRAY of one-field STRUCTs</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>ARRAY</td>
<td>Returns an ARRAY of STRUCTs with anonymous or duplicate fields.</td>
</tr>
<tr>
<td><code>ARRAY(SELECT  AS TypeName SUM(x) a, SUM(y) b, SUM(z) c from t)</code></td>
<td>array&lt;TypeName></td>
<td>Selecting into a named type. Assume TypeName is a STRUCT type with fields
a,b,c.</td>
</tr>
<tr>
<td style="vertical-align:top">STRUCT</td>
<td><code>(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>STRUCT</td>
<td>Constructs a STRUCT with anonymous or duplicate fields.</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">EXISTS</td>
<td><code>EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>NOT EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">IN</td>
<td><code>x IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>x NOT IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

[exp-sub-link-to-subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax#subqueries

[exp-sub-link-to-subqueries]: #subqueries

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Data Types

<!-- BEGIN CONTENT -->
ZetaSQL supports simple data types such as integers, as well as more
complex types such as ARRAY,
PROTO, and STRUCT. This page provides an overview of each data type,
including allowed values. For information on
data type literals and constructors, see
[Lexical Structure and Syntax][lexical-literals].

### Data type properties

When storing and querying data, it is helpful to keep the following data type
properties in mind:

<table>
<thead>
<tr>
<th>Property</th>
<th>Description</th>
<th>Applies To</th>
</tr>
</thead>
<tbody>
<tr>
<td>Nullable</td>
<td style="white-space:nowrap"><code>NULL</code> is a valid value.</td>
<td>

All data types.

</td>
</tr>
<tr>
<td>Orderable</td>
<td style="white-space:nowrap">Can be used in an <code>ORDER BY</code> clause.</td>
<td>All data types except for:
<ul>
<li>PROTO</li>
<li>ARRAY</li>
<li>STRUCT</li>
</ul>
</td>
</tr>
<tr>
<td>Groupable</td>
<td style="white-space:nowrap">Can generally appear in an expression following<br>
  <code>GROUP BY</code>,
  <code>DISTINCT</code>, and <code>PARTITION BY</code>.<br>
  However, <code>PARTITION BY</code> expressions cannot include<br>
  <a href="#floating_point_types">floating point types</a>.<br>
</td>
<td>All data types except for:<ul>
 <li>PROTO</li>
</ul>
An ARRAY type is groupable if its element type is
groupable. Two arrays are in the same group if and only if one of the following
statements is true:

<ol>
  <li>The two arrays are both null.</li>
  <li>The two arrays have the same number of elements and all corresponding
    elements are in the same groups.</li>
</ol>

<br>A STRUCT type is groupable if its field types are groupable. Two structs
are in the same group if and only if one of the following statements is true:

<ol>
  <li>The two structs are both null.</li>
  <li>All corresponding field values between the structs are in the same groups.</li>
</ol>

</td>
</tr>
<tr>
<td>Comparable</td>
<td>Values of the same type can be compared to each other.</td>
<td>All data types, with the following exceptions:

<br/><br/>
Equality comparisons for ARRAY data types are supported as long as the element
types are the same, and the element types are comparable. Less than and greater
than comparisons are not supported.

<br/><br/>
Equality comparisons for STRUCTs are supported field by field, in field order.
Field names are ignored. Less than and greater than comparisons are not
supported.

<br/><br/>
Protocol Buffer comparisons are not supported.

<br /><br />
All types that support comparisons
can be used in a <code>JOIN</code> condition. See

<a href="#join_types">

JOIN Types
</a>

for an explanation of join conditions.</td></tr>
</tbody>
</table>

### Numeric types

Numeric types include integer types, floating point types and the `NUMERIC` data
type.

#### Integer types

Integers are numeric values that do not have fractional components.

<table>
<thead>
<tr>
<th>Name</th>
<th>Storage Size</th>
<th>Range</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>INT32</code></td>
<td>4 bytes</td>
<td>-2,147,483,648 to 2,147,483,647</td>
</tr>

<tr>
<td><code>UINT32</code></td>
<td>4 bytes</td>
<td>0 to 4,294,967,295</td>
</tr>

<tr>
<td><code>INT64</code></td>
<td>8 bytes</td>
<td>-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807</td>
</tr>

<tr>
<td><code>UINT64</code></td>
<td>8 bytes</td>
<td>0 to 18,446,744,073,709,551,615</td>
</tr>

</tbody>
</table>

#### NUMERIC type

The `NUMERIC` data type is an exact numeric value with 38 digits of precision
and 9 decimal digits of scale. Precision is the number of digits that the number
contains. Scale is how many of these digits appear after the decimal point.

This type can represent decimal fractions exactly, and is suitable for financial
calculations.

<table>
<thead>
<tr>
<th>Name</th>
<th>Storage Size</th>
<th>Description</th>
<th>Range</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>NUMERIC</code></td>
<td>16 bytes</td>
<td>Decimal values with 38 decimal digits of precision and 9 decimal digits of
scale.</td>
<td>-99999999999999999999999999999.999999999 to
  99999999999999999999999999999.999999999</td>
</tr>
</tbody>
</table>

#### Floating point types {: #floating_point_types }

Floating point values are approximate numeric values with fractional components.

<table>
<thead>
<tr>
<th>Name</th>
<th>Storage Size</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>FLOAT</code></td>
<td>4 bytes</td>
<td>Single precision (approximate) decimal values.</td>
</tr>

<tr>
<td><code>DOUBLE</code></td>
<td>8 bytes</td>
<td>Double precision (approximate) decimal values.</td>
</tr>
</tbody>
</table>

##### Floating point semantics

When working with floating point numbers, there are special non-numeric values
that need to be considered: `NaN` and `+/-inf`

Arithmetic operators provide standard IEEE-754 behavior for all finite input
values that produce finite output and for all operations for which at least one
input is non-finite.

Function calls and operators return an overflow error if the input is finite
but the output would be non-finite. If the input contains non-finite values, the
output can be non-finite. In general functions do not introduce `NaN`s or
`+/-inf`. However, specific functions like `IEEE_DIVIDE` can return non-finite
values on finite input. All such cases are noted explicitly in
[Mathematical functions][mathematical-functions].

##### Mathematical function examples

<table>
<thead>
<tr>
<th>Left Term</th>
<th>Operator</th>
<th>Right Term</th>
<th>Returns</th>
</tr>
</thead>
<tbody>
<tr>
<td>Any value</td>
<td><code>+</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
<tr>
<td>1.0</td>
<td><code>+</code></td>
<td><code>+inf</code></td>
<td><code>+inf</code></td>
</tr>
<tr>
<td>1.0</td>
<td><code>+</code></td>
<td><code>-inf</code></td>
<td><code>-inf</code></td>
</tr>
<tr>
<td><code>-inf</code></td>
<td><code>+</code></td>
<td><code>+inf</code></td>
<td><code>NaN</code></td>
</tr>
<tr>
<td>Maximum <code>DOUBLE</code> value</td>
<td><code>+</code></td>
<td>Maximum <code>DOUBLE</code> value</td>
<td>Overflow error</td>
</tr>
<tr>
<td>Minimum <code>DOUBLE</code> value</td>
<td><code>/</code></td>
<td>2.0</td>
<td>0.0</td>
</tr>
<tr>
<td>1.0</td>
<td><code>/</code></td>
<td><code>0.0</code></td>
<td>"Divide by zero" error</td>
</tr>
</tbody>
</table>

Comparison operators provide standard IEEE-754 behavior for floating point
input.

##### Comparison operator examples

<table>
<thead>
<tr>
<th>Left Term</th>
<th>Operator</th>
<th>Right Term</th>
<th>Returns</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NaN</code></td>
<td><code>=</code></td>
<td>Any value</td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td><code>&lt;</code></td>
<td>Any value</td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td>Any value</td>
<td><code>&lt;</code></td>
<td><code>NaN</code></td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td>-0.0</td>
<td><code>=</code></td>
<td>0.0</td>
<td><code>TRUE</code></td>
</tr>
<tr>
<td>-0.0</td>
<td><code>&lt;</code></td>
<td>0.0</td>
<td><code>FALSE</code></td>
</tr>
</tbody>
</table>

Floating point values are sorted in this order, from least to greatest:

  1. `NULL`
  2. `NaN` &mdash; All `NaN` values are considered equal when sorting.
  3. `-inf`
  4. Negative numbers
  5. 0 or -0 &mdash; All zero values are considered equal when sorting.
  6. Positive numbers
  7. `+inf`

Special floating point values are grouped this way, including both grouping
done by a `GROUP BY` clause and grouping done by the `DISTINCT` keyword:

  * `NULL`
  * `NaN` &mdash; All `NaN` values are considered equal when grouping.
  * `-inf`
  * 0 or -0 &mdash; All zero values are considered equal when grouping.
  * `+inf`

### Boolean type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>BOOL</code></td>
<td>Boolean values are represented by the keywords <code>TRUE</code> and
<code>FALSE</code> (case insensitive).</td>
</tr>
</tbody>
</table>

### String type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRING</code></td>
<td>Variable-length character (Unicode) data.</td>
</tr>
</tbody>
</table>

Input STRING values must be UTF-8 encoded and output STRING values will be UTF-8
encoded. Alternate encodings like CESU-8 and Modified UTF-8 are not treated as
valid UTF-8.

All functions and operators that act on STRING values operate on Unicode
characters rather than bytes. For example, functions like `SUBSTR` and `LENGTH`
applied to STRING input count the number of characters, not bytes.

Each Unicode character has a numeric value called a code point assigned to it.
Lower code points are assigned to lower characters. When characters are
compared, the code points determine which characters are less than or greater
than other characters.

Most functions on STRING are also defined on BYTES. The BYTES version operates
on raw bytes rather than Unicode characters. STRING and BYTES are separate types
that cannot be used interchangeably. There is no implicit casting in either
direction. Explicit casting between STRING and BYTES does UTF-8 encoding and
decoding. Casting BYTES to STRING returns an error if the bytes are not
valid UTF-8.

### Bytes type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>BYTES</code></td>
<td>Variable-length binary data.</td>
</tr>
</tbody>
</table>

STRING and BYTES are separate types that cannot be used interchangeably. Most
functions on STRING are also defined on BYTES. The BYTES version operates on raw
bytes rather than Unicode characters. Casts between STRING and BYTES enforce
that the bytes are encoded using UTF-8.

### Date type

<table>
<thead>
<tr>
<th>Name</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DATE</code></td>
<td>0001-01-01 to 9999-12-31.</td>
</tr>
</tbody>
</table>

The DATE type represents a logical calendar date, independent of time zone. A
DATE value does not represent a specific 24-hour time period. Rather, a given
DATE value represents a different 24-hour period when interpreted in different
time zones, and may represent a shorter or longer day during Daylight Savings
Time transitions.
To represent an absolute point in time,
use a [timestamp][timestamp-type].

##### Canonical format

```
'YYYY-[M]M-[D]D'
```

+ `YYYY`: Four-digit year
+ `[M]M`: One or two digit month
+ `[D]D`: One or two digit day

### Datetime type

<table>
<thead>
<tr>
<th>Name</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DATETIME</code></td>
<td>
    
        0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999999<br/>
        <hr/>
        0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999<br/>
    
</td>
</tr>
</tbody>
</table>

A DATETIME object represents a date and time, as they might be displayed
on a calendar or clock, independent of time zone.
It includes the year, month, day, hour, minute, second,
and subsecond.
The range of subsecond precision is determined by the SQL engine.
To represent an absolute point in time,
use a [timestamp][timestamp-type].

##### Canonical format

```
YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD|.DDDDDDDDD]]
```

<ul>
    <li><code>YYYY</code>: Four-digit year</li>
    <li><code>[M]M</code>: One or two digit month</li>
    <li><code>[D]D</code>: One or two digit day</li>
    <li><code>( |T)</code>: A space or a `T` separator</li>
    <li><code>[H]H</code>: One or two digit hour (valid values from 00 to 23)</li>
    <li><code>[M]M</code>: One or two digit minutes (valid values from 00 to 59)</li>
    <li><code>[S]S</code>: One or two digit seconds (valid values from 00 to 59)</li>
    
        <li><code>[.DDDDDDDDD|.DDDDDD]</code>: Up to six or nine fractional digits (microsecond or nanosecond precision)</li>
    
</ul>

### Time type

<table>
<thead>
<tr>
<th>Name</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TIME</code></td>

    <td>
        00:00:00 to 23:59:59.999999999<br/>
        <hr/>
        00:00:00 to 23:59:59.999999<br/>
    </td>

</tr>
</tbody>
</table>

A TIME object represents a time, as might be displayed on a watch,
independent of a specific date and timezone.
The range of
subsecond precision is determined by the
SQL engine. To represent
an absolute point in time, use a [timestamp][timestamp-type].

##### Canonical format

```
[H]H:[M]M:[S]S[.DDDDDD|.DDDDDDDDD]
```

<ul>
    <li><code>[H]H</code>: One or two digit hour (valid values from 00 to 23)</li>
    <li><code>[M]M</code>: One or two digit minutes (valid values from 00 to 59)</li>
    <li><code>[S]S</code>: One or two digit seconds (valid values from 00 to 59)</li>
    
        <li><code>[.DDDDDDDDD|.DDDDDD]</code>: Up to six or nine fractional digits (microsecond or nanosecond precision)</li>
    
</ul>

### Timestamp type

<table>
<thead>
<tr>
<th>Name</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TIMESTAMP</code></td>

    <td>
      0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999999 UTC<br/>
      <hr/>
      0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999 UTC<br/>
    </td>

</tr>
</tbody>
</table>

A TIMESTAMP object represents an absolute point in time,
independent of any time zone or convention such as Daylight Savings Time
with
microsecond or nanosecond
precision.
The range of subsecond precision is determined by the SQL engine.

+  To represent a date as it might appear on a calendar,
   use a [DATE][date-type] object.
+  To represent a time, as it might appear on a clock,
   use a [TIME][time-type] object.
+  To represent a date and time, as they might appear on a calendar and clock,
   use a [DATETIME][datetime-type] object.

<div>

</div>

##### Canonical format

```
YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD|.DDDDDDDDD]][time zone]
```

<ul>
    <li><code>YYYY</code>: Four-digit year</li>
    <li><code>[M]M</code>: One or two digit month</li>
    <li><code>[D]D</code>: One or two digit day</li>
    <li><code>( |T)</code>: A space or a `T` separator</li>
    <li><code>[H]H</code>: One or two digit hour (valid values from 00 to 23)</li>
    <li><code>[M]M</code>: One or two digit minutes (valid values from 00 to 59)</li>
    <li><code>[S]S</code>: One or two digit seconds (valid values from 00 to 59)</li>
    
        <li><code>[.DDDDDDDDD|.DDDDDD]</code>: Up to six or nine fractional digits (microsecond or nanosecond precision)</li>
    
    <li><code>[time zone]</code>: String representing the time zone.
                                  When a time zone is not explicitly specified, the
                                  default time zone, which is implementation defined, is used.
                                  See the <a href="#time_zones">time zones</a> section for details.
   </li>
</ul>

#### Time zones

Time zones are used when parsing timestamps or formatting timestamps
for display. The timestamp value itself does not store a specific time zone,
nor does it change when you apply a time zone offset.

Time zones are represented by strings in one of these two canonical formats:

+ Offset from Coordinated Universal Time (UTC), or the letter `Z` for UTC
+ Time zone name from the [tz database][tz-database]{: class=external target=_blank }

##### Offset from Coordinated Universal Time (UTC)

```
(+|-)H[H][:M[M]]
Z
```

**Examples**

```
-08:00
-8:15
+3:00
+07:30
-7
Z
```

When using this format, no space is allowed between the time zone and the rest
of the timestamp.

```
2014-09-27 12:30:00.45-8:00
2014-09-27T12:30:00.45Z
```

##### Time zone name

```
continent/[region/]city
```

Time zone names are from the [tz database][tz-database]{: class=external target=_blank }.
For a less comprehensive but simpler reference, see the
[List of tz database time zones][tz-database-list]{: class=external target=_blank }
on Wikipedia.

**Examples**

```
America/Los_Angeles
America/Argentina/Buenos_Aires
```

When using a time zone name, a space is required between the name and the rest
of the timestamp:

```
2014-09-27 12:30:00.45 America/Los_Angeles
```

Note that not all time zone names are interchangeable even if they do happen to
report the same time during a given part of the year. For example,
`America/Los_Angeles` reports the same time as `UTC-7:00` during Daylight
Savings Time, but reports the same time as `UTC-8:00` outside of Daylight
Savings Time.

If a time zone is not specified, the default time zone value is used.

##### Leap seconds

A timestamp is simply an offset from 1970-01-01 00:00:00 UTC, assuming there are
exactly 60 seconds per minute. Leap seconds are not represented as part of a
stored timestamp.

If the input contains values that use ":60" in the seconds field to represent a
leap second, that leap second is not preserved when converting to a timestamp
value. Instead that value is interpreted as a timestamp with ":00" in the
seconds field of the following minute.

Leap seconds do not affect timestamp computations. All timestamp computations
are done using Unix-style timestamps, which do not reflect leap seconds. Leap
seconds are only observable through functions that measure real-world time. In
these functions, it is possible for a timestamp second to be skipped or repeated
when there is a leap second.

### Array type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ARRAY</code></td>
<td>Ordered list of zero or more elements of any non-ARRAY type.</td>
</tr>
</tbody>
</table>

An ARRAY is an ordered list of zero or more elements of non-ARRAY values.
ARRAYs of ARRAYs are not allowed. Queries that would produce an ARRAY of
ARRAYs will return an error. Instead a STRUCT must be inserted between the
ARRAYs using the `SELECT AS STRUCT` construct.

An empty ARRAY and a `NULL` ARRAY are two distinct values. ARRAYs can contain
`NULL` elements.

#### Declaring an ARRAY type

```
ARRAY<T>
```

ARRAY types are declared using the angle brackets (`<` and `>`). The type
of the elements of an ARRAY can be arbitrarily complex with the exception that
an ARRAY cannot directly contain another ARRAY.

**Examples**

<table>
<thead>
<tr>
<th>Type Declaration</th>
<th>Meaning</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>
ARRAY&lt;INT64&gt;
</code>
</td>
<td>Simple ARRAY of 64-bit integers.</td>
</tr>
<tr>
<td style="white-space:nowrap">
<code>
ARRAY&lt;STRUCT&lt;INT64, INT64&gt;&gt;
</code>
</td>
<td>An ARRAY of STRUCTs, each of which contains two 64-bit integers.</td>
</tr>
<tr>
<td style="white-space:nowrap">
<code>
ARRAY&lt;ARRAY&lt;INT64&gt;&gt;
</code><br/>
(not supported)
</td>
<td>This is an <strong>invalid</strong> type declaration which is included here
just in case you came looking for how to create a multi-level ARRAY. ARRAYs
cannot contain ARRAYs directly. Instead see the next example.</td>
</tr>
<tr>
<td style="white-space:nowrap">
<code>
ARRAY&lt;STRUCT&lt;ARRAY&lt;INT64&gt;&gt;&gt;
</code>
</td>
<td>An ARRAY of ARRAYS of 64-bit integers. Notice that there is a STRUCT between
the two ARRAYs because ARRAYs cannot hold other ARRAYs directly.</td>
</tr>
<tbody>
</table>

### Enum type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ENUM</code></td>
<td>Named type that maps STRING constants to INT32 constants.</td>
</tr>
</tbody>
</table>

An ENUM is a named type that enumerates a list of possible values, each of which
has:

+ An integer value. Integers are used for comparison and ordering ENUM values.
There is no requirement that these integers start at zero or that they be
contiguous.
+ A string value. Strings are case sensitive.
+ Optional alias values. One or more additional string values that act as
aliases.

Enum values are referenced using their integer value or their string value.
You reference an ENUM type, such as when using CAST, by using its fully
qualified name.

You cannot create new ENUM types using ZetaSQL.

### Struct type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT</code></td>
<td>Container of ordered fields each with a type (required) and field name
(optional).</td>
</tr>
</tbody>
</table>

#### Declaring a STRUCT type

```
STRUCT<T>
```

STRUCT types are declared using the angle brackets (`<` and `>`). The type of
the elements of a STRUCT can be arbitrarily complex.

**Examples**

<table>
<thead>
<tr>
<th>Type Declaration</th>
<th>Meaning</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>
STRUCT&lt;INT64&gt;
</code>
</td>
<td>Simple STRUCT with a single unnamed 64-bit integer field.</td>
</tr>
<tr>
<td style="white-space:nowrap">
<code>
STRUCT&lt;x STRUCT&lt;y INT64, z INT64&gt;&gt;
</code>
</td>
<td>A STRUCT with a nested STRUCT named <code>x</code> inside it. The STRUCT
<code>x</code> has two fields, <code>y</code> and <code>z</code>, both of which
are 64-bit integers.</td>
</tr>
<tr>
<td style="white-space:nowrap">
<code>
STRUCT&lt;inner_array ARRAY&lt;INT64&gt;&gt;
</code>
</td>
<td>A STRUCT containing an ARRAY named <code>inner_array</code> that holds
64-bit integer elements.</td>
</tr>
<tbody>
</table>

#### Constructing a STRUCT

##### Tuple syntax

```
(expr1, expr2 [, ... ])
```

The output type is an anonymous STRUCT type with anonymous fields with types
matching the types of the input expressions. There must be at least two
expressions specified. Otherwise this syntax is indistinguishable from an
expression wrapped with parentheses.

**Examples**

<table>
<thead>
<tr>
<th>Syntax</th>
<th>Output Type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td style="white-space:nowrap"><code>(x, x+y)</code></td>
<td style="white-space:nowrap"><code>STRUCT&lt;?,?&gt;</code></td>
<td>If column names are used (unquoted strings), the STRUCT field data type is
derived from the column data type. <code>x</code> and <code>y</code> are
columns, so the data types of the STRUCT fields are derived from the column
types and the output type of the addition operator.</td>
</tr>
</tbody>
</table>

This syntax can also be used with STRUCT comparison for comparison expressions
using multi-part keys, e.g. in a `WHERE` clause:

```
WHERE (Key1,Key2) IN ( (12,34), (56,78) )
```

##### Typeless struct syntax

```
STRUCT( expr1 [AS field_name] [, ... ])
```

Duplicate field names are allowed. Fields without names are considered anonymous
fields and cannot be referenced by name. STRUCT values can be `NULL`, or can
have `NULL` field values.

**Examples**

<table>
<thead>
<tr>
<th>Syntax</th>
<th>Output Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT(1,2,3)</code></td>
<td><code>STRUCT&lt;int64,int64,int64&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT()</code></td>
<td><code>STRUCT&lt;&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT('abc')</code></td>
<td><code>STRUCT&lt;string&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT(1, t.str_col)</code></td>
<td><code>STRUCT&lt;int64, str_col string&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT(1 AS a, 'abc' AS b)</code></td>
<td><code>STRUCT&lt;a int64, b string&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT(str_col AS abc)</code></td>
<td><code>STRUCT&lt;abc string&gt;</code></td>
</tr>
</tbody>
</table>

##### Typed struct syntax

```
STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
```

Typed syntax allows constructing STRUCTs with an explicit STRUCT data type. The
output type is exactly the `field_type` provided. The input expression is
coerced to `field_type` if the two types are not the same, and an error is
produced if the types are not compatible. `AS alias` is not allowed on the input
expressions. The number of expressions must match the number of fields in the
type, and the expression types must be coercible or literal-coercible to the
field types.

**Examples**

<table>
<thead>
<tr>
<th>Syntax</th>
<th>Output Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT&lt;int64&gt;(5)</code></td>
<td><code>STRUCT&lt;int64&gt;</code></td>
</tr>

<tr>
<td><code>STRUCT&lt;date&gt;("2011-05-05")</code></td>
<td><code>STRUCT&lt;date&gt;</code></td>
</tr>

<tr>
<td><code>STRUCT&lt;x int64, y string&gt;(1, t.str_col)</code></td>
<td><code>STRUCT&lt;x int64, y string&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT&lt;int64&gt;(int_col)</code></td>
<td><code>STRUCT&lt;int64&gt;</code></td>
</tr>
<tr>
<td><code>STRUCT&lt;x int64&gt;(5 AS x)</code></td>
<td>Error - Typed syntax does not allow <code>AS</code></td>
</tr>
</tbody>
</table>

#### Limited comparisons for STRUCT

STRUCTs can be directly compared using equality operators:

  * Equal (`=`)
  * Not Equal (`!=` or `<>`)
  * [`NOT`] `IN`

Notice, though, that these direct equality comparisons compare the fields of
the STRUCT pairwise in ordinal order ignoring any field names. If instead you
want to compare identically named fields of a STRUCT, you can compare the
individual fields directly.

### Protocol buffer type

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>PROTO</code></td>
<td>An instance of protocol buffer.</td>
</tr>
</tbody>
</table>

Protocol buffers provide structured data types with a defined serialization
format and cross-language support libraries. Protocol buffer message types can
contain optional, required or repeated fields, including nested messages. See
the
[Protocol Buffers Developer Guide][protocol-buffers-dev-guide] for more detail.

Protocol buffer message types behave similarly to STRUCT types, and support
similar operations like reading field values by name. Protocol buffer types are
always named types, and can be referred to by their fully-qualified protocol
buffer name (i.e. `package.ProtoName`). Protocol buffers support some additional
behavior beyond STRUCTs, like default field values, and checking for the
presence of optional fields.

Protocol buffer ENUM types are also available and can be referenced using the
fully-qualified ENUM type name.

See [Using Protocol Buffers][protocol-buffers]
for more information.

#### Limited comparisons for PROTO

No direct comparison of PROTO values is supported. There are a couple possible
workarounds:

  * The most accurate way to compare PROTOs is to do a pair-wise comparison
    between the fields of the PROTOs. This can also be used to `GROUP BY` or
    `ORDER BY` PROTO fields.
  * For simple equality comparisons, you can cast a PROTO to BYTES and compare
    the results.
  * To get a simple approximation for inequality comparisons, you can cast PROTO
    to STRING. Note that this will do lexicographical ordering for numeric
    fields.

[protocol-buffers-dev-guide]: https://developers.google.com/protocol-buffers/docs/overview
[tz-database]: http://www.iana.org/time-zones
[tz-database-list]: http://en.wikipedia.org/wiki/List_of_tz_database_time_zones
[ogc-sfs]: http://www.opengeospatial.org/standards/sfs#downloads
[WGS84-reference-ellipsoid]: https://en.wikipedia.org/wiki/World_Geodetic_System
[timestamp-type]: #timestamp_type
[date-type]: #date_type
[datetime-type]: #datetime_type
[time-type]: #time_type
[protocol-buffers]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers
[lexical-literals]: https://github.com/google/zetasql/blob/master/docs/lexical#literals

[protocol-buffers]: #protocol-buffers
[lexical-literals]: #literals
[geography-functions]: #geography-functions
[mathematical-functions]: #mathematical-functions

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Query Syntax

<!-- BEGIN CONTENT -->

Query statements scan one or more tables or expressions and return the computed
result rows. This topic describes the syntax for SQL queries in
ZetaSQL.

### SQL Syntax

<pre class="lang-sql prettyprint">
<span class="var">query_statement</span>:
    <span class="var">query_expr</span>

<span class="var">query_expr</span>:
    [ <a href="#with_clause">WITH</a> <span class="var"><a href="#with_query_name">with_query_name</a></span> AS ( <span class="var">query_expr</span> ) [, ...] ]
    { <span class="var">select</span> | ( <span class="var">query_expr</span> ) | <span class="var">query_expr</span> <span class="var">set_op</span> <span class="var">query_expr</span> }
    [ <a href="#order_by_clause">ORDER</a> BY <span class="var">expression</span> [{ ASC | DESC }] [, ...] ]
    [ <a href="#limit_clause_and_offset_clause">LIMIT</a> <span class="var">count</span> [ OFFSET <span class="var">skip_rows</span> ] ]

<span class="var">select</span>:
    <a href="#select_list">SELECT</a> [ AS { <span class="var"><a href="#select_as_typename">typename</a></span> | <a href="#select_as_struct">STRUCT</a> | <a href="#select_as_value">VALUE</a> } ] [{ ALL | DISTINCT }]
        { [ <span class="var">expression</span>. ]* [ <a href="#select_except">EXCEPT</a> ( <span class="var">column_name</span> [, ...] ) ]<br>            [ <a href="#select_replace">REPLACE</a> ( <span class="var">expression</span> [ AS ] <span class="var">column_name</span> [, ...] ) ]<br>        | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
    [ <a href="#from_clause">FROM</a> <span class="var">from_item</span> [ <span class="var">tablesample_type</span> ] [, ...] ]
    [ <a href="#where_clause">WHERE</a> <span class="var">bool_expression</span> ]
    [ <a href="#group_by_clause">GROUP</a> BY { <span class="var">expression</span> [, ...] | ROLLUP ( <span class="var">expression</span> [, ...] ) } ]
    [ <a href="#having_clause">HAVING</a> <span class="var">bool_expression</span> ]
    [ <a href="#window_clause">WINDOW</a> <span class="var">named_window_expression</span> AS { <span class="var">named_window</span> | ( [ <span class="var">window_definition</span> ] ) } [, ...] ]

<span class="var">set_op</span>:
    <a href="#union">UNION</a> { ALL | DISTINCT } | <a href="#intersect">INTERSECT</a> { ALL | DISTINCT } | <a href="#except">EXCEPT</a> { ALL | DISTINCT }

<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">field_path</span> |
    { <a href="#unnest">UNNEST</a>( <span class="var">array_expression</span> ) | UNNEST( <span class="var">array_path</span> ) | <span class="var">array_path</span> }
        [ [ AS ] <span class="var">alias</span> ] [ WITH OFFSET [ [ AS ] <span class="var">alias</span> ] ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}

<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] <a href="#join_types">JOIN</a> <span class="var">from_item</span>
    [ { <a href="#on_clause">ON</a> <span class="var">bool_expression</span> | <a href="#using_clause">USING</a> ( <span class="var">join_column</span> [, ...] ) } ]

<span class="var">join_type</span>:
    { <a href="#inner_join">INNER</a> | <a href="#cross_join">CROSS</a> | <a href="#full_outer_join">FULL [OUTER]</a> | <a href="#left_outer_join">LEFT [OUTER]</a> | <a href="#right_outer_join">RIGHT [OUTER]</a> }

<span class="var">tablesample_type</span>:
    <a href="#tablesample_operator">TABLESAMPLE</a> <span class="var">sample_method</span> (<span class="var">sample_size</span> <span class="var">percent_or_rows</span> )

<span class="var">sample_method</span>:
    { BERNOULLI | SYSTEM | RESERVOIR }

<span class="var">sample_size</span>:
    <span class="var">numeric_value_expression</span>

<span class="var">percent_or_rows</span>:
    { PERCENT | ROWS }
</pre>

Notation:

+ Square brackets "[ ]" indicate optional clauses.
+ Parentheses "( )" indicate literal parentheses.
+ The vertical bar "|" indicates a logical OR.
+ Curly braces "{ }" enclose a set of options.
+ A comma followed by an ellipsis within square brackets "[, ... ]" indicates that
  the preceding item can repeat in a comma-separated list.

### SELECT list

Syntax:

<pre>
SELECT [ AS { <span class="var">typename</span> | STRUCT | VALUE } ] [{ ALL | DISTINCT }]
    { [ <span class="var">expression</span>. ]* [ EXCEPT ( <span class="var">column_name</span> [, ...] ) ]<br>        [ REPLACE ( <span class="var">expression</span> [ AS ] <span class="var">column_name</span> [, ...] ) ]<br>    | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
</pre>

The `SELECT` list defines the columns that the query will return. Expressions in
the `SELECT` list can refer to columns in any of the `from_item`s in its
corresponding `FROM` clause.

Each item in the `SELECT` list is one of:

<ul>
<li>*</li>
<li><code>expression</code></li>

<li><code>expression.*</code></li>

</ul>

#### SELECT *

`SELECT *`, often referred to as *select star*, produces one output column for
each column that is visible after executing the full query.

```
SELECT * FROM (SELECT "apple" AS fruit, "carrot" AS vegetable);

+-------+-----------+
| fruit | vegetable |
+-------+-----------+
| apple | carrot    |
+-------+-----------+
```

#### SELECT `expression`

Items in a `SELECT` list can be expressions. These expressions evaluate to a
single value and produce one output column, with an optional explicit `alias`.

If the expression does not have an explicit alias, it receives an implicit alias
according to the rules for [implicit aliases][implicit-aliases], if possible.
Otherwise, the column is anonymous and you cannot refer to it by name elsewhere
in the query.

#### SELECT `expression.*`

An item in a `SELECT` list can also take the form of `expression.*`. This
produces one output column for each column or top-level field of `expression`.
The expression must either be a table alias or evaluate to a single value of a
data type with fields, such as a STRUCT.

The following query produces one output column for each column in the table
`groceries`, aliased as `g`.

```
WITH groceries AS
  (SELECT "milk" AS dairy,
   "eggs" AS protein,
   "bread" AS grain)
SELECT g.*
FROM groceries AS g;

+-------+---------+-------+
| dairy | protein | grain |
+-------+---------+-------+
| milk  | eggs    | bread |
+-------+---------+-------+
```
More examples:

```
WITH locations AS
  (SELECT STRUCT("Seattle" AS city, "Washington" AS state) AS location
  UNION ALL
  SELECT STRUCT("Phoenix" AS city, "Arizona" AS state) AS location)
SELECT l.location.*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
| Phoenix | Arizona    |
+---------+------------+
```

```
WITH locations AS
  (SELECT ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),
    ("Phoenix", "Arizona")] AS location)
SELECT l.LOCATION[offset(0)].*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
+---------+------------+
```

#### SELECT modifiers

You can modify the results returned from a `SELECT` query, as follows.

##### SELECT DISTINCT

A `SELECT DISTINCT` statement discards duplicate rows and returns only the
remaining rows. `SELECT DISTINCT` cannot return columns of the following types:

<ul>

  
  <li>PROTO</li>
  

  

  

</ul>

##### SELECT * EXCEPT

A `SELECT * EXCEPT` statement specifies the names of one or more columns to
exclude from the result. All matching column names are omitted from the output.

```
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * EXCEPT (order_id)
FROM orders;

+-----------+----------+
| item_name | quantity |
+-----------+----------+
| sprocket  | 200      |
+-----------+----------+
```

**Note:** `SELECT * EXCEPT` does not exclude columns that do not have names.

##### SELECT * REPLACE

A `SELECT * REPLACE` statement specifies one or more
`expression AS identifier` clauses. Each identifier must match a column name
from the `SELECT *` statement. In the output column list, the column that
matches the identifier in a `REPLACE` clause is replaced by the expression in
that `REPLACE` clause.

A `SELECT * REPLACE` statement does not change the names or order of columns.
However, it can change the value and the value type.

```
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE ("widget" AS item_name)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | widget    | 200      |
+----------+-----------+----------+

WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE (quantity/2 AS quantity)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | sprocket  | 100      |
+----------+-----------+----------+
```
**Note:** `SELECT * REPLACE` does not replace columns that do not have names.

##### SELECT ALL
A `SELECT ALL` statement returns all rows, including duplicate rows.
`SELECT ALL` is the default behavior of `SELECT`.

#### Value tables

In ZetaSQL, a value table is a table where the row type is a single
value.  In a regular table, each row is made up of columns, each of which has a
name and a type.  In a value table, the row type is just a single value, and
there are no column names.

Most commonly, value tables are used for protocol buffer value tables, where the
table contains a stream of protocol buffer values. In this case, the top-level
protocol buffer fields can be used in the same way that column names are used
when querying a regular table.

In contexts where a query with exactly one column is expected, a value table
query can be used instead.  For example, scalar subqueries and array subqueries
(see [Subqueries][subqueries]) normally require a single-column query, but in
ZetaSQL, they also allow using a value table query.

A query will produce a value table if it uses `SELECT AS`, using one of the
syntaxes below:

##### SELECT AS STRUCT

Syntax:

```
SELECT AS STRUCT expr1 [struct_field_name1] [,... ]
```

This produces a value table with a STRUCT row type, where the STRUCT field
names and types match the column names and types produced in the `SELECT` list.
Anonymous columns and duplicate columns are allowed.

Example:

```
SELECT AS STRUCT 1 x, 2, 3 x
```

The query above produces STRUCT values of type `STRUCT<int64 x, int64, int64
x>.` The first and third fields have the same name `x`, and the second field is
anonymous.

The example above produces the same result as this query using a struct
constructor:

```
SELECT AS VALUE STRUCT(1 AS x, 2, 3 AS x)
```

Example:

```
SELECT
  ARRAY(SELECT AS STRUCT t.f1, t.f2 WHERE t.f3=true)
FROM
  Table t
```

`SELECT AS STRUCT` can be used in a scalar or array subquery to produce a single
STRUCT type grouping multiple values together. Scalar
and array subqueries (see [Subqueries][subqueries]) are normally not allowed to
return multiple columns.

##### SELECT AS VALUE

`SELECT AS VALUE` produces a value table from any `SELECT` list that
produces exactly one column. Instead of producing an output table with one
column, possibly with a name, the output will be a value table where the row
type is just the value type that was produced in the one `SELECT` column.  Any
alias the column had will be discarded in the value table.

Example:

```
SELECT AS VALUE Int64Column FROM Table;
```

The query above produces a table with row type INT64.

Example:

```
SELECT AS VALUE STRUCT(1 a, 2 b) xyz FROM Table;
```

The query above produces a table with row type `STRUCT<a int64, b int64>`.

Example:

```
SELECT AS VALUE v FROM ValueTable v WHERE v.field=true;
```

Given a value table `v` as input, the query above filters out certain values in
the `WHERE` clause, and then produces a value table using the exact same value
that was in the input table. If the query above did not use `SELECT AS VALUE`,
then the output table schema would differ from the input table schema because
the output table would be a regular table with a column named `v` containing the
input value.

#### Aliases

See [Using Aliases][using-aliases] for information on syntax and visibility for
`SELECT` list aliases.

### FROM clause

The `FROM` clause indicates the table or tables from which to retrieve rows, and
specifies how to join those rows together to produce a single stream of
rows for processing in the rest of the query.

#### Syntax

<pre>
<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">field_path</span> |
    { <a href="#unnest">UNNEST</a>( <span class="var">array_expression</span> ) | UNNEST( <span class="var">array_path</span> ) | <span class="var">array_path</span> }
        [ [ AS ] <span class="var">alias</span> ] [ WITH OFFSET [ [ AS ] <span class="var">alias</span> ] ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}
</pre>

##### table_name

The name (optionally qualified) of an existing table.

<pre>
SELECT * FROM Roster;
SELECT * FROM db.Roster;
</pre>

##### join

See <a href="#join_types">JOIN Types</a> below.

##### select

<code>( select ) [ [ AS ] alias ]</code> is a table <a href="#subqueries">subquery</a>.

##### field_path

<p>In the <code>FROM</code> clause, <code>field_path</code> is any path that
resolves to a field within a data type. <code>field_path</code> can go
arbitrarily deep into a nested data structure.</p>

<p>Some examples of valid <code>field_path</code> values include:</p>

<pre>
SELECT * FROM T1 t1, t1.array_column;

SELECT * FROM T1 t1, t1.struct_column.array_field;

SELECT (SELECT ARRAY_AGG(c) FROM t1.array_column c) FROM T1 t1;

SELECT a.struct_field1 FROM T1 t1, t1.array_of_structs a;

SELECT (SELECT STRING_AGG(a.struct_field1) FROM t1.array_of_structs a) FROM T1 t1;
</pre>

<p>Field paths in the FROM clause must end in an
array or a repeated field. In
addition, field paths cannot contain arrays
or repeated fields before the end of the path. For example, the path
<code>array_column.some_array.some_array_field</code> is invalid because it
contains an array before the end of the path.</p>

<p class="note">Note: If a path has only one name, it is interpreted as a table.
To work around this, wrap the path using <code>UNNEST</code>, or use the
fully-qualified path.</p>

<p class="note">Note: If a path has more than one name, and it matches a field
name, it is interpreted as a field name. To force the path to be interpreted as
a table name, wrap the path using <code>`</code>. </p>

##### UNNEST

The `UNNEST` operator takes an `ARRAY` and returns a
table, with one row for each element in the `ARRAY`.
You can also use `UNNEST` outside of the `FROM` clause with the
[`IN` operator][in-operator].

For input `ARRAY`s of most element types, the output of `UNNEST` generally has one
column. This single column has an optional `alias`, which you can use to refer
to the column elsewhere in the query. `ARRAYS` with these element types return
multiple columns:

 + STRUCT
 + PROTO

`UNNEST` destroys the order of elements in the input
`ARRAY`. Use the optional `WITH OFFSET` clause to
return a second column with the array element indexes (see below).

For an input `ARRAY` of `STRUCT`s, `UNNEST`
returns a row for each `STRUCT`, with a separate column for each field in the
`STRUCT`. The alias for each column is the name of the corresponding `STRUCT`
field.

**Example**

```
SELECT *
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')]);

+---+-----+
| x | y   |
+---+-----+
| 3 | bar |
| 1 | foo |
+---+-----+
```

Because the `UNNEST` operator returns a
[value table][query-value-tables],
you can alias `UNNEST` to define a range variable that you can reference
elsewhere in the query. If you reference the range variable in the `SELECT`
list, the query returns a `STRUCT` containing all of the fields of the original
`STRUCT` in the input table.

**Example**

```
SELECT *, struct_value
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')])
       AS struct_value;

+---+-----+--------------+
| x | y   | struct_value |
+---+-----+--------------+
| 3 | bar | {3, bar}     |
| 1 | foo | {1, foo}     |
+---+-----+--------------+
```

For an input `ARRAY` of `PROTO`s, `UNNEST`
returns a row for each `PROTO`, with a separate column for each field in the
`PROTO`. The alias for each column is the name of the corresponding `PROTO`
field.

**Example**

```
SELECT *
FROM UNNEST(
  ARRAY<zetasql.examples.music.Album>[
    NEW zetasql.examples.music.Album (
      'The Goldberg Variations' AS album_name,
      ['Aria', 'Variation 1', 'Variation 2'] AS song
    )
  ]
);
+-------------------------+--------+----------------------------------+
| album_name              | singer | song                             |
+-------------------------+--------+----------------------------------+
| The Goldberg Variations | NULL   | [Aria, Variation 1, Variation 2] |
+-------------------------+--------+----------------------------------+
```

As with `STRUCT`s, you can alias `UNNEST` here to define a range variable. You
can reference this alias in the `SELECT` list to return a value table where each
row is a `PROTO` element from the `ARRAY`.

```
SELECT proto_value
FROM UNNEST(
  ARRAY<zetasql.examples.music.Album>[
    NEW zetasql.examples.music.Album (
      'The Goldberg Variations' AS album_name,
      ['Aria', 'Var. 1'] AS song
    )
  ]
) AS proto_value;
+---------------------------------------------------------------------+
| proto_value                                                         |
+---------------------------------------------------------------------+
| {album_name: "The Goldberg Variations" song: "Aria" song: "Var. 1"} |
+---------------------------------------------------------------------+
```

<p>ARRAY unnesting can be either explicit or implicit. In explicit unnesting,
<code>array_expression</code> must return an ARRAY value but does not need to resolve
to an ARRAY, and the <code>UNNEST</code> keyword is required.</p>

<p>Example:</p>

<pre>
SELECT * FROM UNNEST ([1, 2, 3]);
</pre>

<p>In implicit unnesting, <code>array_path</code> must resolve to an ARRAY and the
<code>UNNEST</code> keyword is optional.</p>

<p>Example:</p>

<pre>
SELECT x
FROM mytable AS t,
  t.struct_typed_column.array_typed_field1 AS x;
</pre>

<p>In this scenario, <code>array_path</code> can go arbitrarily deep into a data
structure, but the last field must be ARRAY-typed. No previous field in the
expression can be ARRAY-typed because it is not possible to extract a named
field from an ARRAY.</p>

<p><code>UNNEST</code> treats NULLs as follows:</p>

<ul>
<li>NULL and empty ARRAYs produces zero rows.</li>
<li>An ARRAY containing NULLs produces rows containing NULL values.</li>
</ul>

<p>The optional <code>WITH</code> <code>OFFSET</code> clause returns a separate
column containing the "offset" value (i.e. counting starts at zero) for each row
produced by the <code>UNNEST</code> operation. This column has an optional
<code>alias</code>; the default alias is offset.</p>

<p>Example:</p>

<pre>
SELECT * FROM UNNEST ( ) WITH OFFSET AS num;
</pre>

See the [`Arrays topic`][working-with-arrays]
for more ways to use `UNNEST`, including construction, flattening, and
filtering.

##### with_query_name

<p>The query names in a <code>WITH</code> clause (see <a
href="#with_clause">WITH Clause</a>) act like names of temporary tables that you
can reference anywhere in the <code>FROM</code> clause. In the example below,
<code>subQ1</code> and <code>subQ2</code> are <code>with_query_names</code>.</p>

<p>Example:</p>

<pre>
WITH
  subQ1 AS (SELECT * FROM Roster WHERE SchoolID = 52),
  subQ2 AS (SELECT SchoolID FROM subQ1)
SELECT DISTINCT * FROM subQ2;
</pre>

<p>The <code>WITH</code> clause hides any permanent tables with the same name
for the duration of the query, unless you qualify the table name, e.g.

 <code>db.Roster</code>.

</p>

<a id="subqueries"></a>
#### Subqueries

A subquery is a query that appears inside another statement, and is written
inside parentheses. These are also referred to as "sub-SELECTs" or
"nested SELECTs". The full `SELECT` syntax is valid in
subqueries.

There are two types of subquery:

+  [Expression Subqueries][expression-subqueries],
   which you can use in a query wherever expressions are valid. Expression
   subqueries return a single value.
+  Table subqueries, which you can use only in a `FROM` clause. The outer
query treats the result of the subquery as a table.

Note that there must be parentheses around both types of subqueries.

Example:

```
SELECT AVG ( PointsScored )
FROM
( SELECT PointsScored
  FROM Stats
  WHERE SchoolID = 77 )
```

Optionally, a table subquery can have an alias.

Example:

```
SELECT r.LastName
FROM
( SELECT * FROM Roster) AS r;
```

#### TABLESAMPLE operator

You can use the `TABLESAMPLE` operator to select a random sample of a data
set. This operator is useful when working with tables that have large amounts of
data and precise answers are not required.

Syntax:

<pre>
<span class="var">tablesample_type</span>:
    <a href="#tablesample_operator">TABLESAMPLE</a> <span class="var">sample_method</span> (<span class="var">sample_size</span> <span class="var">percent_or_rows</span>)
    [ REPEATABLE(repeat_argument) ]

<span class="var">sample_method</span>:
    { BERNOULLI | SYSTEM | RESERVOIR }

<span class="var">sample_size</span>:
    <span class="var">numeric_value_expression</span>

<span class="var">percent_or_rows</span>:
    { PERCENT | ROWS }
</pre>

When using the `TABLESAMPLE` operator, you must specify the sampling algorithm
to use:

+ `BERNOULLI` - each row is independently selected
with the probability given in the `percent` clause. As a result, you get
approximately `N * percent/100` rows.
+ `SYSTEM` - produces a sample using an
unspecified engine-dependent method, which may be more efficient but less
probabilistically random than other methods.  For example, it could choose
random disk blocks and return data from those blocks.
+ `RESERVOIR` - takes as parameter an actual sample size
K (expressed as a number of rows). If the input is smaller than K, it outputs
the entire input relation. If the input is larger than K, reservoir sampling
outputs a sample of size exactly K, where any sample of size K is equally
likely.

The `TABLESAMPLE` operator requires that you select either `ROWS` or `PERCENT`.
If you select `PERCENT`, the value must be between 0 and 100. If you select
`ROWS`, the value must be greater than or equal to 0.

The `REPEATABLE` clause is optional. When it is used, repeated executions of
the sampling operation return a result table with identical rows for a
given repeat argument, as long as the underlying data does
not change. `repeat_argument` represents a sampling seed
and must be a positive value of type `INT64`.

The following examples illustrate the use of the `TABLESAMPLE` operator.

Select from a table using the `RESERVOIR` sampling method:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE RESERVOIR (100 ROWS);
```

Select from a table using the `BERNOULLI` sampling method:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE BERNOULLI (0.1 PERCENT);
```

Using `TABLESAMPLE` with a repeat argument:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE RESERVOIR (100 ROWS) REPEATABLE(10);
```

Using `TABLESAMPLE` with a subquery:

```sql
SELECT Subject FROM
(SELECT MessageId, Subject FROM Messages WHERE ServerId="test")
TABLESAMPLE BERNOULLI(50 PERCENT)
WHERE MessageId > 3;
```

Using a `TABLESAMPLE` operation with a join to another table.

```sql
SELECT S.Subject
FROM
(SELECT MessageId, ThreadId FROM Messages WHERE ServerId="test") AS R
TABLESAMPLE RESERVOIR(5 ROWS),
Threads AS S
WHERE S.ServerId="test" AND R.ThreadId = S.ThreadId;
```

#### Aliases

See [Using Aliases][using-aliases] for information on syntax and visibility for
`FROM` clause aliases.

<a id="join_types"></a>
### JOIN types

#### Syntax

<pre>
<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] JOIN <span class="var">from_item</span>
    [ <a href="#on_clause">ON</a> <span class="var">bool_expression</span> | <a href="#using_clause">USING</a> ( <span class="var">join_column</span> [, ...] ) ]

<span class="var">join_type</span>:
    { <a href="#inner_join">INNER</a> | <a href="#cross_join">CROSS</a> | <a href="#full_outer_join">FULL [OUTER]</a> | <a href="#left_outer_join">LEFT [OUTER]</a> | <a href="#right_outer_join">RIGHT [OUTER]</a> }
</pre>

The `JOIN` clause merges two `from_item`s so that the `SELECT` clause can
query them as one source. The `join_type` and `ON` or `USING` clause (a
"join condition") specify how to combine and discard rows from the two
`from_item`s to form a single source.

All `JOIN` clauses require a `join_type`.

A `JOIN` clause requires a join condition unless one of the following conditions
is true:

+  `join_type` is `CROSS`.
+  One or both of the `from_item`s is not a table, e.g. an
   `array_path` or `field_path`.

#### [INNER] JOIN

An `INNER JOIN`, or simply `JOIN`, effectively calculates the Cartesian product
of the two `from_item`s and discards all rows that do not meet the join
condition. "Effectively" means that it is possible to implement an `INNER JOIN`
without actually calculating the Cartesian product.

#### CROSS JOIN

`CROSS JOIN` returns the Cartesian product of the two `from_item`s. In other
words, it combines each row from the first `from_item` with each row from the
second `from_item`. If there are *M* rows from the first and *N* rows from the
second, the result is *M* * *N* rows. Note that if either `from_item` has zero
rows, the result is zero rows.

**Comma cross joins**

`CROSS JOIN`s can be written explicitly (see directly above) or implicitly using
a comma to separate the `from_item`s.

Example of an implicit "comma cross join":

```
SELECT * FROM Roster, TeamMascot;
```

Here is the explicit cross join equivalent:

```
SELECT * FROM Roster CROSS JOIN TeamMascot;
```

You cannot write comma cross joins inside parentheses.

Invalid - comma cross join inside parentheses:

```
SELECT * FROM t CROSS JOIN (Roster, TeamMascot);  // INVALID.
```

See [Sequences of JOINs][sequences-of-joins] for details on how a comma cross
join behaves in a sequence of JOINs.

#### FULL [OUTER] JOIN

A `FULL OUTER JOIN` (or simply `FULL JOIN`) returns all fields for all rows in
both `from_item`s that meet the join condition.

`FULL` indicates that <em>all rows</em> from both `from_item`s are
returned, even if they do not meet the join condition.

`OUTER` indicates that if a given row from one `from_item` does not
join to any row in the other `from_item`, the row will return with NULLs
for all columns from the other `from_item`.

#### LEFT [OUTER] JOIN

The result of a `LEFT OUTER JOIN` (or simply `LEFT JOIN`) for two
`from_item`s always retains all rows of the left `from_item` in the
`JOIN` clause, even if no rows in the right `from_item` satisfy the join
predicate.

`LEFT` indicates that all rows from the <em>left</em> `from_item` are
returned; if a given row from the left `from_item` does not join to any row
in the <em>right</em> `from_item`, the row will return with NULLs for all
columns from the right `from_item`.  Rows from the right `from_item` that
do not join to any row in the left `from_item` are discarded.

#### RIGHT [OUTER] JOIN

The result of a `RIGHT OUTER JOIN` (or simply `RIGHT JOIN`) is similar and
symmetric to that of `LEFT OUTER JOIN`.

<a id="on_clause"></a>
#### ON clause

The `ON` clause contains a `bool_expression`. A combined row (the result of
joining two rows) meets the join condition if `bool_expression` returns
TRUE.

Example:

```
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

<a id="using_clause"></a>
#### USING clause

The `USING` clause requires a `column_list` of one or more columns which
occur in both input tables. It performs an equality comparison on that column,
and the rows meet the join condition if the equality comparison returns TRUE.

In most cases, a statement with the `USING` keyword is equivalent to using the
`ON` keyword.  For example, the statement:

```
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

is equivalent to:

```
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

The results from queries with `USING` do differ from queries that use `ON` when
you use `SELECT *`. To illustrate this, consider the query:

```
SELECT * FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`.  The results include a
single `LastName` column.

By contrast, consider the following query:

```
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`.  The results include
two `LastName` columns; one from `Roster` and one from `PlayerStats`.

**NOTE**: The `USING` keyword is not supported in
strict
mode.

<a id="sequences_of_joins"></a>
#### Sequences of JOINs

The `FROM` clause can contain multiple `JOIN` clauses in sequence.

Example:

```
SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE;
```

where `a`, `b`, and `c` are any `from_item`s. JOINs are bound from left to
right, but you can insert parentheses to group them in a different order.

Consider the following queries: A (without parentheses) and B (with parentheses)
are equivalent to each other but not to C. The `FULL JOIN` in **bold** binds
first.

 A.

<pre>
SELECT * FROM Roster <b>FULL JOIN</b> TeamMascot USING (SchoolID)
FULL JOIN PlayerStats USING (LastName);
</pre>

B.

<pre>
SELECT * FROM ( (Roster <b>FULL JOIN</b> TeamMascot USING (SchoolID))
FULL JOIN PlayerStats USING (LastName));
</pre>

C.

<pre>
SELECT * FROM (Roster FULL JOIN (TeamMascot <b>FULL JOIN</b> PlayerStats USING
(LastName)) USING (SchoolID)) ;
</pre>

When comma cross joins are present in a query with a sequence of JOINs, they
group from left to right like other `JOIN` types.

Example:

```
SELECT * FROM a JOIN b ON TRUE, b JOIN c ON TRUE;
```

The query above is equivalent to

```
SELECT * FROM ((a JOIN b ON TRUE) CROSS JOIN b) JOIN c ON TRUE);
```

There cannot be a `RIGHT JOIN` or `FULL JOIN` after a comma join.

Invalid - `RIGHT JOIN` after a comma cross join:

```
SELECT * FROM Roster, TeamMascot RIGHT JOIN PlayerStats ON TRUE;  // INVALID.
```

<a id="where_clause"></a>
### WHERE clause

#### Syntax

```
WHERE bool_expression
```

The `WHERE` clause filters out rows by evaluating each row against
`bool_expression`, and discards all rows that do not return TRUE (that is,
rows that return FALSE or NULL).

Example:

```
SELECT * FROM Roster
WHERE SchoolID = 52;
```

The `bool_expression` can contain multiple sub-conditions.

Example:

```
SELECT * FROM Roster
WHERE STARTS_WITH(LastName, "Mc") OR STARTS_WITH(LastName, "Mac");
```

You cannot reference column aliases from the `SELECT` list in the `WHERE`
clause.

Expressions in an `INNER JOIN` have an equivalent expression in the
`WHERE` clause. For example, a query using `INNER` `JOIN` and `ON` has an
equivalent expression using `CROSS JOIN` and `WHERE`.

Example - this query:

```
SELECT * FROM Roster INNER JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

is equivalent to:

```
SELECT * FROM Roster CROSS JOIN TeamMascot
WHERE Roster.SchoolID = TeamMascot.SchoolID;
```

<a id="group_by_clause"></a>
### GROUP BY clause

#### Syntax

<pre>
GROUP BY { <span class="var">expression</span> [, ...] | ROLLUP ( <span class="var">expression</span> [, ...] ) }
</pre>

The `GROUP BY` clause groups together rows in a table with non-distinct values
for the `expression` in the `GROUP BY` clause. For multiple rows in the
source table with non-distinct values for `expression`, the
`GROUP BY` clause produces a single combined row. `GROUP BY` is commonly used
when aggregate functions are present in the `SELECT` list, or to eliminate
redundancy in the output. The data type of `expression` must be [groupable][data-type-properties].

Example:

```
SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY LastName;
```

The `GROUP BY` clause can refer to expression names in the `SELECT` list. The
`GROUP BY` clause also allows ordinal references to expressions in the `SELECT`
list using integer values. `1` refers to the first expression in the
`SELECT` list, `2` the second, and so forth. The expression list can combine
ordinals and expression names.

Example:

```
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY LastName, FirstName;
```

The query above is equivalent to:

```
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY 2, FirstName;
```

`GROUP BY` clauses may also refer to aliases. If a query contains aliases in
the `SELECT` clause, those aliases override names in the corresponding `FROM`
clause.

Example:

```
SELECT SUM(PointsScored), LastName as last_name
FROM PlayerStats
GROUP BY last_name;
```

`GROUP BY` can group rows by the value of an `ARRAY`.
`GROUP BY` will group two arrays if they have the same number of elements and
all corresponding elements are in the same groups, or if both arrays are null.

`GROUP BY ROLLUP` returns the results of `GROUP BY` for
prefixes of the expressions in the `ROLLUP` list, each of which is known as a
*grouping set*.  For the `ROLLUP` list `(a, b, c)`, the grouping sets are
`(a, b, c)`, `(a, b)`, `(a)`, `()`. When evaluating the results of `GROUP BY`
for a particular grouping set, `GROUP BY ROLLUP` treats expressions that are not
in the grouping set as having a `NULL` value. A `SELECT` statement like this
one:

```
SELECT a,    b,    SUM(c) FROM Input GROUP BY ROLLUP(a, b);
```

uses the rollup list `(a, b)`. The result will include the
results of `GROUP BY` for the grouping sets `(a, b)`, `(a)`, and `()`, which
includes all rows. This returns the same rows as:

```
SELECT NULL, NULL, SUM(c) FROM Input               UNION ALL
SELECT a,    NULL, SUM(c) FROM Input GROUP BY a    UNION ALL
SELECT a,    b,    SUM(c) FROM Input GROUP BY a, b;
```

This allows the computation of aggregates for the grouping sets defined by the
expressions in the `ROLLUP` list and the prefixes of that list.

Example:

```
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(day);
```

The query above outputs a row for each day in addition to the rolled up total
across all days, as indicated by a `NULL` day:

```
+------+-------+
| day  | total |
+------+-------+
| NULL | 39.77 |
|    1 | 23.54 |
|    2 |  9.99 |
|    3 |  6.24 |
+------+-------+
```

Example:

```
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  sku,
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(sku, day)
ORDER BY sku, day;
```

The query above returns rows grouped by the following grouping sets:

+ sku and day
+ sku (day is `NULL`)
+ The empty grouping set (day and sku are `NULL`)

The sums for these grouping sets correspond to the total for each
distinct sku-day combination, the total for each sku across all days, and the
grand total:

```
+------+------+-------+
| sku  | day  | total |
+------+------+-------+
| NULL | NULL | 39.77 |
|  123 | NULL | 28.97 |
|  123 |    1 | 18.98 |
|  123 |    2 |  9.99 |
|  456 | NULL |  8.81 |
|  456 |    1 |  4.56 |
|  456 |    3 |  4.25 |
|  789 |    3 |  1.99 |
|  789 | NULL |  1.99 |
+------+------+-------+
```

<a id="having_clause"></a>
### HAVING clause

#### Syntax

```
HAVING bool_expression
```

The `HAVING` clause is similar to the `WHERE` clause: it filters out rows that
do not return TRUE when they are evaluated against the `bool_expression`.

As with the `WHERE` clause, the `bool_expression` can be any expression
that returns a boolean, and can contain multiple sub-conditions.

The `HAVING` clause differs from the `WHERE` clause in that:

  * The `HAVING` clause requires `GROUP BY` or aggregation to be present in the
     query.
  * The `HAVING` clause occurs after `GROUP BY` and aggregation, and before
     `ORDER BY`. This means that the `HAVING` clause is evaluated once for every
     aggregated row in the result set. This differs from the `WHERE` clause,
     which is evaluated before `GROUP BY` and aggregation.

The `HAVING` clause can reference columns available via the `FROM` clause, as
well as `SELECT` list aliases. Expressions referenced in the `HAVING` clause
must either appear in the `GROUP BY` clause or they must be the result of an
aggregate function:

```
SELECT LastName
FROM Roster
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

If a query contains aliases in the `SELECT` clause, those aliases override names
in a `FROM` clause.

```
SELECT LastName, SUM(PointsScored) AS ps
FROM Roster
GROUP BY LastName
HAVING ps > 0;
```

<a id="mandatory_aggregation"></a>
#### Mandatory aggregation

Aggregation does not have to be present in the `HAVING` clause itself, but
aggregation must be present in at least one of the following forms:

##### Aggregation function in the `SELECT` list.

```
SELECT LastName, SUM(PointsScored) AS total
FROM PlayerStats
GROUP BY LastName
HAVING total > 15;
```

##### Aggregation function in the 'HAVING' clause.

```
SELECT LastName
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

##### Aggregation in both the `SELECT` list and `HAVING` clause.

When aggregation functions are present in both the `SELECT` list and `HAVING`
clause, the aggregation functions and the columns they reference do not need
to be the same. In the example below, the two aggregation functions,
`COUNT()` and `SUM()`, are different and also use different columns.

```
SELECT LastName, COUNT(*)
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

<a id="order_by_clause"></a>
### ORDER BY clause

#### Syntax

<pre>
ORDER BY expression
  [{ ASC | DESC }]
  [, ...]
</pre>

The `ORDER BY` clause specifies a column or expression as the sort criterion for
the result set. If an ORDER BY clause is not present, the order of the results
of a query is not defined. Column aliases from a `FROM` clause or `SELECT` list
are allowed. If a query contains aliases in the `SELECT` clause, those aliases
override names in the corresponding `FROM` clause.

**Optional Clauses**

+  `ASC | DESC`: Sort the results in ascending or descending
    order of `expression` values. `ASC` is the default value. 

**Examples**

Use the default sort order (ascending).

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true)
ORDER BY x;
+------+-------+
| x    | y     |
+------+-------+
| 1    | true  |
| 9    | true  |
+------+-------+
```

Use descending sort order.

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true)
ORDER BY x DESC;
+------+-------+
| x    | y     |
+------+-------+
| 9    | true  |
| 1    | true  |
+------+-------+
```

It is possible to order by multiple columns. In the example below, the result
set is ordered first by `SchoolID` and then by `LastName`:

```sql
SELECT LastName, PointsScored, OpponentID
FROM PlayerStats
ORDER BY SchoolID, LastName;
```

The following rules apply when ordering values:

+  NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
   possible value; that is, NULLs appear first in `ASC` sorts and last in `DESC`
   sorts.
+  Floating point data types: see
   [Floating Point Semantics][floating-point-semantics]
   on ordering and grouping.

When used in conjunction with [set operators][set-operators], the `ORDER BY` clause applies to the result set of the entire query; it does not
apply only to the closest `SELECT` statement. For this reason, it can be helpful
(though it is not required) to use parentheses to show the scope of the `ORDER
BY`.

This query without parentheses:

```sql
SELECT * FROM Roster
UNION ALL
SELECT * FROM TeamMascot
ORDER BY SchoolID;
```

is equivalent to this query with parentheses:

```sql
( SELECT * FROM Roster
  UNION ALL
  SELECT * FROM TeamMascot )
ORDER BY SchoolID;
```

but is not equivalent to this query, where the `ORDER BY` clause applies only to
the second `SELECT` statement:

```sql
SELECT * FROM Roster
UNION ALL
( SELECT * FROM TeamMascot
  ORDER BY SchoolID );
```

You can also use integer literals as column references in `ORDER BY` clauses. An
integer literal becomes an ordinal (for example, counting starts at 1) into
the `SELECT` list.

Example - the following two queries are equivalent:

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY LastName;
```

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY 2;
```

#### COLLATE

You can use the `COLLATE` clause to refine how a data is ordered from an `ORDER
BY` clause. *Collation* refers to a set of rules that determine how
STRINGs are compared according to the conventions and
standards of a particular language, region or country. These rules might define
the correct character sequence, with options for specifying case-insensitivity.

<p class="note">Note: You can use <code>COLLATE</code> only on columns of type
STRING.</p>

You add collation to your statement as follows:

```
SELECT ...
FROM ...
ORDER BY value COLLATE collation_string
```

A `collation_string` contains a `collation_name` and can have an optional
`collation_attribute` as a suffix, separated by a colon. The `collation_string`
is a literal or a parameter.  Usually, this name is two letters that represent
the language optionally followed by an underscore and two letters that
represent the region&mdash;for example, `en_US`. These names are defined by the
[Common Locale Data Repository (CLDR)][language-territory-information].
A statement can also have a `collation_name` of `unicode`. This value means that
the statement should return data using the default unicode collation.

In addition to the `collation_name`, a `collation_string` can have an optional
`collation_attribute` as a suffix, separated by a colon. This attribute
specifies if the data comparisons should be case sensitive. Allowed values are
`cs`, for case sensitive, and `ci`, for case insensitive. If a
`collation_attribute` is not supplied, the
[CLDR defaults][tr35-collation-settings]
are used.

##### COLLATE examples

Collate results using English - Canada:

```
SELECT Place
FROM Locations
ORDER BY Place COLLATE "en_CA"
```

Collate results using a parameter:

```
#@collate_param = "arg_EG"
SELECT Place
FROM Locations
ORDER BY Place COLLATE @collate_param
```

Using multiple `COLLATE` clauses in a statement:

```
SELECT APlace, BPlace, CPlace
FROM Locations
ORDER BY APlace COLLATE "en_US" ASC,
         BPlace COLLATE "ar_EG" DESC,
         CPlace COLLATE "en" DESC
```

Case insensitive collation:

```
SELECT Place
FROM Locations
ORDER BY Place COLLATE "en_US:ci"
```

Default Unicode case-insensitive collation:

```
SELECT Place
FROM Locations
ORDER BY Place COLLATE "unicode:ci"
```

<a id="window_clause"></a>
### WINDOW clause

#### Syntax

<pre>
WINDOW named_window_expression [, ...]

named_window_expression:
  named_window AS { named_window | ( [ window_specification ] ) }
</pre>

A `WINDOW` clause defines a list of named windows.
A named window represents a group of rows in a table upon which to use an
[analytic function][analytic-concepts]. A named window can be defined with
a [window specification][query-window-specification] or reference another
named window. If another named window is referenced, the definition of the
referenced window must precede the referencing window.

**Examples**

These examples reference a table called [`Produce`][produce-table].
They all return the same [result][named-window-example]. Note the different
ways you can combine named windows and use them in an analytic function's
`OVER` clause.

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (item_window) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases
  ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
```

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (d) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  c AS (b ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),
  d AS (c)
```

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (c ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  c AS b
```

<a id="set_operators"></a>
### Set operators

#### Syntax

<pre>
UNION { ALL | DISTINCT } | <a href="#intersect">INTERSECT</a> { ALL | DISTINCT } | <a href="#except">EXCEPT</a> { ALL | DISTINCT }
</pre>

Set operators combine results from two or
more input queries into a single result set. You
must specify `ALL` or `DISTINCT`; if you specify `ALL`, then all rows are
retained.  If `DISTINCT` is specified, duplicate rows are discarded.

<p>If a given row R appears exactly m times in the first input query and n times
in the second input query (m >= 0, n >= 0):</p>

<ul>
<li>For <code>UNION ALL</code>, R appears exactly m + n times in the result.
</li><li>For <code>INTERSECT ALL</code>, R will appear
exactly `MIN(m, n)` in the result.</li><li>For
<code>EXCEPT ALL</code>, R appears exactly `MAX(m - n, 0)` in the result.
</li><li>For <code>UNION DISTINCT</code>, the <code>DISTINCT</code>
is computed after the <code>UNION</code> is computed, so R appears exactly one
time.</li><li>For <code>INTERSECT DISTINCT</code>, the
<code>DISTINCT</code> is computed after the result above is computed.
</li><li>For <code>EXCEPT DISTINCT</code>, row
R appears once in the output if m > 0 and n = 0.</li>
<li>If there are more than two input queries, the above operations generalize
and the output is the same as if the inputs were combined incrementally from
left to right.</li>
</ul>

The following rules apply:

+  For set operations other than `UNION
   ALL`, all column types must support
   equality comparison.
+  The input queries on each side of the operator must return the same
   number of columns.
+  The operators pair the columns returned by each input query according to
   the columns' positions in their respective `SELECT` lists. That is, the first
   column in the first input query is paired with the first column in the second
   input query.
+  The result set always uses the column names from the first input query.
+  The result set always uses the supertypes of input types in corresponding
   columns, so paired columns must also have either the same data type or a
   common supertype.
+  You must use parentheses to separate different set operations; for
   this purpose, set operations such as `UNION ALL` and `UNION DISTINCT` are
   different. If the statement only repeats the same set operation,
   parentheses are not necessary.

Examples:

```
query1 UNION ALL (query2 UNION DISTINCT query3)
query1 UNION ALL query2 UNION ALL query3
```

Invalid:

<pre>
query1 UNION ALL query2 UNION DISTINCT query3<br>query1 UNION ALL query2 INTERSECT ALL query3;  // INVALID.
</pre>

<a id="union"></a>
#### UNION

The `UNION` operator combines the result sets of two or more input queries by
pairing columns from the result set of each query and vertically concatenating
them.

<a id="intersect"></a>
#### INTERSECT

The `INTERSECT` operator returns rows that are found in the result sets of both
the left and right input queries. Unlike `EXCEPT`, the positioning of the input
queries (to the left vs. right of the `INTERSECT` operator) does not matter.

<a id="except"></a>
#### EXCEPT

The `EXCEPT` operator returns rows from the left input query that are
not present in the right input query.

Example:

```sql
SELECT * FROM UNNEST(ARRAY<int64>[1, 2, 3]) AS number
EXCEPT DISTINCT SELECT 1;

+--------+
| number |
+--------+
| 2      |
| 3      |
+--------+
```

<a id="limit-clause_and_offset_clause"></a>
### LIMIT clause and OFFSET clause

#### Syntax

```
LIMIT count [ OFFSET skip_rows ]
```

`LIMIT` specifies a non-negative `count` of type INT64,
and no more than `count` rows will be returned. `LIMIT` `0` returns 0 rows.

If there is a set operation, `LIMIT` is applied after the set operation is
evaluated.

`OFFSET` specifies a non-negative number of rows to skip before applying
`LIMIT`. `skip_rows` is of type INT64.

These clauses accept only literal or parameter values. The rows that are
returned by `LIMIT` and `OFFSET` are unspecified unless these
operators are used after `ORDER BY`.

Examples:

```sql
SELECT *
FROM UNNEST(ARRAY<STRING>['a', 'b', 'c', 'd', 'e']) AS letter
ORDER BY letter ASC LIMIT 2

+---------+
| letter  |
+---------+
| a       |
| b       |
+---------+
```

```sql
SELECT *
FROM UNNEST(ARRAY<STRING>['a', 'b', 'c', 'd', 'e']) AS letter
ORDER BY letter ASC LIMIT 3 OFFSET 1

+---------+
| letter  |
+---------+
| b       |
| c       |
| d       |
+---------+
```

<a id="with_clause"></a>
### WITH clause

The `WITH` clause binds the results of one or more named subqueries to temporary
table names.  Each introduced table name is visible in subsequent `SELECT`
expressions within the same query expression. This includes the following kinds
of `SELECT` expressions:

+ Any `SELECT` expressions in subsequent `WITH` bindings
+ Top level `SELECT` expressions in the query expression on both sides of a set
  operator such as `UNION`
+ `SELECT` expressions inside subqueries within the same query expression

Example:

```
WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2;
```

Another useful role of the `WITH` clause is to break up more complex queries
into a `WITH` `SELECT` statement and `WITH` clauses, where the less desirable
alternative is writing nested table subqueries. If a `WITH` clause contains
multiple subqueries, the subquery names cannot repeat.

ZetaSQL supports `WITH` clauses in subqueries, such as table
subqueries, expression subqueries, and so on.

```
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1) SELECT * FROM q2)
```

The following are scoping rules for `WITH` clauses:

+ Aliases are scoped so that the aliases introduced in a `WITH` clause are
  visible only in the later subqueries in the same `WITH` clause, and in the
  query under the `WITH` clause.
+ Aliases introduced in the same `WITH` clause must be unique, but the same
  alias can be used in multiple `WITH` clauses in the same query.  The local
  alias overrides any outer aliases anywhere that the local alias is visible.
+ Aliased subqueries in a `WITH` clause can never be correlated. No columns from
  outside the query are visible.  The only names from outside that are visible
  are other `WITH` aliases that were introduced earlier in the same `WITH`
  clause.

Here's an example of a statement that uses aliases in `WITH` subqueries:

```
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
        q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery
                                   # on the previous line.
    SELECT * FROM q1)  # q1 resolves to the third inner WITH subquery.
```

NOTE: ZetaSQL does not support `WITH RECURSIVE`.

<a name="using_aliases"></a>
### Using Aliases

An alias is a temporary name given to a table, column, or expression present in
a query. You can introduce explicit aliases in the `SELECT` list or `FROM`
clause, or ZetaSQL will infer an implicit alias for some expressions.
Expressions with neither an explicit nor implicit alias are anonymous and the
query cannot reference them by name.

<a id=explicit_alias_syntax></a>
#### Explicit alias syntax

You can introduce explicit aliases in either the `FROM` clause or the `SELECT`
list.

In a `FROM` clause, you can introduce explicit aliases for any item, including
tables, arrays, subqueries, and `UNNEST` clauses, using `[AS] alias`.  The `AS`
keyword is optional.

Example:

```
SELECT s.FirstName, s2.SongName
FROM Singers AS s, (SELECT * FROM Songs) AS s2;
```

You can introduce explicit aliases for any expression in the `SELECT` list using
`[AS] alias`. The `AS` keyword is optional.

Example:

```
SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname
FROM Singers s;
```

<a id=alias_visibility></a>
#### Explicit alias visibility

After you introduce an explicit alias in a query, there are restrictions on
where else in the query you can reference that alias. These restrictions on
alias visibility are the result of ZetaSQL's name scoping rules.

<a id=from_clause_aliases></a>
##### FROM clause aliases

ZetaSQL processes aliases in a `FROM` clause from left to right,
and aliases are visible only to subsequent path expressions in a `FROM`
clause.

Example:

Assume the `Singers` table had a `Concerts` column of `ARRAY` type.

```
SELECT FirstName
FROM Singers AS s, s.Concerts;
```

Invalid:

```
SELECT FirstName
FROM s.Concerts, Singers AS s;  // INVALID.
```

`FROM` clause aliases are **not** visible to subqueries in the same `FROM`
clause. Subqueries in a `FROM` clause cannot contain correlated references to
other tables in the same `FROM` clause.

Invalid:

```
SELECT FirstName
FROM Singers AS s, (SELECT (2020 - ReleaseDate) FROM s)  // INVALID.
```

You can use any column name from a table in the `FROM` as an alias anywhere in
the query, with or without qualification with the table name.

Example:

```
SELECT FirstName, s.ReleaseDate
FROM Singers s WHERE ReleaseDate = 1975;
```

If the `FROM` clause contains an explicit alias, you must use the explicit alias
instead of the implicit alias for the remainder of the query (see
[Implicit Aliases][implicit-aliases]). A table alias is useful for brevity or
to eliminate ambiguity in cases such as self-joins, where the same table is
scanned multiple times during query processing.

Example:

```
SELECT * FROM Singers as s, Songs as s2
ORDER BY s.LastName
```

Invalid &mdash; `ORDER BY` does not use the table alias:

```
SELECT * FROM Singers as s, Songs as s2
ORDER BY Singers.LastName;  // INVALID.
```

<a id=select-list_aliases></a>
##### SELECT list aliases

Aliases in the `SELECT` list are **visible only** to the following clauses:

+  `GROUP BY` clause
+  `ORDER BY` clause
+  `HAVING` clause

Example:

```
SELECT LastName AS last, SingerID
FROM Singers
ORDER BY last;
```

<a id=aliases_clauses></a>
#### Explicit aliases in GROUP BY, ORDER BY, and HAVING clauses

These three clauses, `GROUP BY`, `ORDER BY`, and `HAVING`, can refer to only the
following values:

+  Tables in the `FROM` clause and any of their columns.
+  Aliases from the `SELECT` list.

`GROUP BY` and `ORDER BY` can also refer to a third group:

+  Integer literals, which refer to items in the `SELECT` list. The integer `1`
   refers to the first item in the `SELECT` list, `2` refers to the second item,
   etc.

Example:

```
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY 1
ORDER BY 2 DESC;
```

The query above is equivalent to:

```
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY sid
ORDER BY s2id DESC;
```

<a id=ambiguous_aliases></a>
#### Ambiguous aliases

ZetaSQL provides an error if a name is ambiguous, meaning it can
resolve to more than one unique object.

Examples:

This query contains column names that conflict between tables, since both
`Singers` and `Songs` have a column named `SingerID`:

```
SELECT SingerID
FROM Singers, Songs;
```

This query contains aliases that are ambiguous in the `GROUP BY` clause because
they are duplicated in the `SELECT` list:

```
SELECT FirstName AS name, LastName AS name,
FROM Singers
GROUP BY name;
```

This query contains aliases that are ambiguous in the `SELECT` list and `FROM`
clause because they share the same name. Assume `table` has columns `x`, `y`,
and `z`. `z` is of type STRUCT and has fields
`v`, `w`, and `x`.

Example:

```
SELECT x, z AS T
FROM table AS T
GROUP BY T.x;
```

The alias `T` is ambiguous and will produce an error because `T.x` in the `GROUP
BY` clause could refer to either `table.x` or `table.z.x`.

A name is **not** ambiguous in `GROUP BY`, `ORDER BY` or `HAVING` if it is both
a column name and a `SELECT` list alias, as long as the name resolves to the
same underlying object.

Example:

```
SELECT LastName, BirthYear AS BirthYear
FROM Singers
GROUP BY BirthYear;
```

The alias `BirthYear` is not ambiguous because it resolves to the same
underlying column, `Singers.BirthYear`.

<a id=implicit_aliases></a>
#### Implicit aliases

In the `SELECT` list, if there is an expression that does not have an explicit
alias, ZetaSQL assigns an implicit alias according to the following
rules. There can be multiple columns with the same alias in the `SELECT` list.

+  For identifiers, the alias is the identifier. For example, `SELECT abc`
   implies `AS abc`.
+  For path expressions, the alias is the last identifier in the path. For
   example, `SELECT abc.def.ghi` implies `AS ghi`.
+  For field access using the "dot" member field access operator, the alias is
   the field name. For example, `SELECT (struct_function()).fname` implies `AS
   fname`.

In all other cases, there is no implicit alias, so the column is anonymous and
cannot be referenced by name. The data from that column will still be returned
and the displayed query results may have a generated label for that column, but
the label cannot be used like an alias.

In a `FROM` clause, `from_item`s are not required to have an alias. The
following rules apply:

<ul>
<li>If there is an expression that does not have an explicit alias, ZetaSQL assigns an implicit alias in these cases:</li>
<ul>
<li>For identifiers, the alias is the identifier. For example, <code>FROM abc</code>
     implies <code>AS abc</code>.</li>
<li>For path expressions, the alias is the last identifier in the path. For
     example, <code>FROM abc.def.ghi</code> implies <code>AS ghi</code></li>
<li>The column produced using <code>WITH OFFSET</code> has the implicit alias <code>offset</code>.</li>
</ul>
<li>Table subqueries do not have implicit aliases.</li>
<li><code>FROM UNNEST(x)</code> does not have an implicit alias.</li>
</ul>

<a id=appendix_a_examples_with_sample_data></a>
### Appendix A: examples with sample data

<a id=sample_tables></a>
#### Sample tables

The following three tables contain sample data about athletes, their schools,
and the points they score during the season. These tables will be used to
illustrate the behavior of different query clauses.

Table Roster:

<table>
<thead>
<tr>
<th>LastName</th>
<th>SchoolID</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
</tr>
</tbody>
</table>

The Roster table includes a list of player names (LastName) and the unique ID
assigned to their school (SchoolID).

Table PlayerStats:

<table>
<thead>
<tr>
<th>LastName</th>
<th>OpponentID</th>
<th>PointsScored</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>51</td>
<td>3</td>
</tr>
<tr>
<td>Buchanan</td>
<td>77</td>
<td>0</td>
</tr>
<tr>
<td>Coolidge</td>
<td>77</td>
<td>1</td>
</tr>
<tr>
<td>Adams</td>
<td>52</td>
<td>4</td>
</tr>
<tr>
<td>Buchanan</td>
<td>50</td>
<td>13</td>
</tr>
</tbody>
</table>

The PlayerStats table includes a list of player names (LastName) and the unique
ID assigned to the opponent they played in a given game (OpponentID) and the
number of points scored by the athlete in that game (PointsScored).

Table TeamMascot:

<table>
<thead>
<tr>
<th>SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>

The TeamMascot table includes a list of unique school IDs (SchoolID) and the
mascot for that school (Mascot).

<a id="join_types_examples"></a>
#### JOIN types

1) [INNER] JOIN

Example:

```
SELECT * FROM Roster JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
</tbody>
</table>

2) CROSS JOIN

Example:

```
SELECT * FROM Roster CROSS JOIN TeamMascot;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Adams</td>
<td>50</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Adams</td>
<td>50</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Adams</td>
<td>50</td>
<td>53</td>
<td>Mustangs</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>53</td>
<td>Mustangs</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>53</td>
<td>Mustangs</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>53</td>
<td>Mustangs</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>

3) FULL [OUTER] JOIN

Example:

```
SELECT * FROM Roster FULL JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>NULL</td>
<td>NULL</td>
</tr>
<tr>
<td>NULL</td>
<td>NULL</td>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>

4) LEFT [OUTER] JOIN

Example:

```
SELECT * FROM Roster LEFT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>

5) RIGHT [OUTER] JOIN

Example:

```
SELECT * FROM Roster RIGHT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>NULL</td>
<td>NULL</td>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>

<a id=group_by_clause></a>
#### GROUP BY clause

Example:

```
SELECT LastName, SUM(PointsScored)
FROM PlayerStats
GROUP BY LastName;
```

<table>
<thead>
<tr>
<th>LastName</th>
<th>SUM</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>7</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
</tbody>
</table>

<a id=set_operators></a>
#### Set operators

<a id=union></a>

##### UNION

The `UNION` operator combines the result sets of two or more `SELECT` statements
by pairing columns from the result set of each `SELECT` statement and vertically
concatenating them.

Example:

```
SELECT Mascot AS X, SchoolID AS Y
FROM TeamMascot
UNION ALL
SELECT LastName, PointsScored
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>X</th>
<th>Y</th>
</tr>
</thead>
<tbody>
<tr>
<td>Jaguars</td>
<td>50</td>
</tr>
<tr>
<td>Knights</td>
<td>51</td>
</tr>
<tr>
<td>Lakers</td>
<td>52</td>
</tr>
<tr>
<td>Mustangs</td>
<td>53</td>
</tr>
<tr>
<td>Adams</td>
<td>3</td>
</tr>
<tr>
<td>Buchanan</td>
<td>0</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
<tr>
<td>Adams</td>
<td>4</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
</tbody>
</table>

<a id=intersect></a>
##### INTERSECT

This query returns the last names that are present in both Roster and
PlayerStats.

```
SELECT LastName
FROM Roster
INTERSECT ALL
SELECT LastName
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
</tr>
<tr>
<td>Coolidge</td>
</tr>
<tr>
<td>Buchanan</td>
</tr>
</tbody>
</table>

<a id=except></a>
##### EXCEPT

The query below returns last names in Roster that are **not** present in
PlayerStats.

```
SELECT LastName
FROM Roster
EXCEPT DISTINCT
SELECT LastName
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Eisenhower</td>
</tr>
<tr>
<td>Davis</td>
</tr>
</tbody>
</table>

Reversing the order of the `SELECT` statements will return last names in
PlayerStats that are **not** present in Roster:

```
SELECT LastName
FROM PlayerStats
EXCEPT DISTINCT
SELECT LastName
FROM Roster;
```

Results:

```
(empty)
```

[language-territory-information]: http://www.unicode.org/cldr/charts/latest/supplemental/language_territory_information.html
[tr35-collation-settings]: http://www.unicode.org/reports/tr35/tr35-collation.html#Setting_Options

[implicit-aliases]: #implicit_aliases
[subqueries]: #subqueries
[using-aliases]: #using_aliases
[sequences-of-joins]: #sequences_of_joins
[set-operators]: #set_operators
[union-syntax]: #union
[join-hints]: #join_hints
[query-value-tables]: #value_tables
[analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[query-window-specification]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#def_window_spec
[named-window-example]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#def_use_named_window
[produce-table]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#produce-table
[flattening-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays#flattening_arrays
[working-with-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays
[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types#data-type-properties
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating-point-semantics

[analytic-concepts]: #analytic-functions-concepts
[query-window-specification]: #def_window_spec
[named-window-example]: #def_use_named_window
[produce-table]: #produce-table
[flattening-arrays]: #flattening-arrays
[in-operator]: #in-operators
[query-value-tables]: #value-tables
[working-with-arrays]: #working-with-arrays
[expression-subqueries]: #expression_subqueries
[data-type-properties]: #data-type-properties
[floating-point-semantics]: #floating-point-semantics

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Analytic Functions Concepts

<!-- BEGIN CONTENT -->

An analytic function computes values over a group of rows and returns a
single result for _each_ row. This is different from an aggregate function,
which returns a single result for _an entire group_ of rows.

It includes an `OVER` clause, which defines a window of rows
around the row being evaluated.  For each row, the analytic function result
is computed using the selected window of rows as input, possibly
doing aggregation.

With analytic functions you can compute moving averages, rank items, calculate
cumulative sums, and perform other analyses.

Analytic functions include the following categories:
[navigation functions][navigation-functions-reference],
[numbering functions][numbering-functions-reference], and
[aggregate analytic functions][aggregate-analytic-functions-reference].

<a name="syntax"></a>
### Analytic Function Syntax

<pre>
analytic_function_name ( [ argument_list ] ) OVER over_clause

<a href="#def_over_clause">over_clause</a>:
  { named_window | ( [ window_specification ] ) }

<a href="#def_window_spec">window_specification</a>:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

<a href="#def_window_frame">window_frame_clause</a>:
  { rows_range } { <a href="#def_window_frame">frame_start</a> | <a href="#def_window_frame">frame_between</a> }

<a href="#def_window_frame">rows_range</a>:
  { ROWS | RANGE }
</pre>

Notation:

+ Square brackets "[ ]" indicate optional clauses.
+ Parentheses "( )" indicate literal parentheses.
+ The vertical bar "|" indicates a logical OR.
+ Curly braces "{ }" enclose a set of options.
+ A comma followed by an ellipsis within square brackets "[, ... ]" indicates that
  the preceding item can repeat in a comma-separated list.

**Description**

An analytic function computes results over a group of rows.
These functions can be used as analytic functions:
[navigation functions][navigation-functions-reference],
[numbering functions][numbering-functions-reference], and
[aggregate analytic functions][aggregate-analytic-functions-reference]

+  `analytic_function_name`: The function that performs an analytic operation.
   For example, the numbering function RANK() could be used here.
+  `argument_list`: Arguments that are specific to the analytic function.
   Some functions have them, some do not.
+  `OVER`: Keyword required in the analytic function syntax preceding
   the [`OVER` clause][over-clause-def].

**Notes**

+  An analytic function can appear as a scalar expression operand in
   two places in the query:
   +  The `SELECT` list. If the analytic function appears in the `SELECT` list,
      its argument list and `OVER` clause cannot refer to aliases introduced
      in the same SELECT list.
   +  The `ORDER BY` clause. If the analytic function appears in the `ORDER BY`
      clause of the query, its argument list can refer to `SELECT`
      list aliases.
+  An analytic function cannot refer to another analytic function in its
   argument list or its `OVER` clause, even indirectly through an alias.
+  An analytic function is evaluated after aggregation. For example, the
   `GROUP BY` clause and non-analytic aggregate functions are evaluated first.
   Because aggregate functions are evaluated before analytic functions,
   aggregate functions can be used as input operands to analytic functions.

**Returns**

A single result for each row in the input.

#### Defining the `OVER` clause {: #def_over_clause }

```zetasql
analytic_function_name ( [ argument_list ] ) OVER over_clause

over_clause:
  { named_window | ( [ window_specification ] ) }
```

**Description**

The `OVER` clause references a window that defines a group of rows in a table
upon which to use an analytic function. You can provide a
[`named_window`][named-windows] that is
[defined in your query][analytic-functions-link-to-window], or you can
define the [specifications for a new window][window-specs-def].

**Notes**

If neither a named window nor window specification is provided, all
input rows are included in the window for every row.

**Examples using the `OVER` clause**

These queries use window specifications:

+  [Compute a grand total][analytic-functions-compute-grand-total]
+  [Compute a subtotal][analytic-functions-compute-subtotal]
+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Compute a moving average][analytic-functions-compute-moving-avg]
+  [Compute the number of items within a range][analytic-functions-compute-item-range]
+  [Get the most popular item in each category][analytic-functions-get-popular-item]
+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Compute rank][analytic-functions-compute-rank]

These queries use a named window:

+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

#### Defining the window specification {: #def_window_spec }

```zetasql
window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC } ] [, ...] ]
  [ window_frame_clause ]
```

**Description**

Defines the specifications for the window.

+  [`named_window`][named-windows]: The name of an existing window that was
   defined with a [`WINDOW` clause][analytic-functions-link-to-window].

   Important: If you use a named window, special rules apply to
   `PARTITION BY`, `ORDER BY`, and `window_frame_clause`. See them [here][named-window-rules].
+  `PARTITION BY`: Breaks up the input rows into separate partitions, over
   which the analytic function is independently evaluated.
   +  Multiple partition expressions are allowed in the `PARTITION BY` clause.
   +  An expression cannot contain floating point types, non-groupable types,
      constants, or analytic functions.
   +  If this optional clause is not used, all rows in the input table
      comprise a single partition.
+  `ORDER BY`: Defines how rows are ordered within a partition.
   This clause is optional in most situations, but is required in some
   cases for [navigation functions][navigation-functions-reference].
+  [`window_frame_clause`][window-frame-clause-def]: For aggregate analytic
   functions, defines the window frame within the current partition.
   The window frame determines what to include in the window.
   If this clause is used, `ORDER BY` is required except for fully
   unbounded windows.

**Notes**

+  If neither the `ORDER BY` clause nor window frame clause are present,
   the window frame includes all rows in that partition.
+  For aggregate analytic functions, if the `ORDER BY` clause is present but
   the window frame clause is not, the following window frame clause is
   used by default:

   ```zetasql
   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
   ```

   For example, the following queries are equivalent:

   ```zetasql
   SELECT book, LAST_VALUE(item)
     OVER (ORDER BY year)
   FROM Library
   ```

   ```zetasql
   SELECT book, LAST_VALUE(item)
     OVER (
       ORDER BY year
       RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
   FROM Library
   ```
+  [Hints][analytic-functions-link-to-hints] are supported on the `PARTITION BY`
   clause and the `ORDER BY` clause.

<a id="named_window_rules"></a>
**Rules for using a named window in the window specification**

If you use a named window in your window specifications, these rules apply:

+  The specifications in the named window can be extended
   with new specifications that you define in the window specification clause.
+  You can't have redundant definitions. If you have an `ORDER BY` clause
   in the named window and the window specification clause, an
   error is thrown.
+  The order of clauses matters. `PARTITION BY` must come first,
   followed by `ORDER BY` and `window_frame_clause`. If you add a named window,
   its window specifications are processed first.

   ```zetasql
   --this works:
   SELECT item, purchases, LAST_VALUE(item)
     OVER (item_window ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular
   FROM Produce
   WINDOW item_window AS (ORDER BY purchases)

   --this does not work:
   SELECT item, purchases, LAST_VALUE(item)
     OVER (item_window ORDER BY purchases) AS most_popular
   FROM Produce
   WINDOW item_window AS (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
   ```
+  A named window and `PARTITION BY` cannot appear together in the
   window specification. If you need `PARTITION BY`, add it to the named window.
+  You cannot refer to a named window in an `ORDER BY` clause, an outer query,
   or any subquery.

**Examples using the window specification**

These queries define partitions in an analytic function:

+  [Compute a subtotal][analytic-functions-compute-subtotal]
+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Get the most popular item in each category][analytic-functions-get-popular-item]
+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Compute rank][analytic-functions-compute-rank]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

These queries include a named window in a window specification:

+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

These queries define how rows are ordered in a partition:

+  [Compute a subtotal][analytic-functions-compute-subtotal]
+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Compute a moving average][analytic-functions-compute-moving-avg]
+  [Compute the number of items within a range][analytic-functions-compute-item-range]
+  [Get the most popular item in each category][analytic-functions-get-popular-item]
+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Compute rank][analytic-functions-compute-rank]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

#### Defining the window frame clause {: #def_window_frame }

```zetasql
window_frame_clause:
  { rows_range } { frame_start | frame_between }

rows_range:
  { ROWS | RANGE }

frame_between:
  {
    BETWEEN  unbounded_preceding AND frame_end_a
    | BETWEEN numeric_preceding AND frame_end_a
    | BETWEEN current_row AND frame_end_b
    | BETWEEN numeric_following AND frame_end_c

frame_start:
  { unbounded_preceding | numeric_preceding | [ current_row ] }

frame_end_a:
  { numeric_preceding | current_row | numeric_following | unbounded_following }

frame_end_b:
  { current_row | numeric_following | unbounded_following }

frame_end_c:
  { numeric_following | unbounded_following }

unbounded_preceding:
  UNBOUNDED PRECEDING

numeric_preceding:
  numeric_expression PRECEDING

unbounded_following:
  UNBOUNDED FOLLOWING

numeric_following:
  numeric_expression FOLLOWING

current_row:
  CURRENT ROW
```

The window frame clause defines the window frame around the current row within
a partition, over which the analytic function is evaluated.
Only aggregate analytic functions can use a window frame clause.

+  `rows_range`: A clause that defines a window frame with physical rows
   or a logical range.
   +  `ROWS`: Computes the window frame based on physical offsets from the
      current row. For example, you could include two rows before and after
      the current row.
   +  `RANGE`: Computes the window frame based on a logical range of rows
      around the current row, based on the current row’s `ORDER BY` key value.
      The provided range value is added or subtracted to the current row's
      key value to define a starting or ending range boundary for the
      window frame. In a range-based window frame, there must be exactly one
      expression in the `ORDER BY` clause, and the expression must have a
      numeric type.

     Tip: If you want to use a range with a date, use `ORDER BY` with the
     `UNIX_DATE()` function. If you want to use a range with a timestamp,
     use the `UNIX_SECONDS()`, `UNIX_MILLIS()`, or `UNIX_MICROS()` function.
+  `frame_between`: Creates a window frame with a lower and upper boundary.
    The first boundary represents the lower boundary. The second boundary
    represents the upper boundary. Only certain boundary combinations can be
    used, as show in the syntax above.
    +  The following boundaries can be used to define the
       beginning of the window frame.
       +  `unbounded_preceding`: The window frame starts at the beginning of the
          partition.
       +  `numeric_preceding` or `numeric_following`: The start of the window
          frame is relative to the
          current row.
       +  `current_row`: The window frame starts at the current row.
    +  `frame_end_a ... frame_end_c`: Defines the end of the window frame.
        + `numeric_preceding` or `numeric_following`: The end of the window
          frame is relative to the current row.
        + `current_row`: The window frame ends at the current row.
        + `unbounded_following`: The window frame ends at the end of the
          partition.
+  `frame_start`: Creates a window frame with a lower boundary.
    The window frame ends at the current row.
    +  `unbounded_preceding`: The window frame starts at the beginning of the
        partition.
    +  `numeric_preceding`: The start of the window frame is relative to the
       current row.
    +  `current_row`: The window frame starts at the current row.
+  `numeric_expression`: An expression that represents a numeric type.
   The numeric expression must be a constant, non-negative integer
   or parameter.

**Notes**

+  If a boundary extends beyond the beginning or end of a partition,
   the window frame will only include rows from within that partition.
+  You cannot use a window frame clause with
   [navigation functions][analytic-functions-link-to-navigation-functions] and
   [numbering functions][analytic-functions-link-to-numbering-functions],
   such as  `RANK()`.

**Examples using the window frame clause**

These queries compute values with `ROWS`:

+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Compute a moving average][analytic-functions-compute-moving-avg]
+  [Get the most popular item in each category][analytic-functions-get-popular-item]
+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

These queries compute values with `RANGE`:

+  [Compute the number of items within a range][analytic-functions-compute-item-range]

These queries compute values with a partially or fully unbound window:

+  [Compute a grand total][analytic-functions-compute-grand-total]
+  [Compute a subtotal][analytic-functions-compute-subtotal]
+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Get the most popular item in each category][analytic-functions-get-popular-item]
+  [Compute rank][analytic-functions-compute-rank]

These queries compute values with numeric boundaries:

+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]
+  [Compute a moving average][analytic-functions-compute-moving-avg]
+  [Compute the number of items within a range][analytic-functions-compute-item-range]
+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

These queries compute values with the current row as a boundary:

+  [Compute a grand total][analytic-functions-compute-grand-total]
+  [Compute a subtotal][analytic-functions-compute-subtotal]
+  [Compute a cumulative sum][analytic-functions-compute-cumulative-sum]

#### Referencing a named window {: #ref_named_window }

```zetasql
SELECT query_expr,
  analytic_function_name ( [ argument_list ] ) OVER over_clause
FROM from_item
WINDOW named_window_expression [, ...]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ ASC | DESC [, ...] ]
  [ window_frame_clause ]

named_window_expression:
  named_window AS { named_window | ( [ window_specification ] ) }
```

A named window represents a group of rows in a table upon which to use an
analytic function. A named window is defined in the
[`WINDOW` clause][analytic-functions-link-to-window], and referenced in
an analytic function's [`OVER` clause][over-clause-def].
In an `OVER` clause, a named window can appear either by itself or embedded
within a [window specification][window-specs-def].

**Examples**

+  [Get the last value in a range][analytic-functions-get-last-value-range]
+  [Use a named window in a window frame clause][analytic-functions-use-named-window]

### Navigation Function Concepts

[Navigation functions][navigation-functions-reference] generally compute some `value_expression` over a different row in the window frame from the
current row. The `OVER` clause syntax varies across navigation functions.

Requirements for the `OVER` clause:

+   `PARTITION BY`: Optional.
+   `ORDER BY`:
    1.  Disallowed for `PERCENTILE_CONT` and `PERCENTILE_DISC`.
    1.   Required for `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD`
    and `LAG`.
+   `window_frame_clause`:
    1.  Disallowed for  `PERCENTILE_CONT`, `PERCENTILE_DISC`, `LEAD` and `LAG`.
    1.  Optional for `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`.

For all navigation functions, the result data type is the same type as
`value_expression`.

### Numbering Function Concepts

[Numbering functions][numbering-functions-reference] assign integer values to
each row based on their position within the specified window.

Example of `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`:

```zetasql
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
  RANK() OVER (ORDER BY x ASC) AS rank,
  DENSE_RANK() OVER (ORDER BY x ASC) AS dense_rank,
  ROW_NUMBER() OVER (ORDER BY x) AS row_num
FROM Numbers

+---------------------------------------------------+
| x          | rank       | dense_rank | row_num    |
+---------------------------------------------------+
| 1          | 1          | 1          | 1          |
| 2          | 2          | 2          | 2          |
| 2          | 2          | 2          | 3          |
| 5          | 4          | 3          | 4          |
| 8          | 5          | 4          | 5          |
| 10         | 6          | 5          | 6          |
| 10         | 6          | 5          | 7          |
+---------------------------------------------------+
```

* `RANK(): `For x=5, `rank` returns 4, since `RANK()` increments by the number
of peers in the previous window ordering group.
* `DENSE_RANK()`: For x=5, `dense_rank` returns 3, since `DENSE_RANK()` always
increments by 1, never skipping a value.
* `ROW_NUMBER(): `For x=5, `row_num` returns 4.

### Aggregate Analytic Function Concepts

An aggregate function is a function that performs a calculation on a
set of values. Most aggregate functions can be used in an
analytic function. These aggregate functions are called
[aggregate analytic functions][aggregate-analytic-functions-reference].

With aggregate analytic functions, the `OVER` clause is simply appended to the aggregate function call; the function call syntax remains otherwise unchanged.
Like their aggregate function counterparts, these analytic functions perform aggregations, but specifically over the relevant window frame for each row.
The result data types of these analytic functions are the same as their
aggregate function counterparts.

### Analytic Function Examples

In these examples, the ==highlighted item== is the current row. The **bolded
items** are the rows that are included in the analysis.

#### Common tables used in examples

The following tables are used in the subsequent aggregate analytic
query examples: [`Produce`][produce-table], [`Employees`][employees-table],
and [`Farm`][farm-table].

##### Produce Table

Some examples reference a table called `Produce`:

```zetasql
WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'orange', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT * FROM Produce

+-------------------------------------+
| item      | category   | purchases  |
+-------------------------------------+
| kale      | vegetable  | 23         |
| orange    | fruit      | 2          |
| cabbage   | vegetable  | 9          |
| apple     | fruit      | 8          |
| leek      | vegetable  | 2          |
| lettuce   | vegetable  | 10         |
+-------------------------------------+
```

##### Employees Table

Some examples reference a table called `Employees`:

```zetasql
WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT * FROM Employees

+-------------------------------------+
| name      | department | start_date |
+-------------------------------------+
| Isabella  | 2          | 1997-09-28 |
| Anthony   | 1          | 1995-11-29 |
| Daniel    | 2          | 2004-06-24 |
| Andrew    | 1          | 1999-01-23 |
| Jacob     | 1          | 1990-07-11 |
| Jose      | 2          | 2013-03-17 |
+-------------------------------------+
```

##### Farm Table

Some examples reference a table called `Farm`:

```zetasql
WITH Farm AS
 (SELECT 'cat' as animal, 23 as population, 'mammal' as category
  UNION ALL SELECT 'duck', 3, 'bird'
  UNION ALL SELECT 'dog', 2, 'mammal'
  UNION ALL SELECT 'goose', 1, 'bird'
  UNION ALL SELECT 'ox', 2, 'mammal'
  UNION ALL SELECT 'goat', 2, 'mammal')
SELECT * FROM Farm

+-------------------------------------+
| animal    | category   | population |
+-------------------------------------+
| cat       | mammal     | 23         |
| duck      | bird       | 3          |
| dog       | mammal     | 2          |
| goose     | bird       | 1          |
| ox        | mammal     | 2          |
| goat      | mammal     | 2          |
+-------------------------------------+
```

#### Compute a grand total

This computes a grand total for all items in the
[`Produce`][produce-table] table.

+  (**==orange==**, **apple**, **leek**, **cabbage**, **lettuce**, **kale**) = 54 total purchases
+  (**orange**, **==apple==**, **leek**, **cabbage**, **lettuce**, **kale**) = 54 total purchases
+  (**orange**, **apple**, **==leek==**, **cabbage**, **lettuce**, **kale**) = 54 total purchases
+  (**orange**, **apple**, **leek**, **==cabbage==**, **lettuce**, **kale**) = 54 total purchases
+  (**orange**, **apple**, **leek**, **cabbage**, **==lettuce==**, **kale**) = 54 total purchases
+  (**orange**, **apple**, **leek**, **cabbage**, **lettuce**, **==kale==**) = 54 total purchases

```zetasql
SELECT item, purchases, category, SUM(purchases)
  OVER () AS total_purchases
FROM Produce

+-------------------------------------------------------+
| item      | purchases  | category   | total_purchases |
+-------------------------------------------------------+
| orange    | 2          | fruit      | 54              |
| leek      | 2          | vegetable  | 54              |
| apple     | 8          | fruit      | 54              |
| cabbage   | 9          | vegetable  | 54              |
| lettuce   | 10         | vegetable  | 54              |
| kale      | 23         | vegetable  | 54              |
+-------------------------------------------------------+
```

#### Compute a subtotal

This computes a subtotal for each category in the
[`Produce`][produce-table] table.

+  fruit
   +  (**==orange==**, **apple**) = 10 total purchases
   +  (**orange**, **==apple==**) = 10 total purchases
+  vegetable
   +  (**==leek==**, **cabbage**, **lettuce**, **kale**) = 44 total purchases
   +  (**leek**, **==cabbage==**, **lettuce**, **kale**) = 44 total purchases
   +  (**leek**, **cabbage**, **==lettuce==**, **kale**) = 44 total purchases
   +  (**leek**, **cabbage**, **lettuce**, **==kale==**) = 44 total purchases

```zetasql
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS total_purchases
FROM Produce

+-------------------------------------------------------+
| item      | purchases  | category   | total_purchases |
+-------------------------------------------------------+
| orange    | 2          | fruit      | 10              |
| apple     | 8          | fruit      | 10              |
| leek      | 2          | vegetable  | 44              |
| cabbage   | 9          | vegetable  | 44              |
| lettuce   | 10         | vegetable  | 44              |
| kale      | 23         | vegetable  | 44              |
+-------------------------------------------------------+
```

#### Compute a cumulative sum

This computes a cumulative sum for each category in the
[`Produce`][produce-table] table. The sum is computed with respect to the
order defined using the `ORDER BY` clause.

+  (**==orange==**, apple, leek, cabbage, lettuce, kale) = 2 total purchases
+  (**orange**, **==apple==**, leek, cabbage, lettuce, kale) = 10 total purchases
+  (**orange**, **apple**, **==leek==**, cabbage, lettuce, kale) = 2 total purchases
+  (**orange**, **apple**, **leek**, **==cabbage==**, lettuce, kale) = 11 total purchases
+  (**orange**, **apple**, **leek**, **cabbage**, **==lettuce==**, kale) = 21 total purchases
+  (**orange**, **apple**, **leek**, **cabbage**, **lettuce**, **==kale==**) = 44 total purchases

```zetasql
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS total_purchases
FROM Produce

+-------------------------------------------------------+
| item      | purchases  | category   | total_purchases |
+-------------------------------------------------------+
| orange    | 2          | fruit      | 2               |
| apple     | 8          | fruit      | 10              |
| leek      | 2          | vegetable  | 2               |
| cabbage   | 9          | vegetable  | 11              |
| lettuce   | 10         | vegetable  | 21              |
| kale      | 23         | vegetable  | 44              |
+-------------------------------------------------------+
```

This does the same thing as the example above. You don't have to add
`CURRENT ROW` as a boundary unless you would like to for readability.

```sql
SELECT item, purchases, category, SUM(purchases)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS UNBOUNDED PRECEDING
  ) AS total_purchases
FROM Produce
```

In this example, all items in the [`Produce`][produce-table] table are included
in the partition. Only preceding rows are analyzed. The analysis starts two
rows prior to the current row in the partition.

+  (==orange==, leek, apple, cabbage, lettuce, kale) = NULL
+  (orange, ==leek==, apple, cabbage, lettuce, kale) = NULL
+  (**orange**, leek, ==apple==, cabbage, lettuce, kale) = 2
+  (**orange**, **leek**, apple, ==cabbage==, lettuce, kale) = 4
+  (**orange**, **leek**, **apple**, cabbage, ==lettuce==, kale) = 12
+  (**orange**, **leek**, **apple**, **cabbage**, lettuce, ==kale==) = 21

```zetasql
SELECT item, purchases, category, SUM(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING
  ) AS total_purchases
FROM Produce;

+-------------------------------------------------------+
| item      | purchases  | category   | total_purchases |
+-------------------------------------------------------+
| orange    | 2          | fruit      | NULL            |
| leek      | 2          | vegetable  | NULL            |
| apple     | 8          | fruit      | 2               |
| cabbage   | 9          | vegetable  | 4               |
| lettuce   | 10         | vegetable  | 12              |
| kale      | 23         | vegetable  | 21              |
+-------------------------------------------------------+
```

#### Compute a moving average

This computes a moving average in the [`Produce`][produce-table] table.
The lower boundary is 1 row before the
current row. The upper boundary is 1 row after the current row.

+  (**==orange==**, **leek**, apple, cabbage, lettuce, kale) = 2 average purchases
+  (**orange**, **==leek==**, **apple**, cabbage, lettuce, kale) = 4 average purchases
+  (orange, **leek**, **==apple==**, **cabbage**, lettuce, kale) = 6.3333 average purchases
+  (orange, leek, **apple**, **==cabbage==**, **lettuce**, kale) = 9 average purchases
+  (orange, leek, apple, **cabbage**, **==lettuce==**, **kale**) = 14 average purchases
+  (orange, leek, apple, cabbage, **lettuce**, **==kale==**) = 16.5 average purchases

```zetasql
SELECT item, purchases, category, AVG(purchases)
  OVER (
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS avg_purchases
FROM Produce

+-------------------------------------------------------+
| item      | purchases  | category   | avg_purchases   |
+-------------------------------------------------------+
| orange    | 2          | fruit      | 2               |
| leek      | 2          | vegetable  | 4               |
| apple     | 8          | fruit      | 6.33333         |
| cabbage   | 9          | vegetable  | 9               |
| lettuce   | 10         | vegetable  | 14              |
| kale      | 23         | vegetable  | 16.5            |
+-------------------------------------------------------+
```

#### Compute the number of items within a range

In this example, we get the number of animals that have a similar population
count in the [`Farm`][farm-table] table.

+  (**==goose==**, **dog**, **ox**, **goat**, duck, cat) = 4 animals between population range 0-2.
+  (**goose**, **==dog==**, **ox**, **goat**, **duck**, cat) = 5 animals between population range 1-3.
+  (**goose**, **dog**, **==ox==**, **goat**, **duck**, cat) = 5 animals between population range 1-3.
+  (**goose**, **dog**, **ox**, **==goat==**, **duck**, cat) = 5 animals between population range 1-3.
+  (goose, **dog**, **ox**, **goat**, **==duck==**, cat) = 4 animals between population range 2-4.
+  (goose, dog, ox, goat, duck, **==cat==**) = 1 animal between population range 22-24.

```zetasql
SELECT animal, population, category, COUNT(*)
  OVER (
    ORDER BY population
    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS similar_population
FROM Farm;

+----------------------------------------------------------+
| animal    | population | category   | similar_population |
+----------------------------------------------------------+
| goose     | 1          | bird       | 4                  |
| dog       | 2          | mammal     | 5                  |
| ox        | 2          | mammal     | 5                  |
| goat      | 2          | mammal     | 5                  |
| duck      | 3          | bird       | 4                  |
| cat       | 23         | mammal     | 1                  |
+----------------------------------------------------------+
```

#### Get the most popular item in each category

This example gets the most popular item in each category. It defines how rows
in a window should be partitioned and ordered in each partition. The
[`Produce`][produce-table] table is referenced.

+  fruit
   +  (**==orange==**, **apple**) = apple is most popular
   +  (**orange**, **==apple==**) = apple is most popular
+  vegetable
   +  (**==leek==**, **cabbage**, **lettuce**, **kale**) = kale is most popular
   +  (**leek**, **==cabbage==**, **lettuce**, **kale**) = kale is most popular
   +  (**leek**, **cabbage**, **==lettuce==**, **kale**) = kale is most popular
   +  (**leek**, **cabbage**, **lettuce**, **==kale==**) = kale is most popular

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS most_popular
FROM Produce

+----------------------------------------------------+
| item      | purchases  | category   | most_popular |
+----------------------------------------------------+
| orange    | 2          | fruit      | apple        |
| apple     | 8          | fruit      | apple        |
| leek      | 2          | vegetable  | kale         |
| cabbage   | 9          | vegetable  | kale         |
| lettuce   | 10         | vegetable  | kale         |
| kale      | 23         | vegetable  | kale         |
+----------------------------------------------------+
```

#### Get the last value in a range

In this example, we get the most popular item in a specific window frame, using
the [`Produce`][produce-table] table. The window frame analyzes up to three
rows at a time. Take a close look at the `most_popular` column for vegetables.
Instead of getting the most popular item in a specific category, it gets the
most popular item in a specific range in that category.

+  fruit
   +  (**==orange==**, **apple**) = apple is most popular
   +  (**orange**, **==apple==**) = apple is most popular
+  vegetable
   +  (**==leek==**, **cabbage**, lettuce, kale) = leek is most popular
   +  (**leek**, **==cabbage==**, **lettuce**, kale) = lettuce is most popular
   +  (leek, **cabbage**, **==lettuce==**, **kale**) = kale is most popular
   +  (leek, cabbage, **lettuce**, **==kale==**) = kale is most popular

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    PARTITION BY category
    ORDER BY purchases
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce

+----------------------------------------------------+
| item      | purchases  | category   | most_popular |
+----------------------------------------------------+
| orange    | 2          | fruit      | apple        |
| apple     | 8          | fruit      | apple        |
| leek      | 2          | vegetable  | cabbage      |
| cabbage   | 9          | vegetable  | lettuce      |
| lettuce   | 10         | vegetable  | kale         |
| kale      | 23         | vegetable  | kale         |
+----------------------------------------------------+
```

This example returns the same results as the one above, but it includes
a named window called `item_window`. Some of the window specifications are
defined directly in the `OVER` clause and some are defined in the named window.

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    item_window
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases)
```

#### Compute rank

This example calculates the rank of each employee within their department,
based on their start date. The window specification is defined directly
in the `OVER` clause. The [`Employees`][employees-table] table is referenced.

+  department 1
   +  (**==Jacob==**, **Anthony**, **Andrew**) = Assign rank 1 to Jacob
   +  (**Jacob**, **==Anthony==**, **Andrew**) = Assign rank 2 to Anthony
   +  (**Jacob**, **Anthony**, **==Andrew==**) = Assign rank 3 to Andrew
+  department 2
   +  (**==Isabella==**, **Daniel**, **Jose**) = Assign rank 1 to Isabella
   +  (**Isabella**, **==Daniel==**, **Jose**) = Assign rank 2 to Daniel
   +  (**Isabella**, **Daniel**, **==Jose==**) = Assign rank 3 to Jose

```zetasql
SELECT name, department, start_date,
  RANK() OVER (PARTITION BY department ORDER BY start_date) AS rank
FROM Employees;

+--------------------------------------------+
| name      | department | start_date | rank |
+--------------------------------------------+
| Jacob     | 1          | 1990-07-11 | 1    |
| Anthony   | 1          | 1995-11-29 | 2    |
| Andrew    | 1          | 1999-01-23 | 3    |
| Isabella  | 2          | 1997-09-28 | 1    |
| Daniel    | 2          | 2004-06-24 | 2    |
| Jose      | 2          | 2013-03-17 | 3    |
+--------------------------------------------+
```

#### Use a named window in a window frame clause {: #def_use_named_window }

You can define some of your logic in a named window and some of it in a
window frame clause. This logic is combined. Here is an example, using the
[`Produce`][produce-table] table.

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (item_window) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases
  ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)

+-------------------------------------------------------+
| item      | purchases  | category   | most_popular    |
+-------------------------------------------------------+
| orange    | 2          | fruit      | apple           |
| apple     | 8          | fruit      | apple           |
| leek      | 2          | vegetable  | lettuce         |
| cabbage   | 9          | vegetable  | kale            |
| lettuce   | 10         | vegetable  | kale            |
| kale      | 23         | vegetable  | kale            |
+-------------------------------------------------------+
```

You can also get the previous results with these examples:

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (item_window) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  c AS (b ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),
  item_window AS (c)
```

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (item_window ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  item_window AS (b)
```

The following example produces an error because a window frame clause has been
defined twice:

```zetasql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (
    item_window
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS most_popular
FROM Produce
WINDOW item_window AS (
  ORDER BY purchases
  ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
```

[named-window-rules]: #named_window_rules
[over-clause-def]: #def_over_clause
[window-specs-def]: #def_window_spec
[window-frame-clause-def]: #def_window_frame
[named-windows]: #ref_named_window
[produce-table]: #produce_table
[farm-table]: #farm_table
[employees-table]: #employees_table
[analytic-functions-link-to-numbering-functions]: #numbering_function_concepts
[analytic-functions-link-to-navigation-functions]: #navigation_function_concepts
[analytic-functions-compute-grand-total]: #compute_a_grand_total
[analytic-functions-compute-subtotal]: #compute_a_subtotal
[analytic-functions-compute-cumulative-sum]: #compute_a_cumulative_sum
[analytic-functions-compute-moving-avg]: #compute_a_moving_average
[analytic-functions-compute-item-range]: #compute_the_number_of_items_within_a_range
[analytic-functions-get-popular-item]: #get_the_most_popular_item_in_each_category
[analytic-functions-get-last-value-range]: #get_the_last_value_in_a_range
[analytic-functions-compute-rank]: #compute_rank
[analytic-functions-use-named-window]: #def_use_named_window
[analytic-functions-link-to-window]: https://github.com/google/zetasql/blob/master/docs/query-syntax#window_clause
[analytic-functions-link-to-hints]: https://github.com/google/zetasql/blob/master/docs/lexical#hints

[analytic-functions-link-to-window]: #window-clause
[analytic-functions-link-to-hints]: #hints
[navigation-functions-reference]: #navigation-functions
[numbering-functions-reference]: #numbering-functions
[aggregate-analytic-functions-reference]: #aggregate-analytic-functions

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Protocol Buffers

<!-- BEGIN CONTENT -->

Protocol buffers are a flexible, efficient mechanism for serializing structured
data. They are easy to create, small in size, and efficient to send over RPCs.

As efficient and as popular as protocol buffers are, however, when it comes to
data storage they do have one drawback: they do not map well to SQL syntax. For
example, SQL syntax expects that a given field can support a `NULL` or default
value. Protocol buffers, on the other hand, do not support `NULL`s very well,
and there isn't a standard way to determine whether a missing field should get a
`NULL` or a default value.

If you're going to query protocol buffers, you need to understand how they are
represented, what features they support, and what data they can contain. If
you're unfamiliar with protocol buffers in general, or would like a refresher on
how they work in languages other than SQL, see the
[Protocol Buffers Developer Guide][protocol-buffers-dev-guide].

### Constructing protocol buffers

This section covers how to construct protocol buffers using ZetaSQL.

<a id=using_new></a>
#### Using NEW

You can create a protocol buffer using the keyword `NEW`:

```
NEW TypeName(field_1 [AS alias], ...field_n [AS alias])
```

When using the `NEW` keyword to create a new protocol buffer:

+ All field expressions must have an [explicit alias][explicit-alias] or end with an identifier.
  For example, the expression `a.b.c` has the [implicit alias][implicit-alias] `c`.
+ `NEW` matches fields by alias to the field names of the protocol buffer.
  Aliases must be unique.
+ The expressions must be implicitly coercible or literal-coercible to the type
  of the corresponding protocol buffer field.

Example:

```
SELECT
  key,
  name,
  NEW zetasql.examples.music.Chart(key AS rank, name AS chart_name)
FROM
  (SELECT 1 as key, "2" as name);
```

To create a protocol buffer with an extension, use this syntax:

```
NEW TypeName(expr1 AS (path.to.extension), ...)
```

+   For `path.to.extension`, provide the path to the extension. Place the
    extension path inside parentheses.
+   `expr1` provides the value to set for the extension. `expr1` must be of the
    same type as the extension or
    [coercible to that type][conversion-rules].

    Example:

    ```
    SELECT
     NEW zetasql.examples.music.Album (
       album AS album_name,
       count AS (zetasql.examples.music.downloads)
     )
     FROM (SELECT 'Bach: The Goldberg Variations' AS album, 30 AS count);

    +----------------------------------------------------------------------------------------+
    | $col1                                                                                  |
    +----------------------------------------------------------------------------------------+
    | {album_name: 'Bach: The Goldberg Variations' [zetasql.examples.music.downloads]: 30} |
    +----------------------------------------------------------------------------------------+
    ```
+   If `path.to.extension` points to a nested protocol buffer extension, `expr1`
    provides an instance or a text format string of that protocol buffer.

    Example:

    ```
    SELECT
     NEW zetasql.examples.music.Album(
       'Bach: The Goldberg Variations' AS album_name,
       NEW zetasql.examples.music.AlbumExtension(
        DATE(1956,1,1) AS release_date
       )
     AS (zetasql.examples.music.AlbumExtension.album_extension));
    +-------------------------------------------------------------+
    | $col1                                                       |
    +-------------------------------------------------------------+
    | album_name: "Bach: The Goldberg Variations"                 |
    | [zetasql.examples.music.AlbumExtension.album_extension] { |
    |   release_date: -5114                                       |
    | }                                                           |
    +-------------------------------------------------------------+
    ```

#### SELECT AS typename

```
SELECT AS catalog.ProtocolBufferName
  expr1 [[AS] protocol_buffer_field1]
  [, ...]
FROM ...
```

A `SELECT AS typename` statement produces a value table where the row type is a
specific named type. Currently, protocol buffers are the only supported type
that can be used with this syntax.

The `SELECT` list may produce multiple columns.  Each produced column must have
an alias (explicitly or implicitly) that matches a unique protocol buffer field
name; to construct the protocol buffer, the query matches each expression with a
protocol buffer field by name. If no explicit alias is given, the expression
must have an implicit alias according to the rules in
[Implicit Aliases][implicit-alias].

When used with `SELECT DISTINCT`, or `GROUP BY` or `ORDER BY` using column
ordinals, these operators are applied first, on the columns in the `SELECT`
list, and then the value construction happens last.  This means that `DISTINCT`
can be applied on the input columns to the value construction, including in
cases where `DISTINCT` would not be allowed after value construction because
equality is not supported on protocol buffer types.

The following is an example of a `SELECT AS typename` query.

```
SELECT AS tests.TestProtocolBuffer mytable.key int64_val, mytable.name string_val
FROM mytable;

```

The query returns the output as a `tests.TestProtocolBuffer` protocol
buffer. `mytable.key int64_val` means that values from the `key` column are
stored in the `int64_val` field in the protocol buffer. Similarly, values from
the `mytable.name` column are stored in the `string_val` protocol buffer field.

 `SELECT AS` does not support setting protocol
buffer extensions. To do so, use the [NEW][new-keyword] keyword instead. For
example,  to create a protocol buffer with an extension, change a query like
this:

```
SELECT AS ProtoType field1, field2, ...
```

to a query like this:

```
SELECT AS VALUE NEW ProtoType(field1, field2, field3 AS (path.to.extension), ...)
```

### Casting Protocol Buffers

You can cast `PROTO` to or from `BYTES` or `STRING`.

```
SELECT CAST('first_name: "Jane", last_name: "Doe", customer_no: 1234'
  as example.CustomerInfo);
```

Casting to or from `BYTES` produces or parses proto2 wire format bytes. If
there is a failure during the serialization or deserialization process, an error
is raised. This can happen, for example, if no value is specified for a
required field.

Casting to or from `STRING` produces or parses the proto2 text format. When
casting from `STRING`, unknown field names aren't parseable. This means you need
to be cautious, because round-tripping from `PROTO` to `STRING` back to `PROTO`
could result in loss of data.

`STRING` literals used where a `PROTO` value is expected will be implicitly cast
to `PROTO`. If the literal value cannot be parsed using the expected `PROTO`
type, an error will be raised. To return `NULL`
instead, use [`SAFE_CAST`][link_to_safe_cast].

<a id=type_mapping></a>
### Type mapping

Protocol buffers are represented using the `PROTO` data type.  A column can
contain `PROTO` values the same way it can contain `INT32` or `STRING` values.

A protocol buffer contains zero or more fields inside it. Each field inside a
protocol buffer has its own type. All data types except `STRUCT` can be
contained inside a `PROTO`. Repeated fields in a protocol buffer are represented
as `ARRAY`s. The following table gives examples of the mapping between various
protocol buffer field types and the resulting ZetaSQL types.

<table>
<thead>
<tr>
<th>Protocol Buffer Field Type</th>
<th style="white-space:nowrap">ZetaSQL Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>optional MessageType msg = 1;</code></td>
<td><code>PROTO&lt;MessageType&gt;</code></td>
</tr>
<tr>
<td><code>required int64 int = 1;</code></td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>
<code>optional int64 int = 1;</code><br>
When reading, if this field isn't set, the default value (0) is returned. By
default, protocol buffer fields do not return <code>NULL</code>.
</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>
<code>optional int64 int = 1 [( zetasql.use_defaults ) = false];</code><br>
When reading, if this field isn't set, a <code>NULL</code> value is returned.
This example uses an annotation, which is described in
<a href="#defaults_and_nulls">Defaults and <code>NULL</code>s</a>.
</td>
<td><code>INT64</code></td>
</tr>

<tr>
<td>
<code>optional int32 date = 1 [( zetasql.format ) = DATE];</code>

</td>
<td><code>DATE</code></td>
</tr>
<tr>
<td>
<code>optional int64 time = 1 [( zetasql.format ) = TIMESTAMP_MICROS];</code>

</td>
<td><code>TIMESTAMP</code></td>
</tr>

<tr>
<td><code>repeated int64 repeat = 1;</code></td>
<td><code>ARRAY&lt;INT64&gt;</code></td>
</tr>
</tbody>
</table>

<a name="defaults_and_nulls"></a>
#### Default values and `NULL`s

Protocol buffer messages themselves do not have a default value &mdash; only the
fields contained inside a protocol buffer have defaults. When a full protocol
buffer value is returned in the result of a query, it is returned as a blob and
all fields are preserved as they were stored, including unset fields. This means
that you can run a query that returns a protocol buffer, and then extract fields
or check field presence in your client code with normal protocol buffer default
behavior.

By default, `NULL` values are never returned when accessing non-repeated leaf
fields contained in a `PROTO` from within a SQL statement, unless a containing
value is also `NULL`.  If the field value is not explicitly set, the default
value for the field is returned.  A change to the default value for a protocol
buffer field affects all future reads of that field for records where the value
is unset.

For example, suppose that `proto_msg` of type `PROTO` has a field named
`leaf_field`. A reference to `proto_msg.leaf_field` returns:

* `NULL` if `proto_msg` is `NULL`.
* A default value if `proto_msg` is not `NULL` but `leaf_field` is not set.
* The value of `leaf_field` if `proto_msg` is not `NULL` and `leaf_field`
  is set.

#### zetasql.use_defaults

You can change this default behavior using a special annotation on your protocol
message definition, `zetasql.use_defaults`, which you set on an individual
field to cause `NULL` values to be returned whenever a field value is not
explicitly set.

This annotation takes a boolean value.  The default is `true`, which means to
use the protocol buffer field defaults.  The annotation normally is written with
the value `false`, meaning that defaults should be ignored and `NULL`s should be
returned.

The following example shows how you can use the `use_defaults` annotation for an
optional protocol buffer field.

```
import "zetasql/public/proto/type_annotation.proto";

message SimpleMessage {
  // String field, where ZetaSQL interprets missing values as NULLs.
  optional string str = 2 [( zetasql.use_defaults ) = false];
}
```

In the case where protocol buffers have empty repeated fields, an empty `ARRAY`
is returned rather than a `NULL`-valued `ARRAY`. This behavior cannot be
changed.

After a value has been read out of a protocol buffer field, that value is
treated like any other value of that type. For non-`PROTO` values, such as
`INT64`, this means that after you get the value, you will not be able to tell
if the value for that field was set explicitly, or if it was read as a default
value.

#### zetasql.use_field_defaults

The `zetasql.use_field_defaults` annotation is just like
`zetasql.use_defaults`, but you set it on a message and it applies to all
unset fields within a given protocol buffer message. If both are present, the
field-level annotation takes precedence.

```
import "zetasql/public/proto/type_annotation.proto";

message AnotherSimpleMessage {
  // Interpret missing value as NULLs for all fields in this message.
  option ( zetasql.use_field_defaults ) = false;

  optional int64 nullable_int = 1;
  optional string nullable_string = 2;
}
```

<a id=checking_if_a_field_has_a_value></a>
#### Checking if a non-repeated field has a value

You can detect whether `optional` fields are set using a virtual field, `has_X`,
where `X` is the name of the field being checked. The type of the `has_X` field
is `BOOL`. The `has_` field is available for any non-repeated field of a `PROTO`
value. This field equals true if the value `X` is explicitly set in the message.

This field is useful for determining if a protocol buffer field has an explicit
value, or if reads will return a default value. Consider the protocol buffer
example, which has a field `country`. You can construct a query to determine if
a Customer protocol buffer message has a value for the country field by using
the virtual field `has_country`:

```
message ShippingAddress {
  optional string name = 1;
  optional string address = 2;
  optional string country = 3;
}
```

```
SELECT
  c.Orders.shipping_address.has_country
FROM
  Customer c;
```

If `has_country` returns `TRUE`, it indicates that the value for the `country`
field has been explicitly set. If it returns `FALSE` or `NULL`, it means the
value is not explicitly set.

<a id=checking_for_a_repeated_value></a>
#### Checking for a repeated value

You can use an `EXISTS` subquery to scan inside a repeated field and check if
any value exists with some desired property. For example, the following query
returns the name of every customer who has placed an order for the product
"Foo".

```
SELECT
  c.name
FROM
  Customers AS c
WHERE
  EXISTS(SELECT
           *
         FROM
           c.Orders.line_item AS item
         WHERE
           item.product_name = "Foo");
```

#### Nullness and nested fields

A `PROTO` value may contain fields which are themselves `PROTO`s. When this
happens it is possible for the nested `PROTO` itself to be `NULL`. In such a
case, the fields contained within that nested field are also `NULL`
regardless of their `use_default_value` settings.

Consider this example proto:

```
syntax = "proto2";

import "zetasql/public/proto/type_annotation.proto";

package some.package;

message NestedMessage {
  optional int64 value = 1 [( zetasql.use_defaults ) = true];
}

message OuterMessage {
  optional NestedMessage nested = 1;
}
```

Running the following query returns a `5` for `value` because it is
explicitly defined.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("nested { value: 5 }" as some.package.OuterMessage) as proto_field);
```

If `value` is not explicitly defined but `nested` is, you get a `0` because
the annotation on the protocol buffer definition says to use default values.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("nested { }" as some.package.OuterMessage) as proto_field);
```

However, if `nested` is not explicitly defined, you get a `NULL` even
though the annotation says to use default values for the `value` field. This is
because the containing message is `NULL`. This behavior applies to both
repeated and non-repeated fields within a nested message.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("" as some.package.OuterMessage) as proto_field);
```

<a id="proto_annotations"></a>
#### Annotations to extend the type system

The ZetaSQL type system contains more types than the protocol buffer
type system.
<a href="https://developers.google.com/protocol-buffers/docs/proto?csw=1#options">
Proto annotations</a> are used to store non-protocol-buffer types inside
serialized protos and read them back as the correct type.

While protocol buffers themselves do not support `DATE` or `TIMESTAMP` types,
you can use annotations on your protocol message definition to indicate that
certain fields should be interpreted as `DATE` or `TIMESTAMP` values when read
using SQL. For instance, a protocol message definition could contain the
following line:

```
optional int32 date = 2 [( zetasql.format ) = DATE];
```

The `zetasql.format` annotation indicates that this field, which stores an
`int32` in the protocol buffer, should be interpreted as a `DATE`. Queries over
the `date` field return a `DATE` type instead of an `INT32` because of the
annotation.

This result is the equivalent of having an `INT32` column and querying it as
follows:

```
SELECT
  DATE_FROM_UNIX_DATE(date)...
```

<a id=querying_protocol_buffers></a>
### Querying protocol buffers

You use the dot operator to access the fields contained within a protocol
buffer. This can not be used to get values of ambiguous fields.
If you need to reference an ambiguous field,
see [`EXTRACT`][proto-extract].

<a id=example_protocol_buffer_message></a>
#### Example protocol buffer message

To illustrate how to query protocol buffers, consider a table, `Customers`, that
contains a column `Orders` of type `PROTO`. The proto stored in `Orders`
contains fields such as the items ordered and the shipping address. The `.proto`
file that defines this protocol buffer might look like this:

```
import "zetasql/public/proto/type_annotation.proto";

message Orders {
  optional string order_number = 1;
  optional int64 date = 2 [( zetasql.format ) = DATE];

  message Address {
    optional string street = 1;
    optional string city = 2;
    optional string state = 3;
    optional string country = 4 [( zetasql.use_defaults ) = true,
                                  default = "United States"];
  }

  optional Address shipping_address = 3;

  message Item {
    optional string product_name = 1;
    optional int32 quantity = 2;
  }

  repeated Item line_item = 4;

  map<string, string> labels = 5;
}
```

An instance of this message might be:

```
{
  order_number: 1234567
  date: 16242
  shipping_address: {
      street: "1234 Main St"
      city: "AnyCity"
      state: "AnyState"
      country: "United States"
  }
  line_item: {
      product_name: "Foo"
      quantity: 10
  }
  line_item: {
      product_name: "Bar"
      quantity: 5
  }
}
```

<a id=querying_top-level_fields></a>
#### Querying top-level fields

You can write a query to return an entire protocol buffer message, or to return
a top-level or nested field of the message.

Using our example protocol buffer message, the following query returns all
protocol buffer values from the `Orders` column:

```
SELECT
  c.Orders
FROM
  Customers c;
```

This query returns the top-level field `order_number` from all protocol buffer
messages in the `Orders` column:

```
SELECT
  c.Orders.order_number
FROM
  Customers c;
```

<a id=querying_nested_paths></a>
#### Querying nested paths

Notice that the `Order` protocol buffer contains another protocol buffer
message, `Address`, in the `shipping_address` field. You can create a query that
returns all orders that have a shipping address in the United States:

```
SELECT
  c.Orders.order_number,
  c.Orders.shipping_address
FROM
  Customers c
WHERE
  c.Orders.shipping_address.country = "United States";
```

#### Returning repeated fields

Often, a protocol buffer message contains one or more repeated fields which are
returned as `ARRAY` values when referenced in SQL statements. For example, our
protocol buffer message contains a repeated field, `line_item`.

The following query returns a collection of `ARRAY`s containing the line items,
each holding all the line items for one order:

```
SELECT
  c.Orders.line_item
FROM
  Customers c;
```

For more information, see
[Working with Arrays][working-with-arrays].

#### Returning the number of elements in an array

As with any other `ARRAY` value, you can return the number of repeated fields in
a protocol buffer using the `ARRAY_LENGTH` function.

```
SELECT
  c.Orders.order_number,
  ARRAY_LENGTH(c.Orders.line_item)
FROM
  Customers c;
```

#### Querying map fields

Maps are not a supported type in ZetaSQL. However, maps are
[implemented in proto3 as repeated fields][protocol-buffer-compatibility],
so you can query maps by querying
the underlying repeated field. The underlying repeated field has `key` and
`value` fields that can be queried.

```
SELECT
  c.Orders.order_number
FROM
  Customers c
WHERE
  EXISTS(SELECT
           *
         FROM
           c.Orders.Labels label
         WHERE
           label.key = "color" AND label.value = "red");
```

<a id="extensions_and_weak_field"></a>

### Extensions

[extensions][protocol-extensions]
can be queried from `PROTO` values.

<a id="extensions"></a>
#### Top-level extensions

If your `PROTO` value contains extensions, you can query those fields using the
following syntax:

```
<identifier_of_proto_value>.(<package_of_extension>.<path_expression_to_extension_field>)
```

For example, consider this proto definition:

```
package some.package;

message Foo {
  optional int32 x = 1;
  extensions 100 to 130;
}

message Point {
  optional int32 x = 1;
  optional int32 y = 2;
}

extend Foo {
  optional int32 bar = 126;
  optional Point point = 127;
}
```

The following sections use this proto definition in a Table, `Test`, which
contains a field, `foo_field` of type `Foo`.

A query that returns the value of the `bar` extension field would resemble the
following:

```sql
SELECT
  foo_field.(some.package.bar)
FROM
  Test;
```

These types of extensions are often referred to as *top-level extensions*.

If you want your statement to return a specific value from a top-level
extension, you would modify it as follows:

```sql
SELECT
  foo_field.(some.package.point).y
FROM
  Test;
```

You can refine your statement to look for a specific value of a top-level
extension as well.

```sql
SELECT
  foo_field.(some.package.bar)
FROM
  Test
WHERE
  foo_field.(some.package.bar) = 5;
```

Note that you can also put back quotes around the components in the extension
path name in case they need to be escaped to avoid collisions with reserved
keywords. For example:

```sql
SELECT
  foo_field.(`some.package`.`bar`).value = 5
FROM
  Test;
```

#### Nested extensions

[Nested extensions][nested-extensions]
are also supported. These are protocol buffer extensions that are declared
within the scope of some other protocol message. For example:

```
package some.package;

message Baz {
  extend Foo {
    optional Baz foo_ext = 127;
  }
  optional int32 a = 1;
  optional int32 b = 2;
  ...
}
```

To construct queries for nested extensions, you use the same parenthetical
syntax as described in the previous section. To reference a nested extension,
in addition to specifying the package name, you must also specify the name of
the message where the extension is declared. For example:

```sql
SELECT
  foo_field.(some.package.Baz.foo_ext)
FROM
  Test;
```

You can reference a specific field in a nested extension using the same syntax
described in the previous section. For example:

```sql
SELECT
  foo_field.(some.package.Baz.foo_ext).a
FROM
  Test;
```

#### Correlated `CROSS JOIN` and repeated extensions

Correlated `CROSS JOIN` is used to "flatten" repeated fields. That is, the
values of the protocol buffer are duplicated once per entry in the repeated
field. In practice, this means that the repeated field is `UNNEST`ed. For
standard repeated fields, this unnesting happens implicitly. For repeated
extensions the `UNNEST` must be specified explicitly.

Consider the following protocol buffer:

```
syntax = "proto2";

package some.package;

message Example {
  optional int64 record_key = 1;
  repeated int64 repeated_value = 2;
  extensions 3 to 3;
}

message Extension {
  extend Example {
    repeated int64 repeated_extension_value = 3;
  }
}
```

The following query which uses the standard repeated field, `repeated_value` in
a correlated `CROSS JOIN` runs without an explicit `UNNEST`.

```
WITH t AS
  (SELECT
     CAST("""
       record_key: 1
       repeated_value: 1
       repeated_value: 2
       repeated_value: 3
       [some.package.Extension.repeated_extension_value]: 4
       [some.package.Extension.repeated_extension_value]: 5
       [some.package.Extension.repeated_extension_value]: 6"""
     as some.package.Example) as proto_field)
SELECT
  t.proto_field.record_key,
  value
FROM
  t,
  t.proto_field.repeated_value value;
```

This query, which uses the repeated extension field, `repeated_extension_value`
in the correlated `CROSS JOIN` requires an explicit `UNNEST`.

```
WITH t AS
  (SELECT
     CAST("""
       record_key: 1
       repeated_value: 1
       repeated_value: 2
       repeated_value: 3
       [some.package.Extension.repeated_extension_value]: 4
       [some.package.Extension.repeated_extension_value]: 5
       [some.package.Extension.repeated_extension_value]: 6"""
     as some.package.Example) as proto_field)
SELECT
  t.proto_field.record_key,
  value
FROM
  t,
  UNNEST(t.proto_field.(some.package.Extension.repeated_extension_value)) value;
```

[protocol-buffer-compatibility]: https://developers.google.com/protocol-buffers/docs/proto3#backwards-compatibility
[protocol-buffers-dev-guide]: https://developers.google.com/protocol-buffers
[nested-extensions]: https://developers.google.com/protocol-buffers/docs/proto#nested-extensions

[new-keyword]: #using_new
[explicit-alias]: https://github.com/google/zetasql/blob/master/docs/query-syntax#explicit_alias_syntax
[implicit-alias]: https://github.com/google/zetasql/blob/master/docs/query-syntax#implicit_aliases
[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules
[working-with-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md

[explicit-alias]: #explicit-alias-syntax
[implicit-alias]: #implicit_aliases
[conversion-rules]: #conversion_rules
[working-with-arrays]: #working-with-arrays
[link_to_safe_cast]: #safe_casting
[proto-extract]: #proto_extract

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Working with Arrays

<!-- BEGIN CONTENT -->

In ZetaSQL, an array is an ordered list consisting of zero or more
values of the same data type. You can construct arrays of simple data types,
such as `INT64`, and complex data types, such as `STRUCT`s. The current
exception to this is the
[`ARRAY`][array-data-type] data
type: arrays of arrays are not supported.

With ZetaSQL, you can construct array literals,
 build arrays from subqueries using the
[`ARRAY`][array-function] function,
 and aggregate values into an array using the
[`ARRAY_AGG`][array-agg-function]
function.

You can combine arrays using functions like
`ARRAY_CONCAT()`, and convert arrays to strings using `ARRAY_TO_STRING()`.

### Constructing arrays

#### Using array literals

You can build an array literal in ZetaSQL using brackets (`[` and
`]`). Each element in an array is separated by a comma.

```sql
SELECT [1, 2, 3] as numbers;

SELECT ["apple", "pear", "orange"] as fruit;

SELECT [true, false, true] as booleans;
```

You can also create arrays from any expressions that have compatible types. For
example:

```sql
SELECT [a, b, c]
FROM
  (SELECT 5 AS a,
          37 AS b,
          406 AS c);

SELECT [a, b, c]
FROM
  (SELECT CAST(5 AS INT64) AS a,
          CAST(37 AS DOUBLE) AS b,
          406 AS c);
```

Notice that the second example contains three expressions: one that returns an
`INT64`, one that returns a `DOUBLE`, and one that
declares a literal. This expression works because all three expressions share
`DOUBLE` as a supertype.

To declare a specific data type for an array, use angle
brackets (`<` and `>`). For example:

```sql
SELECT ARRAY<DOUBLE>[1, 2, 3] as floats;
```

Arrays of most data types, such as `INT64` or `STRING`, don't require
that you declare them first.

```sql
SELECT [1, 2, 3] as numbers;
```

You can write an empty array of a specific type using `ARRAY<type>[]`. You can
also write an untyped empty array using `[]`, in which case ZetaSQL
attempts to infer the array type from the surrounding context. If
ZetaSQL cannot infer a type, the default type `ARRAY<INT64>` is used.

#### Using generated values

You can also construct an `ARRAY` with generated values.

##### Generating arrays of integers

[`GENERATE_ARRAY`][generate-array-function]
generates an array of values from a starting and ending value and a step value.
For example, the following query generates an array that contains all of the odd
integers from 11 to 33, inclusive:

```sql
SELECT GENERATE_ARRAY(11, 33, 2) AS odds;

+--------------------------------------------------+
| odds                                             |
+--------------------------------------------------+
| [11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33] |
+--------------------------------------------------+
```

You can also generate an array of values in descending order by giving a
negative step value:

```sql
SELECT GENERATE_ARRAY(21, 14, -1) AS countdown;

+----------------------------------+
| countdown                        |
+----------------------------------+
| [21, 20, 19, 18, 17, 16, 15, 14] |
+----------------------------------+
```

##### Generating arrays of dates

[`GENERATE_DATE_ARRAY`][generate-date-array]
generates an array of `DATE`s from a starting and ending `DATE` and a step
`INTERVAL`.

You can generate a set of `DATE` values using `GENERATE_DATE_ARRAY`. For
example, this query returns the current `DATE` and the following
`DATE`s at 1 `WEEK` intervals up to and including a later `DATE`:

```sql
SELECT
  GENERATE_DATE_ARRAY('2017-11-21', '2017-12-31', INTERVAL 1 WEEK)
    AS date_array;

+--------------------------------------------------------------------------+
| date_array                                                               |
+--------------------------------------------------------------------------+
| [2017-11-21, 2017-11-28, 2017-12-05, 2017-12-12, 2017-12-19, 2017-12-26] |
+--------------------------------------------------------------------------+
```

### Casting Arrays

You can use [`CAST`][casting]
to cast arrays from one element type to another. The element types of the input
`ARRAY` must be castable to the element types of the target `ARRAY`. For
example, casting from type `ARRAY<INT32>` to `ARRAY<INT64>` or `ARRAY<STRING>`
is valid; casting from type `ARRAY<INT32>` to `ARRAY<BYTES>` is not valid.

**Example**

```sql
SELECT CAST(int_array AS ARRAY<DOUBLE>) AS double_array
FROM (SELECT ARRAY<INT32>[1, 2, 3] AS int_array);

+--------------+
| double_array |
+--------------+
| [1, 2, 3]    |
+--------------+
```

### Accessing Array Elements

Consider the following table, `sequences`:

```sql
+---------------------+
| some_numbers        |
+---------------------+
| [0, 1, 1, 2, 3, 5]  |
| [2, 4, 8, 16, 32]   |
| [5, 10]             |
+---------------------+
```

This table contains the column `some_numbers` of the `ARRAY` data type.
To access elements from the arrays in this column, you must specify which type
of indexing you want to use: either
[`OFFSET`][offset-and-ordinal],
for zero-based indexes, or
[`ORDINAL`][offset-and-ordinal],
for one-based indexes.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
       some_numbers[OFFSET(1)] AS offset_1,
       some_numbers[ORDINAL(1)] AS ordinal_1
FROM sequences;

+--------------------+----------+-----------+
| some_numbers       | offset_1 | ordinal_1 |
+--------------------+----------+-----------+
| [0, 1, 1, 2, 3, 5] | 1        | 0         |
| [2, 4, 8, 16, 32]  | 4        | 2         |
| [5, 10]            | 10       | 5         |
+--------------------+----------+-----------+
```

You can use this DML statement to insert the example data:

```sql
INSERT sequences
  (some_numbers, id)
VALUES
  ([0, 1, 1, 2, 3, 5], 1),
  ([2, 4, 8, 16, 32], 2),
  ([5, 10], 3);
```

This query shows how to use `OFFSET()` and `ORDINAL()`:

```sql
SELECT some_numbers,
       some_numbers[OFFSET(1)] AS offset_1,
       some_numbers[ORDINAL(1)] AS ordinal_1
FROM sequences;

+---------------+----------+-----------+
| some_numbers  | offset_1 | ordinal_1 |
+---------------+----------+-----------+
| [0,1,1,2,3,5] |        1 |         0 |
+---------------+----------+-----------+
| [2,4,8,16,32] |        4 |         2 |
+---------------+----------+-----------+
| [5,10]        |       10 |         5 |
+---------------+----------+-----------+
```

### Finding Lengths

The `ARRAY_LENGTH()` function returns the length of an array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
       ARRAY_LENGTH(some_numbers) AS len
FROM sequences;

+--------------------+--------+
| some_numbers       | len    |
+--------------------+--------+
| [0, 1, 1, 2, 3, 5] | 6      |
| [2, 4, 8, 16, 32]  | 5      |
| [5, 10]            | 2      |
+--------------------+--------+
```

Here's an example query, assuming the same definition of the `sequences` table
as above, with the same sample rows:

```sql
SELECT some_numbers,
       ARRAY_LENGTH(some_numbers) AS len
FROM sequences;

+---------------+------+
| some_numbers  | len  |
+---------------+------+
| [0,1,1,2,3,5] |    6 |
+---------------+------+
| [2,4,8,16,32] |    5 |
+---------------+------+
| [5,10]        |    2 |
+---------------+------+
```

### Flattening arrays

To convert an `ARRAY` into a set of rows, also known as "flattening," use the
[`UNNEST`][unnest-query]
operator. `UNNEST` takes an `ARRAY` and returns a table with a single row for
each element in the `ARRAY`.

Because `UNNEST` destroys the order of the `ARRAY` elements, you may
wish to restore order to the table. To do so, use the optional `WITH OFFSET`
clause to return an additional column with the offset for each array element,
then use the `ORDER BY` clause to order the rows by their offset.

**Example**

```sql
SELECT *
FROM UNNEST(['foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'])
  AS element
WITH OFFSET AS offset
ORDER BY offset;

+----------+--------+
| element  | offset |
+----------+--------+
| foo      | 0      |
| bar      | 1      |
| baz      | 2      |
| qux      | 3      |
| corge    | 4      |
| garply   | 5      |
| waldo    | 6      |
| fred     | 7      |
+----------+--------+
```

To flatten an entire column of `ARRAY`s while preserving the values
of the other columns in each row, use a
[`CROSS JOIN`][cross-join-query]
to join the table containing the `ARRAY` column to the `UNNEST` output of that
`ARRAY` column.

This is a correlated cross join: the `UNNEST` operator references the column of
`ARRAY`s from each row in the source table, which appears previously in the
`FROM` clause. For each row `N` in the source table, `UNNEST` flattens the
`ARRAY` from row `N` into a set of rows containing the `ARRAY` elements, and
then the `CROSS JOIN` joins this new set of rows with the single row `N` from
the source table.

**Example**

The following example uses [`UNNEST`][unnest-query]
to return a row for each element in the array column. Because of the
`CROSS JOIN`, the `id` column contains the `id` values for the row in
`sequences` that contains each number.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id, flattened_numbers
FROM sequences
CROSS JOIN UNNEST(sequences.some_numbers) AS flattened_numbers;

+------+-------------------+
| id   | flattened_numbers |
+------+-------------------+
|    1 |                 0 |
|    1 |                 1 |
|    1 |                 1 |
|    1 |                 2 |
|    1 |                 3 |
|    1 |                 5 |
|    2 |                 2 |
|    2 |                 4 |
|    2 |                 8 |
|    2 |                16 |
|    2 |                32 |
|    3 |                 5 |
|    3 |                10 |
+------+-------------------+
```

Note that for correlated cross joins the `UNNEST` operator is optional and the
`CROSS JOIN` can be expressed as a comma-join. Using this shorthand notation,
the above example becomes:

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id, flattened_numbers
FROM sequences, sequences.some_numbers AS flattened_numbers;

+------+-------------------+
| id   | flattened_numbers |
+------+-------------------+
|    1 |                 0 |
|    1 |                 1 |
|    1 |                 1 |
|    1 |                 2 |
|    1 |                 3 |
|    1 |                 5 |
|    2 |                 2 |
|    2 |                 4 |
|    2 |                 8 |
|    2 |                16 |
|    2 |                32 |
|    3 |                 5 |
|    3 |                10 |
+------+-------------------+
```

### Querying Nested and Repeated Fields

If a table contains an `ARRAY` of `STRUCT`s or `PROTO`s, you can
[flatten the `ARRAY`][flattening-arrays] to query the fields of the `STRUCT` or
`PROTO`.
You can also flatten `ARRAY` type fields of `STRUCT` values and repeated fields
of `PROTO` values. ZetaSQL treats repeated `PROTO` fields as
`ARRAY`s.

#### Querying STRUCT elements in an ARRAY

The following example uses `UNNEST` with `CROSS JOIN` to flatten an `ARRAY` of
`STRUCT`s.

```sql
WITH races AS (
  SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
     STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
     STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
     STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
     STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
     STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
     STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
     STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
       AS participants)
SELECT
  race,
  participant
FROM races r
CROSS JOIN UNNEST(r.participants) as participant;

+------+---------------------------------------+
| race | participant                           |
+------+---------------------------------------+
| 800M | {Rudisha, [23.4, 26.3, 26.4, 26.1]}   |
| 800M | {Makhloufi, [24.5, 25.4, 26.6, 26.1]} |
| 800M | {Murphy, [23.9, 26, 27, 26]}          |
| 800M | {Bosse, [23.6, 26.2, 26.5, 27.1]}     |
| 800M | {Rotich, [24.7, 25.6, 26.9, 26.4]}    |
| 800M | {Lewandowski, [25, 25.7, 26.3, 27.2]} |
| 800M | {Kipketer, [23.2, 26.1, 27.3, 29.4]}  |
| 800M | {Berian, [23.7, 26.1, 27, 29.3]}      |
+------+---------------------------------------+
```

```sql
SELECT race,
       participant.name,
       participant.splits
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r
CROSS JOIN UNNEST(r.participants) AS participant;

+------+-------------+-----------------------+
| race | name        | splits                |
+------+-------------+-----------------------+
| 800M | Rudisha     | [23.4,26.3,26.4,26.1] |
+------+-------------+-----------------------+
| 800M | Makhloufi   | [24.5,25.4,26.6,26.1] |
+------+-------------+-----------------------+
| 800M | Murphy      | [23.9,26,27,26]       |
+------+-------------+-----------------------+
| 800M | Bosse       | [23.6,26.2,26.5,27.1] |
+------+-------------+-----------------------+
| 800M | Rotich      | [24.7,25.6,26.9,26.4] |
+------+-------------+-----------------------+
| 800M | Lewandowski | [25,25.7,26.3,27.2]   |
+------+-------------+-----------------------+
| 800M | Kipketer    | [23.2,26.1,27.3,29.4] |
+------+-------------+-----------------------+
| 800M | Berian      | [23.7,26.1,27,29.3]   |
+------+-------------+-----------------------+
```

You can find specific information from repeated fields. For example, the
following query returns the fastest racer in an 800M race.

<p class="note">This example does not involve flattening an array, but does
represent a common way to get information from a repeated field.</p>

**Example**

```sql
WITH races AS (
  SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
     STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
     STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
     STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
     STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
     STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
     STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
     STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
       AS participants)
SELECT
  race,
  (SELECT name
   FROM UNNEST(participants)
   ORDER BY (
     SELECT SUM(duration)
     FROM UNNEST(splits) AS duration) ASC
   LIMIT 1) AS fastest_racer
FROM races;

+------+---------------+
| race | fastest_racer |
+------+---------------+
| 800M | Rudisha       |
+------+---------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants)
        ORDER BY (
          SELECT SUM(duration)
          FROM UNNEST(splits) AS duration) ASC
          LIMIT 1) AS fastest_racer
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+---------------+
| race | fastest_racer |
+------+---------------+
| 800M | Rudisha       |
+------+---------------+
```

#### Querying PROTO elements in an ARRAY

To query the fields of `PROTO` elements in an `ARRAY`, use `UNNEST` and
`CROSS JOIN`.

**Example**

The following query shows the contents of a table where one row contains an
`ARRAY` of `PROTO`s. All of the `PROTO` field values in the `ARRAY` appear in a
single row.

```sql
WITH table AS (
  SELECT
    'Let It Be' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(2 AS rank, 'Oricon' AS chart_name)]
     AS charts
  UNION ALL
  SELECT
    'Rubber Soul' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(24 AS rank, 'Oricon' AS chart_name)]
     AS charts
)
SELECT *
FROM table;

+-------------+---------------------------------+
| album_name  | charts                          |
+-------------+---------------------------------+
| Let It Be   | [chart_name: "US 100", rank: 1, |
|             | chart_name: "UK 40", rank: 1,   |
|             | chart_name: "Oricon" rank: 2]   |
+-------------+---------------------------------+
| Rubber Soul | [chart_name: "US 100", rank: 1, |
|             | chart_name: "UK 40", rank: 1,   |
|             | chart_name: "Oricon" rank: 24]  |
+-------------+---------------------------------+
```

To return the value of the individual fields of the `PROTO`s inside an `ARRAY`,
use `UNNEST` to flatten the `ARRAY`, then use a `CROSS JOIN` to apply the
`UNNEST` operator to each row of the `ARRAY` column. The `CROSS JOIN` also
joins the duplicated values of other columns to the result of `UNNEST`, so you
can query these columns together with the fields of the `PROTO`s in the `ARRAY`.

**Example**

The following example uses `UNNEST` to flatten the `ARRAY` `charts`. The `CROSS
JOIN` applies the `UNNEST` operator to every row in the `charts` column and
joins the duplicated value of `table.album_name` to the `chart` table. This
allows the query to include the `table.album_name` column in the `SELECT` list
together with the `PROTO` fields `chart.chart_name` and `chart.rank`.

```sql
WITH table AS (
  SELECT
    'Let It Be' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(2 AS rank, 'Oricon' AS chart_name)]
     AS charts
  UNION ALL
  SELECT
    'Rubber Soul' AS album_name,
    ARRAY[
         NEW zetasql.examples.music.Chart(1 AS rank, 'US 100' AS chart_name),
         NEW zetasql.examples.music.Chart(1 AS rank, 'UK 40' AS chart_name),
         NEW zetasql.examples.music.Chart(24 AS rank, 'Oricon' AS chart_name)]
     AS charts
)
SELECT table.album_name, chart.chart_name, chart.rank
FROM table
CROSS JOIN UNNEST(charts) AS chart;

+-------------+------------+------+
| album_name  | chart_name | rank |
+-------------+------------+------+
| Let It Be   | US 100     |    1 |
| Let It Be   | UK 40      |    1 |
| Let It Be   | Oricon     |    2 |
| Rubber Soul | US 100     |    1 |
| Rubber Soul | UK 40      |    1 |
| Rubber Soul | Oricon     |   24 |
+-------------+------------+------+
```

#### Querying ARRAY-type fields in a STRUCT

You can also get information from nested repeated fields. For example, the
following statement returns the runner who had the fastest lap in an 800M race.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
    AS participants)
SELECT
race,
(SELECT name
 FROM UNNEST(participants),
   UNNEST(splits) AS duration
 ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM races;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants),
          UNNEST(splits) AS duration
        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

Notice that the preceding query uses the comma operator (`,`) to perform an
implicit `CROSS JOIN`. It is equivalent to the following example, which uses
an explicit `CROSS JOIN`.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits)]
    AS participants)
SELECT
race,
(SELECT name
 FROM UNNEST(participants)
 CROSS JOIN UNNEST(splits) AS duration
 ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM races;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

```sql
SELECT race,
       (SELECT name
        FROM UNNEST(participants)
        CROSS JOIN UNNEST(splits) AS duration
        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Berian" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]
     AS participants
  ) AS r;

+------+-------------------------+
| race | runner_with_fastest_lap |
+------+-------------------------+
| 800M | Kipketer                |
+------+-------------------------+
```

Note that flattening arrays with a `CROSS JOIN` excludes rows that have empty
or NULL arrays. If you want to include these rows, use a `LEFT JOIN`.

```sql
WITH races AS (
 SELECT "800M" AS race,
   [STRUCT("Rudisha" as name, [23.4, 26.3, 26.4, 26.1] as splits),
    STRUCT("Makhloufi" as name, [24.5, 25.4, 26.6, 26.1] as splits),
    STRUCT("Murphy" as name, [23.9, 26.0, 27.0, 26.0] as splits),
    STRUCT("Bosse" as name, [23.6, 26.2, 26.5, 27.1] as splits),
    STRUCT("Rotich" as name, [24.7, 25.6, 26.9, 26.4] as splits),
    STRUCT("Lewandowski" as name, [25.0, 25.7, 26.3, 27.2] as splits),
    STRUCT("Kipketer" as name, [23.2, 26.1, 27.3, 29.4] as splits),
    STRUCT("Berian" as name, [23.7, 26.1, 27.0, 29.3] as splits),
    STRUCT("Nathan" as name, ARRAY<DOUBLE>[] as splits),
    STRUCT("David" as name, NULL as splits)]
    AS participants)
SELECT
  name, sum(duration) AS finish_time
FROM races, races.participants LEFT JOIN participants.splits duration
GROUP BY name;

+-------------+--------------------+
| name        | finish_time        |
+-------------+--------------------+
| Murphy      | 102.9              |
| Rudisha     | 102.19999999999999 |
| David       | NULL               |
| Rotich      | 103.6              |
| Makhloufi   | 102.6              |
| Berian      | 106.1              |
| Bosse       | 103.4              |
| Kipketer    | 106                |
| Nathan      | NULL               |
| Lewandowski | 104.2              |
+-------------+--------------------+
```

```sql
SELECT
  name, sum(duration) as duration
FROM
  (SELECT "800M" AS race,
    [STRUCT("Rudisha" AS name, [23.4, 26.3, 26.4, 26.1] AS splits),
     STRUCT("Makhloufi" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),
     STRUCT("Murphy" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),
     STRUCT("Bosse" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),
     STRUCT("Rotich" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),
     STRUCT("Lewandowski" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),
     STRUCT("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),
     STRUCT("Nathan" as name, ARRAY<DOUBLE>[] as splits),
     STRUCT("David" as name, NULL as splits)]
     AS participants) AS races,
  races.participants LEFT JOIN participants.splits duration
GROUP BY name;

+-------------+--------------------+
| name        | duration           |
+-------------+--------------------+
| Murphy      | 102.9              |
| Rudisha     | 102.19999999999999 |
| David       | NULL               |
| Rotich      | 103.6              |
| Makhloufi   | 102.6              |
| Bosse       | 103.4              |
| Kipketer    | 106                |
| Nathan      | NULL               |
| Lewandowski | 104.2              |
+-------------+--------------------+
```

#### Querying repeated fields

ZetaSQL represents repeated fields of `PROTO`s as `ARRAY`s. You
can query these `ARRAY`s using `UNNEST` and `CROSS JOIN`.

The following example queries a table containing a column of `PROTO`s with the
alias `album` and the repeated field `song`. All values of `song` for each
`album` appear on the same row.

**Example**

```sql
WITH table AS (
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Let It Be' AS album_name,
      ['Across the Universe', 'Get Back', 'Dig It'] AS song
    ) AS album
    UNION ALL
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Rubber Soul' AS album_name,
      ['Drive My Car', 'The Word', 'Michelle'] AS song
    ) AS album
)
SELECT *
FROM table;

+-------------+------------------+---------------------+
| band_name   | album.album_name | album.song          |
+-------------+------------------+---------------------+
| The Beatles | Let It Be        | Across the Universe |
|             |                  | Get Back            |
|             |                  | Dig It              |
| The Beatles | Rubber Soul      | Drive My Car        |
|             |                  | The Word            |
|             |                  | Michelle            |
+-------------+------------------+---------------------+
```

To query the individual values of a repeated field, reference the field name
using dot notation to return an `ARRAY`, and
[flatten the `ARRAY` using `UNNEST`][flattening-arrays]. Use `CROSS JOIN` to
apply the `UNNEST` operator to each row and join the flattened `ARRAY`
to the duplicated value of any non-repeated fields or columns.

**Example**

The following example queries the table from the previous example and returns
the values of the repeated field as an `ARRAY`. The `UNNEST` operator flattens
the `ARRAY` that represents the repeated field `song`. `CROSS JOIN` applies
the `UNNEST` operator to each row and joins the output of `UNNEST` to the
duplicated value of the column `band_name` and the non-repeated field
`album_name`.

```sql
WITH table AS (
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Let It Be' AS album_name,
      ['Across the Universe', 'Get Back', 'Dig It'] AS song
    ) AS album
    UNION ALL
  SELECT
    'The Beatles' AS band_name,
    NEW zetasql.examples.music.Album(
      'Rubber Soul' AS album_name,
      ['Drive My Car', 'The Word', 'Michelle'] AS song
    ) AS album
)
SELECT band_name, album.album_name, song_name
FROM table
CROSS JOIN UNNEST(album.song) AS song_name;

+-------------+-------------+---------------------+
| band_name   | album_name  | song_name           |
+-------------+-------------+---------------------+
| The Beatles | Let It Be   | Across the Universe |
| The Beatles | Let It Be   | Get Back            |
| The Beatles | Let It Be   | Dig It              |
| The Beatles | Rubber Soul | Drive My Car        |
| The Beatles | Rubber Soul | The Word            |
| The Beatles | Rubber Soul | Michelle            |
+-------------+-------------+---------------------+

```

### Creating Arrays From Subqueries

A common task when working with arrays is turning a subquery result into an
array. In ZetaSQL, you can accomplish this using the
[`ARRAY()`][array-function] function.

For example, consider the following operation on the `sequences` table:

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
  UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
  UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x) AS doubled
FROM sequences;

+--------------------+---------------------+
| some_numbers       | doubled             |
+--------------------+---------------------+
| [0, 1, 1, 2, 3, 5] | [0, 2, 2, 4, 6, 10] |
| [2, 4, 8, 16, 32]  | [4, 8, 16, 32, 64]  |
| [5, 10]            | [10, 20]            |
+--------------------+---------------------+
```

```sql
SELECT some_numbers,
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x) AS doubled
FROM sequences;

+---------------+----------------+
| some_numbers  | doubled        |
+---------------+----------------+
| [0,1,1,2,3,5] | [0,2,2,4,6,10] |
+---------------+----------------+
| [2,4,8,16,32] | [4,8,16,32,64] |
+---------------+----------------+
| [5,10]        | [10,20]        |
+---------------+----------------+
```

This example starts with a table named sequences. This table contains a column,
`some_numbers`, of type `ARRAY<INT64>`.

The query itself contains a subquery. This subquery selects each row in the
`some_numbers` column and uses
[`UNNEST`][unnest-query] to return the
array as a set of rows. Next, it multiplies each value by two, and then
recombines the rows back into an array using the `ARRAY()` operator.

### Filtering Arrays
The following example uses a `WHERE` clause in the `ARRAY()` operator's subquery
to filter the returned rows.

<p class='note'><b>Note:</b> In the following examples, the resulting rows are
not ordered.</p>

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x
        WHERE x < 5) AS doubled_less_than_five
FROM sequences;

+------------------------+
| doubled_less_than_five |
+------------------------+
| [0, 2, 2, 4, 6]        |
| [4, 8]                 |
| []                     |
+------------------------+
```

```sql
SELECT
  ARRAY(SELECT x * 2
        FROM UNNEST(some_numbers) AS x
        WHERE x < 5) AS doubled_less_than_five
FROM sequences;

+------------------------+
| doubled_less_than_five |
+------------------------+
| [0,2,2,4,6]            |
+------------------------+
| [4,8]                  |
+------------------------+
| []                     |
+------------------------+
```

Notice that the third row contains an empty array, because the elements in the
corresponding original row (`[5, 10]`) did not meet the filter requirement of
`x < 5`.

You can also filter arrays by using `SELECT DISTINCT` to return only
unique elements within an array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers)
SELECT ARRAY(SELECT DISTINCT x
             FROM UNNEST(some_numbers) AS x) AS unique_numbers
FROM sequences;

+-----------------+
| unique_numbers  |
+-----------------+
| [0, 1, 2, 3, 5] |
+-----------------+
```

```sql
SELECT ARRAY(SELECT DISTINCT x
             FROM UNNEST(some_numbers) AS x) AS unique_numbers
FROM sequences
WHERE id = 1;

+----------------+
| unique_numbers |
+----------------+
| [0,1,2,3,5]    |
+----------------+
```

You can also filter rows of arrays by using the
[`IN`][in-operators] keyword. This
keyword filters rows containing arrays by determining if a specific
value matches an element in the array.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT
   ARRAY(SELECT x
         FROM UNNEST(some_numbers) AS x
         WHERE 2 IN UNNEST(some_numbers)) AS contains_two
FROM sequences;

+--------------------+
| contains_two       |
+--------------------+
| [0, 1, 1, 2, 3, 5] |
| [2, 4, 8, 16, 32]  |
| []                 |
+--------------------+
```

```sql
SELECT
   ARRAY(SELECT x
         FROM UNNEST(some_numbers) AS x
         WHERE 2 IN UNNEST(some_numbers)) AS contains_two
FROM sequences;

+---------------+
| contains_two  |
+---------------+
| [0,1,1,2,3,5] |
+---------------+
| [2,4,8,16,32] |
+---------------+
| []            |
+---------------+
```

Notice again that the third row contains an empty array, because the array in
the corresponding original row (`[5, 10]`) did not contain `2`.

### Scanning Arrays

To check if an array contains a specific value, use the [`IN`][in-operators]
operator with [`UNNEST`][unnest-query]. To
check if an array contains a value matching a condition, use the [`EXISTS`][expression-subqueries]
function with `UNNEST`.

#### Scanning for specific values

To scan an array for a specific value, use the `IN` operator with `UNNEST`.

**Example**

The following example returns `true` if the array contains the number 2.

```sql
SELECT 2 IN UNNEST([0, 1, 1, 2, 3, 5]) AS contains_value;

+----------------+
| contains_value |
+----------------+
| true           |
+----------------+
```

To return the rows of a table where the array column contains a specific value,
filter the results of `IN UNNEST` using the `WHERE` clause.

**Example**

The following example returns the `id` value for the rows where the array
column contains the value 2.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id AS matching_rows
FROM sequences
WHERE 2 IN UNNEST(sequences.some_numbers)
ORDER BY matching_rows;

+---------------+
| matching_rows |
+---------------+
| 1             |
| 2             |
+---------------+
```

#### Scanning for values that satisfy a condition

To scan an array for values that match a condition, use `UNNEST` to return a
table of the elements in the array, use `WHERE` to filter the resulting table in
a subquery, and use `EXISTS` to check if the filtered table contains any rows.

**Example**

The following example returns the `id` value for the rows where the array
column contains values greater than 5.

```sql
WITH sequences AS
  (SELECT 1 AS id, [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT 2 AS id, [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT 3 AS id, [5, 10] AS some_numbers)
SELECT id AS matching_rows FROM sequences
WHERE EXISTS (SELECT *
              FROM UNNEST(some_numbers) AS x
              WHERE x > 5);

+---------------+
| matching_rows |
+---------------+
| 2             |
| 3             |
+---------------+
```

##### Scanning for STRUCT field values that satisfy a condition

To search an array of `STRUCT`s for a field whose value matches a condition, use
`UNNEST` to return a table with a column for each `STRUCT` field, then filter
non-matching rows from the table using `WHERE EXISTS`.

**Example**

The following example returns the rows where the array column contains a
`STRUCT` whose field `b` has a value greater than 3.

```sql
WITH sequences AS
  (SELECT 1 AS id, [STRUCT(0 AS a, 1 AS b)] AS some_numbers
   UNION ALL SELECT 2 AS id, [STRUCT(2 AS a, 4 AS b)] AS some_numbers
   UNION ALL SELECT 3 AS id, [STRUCT(5 AS a, 3 AS b), STRUCT (7 AS a, 4 AS b)]
     AS some_numbers)
SELECT id AS matching_rows
FROM sequences
WHERE EXISTS (SELECT 1
              FROM UNNEST(some_numbers)
              WHERE b > 3);

+---------------+
| matching_rows |
+---------------+
| 2             |
| 3             |
+---------------+
```

### Arrays and Aggregation

With ZetaSQL, you can aggregate values into an array using
`ARRAY_AGG()`.

```sql
WITH fruits AS
  (SELECT "apple" AS fruit
   UNION ALL SELECT "pear" AS fruit
   UNION ALL SELECT "banana" AS fruit)
SELECT ARRAY_AGG(fruit) AS fruit_basket
FROM fruits;

+-----------------------+
| fruit_basket          |
+-----------------------+
| [apple, pear, banana] |
+-----------------------+
```

Consider the following table, `fruits`:

```sql
CREATE TABLE fruits (
  fruit STRING(MAX),
  id INT64 NOT NULL
) PRIMARY KEY(id);

```
Assume the table is populated with the following data:

```sql
+----+--------------+
| id | fruit        |
+----+--------------+
| 1  | "apple"      |
| 2  | "pear"       |
| 3  | "banana"     |
+----+--------------+
```

You can use this DML statement to insert the example data:

```sql
INSERT fruits
  (fruit, id)
VALUES
  ("apple", 1),
  ("pear", 2),
  ("banana", 3);
```

This query shows how to use `ARRAY_AGG()`:

```sql
SELECT ARRAY_AGG(fruit) AS fruit_basket
FROM fruits;

+---------------------+
| fruit_basket        |
+---------------------+
| [apple,pear,banana] |
+---------------------+
```

The array returned by `ARRAY_AGG()` is in an arbitrary order, since the order in
which the function concatenates values is not guaranteed. To order the array
elements, use `ORDER BY`. For example:

```sql
WITH fruits AS
  (SELECT "apple" AS fruit
   UNION ALL SELECT "pear" AS fruit
   UNION ALL SELECT "banana" AS fruit)
SELECT ARRAY_AGG(fruit ORDER BY fruit) AS fruit_basket
FROM fruits;

+-----------------------+
| fruit_basket          |
+-----------------------+
| [apple, banana, pear] |
+-----------------------+
```

You can also apply aggregate functions such as `SUM()` to the elements in an
array. For example, the following query returns the sum of array elements for
each row of the `sequences` table.

```sql
WITH sequences AS
  (SELECT [0, 1, 1, 2, 3, 5] AS some_numbers
   UNION ALL SELECT [2, 4, 8, 16, 32] AS some_numbers
   UNION ALL SELECT [5, 10] AS some_numbers)
SELECT some_numbers,
  (SELECT SUM(x)
   FROM UNNEST(s.some_numbers) x) AS sums
FROM sequences s;

+--------------------+------+
| some_numbers       | sums |
+--------------------+------+
| [0, 1, 1, 2, 3, 5] | 12   |
| [2, 4, 8, 16, 32]  | 62   |
| [5, 10]            | 15   |
+--------------------+------+
```

```sql
SELECT some_numbers,
  (SELECT SUM(x)
   FROM UNNEST(s.some_numbers) x) AS sums
FROM sequences s;

+---------------+------+
| some_numbers  | sums |
+---------------+------+
| [0,1,1,2,3,5] |   12 |
+---------------+------+
| [2,4,8,16,32] |   62 |
+---------------+------+
| [5,10]        |   15 |
+---------------+------+
```

ZetaSQL also supports an aggregate function, `ARRAY_CONCAT_AGG()`,
which concatenates the elements of an array column across rows.

```sql
WITH aggregate_example AS
  (SELECT [1,2] AS numbers
   UNION ALL SELECT [3,4] AS numbers
   UNION ALL SELECT [5, 6] AS numbers)
SELECT ARRAY_CONCAT_AGG(numbers) AS count_to_six_agg
FROM aggregate_example;

+--------------------------------------------------+
| count_to_six_agg                                 |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

**Note:** The array returned by `ARRAY_CONCAT_AGG()` is
non-deterministic, since the order in which the function concatenates values is
not guaranteed.

### Converting Arrays to Strings

The `ARRAY_TO_STRING()` function allows you to convert an `ARRAY<STRING>` to a
single `STRING` value or an `ARRAY<BYTES>` to a single `BYTES` value where the
resulting value is the ordered concatenation of the array elements.

The second argument is the separator that the function will insert between
inputs to produce the output; this second argument must be of the same
type as the elements of the first argument.

Example:

```sql
WITH greetings AS
  (SELECT ["Hello", "World"] AS greeting)
SELECT ARRAY_TO_STRING(greeting, " ") AS greetings
FROM greetings;

+-------------+
| greetings   |
+-------------+
| Hello World |
+-------------+
```

The optional third argument takes the place of `NULL` values in the input
array.

+ If you omit this argument, then the function ignores `NULL` array
elements.

+ If you provide an empty string, the function inserts a
separator for `NULL` array elements.

Example:

```sql
SELECT
  ARRAY_TO_STRING(arr, ".", "N") AS non_empty_string,
  ARRAY_TO_STRING(arr, ".", "") AS empty_string,
  ARRAY_TO_STRING(arr, ".") AS omitted
FROM (SELECT ["a", NULL, "b", NULL, "c", NULL] AS arr);

+------------------+--------------+---------+
| non_empty_string | empty_string | omitted |
+------------------+--------------+---------+
| a.N.b.N.c.N      | a..b..c.     | a.b.c   |
+------------------+--------------+---------+
```

### Combining Arrays

In some cases, you might want to combine multiple arrays into a single array.
You can accomplish this using the `ARRAY_CONCAT()` function.

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

+--------------------------------------------------+
| count_to_six                                     |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

### Zipping Arrays

Given two arrays of equal size, you can merge them into a single array
consisting of pairs of elements from input arrays, taken from their
corresponding positions. This operation is sometimes called
[zipping][convolution].

You can zip arrays with `UNNEST` and `WITH OFFSET`. In this example, each
value pair is stored as a `STRUCT` in an array.

```sql
WITH combinations AS (
  SELECT
    ['a', 'b'] AS letters,
    [1, 2, 3] AS numbers
)
SELECT ARRAY_AGG(
  STRUCT(letter, numbers[OFFSET(letters_offset)] AS number)
) AS pairs
FROM combinations, UNNEST(letters) AS letter WITH OFFSET AS letters_offset;

+------------------------------+
| pairs                        |
+------------------------------+
| [{ letter: "a", number: 1 }, |
|  { letter: "b", number: 2 }] |
+------------------------------+
```

You can use input arrays of different lengths as long as the first array
is equal to or less than the length of the second array. The zipped array
will be the length of the shortest input array.

### Building Arrays of Arrays

ZetaSQL does not support building
[arrays of arrays][array-data-type]
directly. Instead, you must create an array of structs, with each struct
containing a field of type `ARRAY`. To illustrate this, consider the following
`points` table:

```sql
+----------+
| point    |
+----------+
| [1, 5]   |
| [2, 8]   |
| [3, 7]   |
| [4, 1]   |
| [5, 7]   |
+----------+
```

Now, let's say you wanted to create an array consisting of each `point` in the
`points` table. To accomplish this, wrap the array returned from each row in a
`STRUCT`, as shown below.

```sql
WITH points AS
  (SELECT [1, 5] as point
   UNION ALL SELECT [2, 8] as point
   UNION ALL SELECT [3, 7] as point
   UNION ALL SELECT [4, 1] as point
   UNION ALL SELECT [5, 7] as point)
SELECT ARRAY(
  SELECT STRUCT(point)
  FROM points)
  AS coordinates;

+-------------------+
| coordinates       |
+-------------------+
| [{point: [1,5]},  |
|  {point: [2,8]},  |
|  {point: [5,7]},  |
|  {point: [3,7]},  |
|  {point: [4,1]}]  |
+--------------------+
```

You can use this DML statement to insert the example data:

```sql
INSERT points
  (point, id)
VALUES
  ([1, 5], 1),
  ([2, 8], 2),
  ([3, 7], 3),
  ([4, 1], 4),
  ([5, 7], 5);
```

```sql
SELECT ARRAY(
  SELECT STRUCT(point)
  FROM points)
  AS coordinates;

+--------------+
| coordinates  |
+--------------+
| point: [1,5] |
| point: [2,8] |
| point: [3,7] |
| point: [4,1] |
| point: [5,7] |
+--------------+
```

[flattening-arrays]: #flattening_arrays
[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types#array_type
[unnest-query]: https://github.com/google/zetasql/blob/master/docs/query-syntax#unnest
[cross-join-query]: https://github.com/google/zetasql/blob/master/docs/query-syntax#cross_join
[convolution]: https://en.wikipedia.org/wiki/Convolution_(computer_science)

[array-data-type]: #array-type
[array-function]: #array
[array-agg-function]: #array_agg
[generate-array-function]: #generate_array
[generate-date-array]: #generate_date_array
[casting]: #casting
[offset-and-ordinal]: #offset_and_ordinal
[unnest-query]: #unnest
[cross-join-query]: #cross-join
[in-operators]: #in_operators
[expression-subqueries]: #expression_subqueries

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## ZetaSQL Data Model

<!-- BEGIN CONTENT -->

The following sections provide an overview of the ZetaSQL data
model.

### Standard SQL Tables

ZetaSQL data is stored in tables. Each table consists of an ordered
list of columns and a number of rows. Each column has a name used to identify it
through SQL statements, and is assigned a specific data type.

For example, the following table, **Singers**, is a standard SQL table.

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
<th>Default Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>SingerId</td>
<td><code>INT64</code></td>
<td><code>&lt;auto-increment&gt;</code></td>
</tr>
<tr>
<td>FirstName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>LastName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>BirthDate</td>
<td><code>DATE</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Status</td>
<td><code>STRING</code></td>
<td><code>"active"</code></td>
</tr>
<tr>
<td>SingerInfo</td>
<td><code>PROTO&lt;SingerMetadata&gt;</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Albums</td>
<td><code>PROTO&lt;Album&gt;</code></td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

The proto, `SingerMetadata`, has the following definition:

```
message SingerMetadata {
  optional string    nationality = 1;
  repeated Residence residence   = 2;

  message Residence {
    required int64  start_year   = 1;
    optional int64  end_year     = 2;
    optional string city         = 3;
    optional string country      = 4 [default = "USA"];
  }
}
```

A `SELECT *` statement on this table would return rows similar to the following:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>Status</th>
<th>SingerInfo</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>Marc</td>
<td>Richards</td>
<td>1970-09-03</td>
<td>active</td>
<td>{nationality: "England"}</td>
</tr>
<tr>
<td>2</td>
<td>Catalina</td>
<td>Smith</td>
<td>1990-08-17</td>
<td>inactive</td>
<td>{nationality: "U.S.A."}</td>
</tr>
<tr>
<td>3</td>
<td>Lea</td>
<td>Martin</td>
<td>1991-11-09</td>
<td>active</td>
<td>{nationality: "Australia"}</td>
</tr>
<tr>
<td>4</td>
<td>Xanathe</td>
<td>Riou</td>
<td>1995-05-23</td>
<td>inactive</td>
<td>{nationality: U.S.A."}</td>
</tr>
</tbody>
</table>

While tables do not have a type, some operations will construct an implicit
`STRUCT` type out of a SQL row, using the column names and types for field
definitions.

For more information on the data types ZetaSQL supports, see
[Data Types][data-types].

### Constraints

Constraints require that any writes to one or more columns, such as inserts or
updates, conform to certain rules.
[Data manipulation language (DML)][data-manipulation-language]
statements enforce constraints. ZetaSQL  supports the
following constraints:

* **Primary key constraint.** A primary key consists of one or more columns, and
  specifies that the value of each row of these combined columns must be unique
  within that table. A table can contain at most one primary key constraint.

  Some [data manipulation language (DML)][data-manipulation-language]
  keywords may require the existence of a primary key.
  ZetaSQL also implicitly builds an index on the primary key. The
  default order of this index is ascending. The primary key can contain `NULL`
  values.
* **Unique constraint.** Specifies that one or more columns must contain only
  unique values. Unlike a primary key, more than one unique constraint can exist
  on a table.

### Indexes

An index allows the database engine to query a column or set of columns more
quickly. You can specify that sort order is ascending or descending. A unique
or primary key index defines an indexed column that is subject to the uniqueness
constraint.

<a name="pseudo-columns"></a>
### Pseudo-columns

ZetaSQL tables support pseudo-columns. Pseudo-columns contain data elements
that you can query like regular columns, but are not considered real columns in
the table. Pseudo-column values may not be physically stored with each row, but
the query engine will materialize a value for the column using some appropriate
mechanism.

For example, an engine might support a pseudo-column called `ROWNUM`, which
returns a number indicating the order in which a row was returned. You can then
construct a query like this:

```
SELECT ROWNUM, SingerId, FirstName, LastName FROM Singers
WHERE Status = "active"
```

Here's an example of rows returned by this query:

<table>
<thead>
<tr>
<th>ROWNUM</th>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>1</td>
<td>Marc</td>
<td>Richards</td>
</tr>
<tr>
<td>2</td>
<td>3</td>
<td>Lea</td>
<td>Martin</td>
</tr>
</tbody>
</table>

In this case,the schema for the **Singers** table does not define a column,
`ROWNUM`. Instead, the engine materializes the data only when requested.

To return a value of a pseudo-column, you must specify it in your query.
Pseudo-columns do not show up in `SELECT *` statements. For example:

```
SELECT * FROM singers
```

This query will return all named columns in the table, but won't include
pseudo-columns such as `ROWNUM`.

### Value tables

In addition to standard SQL tables, ZetaSQL supports _value tables_.
In a value table, rather than having rows made up of a list of columns, each row
is a single value of a specific type. These types of tables are common when
working with protocol buffers that may be stored in files instead of in the
database. 

<a id="value-table-example"></a>
For example, the following protocol buffer definition, `AlbumReview`, contains
data about the reviews for an album.

```
message AlbumReview {
  optional string albumtitle = 1;
  optional string reviewer = 2;
  optional string review = 3;
}
```

A list of `AlbumReview` protocol buffers is stored in a file, `AlbumReviewData`.

```
{albumtitle: "Songs on a Broken Banjo", reviewer: "Dan Starling", review: "Off key"}
{albumtitle: "Six and Seven", reviewer: "Alice Wayfarer", review: "Hurt my ears!"}
{albumtitle: "Go! Go! Go!", reviewer: "Eustace Millson", review: "My kids loved it!"}
```

The following query returns a stream of rows, with each row a value of type
AlbumReview.

```
SELECT a FROM AlbumReviewsData a
```

To get specific data, such as all album titles in
the table, you have two options. You can specify `albumtitle` as a protocol
buffer field:

```
SELECT a.albumtitle FROM AlbumReviewsData a
```

You can also access the top-level fields inside the value (if there
are any) like columns in a regular SQL table:

```
SELECT albumtitle FROM AlbumReviewsData
```

Value tables are not limited for use with compound data types.
A value table can consist of any supported ZetaSQL data type, although value
tables consisting of scalar types occur less frequently than structs or
protocol buffers.

#### Returning query results as value tables

You can use ZetaSQL to return query results as a value table. This is useful
when you want to create a compound value, such as a protocol buffer, from a
query result and store it as a table that acts like a value table.
To return a query result as a
value table, use the `SELECT AS` statement. See
[Query Syntax][query-syntax-value-tables]
for more information and examples.

##### Example: Copying protocol buffers using value tables

In some cases you might not want to work with the data within a protocol buffer,
but with the protocol buffer itself. 

Using `SELECT AS VALUE` can help you keep your ZetaSQL statements as simple
as possible. To illustrate this, consider the [AlbumReview][value-table-example]
example specified earlier. To create a new table from this data, you could
write:

```
CREATE TABLE Reviews AS
SELECT albumreviews FROM AlbumReviewData albumreviews;
```

This statement creates a standard SQL table that has a single column,
`albumreviews`, which has a protocol buffer value of type
`AlbumReviewData`. To retrieve all album titles from this table, you'd need to
write a query similar to:

```
SELECT r.albumreviews.albumtitle
FROM Reviews r;
```

Now, consider the same initial `CREATE TABLE` statement, this time modified to
use `SELECT AS VALUE`:

```
CREATE TABLE Reviews AS
SELECT AS VALUE albumreviews FROM AlbumReview albumreviews;
```

This statement creates a value table, instead of a standard SQL table. As a
result, you can query any protocol buffer field as if it was a column. Now, if
you want to retrieve all album titles from this table, you can write a much
simpler query:

```
SELECT albumtitle
FROM Reviews;
```

#### Set operations on value tables

Normally, a `SET` operation like `UNION ALL` expects all tables to be either
standard SQL tables or value tables. However, ZetaSQL allows you to combine
standard SQL tables with value tables&mdash;provided that the standard SQL table
consists of a single column with a type that matches the value table's type. The
result of these operations is always a value table.

For example, consider the following definition for a table,
**SingersAndAlbums**.

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
</tr>
</thead>
<tbody>
<tr>
<td>SingerId</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>AlbumId</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>AlbumReview</td>
<td><code>PROTO&lt;AlbumReview&gt;</code></td>
</tr>
</tbody>
</table>

Next, we have a file, `AlbumReviewData` that contains a list of `AlbumReview`
protocol buffers.

```
{albumtitle: "Songs on a Broken Banjo", reviewer: "Dan Starling", review: "Off key"}
{albumtitle: "Six and Seven", reviewer: "Alice Wayfarer", review: "Hurt my ears!"}
{albumtitle: "Go! Go! Go!", reviewer: "Eustace Millson", review: "My kids loved it!"}
```

The following query combines the `AlbumReview` data from the
**SingersAndAlbums** with the data stored in the `AlbumReviewData` file and
stores it in a new value table, **AllAlbumReviews**.

```
SELECT AS VALUE sa.AlbumReview FROM SingersAndAlbums sa
UNION ALL
SELECT a FROM AlbumReviewData a
```

#### Pseudo-columns and value tables

The [Pseudo-columns][pseudo-columns] section  describes how pseudo-columns
work with standard SQL tables. In most cases, pseudo-columns work the same with
value tables. For example, consider this query:

```
SELECT a.ROWNUM, a.albumtitle AS title FROM AlbumReviewData a
```

The following table demonstrates the result of this query:

<table>
<thead>
<tr>
<th>ROWNUM</th>
<th>title</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>"Songs on a Broken Banjo"</td>
</tr>
<tr>
<td>2</td>
<td>"Six and Seven"</td>
</tr>
<tr>
<td>3</td>
<td>"Go! Go! Go!"</td>
</tr>
</tbody>
</table>

This example works because `a` is an alias of the table `AlbumReviewData`, and
this table has a `ROWNUM` pseudo-column. As a result, `AlbumReviewData a` represents the scanned rows,
not the value.

However, if you tried to construct the query like this:

```
SELECT a.ROWNUM, a.albumtitle AS title FROM (SELECT a FROM AlbumReviewData a)
```

This query does not work. The reason it fails is because the subquery, `SELECT
a FROM AlbumReviewData a`, returns an `AlbumReviewData` value only, and this
value does not have a field called `ROWNUM`.

[value-table-example]: #value-table-example
[pseudo-columns]: #pseudo-columns
[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types
[data-manipulation-language]: https://github.com/google/zetasql/blob/master/docs/data-manipulation-language
[query-syntax-value-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax#value_tables

[data-types]: #data-types
[data-manipulation-language]: #data-manipulation-reference
[query-syntax-value-tables]: #value-tables

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## User-Defined Functions

<!-- BEGIN CONTENT -->

ZetaSQL supports
user-defined functions (UDFs). A UDF enables you to create a function using
another SQL expression or another programming
language, such as JavaScript. These functions accept columns of input and perform actions,
returning the result of those actions as a value.

###  General UDF and TVF  Syntax

User-defined functions and table-valued
functions in ZetaSQL use the
following general syntax:

<pre>
CREATE [TEMPORARY | TEMP] [TABLE | AGGREGATE] FUNCTION
  <span class="var">function_name</span> ([<span class="var">function_parameter</span>[, ...]])
  [RETURNS { <span class="var">data_type</span> | TABLE&lt;<span class="var">argument_name data_type</span> [, ...]*&gt; }]
  { [LANGUAGE <span class="var">language</span> AS <span class="var">"""body"""</span>] | [AS (<span class="var">function_definition</span>)] };

<span class="var">function_parameter</span>:
  <span class="var">param_name param_type</span> [NOT AGGREGATE]
</pre>

This syntax consists of the following components:

+   **CREATE [TEMPORARY | TEMP]
    [TABLE | AGGREGATE] FUNCTION**.
    Creates a new function. A function can contain zero or more
    `function_parameter`s. To make the function
    temporary, use the `TEMP` or `TEMPORARY` keyword. To
    make a [table-valued function][table-valued function], use the `TABLE`
    keyword. To make
    an [aggregate function][aggregate-udf-parameters], use the
    `AGGREGATE` keyword.
*   **function_parameter**. Consists of a comma-separated `param_name` and
    `param_type` pair. The value of `param_type` is a ZetaSQL
    [data type][data-types].
    The value of `param_type` may also be `ANY TYPE` for a
    SQL user-defined function or `ANY TABLE` for a table-valued function.
+   **[RETURNS data_type]**. Specifies the data type
    that the function returns. If the function is defined in SQL, then the
    `RETURNS` clause is optional and ZetaSQL infers the result type
    of the function from the SQL function body. If the
    function is defined in an external language, then the `RETURNS` clause is
    required. See [Supported UDF data types][supported-external-udf-data-types]
    for more information about allowed values for
    `data_type`.
+   **[RETURNS TABLE]**. Specifies the schema of the table that a table-valued
    function returns as a comma-separated list of `argument_name` and `TYPE`
    pairs. Can only appear after `CREATE TEMP TABLE FUNCTION`. If
    `RETURNS TABLE` is absent, ZetaSQL infers the output schema from
    the `AS SELECT...` statement in the function body.
    
*   **[LANGUAGE language AS """body"""]**. Specifies the external language
    for the function and the code that defines the function.
*   **AS (function_definition)**. Specifies the SQL code that defines the
    function. For user-defined functions (UDFs),
    `function_definition` is a SQL expression. For table-valued functions
    (TVFs), `function_definition` is a SQL query.
*   **[NOT AGGREGATE]**. Specifies that a parameter is not
    aggregate. Can only appear after `CREATE AGGREGATE FUNCTION`. A
    non-aggregate parameter [can appear anywhere in the function
    definition.][aggregate-udf-parameters]

### External UDF structure

Create external UDFs using the following structure.

```sql
CREATE [TEMPORARY | TEMP] FUNCTION function_name ([function_parameter[, ...]])
  [RETURNS data_type]
  [LANGUAGE language]
  AS external_code
```

### External UDF examples

The following example creates a UDF.

```sql
CREATE FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, multiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can make this function temporary by using the `TEMP` or `TEMPORARY`
keyword.

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, multiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can create multiple UDFs before a query. For example:

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION divideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x / 2;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x,
  y,
  multiplyInputs(x, y) as product,
  divideByTwo(x) as half_x,
  divideByTwo(y) as half_y
FROM numbers;

+-----+-----+--------------+--------+--------+
| x   | y   | product      | half_x | half_y |
+-----+-----+--------------+--------+--------+
| 1   | 5   | 5            | 0.5    | 2.5    |
| 2   | 10  | 20           | 1      | 5      |
| 3   | 15  | 45           | 1.5    | 7.5    |
+-----+-----+--------------+--------+--------+
```

You can pass the result of a UDF as input to another UDF. For example:

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION divideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x/2;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x,
  y,
  multiplyInputs(divideByTwo(x), divideByTwo(y)) as half_product
FROM numbers;

+-----+-----+--------------+
| x   | y   | half_product |
+-----+-----+--------------+
| 1   | 5   | 1.25         |
| 2   | 10  | 5            |
| 3   | 15  | 11.25        |
+-----+-----+--------------+
```

 The following example sums the values of all
fields named "foo" in the given JSON string.

```sql
CREATE TEMP FUNCTION SumFieldsNamedFoo(json_row STRING)
  RETURNS FLOAT64
  LANGUAGE js AS """
function SumFoo(obj) {
  var sum = 0;
  for (var field in obj) {
    if (obj.hasOwnProperty(field) && obj[field] != null) {
      if (typeof obj[field] == "object") {
        sum += SumFoo(obj[field]);
      } else if (field == "foo") {
        sum += obj[field];
      }
    }
  }
  return sum;
}
var row = JSON.parse(json_row);
return SumFoo(row);
""";

WITH Input AS (
  SELECT STRUCT(1 AS foo, 2 AS bar, STRUCT('foo' AS x, 3.14 AS foo) AS baz) AS s, 10 AS foo UNION ALL
  SELECT NULL, 4 AS foo UNION ALL
  SELECT STRUCT(NULL, 2 AS bar, STRUCT('fizz' AS x, 1.59 AS foo) AS baz) AS s, NULL AS foo
)
SELECT
  TO_JSON_STRING(t) AS json_row,
  SumFieldsNamedFoo(TO_JSON_STRING(t)) AS foo_sum
FROM Input AS t;
+---------------------------------------------------------------------+---------+
| json_row                                                            | foo_sum |
+---------------------------------------------------------------------+---------+
| {"s":{"foo":1,"bar":2,"baz":{"x":"foo","foo":3.14}},"foo":10}       | 14.14   |
| {"s":null,"foo":4}                                                  | 4       |
| {"s":{"foo":null,"bar":2,"baz":{"x":"fizz","foo":1.59}},"foo":null} | 1.59    |
+---------------------------------------------------------------------+---------+
```

#### Supported external UDF languages

External UDFs support code written in JavaScript, which you specify using `js`
as the `LANGUAGE`. For example:

```sql
CREATE TEMP FUNCTION greeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
  return "Hello, " + a + "!";
  """;
SELECT greeting(name) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS name;

+----------------+
| everyone       |
+----------------+
| Hello, Hannah! |
| Hello, Max!    |
| Hello, Jakob!  |
+----------------+
```

See [SQL type encodings in JavaScript](#sql-type-encodings-in-javascript) for
information on how ZetaSQL data types map to JavaScript types.

#### Supported external UDF data types

For external UDFs, ZetaSQL supports the following data types:

<ul>
<li>ARRAY</li><li>BOOL</li><li>BYTES</li><li>DATE</li><li>DOUBLE</li><li>FLOAT</li><li>INT32</li><li>NUMERIC</li><li>STRING</li><li>STRUCT</li><li>TIMESTAMP</li>
</ul>

#### SQL type encodings in JavaScript

Some SQL types have a direct mapping to JavaScript types, but others do not.

Because JavaScript does not support a 64-bit integer type,
`INT64` is unsupported as an input type for JavaScript
UDFs. Instead, use `DOUBLE` to represent integer
values as a number, or `STRING` to represent integer
values as a string.

ZetaSQL does support `INT64` as a return type
in JavaScript UDFs. In this case, the JavaScript function body can return either
a JavaScript Number or a String. ZetaSQL then converts either of
these types to `INT64`.

ZetaSQL represents types in the following manner:

<table>
<tr>
<th>ZetaSQL Data Type</th>
<th>JavaScript Data Type</th>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
</tr>

<tr>
<td>BOOL</td>
<td>BOOLEAN</td>
</tr>

<tr>
<td>BYTES</td>
<td>base64-encoded STRING</td>
</tr>

<tr>
<td>DOUBLE</td>
<td>NUMBER</td>
</tr>

<tr>
<td>FLOAT</td>
<td>NUMBER</td>
</tr>

<tr>
<td>NUMERIC</td>
<td>If a NUMERIC value can be represented exactly as an
  <a href="https://en.wikipedia.org/wiki/Floating-point_arithmetic#IEEE_754:_floating_point_in_modern_computers">IEEE 754 floating-point</a>
  value and has no fractional part, it is encoded as a Number. These values are
  in the range [-2<sup>53</sup>, 2<sup>53</sup>]. Otherwise, it is encoded as a
  String.</td>
</tr>

<tr>
<td>INT32</td>
<td>NUMBER</td>
</tr>

<tr>
<td>STRING</td>
<td>STRING</td>
</tr>

<tr>
<td>STRUCT</td>
<td>OBJECT where each STRUCT field is a named field</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td>DATE with a microsecond field containing the <code>microsecond</code>
fraction of the timestamp</td>
</tr>

<tr>
<td>DATE</td>
<td>DATE</td>
</tr>

<tr>
<td>UINT32</td>
<td>NUMBER</td>
</tr>

</table>

#### Quoting rules

You must enclose external code in quotes. For simple, one line code snippets,
you can use a standard quoted string:

```sql
CREATE TEMP FUNCTION plusOne(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js
AS "return x+1;";
SELECT val, plusOne(val) AS result
FROM UNNEST([1, 2, 3, 4, 5]) AS val;

+-----------+-----------+
| val       | result    |
+-----------+-----------+
| 1         | 2         |
| 2         | 3         |
| 3         | 4         |
| 4         | 5         |
| 5         | 6         |
+-----------+-----------+
```

In cases where the snippet contains quotes, or consists of multiple lines, use
triple-quoted blocks:

```sql
CREATE TEMP FUNCTION customGreeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
  var d = new Date();
  if (d.getHours() < 12) {
    return 'Good Morning, ' + a + '!';
  } else {
    return 'Good Evening, ' + a + '!';
  }
  """;
SELECT customGreeting(names) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS names;
+-----------------------+
| everyone              |
+-----------------------+
| Good Morning, Hannah! |
| Good Morning, Max!    |
| Good Morning, Jakob!  |
+-----------------------+
```

### SQL UDF structure

Create SQL UDFs using the following syntax:

<pre>
CREATE [TEMPORARY | TEMP] [AGGREGATE] FUNCTION <span class="var">function_name</span> ([<span class="var">function_parameter</span>[, ...]])
  [RETURNS <span class="var">data_type</span>]
  AS (<span class="var">sql_expression</span>)

<span class="var">function_parameter</span>:
  <span class="var">param_name param_type</span> [NOT AGGREGATE]
</pre>

#### Aggregate UDF parameters

An aggregate UDF can take aggregate or non-aggregate parameters. Like other
[aggregate functions][aggregate-fns-link], aggregate UDFs normally aggregate
parameters across all rows in a [group][group-by-link]. However, you can specify
a parameter as non-aggregate with the `NOT AGGREGATE` keyword. A non-aggregate
parameter is a scalar argument with a constant value for all rows in a group;
for example, literals or grouping expressions are valid non-aggregate
parameters. Inside the UDF definition, aggregate parameters can only appear as
arguments to aggregate function calls. Non-aggregate parameters can appear
anywhere in the UDF definition.

#### Templated SQL UDF parameters

A templated parameter can match more than one argument type at function call
time. If a function signature includes a templated parameter, ZetaSQL
allows function calls to pass one of several argument types to the function.

SQL user-defined function signatures can contain the following templated
`param_type` value:

+ `ANY TYPE`. The function will accept an input of any type for this argument.
  If more than one parameter has the type `ANY TYPE`, ZetaSQL does
  not enforce any relationship between these arguments at the time of function
  creation. However, passing the function arguments of types that are
  incompatible with the function definition will result in an error at call
  time.

### SQL UDF examples

The following example shows a UDF that employs a SQL function.

```sql
CREATE TEMP FUNCTION addFourAndDivide(x INT64, y INT64) AS ((x + 4) / y);
WITH numbers AS
  (SELECT 1 as val
  UNION ALL
  SELECT 3 as val
  UNION ALL
  SELECT 4 as val
  UNION ALL
  SELECT 5 as val)
SELECT val, addFourAndDivide(val, 2) AS result
FROM numbers;

+-----+--------+
| val | result |
+-----+--------+
| 1   | 2.5    |
| 3   | 3.5    |
| 4   | 4      |
| 5   | 4.5    |
+-----+--------+
```

The following example shows an aggregate UDF that uses a non-aggregate
parameter. Inside the function definition, the aggregate `SUM` method takes the
aggregate parameter `dividend`, while the non-aggregate division operator
( `/` ) takes the non-aggregate parameter `divisor`.

```sql
CREATE TEMP AGGREGATE FUNCTION scaled_sum(dividend DOUBLE, divisor DOUBLE NOT AGGREGATE)
AS (SUM(dividend) / divisor);
SELECT scaled_sum(col1, 2) AS scaled_sum
FROM (SELECT 1 AS col1 UNION ALL
      SELECT 3 AS col1 UNION ALL
      SELECT 5 AS col1
);

+------------+
| scaled_sum |
+------------+
| 4.5        |
+------------+
```

The following example shows a SQL UDF that uses a
[templated parameter][templated-parameters]. The resulting function
accepts arguments of various types.

```sql
CREATE TEMP FUNCTION addFourAndDivideAny(x ANY TYPE, y ANY TYPE) AS (
  (x + 4) / y
);
SELECT addFourAndDivideAny(3, 4) AS integer_output,
       addFourAndDivideAny(1.59, 3.14) AS floating_point_output;

+----------------+-----------------------+
| integer_output | floating_point_output |
+----------------+-----------------------+
| 1.75           | 1.7802547770700636    |
+----------------+-----------------------+
```

The following example shows a SQL UDF that uses a
[templated parameter][templated-parameters] to return the last
element of an array of any type.

```sql
CREATE TEMP FUNCTION lastArrayElement(arr ANY TYPE) AS (
  arr[ORDINAL(ARRAY_LENGTH(arr))]
);
SELECT
  names[OFFSET(0)] AS first_name,
  lastArrayElement(names) AS last_name
FROM (
  SELECT ['Fred', 'McFeely', 'Rogers'] AS names UNION ALL
  SELECT ['Marie', 'Skłodowska', 'Curie']
);

+------------+-----------+
| first_name | last_name |
+------------+-----------+
| Fred       | Rogers    |
| Marie      | Curie     |
+------------+-----------+
```

### Table-valued functions {#tvfs}

A *table-valued function* (TVF) is a function that returns an entire output
table instead of a single scalar value, and appears in the FROM clause like a
table subquery.

#### TVF structure

You create a TVF using the following structure.

```sql
CREATE TEMPORARY | TEMP TABLE FUNCTION function_name
  ([argument_name data_type][, ...]*)
  [RETURNS TABLE<argument_name data_type [, ...]*>]
  [[LANGUAGE language AS """body"""] | [AS SELECT...]];
```

#### Specifying TVF arguments {#tvf-arguments}

An argument to a TVF can be of any supported ZetaSQL type or a table.

Specify a table argument the same way you specify the fields of a
[STRUCT][data-types-struct].

```sql
argument_name TABLE<column_name data_type , column_name data_type [, ...]*>
```

The table argument can specify a [value table][datamodel-value-tables],
in which each row
is a single column of a specific type. To specify a value table as an argument,
include only the `data_type`, leaving out the `column_name`:

```sql
argument_name TABLE<data_type>
```

In many cases, the `data_type` of the single column in the value table is a
protocol buffer; for example:

```sql
CREATE TEMP TABLE FUNCTION AggregatedMovieLogs(
  TicketPurchases TABLE<analysis_conduit.codelab.MovieTicketPurchase>)
```

The function body can refer directly to fields within the proto.

You have the option to specify the input table using the templated type `ANY
TABLE` in place of `TABLE<column_name data_type [, ...]*>`. This option enables
you to create a polymorphic TVF that accepts any table as input.

**Example**

The following example implements a pair of TVFs that define parameterized views
of a range of rows from the Customer table. The first returns all rows for a
range of CustomerIds; the second calls the first function and applies an
additional filter based on CustomerType.

```sql
CREATE TEMP TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)
  AS
    SELECT *
    FROM Customer
    WHERE CustomerId >= MinId AND CustomerId <= MaxId;

CREATE TEMP TABLE FUNCTION CustomerRangeWithCustomerType(
    MinId INT64,
    MaxId INT64,
    customer_type ads.boulder.schema.CustomerType)
  AS
    SELECT * FROM CustomerRange(MinId, MaxId)
    WHERE Info.type = customer_type;
```

##### Templated SQL TVF Parameters

SQL table-valued function signatures can contain the following [templated
parameter][templated-parameters] values for `param_type`:

+ `ANY TYPE`. The function will accept an input of any type for this argument.
  If more than one parameter has the type `ANY TYPE`, ZetaSQL does
  not enforce any relationship between these arguments at the time of function
  creation. However, passing the function arguments of types that are
  incompatible with the function definition will result in an error at call
  time.
+ `ANY TABLE`. The function will accept an
argument of any relation type for this argument. However, passing the function
arguments of types that are incompatible with the function definition will
result in an error at call time.

**Examples**

The following function returns all rows from the input table if the first
argument is greater than the second argument; otherwise it returns no rows.

```sql
CREATE TEMP TABLE FUNCTION MyFunction(
     first ANY TYPE,
     second ANY TYPE,
     third ANY TABLE)
   AS
     SELECT *
     FROM third
     WHERE first > second;

```

The following function accepts two integers and a table with any set of columns
and returns rows from the table where the predicate evaluates to true. The input
table `selected_customers` must contain a column named `Info` that has a field
named `creation_time`, and `creation_time` must be a numeric type, or the
function will return an error.

```sql
CREATE TEMP TABLE FUNCTION CustomerInfoProtoCreationTimeRange(
    min_creation_time INT64,
    max_creation_time INT64,
    selected_customers ANY TABLE)
  AS
    SELECT *
    FROM selected_customers
    WHERE Info.creation_time >= min_creation_time
    AND Info.creation_time <= max_creation_time;
```

#### Calling TVFs

To call a TVF, use the function call in place of the table name in a `FROM`
clause.

**Example**

The following query calls the `CustomerRangeWithCustomerType` function to
return a table with rows for customers with a CustomerId between 100
and 200.

```sql
SELECT CustomerId, Info
FROM CustomerRangeWithCustomerType(100, 200, 'CUSTOMER_TYPE_ADVERTISER');
```

[table-valued function]: #tvfs
[aggregate-udf-parameters]: #aggregate-udf-parameters
[templated-parameters]: #templated-sql-udf-parameters
[supported-external-udf-data-types]: #supported-external-udf-data-types
[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types
[data-types-struct]: https://github.com/google/zetasql/blob/master/docs/data-types#struct_type
[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model#value-tables
[group-by-link]: https://github.com/google/zetasql/blob/master/docs/query-syntax#group_by_clause
[aggregate-fns-link]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions

[data-types]: #data-types
[data-types-struct]: #struct-type
[datamodel-value-tables]: #value-tables
[aggregate-fns-link]: #aggregate-functions
[group-by-link]: #group-by-clause

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Approximate Aggregation

<!-- BEGIN CONTENT -->

This topic explains the concepts behind approximate
aggregation in ZetaSQL.

### What is approximate aggregation?

Approximate aggregation is the estimation of aggregate function
outputs, such as cardinality and quantiles. Approximate
aggregation requires less memory than normal aggregation functions like
`COUNT(DISTINCT ...)`, but also introduces statistical uncertainty. This makes it
appropriate for large data streams for which linear memory usage is impractical,
as well as for data that is already approximate. Where exact results are
necessary, use exact [aggregate functions][exact-aggregate-fns]. This topic
describes the concepts behind approximate aggregation in ZetaSQL.

### Fixed-precision approximate aggregation

ZetaSQL supports
[aggregate functions with fixed precision][link-to-APPROX_] for estimating
cardinality and quantiles. These functions work directly on the input data,
rather than an intermediate estimation of the data. These functions do not allow
users to specify the precision for the estimation.

### Storing estimated aggregate values as sketches

ZetaSQL supports
[HyperLogLog++ cardinality estimation functions][hll-functions] for estimating
the number of distinct values in a large dataset.
ZetaSQL supports [KLL16 quantile estimation functions][kll-functions]
for estimating quantile values in a large dataset.

These functions operate on sketches that compress an arbitrary set into a
fixed-memory representation. ZetaSQL stores these sketches as
`BYTES`. You can merge the sketches to produce a new sketch that represents the
union of the input sketches, before you extract a final numeric estimate from
the sketch.

For example, consider the following table, which contains ice-cream flavors and
the number of people who claimed to enjoy that flavor:

Flavor            | People
------------------|--------
Vanilla           | 3945
Chocolate         | 1728
Strawberry        | 2051

If this table is the result of aggregation, then it may not be possible to use
the table to calculate cardinality. If you wanted to know the number of unique
respondents, you couldn't use `SUM` to aggregate the `People` column, because
some respondents may have responded positively to more than one flavor. On the
other hand, performing an aggregate function over the underlying raw
data can consume large amounts of time and memory.

One solution is to store an approximate aggregate or **sketch** of the raw
data. A sketch is a summary of the raw data. Sketches require less memory than
the raw data, and you can extract an estimate, such as the estimated number of
unique users, from the sketch.

#### Specifying approximation precision

ZetaSQL's sketch-based approximate aggregation functions
allow you to specify the precision of the sketch when you create it. The
precision of the sketch affects the accuracy of the estimate you can extract
from the sketch. Higher precision requires additional memory to process the
sketches or store them on disk, and reduces the relative error of any estimate
you extract from the sketch. Once you have created the sketch, you can only
merge it with other sketches of the same precision.

#### Merging sketches

You can merge two or more sketches to produce a new sketch which represents an
estimate of the union of the data underlying the different sketches. The merge
function, such as `HLL_COUNT.MERGE`,
returns the estimate as a number, whereas the partial merge
function, such as
`HLL_COUNT.MERGE_PARTIAL`, returns the new sketch in `BYTES`.
A partial merge is useful if you want to reduce a table that already contains
sketches, but do not yet want to extract an estimate. For example, use this
function if you want to create a sketch that you will merge with another sketch
later.

#### Extracting estimates from sketches

Once you have stored a sketch or merged two or more sketches into a new sketch,
you can use the extract function, such as
`HLL_COUNT.EXTRACT`, to return an estimate of the underlying data,
such as the estimated number of unique users, as a number.

### Algorithms

 This section describes the
approximate aggregate algorithms that ZetaSQL supports.

#### HyperLogLog++

The [HyperLogLog++][HyperLogLogPlusPlus-paper] algorithm is an improvement on
the [HyperLogLog][hll-link-to-hyperloglog-wikipedia] algorithm for estimating
distinct values in a data set. In ZetaSQL, the
[HyperLogLog++ cardinality estimation functions][hll-functions] use this
algorithm. The HyperLogLog++ algorithm improves on the HyperLogLog algorithm by
using bias correction to reduce error for an important range of cardinalities.

#### KLL16

The [KLL16][link-to-kll-paper] algorithm is an algorithm for estimating
quantile values in a data set. In ZetaSQL, the
[KLL16 quantile estimation functions][kll-functions] use this
algorithm. The KLL16 algorithm improves on the [MP80 algorithm][mp80] by
using variable-size buffers to reduce memory use for large data sets.

[link-to-kll-paper]: https://arxiv.org/pdf/1603.05346v2.pdf
[mp80]: https://polylogblog.files.wordpress.com/2009/08/80munro-median.pdf
[HyperLogLogPlusPlus-paper]: https://research.google.com/pubs/pub40671.html
[hll-link-to-hyperloglog-wikipedia]: https://en.wikipedia.org/wiki/HyperLogLog

[hll-link-to-approx-count-distinct]: #approx_count_distinct
[link-to-APPROX_]: #approximate-aggregate-functions
[hll-functions]: #hyperloglog-functions
[exact-aggregate-fns]: #aggregate-functions
[kll-functions]: #kll16-quantile-functions

<!-- END CONTENT -->

<!-- Functions -->
## Function Reference

## Aggregate functions

An *aggregate function* is a function that performs a calculation on a set of
values. `COUNT`, `MIN` and `MAX` are examples of aggregate functions.

```sql
SELECT COUNT(*) as total_count, COUNT(fruit) as non_null_count,
       MIN(fruit) as min, MAX(fruit) as max
FROM (SELECT NULL as fruit UNION ALL
      SELECT "apple" as fruit UNION ALL
      SELECT "pear" as fruit UNION ALL
      SELECT "orange" as fruit)

+-------------+----------------+-------+------+
| total_count | non_null_count | min   | max  |
+-------------+----------------+-------+------+
| 4           | 3              | apple | pear |
+-------------+----------------+-------+------+
```

The following sections describe the aggregate functions that ZetaSQL
supports.

#### ANY_VALUE

```
ANY_VALUE(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
Rows for which `expression` is `NULL` are considered and may be selected.

**Supported Argument Types**

Any

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

Matches the input data type.

**Examples**

```sql
SELECT ANY_VALUE(fruit) as any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+-----------+
| any_value |
+-----------+
| apple     |
+-----------+
```

```sql
SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+--------+-----------+
| fruit  | any_value |
+--------+-----------+
| pear   | pear      |
| apple  | pear      |
| banana | apple     |
+--------+-----------+
```

#### ARRAY_AGG
```
ARRAY_AGG([DISTINCT] expression [{IGNORE|RESPECT} NULLS] [HAVING (MAX | MIN) expression2]
          [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
[OVER (...)]
```

**Description**

Returns an ARRAY of `expression` values.

**Supported Argument Types**

All data types except ARRAY.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified,
    the NULL values are excluded from the result. If `RESPECT NULLS` is
    specified or if neither is specified,
    the NULL values are included in the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit `n` must be a constant INT64.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

ARRAY

If there are zero input rows, this function returns `NULL`.

**Examples**

```sql
SELECT ARRAY_AGG(x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [2, 1, -2, 3, -2, 1, 2] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+---------------+
| array_agg     |
+---------------+
| [2, 1, -2, 3] |
+---------------+
```

```sql
SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [1, -2, 3, -2, 1] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [1, 1, 2, -2, -2, 2, 3] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [2, 1, -2, 3, -2] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x LIMIT 2) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-----------+
| array_agg |
+-----------+
| [-2, 1]   |
+-----------+
```

```sql
SELECT
  x,
  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+----+-------------------------+
| x  | array_agg               |
+----+-------------------------+
| 1  | [1, 1]                  |
| 1  | [1, 1]                  |
| 2  | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| 2  | [1, 1, 2, -2, -2, 2]    |
| 3  | [1, 1, 2, -2, -2, 2, 3] |
+----+-------------------------+
```

#### ARRAY_CONCAT_AGG

```
ARRAY_CONCAT_AGG(expression [HAVING (MAX | MIN) expression2]  [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
```

**Description**

Concatenates elements from `expression` of type
ARRAY, returning a single
ARRAY as a result. This function ignores NULL input
arrays, but respects the NULL elements in non-NULL input arrays.

**Supported Argument Types**

ARRAY

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input arrays, not
    the number of elements in the arrays. An empty array counts as 1. A NULL
    array is not counted.
    The limit `n` must be a constant INT64.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

ARRAY

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x)) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [5, 6, 7, 8, 9, 1, 2, 3, 4]       |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+--------------------------+
| array_concat_agg         |
+--------------------------+
| [1, 2, 3, 4, 5, 6]       |
+--------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+------------------+
| array_concat_agg |
+------------------+
| [5, 6, 7, 8, 9]  |
+------------------+
```

#### AVG
```
AVG([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the average of non-`NULL` input values, or `NaN` if the input contains a
`NaN`.

**Supported Argument Types**

Any numeric input type, such as  INT64. Note that, for
floating point input types, the return result is non-deterministic, which
means you might receive a different result each time you use this function.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

+ NUMERIC if
  the input type is NUMERIC.
+ DOUBLE

**Examples**

```sql
SELECT AVG(x) as avg
FROM UNNEST([0, 2, 4, 4, 5]) as x;

+-----+
| avg |
+-----+
| 3   |
+-----+
```

```sql
SELECT AVG(DISTINCT x) AS avg
FROM UNNEST([0, 2, 4, 4, 5]) AS x;

+------+
| avg  |
+------+
| 2.75 |
+------+
```

```sql
SELECT
  x,
  AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x;

+------+------+
| x    | avg  |
+------+------+
| NULL | NULL |
| 0    | 0    |
| 2    | 1    |
| 4    | 3    |
| 4    | 4    |
| 5    | 4.5  |
+------+------+
```

#### BIT_AND
```
BIT_AND([DISTINCT] expression [HAVING (MAX | MIN) expression2])
```

**Description**

Performs a bitwise AND operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x;

+---------+
| bit_and |
+---------+
| 1       |
+---------+
```

#### BIT_OR
```
BIT_OR([DISTINCT] expression [HAVING (MAX | MIN) expression2])
```

**Description**

Performs a bitwise OR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x;

+--------+
| bit_or |
+--------+
| 61601  |
+--------+
```

#### BIT_XOR
```
BIT_XOR([DISTINCT] expression [HAVING (MAX | MIN) expression2])
```

**Description**

Performs a bitwise XOR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 5678    |
+---------+
```

```sql
SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

#### COUNT

1.
```
COUNT(*)  [OVER (...)]
```

2.
```
COUNT([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

**Supported Argument Types**

`expression` can be any data type.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

INT64

**Examples**

```sql
SELECT
  COUNT(*) AS count_star,
  COUNT(DISTINCT x) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------------+--------------+
| count_star | count_dist_x |
+------------+--------------+
| 4          | 3            |
+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------+------------+--------------+
| x    | count_star | count_dist_x |
+------+------------+--------------+
| 1    | 3          | 2            |
| 4    | 3          | 2            |
| 4    | 3          | 2            |
| 5    | 1          | 1            |
+------+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(x) OVER (PARTITION BY MOD(x, 3)) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

+------+------------+---------+
| x    | count_star | count_x |
+------+------------+---------+
| NULL | 1          | 0       |
| 1    | 3          | 3       |
| 4    | 3          | 3       |
| 4    | 3          | 3       |
| 5    | 1          | 1       |
+------+------------+---------+
```

#### COUNTIF
```
COUNTIF([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the count of `TRUE` values for `expression`. Returns `0` if there are
zero input rows, or if `expression` evaluates to `FALSE` or `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

INT64

**Examples**

```sql
SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive
FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x;

+--------------+--------------+
| num_negative | num_positive |
+--------------+--------------+
| 3            | 4            |
+--------------+--------------+
```

```sql
SELECT
  x,
  COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS num_negative
FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x;

+------+--------------+
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
+------+--------------+
```

#### LOGICAL_AND
```
LOGICAL_AND(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the logical AND of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_AND(x) AS logical_and FROM UNNEST([true, false, true]) AS x;

+-------------+
| logical_and |
+-------------+
| false       |
+-------------+
```

#### LOGICAL_OR
```
LOGICAL_OR(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the logical OR of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_OR(x) AS logical_or FROM UNNEST([true, false, true]) AS x;

+------------+
| logical_or |
+------------+
| true       |
+------------+
```

#### MAX
```
MAX(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the maximum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any data type except:
`ARRAY`
`STRUCT`
`PROTO`

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MAX(x) AS max
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| max |
+-----+
| 55  |
+-----+
```

```sql
SELECT x, MAX(x) OVER (PARTITION BY MOD(x, 2)) AS max
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | max  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 8    |
| 4    | 8    |
| 37   | 55   |
| 55   | 55   |
+------+------+
```

#### MIN
```
MIN(expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the minimum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any data type except:
`ARRAY`
`STRUCT`
`PROTO`

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MIN(x) AS min
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| min |
+-----+
| 4   |
+-----+
```

```sql
SELECT x, MIN(x) OVER (PARTITION BY MOD(x, 2)) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | min  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 4    |
| 4    | 4    |
| 37   | 37   |
| 55   | 37   |
+------+------+
```

#### STRING_AGG
```
STRING_AGG([DISTINCT] expression [, delimiter] [HAVING (MAX | MIN) expression2]  [ORDER BY key [{ASC|DESC}] [, ... ]]  [LIMIT n])
[OVER (...)]
```

**Description**

Returns a value (either STRING or
BYTES) obtained by concatenating non-null values.

If a `delimiter` is specified, concatenated values are separated by that
delimiter; otherwise, a comma is used as a delimiter.

**Supported Argument Types**

STRING
BYTES

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input strings,
    not the number of characters or bytes in the inputs. An empty string counts
    as 1. A NULL string is not counted.
    The limit `n` must be a constant INT64.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

STRING
BYTES

**Examples**

```sql
SELECT STRING_AGG(fruit) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+------------------------+
| string_agg             |
+------------------------+
| apple,pear,banana,pear |
+------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| apple & pear & banana & pear |
+------------------------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+-----------------------+
| string_agg            |
+-----------------------+
| apple & pear & banana |
+-----------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| pear & pear & apple & banana |
+------------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+--------------+
| string_agg   |
+--------------+
| apple & pear |
+--------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+---------------+
| string_agg    |
+---------------+
| pear & banana |
+---------------+
```

```sql
SELECT
  fruit,
  STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+--------+------------------------------+
| fruit  | string_agg                   |
+--------+------------------------------+
| NULL   | NULL                         |
| pear   | pear & pear                  |
| pear   | pear & pear                  |
| apple  | pear & pear & apple          |
| banana | pear & pear & apple & banana |
+--------+------------------------------+
```

#### SUM
```
SUM([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the sum of non-null values.

If the expression is a floating point value, the sum is non-deterministic, which
means you might receive a different result each time you use this function.

**Supported Argument Types**

Any supported numeric data types.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Data Types**

+ Returns INT64 if the input is a signed integer.
+ Returns UINT64 if the input is an unsigned integer.
+ Returns
  NUMERIC if the input type is
  NUMERIC.
+ Returns DOUBLE if the input is a floating point
value.

Returns `NULL` if the input contains only `NULL`s.

Returns `Inf` if the input contains `Inf`.

Returns `-Inf` if the input contains `-Inf`.

Returns `NaN` if the input contains a `NaN`.

Returns `NaN` if the input contains a combination of `Inf` and `-Inf`.

**Examples**

```sql
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 25  |
+-----+
```

```sql
SELECT SUM(DISTINCT x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 15  |
+-----+
```

```sql
SELECT
  x,
  SUM(x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
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
+---+-----+
```

```sql
SELECT
  x,
  SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
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
+---+-----+
```

## Aggregate Analytic Functions

The following sections describe the aggregate analytic functions that
ZetaSQL supports. For an explanation of how analytic functions work,
see [Analytic Function Concepts][analytic-function-concepts]. For an explanation
of how aggregate analytic functions work, see
[Aggregate Analytic Function Concepts][aggregate-analytic-concepts].

ZetaSQL supports the following
[aggregate functions][analytic-functions-link-to-aggregate-functions]
as analytic functions:

* [ANY_VALUE](#any_value)
* [ARRAY_AGG](#array_agg)
* [AVG](#avg)
* [CORR](#corr)
* [COUNT](#count)
* [COUNTIF](#countif)
* [COVAR_POP](#covar_pop)
* [COVAR_SAMP](#covar_samp)
* [LOGICAL_AND](#logical_and)
* [LOGICAL_OR](#logical_or)
* [MAX](#max)
* [MIN](#min)
* [STDDEV_POP](#stddev_pop)
* [STDDEV_SAMP](#stddev_samp)
* [STRING_AGG](#string_agg)
* [SUM](#sum)
* [VAR_POP](#var_pop)
* [VAR_SAMP](#var_samp)

`OVER` clause requirements:

+ `PARTITION BY`: Optional.
+ `ORDER BY`: Optional. Disallowed if `DISTINCT` is present.
+ `window_frame_clause`: Optional. Disallowed if `DISTINCT` is present.

Example:

```
COUNT(*) OVER (ROWS UNBOUNDED PRECEDING)
```

```
SUM(DISTINCT x) OVER ()
```

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[aggregate-analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#aggregate_analytic_function_concepts

[analytic-functions-link-to-aggregate-functions]: #aggregate_functions

[analytic-function-concepts]: #analytic-function-concepts
[aggregate-analytic-concepts]: #aggregate-analytic-function-concepts
[analytic-functions-link-to-aggregate-functions]: #aggregate-functions

## Approximate Aggregate Functions

Approximate aggregate functions are scalable in terms of memory usage and time,
but produce approximate results instead of exact results. For more background,
see [Approximate Aggregation][link-to-approximate-aggregation].

#### APPROX_COUNT_DISTINCT

```
APPROX_COUNT_DISTINCT(expression)
```

**Description**

Returns the approximate result for `COUNT(DISTINCT expression)`. The value
returned is a statistical estimate&mdash;not necessarily the actual value.

This function is less accurate than `COUNT(DISTINCT expression)`, but performs
better on huge input.

**Supported Argument Types**

Any data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

INT64

**Examples**

```sql
SELECT APPROX_COUNT_DISTINCT(x) as approx_distinct
FROM UNNEST([0, 1, 1, 2, 3, 5]) as x;

+-----------------+
| approx_distinct |
+-----------------+
| 5               |
+-----------------+
```

#### APPROX_QUANTILES

```
APPROX_QUANTILES([DISTINCT] expression, number [{IGNORE|RESPECT} NULLS] [HAVING (MAX | MIN) expression2])
```

**Description**

Returns the approximate boundaries for a group of `expression` values, where
`number` represents the number of quantiles to create. This function returns
an array of `number` + 1 elements, where the first element is the approximate
minimum and the last element is the approximate maximum.

**Supported Argument Types**

`expression` can be any supported data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

`number` must be INT64.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified or if neither is specified,
    the NULL values are excluded from the result. If `RESPECT NULLS` is
    specified,
    the NULL values are included in the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

An ARRAY of the type specified by the `expression`
parameter.

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT APPROX_QUANTILES(x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 5, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 100)[OFFSET(90)] AS percentile_90
FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS x;

+---------------+
| percentile_90 |
+---------------+
| 9             |
+---------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 6, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 4, 10]    |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 6, 10]    |
+------------------+
```

#### APPROX_TOP_COUNT

```
APPROX_TOP_COUNT(expression, number [HAVING (MAX | MIN) expression2])
```

**Description**

Returns the approximate top elements of `expression`. The `number` parameter
specifies the number of elements returned.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields. The first field
(named `value`) contains an input value. The second field (named `count`)
contains an INT64 specifying the number of times the
value was returned.

Returns `NULL` if there are zero input rows.

**Examples**

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST(["apple", "apple", "pear", "pear", "pear", "banana"]) as x;

+-------------------------+
| approx_top_count        |
+-------------------------+
| [{pear, 3}, {apple, 2}] |
+-------------------------+
```

**NULL handling**

APPROX_TOP_COUNT does not ignore NULLs in the input. For example:

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST([NULL, "pear", "pear", "pear", "apple", NULL]) as x;

+------------------------+
| approx_top_count       |
+------------------------+
| [{pear, 3}, {NULL, 2}] |
+------------------------+
```

#### APPROX_TOP_SUM

```
APPROX_TOP_SUM(expression, weight, number [HAVING (MAX | MIN) expression2])
```

**Description**

Returns the approximate top elements of `expression`, based on the sum of an
assigned `weight`. The `number` parameter specifies the number of elements
returned.

If the `weight` input is negative or `NaN`, this function returns an error.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`weight` must be one of the following:

<ul>
<li>INT64</li>

<li>UINT64</li>

<li>DOUBLE</li>
</ul>

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields: `value` and `sum`.
The `value` field contains the value of the input expression. The `sum` field is
the same type as `weight`, and is the approximate sum of the input weight
associated with the `value` field.

Returns `NULL` if there are zero input rows.

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

+--------------------------+
| approx_top_sum           |
+--------------------------+
| [{pear, 6}, {banana, 5}] |
+--------------------------+
```

**NULL handling**

APPROX_TOP_SUM does not ignore NULL values for the `expression` and `weight`
parameters.

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, NULL AS weight), ("pear", 0), ("pear", NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{pear, 0}, {apple, NULL}] |
+----------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, 2)]);

+-------------------------+
| approx_top_sum          |
+-------------------------+
| [{NULL, 2}, {apple, 0}] |
+-------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{apple, 0}, {NULL, NULL}] |
+----------------------------+
```

[link-to-approximate-aggregation]: https://github.com/google/zetasql/blob/master/docs/approximate-aggregation

[link-to-approximate-aggregation]: #approximate-aggregation

## HyperLogLog++ Functions

ZetaSQL supports the following approximate aggregate functions using
the HyperLogLog++ algorithm. For an explanation of how approximate aggregate
functions work, see [Approximate Aggregation][approximate-aggregation-concept].

#### HLL_COUNT.INIT
```
HLL_COUNT.INIT(input [, precision])
```

**Description**

An aggregate function that takes one or more `input` values and aggregates them
into a [HyperLogLog++][hll-link-to-hyperloglog-wikipedia] sketch. Each sketch
is represented using the `BYTES` data type. You can then merge sketches using
`HLL_COUNT.MERGE` or `HLL_COUNT.MERGE_PARTIAL`. If no merging is needed,
you can extract the final count of distinct values from the sketch using
`HLL_COUNT.EXTRACT`.

This function supports an optional parameter, `precision`. This parameter
defines the accuracy of the estimate at the cost of additional memory required
to process the sketches or store them on disk. The following table shows the
allowed precision values, the maximum sketch size per group, and confidence
interval (CI) of typical precisions:

|   Precision  | Max. Sketch Size (KiB) | 65% CI | 95% CI | 99% CI |
|--------------|------------------------|--------|--------|--------|
| 10           | 1                      | ±1.63% | ±3.25% | ±6.50% |
| 11           | 2                      | ±1.15% | ±2.30% | ±4.60% |
| 12           | 4                      | ±0.81% | ±1.63% | ±3.25% |
| 13           | 8                      | ±0.57% | ±1.15% | ±1.72% |
| 14           | 16                     | ±0.41% | ±0.81% | ±1.22% |
| 15 (default) | 32                     | ±0.29% | ±0.57% | ±0.86% |
| 16           | 64                     | ±0.20% | ±0.41% | ±0.61% |
| 17           | 128                    | ±0.14% | ±0.29% | ±0.43% |
| 18           | 256                    | ±0.10% | ±0.20% | ±0.41% |
| 19           | 512                    | ±0.07% | ±0.14% | ±0.29% |
| 20           | 1024                   | ±0.05% | ±0.10% | ±0.20% |
| 21           | 2048                   | ±0.04% | ±0.07% | ±0.14% |
| 22           | 4096                   | ±0.03% | ±0.05% | ±0.10% |
| 23           | 8192                   | ±0.02% | ±0.04% | ±0.07% |
| 24           | 16384                  | ±0.01% | ±0.03% | ±0.05% |

If the input is NULL, this function returns NULL.

For more information, see
[HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm][hll-link-to-research-whitepaper].

**Supported input types**

INT64, UINT64, NUMERIC, STRING, BYTES

**Return type**

BYTES

**Example**

```sql
SELECT
  HLL_COUNT.INIT(respondent) AS respondents_hll,
  flavor,
  country
FROM UNNEST([
  STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
  (1, "Chocolate", "CH"),
  (2, "Chocolate", "US"),
  (2, "Strawberry", "US")])
GROUP BY flavor, country;
```

#### HLL_COUNT.MERGE
```
HLL_COUNT.MERGE(sketch)
```

**Description**

An aggregate function that returns the cardinality of several
[HyperLogLog++][hll-link-to-research-whitepaper] set sketches by computing their union.

Each `sketch` must have the same precision and be initialized on the same type.
Attempts to merge sketches with different precisions or for different types
results in an error. For example, you cannot merge a sketch initialized
from INT64 data with one initialized from STRING data.

This function ignores NULL values when merging sketches. If the merge happens
over zero rows or only over NULL values, the function returns `0`.

**Supported input types**

BYTES

**Return type**

INT64

**Example**

```sql
SELECT HLL_COUNT.MERGE(respondents_hll) AS num_respondents, flavor
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country)
GROUP BY flavor;
```

#### HLL_COUNT.MERGE_PARTIAL
```
HLL_COUNT.MERGE_PARTIAL(sketch)
```

**Description**

An aggregate function that takes one or more
[HyperLogLog++][hll-link-to-research-whitepaper] `sketch`
inputs and merges them into a new sketch.

This function returns NULL if there is no input or all inputs are NULL.

**Supported input types**

BYTES

**Return type**

BYTES

**Example**

```sql
SELECT HLL_COUNT.MERGE_PARTIAL(respondents_hll) AS num_respondents, flavor
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country)
GROUP BY flavor;
```

#### HLL_COUNT.EXTRACT
```
HLL_COUNT.EXTRACT(sketch)
```

**Description**

A scalar function that extracts an cardinality estimate of a single
[HyperLogLog++][hll-link-to-research-whitepaper] sketch.

If `sketch` is NULL, this function returns a cardinality estimate of `0`.

**Supported input types**

BYTES

**Return type**

INT64

**Example**

```sql
SELECT
  flavor,
  country,
  HLL_COUNT.EXTRACT(respondents_hll) AS num_respondents
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country);

+------------+---------+-----------------+
| flavor     | country | num_respondents |
+------------+---------+-----------------+
| Vanilla    | CH      | 1               |
| Chocolate  | CH      | 1               |
| Chocolate  | US      | 1               |
| Strawberry | US      | 1               |
+------------+---------+-----------------+
```

[hll-link-to-hyperloglog-wikipedia]: https://en.wikipedia.org/wiki/HyperLogLog
[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html
[hll-link-to-approx-count-distinct]: #approx_count_distinct
[approximate-aggregation-concept]: https://github.com/google/zetasql/blob/master/docs/approximate-aggregation

[approximate-aggregation-concept]: #approximate-aggregation

## KLL16 Quantile Functions

ZetaSQL supports the following functions for estimating quantiles
with approximate aggregate quantile sketches using the [KLL16 algorithm][link-to-kll-paper].
For an explanation of how approximate aggregate functions work, see
[Approximate Aggregation][approximate-aggregation-concept].

Quantiles can be defined in two ways. First, for a positive integer *q*,
*q-quantiles* are a set of values that partition an input set into *q* subsets
of nearly equal size; that is, there are *q*-1 of the *q*-quantiles. Some of
these have specific names: the single 2-quantile is the median; the 4-quantiles
are quartiles, the 100-quantiles are percentiles, etc.

To extract a set of *q*-quantiles, use the following functions, where *q* is the
`number` argument:

+ `KLL_QUANTILES.MERGE_INT64`
+ `KLL_QUANTILES.MERGE_UINT64`
+ `KLL_QUANTILES.MERGE_DOUBLE`
+ `KLL_QUANTILES.EXTRACT_INT64`
+ `KLL_QUANTILES.EXTRACT_UINT64`
+ `KLL_QUANTILES.EXTRACT_DOUBLE`

Alternatively, quantiles can be defined as individual *Φ-quantiles*, where Φ is
a real number with 0 <= Φ <= 1. The Φ-quantile *x* is an element of the input
such that a Φ fraction of the input is less than or equal to *x*, and a (1-Φ)
fraction is greater than or equal to *x*. In this notation, the median is the
0.5-quantile, and the 95th percentile is the 0.95-quantile.

To extract individual Φ-quantiles, use the following functions, where Φ is the
`phi` argument:

+ `KLL_QUANTILES.MERGE_POINT_INT64`
+ `KLL_QUANTILES.MERGE_POINT_UINT64`
+ `KLL_QUANTILES.MERGE_POINT_DOUBLE`
+ `KLL_QUANTILES.EXTRACT_POINT_INT64`
+ `KLL_QUANTILES.EXTRACT_POINT_UINT64`
+ `KLL_QUANTILES.EXTRACT_POINT_DOUBLE`

#### KLL_QUANTILES.INIT_INT64

```
KLL_QUANTILES.INIT_INT64(input[, precision])
```

**Description**

Takes one or more `input` values and aggregates them into a
[KLL16][link-to-kll-paper] sketch. This function represents the output sketch
using the `BYTES` data type. This is an
aggregate function.

The `precision` argument defines the exactness of the returned approximate
quantile *q*. By default, the rank of the approximate quantile in the input can
be at most ±1/1000 * *n* off from ⌈Φ * *n*⌉, where *n* is the number of rows in
the input and ⌈Φ * *n*⌉ is the rank of the exact quantile. If you provide a
value for `precision`, the rank of the approximate quantile in the input can be
at most ±1/`precision` * *n* off from the rank of the exact quantile. The error
is within this error bound in 99.999% of cases.

Note: This error guarantee only applies to the difference between exact and
approximate ranks: the numerical difference between the exact and approximated
value for a quantile can be arbitrarily large.

**Example**

```
SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
FROM (SELECT 1 AS x UNION ALL
      SELECT 2 AS x UNION ALL
      SELECT 3 AS x UNION ALL
      SELECT 4 AS x UNION ALL
      SELECT 5 AS x);

+----------------------------------------------------------------------+
| kll_sketch                                                           |
+----------------------------------------------------------------------+
| "\010q\020\005 \004\212\007\025\010\200                              |
| \020\350\007\032\001\001\"\001\005*\007\n\005\001\002\003\004\005"   |
+----------------------------------------------------------------------+
```

The query above takes a column of type `INT64` and
outputs a sketch as `BYTES`
that allows you to retrieve values whose ranks are within
±1/1000 * 5 = ±1/200 ≈ 0 ranks of their exact quantile.

**Supported Argument Types**

+ `input`: `INT64`
+ `precision`: `INT64`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`BYTES`

#### KLL_QUANTILES.INIT_UINT64

```
KLL_QUANTILES.INIT_UINT64(input[, precision])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64), but accepts
`input` of type `UINT64`.

**Supported Argument Types**

+ `input`: `UINT64`
+ `precision`: `INT64`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`BYTES`

#### KLL_QUANTILES.INIT_DOUBLE

```
KLL_QUANTILES.INIT_DOUBLE(input[, precision])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64), but accepts
`input` of type `DOUBLE`.

`KLL_QUANTILES.INIT_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ `input`: `DOUBLE`
+ `precision`: `INT64`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`BYTES`

#### KLL_QUANTILES.MERGE_PARTIAL

```
KLL_QUANTILES.MERGE_PARTIAL(sketch)
```

**Description**

Takes KLL16 sketches of the same underlying type and merges them to return a new
sketch of the same underlying type. This is an aggregate function.

Returns an error if two or more sketches don't have compatible underlying types,
such as one sketch of `INT64` values and another of
`DOUBLE` values.

Returns an error if two or more sketches have different precisions.

Returns an error if one or more inputs are not a valid KLL16 quantiles sketch.

Ignores `NULL` sketches. If the input contains zero rows or only `NULL`
sketches, the function returns `NULL`.

You can initialize sketches with different optional clauses and merge them. For
example, you can initialize a sketch with the `DISTINCT` clause and another
sketch without any optional clauses, and then merge these two sketches.
However, if you initialize sketches with the `DISTINCT` clause and merge them,
the resulting sketch may still contain duplicates.

**Example**

```
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

+-----------------------------------------------------------------------------+
| merged_sketch                                                               |
+-----------------------------------------------------------------------------+
| "\010q\020\n \004\212\007\032\010\200 \020\350\007\032\001\001\"\001\n*     |
| \014\n\n\001\002\003\004\005\006\007\010\t\n"                               |
+-----------------------------------------------------------------------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches into a new sketch, also as
`BYTES`. Both input sketches have the same underlying
data type and precision.

**Supported Argument Types**

`BYTES`

**Return Types**

`BYTES`

#### KLL_QUANTILES.MERGE_INT64

```
KLL_QUANTILES.MERGE_INT64(sketch, number)
```

**Description**

Takes KLL16 sketches as `BYTES` and merges them into
a new sketch, then
returns the quantiles that divide the input into `number` equal-sized
groups, along with the minimum and maximum values of the input. The output is
an `ARRAY` containing the exact minimum value from
the input data that you used
to initialize the sketches, each approximate quantile, and the exact maximum
value from the initial input data. This is an aggregate function.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if two or more sketches have different precisions.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```
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

+---------------+
| merged_sketch |
+---------------+
| [1,5,10]      |
+---------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches and returns an `ARRAY`
containing the minimum,
median, and maximum values in the input sketches.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `INT64`.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`ARRAY` of type INT64.

#### KLL_QUANTILES.MERGE_UINT64

```
KLL_QUANTILES.MERGE_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64), but accepts
`input` of type `UINT64`.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `UINT64`.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`ARRAY` of type `UINT64`.

#### KLL_QUANTILES.MERGE_DOUBLE

```
KLL_QUANTILES.MERGE_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64), but accepts
`input` of type `DOUBLE`.

`KLL_QUANTILES.MERGE_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `DOUBLE`.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`ARRAY` of type `DOUBLE`.

#### KLL_QUANTILES.MERGE_POINT_INT64

```
KLL_QUANTILES.MERGE_POINT_INT64(sketch, phi)
```

**Description**

Takes KLL16 sketches as `BYTES` and merges them, then
extracts a single
quantile from the merged sketch. The `phi` argument specifies the quantile
to return as a fraction of the total number of rows in the input, normalized
between 0 and 1. This means that the function will return a value *v* such that
approximately Φ * *n* inputs are less than or equal to *v*, and a (1-Φ) / *n*
inputs are greater than or equal to *v*. This is an aggregate function.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

Returns an error if two or more sketches have different precisions.

**Example**

```
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

+---------------+
| merged_sketch |
+---------------+
|             9 |
+---------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches and returns the value of the ninth decile or 90th
percentile of the merged sketch.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `INT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`INT64`

#### KLL_QUANTILES.MERGE_POINT_UINT64

```
KLL_QUANTILES.MERGE_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64), but
accepts `input` of type `UINT64`.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `UINT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`UINT64`

#### KLL_QUANTILES.MERGE_POINT_DOUBLE

```
KLL_QUANTILES.MERGE_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64), but
accepts `input` of type `DOUBLE`.

`KLL_QUANTILES.MERGE_POINT_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `DOUBLE`.
+ `phi` is a `DOUBLE` between 0 and 1.

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

[analytic-functions]: #analytic-functions-concepts
[floating-point-semantics]: #floating-point-semantics

**Return Types**

`DOUBLE`

#### KLL_QUANTILES.EXTRACT_INT64
```
KLL_QUANTILES.EXTRACT_INT64(sketch, number)
```

**Description**

Takes a single KLL16 sketch as `BYTES` and returns a
selected `number`
of quantiles. The output is an `ARRAY` containing the
exact minimum value from
the input data that you used to initialize the sketch, each approximate
quantile, and the exact maximum value from the initial input data. This is a
scalar function, similar to `KLL_QUANTILES.MERGE_INT64`, but scalar rather than
aggregate.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

+---------+
| median  |
+---------+
| [1,3,5] |
+---------+
```

The query above initializes a KLL16 sketch from five rows of data. Then
it returns an `ARRAY` containing the minimum, median,
and maximum values in the input sketch.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `INT64`.

**Return Types**

`ARRAY` of type `INT64`.

#### KLL_QUANTILES.EXTRACT_UINT64
```
KLL_QUANTILES.EXTRACT_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64), but accepts
sketches initialized on data of type of type `UINT64`.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `UINT64`.

**Return Types**

`ARRAY` of type `UINT64`.

#### KLL_QUANTILES.EXTRACT_DOUBLE
```
KLL_QUANTILES.EXTRACT_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64), but accepts
sketches initialized on data of type of type `DOUBLE`.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `DOUBLE`.

**Return Types**

`ARRAY` of type `DOUBLE`.

#### KLL_QUANTILES.EXTRACT_POINT_INT64
```
KLL_QUANTILES.EXTRACT_POINT_INT64(sketch, phi)
```

**Description**

Takes a single KLL16 sketch as `BYTES` and returns a
single quantile.
The `phi` argument specifies the quantile to return as a fraction of the total
number of rows in the input, normalized between 0 and 1. This means that the
function will return a value *v* such that approximately Φ * *n* inputs are less
than or equal to *v*, and a (1-Φ) / *n* inputs are greater than or equal to *v*.
This is a scalar function.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```
SELECT KLL_QUANTILES.EXTRACT_POINT_INT64(kll_sketch, .8) AS quintile
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

+----------+
| quintile |
+----------+
|      4   |
+----------+
```

The query above initializes a KLL16 sketch from five rows of data. Then
it returns the value of the eighth decile or 80th percentile of the sketch.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `INT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`INT64`

#### KLL_QUANTILES.EXTRACT_POINT_UINT64
```
KLL_QUANTILES.EXTRACT_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts sketches initialized on data of type of type
`UINT64`.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `UINT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`UINT64`

#### KLL_QUANTILES.EXTRACT_POINT_DOUBLE
```
KLL_QUANTILES.EXTRACT_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts sketches initialized on data of type of type
`DOUBLE`.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `DOUBLE`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`DOUBLE`

[link-to-kll-paper]: https://arxiv.org/pdf/1603.05346v2.pdf
[approximate-aggregation-concept]: https://github.com/google/zetasql/blob/master/docs/approximate-aggregation#storing-estimated-aggregate-values-as-sketches
[sort-order]: https://github.com/google/zetasql/blob/master/docs/data-types#comparison_operator_examples

[approximate-aggregation-concept]: #storing-estimated-aggregate-values-as-sketches
[sort-order]: #comparison-operator-examples

## Numbering Functions

The following sections describe the numbering functions that ZetaSQL
supports. Numbering functions are a subset of analytic functions. For an
explanation of how analytic functions work, see
[Analytic Function Concepts][analytic-function-concepts]. For a
description of how numbering functions work, see the
[Numbering Function Concepts][numbering-function-concepts].

`OVER` clause requirements:

+ `PARTITION BY`: Optional.
+ `ORDER BY`: Required, except for `ROW_NUMBER()`.
+ `window_frame_clause`: Disallowed.

#### RANK

**Description**

Returns the ordinal (1-based) rank of each row within the ordered partition.
All peer rows receive the same rank value. The next row or set of peer rows
receives a rank value which increments by the number of peers with the previous
rank value, instead of `DENSE_RANK`, which always increments by 1.

**Supported Argument Types**

INT64

#### DENSE_RANK

**Description**

Returns the ordinal (1-based) rank of each row within the window partition.
All peer rows receive the same rank value, and the subsequent rank value is
incremented by one.

**Supported Argument Types**

INT64

#### PERCENT_RANK

**Description**

Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
the <code>RANK</code> of the row and NR is the number of rows in the partition.
Returns 0 if NR=1.

**Supported Argument Types**

DOUBLE

#### CUME_DIST

**Description**

Return the relative rank of a row defined as NP/NR. NP is defined to be the
number of rows that either precede or are peers with the current row. NR is the
number of rows in the partition.

**Supported Argument Types**

DOUBLE

#### NTILE

```
NTILE(constant_integer_expression)
```

**Description**

This function divides the rows into <code>constant_integer_expression</code>
buckets based on row ordering and returns the 1-based bucket number that is
assigned to each row. The number of rows in the buckets can differ by at most 1.
The remainder values (the remainder of number of rows divided by buckets) are
distributed one for each bucket, starting with bucket 1. If
<code>constant_integer_expression</code> evaluates to NULL, 0 or negative, an
error is provided.

**Supported Argument Types**

INT64

#### ROW_NUMBER

**Description**

Does not require the <code>ORDER BY</code> clause. Returns the sequential
row ordinal (1-based) of each row for each ordered partition. If the
<code>ORDER BY</code> clause is unspecified then the result is
non-deterministic.

**Supported Argument Types**

INT64

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[numbering-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#numbering_function_concepts

[analytic-function-concepts]: #analytic-function-concepts
[numbering-function-concepts]: #numbering-function-concepts

## Bit Functions

ZetaSQL supports the following bit functions.

#### BIT_COUNT
```
BIT_COUNT(expression)
```

**Description**

The input, `expression`, must be an
integer or BYTES.

Returns the number of bits that are set in the input `expression`.
For signed integers, this is the number of bits in two's complement form.

**Return Data Type**

INT64

**Example**

```sql
SELECT a, BIT_COUNT(a) AS a_bits, FORMAT("%T", b) as b, BIT_COUNT(b) AS b_bits
FROM UNNEST([
  STRUCT(0 AS a, b'' AS b), (0, b'\x00'), (5, b'\x05'), (8, b'\x00\x08'),
  (0xFFFF, b'\xFF\xFF'), (-2, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE'),
  (-1, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
  (NULL, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
]) AS x;

+-------+--------+---------------------------------------------+--------+
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
+-------+--------+---------------------------------------------+--------+
```

## Mathematical functions

All mathematical functions have the following behaviors:

+  They return `NULL` if any of the input parameters is `NULL`.
+  They return `NaN` if any of the arguments is `NaN`.

#### ABS

```
ABS(X)
```

**Description**

Computes absolute value. Returns an error if the argument is an integer and the
output value cannot be represented as the same type; this happens only for the
largest negative input value, which has no positive representation. Returns
`+inf` for a `+/-inf` argument.

#### SIGN

```
SIGN(X)
```

**Description**

Returns -1, 0, or +1 for negative, zero and positive arguments respectively.
For floating point arguments, this function does not distinguish between
positive and negative zero. Returns `NaN` for a `NaN` argument.

#### IS_INF

```
IS_INF(X)
```

**Description**

Returns `TRUE` if the value is positive or negative infinity. Returns
`NULL` for `NULL` inputs.

#### IS_NAN

```
IS_NAN(X)
```

**Description**

Returns `TRUE` if the value is a `NaN` value. Returns `NULL` for `NULL` inputs.

#### IEEE_DIVIDE

```
IEEE_DIVIDE(X, Y)
```

**Description**

Divides X by Y; this function never fails. Returns
`DOUBLE` unless
both X and Y are `FLOAT`, in which case it returns
FLOAT. Unlike the division operator (/),
this function does not generate errors for division by zero or overflow.</p>

Special cases:

+ If the result overflows, returns `+/-inf`.
+ If Y=0 and X=0, returns `NaN`.
+ If Y=0 and X!=0, returns `+/-inf`.
+ If X = `+/-inf` and Y = `+/-inf`, returns `NaN`.

The behavior of `IEEE_DIVIDE` is further illustrated in the table below.

##### Special cases for `IEEE_DIVIDE`

The following table lists special cases for `IEEE_DIVIDE`.

<table>
<thead>
<tr>
<th>Numerator Data Type (X)</th>
<th>Denominator Data Type (Y)</th>
<th>Result Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>Anything except 0</td>
<td>0</td>
<td><code>+/-inf</code></td>
</tr>
<tr>
<td>0</td>
<td>0</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td>0</td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td>0</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

#### RAND

```
RAND()
```

**Description**

Generates a pseudo-random value of type DOUBLE in the
range of [0, 1), inclusive of 0 and exclusive of 1.

#### SQRT

```
SQRT(X)
```

**Description**

Computes the square root of X. Generates an error if X is less than 0. Returns
`+inf` if X is `+inf`.

#### POW

```
POW(X, Y)
```

**Description**

Returns the value of X raised to the power of Y. If the result underflows and is
not representable, then the function returns a  value of zero. Returns an error
if one of the following is true:

+ X is a finite value less than 0 and Y is a noninteger
+ X is 0 and Y is a finite value less than 0

The behavior of `POW()` is further illustrated in the table below.

#### POWER

```
POWER(X, Y)
```

**Description**

Synonym of `POW()`.

##### Special cases for `POW(X, Y)` and `POWER(X, Y)`

The following are special cases for `POW(X, Y)` and `POWER(X, Y)`.

<table>
<thead>
<tr>
<th>X</th>
<th>Y</th>
<th>POW(X, Y) or POWER(X, Y)</th>
</tr>
</thead>
<tbody>
<tr>
<td>1.0</td>
<td>Any value including <code>NaN</code></td>
<td>1.0</td>
</tr>
<tr>
<td>any including <code>NaN</code></td>
<td>0</td>
<td>1.0</td>
</tr>
<tr>
<td>-1.0</td>
<td><code>+/-inf</code></td>
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
<td>0</td>
</tr>
<tr>
<td>ABS(X) &lt; 1</td>
<td><code>+inf</code></td>
<td>0</td>
</tr>
<tr>
<td>ABS(X) &gt; 1</td>
<td><code>+inf</code></td>
<td><code>+inf</code></td>
</tr>
<tr>
<td><code>-inf</code></td>
<td>Y &lt; 0</td>
<td>0</td>
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
</tbody>
</table>

#### EXP

```
EXP(X)
```

**Description**

Computes *e* to the power of X, also called the natural exponential function. If
the result underflows, this function returns a zero. Generates an error if the
result overflows. If X is `+/-inf`, then this function returns `+inf` or 0.

#### LN

```
LN(X)
```

**Description**

Computes the natural logarithm of X. Generates an error if X is less than or
equal to zero. If X is `+inf`, then this function returns `+inf`.

#### LOG

```
LOG(X [, Y])
```

**Description**

If only X is present, `LOG` is a synonym of `LN`. If Y is also present, `LOG`
computes the logarithm of X to base Y. Generates an error in these cases:

+ X is less than or equal to zero
+ Y is 1.0
+ Y is less than or equal to zero.

The behavior of `LOG(X, Y)` is further illustrated in the table below.

<a name="special_log"></a>

##### Special cases for `LOG(X, Y)`

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
<td>0.0 Y &lt; 1.0</td>
<td><code>-inf</code></td>
</tr>
<tr>
<td><code>+inf</code></td>
<td>Y &gt; 1.0</td>
<td><code>+inf</code></td>
</tr>
</tbody>
</table>

#### LOG10

```
LOG10(X)
```

**Description**

Similar to `LOG`, but computes logarithm to base 10.

#### GREATEST

```
GREATEST(X1,...,XN)
```

**Description**

Returns <code>NULL</code> if any of the inputs is <code>NULL</code>. Otherwise, returns <code>NaN</code> if any of
the inputs is <code>NaN</code>. Otherwise, returns the largest value among X1,...,XN
according to the &lt; comparison.

#### LEAST

```
LEAST(X1,...,XN)
```

**Description**

Returns `NULL` if any of the inputs is `NULL`. Returns `NaN` if any of the
inputs is `NaN`. Otherwise, returns the smallest value among X1,...,XN
according to the &gt; comparison.

#### DIV

```
DIV(X, Y)
```

**Description**

Returns the result of integer division of X by Y. Division by zero returns
an error. Division by -1 may overflow. See the table below for possible result
types.

#### SAFE_DIVIDE

```
SAFE_DIVIDE(X, Y)
```

**Description**

Equivalent to the division operator (<code>/</code>), but returns
<code>NULL</code> if an error occurs, such as a division by zero error.

#### SAFE_MULTIPLY

```
SAFE_MULTIPLY(X, Y)
```

**Description**

Equivalent to the multiplication operator (<code>*</code>), but returns
<code>NULL</code> if overflow occurs.

#### SAFE_NEGATE

```
SAFE_NEGATE(X)
```

**Description**

Equivalent to the unary minus operator (<code>-</code>), but returns
<code>NULL</code> if overflow occurs.

#### SAFE_ADD

```
SAFE_ADD(X, Y)
```

**Description**

Equivalent to the addition operator (<code>+</code>), but returns
<code>NULL</code> if overflow occurs.

#### SAFE_SUBTRACT

```
SAFE_SUBTRACT(X, Y)
```

**Description**

Equivalent to the subtraction operator (<code>-</code>), but returns
<code>NULL</code> if overflow occurs.

#### MOD

```
MOD(X, Y)
```

**Description**

Modulo function: returns the remainder of the division of X by Y. Returned
value has the same sign as X. An error is generated if Y is 0. See the table
below for possible result types.

<a name="result_div_mod"></a>

##### Result types for `DIV(X, Y)` and `MOD(X, Y)`

<table>
<thead>
<tr><th>&nbsp;</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th></tr>
</thead>
<tbody><tr><td>INT32</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td></tr><tr><td>INT64</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td></tr><tr><td>UINT32</td><td>INT64</td><td>INT64</td><td>UINT64</td><td>UINT64</td></tr><tr><td>UINT64</td><td>ERROR</td><td>ERROR</td><td>UINT64</td><td>UINT64</td></tr></tbody>
</table>

<a name="rounding_functions"></a>

#### ROUND

```
ROUND(X [, N])
```

**Description**

If only X is present, `ROUND` rounds X to the nearest integer. If N is present,
`ROUND` rounds X to N decimal places after the decimal point. If N is negative,
`ROUND` will round off digits to the left of the decimal point. Rounds halfway
cases away from zero. Generates an error if overflow occurs.

#### TRUNC

```
TRUNC(X [, N])
```

**Description**

If only X is present, `TRUNC` rounds X to the nearest integer whose absolute
value is not greater than the absolute value of X. If N is also present, `TRUNC`
behaves like `ROUND(X, N)`, but always rounds towards zero and never overflows.

#### CEIL

```
CEIL(X)
```

**Description**

Returns the smallest integral value (with DOUBLE
type) that is not less than X.

#### CEILING

```
CEILING(X)
```

**Description**

Synonym of CEIL(X)

#### FLOOR

```
FLOOR(X)
```

**Description**

Returns the largest integral value (with DOUBLE
type) that is not greater than X.

##### Example rounding function behavior
Example behavior of ZetaSQL rounding functions:

<table>
<thead>
<tr>
<th>Input "X"</th>
<th>ROUND(X)</th>
<th>TRUNC(X)</th>
<th>CEIL(X)</th>
<th>FLOOR(X)</th>
</tr>
</thead>
<tbody>
<tr>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.3</td>
<td>2.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.8</td>
<td>3.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.5</td>
<td>3.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>-2.3</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.8</td>
<td>-3.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.5</td>
<td>-3.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>0</td>
<td>0</td>
<td>0</td>
<td>0</td>
<td>0</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

<a name="trigonometric_and_hyperbolic_functions"></a>

#### COS

```
COS(X)
```

**Description**

Computes the cosine of X where X is specified in radians. Never fails.

#### COSH

```
COSH(X)
```

**Description**

Computes the hyperbolic cosine of X where X is specified in radians.
Generates an error if overflow occurs.

#### ACOS

```
ACOS(X)
```

**Description**

Computes the principal value of the inverse cosine of X. The return value is in
the range [0,&pi;]. Generates an error if X is a value outside of the
range [-1, 1].

#### ACOSH

```
ACOSH(X)
```

**Description**

Computes the inverse hyperbolic cosine of X. Generates an error if X is a value
less than 1.

#### SIN

```
SIN(X)
```

**Description**

Computes the sine of X where X is specified in radians. Never fails.

#### SINH

```
SINH(X)
```

**Description**

Computes the hyperbolic sine of X where X is specified in radians. Generates
an error if overflow occurs.

#### ASIN

```
ASIN(X)
```

**Description**

Computes the principal value of the inverse sine of X. The return value is in
the range [-&pi;/2,&pi;/2]. Generates an error if X is outside of
the range [-1, 1].

#### ASINH

```
ASINH(X)
```

**Description**

Computes the inverse hyperbolic sine of X. Does not fail.

#### TAN

```
TAN(X)
```

**Description**

Computes the tangent of X where X is specified in radians. Generates an error if
overflow occurs.

#### TANH

```
TANH(X)
```

**Description**

Computes the hyperbolic tangent of X where X is specified in radians. Does not
fail.

#### ATAN

```
ATAN(X)
```

**Description**

Computes the principal value of the inverse tangent of X. The return value is
in the range [-&pi;/2,&pi;/2]. Does not fail.

#### ATANH

```
ATANH(X)
```

**Description**

Computes the inverse hyperbolic tangent of X. Generates an error if X is outside
of the range [-1, 1].

#### ATAN2

```
ATAN2(Y, X)
```

**Description**

Calculates the principal value of the inverse tangent of Y/X using the signs of
the two arguments to determine the quadrant. The return value is in the range
[-&pi;,&pi;]. The behavior of this function is further illustrated in
<a href="#special_atan2">the table below</a>.

<a name="special_atan2"></a>
##### Special cases for `ATAN2()`

<table>
<thead>
<tr>
<th>Y</th>
<th>X</th>
<th>ATAN2(Y, X)</th>
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
<td>0</td>
<td>0</td>
<td>0, &pi; or -&pi; depending on the sign of X and Y</td>
</tr>
<tr>
<td>Finite value</td>
<td><code>-inf</code></td>
<td>&pi; or -&pi; depending on the sign of Y</td>
</tr>
<tr>
<td>Finite value</td>
<td><code>+inf</code></td>
<td>0</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td>Finite value</td>
<td>&pi;/2 or &pi;/2 depending on the sign of Y</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>-inf</code></td>
<td>&frac34;&pi; or -&frac34;&pi; depending on the sign of Y</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+inf</code></td>
<td>&pi;/4 or -&pi;/4 depending on the sign of Y</td>
</tr>
</tbody>
</table>

<a name="special_trig_hyperbolic"></a>
##### Special cases for trigonometric and hyperbolic rounding functions

<table>
<thead>
<tr>
<th>X</th>
<th>COS(X)</th>
<th>COSH(X)</th>
<th>ACOS(X)</th>
<th>ACOSH(X)</th>
<th>SIN(X)</th>
<th>SINH(X)</th>
<th>ASIN(X)</th>
<th>ASINH(X)</th>
<th>TAN(X)</th>
<th>TANH(X)</th>
<th>ATAN(X)</th>
<th>ATANH(X)</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>+/-inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td>=+1.0</td>
<td>&pi;/2</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td>-1.0</td>
<td>-&pi;/2</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types#data_type_properties

[data-type-properties]: #data-type-properties

## Navigation Functions

The following sections describe the navigation functions that ZetaSQL
supports. Navigation functions are a subset of analytic functions. For an
explanation of how analytic functions work, see [Analytic
Function Concepts][analytic-function-concepts]. For an explanation of how
navigation functions work, see
[Navigation Function Concepts][navigation-function-concepts].

#### FIRST_VALUE

```
FIRST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of the `value_expression` for the first row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

ANY

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

+-----------------+-------------+----------+--------------+------------------+
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
+-----------------+-------------+----------+--------------+------------------+
```

#### LAST_VALUE

```
LAST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of the `value_expression` for the last row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

ANY

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

+-----------------+-------------+----------+--------------+------------------+
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
+-----------------+-------------+----------+--------------+------------------+

```

#### NTH_VALUE

```
NTH_VALUE (value_expression, constant_integer_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of `value_expression` at the Nth row of the current window
frame, where Nth is defined by `constant_integer_expression`. Returns NULL if
there is no such row.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `constant_integer_expression` can be any constant expression that returns an
  integer.

**Return Data Type**

ANY

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

+-----------------+-------------+----------+--------------+----------------+
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
+-----------------+-------------+----------+--------------+----------------+
```

#### LEAD

```
LEAD (value_expression[, offset [, default_expression]])
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

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

ANY

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

+-----------------+-------------+----------+-----------------+
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
+-----------------+-------------+----------+-----------------+
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

+-----------------+-------------+----------+------------------+
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
+-----------------+-------------+----------+------------------+
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

+-----------------+-------------+----------+------------------+
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
+-----------------+-------------+----------+------------------+
```

#### LAG

```
LAG (value_expression[, offset [, default_expression]])
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

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

ANY

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

+-----------------+-------------+----------+------------------+
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
+-----------------+-------------+----------+------------------+
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

+-----------------+-------------+----------+-------------------+
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
+-----------------+-------------+----------+-------------------+
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

+-----------------+-------------+----------+-------------------+
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
+-----------------+-------------+----------+-------------------+
```

#### PERCENTILE_CONT

```
PERCENTILE_CONT (value_expression, percentile [{RESPECT | IGNORE} NULLS])
```

**Description**

Computes the specified percentile value for the value_expression, with linear
interpolation.

This function ignores NULL values if `RESPECT NULLS` is absent.  If `RESPECT
NULLS` is present:

+ Interpolation between two `NULL` values returns `NULL`.
+ Interpolation between a `NULL` value and a non-`NULL` value returns the
  non-`NULL` value.

**Supported Argument Types**

+ `value_expression` is a numeric expression.
+ `percentile` is a `DOUBLE` literal in the range `[0, 1]`.

**Return Data Type**

`DOUBLE`

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

+-----+-------------+--------+--------------+-----+
| min | percentile1 | median | percentile90 | max |
+-----+-------------+--------+--------------+-----+
| 0   | 0.03        | 1.5    | 2.7          | 3   |
+-----+-------------+--------+--------------+-----+
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```
SELECT
  PERCENTILE_CONT(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_CONT(x, 0.01 RESPECT NULLS) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_CONT(x, 0.9 RESPECT NULLS) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

+------+-------------+--------+--------------+-----+
| min  | percentile1 | median | percentile90 | max |
+------+-------------+--------+--------------+-----+
| NULL | 0           | 1      | 2.6          | 3   |
+------+-------------+--------+--------------+-----+
```

#### PERCENTILE_DISC

```
PERCENTILE_DISC (value_expression, percentile [{RESPECT | IGNORE} NULLS])
```

**Description**

Computes the specified percentile value for a discrete `value_expression`. The
returned value is the first sorted value of `value_expression` with cumulative
distribution greater than or equal to the given `percentile` value.

This function ignores `NULL` values unless `RESPECT NULLS` is present.

**Supported Argument Types**

+ `value_expression` can be any orderable type.
+ `percentile` is a `DOUBLE` literal in the range `[0, 1]`.

**Return Data Type**

`ANY`

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```
SELECT
  x,
  PERCENTILE_DISC(x, 0) OVER() AS min,
  PERCENTILE_DISC(x, 0.5) OVER() AS median,
  PERCENTILE_DISC(x, 1) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

+------+-----+--------+-----+
| x    | min | median | max |
+------+-----+--------+-----+
| c    | a   | b      | c   |
| NULL | a   | b      | c   |
| b    | a   | b      | c   |
| a    | a   | b      | c   |
+------+-----+--------+-----+
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```
SELECT
  x,
  PERCENTILE_DISC(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_DISC(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_DISC(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

+------+------+--------+-----+
| x    | min  | median | max |
+------+------+--------+-----+
| c    | NULL | a      | c   |
| NULL | NULL | a      | c   |
| b    | NULL | a      | c   |
| a    | NULL | a      | c   |
+------+------+--------+-----+

```

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[navigation-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#navigation_function_concepts

[analytic-function-concepts]: #analytic-function-concepts
[navigation-function-concepts]: #navigation-function-concepts

## Hash functions

#### FARM_FINGERPRINT
```
FARM_FINGERPRINT(value)
```

**Description**

Computes the fingerprint of the STRING or BYTES input using the `Fingerprint64`
function from the
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
+---+-------+-------+----------------------+
| x | y     | z     | row_fingerprint      |
+---+-------+-------+----------------------+
| 1 | foo   | true  | -1541654101129638711 |
| 2 | apple | false | 2794438866806483259  |
| 3 |       | true  | -4880158226897771312 |
+---+-------+-------+----------------------+
```

#### FINGERPRINT

```
FINGERPRINT(input)
```

**Description**

Computes the fingerprint of the STRING
or BYTES input using Fingerprint.

**Return type**

UINT64

**Examples**

```sql
SELECT FINGERPRINT("Hello World") as fingerprint;

+----------------------+
| fingerprint          |
+----------------------+
| 4584092443788135411  |
+----------------------+
```

#### MD5
```
MD5(input)
```

**Description**

Computes the hash of the input using the
[MD5 algorithm][hash-link-to-md5-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 16 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT MD5("Hello World") as md5;

+-------------------------------------------------+
| md5                                             |
+-------------------------------------------------+
| \xb1\n\x8d\xb1d\xe0uA\x05\xb7\xa9\x9b\xe7.?\xe5 |
+-------------------------------------------------+
```

#### SHA1
```
SHA1(input)
```

**Description**

Computes the hash of the input using the
[SHA-1 algorithm][hash-link-to-sha-1-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 20 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA1("Hello World") as sha1;

+-----------------------------------------------------------+
| sha1                                                      |
+-----------------------------------------------------------+
| \nMU\xa8\xd7x\xe5\x02/\xabp\x19w\xc5\xd8@\xbb\xc4\x86\xd0 |
+-----------------------------------------------------------+
```

#### SHA256
```
SHA256(input)
```

**Description**

Computes the hash of the input using the
[SHA-256 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 32 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA256("Hello World") as sha256;
```

#### SHA512
```
SHA512(input)
```

**Description**

Computes the hash of the input using the
[SHA-512 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
STRING or BYTES. The string version treats the input as an array of bytes.

This function returns 64 bytes.

**Return type**

BYTES

**Example**

```sql
SELECT SHA512("Hello World") as sha512;
```

[hash-link-to-farmhash-github]: https://github.com/google/farmhash
[hash-link-to-md5-wikipedia]: https://en.wikipedia.org/wiki/MD5
[hash-link-to-sha-1-wikipedia]: https://en.wikipedia.org/wiki/SHA-1
[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

## String functions

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

#### BYTE_LENGTH

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

#### CHAR_LENGTH

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
#### CHARACTER_LENGTH
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

#### CODE_POINTS_TO_BYTES
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

#### CODE_POINTS_TO_STRING
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

#### CONCAT
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

#### ENDS_WITH
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

#### FORMAT {: #format_string }

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

##### Syntax

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

##### Supported format specifiers

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
##### %g and %G behavior
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
##### %t and %T behavior

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

##### Error conditions

If a format specifier is invalid, or is not compatible with the related
argument type, or the wrong number or arguments are provided, then an error is
produced.  For example, the following `<format_string>` expressions are invalid:

```
FORMAT('%s', 1)
```

```
FORMAT('%')
```

##### NULL argument handling

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

##### Additional semantic rules

DOUBLE and
FLOAT values can be `+/-inf` or `NaN`.  When an argument has one of
those values, the result of the format specifiers `%f`, `%F`, `%e`, `%E`, `%g`,
`%G`, and `%t` are `inf`, `-inf`, or `nan` (or the same in uppercase) as
appropriate.  This is consistent with how ZetaSQL casts these values
to STRING.  For `%T`, ZetaSQL returns quoted strings for
DOUBLE values that don't have non-string literal
representations.

#### FROM_BASE32

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

#### FROM_BASE64

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

#### FROM_HEX
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

#### LENGTH
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

#### LPAD
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

#### LOWER
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

#### LTRIM
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

#### NORMALIZE
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

#### NORMALIZE_AND_CASEFOLD
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

#### REGEXP_CONTAINS

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

## Performs a full match, using ^ and $. Due to regular expression operator
## precedence, it is good practice to use parentheses around everything between ^
## and $.
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

#### REGEXP_EXTRACT

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

#### REGEXP_EXTRACT_ALL

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

#### REGEXP_MATCH

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

#### REGEXP_REPLACE

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

#### REPLACE
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

#### REPEAT
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

#### REVERSE
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

#### RPAD
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

#### RTRIM
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

#### SAFE_CONVERT_BYTES_TO_STRING

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

#### SPLIT

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

#### STARTS_WITH
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

#### STRPOS
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

#### SUBSTR
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

#### TO_BASE32

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

#### TO_BASE64

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

#### TO_CODE_POINTS
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

#### TO_HEX
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

#### TRIM
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

#### UPPER
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

[string-link-to-operators]: #operators

[string-link-to-operators]: #operators

## JSON functions

ZetaSQL supports functions that help you retrieve data stored in
JSON-formatted strings and functions that help you transform data into
JSON-formatted strings.

#### JSON_EXTRACT or JSON_EXTRACT_SCALAR

<a id="json_extract"></a>`JSON_EXTRACT(json_string_expr,
json_path_string_literal)`, which returns JSON values as STRINGs.

<a id="json_extract_scalar"></a>`JSON_EXTRACT_SCALAR(json_string_expr,
json_path_string_literal)`, which returns scalar JSON values as STRINGs.

**Description**

Extracts JSON values or JSON scalar values as strings.

+  `json_string_expr`: A JSON-formatted string. For example:

    ```
    {"class" : {"students" : [{"name" : "Jane"}]}}
    ```
+  `json_path_string_literal`: The [JSONpath][jsonpath-format] format.
   This identifies the value or values you want to obtain from the
   JSON-formatted string. If `json_path_string_literal` returns a JSON `null`,
   this is converted into a SQL `NULL`.

In cases where a JSON key uses invalid JSONPath characters, you can escape
those characters using single quotes and brackets.

**Return type**

`STRING`s

**Examples**

```sql
SELECT JSON_EXTRACT(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------------------------------------------------+
| json_text_string                                          |
+-----------------------------------------------------------+
| {"class":{"students":[{"name":"Jane"}]}}                  |
| {"class":{"students":[]}}                                 |
| {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
+-----------------------------------------------------------+
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------+
| first_student   |
+-----------------+
| {"name":"Jane"} |
| NULL            |
| {"name":"John"} |
+-----------------+
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-------------------+
| second_student    |
+-------------------+
| NULL              |
| NULL              |
| NULL              |
| "Jamie"           |
+-------------------+
```

```sql
SELECT JSON_EXTRACT(json_text, "$.class['students']") AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+------------------------------------+
| student_names                      |
+------------------------------------+
| [{"name":"Jane"}]                  |
| []                                 |
| [{"name":"John"},{"name":"Jamie"}] |
+------------------------------------+
```

```sql
SELECT JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.name') as json_name,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.name') as scalar_name,
  JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.age') as json_age,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.age') as scalar;

+-----------+-------------+----------+--------+
| json_name | scalar_name | json_age | scalar |
+-----------+-------------+----------+--------+
| "Jakob"   | Jakob       | "6"      | 6      |
+-----------+-------------+----------+--------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c") as hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

#### JSON_QUERY or JSON_VALUE

<a id="json_query"></a>`JSON_QUERY(json_string_expr, json_path_string_literal)`,
which returns JSON values as STRINGs.

<a id="json_value"></a>`JSON_VALUE(json_string_expr, json_path_string_literal)`,
which returns scalar JSON values as STRINGs.

**Description**

Extracts JSON values or JSON scalar values as strings.

+  `json_string_expr`: A JSON-formatted string. For example:

  ```
  {"class" : {"students" : [{"name" : "Jane"}]}}
  ```
+  `json_path_string_literal`: The [JSONpath][jsonpath-format] format.
   This identifies the value or values you want to obtain from the
   JSON-formatted string. If `json_path_string_literal` returns a JSON `null`,
   this is converted into a SQL `NULL`.

In cases where a JSON key uses invalid JSONPath characters,
you can escape those characters using double quotes.

**Examples**

```sql
SELECT JSON_QUERY(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------------------------------------------------+
| json_text_string                                          |
+-----------------------------------------------------------+
| {"class":{"students":[{"name":"Jane"}]}}                  |
| {"class":{"students":[]}}                                 |
| {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
+-----------------------------------------------------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------+
| first_student   |
+-----------------+
| {"name":"Jane"} |
| NULL            |
| {"name":"John"} |
+-----------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-------------------+
| second_student    |
+-------------------+
| NULL              |
| NULL              |
| NULL              |
| "Jamie"           |
+-------------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class."students"') AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+------------------------------------+
| student_names                      |
+------------------------------------+
| [{"name":"Jane"}]                  |
| []                                 |
| [{"name":"John"},{"name":"Jamie"}] |
+------------------------------------+
```

```sql
SELECT JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.name') as json_name,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.name') as scalar_name,
  JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.age') as json_age,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.age') as scalar;

+-----------+-------------+----------+--------+
| json_name | scalar_name | json_age | scalar |
+-----------+-------------+----------+--------+
| "Jakob"   | Jakob       | "6"      | 6      |
+-----------+-------------+----------+--------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes. For example:

```sql
SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c') as hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

#### TO_JSON_STRING

```
TO_JSON_STRING(value[, pretty_print])
```

**Description**

Returns a JSON-formatted string representation of `value`. This function
supports an optional `pretty_print` parameter. If `pretty_print` is present, the
returned value is formatted for easy readability.

<table>
<thead>
<tr>
<th>Input data type</th>
<th>Returned value</th>
</tr>
</thead>
<tbody>
 <tr>
    <td>NULL of any type</td>
    <td><code>null</code></td>
 </tr>
  <tr>
    <td>BOOL</td>
    <td><code>true</code> or <code>false</code>.</td>
 </tr>

  <tr>
    <td>INT32, UINT32</td>
    <td><p>Same as <code>CAST(value AS STRING)</code>. For example:</p>
    <code>-1, 0, 12345678901</code>
    </td>
 </tr>

 <tr>
    <td>INT64, UINT64</td>
    <td><p>Same as <code>CAST(value AS STRING)</code> when <code>value</code> is
    in the range of [-2<sup>53</sup>, 2<sup>53</sup>], which is the range of integers that can be
    represented losslessly as IEEE 754 double-precision floating point numbers.
    Values outside of this range are represented as quoted strings. For example:
    </p>
    <code>-1</code><br>
    <code>0</code><br>
    <code>12345678901</code><br>
    <code>9007199254740992</code><br>
    <code>-9007199254740992</code><br>
    <code>"9007199254740993"</code><br>
    <p><code>9007199254740993</code> is greater than 2<sup>53</sup>, so it is represented
    as a quoted string.</p>
    </td>
 </tr>
 <tr>
    <td>NUMERIC</td>
   <td><p>Same as <code>CAST(value AS STRING)</code> when <code>value</code> is
     in the range of [-2<sup>53</sup>, 2<sup>53</sup>] and has no fractional
     part. Values outside of this range are represented as quoted strings. For
     example:</p>
     <code>-1</code><br/>
     <code>0</code><br/>
     <code>&quot;9007199254740993&quot;</code><br/>
     <code>&quot;123.56&quot;</code>
    </td>
 </tr>
 <tr>
    <td>FLOAT, DOUBLE</td>

    <td><code>+/-inf</code> and <code>NaN</code> are represented as
    <code>Infinity</code>, <code>-Infinity</code>, and <code>NaN</code>,
    respectively.
    <p>Otherwise, the same as <code>CAST(value AS STRING)</code>.</p>
    </td>
 </tr>
 <tr>
    <td>STRING</td>
    <td>Quoted string value, escaped according to the JSON standard.
    Specifically, <code>"</code>, <code>\</code>, and the control characters
    from <code>U+0000</code> to <code>U+001F</code> are escaped.</td>
 </tr>
 <tr>
    <td>BYTES</td>
    <td><p>Quoted RFC 4648 base64-escaped value. For example:</p>
    <p><code>"R29vZ2xl"</code> is the base64 representation of bytes
    <code>b"Google"</code></p>
    </td>
 </tr>
 
 <tr>
    <td>ENUM</td>
    <td><p>Quoted enum name as a string.</p>
    <p>Invalid enum values are represented as their number, such as 0 or 42.</p>
    </td>
 </tr>
 
 <tr>
    <td>DATE</td>
    <td><p>Quoted date. For example:</p>
    <code>"2017-03-06"</code>
    </td>
 </tr>
 <tr>
    <td>TIMESTAMP</td>
    <td><p>Quoted ISO 8601 date-time, where T separates the date and time and
    Zulu/UTC represents the time zone. For example:</p>
    <code>"2017-03-06T12:34:56.789012Z"</code>
    </td>
 </tr>
 <tr>
    <td>DATETIME</td>
    <td><p>Quoted ISO 8601 date-time, where T separates the date and time. For
    example:</p>
    <code>"2017-03-06T12:34:56.789012"</code>
    </td>
 </tr>
 <tr>
    <td>TIME</td>
    <td><p>Quoted ISO 8601 time. For example:</p>
    <code>"12:34:56.789012"</code></td>
 </tr>
 <tr>
    <td>ARRAY</td>
    <td><p><code>[elem1,elem2,...]</code>, where each <code>elem</code> is
    formatted according to the element type.</p>
    Example with formatting:
<pre>[
  elem1,
  elem2,
  ...
]</pre>
    <p>Where each elem is formatted according to the element type. The empty
    array is represented as <code>[]</code>.</p>
    </td>
 </tr>
 <tr>
    <td>STRUCT</td>
    <td><code>{"field_name1":field_value1,"field_name2":field_value2,...}</code>
    <p>Where each <code>field_value</code> is formatted according to its type.
    </p>
    Example with formatting:
<pre>{
  "field_name1": field_value1,
  "field_name2": field_value2,
  ...
}</pre>
    <p>Where each <code>field_value</code> is formatted according to its type.
    If a <code>field_value</code> is a non-empty ARRAY or STRUCT,
    elements are indented to the appropriate level. The empty struct is
    represented as <code>{}</code>.
    </p>
    <p>Fields with duplicate names might result in unparseable JSON. Anonymous
    fields are represented with <code>""</code>.
    </p>
    <p>Invalid UTF-8 field names might result in unparseable JSON. String values
    are escaped according to the JSON standard. Specifically, <code>"</code>,
    <code>\</code>, and the control characters from <code>U+0000</code> to
    <code>U+001F</code> are escaped.</p>
    </td>
 </tr>

 <tr>
    <td>PROTO</td>
    <td><code>{"fieldName1":field_value1,"fieldName2":field_value2,...}</code>
    Example with formatting:
<pre>{
  "fieldName1": field_value1,
  "fieldName2": field_value2,
  ...
}</pre>
    <p>Field names with underscores are converted to camel-case in accordance
    with
    <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
    protobuf json conversion</a>. Field values are formatted according to
    <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
    protobuf json conversion</a>. If a <code>field_value</code> is a non-empty
    repeated field or submessage, elements/fields are indented to the
    appropriate level. The empty struct is represented as <code>{}</code>.</p>
    <ul>
    <li>Field names that are not valid UTF-8 might result in unparseable JSON.
    </li>
    <li>Field annotations are not taken into account.</li>
    <li>Repeated fields are represented as arrays.</li>
    <li>Submessages are formatted as values of PROTO type.</li>
    <li>Extensions fields are included in the output, where the extension field
    name is enclosed in brackets and prefixed with the full name of the
    extension type.
    </ul>
    </td>
 </tr>

</tbody>
</table>

**Return type**

JSON string representation of the value.

**Examples**

Convert rows in a table to JSON.

```sql
WITH Input AS (
  SELECT [1, 2] AS x, 'foo' AS y, STRUCT(true AS a, DATE '2017-04-05' AS b) AS s UNION ALL
  SELECT NULL AS x, '' AS y, STRUCT(false AS a, DATE '0001-01-01' AS b) AS s UNION ALL
  SELECT [3] AS x, 'bar' AS y, STRUCT(NULL AS a, DATE '2016-12-05' AS b) AS s
)
SELECT
  t,
  TO_JSON_STRING(t) AS json_row
FROM Input AS t;
```

The above query produces the following result:

```json
+-----------------------------------+-------------------------------------------------------+
| t                                 | json_row                                              |
+-----------------------------------+-------------------------------------------------------+
| {[1, 2], foo, {true, 2017-04-05}} | {"x":[1,2],"y":"foo","s":{"a":true,"b":"2017-04-05"}} |
| {NULL, , {false, 0001-01-01}}     | {"x":null,"y":"","s":{"a":false,"b":"0001-01-01"}}    |
| {[3], bar, {NULL, 2016-12-05}}    | {"x":[3],"y":"bar","s":{"a":null,"b":"2016-12-05"}}   |
+-----------------------------------+-------------------------------------------------------+
```

Convert rows in a table to JSON with formatting.

```sql
WITH Input AS (
  SELECT [1, 2] AS x, 'foo' AS y, STRUCT(true AS a, DATE '2017-04-05' AS b) AS s UNION ALL
  SELECT NULL AS x, '' AS y, STRUCT(false AS a, DATE '0001-01-01' AS b) AS s UNION ALL
  SELECT [3] AS x, 'bar' AS y, STRUCT(NULL AS a, DATE '2016-12-05' AS b) AS s
)
SELECT
  TO_JSON_STRING(t, true) AS json_row
FROM Input AS t;
```

The above query produces the following result:

```json
+-----------------------+
| json_row              |
+-----------------------+
| {                     |
|  "x": [               |
|    1,                 |
|    2                  |
|  ],                   |
|  "y": "foo",          |
|  "s": {               |
|    "a": true,         |
|    "b": "2017-04-05"  |
|  }                    |
|}                      |
| {                     |
|  "x": null,           |
|  "y": "",             |
|  "s": {               |
|    "a": false,        |
|    "b": "0001-01-01"  |
|  }                    |
|}                      |
| {                     |
|  "x": [               |
|    3                  |
|  ],                   |
|  "y": "bar",          |
|  "s": {               |
|    "a": null,         |
|    "b": "2016-12-05"  |
|  }                    |
|}                      |
+-----------------------+
```

#### JSONPath format

Most JSON functions pass in a `json_string_expr` and `json_path_string_literal`
parameter. The `json_string_expr` parameter passes in a JSON-formatted
string, and the `json_path_string_literal` parameter identifies the value or
values you want to obtain from the JSON-formatted string.

The `json_string_expr` parameter must be a JSON string that is
formatted like this:

```json
{"class" : {"students" : [{"name" : "Jane"}]}}
```

You construct the `json_path_string_literal` parameter using the
[JSONPath][json-path] format. As part of this format, this parameter must start
with a `$` symbol, which refers to the outermost level of the JSON-formatted
string. You can identify child values using dots. If the JSON object is an
array, you can use brackets to specify the array index. If the keys contain
`$`, dots, or brackets, refer to each JSON function for how to escape
them.

JSONPath | Description            | Example               | Result using the above `json_string_expr`
-------- | ---------------------- | --------------------- | -----------------------------------------
$        | Root object or element | "$"                   | `{"class":{"students":[{"name":"Jane"}]}}`
.        | Child operator         | "$.class.students"    | `[{"name":"Jane"}]`
[]       | Subscript operator     | "$.class.students[0]" | `{"name":"Jane"}`

A JSON functions returns `NULL` if the `json_path_string_literal` parameter does
not match a value in `json_string_expr`. If the selected value for a scalar
function is not scalar, such as an object or an array, the function
returns `NULL`.

If the JSONPath is invalid, the function raises an error.

[jsonpath-format]: #jsonpath_format
[json-path]: https://github.com/json-path/JsonPath#operators

## Array functions

#### ARRAY

```
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
  `ARRAY` function returns an error: ZetaSQL does not support
  `ARRAY`s with elements of type
  [`ARRAY`][array-data-type].
+ If the subquery returns zero rows, the `ARRAY` function returns an empty
`ARRAY`. It never returns a `NULL` `ARRAY`.

**Return type**

ARRAY

**Examples**

```
SELECT ARRAY
  (SELECT 1 UNION ALL
   SELECT 2 UNION ALL
   SELECT 3) AS new_array;

+-----------+
| new_array |
+-----------+
| [1, 2, 3] |
+-----------+
```

To construct an `ARRAY` from a subquery that contains multiple
columns, change the subquery to use `SELECT AS STRUCT`. Now
the `ARRAY` function will return an `ARRAY` of `STRUCT`s. The `ARRAY` will
contain one `STRUCT` for each row in the subquery, and each of these `STRUCT`s
will contain a field for each column in that row.

```
SELECT
  ARRAY
    (SELECT AS STRUCT 1, 2, 3
     UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array;

+------------------------+
| new_array              |
+------------------------+
| [{1, 2, 3}, {4, 5, 6}] |
+------------------------+
```

Similarly, to construct an `ARRAY` from a subquery that contains
one or more `ARRAY`s, change the subquery to use `SELECT AS STRUCT`.

```
SELECT ARRAY
  (SELECT AS STRUCT [1, 2, 3] UNION ALL
   SELECT AS STRUCT [4, 5, 6]) AS new_array;

+----------------------------+
| new_array                  |
+----------------------------+
| [{[1, 2, 3]}, {[4, 5, 6]}] |
+----------------------------+
```

#### ARRAY_CONCAT

```
ARRAY_CONCAT(array_expression_1 [, array_expression_n])
```

**Description**

Concatenates one or more arrays with the same element type into a single array.

Note: You can also use the [|| concatenation operator][array-link-to-operators]
to concatenate arrays.

**Return type**

ARRAY

**Examples**

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

+--------------------------------------------------+
| count_to_six                                     |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

#### ARRAY_LENGTH

```
ARRAY_LENGTH(array_expression)
```

**Description**

Returns the size of the array. Returns 0 for an empty array. Returns `NULL` if
the `array_expression` is `NULL`.

**Return type**

INT64

**Examples**

```sql
WITH items AS
  (SELECT ["coffee", NULL, "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list, ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;

+---------------------------------+------+
| list                            | size |
+---------------------------------+------+
| [coffee, NULL, milk]            | 3    |
| [cake, pie]                     | 2    |
+---------------------------------+------+
```

#### ARRAY_TO_STRING

```
ARRAY_TO_STRING(array_expression, delimiter[, null_text])
```

**Description**

Returns a concatenation of the elements in `array_expression`
as a STRING. The value for `array_expression`
can either be an array of STRING or
BYTES data types.

If the `null_text` parameter is used, the function replaces any `NULL` values in
the array with the value of `null_text`.

If the `null_text` parameter is not used, the function omits the `NULL` value
and its preceding delimiter.

**Examples**

```sql
WITH items AS
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie                      |
+--------------------------------+
```

```sql
WITH items AS
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie--MISSING             |
+--------------------------------+
```

#### GENERATE_ARRAY
```
GENERATE_ARRAY(start_expression, end_expression[, step_expression])
```

**Description**

Returns an array of values. The `start_expression` and `end_expression`
parameters determine the inclusive start and end of the array.

The `GENERATE_ARRAY` function accepts the following data types as inputs:

<ul>
<li>INT64</li><li>UINT64</li><li>NUMERIC</li><li>DOUBLE</li>
</ul>

The `step_expression` parameter determines the increment used to
generate array values. The default value for this parameter is `1`.

This function returns an error if `step_expression` is set to 0, or if any
input is `NaN`.

If any argument is `NULL`, the function will return a `NULL` array.

**Return Data Type**

ARRAY

**Examples**

The following returns an array of integers, with a default step of 1.

```sql
SELECT GENERATE_ARRAY(1, 5) AS example_array;

+-----------------+
| example_array   |
+-----------------+
| [1, 2, 3, 4, 5] |
+-----------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_ARRAY(0, 10, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| [0, 3, 6, 9]  |
+---------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_ARRAY(10, 0, -3) AS example_array;

+---------------+
| example_array |
+---------------+
| [10, 7, 4, 1] |
+---------------+
```

The following returns an array using the same value for the `start_expression`
and `end_expression`.

```sql
SELECT GENERATE_ARRAY(4, 4, 10) AS example_array;

+---------------+
| example_array |
+---------------+
| [4]           |
+---------------+
```

The following returns an empty array, because the `start_expression` is greater
than the `end_expression`, and the `step_expression` value is positive.

```sql
SELECT GENERATE_ARRAY(10, 0, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| []            |
+---------------+
```

The following returns a `NULL` array because `end_expression` is `NULL`.

```sql
SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array;

+---------------+
| example_array |
+---------------+
| NULL          |
+---------------+
```

The following returns multiple arrays.

```sql
SELECT GENERATE_ARRAY(start, 5) AS example_array
FROM UNNEST([3, 4, 5]) AS start;

+---------------+
| example_array |
+---------------+
| [3, 4, 5]     |
| [4, 5]        |
| [5]           |
+---------------+
```

#### GENERATE_DATE_ARRAY
```
GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])
```

**Description**

Returns an array of dates. The `start_date` and `end_date`
parameters determine the inclusive start and end of the array.

The `GENERATE_DATE_ARRAY` function accepts the following data types as inputs:

+ `start_date` must be a DATE
+ `end_date` must be a DATE
+ `INT64_expr` must be an INT64
+ `date_part` must be either DAY, WEEK, MONTH, QUARTER, or YEAR.

The `INT64_expr` parameter determines the increment used to generate dates. The
default value for this parameter is 1 day.

This function returns an error if `INT64_expr` is set to 0.

**Return Data Type**

An ARRAY containing 0 or more DATE values.

**Examples**

The following returns an array of dates, with a default step of 1.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;

+--------------------------------------------------+
| example                                          |
+--------------------------------------------------+
| [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
+--------------------------------------------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_DATE_ARRAY(
 '2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example;

+--------------------------------------+
| example                              |
+--------------------------------------+
| [2016-10-05, 2016-10-07, 2016-10-09] |
+--------------------------------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL -3 DAY) AS example;

+--------------------------+
| example                  |
+--------------------------+
| [2016-10-05, 2016-10-02] |
+--------------------------+
```

The following returns an array using the same value for the `start_date`and
`end_date`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-05', INTERVAL 8 DAY) AS example;

+--------------+
| example      |
+--------------+
| [2016-10-05] |
+--------------+

```

The following returns an empty array, because the `start_date` is greater
than the `end_date`, and the `step` value is positive.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL 1 DAY) AS example;

+---------+
| example |
+---------+
| []      |
+---------+
```

The following returns a `NULL` array, because one of its inputs is
`NULL`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example;

+---------+
| example |
+---------+
| NULL    |
+---------+
```

The following returns an array of dates, using MONTH as the `date_part`
interval:

```sql
SELECT GENERATE_DATE_ARRAY('2016-01-01',
  '2016-12-31', INTERVAL 2 MONTH) AS example;

+--------------------------------------------------------------------------+
| example                                                                  |
+--------------------------------------------------------------------------+
| [2016-01-01, 2016-03-01, 2016-05-01, 2016-07-01, 2016-09-01, 2016-11-01] |
+--------------------------------------------------------------------------+
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

+--------------------------------------------------------------+
| date_range                                                   |
+--------------------------------------------------------------+
| [2016-01-01, 2016-01-08, 2016-01-15, 2016-01-22, 2016-01-29] |
| [2016-04-01, 2016-04-08, 2016-04-15, 2016-04-22, 2016-04-29] |
| [2016-07-01, 2016-07-08, 2016-07-15, 2016-07-22, 2016-07-29] |
| [2016-10-01, 2016-10-08, 2016-10-15, 2016-10-22, 2016-10-29] |
+--------------------------------------------------------------+
```

#### GENERATE_TIMESTAMP_ARRAY

```
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
+ Allowed `date_part` values are
  
  `MICROSECOND` or `NANOSECOND` (depends on what the SQL engine supports),
  
   `MILLISECOND`,
  
  `SECOND`, `MINUTE`, `HOUR`, or `DAY`.

The `step_expression` parameter determines the increment used to generate
timestamps.

**Return Data Type**

An `ARRAY` containing 0 or more
`TIMESTAMP` values.

**Examples**

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1 day.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00',
                                INTERVAL 1 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-06 00:00:00+00, 2016-10-07 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1
second.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:02',
                                INTERVAL 1 SECOND) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 00:00:01+00, 2016-10-05 00:00:02+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMPS` with a negative
interval.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-01 00:00:00',
                                INTERVAL -2 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-06 00:00:00+00, 2016-10-04 00:00:00+00, 2016-10-02 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` with a single element, because
`start_timestamp` and `end_timestamp` have the same value.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+--------------------------+
| timestamp_array          |
+--------------------------+
| [2016-10-05 00:00:00+00] |
+--------------------------+
```

The following example returns an empty `ARRAY`, because `start_timestamp` is
later than `end_timestamp`.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| []              |
+-----------------+
```

The following example returns a null `ARRAY`, because one of the inputs is
`NULL`.

```
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', NULL, INTERVAL 1 HOUR)
  AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| NULL            |
+-----------------+
```

The following example generates `ARRAY`s of `TIMESTAMP`s from columns containing
values for `start_timestamp` and `end_timestamp`.

```
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

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 01:00:00+00, 2016-10-05 02:00:00+00] |
| [2016-10-05 12:00:00+00, 2016-10-05 13:00:00+00, 2016-10-05 14:00:00+00] |
| [2016-10-05 23:59:00+00, 2016-10-06 00:59:00+00, 2016-10-06 01:59:00+00] |
+--------------------------------------------------------------------------+
```

#### OFFSET and ORDINAL

```
array_expression[OFFSET(zero_based_offset)]
array_expression[ORDINAL(one_based_offset)]
```

**Description**

Accesses an ARRAY element by position and returns the
element. `OFFSET` means that the numbering starts at zero, `ORDINAL` means that
the numbering starts at one.

A given array can be interpreted as either 0-based or 1-based. When accessing an
array element, you must preface the array position with `OFFSET` or `ORDINAL`,
respectively; there is no default behavior.

Both `OFFSET` and `ORDINAL` generate an error if the index is out of range.

**Return type**

Varies depending on the elements in the ARRAY.

**Examples**

```sql
WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list, list[OFFSET(1)] as offset_1, list[ORDINAL(1)] as ordinal_1
FROM items;

+----------------------------------+-----------+-----------+
| list                             | offset_1  | ordinal_1 |
+----------------------------------+-----------+-----------+
| [apples, bananas, pears, grapes] | bananas   | apples    |
| [coffee, tea, milk]              | tea       | coffee    |
| [cake, pie]                      | pie       | cake      |
+----------------------------------+-----------+-----------+
```

#### ARRAY_REVERSE
```
ARRAY_REVERSE(value)
```

**Description**

Returns the input ARRAY with elements in reverse order.

**Return type**

ARRAY

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

+-----------+-------------+
| arr       | reverse_arr |
+-----------+-------------+
| [1, 2, 3] | [3, 2, 1]   |
| [4, 5]    | [5, 4]      |
| []        | []          |
+-----------+-------------+
```

#### SAFE_OFFSET and SAFE_ORDINAL

```
array_expression[SAFE_OFFSET(zero_based_offset)]
array_expression[SAFE_ORDINAL(one_based_offset)]
```

**Description**

Identical to `OFFSET` and `ORDINAL`, except returns `NULL` if the index is out
of range.

**Return type**

Varies depending on the elements in the ARRAY.

**Example**

```sql
WITH items AS
  (SELECT ["apples", "bananas", "pears", "grapes"] as list
  UNION ALL
  SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)

SELECT list,
  list[SAFE_OFFSET(3)] as safe_offset_3,
  list[SAFE_ORDINAL(3)] as safe_ordinal_3
FROM items;

+----------------------------------+---------------+----------------+
| list                             | safe_offset_3 | safe_ordinal_3 |
+----------------------------------+---------------+----------------+
| [apples, bananas, pears, grapes] | grapes        | pears          |
| [coffee, tea, milk]              | NULL          | milk           |
| [cake, pie]                      | NULL          | NULL           |
+----------------------------------+---------------+----------------+
```

[subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax#subqueries
[datamodel-sql-tables]: https://github.com/google/zetasql/blob/master/docs/data-model#standard-sql-tables
[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model#value-tables
[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types#array_type

[array-link-to-operators]: #operators

[subqueries]: #subqueries
[datamodel-sql-tables]: #standard-sql-tables
[datamodel-value-tables]: #value-tables
[array-data-type]: #array-type
[array-link-to-operators]: #operators

## Date functions

ZetaSQL supports the following `DATE` functions.

#### CURRENT_DATE

```
CURRENT_DATE([time_zone])
```

**Description**

Returns the current date as of the specified or default timezone.

This function supports an optional `time_zone` parameter. This parameter is a
string representing the timezone to use. If no timezone is specified, the
default timezone, which is implementation defined, is used. See
[Timezone definitions][date-functions-link-to-timezone-definitions] for information on how to
specify a time zone.

If the `time_zone` parameter evaluates to `NULL`, this function returns `NULL`.

**Return Data Type**

DATE

**Example**

```sql
SELECT CURRENT_DATE() as the_date;

+--------------+
| the_date     |
+--------------+
| 2016-12-25   |
+--------------+
```

#### EXTRACT

```
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
time part.

```sql
SELECT EXTRACT(DAY FROM DATE '2013-12-25') as the_day;

+---------+
| the_day |
+---------+
| 25      |
+---------+
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of dates near the end of the year.

```sql
SELECT
  date,
  EXTRACT(ISOYEAR FROM date) AS isoyear,
  EXTRACT(ISOWEEK FROM date) AS isoweek,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(WEEK FROM date) AS week
FROM UNNEST(GENERATE_DATE_ARRAY('2015-12-23', '2016-01-09')) AS date
ORDER BY date;
+------------+---------+---------+------+------+
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
+------------+---------+---------+------+------+
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

+------------+-------------+-------------+
| date       | week_sunday | week_monday |
+------------+-------------+-------------+
| 2017-11-05 | 45          | 44          |
+------------+-------------+-------------+
```

#### DATE
```
1. DATE(year, month, day)
2. DATE(timestamp_expression[, timezone])
3. DATE(datetime_expression)
```

**Description**

1. Constructs a DATE from INT64 values representing the year, month, and day.
2. Extracts the DATE from a TIMESTAMP expression. It supports an
   optional parameter to [specify a timezone][date-functions-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Extracts the DATE from a DATETIME expression.

**Return Data Type**

DATE

**Example**

```sql
SELECT
  DATE(2016, 12, 25) as date_ymd,
  DATE(DATETIME "2016-12-25 23:59:59") as date_dt,
  DATE(TIMESTAMP "2016-12-25 05:30:00+07", "America/Los_Angeles") as date_tstz;

+------------+------------+------------+
| date_ymd   | date_dt    | date_tstz  |
+------------+------------+------------+
| 2016-12-25 | 2016-12-25 | 2016-12-24 |
+------------+------------+------------+

```

#### DATE_ADD
```
DATE_ADD(date_expression, INTERVAL INT64_expr date_part)
```

**Description**

Adds a specified time interval to a DATE.

`DATE_ADD` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original date's day, then the result day is the last day of the
new month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_ADD(DATE "2008-12-25", INTERVAL 5 DAY) as five_days_later;

+--------------------+
| five_days_later    |
+--------------------+
| 2008-12-30         |
+--------------------+
```

#### DATE_SUB
```
DATE_SUB(date_expression, INTERVAL INT64_expr date_part)
```

**Description**

Subtracts a specified time interval from a DATE.

`DATE_SUB` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original date's day, then the result day is the last day of the
new month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_SUB(DATE "2008-12-25", INTERVAL 5 DAY) as five_days_ago;

+---------------+
| five_days_ago |
+---------------+
| 2008-12-20    |
+---------------+
```

#### DATE_DIFF
```
DATE_DIFF(date_expression, date_expression, date_part)
```

**Description**

Returns the number of `date_part` boundaries between the two `date_expression`s.
If the first date occurs before the second date, then the result is
non-positive.

`DATE_DIFF` supports the following `date_part` values:

+  `DAY`
+  `WEEK` This date part begins on Sunday.
+  `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
   `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
   boundaries. ISO weeks begin on Monday.
+  `MONTH`
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
SELECT DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY) as days_diff;

+-----------+
| days_diff |
+-----------+
| 559       |
+-----------+

```

```sql
SELECT
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', DAY) as days_diff,
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', WEEK) as weeks_diff;

+-----------+------------+
| days_diff | weeks_diff |
+-----------+------------+
| 1         | 1          |
+-----------+------------+
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

```
SELECT
  DATE_DIFF('2017-12-30', '2014-12-30', YEAR) AS year_diff,
  DATE_DIFF('2017-12-30', '2014-12-30', ISOYEAR) AS isoyear_diff;

+-----------+--------------+
| year_diff | isoyear_diff |
+-----------+--------------+
| 3         | 2            |
+-----------+--------------+
```

The following example shows the result of `DATE_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATE_DIFF` with the date part `WEEK` returns 0 because this time part
uses weeks that begin on Sunday. `DATE_DIFF` with the date part `WEEK(MONDAY)`
returns 1. `DATE_DIFF` with the date part `ISOWEEK` also returns 1 because
ISO weeks begin on Monday.

```
SELECT
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

+-----------+-------------------+--------------+
| week_diff | week_weekday_diff | isoweek_diff |
+-----------+-------------------+--------------+
| 0         | 1                 | 1            |
+-----------+-------------------+--------------+
```

#### DATE_TRUNC
```
DATE_TRUNC(date_expression, date_part)
```

**Description**

Truncates the date to the specified granularity.

`DATE_TRUNC` supports the following values for `date_part`:

+  `DAY`
+  `WEEK`
+  `WEEK(<WEEKDAY>)`: Truncates `date_expression` to the preceding week
   boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
   `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
   `SATURDAY`.
+  `ISOWEEK`: Truncates `date_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+  `MONTH`
+  `QUARTER`
+  `YEAR`
+  `ISOYEAR`: Truncates `date_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

DATE

**Examples**

```
SELECT DATE_TRUNC(DATE '2008-12-25', MONTH) as month;

+------------+
| month      |
+------------+
| 2008-12-01 |
+------------+
```

In the following example, the original date falls on a Sunday. Because
the `date_part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATE` for the
preceding Monday.

```
SELECT date AS original, DATE_TRUNC(date, WEEK(MONDAY)) AS truncated
FROM (SELECT DATE('2017-11-05') AS date);

+------------+------------+
| original   | truncated  |
+------------+------------+
| 2017-11-05 | 2017-10-30 |
+------------+------------+
```

In the following example, the original `date_expression` is in the Gregorian
calendar year 2015. However, `DATE_TRUNC` with the `ISOYEAR` date part
truncates the `date_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `date_expression` 2015-06-15 is
2014-12-29.

```
SELECT
  DATE_TRUNC('2015-06-15', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATE '2015-06-15') AS isoyear_number;

+------------------+----------------+
| isoyear_boundary | isoyear_number |
+------------------+----------------+
| 2014-12-29       | 2015           |
+------------------+----------------+
```

#### DATE_FROM_UNIX_DATE
```
DATE_FROM_UNIX_DATE(INT64_expression)
```

**Description**

Interprets `INT64_expression` as the number of days since 1970-01-01.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_FROM_UNIX_DATE(14238) as date_from_epoch;

+-----------------+
| date_from_epoch |
+-----------------+
| 2008-12-25      |
+-----------------+
```

#### FORMAT_DATE
```
FORMAT_DATE(format_string, date_expr)
```

**Description**

Formats the `date_expr` according to the specified `format_string`.

See [Supported Format Elements For DATE][date-functions-link-to-supported-format-elements-for-date]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Examples**

```sql
SELECT FORMAT_DATE("%x", DATE "2008-12-25") as US_format;

+------------+
| US_format  |
+------------+
| 12/25/08   |
+------------+
```

```sql
SELECT FORMAT_DATE("%b-%d-%Y", DATE "2008-12-25") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT FORMAT_DATE("%b %Y", DATE "2008-12-25") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

#### PARSE_DATE
```
PARSE_DATE(format_string, date_string)
```

**Description**

Uses a `format_string` and a string representation of a date to return a DATE
object.

When using `PARSE_DATE`, keep the following in mind:

+ *Unspecified fields.* Any unspecified field is initialized from `1970-01-01`.
+ *Case insensitive names.* Names, such as `Monday`, `February`, and so on, are
case insensitive.
+ *Whitespace.* One or more consecutive white spaces in the format string
matches zero or more consecutive white spaces in the date string. In
addition, leading and trailing white spaces in the date string are always
allowed -- even if they are not in the format string.
+ *Format precedence.* When two (or more) format elements have overlapping
information (for example both `%F` and `%Y` affect the year), the last one
generally overrides any earlier ones.

Note: This function supports [format elements][date-functions-link-to-supported-format-elements-for-date],
but does not have full support for `%Q`, `%a`, `%A`, `%g`, `%G`, `%j`, `%u`, `%U`, `%V`, `%w`, and `%W`.

**Return Data Type**

DATE

**Example**

```sql
SELECT PARSE_DATE("%x", "12/25/08") as parsed;

+------------+
| parsed     |
+------------+
| 2008-12-25 |
+------------+
```

#### UNIX_DATE
```
UNIX_DATE(date_expression)
```

**Description**

Returns the number of days since 1970-01-01.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_DATE(DATE "2008-12-25") as days_from_epoch;

+-----------------+
| days_from_epoch |
+-----------------+
| 14238           |
+-----------------+
```

#### Supported Format Elements for DATE

Unless otherwise noted, DATE functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal
number (00-99).</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y produces as many
    characters as it takes to fully render the year.</td>
 </tr>
</table>

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601
[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date
[date-functions-link-to-supported-format-elements-for-date]: #supported_format_elements_for_date

[date-functions-link-to-timezone-definitions]: #timezone_definitions

[date-functions-link-to-timezone-definitions]: #timezone_definitions

## DateTime functions

ZetaSQL supports the following `DATETIME` functions.

#### CURRENT_DATETIME
```
CURRENT_DATETIME([timezone])
```

**Description**

Returns the current time as a DATETIME object.

This function supports an optional `timezone` parameter.
See [Timezone definitions][datetime-link-to-timezone-definitions] for information on how to
specify a time zone.

**Return Data Type**

DATETIME

**Example**

```sql
SELECT CURRENT_DATETIME() as now;

+----------------------------+
| now                        |
+----------------------------+
| 2016-05-19 10:38:47.046465 |
+----------------------------+
```

#### DATETIME
```
1. DATETIME(year, month, day, hour, minute, second)
2. DATETIME(date_expression, time_expression)
3. DATETIME(timestamp_expression [, timezone])
```

**Description**

1. Constructs a DATETIME object using INT64 values representing the year, month,
   day, hour, minute, and second.
2. Constructs a DATETIME object using a DATE object and a TIME object.
3. Constructs a DATETIME object using a TIMESTAMP object. It supports an
   optional parameter to [specify a timezone][datetime-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.

**Return Data Type**

DATETIME

**Example**

```sql
SELECT
  DATETIME(2008, 12, 25, 05, 30, 00) as datetime_ymdhms,
  DATETIME(TIMESTAMP "2008-12-25 05:30:00+00", "America/Los_Angeles") as datetime_tstz;

+---------------------+---------------------+
| datetime_ymdhms     | datetime_tstz       |
+---------------------+---------------------+
| 2008-12-25 05:30:00 | 2008-12-24 21:30:00 |
+---------------------+---------------------+
```

#### EXTRACT
```
EXTRACT(part FROM datetime_expression)
```

**Description**

Returns an `INT64` value that corresponds to the specified `part` from
a supplied `datetime_expression`.

Allowed `part` values are:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`
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

INT64, except in the following cases:

+ If `part` is `DATE`, returns a `DATE` object.
+ If `part` is `TIME`, returns a `TIME` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00)) as hour;

+------------------+
| hour             |
+------------------+
| 15               |
+------------------+
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
+---------------------+---------+---------+------+------+
| datetime            | isoyear | isoweek | year | week |
+---------------------+---------+---------+------+------+
| 2005-01-03 12:34:56 | 2005    | 1       | 2005 | 1    |
| 2007-12-31 00:00:00 | 2008    | 1       | 2007 | 52   |
| 2009-01-01 00:00:00 | 2009    | 1       | 2009 | 0    |
| 2009-12-31 00:00:00 | 2009    | 53      | 2009 | 52   |
| 2017-01-02 00:00:00 | 2017    | 1       | 2017 | 1    |
| 2017-05-26 00:00:00 | 2017    | 21      | 2017 | 21   |
+---------------------+---------+---------+------+------+
```

In the following example, `datetime_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT DATETIME(TIMESTAMP '2017-11-05 00:00:00-8') AS datetime)
SELECT
  datetime,
  EXTRACT(WEEK(SUNDAY) FROM datetime) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM datetime) AS week_monday
FROM table;

+---------------------+-------------+---------------+
| datetime            | week_sunday | week_monday   |
+---------------------+-------------+---------------+
| 2017-11-06 00:00:00 | 45          | 44            |
+---------------------+-------------+---------------+
```

#### DATETIME_ADD
```
DATETIME_ADD(datetime_expression, INTERVAL INT64_expr part)
```

**Description**

Adds `INT64_expr` units of `part` to the DATETIME object.

`DATETIME_ADD` supports the following values for `part`:

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

DATETIME

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as later;

+-----------------------------+------------------------+
| original_date               | later                  |
+-----------------------------+------------------------+
| 2008-12-25 15:30:00         | 2008-12-25 15:40:00    |
+-----------------------------+------------------------+
```

#### DATETIME_SUB
```
DATETIME_SUB(datetime_expression, INTERVAL INT64_expr part)
```

**Description**

Subtracts `INT64_expr` units of `part` from the DATETIME.

`DATETIME_SUB` supports the following values for `part`:

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

DATETIME

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as earlier;

+-----------------------------+------------------------+
| original_date               | earlier                |
+-----------------------------+------------------------+
| 2008-12-25 15:30:00         | 2008-12-25 15:20:00    |
+-----------------------------+------------------------+
```

#### DATETIME_DIFF
```
DATETIME_DIFF(datetime_expression, datetime_expression, part)
```

**Description**

Returns the number of `part` boundaries between the two `datetime_expression`s.
If the first `DATETIME` occurs before the second `DATETIME`, then the result is
non-positive. Throws an error if the computation overflows the result type, such
as if the difference in microseconds between the two `DATETIME` objects would
overflow an `INT64` value.

`DATETIME_DIFF` supports the following values for `part`:

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
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Uses the [ISO 8601][ISO-8601]
  week-numbering year boundary. The ISO year boundary is the Monday of the
  first week whose Thursday belongs to the corresponding Gregorian calendar
  year.

**Return Data Type**

INT64

**Example**

```sql
SELECT
  DATETIME "2010-07-07 10:20:00" as first_datetime,
  DATETIME "2008-12-25 15:30:00" as second_datetime,
  DATETIME_DIFF(DATETIME "2010-07-07 10:20:00",
    DATETIME "2008-12-25 15:30:00", DAY) as difference;

+----------------------------+------------------------+------------------------+
| first_datetime             | second_datetime        | difference             |
+----------------------------+------------------------+------------------------+
| 2010-07-07 10:20:00        | 2008-12-25 15:30:00    | 559                    |
+----------------------------+------------------------+------------------------+

```

```sql
SELECT
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', DAY) as days_diff,
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', WEEK) as weeks_diff;

+-----------+------------+
| days_diff | weeks_diff |
+-----------+------------+
| 1         | 1          |
+-----------+------------+

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

```
SELECT
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', YEAR) AS year_diff,
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', ISOYEAR) AS isoyear_diff;

+-----------+--------------+
| year_diff | isoyear_diff |
+-----------+--------------+
| 3         | 2            |
+-----------+--------------+
```

The following example shows the result of `DATETIME_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATETIME_DIFF` with the date part `WEEK` returns 0 because this time
part uses weeks that begin on Sunday. `DATETIME_DIFF` with the date part
`WEEK(MONDAY)` returns 1. `DATETIME_DIFF` with the date part
`ISOWEEK` also returns 1 because ISO weeks begin on Monday.

```
SELECT
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

+-----------+-------------------+--------------+
| week_diff | week_weekday_diff | isoweek_diff |
+-----------+-------------------+--------------+
| 0         | 1                 | 1            |
+-----------+-------------------+--------------+
```

#### DATETIME_TRUNC

```
DATETIME_TRUNC(datetime_expression, part)
```

**Description**

Truncates a `DATETIME` object to the granularity of `part`.

`DATETIME_TRUNC` supports the following values for `part`:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`: Truncates `datetime_expression` to the preceding week
  boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Truncates `datetime_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Truncates `datetime_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

DATETIME

**Examples**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original,
  DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY) as truncated;

+----------------------------+------------------------+
| original                   | truncated              |
+----------------------------+------------------------+
| 2008-12-25 15:30:00        | 2008-12-25 00:00:00    |
+----------------------------+------------------------+
```

In the following example, the original `DATETIME` falls on a Sunday. Because the
`part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATETIME` for the
preceding Monday.

```sql
SELECT
 datetime AS original,
 DATETIME_TRUNC(datetime, WEEK(MONDAY)) AS truncated
FROM (SELECT DATETIME(TIMESTAMP '2017-11-05 00:00:00') AS datetime);

+---------------------+---------------------+
| original            | truncated           |
+---------------------+---------------------+
| 2017-11-05 00:00:00 | 2017-10-30 00:00:00 |
+---------------------+---------------------+

```

In the following example, the original `datetime_expression` is in the Gregorian
calendar year 2015. However, `DATETIME_TRUNC` with the `ISOYEAR` date part
truncates the `datetime_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `datetime_expression`
2015-06-15 00:00:00 is 2014-12-29.

```
SELECT
  DATETIME_TRUNC('2015-06-15 00:00:00', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATETIME '2015-06-15 00:00:00') AS isoyear_number;

+---------------------+----------------+
| isoyear_boundary    | isoyear_number |
+---------------------+----------------+
| 2014-12-29 00:00:00 | 2015           |
+---------------------+----------------+
```

#### FORMAT_DATETIME

```
FORMAT_DATETIME(format_string, datetime_expression)
```

**Description**

Formats a DATETIME object according to the specified `format_string`. See
[Supported Format Elements For DATETIME][datetime-functions-link-to-supported-format-elements-for-datetime]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Examples**

```sql
SELECT
  FORMAT_DATETIME("%c", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+--------------------------+
| formatted                |
+--------------------------+
| Thu Dec 25 15:30:00 2008 |
+--------------------------+
```

```sql
SELECT
  FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT
  FORMAT_DATETIME("%b %Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

#### PARSE_DATETIME

```
PARSE_DATETIME(format_string, string)
```
**Description**

Uses a `format_string` and a `STRING` representation
of a `DATETIME` to return a
`DATETIME`. See
[Supported Format Elements For DATETIME][datetime-functions-link-to-supported-format-elements-for-datetime]
for a list of format elements that this function supports.

`PARSE_DATETIME` parses `string` according to the following rules:

+ **Unspecified fields.** Any unspecified field is initialized from
`1970-01-01 00:00:00.0`. For example, if the year is unspecified then it
defaults to `1970`.
+ **Case insensitive names.** Names, such as `Monday` and `February`,
are case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
matches zero or more consecutive white spaces in the
`DATETIME` string. Leading and trailing
white spaces in the `DATETIME` string are always
allowed&mdash;even if they are not in the format string.
+ **Format precedence.** When two or more format elements have overlapping
information, the last one generally overrides any earlier ones, with some
exceptions. For example, both `%F` and `%Y` affect the year, so the earlier
element overrides the later. See the descriptions
of `%s`, `%C`, and `%y` in
[Supported Format Elements For DATETIME][datetime-functions-link-to-supported-format-elements-for-datetime].

Note: This function supports [format elements][datetime-functions-link-to-supported-format-elements-for-datetime],
but does not have full support for `%Q`, `%a`, `%A`, `%g`, `%G`, `%j`, `%u`, `%U`, `%V`, `%w`, and `%W`.

**Examples**

The following example parses a `STRING` literal as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1998-10-18 13:45:55') AS datetime;
```

The above query returns the following output:

```
+---------------------+
| datetime            |
+---------------------+
| 1998-10-18 13:45:55 |
+---------------------+
```

The following example parses a `STRING` literal
containing a date in a natural language format as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%A, %B %e, %Y','Wednesday, December 19, 2018')
  AS datetime;
```

The above query returns the following output:

```
+---------------------+
| datetime            |
+---------------------+
| 2018-12-19 00:00:00 |
+---------------------+
```

**Return Data Type**

DATETIME

#### Supported format elements for DATETIME

Unless otherwise noted, DATETIME functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal number (00-99).</td>
 </tr>
 <tr>
    <td>%c</td>
    <td>The date and time representation.</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
 </tr>
 <tr>
    <td>%s</td>
    <td>The number of seconds since 1970-01-01 00:00:00. Always overrides all
    other format elements, independent of where %s appears in the string.
    If multiple %s elements appear, then the last one takes precedence.</td>
</tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
 </tr>
 <tr>
    <td>%E#S</td>
    <td>Seconds with # digits of fractional precision.</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y
    produces as many characters as it takes to fully render the
year.</td>
 </tr>
</table>

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601
[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date
[datetime-functions-link-to-supported-format-elements-for-datetime]: #supported_format_elements_for_datetime

[datetime-link-to-timezone-definitions]: #timezone_definitions

[datetime-link-to-timezone-definitions]: #timezone_definitions

## Debugging functions

ZetaSQL supports the following debugging functions.

#### ERROR
```
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
    ELSE ERROR(concat('Found unexpected value: ', value))
  END AS new_value
FROM (
  SELECT 'foo' AS value UNION ALL
  SELECT 'bar' AS value UNION ALL
  SELECT 'baz' AS value);

Found unexpected value: baz
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
WHERE IF(x > 0, true, ERROR(FORMAT('Error: x must be positive but is %t', x)));'

Error: x must be positive but is -1
```

## Time functions

ZetaSQL supports the following `TIME` functions.

#### CURRENT_TIME
```
CURRENT_TIME()
```

**Description**

Returns the current time as a TIME object.

**Return Data Type**

TIME

**Example**

```sql
SELECT CURRENT_TIME() as now;

+----------------------------+
| now                        |
+----------------------------+
| 15:31:38.776361            |
+----------------------------+
```

#### TIME
```
1. TIME(hour, minute, second)
2. TIME(timestamp, [timezone])
3. TIME(datetime)
```

**Description**

1. Constructs a `TIME` object using `INT64` values representing the hour,
   minute, and second.
2. Constructs a `TIME` object using a `TIMESTAMP` object. It supports an
   optional
   parameter to [specify a timezone][time-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Constructs a `TIME` object using a
  `DATETIME` object.

**Return Data Type**

TIME

**Example**

```sql
SELECT
  TIME(15, 30, 00) as time_hms,
  TIME(TIMESTAMP "2008-12-25 15:30:00+08", "America/Los_Angeles") as time_tstz;
+----------+-----------+
| time_hms | time_tstz |
+----------+-----------+
| 15:30:00 | 23:30:00  |
+----------+-----------+
```

```sql
SELECT
  TIME(DATETIME "2008-12-25 15:30:00.000000") AS time_dt;
+----------+
| time_dt  |
+----------+
| 15:30:00 |
+----------+
```

#### EXTRACT
```
EXTRACT(part FROM time_expression)
```

**Description**

Returns an `INT64` value that corresponds to the specified `part` from
a supplied `time_expression`.

Allowed `part` values are:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

INT64

**Example**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM TIME "15:30:00") as hour;

+------------------+
| hour             |
+------------------+
| 15               |
+------------------+
```

#### TIME_ADD
```
TIME_ADD(time_expression, INTERVAL INT64_expr part)
```

**Description**

Adds `INT64_expr` units of `part` to the TIME object.

`TIME_ADD` supports the following values for `part`:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you add an hour to `23:30:00`, the returned
value is `00:30:00`.

**Return Data Types**

TIME

**Example**

```sql
SELECT
  TIME "15:30:00" as original_time,
  TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE) as later;

+-----------------------------+------------------------+
| original_time               | later                  |
+-----------------------------+------------------------+
| 15:30:00                    | 15:40:00               |
+-----------------------------+------------------------+
```

#### TIME_SUB
```
TIME_SUB(time_expression, INTERVAL INT_expr part)
```

**Description**

Subtracts `INT64_expr` units of `part` from the TIME object.

`TIME_SUB` supports the following values for `part`:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you subtract an hour from `00:30:00`, the
returned value is `23:30:00`.

**Return Data Type**

TIME

**Example**

```sql
SELECT
  TIME "15:30:00" as original_date,
  TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE) as earlier;

+-----------------------------+------------------------+
| original_date                | earlier                |
+-----------------------------+------------------------+
| 15:30:00                    | 15:20:00               |
+-----------------------------+------------------------+
```

#### TIME_DIFF
```
TIME_DIFF(time_expression, time_expression, part)
```

**Description**

Returns the number of whole specified `part` intervals between two
TIME objects. Throws an error if the computation overflows the result type,
such as if the difference in microseconds between the two time objects would
overflow an INT64 value.

`TIME_DIFF` supports the following values for `part`:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

INT64

**Example**

```sql
SELECT
  TIME "15:30:00" as first_time,
  TIME "14:35:00" as second_time,
  TIME_DIFF(TIME "15:30:00", TIME "14:35:00", MINUTE) as difference;

+----------------------------+------------------------+------------------------+
| first_time                 | second_time            | difference             |
+----------------------------+------------------------+------------------------+
| 15:30:00                   | 14:35:00               | 55                     |
+----------------------------+------------------------+------------------------+
```

#### TIME_TRUNC

```
TIME_TRUNC(time_expression, part)
```

**Description**

Truncates a TIME object to the granularity of `part`.

`TIME_TRUNC` supports the following values for `part`:

+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

TIME

**Example**

```sql
SELECT
  TIME "15:30:00" as original,
  TIME_TRUNC(TIME "15:30:00", HOUR) as truncated;

+----------------------------+------------------------+
| original                   | truncated              |
+----------------------------+------------------------+
| 15:30:00                   | 15:00:00               |
+----------------------------+------------------------+
```

#### FORMAT_TIME

```
FORMAT_TIME(format_string, time_object)
```

**Description**
Formats a TIME object according to the specified `format_string`. See
[Supported Format Elements For TIME][time-link-to-supported-format-elements-for-time]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Example**

```sql
SELECT FORMAT_TIME("%R", TIME "15:30:00") as formatted_time;

+----------------+
| formatted_time |
+----------------+
| 15:30          |
+----------------+
```

#### PARSE_TIME

```
PARSE_TIME(format_string, string)
```

**Description**

Uses a `format_string` and a string to return a TIME object. See
[Supported Format Elements For TIME][time-link-to-supported-format-elements-for-time]
for a list of format elements that this function supports.

When using `PARSE_TIME`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from
`00:00:00.0`. For instance, if `seconds` is unspecified then it
defaults to `00`, and so on.
+ **Whitespace.** One or more consecutive white spaces in the format string
matches zero or more consecutive white spaces in the TIME string. In
addition, leading and trailing white spaces in the TIME string are always
allowed&mdash;even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
information, the last one generally overrides any earlier ones.

**Return Data Type**

TIME

**Example**

```sql
SELECT PARSE_TIME("%H", "15") as parsed_time;

+-------------+
| parsed_time |
+-------------+
| 15:00:00    |
+-------------+
```

#### Supported format elements for TIME

Unless otherwise noted, TIME functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
 </tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
 </tr>
 <tr>
    <td>%E#S</td>
    <td>Seconds with # digits of fractional precision.</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
 </tr>
</table>

[time-link-to-supported-format-elements-for-time]: #supported_format_elements_for_time

[time-link-to-timezone-definitions]: #timezone_definitions

[time-link-to-timezone-definitions]: #timezone_definitions

## Timestamp functions

ZetaSQL supports the following `TIMESTAMP` functions.

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined date and timestamp min/max values.

#### CURRENT_TIMESTAMP

```sql
CURRENT_TIMESTAMP()
```

**Description**

Parentheses are optional. This function handles leap seconds by smearing them
across a window of 20 hours around the inserted leap
second.
`CURRENT_TIMESTAMP()` produces a TIMESTAMP value that is continuous,
non-ambiguous, has exactly 60 seconds per minute and does not repeat values over
the leap second.

**Supported Input Types**

Not applicable

**Result Data Type**

TIMESTAMP

**Example**

```sql
SELECT CURRENT_TIMESTAMP() as now;

+----------------------------------+
| now                              |
+----------------------------------+
| 2016-05-16 18:12:47.145482639+00 |
+----------------------------------+
```

#### EXTRACT

```sql
EXTRACT(part FROM timestamp_expression [AT TIME ZONE tz_spec])
```

**Description**

Returns an `INT64` value that corresponds to the specified `part` from
a supplied `timestamp_expression`.

Allowed `part` values are:

+ `NANOSECOND`
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`
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
<li><code>DATETIME</code></li>
<li><code>TIME</code></li>
</ul>

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

See [Timezone definitions][timestamp-link-to-timezone-definitions] for
information on how to specify a time zone.

**Return Data Type**

Generally
`INT64`
. Returns
`DATE` if `part` is
`DATE`.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
time part.

```sql
SELECT EXTRACT(DAY
  FROM TIMESTAMP "2008-12-25 15:30:00" AT TIME ZONE "America/Los_Angeles")
  AS the_day;

+------------+
| the_day    |
+------------+
| 25         |
+------------+
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of timestamps.

```sql
WITH Timestamps AS (
  SELECT TIMESTAMP '2005-01-03 12:34:56' AS timestamp UNION ALL
  SELECT TIMESTAMP '2007-12-31' UNION ALL
  SELECT TIMESTAMP '2009-01-01' UNION ALL
  SELECT TIMESTAMP '2009-12-31' UNION ALL
  SELECT TIMESTAMP '2017-01-02' UNION ALL
  SELECT TIMESTAMP '2017-05-26'
)
SELECT
  timestamp,
  EXTRACT(ISOYEAR FROM timestamp) AS isoyear,
  EXTRACT(ISOWEEK FROM timestamp) AS isoweek,
  EXTRACT(YEAR FROM timestamp) AS year,
  EXTRACT(WEEK FROM timestamp) AS week
FROM Timestamps
ORDER BY timestamp;

+------------------------+---------+---------+------+------+
| timestamp              | isoyear | isoweek | year | week |
+------------------------+---------+---------+------+------+
| 2005-01-03 12:34:56+00 | 2005    | 1       | 2005 | 1    |
| 2007-12-31 00:00:00+00 | 2008    | 1       | 2007 | 52   |
| 2009-01-01 00:00:00+00 | 2009    | 1       | 2009 | 0    |
| 2009-12-31 00:00:00+00 | 2009    | 53      | 2009 | 52   |
| 2017-01-02 00:00:00+00 | 2017    | 1       | 2017 | 1    |
| 2017-05-26 00:00:00+00 | 2017    | 21      | 2017 | 21   |
+------------------------+---------+---------+------+------+
```

In the following example, `timestamp_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT TIMESTAMP('2017-11-05 00:00:00') AS timestamp)
SELECT
  timestamp,
  EXTRACT(WEEK(SUNDAY) FROM timestamp) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM timestamp) AS week_monday
FROM table;

+------------------------+-------------+---------------+
| timestamp              | week_sunday | week_monday   |
+------------------------+-------------+---------------+
| 2017-11-05 00:00:00+00 | 45          | 44            |
+------------------------+-------------+---------------+
```

#### STRING

```sql
STRING(timestamp_expression[, timezone])
```

**Description**

Converts a `timestamp_expression` to a STRING data type. Supports an optional
parameter to specify a time zone. See
[Timezone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT STRING(TIMESTAMP "2008-12-25 15:30:00", "America/Los_Angeles") as string;

+-------------------------------+
| string                        |
+-------------------------------+
| 2008-12-25 07:30:00-08        |
+-------------------------------+
```

#### TIMESTAMP

```sql
TIMESTAMP(
  string_expression[, timezone] |
  date_expression[, timezone] |
  datetime_expression[, timezone]
)
```

**Description**

+  `string_expression[, timezone]`: Converts a STRING expression to a TIMESTAMP
   data type. `string_expression` must include a
   timestamp literal.
   If `string_expression` includes a timezone in the timestamp literal, do not
   include an explicit `timezone`
   argument.
+  `date_expression[, timezone]`: Converts a DATE object to a TIMESTAMP
   data type.
+  `datetime_expression[, timezone]`: Converts a
   DATETIME object to a TIMESTAMP data type.

This function supports an optional
parameter to [specify a time zone][timestamp-link-to-timezone-definitions]. If
no time zone is specified, the default time zone, which is implementation defined,
is used.

**Return Data Type**

TIMESTAMP

**Examples**

In these examples, a time zone is specified.

```sql
SELECT CAST(
  TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles") AS STRING
) AS timestamp_str;

+------------------------+
| timestamp_str          |
+------------------------+
| 2008-12-25 23:30:00+00 |
+------------------------+
```

```sql
SELECT CAST(
  TIMESTAMP("2008-12-25 15:30:00 America/Los_Angeles") AS STRING
) AS timestamp_str_timezone;

+------------------------+
| timestamp_str_timezone |
+------------------------+
| 2008-12-25 23:30:00+00 |
+------------------------+
```

```sql
SELECT CAST(
  TIMESTAMP(DATETIME "2008-12-25 15:30:00", "America/Los_Angeles") AS STRING
) AS timestamp_datetime;

+------------------------+
| timestamp_datetime     |
+------------------------+
| 2008-12-25 23:30:00+00 |
+------------------------+
```

```sql
SELECT CAST(
  TIMESTAMP(DATE "2008-12-25", "America/Los_Angeles") AS STRING
) AS timestamp_date;

+------------------------+
| timestamp_date         |
+------------------------+
| 2008-12-25 08:00:00+00 |
+------------------------+
```

In these examples, assume that the default time zone is UTC.

```sql
SELECT CAST(
  TIMESTAMP("2008-12-25 15:30:00") AS STRING
) AS timestamp_str;

+------------------------+
| timestamp_str          |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

```sql
SELECT CAST(
  TIMESTAMP(DATE "2008-12-25") AS STRING
) AS timestamp_date;

+------------------------+
| timestamp_date         |
+------------------------+
| 2008-12-25 00:00:00+00 |
+------------------------+
```

#### TIMESTAMP_ADD

```sql
TIMESTAMP_ADD(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds `int64_expression` units of `date_part` to the timestamp, independent of
any time zone.

`TIMESTAMP_ADD` supports the following values for `date_part`:

<ul>
<li><code>NANOSECOND</code></li>
<li><code>MICROSECOND</code></li>
<li><code>MILLISECOND</code></li>
<li><code>SECOND</code></li>
<li><code>MINUTE</code></li>
<li><code>HOUR</code>. Equivalent to 60 <code>MINUTE</code>s.</li>
<li><code>DAY</code>. Equivalent to 24 <code>HOUR</code>s.</li>
</ul>

**Return Data Types**

TIMESTAMP

**Example**

```sql
SELECT
  TIMESTAMP "2008-12-25 15:30:00 UTC" as original,
  TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00 UTC", INTERVAL 10 MINUTE) AS later;

+------------------------+------------------------+
| original               | later                  |
+------------------------+------------------------+
| 2008-12-25 15:30:00+00 | 2008-12-25 15:40:00+00 |
+------------------------+------------------------+
```

#### TIMESTAMP_SUB

```sql
TIMESTAMP_SUB(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts `int64_expression` units of `date_part` from the timestamp,
independent of any time zone.

`TIMESTAMP_SUB` supports the following values for `date_part`:

<ul>
<li><code>NANOSECOND</code></li>
<li><code>MICROSECOND</code></li>
<li><code>MILLISECOND</code></li>
<li><code>SECOND</code></li>
<li><code>MINUTE</code></li>
<li><code>HOUR</code>. Equivalent to 60 <code>MINUTE</code>s.</li>
<li><code>DAY</code>. Equivalent to 24 <code>HOUR</code>s.</li>
</ul>

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT
  TIMESTAMP "2008-12-25 15:30:00 UTC" as original,
  TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00 UTC", INTERVAL 10 MINUTE) AS earlier;

+------------------------+------------------------+
| original               | earlier                |
+------------------------+------------------------+
| 2008-12-25 15:30:00+00 | 2008-12-25 15:20:00+00 |
+------------------------+------------------------+
```

#### TIMESTAMP_DIFF

```sql
TIMESTAMP_DIFF(timestamp_expression, timestamp_expression, date_part)
```

**Description**

<div>
    <p>
        Returns the number of whole specified <code>date_part</code> intervals
        between two timestamps. The first <code>timestamp_expression</code>
        represents the later date; if the first
        <code>timestamp_expression</code> is earlier than the second
        <code>timestamp_expression</code>, the output is negative.
        Throws an error if the computation overflows the result type, such as
        if the difference in nanoseconds between the two timestamps
        would overflow an <code>INT64</code> value.
    </p>
</div>

`TIMESTAMP_DIFF` supports the following values for `date_part`:

<ul>
<li><code>NANOSECOND</code></li>
<li><code>MICROSECOND</code></li>
<li><code>MILLISECOND</code></li>
<li><code>SECOND</code></li>
<li><code>MINUTE</code></li>
<li><code>HOUR</code>. Equivalent to 60 <code>MINUTE</code>s.</li>
<li><code>DAY</code>. Equivalent to 24 <code>HOUR</code>s.</li>
</ul>

**Return Data Type**

INT64

**Example**

```sql
SELECT
  TIMESTAMP "2010-07-07 10:20:00 UTC" as later_timestamp,
  TIMESTAMP "2008-12-25 15:30:00 UTC" as earlier_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00 UTC",
    TIMESTAMP "2008-12-25 15:30:00 UTC", HOUR) AS hours;

+------------------------+------------------------+-------+
| later_timestamp        | earlier_timestamp      | hours |
+------------------------+------------------------+-------+
| 2010-07-07 10:20:00+00 | 2008-12-25 15:30:00+00 | 13410 |
+------------------------+------------------------+-------+
```

In the following example, the first timestamp occurs before the second
timestamp, resulting in a negative output.

```sql
SELECT TIMESTAMP_DIFF(TIMESTAMP "2018-08-14", TIMESTAMP "2018-10-14", DAY);

+---------------+
| negative_diff |
+---------------+
| -61           |
+---------------+
```

#### TIMESTAMP_TRUNC

```sql
TIMESTAMP_TRUNC(timestamp_expression, date_part[, time_zone])
```

**Description**

Truncates a timestamp to the granularity of `date_part`.

`TIMESTAMP_TRUNC` supports the following values for `date_part`:

+ `NANOSECOND`
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>):` Truncates `timestamp_expression` to the preceding
  week boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Truncates `timestamp_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Truncates `timestamp_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

`TIMESTAMP_TRUNC` function supports an optional `time_zone` parameter. This
parameter applies to the following `date_parts`:

+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`
+ `MONTH`
+ `QUARTER`
+ `YEAR`

Use this parameter if you want to use a time zone other than the
default time zone, which is implementation defined, as part of the
truncate operation.

When truncating a `TIMESTAMP` to `MINUTE`
or`HOUR`, `TIMESTAMP_TRUNC` determines the civil time of the
`TIMESTAMP` in the specified (or default) time zone
and subtracts the minutes and seconds (when truncating to HOUR) or the seconds
(when truncating to MINUTE) from that `TIMESTAMP`.
While this provides intuitive results in most cases, the result is
non-intuitive near daylight savings transitions that are not hour aligned.

**Return Data Type**

TIMESTAMP

**Examples**

```sql
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP '2008-12-25 15:30:00', DAY, 'UTC') as utc,
  TIMESTAMP_TRUNC(TIMESTAMP '2008-12-25 15:30:00', DAY, 'America/Los_Angeles') as la;

+------------------------+------------------------+
| utc                    | la                     |
+------------------------+------------------------+
| 2008-12-25 00:00:00+00 | 2008-12-25 08:00:00+00 |
+------------------------+------------------------+
```

In the following example, `timestamp_expression` has a time zone offset of +12.
The first column shows the `timestamp_expression` in UTC time. The second
column shows the output of `TIMESTAMP_TRUNC` using weeks that start on Monday.
Because the `timestamp_expression` falls on a Sunday in UTC, `TIMESTAMP_TRUNC`
truncates it to the preceding Monday. The third column shows the same function
with the optional [Timezone definition][timestamp-link-to-timezone-definitions]
argument 'Pacific/Auckland'. Here the function truncates the
`timestamp_expression` using New Zealand Daylight Time, where it falls on a
Monday.

```sql
SELECT
  timestamp,
  TIMESTAMP_TRUNC(timestamp, WEEK(MONDAY)) AS utc_truncated,
  TIMESTAMP_TRUNC(timestamp, WEEK(MONDAY), 'Pacific/Auckland') AS nzdt_truncated
FROM (SELECT TIMESTAMP('2017-11-06 00:00:00+12') AS timestamp);

+------------------------+------------------------+------------------------+
| timestamp              | utc_truncated          | nzdt_truncated         |
+------------------------+------------------------+------------------------+
| 2017-11-05 12:00:00+00 | 2017-10-30 00:00:00+00 | 2017-11-05 11:00:00+00 |
+------------------------+------------------------+------------------------+
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
  TIMESTAMP_TRUNC('2015-06-15 00:00:00+00', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM TIMESTAMP '2015-06-15 00:00:00+00') AS isoyear_number;

+------------------------+----------------+
| isoyear_boundary       | isoyear_number |
+------------------------+----------------+
| 2014-12-29 00:00:00+00 | 2015           |
+------------------------+----------------+
```

#### FORMAT_TIMESTAMP

```sql
FORMAT_TIMESTAMP(format_string, timestamp[, time_zone])
```

**Description**

Formats a timestamp according to the specified `format_string`.

See [Supported Format Elements For TIMESTAMP][timestamp-link-to-supported-format-elements-for-time-for-timestamp]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Example**

```sql
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:30:00", "America/Los_Angeles")
  AS formatted;

+--------------------------+
| formatted                |
+--------------------------+
| Thu Dec 25 07:30:00 2008 |
+--------------------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

#### PARSE_TIMESTAMP

```sql
PARSE_TIMESTAMP(format_string, string[, time_zone])
```

**Description**

Uses a `format_string` and a string representation of a timestamp to return a
TIMESTAMP object.

When using `PARSE_TIMESTAMP`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01
00:00:00.0`. This initialization value uses the time zone specified by the
function's time zone argument, if present. If not, the initialization value uses
the default time zone, which is implementation defined.  For instance, if the year
is unspecified then it defaults to `1970`, and so on.
+ **Case insensitive names.** Names, such as `Monday`, `February`, and so on, are
case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
matches zero or more consecutive white spaces in the timestamp string. In
addition, leading and trailing white spaces in the timestamp string are always
allowed -- even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
information (for example both `%F` and `%Y` affect the year), the last one
generally overrides any earlier ones, with some exceptions (see the descriptions
of `%s`, `%C`, and `%y`).

Note: This function supports [format elements][timestamp-link-to-supported-format-elements-for-time-for-timestamp],
but does not have full support for `%Q`, `%a`, `%A`, `%g`, `%G`, `%j`, `%u`, `%U`, `%V`, `%w`, and `%W`.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008", "America/Los_Angeles") as parsed;

+------------------------+
| parsed                 |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### TIMESTAMP_SECONDS

```sql
TIMESTAMP_SECONDS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since 1970-01-01 00:00:00
UTC.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_SECONDS(1230219000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### TIMESTAMP_MILLIS

```sql
TIMESTAMP_MILLIS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since 1970-01-01
00:00:00 UTC.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_MILLIS(1230219000000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### TIMESTAMP_MICROS

```sql
TIMESTAMP_MICROS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since 1970-01-01
00:00:00 UTC.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_MICROS(1230219000000000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### UNIX_SECONDS

```sql
UNIX_SECONDS(timestamp_expression)
```

**Description**

Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher
levels of precision.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00 UTC") as seconds;

+------------+
| seconds    |
+------------+
| 1230219000 |
+------------+
```

#### UNIX_MILLIS

```sql
UNIX_MILLIS(timestamp_expression)
```

**Description**

Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates
higher levels of precision.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00 UTC") as millis;

+---------------+
| millis        |
+---------------+
| 1230219000000 |
+---------------+
```

#### UNIX_MICROS

```sql
UNIX_MICROS(timestamp_expression)
```

**Description**

Returns the number of microseconds since 1970-01-01 00:00:00 UTC. Truncates
higher levels of precision.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00 UTC") as micros;

+------------------+
| micros           |
+------------------+
| 1230219000000000 |
+------------------+
```

#### TIMESTAMP_FROM_UNIX_SECONDS

```sql
TIMESTAMP_FROM_UNIX_SECONDS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since
1970-01-01 00:00:00 UTC and creates a timestamp.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_SECONDS(1230219000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### TIMESTAMP_FROM_UNIX_MILLIS

```sql
TIMESTAMP_FROM_UNIX_MILLIS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since
1970-01-01 00:00:00 UTC and creates a timestamp.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MILLIS(1230219000000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### TIMESTAMP_FROM_UNIX_MICROS

```sql
TIMESTAMP_FROM_UNIX_MICROS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since
1970-01-01 00:00:00 UTC and creates a timestamp.

**Return Data Type**

TIMESTAMP

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MICROS(1230219000000000) as timestamp;

+------------------------+
| timestamp              |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

#### Supported format elements for TIMESTAMP

Unless otherwise noted, TIMESTAMP functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal
number (00-99).</td>
 </tr>
 <tr>
    <td>%c</td>
    <td>The date and time representation in the format %a %b %e %T %Y.</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The
    <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The
    <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
 </tr>
 <tr>
    <td>%s</td>
    <td>The number of seconds since 1970-01-01 00:00:00 UTC. Always overrides all
    other format elements, independent of where %s appears in the string.
    If multiple %s elements appear, then the last one takes precedence.</td>
</tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
</tr>
 <tr>
    <td>%V</td>
   <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
 </tr>
 <tr>
    <td>%Z</td>
    <td>The time zone name.</td>
 </tr>
 <tr>
    <td>%z</td>
    <td>The offset from the Prime Meridian in the format +HHMM or -HHMM as
    appropriate,
with positive values representing locations east of Greenwich.</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
 </tr>
 <tr>
    <td>%Ez</td>
    <td>RFC 3339-compatible numeric time zone (+HH:MM or -HH:MM).</td>
 </tr>
 <tr>
    <td>%E#S</td>
    <td>Seconds with # digits of fractional precision.</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y
    produces as many characters as it takes to fully render the
year.</td>
 </tr>
</table>

#### Timezone definitions

Certain date and timestamp functions allow you to override the default time zone
and specify a different one. You can specify a time zone by either supplying
the [time zone name][timezone-by-name] (for example, `America/Los_Angeles`)
or time zone offset from UTC (for example, -08).

If you choose to use a time zone offset, use this format:

```
(+|-)H[H][:M[M]]
```

The following timestamps are equivalent because the time zone offset
for `America/Los_Angeles` is `-08` for the specified date and time.

```
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00 America/Los_Angeles") as millis;
```

```
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00-08:00") as millis;
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601
[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date
[timezone-by-name]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[timestamp-link-to-timezone-definitions]: #timezone_definitions
[timestamp-link-to-supported-format-elements-for-time-for-timestamp]: #supported_format_elements_for_timestamp

## Protocol buffer functions

ZetaSQL supports the following protocol buffer functions.

#### PROTO_DEFAULT_IF_NULL
```
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
SELECT PROTO_DEFAULT_IF_NULL(book.country) as origin FROM library_books;
```

`Book` is a type that contains a field called `country`.

```
message Book {
  optional string country = 4 [default = 'Unknown'];
}
```

This is the result if `book.country` evaluates to `Canada`.

```sql
+-----------------+
| origin          |
+-----------------+
| Canada          |
+-----------------+
```

This is the result if `book` is `NULL`. Since `book` is `NULL`,
`book.country` evaluates to `NULL` and therefore the function result is the
default value for `country`.

```sql
+-----------------+
| origin          |
+-----------------+
| Unknown         |
+-----------------+
```

#### FROM PROTO

```
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

+------------+
| $col1      |
+------------+
| 2019-10-30 |
+------------+
```

Pass in and return a `DATE` type.

```sql
SELECT FROM_PROTO(DATE '2019-10-30')

+------------+
| $col1      |
+------------+
| 2019-10-30 |
+------------+
```

#### TO PROTO

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

+--------------------------------+
| $col1                          |
+--------------------------------+
| {year: 2019 month: 10 day: 30} |
+--------------------------------+
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

+--------------------------------+
| $col1                          |
+--------------------------------+
| {year: 2019 month: 10 day: 30} |
+--------------------------------+
```

#### EXTRACT {#proto_extract}

```sql
EXTRACT( extraction_type (field) FROM proto_expression )

extraction_type:
  { FIELD | RAW | HAS }
```

**Description**

Extracts a value from a proto. `proto_expression` represents the expression
that returns a proto, `field` represents the field of the proto to extract from,
and `extraction_type` determines the type of data to return. `EXTRACT` can be
used to get values of ambiguous fields. An alternative to `EXTRACT` is the
[dot operator][querying-protocol-buffers].

**Extraction Types**

You can choose the type of information to get with `EXTRACT`. Your choices are:

+  `FIELD`: Extract a value from a field.
+  `RAW`: Extract an uninterpreted value from a field.
   Raw values ignore any ZetaSQL type annotations.
+  `HAS`: Returns `true` if a field is set in a proto message;
   otherwise, `false`. Returns an error if this is used with a scalar proto3
   field. Alternatively, use [`has_x`][has-value], to perform this task.

**Return Type**

The return type depends upon the extraction type in the query.

+  `FIELD`: Type of proto field.
+  `RAW`: Type of proto field, ignoring format annotations if present.
+  `HAS`: `BOOL`

**Examples**

Extract the year from a proto called `Date`.

```sql
SELECT EXTRACT(FIELD(year) FROM new google.type.Date(
    2019 as year,
    10 as month,
    30 as day
  )
) as year;

+------------------+
| year             |
+------------------+
| 2019             |
+------------------+
```

Set up a proto2 called `Book`.

```sql
message Book {
  optional int32 publish_date = 1 [ (zetasql.format) = DATE ];
}
```

Extract `publish_date` from a proto called `Book`.

```sql
SELECT EXTRACT(FIELD(publish_date) FROM new Book(
    ‘1970-05-04’ as publish_date,
  )
) as release_date;

+------------------+
| release_date     |
+------------------+
| 1970-05-04       |
+------------------+
```

Extract the uninterpreted `publish_date` from a proto called `Book`.
In this example, the uninterpreted value is the number of days between
1970-01-01 and 1970-05-04.

```sql
SELECT EXTRACT(RAW(publish_date) FROM new Book(
    ‘1970-05-04’ as publish_date,
  )
) as release_date;

+------------------+
| release_date     |
+------------------+
| 123              |
+------------------+
```

Check to see if `publish_date` is set in a proto2 called `Book`.
In this example, `publish_date` is set to 1970-05-04.

```sql
SELECT EXTRACT(HAS(publish_date) FROM new Book(
    ‘1970-05-04’ as publish_date,
  )
) as has_release_date;

+------------------+
| has_release_date |
+------------------+
| true             |
+------------------+
```

Check to see if `publish_date` is set in a proto2 called `Book`.
In this example, `publish_date` is not set.

```sql
SELECT EXTRACT(HAS(publish_date) FROM new Book()) as has_release_date;

+------------------+
| has_release_date |
+------------------+
| false            |
+------------------+
```

[querying-protocol-buffers]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers#querying-protocol-buffers
[has-value]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers#checking-if-a-non-repeated-field-has-a-value

[querying-protocol-buffers]: #querying-protocol-buffers
[has-value]: #checking-if-a-non-repeated-field-has-a-value

## Security functions

ZetaSQL supports the following security functions.

#### SESSION_USER
```
SESSION_USER()
```

**Description**

Returns the email address of the user that is running the query.

**Return Data Type**

STRING

**Example**

```sql
SELECT SESSION_USER() as user;

+----------------------+
| user                 |
+----------------------+
| jdoe@example.com     |
+----------------------+

```

## Net functions

#### NET.IP_FROM_STRING

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
```

| addr_str                                | ip_from_string                                                      |
|-----------------------------------------|---------------------------------------------------------------------|
| 48.49.50.51                             | b"0123"                                                             |
| ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
| 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
| ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |

#### NET.SAFE_IP_FROM_STRING

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
```

| addr_str                                | safe_ip_from_string                                                 |
|-----------------------------------------|---------------------------------------------------------------------|
| 48.49.50.51                             | b"0123"                                                             |
| ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
| 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
| ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |
| 48.49.50.51/32                          | NULL                                                                |
| 48.49.50                                | NULL                                                                |
| ::wxyz                                  | NULL                                                                |

#### NET.IP_TO_STRING

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
```

| addr_bin                                                            | ip_to_string                            |
|---------------------------------------------------------------------|-----------------------------------------|
| b"0123"                                                             | 48.49.50.51                             |
| b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" | ::1                                     |
| b"0123456789@ABCDE"                                                 | 3031:3233:3435:3637:3839:4041:4243:4445 |
| b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" | ::ffff:192.0.2.128                      |

#### NET.IP_NET_MASK

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
```

| x  | y   | ip_net_mask                                                         |
|----|-----|---------------------------------------------------------------------|
| 4  | 0   | b"\x00\x00\x00\x00"                                                 |
| 4  | 20  | b"\xff\xff\xf0\x00"                                                 |
| 4  | 32  | b"\xff\xff\xff\xff"                                                 |
| 16 | 0   | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
| 16 | 1   | b"\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
| 16 | 128 | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" |

#### NET.IP_TRUNC

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
```

| addr_bin            | prefix_length | ip_trunc                              |
|---------------------|---------------|---------------------------------------|
| b"\xaa\xbb\xcc\xdd" | 0             | b"\x00\x00\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 11            | b"\xaa\xa0\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 12            | b"\xaa\xb0\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 24            | b"\xaa\xbb\xcc\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 32            | b"\xaa\xbb\xcc\xdd"                   |
| b"0123456789@ABCDE" | 80            | b"0123456789\x00\x00\x00\x00\x00\x00" |

#### NET.IPV4_FROM_INT64

```
NET.IPV4_FROM_INT64(integer_value)
```

**Description**

Converts an IPv4 address from integer format to binary (BYTES) format in network
byte order. In the integer input, the least significant bit of the IP address is
stored in the least significant bit of the integer, regardless of host or client
architecture. For example, `1` means `0.0.0.1`, and `0x1FF` means `0.0.1.255`.

This function checks that either all the most significant 32 bits are 0, or all
the most significant 33 bits are 1 (sign-extended from a 32 bit integer).
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
```

| x          | x_hex      | ipv4_from_int64     |
|------------|------------|---------------------|
| 0          | 0x0        | b"\x00\x00\x00\x00" |
| 11259375   | 0xABCDEF   | b"\x00\xab\xcd\xef" |
| 4294967295 | 0xFFFFFFFF | b"\xff\xff\xff\xff" |
| -1         | -0x1       | b"\xff\xff\xff\xff" |
| -2         | -0x2       | b"\xff\xff\xff\xfe" |

#### NET.IPV4_TO_INT64

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
```

| addr_bin            | ipv4_to_int64 |
|---------------------|---------------|
| b"\x00\x00\x00\x00" | 0x0           |
| b"\x00\xab\xcd\xef" | 0xABCDEF      |
| b"\xff\xff\xff\xff" | 0xFFFFFFFF    |

#### NET.FORMAT_IP (DEPRECATED)
```
NET.FORMAT_IP(integer)
```

**Description**

This function is deprecated. It is the same as
[`NET.IP_TO_STRING`][net-link-to-ip-to-string]`(`[`NET.IPV4_FROM_INT64`][net-link-to-ipv4-from-int64]`(integer))`,
except that this function does not allow negative input values.

**Return Data Type**

STRING

#### NET.PARSE_IP (DEPRECATED)
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

#### NET.FORMAT_PACKED_IP (DEPRECATED)
```
NET.FORMAT_PACKED_IP(bytes_value)
```

**Description**

This function is deprecated. It is the same as [`NET.IP_TO_STRING`][net-link-to-ip-to-string].

**Return Data Type**

STRING

#### NET.PARSE_PACKED_IP (DEPRECATED)
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

#### NET.IP_IN_NET
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

#### NET.MAKE_NET
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

#### NET.HOST
```
NET.HOST(url)
```

**Description**

Takes a URL as a STRING and returns the host as a STRING. For best results, URL
values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply with RFC 3986 formatting,
this function makes a best effort to parse the input and return a relevant
result. If the function cannot parse the input, it
returns NULL.

<p class="note"><b>Note:</b> The function does not perform any normalization.</a>
</p>

**Return Data Type**

STRING

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

#### NET.PUBLIC_SUFFIX
```
NET.PUBLIC_SUFFIX(url)
```

**Description**

Takes a URL as a STRING and returns the public suffix (such as `com`, `org`,
or `net`) as a STRING. A public suffix is an ICANN domain registered at
[publicsuffix.org][net-link-to-public-suffix]. For best results, URL values
should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply
with RFC 3986 formatting, this function makes a best effort to parse the input
and return a relevant result.

This function returns NULL if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle (not leading or trailing);
+ The parsed host does not contain any public suffix.

Before looking up the public suffix, this function temporarily normalizes the
host by converting upper case English letters to lower case and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode].
The function then returns the public suffix as part of the original host instead
of the normalized host.

<p class="note"><b>Note:</b> The function does not perform
<a href="https://en.wikipedia.org/wiki/Unicode_equivalence">Unicode normalization</a>.
</p>

<p class="note"><b>Note:</b> The public suffix data at
<a href="https://publicsuffix.org/list/">publicsuffix.org</a> also contains
private domains. This function ignores the private domains.</p>

<p class="note"><b>Note:</b> The public suffix data may change over time.
Consequently, input that produces a NULL result now may produce a non-NULL value
in the future.</p>

**Return Data Type**

STRING

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

#### NET.REG_DOMAIN
```
NET.REG_DOMAIN(url)
```

**Description**

Takes a URL as a STRING and returns the registered or registerable domain (the
[public suffix](#netpublic_suffix) plus one preceding label), as a
STRING. For best results, URL values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply with RFC 3986 formatting,
this function makes a best effort to parse the input and return a relevant
result.

This function returns NULL if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle (not leading or trailing);
+ The parsed host does not contain any public suffix;
+ The parsed host contains only a public suffix without any preceding label.

Before looking up the public suffix, this function temporarily normalizes the
host by converting upper case English letters to lowercase and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode]. The function then returns
the registered or registerable domain as part of the original host instead of
the normalized host.

<p class="note"><b>Note:</b> The function does not perform
<a href="https://en.wikipedia.org/wiki/Unicode_equivalence">Unicode normalization</a>.
</p>

<p class="note"><b>Note:</b> The public suffix data at
<a href="https://publicsuffix.org/list/">publicsuffix.org</a> also contains
private domains. This function does not treat a private domain as a public
suffix. For example, if "us.com" is a private domain in the public suffix data,
NET.REG_DOMAIN("foo.us.com") returns "us.com" (the public suffix "com" plus
the preceding label "us") rather than "foo.us.com" (the private domain "us.com"
plus the preceding label "foo").
</p>

<p class="note"><b>Note:</b> The public suffix data may change over time.
Consequently, input that produces a NULL result now may produce a non-NULL value
in the future.</p>

**Return Data Type**

STRING

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

[net-link-to-ipv6-rfc]: http://www.ietf.org/rfc/rfc2373.txt
[net-link-to-cidr-notation]: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing
[net-link-to-rfc-3986-appendix-a]: https://tools.ietf.org/html/rfc3986#appendix-A
[net-link-to-public-suffix]: https://publicsuffix.org/list/
[net-link-to-punycode]: https://en.wikipedia.org/wiki/Punycode

[net-link-to-ip-from-string]: #netip-from-string
[net-link-to-ip-to-string]: #netip-to-string
[net-link-to-ipv4-from-int64]: #netipv4-from-int64
[net-link-to-ipv4-to-int64]: #netipv4-to-int64

<!-- Statements -->
<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Data Definition Language statements

<!-- BEGIN CONTENT -->

ZetaSQL specifies the syntax for Data Definition Language (DDL)
statements.

Where possible, this topic provides a link to the product-specific documentation
for each statement.

### CREATE DATABASE

<pre>
CREATE
  DATABASE
  database_name
  [OPTIONS (key=value, ...)]
</pre>

**Description**

The `CREATE DATABASE` statement creates a database. If you have schema
options, you can add them when you create the database. These options are
system-specific and follow the ZetaSQL
[`HINT` syntax][hints].

**Example**

```sql
CREATE DATABASE library OPTIONS(
  base_dir=`/city/orgs`,
  owner='libadmin'
);

+--------------------+
| Database           |
+--------------------+
| library            |
+--------------------+
```

### CREATE TABLE

<pre>
CREATE
   [ OR REPLACE ]
   [ TEMP | TEMPORARY ]
   TABLE
   [ IF NOT EXISTS ]
   table_name [ ( <span class="var">table_element</span>, ... ) ]
   [ PARTITION [ hints ] BY partition_expression, ... ]
   [ CLUSTER [ hints ] BY cluster_expression, ... ]
   [ OPTIONS (key=value, ...) ]
   [ AS query ];

<span class="var">table_element:</span>
   <span class="var"><a href="#defining-columns">column_definition</a></span> | <span class="var"><a href="#defining-table-constraints">constraint_definition</a></span>
</pre>

**Description**

The `CREATE TABLE` statement creates a table and adds any table elements
defined in the table element list `(table_element, ...)`. If the `AS query`
clause is absent, the table element list must be present and contain at
least one column definition. A table element can be a column definition
or constraint definition. To learn more about column definitions, see
[Defining Columns][defining-columns]. To learn more about constraint
definitions, see [Defining Constraints][defining-constraints].

In a table, if both the table element list and the `AS query` clause
are present, then the number of columns in the table element list must
match the number of columns selected in `query`, and the type of each
column selected in `query` must be assignable to the column type in the
corresponding position of the table element list.

**Optional Clauses**

+  `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
   appear with `IF NOT EXISTS`.
+  `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
   system-specific.
+  `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
   statement will have no effect. Cannot appear with `OR REPLACE`.
+  `PARTITION BY`: Creates partitions of a table. The expression cannot
   contain floating point types, non-groupable types, constants,
   aggregate functions, or analytic functions.
+  `CLUSTER BY`: Co-locates rows if the rows are not distinct for the values
   produced by the provided list of expressions.
   If the table was partitioned, co-location occurs within each partition.
   If the table was not partitioned, co-location occurs within the table.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the table. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]
+  `AS query`: Materializes the result of `query` into the new table.

**Examples**

Create a table.

```sql
CREATE TABLE books (title STRING, author STRING);
```

Create a table in a schema called `library`.

```sql
CREATE TABLE library.books (title STRING, author STRING);
```

Create a table that contains schema options.

```sql
CREATE TABLE books (title STRING, author STRING) OPTIONS (storage_kind=FLASH_MEMORY);
```

Partition a table.

```sql
CREATE TABLE books (title STRING, author STRING, publisher STRING, release_date DATE)
PARTITION BY publisher, author;
```

Partition a table with a pseudocolumn. In the example below, SYSDATE represents
the date when the book was added to the database. Replace SYSDATE with a
psuedocolumn supported by your SQL service.

```sql
CREATE TABLE books (title STRING, author STRING)
PARTITION BY sysdate;
```

Cluster a table.

```sql
CREATE TABLE books (
  title STRING,
  first_name STRING,
  last_name STRING
)
CLUSTER BY last_name, first_name;
```

#### Defining Columns

<pre>
<span class="var">column_definition:</span>
   column_name
   [ column_type ]
   [ <span class="var">generation_clause</span> ]
   [ <span class="var">column_attribute</span>, ... ]
   [ OPTIONS (...) ]

<span class="var">column_attribute:</span>
  PRIMARY KEY
  | NOT NULL
  | HIDDEN
  | [ CONSTRAINT constraint_name ] <span class="var"><a href="#defining-foreign-references">foreign_reference</a></span>

<span class="var">generation_clause:</span>
  AS generation_expression
</pre>

**Description**

A column exists within a table. Each column describes one attribute of the
rows that belong to the table.

+  The name of a column must be unique within a table.
+  Columns in a table are ordered. The order will have consequences on
   some sorts of SQL statements such as `SELECT * FROM Table` and
   `INSERT INTO Table VALUES (...)`.
+  A column can be generated. A generated column is a column in a base
   table whose value is defined as a function of other columns in the
   same row. To use a generated column, include the `generation_clause`
   in the column definition.
+  If a column is not generated, `column_type` is required.

**Attribute Kinds**

+  `PRIMARY KEY`: Defines this column as the [primary key][primary-key]
   of the table.
+  `NOT NULL`: A value on a column cannot be null. More specifically, this is
   shorthand for a constraint of the shape `CHECK [column_name] IS NOT NULL`.
+  `HIDDEN`: Hides a column if it should not appear in `SELECT * expansions`
   or in structifications of a row variable, such as `SELECT t FROM Table t`.
+  `[ CONSTRAINT constraint_name ] foreign_reference`: The column in a
   [foreign table to reference][defining-foreign-reference].
   You can optionally name the constraint.
   A constraint name must be unique within its schema; it cannot share the name
   of other constraints.
   If a constraint name is not provided, one is generated by the
   implementing engine. Users can use INFORMATION_SCHEMA to look up
   the generated names of table constraints.

**Optional Clauses**

+  `column_type`: The  data type of the column.
    This is optional for generated columns, but is required for non-generated
    columns.
+  `generation_clause`: The function that describes a generation expression.
   A generation expression must be a scalar expression. Subqueries are not
   allowed.
+  `column_attribute`: A characteristic of the column.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the column. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]

**Examples**

Create a table with a primary key that can't be null.

```sql
CREATE TABLE books (title STRING, author STRING, isbn INT64 PRIMARY KEY NOT NULL);
```

Create a table with a generated column. In this example,
the generated column holds the first and last name of an author.

```sql
CREATE TABLE authors(
  firstName STRING HIDDEN,
  lastName STRING HIDDEN,
  fullName STRING CONCAT(firstName, " ", lastName)
);
```

Create a table that contains schema options on column definitions.

```sql
CREATE TABLE books (
  title STRING NOT NULL PRIMARY KEY,
  author STRING
      OPTIONS (is_deprecated=true, comment="Replaced by authorId column"),
  authorId INT64 REFERENCES authors (id),
  category STRING OPTIONS (description="LCC Subclass")
)
```

#### Defining Table Constraints

<pre>
<span class="var">constraint_definition:</span>
   <span class="var">primary_key</span>
   | <span class="var">foreign_key</span>
   | <span class="var">check_constraint</span>

<span class="var">primary_key:</span>
  PRIMARY KEY (column_name [ ASC | DESC ], ... )
  [ OPTIONS (...) ]

<span class="var">foreign_key:</span>
  [ CONSTRAINT constraint_name ]
  FOREIGN KEY (column_name, ... )
  <span class="var"><a href="#defining-foreign-references">foreign_reference</a></span>
  [ OPTIONS (...) ]

<span class="var">check_constraint:</span>
  [ CONSTRAINT constraint_name ]
  CHECK ( boolean_expression )
  [ ENFORCED | NOT ENFORCED ]
  [ OPTIONS (...) ]
</pre>

**Description**

A `constraint_definition` is a rule enforced on the columns of a table.

**Constraint Kinds**

+  `primary_key`: Defines the [primary key][primary-key] for a table.
+  `foreign_key`:  Defines a foreign key for a table. A foreign key links
   two tables together.
+  `check_constraint`: Restricts the data that can be added to certain
   columns used by the expressions of the constraints.

**Optional Clauses**

+  `CONSTRAINT`: Names the constraint.
   A constraint name must be unique within its schema; it cannot share the name
   of other constraints.
   If a constraint name is not provided, one is generated by the
   implementing engine. Users can use INFORMATION_SCHEMA to look up
   the generated names of table constraints.
+  `ASC | DESC`: Specifies that the engine should optimize reading the
   index records in ascending or descending order by this key part.
   `ASC` is the default. Each key part will be sorted with respect to
   the sort defined by any key parts to the left.
+  `ENFORCED | NOT ENFORCED`: Specifies whether or not the constraint
   is enforced.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the constraint. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]

**Examples**

Create a primary key constraint, using the `title` and `author` columns
in a table called `books`.

```sql
CREATE TABLE books (title STRING, author STRING, PRIMARY KEY (title ASC, author ASC));
```

Create a foreign key constraint. When data in the `top_authors` table
is updated or deleted, make the same change in the `authors` table.

```sql
CREATE TABLE top_authors (
  author_first_name STRING,
  author_last_name STRING,
  CONSTRAINT fk_top_authors_name
    FOREIGN KEY (author_first_name, author_last_name)
    REFERENCES authors (first_name, last_name)
);
```

Create a check constraint. A row that contains values for `words_per_chapter`
and `words_per_book` can only only be inserted into the `page_count_average`
table if the `words_per_chapter` value is less than the `words_per_book` value.

```sql
CREATE TABLE page_count_average (
  words_per_chapter INT64,
  words_per_book INT64,
  CHECK (words_per_chapter < words_per_book)
);
```

#### Defining Foreign References

<pre>
<span class="var">foreign_reference:</span>
  REFERENCES table_name (column_name, ... )
  [ MATCH { SIMPLE | FULL | NOT DISTINCT } ]
  [ ON UPDATE <span class="var">referential_action</span> ]
  [ ON DELETE <span class="var">referential_action</span> ]
  [ ENFORCED | NOT ENFORCED ]

<span class="var">referential_action:</span>
  NO ACTION | RESTRICT | CASCADE | SET NULL
</pre>

**Description**

A foreign key is used to define a relationship between the rows in two tables.
The foreign key in one table can reference one or more columns in
another table. A foreign reference can be used to assign constraints
to a foreign key and to give a foreign key a unique name.

**Optional Clauses**

+  `MATCH`: Specifies when a constraint validation for a referencing row
   passes or fails. Your choices are:
   +  `SIMPLE`:
      +  Passes if *any column* of the local key is `NULL`.
      +  Passes if the columns of the local key are pairwise-equal to the
         columns of the referenced key for some row of the referenced table.
   +  `FULL`:
      +  Passes if *all columns* of the local key are `NULL`.
      +  Passes if the columns of the local key are pairwise-equal to the
         columns of the referenced key for some row of the referenced table.
   +  `NOT DISTINCT`:
      +  Passes if the columns of the local key are pairwise-not-distinct from
         the columns of the referenced key for some row of the referenced table.
+  `ON UPDATE`: If data in the referenced table updates, your choices are:
   +  `RESTRICT`: Fail the transaction.
   +  `NO ACTION`: Don't update the referencing table. If the data in the
      referencing table does not satisfy the constraint before it is
      checked, the transaction will fail.
   +  `CASCADE`: For each updated row in the referenced table,
      update all matching rows in the referencing table so that
      they continue to match the same row after the transaction.
   +  `SET NULL`: Any change to a referenced column in the referenced table
      causes the corresponding referencing column in matching rows of the
      referencing table to be set to null.
+  `ON DELETE`: If data in the referenced table is deleted, your choices are:
   +  `RESTRICT`: Fail the transaction.
   +  `NO ACTION`: Don't delete the data in the referencing table. If the
       data in the referencing table does not satisfy the constraint before it
       is checked, the transaction will fail.
   +  `CASCADE`: For each deleted row in the referenced table,
      delete all matching rows in the referencing table so that
      they continue to match the same row after the transaction.
   +  `SET NULL`: If a row of the referenced table is deleted, then all
       referencing columns in all matching rows of the referencing table
       are set to null.
+  `ENFORCED | NOT ENFORCED`: Specifies whether or not the constraint
   is enforced.

**Examples**

When data in the `top_books` table is updated or deleted, make the
same change in the `books` table.

```sql
CREATE TABLE top_books (
  book_name STRING,
  CONSTRAINT fk_top_books_name
    FOREIGN KEY (book_name)
    REFERENCES books (title)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);
```

#### Using CREATE TABLE AS

```
CREATE TABLE table_definition AS query
```

**Description**

The `CREATE TABLE AS` statement creates a table and materializes the result
of a query into this new table. To learn more about the syntax for this
statement, see [`CREATE TABLE`][create-table].

**Examples**

Copy all rows from a table called `old_books` to a new table called
`books`.

```sql
CREATE TABLE books AS (SELECT * FROM old_books);
```

Copy rows from a table called `old_books` to a new table called `books`.
If a book was published before 1900, do not add it to `books`.

```sql
CREATE TABLE books
AS (SELECT * FROM old_books where year >= 1900)
```

Copy rows from a table called `old_books` to a new table called `books`.
Only copy the `title` column into `books`.

```sql
CREATE TABLE books AS (SELECT title FROM old_books);
```

Copy rows from `old_books` and `ancient_books` to a new table called
`books`.

```sql
CREATE TABLE books AS (
  SELECT * FROM old_books UNION ALL
  SELECT * FROM ancient_books);
```

### CREATE VIEW

```
CREATE
  [OR REPLACE]
  [TEMP | TEMPORARY]
  VIEW
  [IF NOT EXISTS]
  view_name
  [OPTIONS (key=value, ...)]
AS query;
```

**Description**

The `CREATE VIEW` statement creates a view based on a specific query.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    system-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.

### CREATE EXTERNAL TABLE

```
CREATE
  [OR REPLACE]
  [TEMP | TEMPORARY]
  EXTERNAL TABLE
  [IF NOT EXISTS]
  table_name
  [OPTIONS (key=value, ...)];
```

**Description**

The `CREATE EXTERNAL TABLE` creates a table from external data. `CREATE EXTERNAL
TABLE` also supports creating persistent definitions.

The `CREATE EXTERNAL TABLE` does not build a new table; instead, it creates a
pointer to data that exists outside of the database.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    system-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.

### CREATE INDEX

```
CREATE
  [OR REPLACE]
  [UNIQUE]
  INDEX
  [IF NOT EXISTS]
  index_name
  ON
  table_name [[AS] alias]
  [UNNEST(array_expression) [[AS] alias] [WITH OFFSET [[AS] alias]] ...]
  (key_expression [ASC|DESC], ...)
  [STORING (stored_expression, ...)]
  [OPTIONS (key=value, ...)];
```

**Description**

The `CREATE INDEX` statement creates a secondary index for one or more
values computed from expressions in a table.

**Expressions**

+  `array_expression`: An immutable expression that is used to produce an array
   value from each row of an indexed table.
+  `key_expression`: An immutable expression that is used to produce an index
    key value. The expression must have a type that satisfies the requirement of
    an index key column.
+  `stored_expression`: An immutable expression that is used to produce a value
    stored in the index.

**Optional Clauses**

+  `OR REPLACE`: If the index already exists, replace it. This cannot
    appear with `IF NOT EXISTS`.
+  `UNIQUE`: Do not index the same key multiple times. Systems can choose how to
    resolve conflicts in case the index is over a non-unique key.
+  `IF NOT EXISTS`: Do not create an index if it already exists. This cannot
    appear with `OR REPLACE`.
+  `UNNEST(array_name)`: Create an index for the elements in an array.
+  `WITH OFFSET`: Return a separate column containing the offset value
    for each row produced by the `UNNEST` operation.
+  `ASC | DESC`: Sort the indexed values in ascending or descending
    order. `ASC` is the default value with respect to the sort defined by any
    key parts to the left.
+  `STORING`: Specify additional values computed from the indexed base table row
    to materialize with the index entry.
+  `OPTIONS`: If you have schema options, you can add them when you create the
    index. These options are system-specific and follow the
    ZetaSQL[`HINT` syntax][hints].

**Examples**

Create an index on a column in a table.

```sql
CREATE INDEX i1 ON KeyValue (Key);
```

Create an index on multiple columns in a table.

```sql
CREATE INDEX i1 ON KeyValue (Key, Value);
```

If the index already exists, replace it.

```sql
CREATE OR REPLACE INDEX i1 ON KeyValue (Key, Value);
```

If the index already exists, don't replace it.

```sql
CREATE INDEX IF NOT EXISTS i1 ON KeyValue (Key, Value);
```

Create an index that contains unique values.

```sql
CREATE UNIQUE INDEX i1 ON Books (Title);
```

Create an index that contains a schema option.

```sql
CREATE INDEX i1 ON KeyValue (Value) OPTIONS (page_count=1);
```

Reference the table name for a column.

```sql
CREATE INDEX i1 ON KeyValue (KeyValue.Key, KeyValue.Value);
```

```sql
CREATE INDEX i1 on KeyValue AS foo (foo.Key, foo.Value);
```

Use the path expression for a key.

```sql
CREATE INDEX i1 ON KeyValue (Key.sub_field1.sub_field2);
```

Choose the sort order for the columns assigned to an index.

```sql
CREATE INDEX i1 ON KeyValue (Key DESC, Value ASC);
```

Create an index on an array, but not the elements in an array.

```sql
CREATE INDEX i1 ON Books (BookList);
```

Create an index for the elements in an array.

```sql
CREATE INDEX i1 ON Books UNNEST (BookList) (BookList);
```

```sql
CREATE INDEX i1 ON Books UNNEST (BookListA) AS a UNNEST (BookListB) AS b (a, b);
```

Create an index for the elements in an array using an offset.

```sql
CREATE index i1 on Books UNNEST(BookList) WITH OFFSET (BookList, offset);
```

```sql
CREATE index i1 on Books UNNEST(BookList) WITH OFFSET AS foo (BookList, foo);
```

Store an additional column but don't sort it.

```sql
CREATE INDEX i1 ON KeyValue (Value) STORING (Key);
```

Store multiple additional columns and don't sort them.

```sql
CREATE INDEX i1 ON Books (Title) STORING (First_Name, Last_Name);
```

Store a column but don't sort it. Reference a table name.

```sql
CREATE INDEX i1 ON Books (InStock) STORING (Books.Title);
```

Use an expression in the `STORING` clause.

```sql
CREATE INDEX i1 ON KeyValue (Key) STORING (Key+1);
```

Use an implicit alias in the `STORING` clause.

```sql
CREATE INDEX i1 ON KeyValue (Key) STORING (KeyValue);
```

### CREATE CONSTANT

Documentation is pending for this feature.

### CREATE FUNCTION

Documentation is pending for this feature.

### CREATE PROCEDURE

Documentation is pending for this feature.

### CREATE ROW POLICY

Documentation is pending for this feature.

### CREATE TABLE FUNCTION

Documentation is pending for this feature.

### DEFINE TABLE

```
DEFINE TABLE table_name (options);
```

**Description**

The `DEFINE TABLE` statement allows queries to run against an exported data
source.

### ALTER

<pre>
ALTER <span class="var">object_type</span> <span class="var">alter_action</span>, ...

<span class="var">object_type</span>:
  TABLE
  | VIEW

<span class="var">alter_action</span>:
  SET OPTIONS (key=value, ...)
  | ADD COLUMN [ IF NOT EXISTS ]  column_definition
  | DROP COLUMN [ IF EXISTS ]  column_name
</pre>

**Description**

The `ALTER` statement modifies metadata for a table or a view.

`table_name` is any identifier or dotted path.

The option entries are system-specific. These follow the ZetaSQL
[`HINT` syntax][hints].

SET OPTIONS action raises an error under these conditions:

+   The table does not exist.
+   A key is not recognized.

The following semantics apply:

+   The value is updated for each key in the `SET OPTIONS` clause.
+   Keys not in the `SET OPTIONS` clause remain unchanged.
+   Setting a key value to `NULL` clears it.

ADD and DROP COLUMN actions raise an error under these conditions:

+   ADD COLUMN or DROP COLUMN are applied to a view.
+   ADD COLUMN specified without IF NOT EXIST and the column already exists.
+   DROP COLUMN specified without IF EXIST and the column does not exist.

**Examples**

The following examples illustrate ways to use the `ALTER SET OPTIONS` statement:

Update table description.

```sql
ALTER TABLE my_dataset.my_table
SET OPTIONS (description='my table');
```

Remove table description.

```sql
ALTER TABLE my_dataset.my_table
SET OPTIONS (description=NULL);
```

The following example illustrates using the `ALTER ADD COLUMN` statement:

```sql
ALTER TABLE mydataset.mytable
    ADD COLUMN A STRING,
    ADD COLUMN IF NOT EXISTS B GEOGRAPHY,
    ADD COLUMN C ARRAY<NUMERIC>,
    ADD COLUMN D DATE OPTIONS(description="my description")
```

Add column A of type STRUCT.

```sql
ALTER TABLE mydataset.mytable
ADD COLUMN A STRUCT<
               B GEOGRAPHY,
               C ARRAY<INT64>,
               D INT64 NOT NULL,
               E TIMESTAMP OPTIONS(description="creation time")
             >
```

### RENAME

```
RENAME object_type old_name_path TO new_name_path;
```

**Description**

The `RENAME` object renames an object. `object_type` indicates what type of
object to rename.

### DROP

```
DROP object_type [IF EXISTS] object_path;
```

**Description**

The `DROP` statement drops an object. `object_type` indicates what type of
object to drop.

**Optional Clauses**

+   `IF EXISTS`: If no object exists at `object_path`, the `DROP` statement will
    have no effect.

### Terminology

#### Primary key

A primary key constraint is an attribute of a table. A table can have at most
one primary key constraint that includes one or more columns of that table.
Each row in the table has a tuple that is the row's primary key. The primary key
tuple has values that correspond to the columns in the primary key constraint.
The primary key of all rows must be unique among the primary keys of all rows in
the table.

**Examples**

In this example, a column in a table called `books` is assigned to a primary
key. For each row in this table, the value in the `id` column must be distinct
from the value in the `id` column of all other rows in the table.

```sql
CREATE TABLE books (title STRING, id STRING, PRIMARY KEY (id));
```

In this example, multiple columns in a table called `books` are assigned to a
primary key. For each row in this table, the tuple of values in the `title` and
`name` columns must together be distinct from the values in the respective
`title` and `name` columns of all other rows in the table.

```sql
CREATE TABLE books (title STRING, name STRING, PRIMARY KEY (title, name));
```

[primary-key]: #primary-key
[create-table]: #create-table
[hints]: lexical#hints
[defining-columns]: #defining-columns
[defining-constraints]: #defining-table-constraints
[defining-foreign-reference]: #defining-foreign-references

[hints]: #hints

<!-- END CONTENT -->

<!-- This file is auto-generated. DO NOT EDIT.                               -->

## Data Manipulation Reference

<!-- BEGIN CONTENT -->

ZetaSQL supports the following statements for manipulating data:

+ `INSERT`
+ `UPDATE`
+ `DELETE`

<a name="example-data"></a>
### Example data

The sections in this topic use the following table schemas.

<a name="singers-table"></a>
#### Singers table

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
<th>Default Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>SingerId</td>
<td><code>INT64 NOT NULL</code></td>
<td><code>&lt;auto-increment&gt;</code></td>
</tr>
<tr>
<td>FirstName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>LastName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>BirthDate</td>
<td><code>DATE</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Status</td>
<td><code>STRING</code></td>
<td><code>"active"</code></td>
</tr>
<tr>
<td>SingerInfo</td>
<td><code>PROTO&lt;SingerMetadata&gt;</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Albums</td>
<td><code>PROTO&lt;Album&gt;</code></td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

The proto, `SingerMetadata`, has the following definition:

<pre>
message SingerMetadata {
  optional string    nationality = 1;
  repeated Residence residence   = 2;<br/>
  message Residence {
    required int64  start_year   = 1;
    optional int64  end_year     = 2;
    optional string city         = 3;
    optional string country      = 4 [default = "USA"];
  }
}
</pre>

The proto, `Album`, has the following definition:

<pre>
message Album {
  optional string title = 1;
  optional int32 tracks = 2;
  repeated string comments = 3;
  repeated Song song = 4;<br/>
  message Song {
    optional string songtitle = 1;
    optional int32 length = 2;
    repeated Chart chart = 3;<br/>
    message Chart {
      optional string chartname = 1;
      optional int32 rank = 2;
    }
  }
}
</pre>

<a name="concerts-table"></a>
#### Concerts table

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
<th>Default Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>VenueId</td>
<td><code>INT64 NOT NULL</code></td>
<td><code>&lt;auto-increment&gt;</code></td>
</tr>
<tr>
<td>SingerId</td>
<td><code>INT64 NOT NULL</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>ConcertDate</td>
<td><code>DATE NOT NULL</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>BeginTime</td>
<td><code>TIMESTAMP</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>EndTime</td>
<td><code>TIMESTAMP</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>TicketPrices</td>
<td><code>ARRAY&lt;INT64&gt;</code></td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

### INSERT statement

Use the `INSERT` statement when you want to add new rows to a table.

<pre>
INSERT [[OR] IGNORE | REPLACE | UPDATE] [INTO] target_name
 (column_1 [, ..., column_n ] )
 input <br>[ASSERT_ROWS_MODIFIED m]<br/>
input ::=
 VALUES (expr_1 [, ..., expr_n ] )
        [, ..., (expr_k_1 [, ..., expr_k_n ] ) ]
| SELECT_QUERY<br/>
expr ::= value_expression | DEFAULT
</pre>

<a name="statement-rules"></a>
`INSERT` statements must comply with the following rules:

+ Column names must be specified.
+ Duplicate names are not allowed in the list of target columns.
+ Values must be added in the same order as the specified columns.
+ The number of values added must match the number of specified columns.
+ Values must have a type that is [compatible](#compatible-types) with the
  target column.
+ All non-null columns must appear in the column list, and have a non-null value
  specified.

Statements that don't follow these rules will raise an error.

The following statement inserts a new row to the
[Singers example table](#singers-table):

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Catalina", "Smith", "1990-08-17", "active", "nationality:'U.S.A.'");
```

This next statement inserts multiple rows into the table:

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (6, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (7, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'");
```

You can construct `INSERT` statements to insert the results of a query. For
example, the following statement inserts a new concert for a singer into
the [Concerts table](#concerts-table).

```
INSERT INTO Concerts
  (VenueId, SingerId, ConcertDate, BeginTime, EndTime, TicketPrices)
SELECT c.VenueId, c.SingerId, DATE "2015-06-01", c.BeginTime, c.EndTime, c.TicketPrices
  FROM Concerts c WHERE c.SingerId = 1;
```

<a name="compatible-types"></a>
#### Value type compatibility

Values added with an `INSERT` statement must be compatible with the target
column's type. A value's type is considered compatible with the target column's
type if one of the following criteria are met:

+ The value type matches the column type exactly. (For example, inserting a
value of type `INT32` in a column that also has a type of `INT32`.)
+ The value type is one that can be implicitly coerced into another type. See
the [Coercion][coercion] of the
[Expressions, Functions, and Operators][functions-and-operators]
topic for more information.
+ The value can be assigned after applying a narrowing integer or floating point
cast. For example, if you attempt to add a value of `INT64` into a column that
supports `INT32`, ZetaSQL automatically adds a narrowing cast to
`INT32`.

#### Default values

If the target table supports a default value for a
column, you can omit the column from the `INSERT` statement. Alternatively, you
can insert the default value explicitly using the `DEFAULT` keyword.

**Note:** If a column does not require a value, the default value is typically
`NULL`.

The following example inserts default values for all unspecified columns.
For example, the [Singers table](#singers-table) has two columns that have
defined default values&mdash;the `SingerId` column, which is auto-incremented;
 and the `Status` column, which has a default value of `active`. For all other
columns, the default value is `NULL`.

```
INSERT INTO Singers
  (FirstName)
VALUES ("Neil");
```

If this was the first row added to the table, a `SELECT` statement querying
this row would return the following:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>Status</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>Neil</td>
<td>NULL</td>
<td>NULL</td>
<td>active</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>

If you construct an `INSERT` statement to use a default value for a
column and that column does not support a default value, the statement fails.
For example, the `Singers` table contains a column, `SingerId`, which cannot be
`NULL`. Consequently, the following statement fails, because the `SingerId` does
not support a default value:

```
INSERT INTO Singers
  (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (DEFAULT, "Andrew", "Duneskipper", "2000-01-15", "active", NULL);
```

#### Duplicate rows

By default, an `INSERT` statement fails when it tries to add a duplicate row.
A duplicate row is defined as one for which the
primary key already exists in the table.

You can change this behavior by choosing one of the following options for
handling duplicate rows:

+ `IGNORE`
+ `REPLACE`
+ `UPDATE`

Your statement would resemble the following:

<pre>
INSERT [OR] IGNORE | REPLACE | UPDATE <em>&lt;rest of statement&gt;</em>
</pre>

These keywords require that your table includes a primary key. If the table does
not have a primary key, the statement fails.

In cases where an `INSERT` statement attempts to add the same row twice,
ZetaSQL treats the inserts as if they were applied sequentially. This means
that:

+ For `INSERT IGNORE` statements, the first row is inserted, and the rest are
ignored.
+ For `INSERT REPLACE` statements, all but the last insert are replaced.
+ For `INSERT UPDATE` statements, updates are applied sequentially, ending with
the last update.

##### INSERT IGNORE

You can instruct your `INSERT` statement to skip any rows that have duplicate
primary keys by using the `IGNORE` keyword.

For example, the following statement instructs ZetaSQL to skip the
row if it already exists in the table:

```
INSERT OR IGNORE INTO Singers
    (FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES ("Catalina", "Smith", "1990-08-17", DEFAULT, "nationality:'U.S.A.'");
```

`INSERT IGNORE` ignores only duplicate rows that have the same primary key.
ZetaSQL raises an error if any other constraint violation occurs, including
duplicate keys in unique indexes.

##### INSERT REPLACE

You can instruct your `INSERT` statement to replace any rows that have duplicate
primary keys by using the `REPLACE` statement. Replaced rows have the same
primary key as the original row.

In the following example, the `REPLACE` keyword is used instead of `IGNORE`.
This time, the duplicate row replaces the existing row in the table.

```
INSERT OR REPLACE INTO Singers
    (FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES ("Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

Using `REPLACE` is the same as deleting the existing row and inserting a
replacement one. As a result, any columns that you do not mention in the
replacement row are cleared and replaced with their default values. If you want
your statement to preserve the values of unspecified columns, use
`INSERT UPDATE`.

##### INSERT UPDATE

With an `INSERT UPDATE` statement, the statement updates the columns specified
for one or more rows. Columns that are not listed in the `INSERT` statement
remain unchanged.

The following example illustrates how `INSERT UPDATE` changes an existing row.
In this case, the status is changed from `active` to `inactive`.

```
INSERT OR UPDATE INTO Singers
    (SingerId, Status)
VALUES (5, "inactive");
```

Notice that if the row does not exist, the previous statement inserts a new row
with values in the specified fields, and all other values set to their
corresponding defaults.

When you use the `UPDATE` keyword, any column that you do not specify remains
unchanged.

#### INSERT and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows are added each time you
use an `INSERT` statement by using the `ASSERT_ROWS_MODIFIED` keyword.

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to insert, and ZetaSQL compares that number
against the number of rows actually modified. This check occurs before
ZetaSQL commits the change to the database. If the row count matches,
ZetaSQL commits the changes.  Otherwise, ZetaSQL returns
an error and rolls back the statement.

When calculating the number of rows modified, ZetaSQL includes rows
that were inserted, updated, or replaced. It does not count rows that were
skipped through the `IGNORE` keyword. To illustrate this, consider the following
example of a **Singers** table:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality: 'U.S.A.'</td>
<td>NULL</td>
</tr>
</tbody>
</table>

```
INSERT OR UPDATE INTO Singers
    (SingerId, FirstName, LastName, Birthdate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (6, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'")
ASSERT_ROWS_MODIFIED 2;
```

One of the value expressions in the `INSERT` statement adds a new row, while the
second updates an existing one. Both result in a total of 2 modified rows, which
matches the `ASSERT_ROWS_MODIFIED 2` clause of the statement.

The following statement uses `INSERT IGNORE` to add rows to the table.

```
INSERT OR IGNORE INTO Singers
    (SingerId, FirstName, LastName, Birthdate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (7, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'")
ASSERT_ROWS_MODIFIED 1;
```

In this case, there is a collision when inserting a new row with a `SingerId` of
`5`, because a row with that `SingerId` value already exists. This statement
inserts one row, which matches the `ASSERT_ROWS_MODIFIED 1` statement.

#### INSERT examples

Add a new row to the `Singers` table.

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Catalina", "Smith", "1990-08-17", DEFAULT, "nationality:'U.S.A.'");
```

**RESULT:** New singer, Catalina Smith, added to table.

Try to add a singer, but only if the `SingerId` is not already in the table.

```
INSERT OR IGNORE INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

**RESULT:** Table unaffected. Catalina Smith remains in the table.

Try to add another singer and replace an existing row if it has the same
`SingerId`.

```
INSERT OR REPLACE INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

**RESULT:** Singer, Catalina Smith, replaced with Zak Sterling.

Add a singer to the table, or update a singer if they already exist.

```
INSERT OR UPDATE INTO Singers
    (SingerId, FirstName, LastName, Status)
VALUES (5, "Zak", "Sterling", "inactive");
```

Existing row for Zak Sterling updated. His status is now `inactive`. All other
values, such as `BirthDate`, remain unchanged.

### DELETE Statement

Use the `DELETE` statement when you want to delete rows from a table.

<pre>
DELETE [FROM] target_name
WHERE condition<br>[ASSERT_ROWS_MODIFIED n]
</pre>

**Note**: `DELETE` statements must comply with all
[statement rules](#statement-rules).

#### WHERE keyword

Each time you construct a `DELETE` statement, you must use the `WHERE` keyword,
followed by a condition. For example:

```
DELETE FROM Singers
WHERE LastName = "Sterling";
```

You can use the identifier for the target of the update as a range variable
within the `WHERE` clause. For example:

```
DELETE FROM Singers s
WHERE s.LastName = "Sterling";
```

Using an identifier can help make your `DELETE` statement more explicit with
regards to what data ZetaSQL should update.

The `WHERE` keyword is mandatory for any `DELETE` statement. However, you can
set the condition for the `WHERE` keyword to be true, which results in the
statement deleting all rows in the table.

```
DELETE FROM Singers s
WHERE true;
```

#### DELETE and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows were deleted each time
you use a `DELETE` statement. You implement this confirmation through the
`ASSERT_ROWS_MODIFIED` keyword.

```
DELETE FROM Singers
WHERE LastName = "Sterling"
ASSERT_ROWS_MODIFIED 1;
```

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to delete, and compare that number against the number of rows
actually deleted. This check occurs before ZetaSQL commits the change
to the database. If the row count matches, ZetaSQL commits the
changes. Otherwise, ZetaSQL returns an error and rolls back the
statement.

### UPDATE statement

Use the `UPDATE` statement when you want to update existing rows within a table.

<pre>
UPDATE target_name
SET update_item [, update_item]*
WHERE condition<br>[ASSERT_ROWS_MODIFIED n]<br/>
update_item ::= path_expression = expression
              | path_expression = DEFAULT
              | (dml_stmt)<br/>
dml_stmt ::= insert_statement | update_statement | delete_statement
</pre>

**Note**: `UPDATE` statements must comply with all
[statement rules](#statement-rules) and use
[compatible types](#compatible-types).

#### WHERE keyword

Each `UPDATE` statement must include the `WHERE` keyword, followed by a
condition. For example, the following statement illustrates an `UPDATE`
statement that modifies the row with the primary key, `5`.

```
UPDATE Singers s
SET s.Status = "inactive"
WHERE s.SingerId = 5;
```

To update all rows in a table, use `WHERE true`. The following statement sets
the SingerInfo value in all rows to `NULL` (the default value for the field).

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE true;
```

#### UPDATE and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows were added each time you
use an `UPDATE` statement. You implement this confirmation through the
`ASSERT_ROWS_MODIFIED` keyword.

**Note**: A row still counts as modified even if the updated values are
identical to the previous values.

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to update. If the row count matches, ZetaSQL
commits the changes. Otherwise, ZetaSQL returns an error and rolls
back the statement.

The `ASSERT_ROWS_MODIFIED` keyword is helpful when you want to be sure your
statement changes only a specific number of rows. For example, the following
statement verifies that exactly one row was found and updated.

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

#### Setting new values

Each `UPDATE` statement requires a `SET` keyword. This keyword identifies the
columns that you want to update.

You specify values for specific columns using the syntax <em>column_name =
expression</em>. Expressions can see all the columns within a table. For
example:

```
UPDATE Singers s
SET s.SingerId = s.SingerId + 1
WHERE true
```

Any assignments within the expression happen simultaneously. For example, the
following statement swaps the values in the `FirstName` and `LastName` column.

```
UPDATE Singers s
SET s.FirstName = s.LastName,
    s.LastName = s.FirstName
WHERE true;
```

Within the `SET` clause, you can use the identifier for the target of the update
as a table alias. For example:

```
UPDATE Singers s
SET s.LastName = "Solo"
WHERE s.SingerId = 5;
```

Using an identifier can help make your `UPDATE` statement more explicit with
regards to what data ZetaSQL should update.

#### Specifying columns

An `UPDATE` statement must explicitly state which columns you want to update.
Any column not included in the `UPDATE` statement remains unmodified.

In the following example, notice that only the `Status` column is
specified &mdash; consequently, the other columns
(such as `SingerId`, `FirstName`, `LastName`, and `Birthdate`) remain unchanged.

```
UPDATE Singers s
SET s.Status = "inactive"
WHERE s.SingerId = 5;
```

You can specify the columns to update in any order; however, you can list each
column only once. For example, the following statement is invalid and would be
rejected by ZetaSQL:

```
UPDATE Singers s
SET s.LastName = "Solo", s.LastName = "Organa"
WHERE s.SingerId = 5;
```

`UPDATE` statements must use values that are compatible with the corresponding
column's type. For example, a value of `1991-10-02` works for a column of type
`Date`, but a value of `October 2, 1991` would not. You can, however, use values
that can be cast to the type of the corresponding column. Casting happens
automatically for numerical values (this is also referred to as
[coercion][coercion]).

If you attempt to set a column with an invalid value, the statement fails.
For example, the following statement does not work:

```
UPDATE Singers s
SET s.Birthdate = "October 2, 1991"
WHERE s.SingerId = 5;
```

#### Returning columns to default values

You can use an `UPDATE` statement to return any value to its corresponding
default by using the `DEFAULT` keyword. In cases where the column contains a
protocol buffer field, that field is cleared.

For example, consider the following **Singers** table:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality: 'USA'"</td>
<td>NULL</td>
</tr>
<tr>
<td>6</td>
<td>Zak</td>
<td>Sterling</td>
<td>1989-1-13</td>
<td>"nationality: 'Australia'"</td>
<td>NULL</td>
</tr>
</tbody>
</table>

If you run the following statement:

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE SingerId = 6;
```

The table is updated as follows:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality:'USA'"</td>
<td>NULL</td>
</tr>
<tr>
<td>6</td>
<td>Zak</td>
<td>Sterling</td>
<td>1989-1-13</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>

Notice that the row for `Zak Sterling` now has `NULL` in the `SingerInfo`
column, because `NULL` is the default value for that column. The following
example has the same effect:

```
UPDATE Singers s
SET s.SingerInfo = NULL
WHERE SingerId = 6;
```

#### Updating fields

ZetaSQL allows you to update non-repeating or repeating fields within
protocol buffers. To illustrate how to update a non-repeating field, consider
the [Singers example table](#singers-table).  It contains a column, `Albums`, of
type `PROTO<Albums>`, and `Albums` contains a non-repeating field `tracks`. The
following statement updates the value of `tracks`:

```
UPDATE Singers s
SET s.Albums.tracks = 15
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

An update can replace a repeated field using an array of values, as shown below.

```
UPDATE Singers s
SET s.Albums.comments = ["A good album!", "Hurt my ears!", "Totally inedible."]
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

#### Nested updates

Inside a parent update statement you can construct DML statements that modify
a repeated field of a protocol buffer or an array.  Such statements are called
**nested updates**.

For example, the `Albums` column contains a repeated field `comments`. This
nested update statement adds a comment to an album:

```
UPDATE Singers s
SET (INSERT s.Albums.comments
     VALUES ("Groovy!"))
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

`Albums` also contains a repeated protocol buffer, `Song`, which provides
information about a song on an album. This nested update statement updates the
album with a new song:

```
UPDATE Singers s
SET (INSERT s.Albums.Song(Song)
     VALUES ("songtitle: 'Bonus Track', length: 180"))
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

If the repeated field is another protocol buffer, you can provide the
protocol buffer as a string literal. For example, the following statement adds a
new song to an album and updates the number of tracks. Notice that this
statement uses `ASSERT_ROWS_MODIFIED` to ensure that only one `Singer` is
updated.

```
UPDATE Singers s
SET (INSERT s.Albums.Song
     VALUES ('''songtitle: 'Bonus Track', length:180''')),
     s.Albums.tracks = 16
WHERE s.SingerId = 5 and s.Albums.title = "Fire is Hot"
ASSERT_ROWS_MODIFIED 1;
```

You can also nest a nested update statement in another nested update statement.
For example, the `Song` protocol buffer itself has another repeated
protocol buffer, `Chart`, which provides information on what chart the song
appeared on, and what rank it had.

The following statement adds a new chart to a song.

```
UPDATE Singers s
SET (UPDATE s.Albums.Song so
    SET (INSERT INTO so.Chart
         VALUES ("chartname: 'Galaxy Top 100', rank: 5"))
    WHERE so.songtitle = "Bonus Track")
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

This next statement updates the chart to reflect a new rank for the song. Notice
that each inner `UPDATE` statement uses `ASSERT_ROWS_MODIFIED 1` to ensure that
only one update is made.

```
UPDATE Singers s
SET (UPDATE s.Albums.Song so
     SET (UPDATE so.Chart c
          SET c.rank = 2
          WHERE c.chartname = "Galaxy Top 100"
          ASSERT_ROWS_MODIFIED 1)
     WHERE so.songtitle = "Bonus Track"
     ASSERT_ROWS_MODIFIED 1)
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

ZetaSQL treats an array or repeated field inside a row that matches
an `UPDATE WHERE` clause as a table, with individual elements of the array or
field treated like rows. These rows can then have nested DML statements run
against them, allowing you to delete, update, and insert data as needed.

##### Modifying multiple fields

The previous sections demonstrated how to update a single value within a
compound data type. You can also perform multiple updates to a compound data
type within a single statement. For example:

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (UPDATE s.Albums.Song song SET song.songtitle = 'No, This Is Rubbish' WHERE song.songtitle = 'This Is Pretty Good'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'"))
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

Nested queries are processed as follows:

1. Delete all rows that match a `WHERE` clause of a `DELETE` statement.
2. Update any remaining rows that match a `WHERE` clause of an `UPDATE`
statement. Each row must match at most one `UPDATE WHERE` clause, or the
statement fails due to overlapping updates.
3. Insert all rows in `INSERT` statements.

You must construct nested statements that affect the same field in the following
order:

+ `DELETE`
+ `UPDATE`
+ `INSERT`

For example:

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (UPDATE s.SingerInfo.Residence r SET r.end_year = 2015 WHERE r.City = 'Eugene'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'"))
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

The following statement is invalid, because the `UPDATE` statement
happens after the `INSERT` statement.

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'")),
    (UPDATE s.SingerInfo.Residence r SET r.end_year = 2015 WHERE r.City = 'Eugene')
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

**Note:** In nested queries, you cannot use `INSERT OR` statements. These types
of statements don't work because arrays and other compound data types do not
always have a primary key, so there is no applicable definition of duplicate
rows.

You can also add `ASSERT_ROWS_MODIFIED` to nested statements, as shown here:

```
UPDATE Singers
SET
    (DELETE FROM AlbumTitles a WHERE a.AlbumTitles = "Too Many Vegetables"
        ASSERT_ROWS_MODIFIED 1),
    (UPDATE AlbumTitles a SET a.AlbumTitles = "Album IV"
        WHERE a.AlbumTitles = "Album Four"),
    (INSERT AlbumTitles VALUES ("The Sloth and the Tiger"));
```

[coercion]: #coercion
[functions-and-operators]: #function_reference

<!-- END CONTENT -->

<!-- END CONTENT -->

