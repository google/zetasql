
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Data Types

<!-- BEGIN CONTENT -->
ZetaSQL supports simple data types such as integers, as well as more
complex types such as ARRAY,
PROTO, and STRUCT. This page provides an overview of each data type,
including allowed values. For information on
data type literals and constructors, see
[Lexical Structure and Syntax][lexical-literals].

## Data type properties

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

<a href="https://github.com/google/zetasql/blob/master/docs/query-syntax#join_types">

JOIN Types
</a>

for an explanation of join conditions.</td></tr>
</tbody>
</table>

## Numeric types

Numeric types include integer types, floating point types and the `NUMERIC` data
type.

### Integer types

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

### NUMERIC type

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

### Floating point types {: #floating_point_types }

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

#### Floating point semantics

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

## Boolean type

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

## String type

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

## Bytes type

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

## Date type

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

## Datetime type

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

## Time type

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

## Timestamp type

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

### Time zones

Time zones are used when parsing timestamps or formatting timestamps
for display. The timestamp value itself does not store a specific time zone,
nor does it change when you apply a time zone offset.

Time zones are represented by strings in one of these two canonical formats:

+ Offset from Coordinated Universal Time (UTC), or the letter `Z` for UTC
+ Time zone name from the [tz database][tz-database]{: class=external target=_blank }

#### Offset from Coordinated Universal Time (UTC)

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

#### Time zone name

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

#### Leap seconds

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

## Array type

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

### Declaring an ARRAY type

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

## Enum type

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

## Struct type

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

### Declaring a STRUCT type

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

### Constructing a STRUCT

#### Tuple syntax

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

#### Typeless struct syntax

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

#### Typed struct syntax

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

### Limited comparisons for STRUCT

STRUCTs can be directly compared using equality operators:

  * Equal (`=`)
  * Not Equal (`!=` or `<>`)
  * [`NOT`] `IN`

Notice, though, that these direct equality comparisons compare the fields of
the STRUCT pairwise in ordinal order ignoring any field names. If instead you
want to compare identically named fields of a STRUCT, you can compare the
individual fields directly.

## Protocol buffer type

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

### Limited comparisons for PROTO

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

[geography-functions]: https://github.com/google/zetasql/blob/master/docs/geography_functions
[mathematical-functions]: https://github.com/google/zetasql/blob/master/docs/mathematical_functions

<!-- END CONTENT -->

