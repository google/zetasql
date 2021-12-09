

# Time functions

ZetaSQL supports the following `TIME` functions.

### CURRENT_TIME

```sql
CURRENT_TIME([timezone])
```

**Description**

Returns the current time as a `TIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `timezone` parameter.
See [Timezone definitions][time-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT CURRENT_TIME() as now;

+----------------------------+
| now                        |
+----------------------------+
| 15:31:38.776361            |
+----------------------------+
```

When a column named `current_time` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][time-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_time` column.

```sql
WITH t AS (SELECT 'column value' AS `current_time`)
SELECT current_time() as now, t.current_time FROM t;

+-----------------+--------------+
| now             | current_time |
+-----------------+--------------+
| 15:31:38.776361 | column value |
+-----------------+--------------+
```

### TIME

```sql
1. TIME(hour, minute, second)
2. TIME(timestamp, [timezone])
3. TIME(datetime)
```

**Description**

1. Constructs a `TIME` object using `INT64`
   values representing the hour, minute, and second.
2. Constructs a `TIME` object using a `TIMESTAMP` object. It supports an
   optional
   parameter to [specify a timezone][time-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Constructs a `TIME` object using a
  `DATETIME` object.

**Return Data Type**

`TIME`

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
SELECT TIME(DATETIME "2008-12-25 15:30:00.000000") AS time_dt;

+----------+
| time_dt  |
+----------+
| 15:30:00 |
+----------+
```

### EXTRACT

```sql
EXTRACT(part FROM time_expression)
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `time_expression`.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`

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

### TIME_ADD

```sql
TIME_ADD(time_expression, INTERVAL int64_expression part)
```

**Description**

Adds `int64_expression` units of `part` to the `TIME` object.

`TIME_ADD` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you add an hour to `23:30:00`, the returned
value is `00:30:00`.

**Return Data Types**

`TIME`

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

### TIME_SUB

```sql
TIME_SUB(time_expression, INTERVAL int64_expression part)
```

**Description**

Subtracts `int64_expression` units of `part` from the `TIME` object.

`TIME_SUB` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you subtract an hour from `00:30:00`, the
returned value is `23:30:00`.

**Return Data Type**

`TIME`

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

### TIME_DIFF

```sql
TIME_DIFF(time_expression_a, time_expression_b, part)
```

**Description**

Returns the whole number of specified `part` intervals between two
`TIME` objects (`time_expression_a` - `time_expression_b`). If the first
`TIME` is earlier than the second one, the output is negative. Throws an error
if the computation overflows the result type, such as if the difference in
nanoseconds
between the two `TIME` objects would overflow an
`INT64` value.

`TIME_DIFF` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

`INT64`

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

### TIME_TRUNC

```sql
TIME_TRUNC(time_expression, part)
```

**Description**

Truncates a `TIME` object to the granularity of `part`.

`TIME_TRUNC` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

`TIME`

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

### FORMAT_TIME

```sql
FORMAT_TIME(format_string, time_object)
```

**Description**
Formats a `TIME` object according to the specified `format_string`. See
[Supported Format Elements For TIME][time-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIME("%R", TIME "15:30:00") as formatted_time;

+----------------+
| formatted_time |
+----------------+
| 15:30          |
+----------------+
```

### PARSE_TIME

```sql
PARSE_TIME(format_string, time_string)
```

**Description**

Converts a [string representation of time][time-format] to a
`TIME` object.

`format_string` contains the [format elements][time-format-elements]
that define how `time_string` is formatted. Each element in
`time_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `time_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIME("%I:%M:%S", "07:30:00")

-- This doesn't work because the seconds element is in different locations.
SELECT PARSE_TIME("%S:%I:%M", "07:30:00")

-- This doesn't work because one of the seconds elements is missing.
SELECT PARSE_TIME("%I:%M", "07:30:00")

-- This works because %T can find all matching elements in time_string.
SELECT PARSE_TIME("%T", "07:30:00")
```

The format string fully supports most format elements except for `%P`.

When using `PARSE_TIME`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from
  `00:00:00.0`. For instance, if `seconds` is unspecified then it
  defaults to `00`, and so on.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the `TIME` string. In
  addition, leading and trailing white spaces in the `TIME` string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information, the last one generally overrides any earlier ones.
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT PARSE_TIME("%H", "15") as parsed_time;

+-------------+
| parsed_time |
+-------------+
| 15:00:00    |
+-------------+
```

```sql
SELECT PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') AS parsed_time

+-------------+
| parsed_time |
+-------------+
| 14:23:38    |
+-------------+
```

### Supported format elements for TIME

Unless otherwise noted, `TIME` functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
    <td>09</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
    <td>9</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
    <td>47</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
    <td>pm</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
    <td>PM</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
    <td>21:47</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
    <td>09:47:00 PM</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
    <td>00</td>
 </tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
    <td>%</td>
 </tr>
 <tr>
    <td>%E&lt;number&gt;S</td>
    <td>Seconds with &lt;number&gt; digits of fractional precision.</td>
    <td>00.000 for %E3S</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
    <td>00.123456</td>
 </tr>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[time-format]: #format_time

[time-format-elements]: #supported_format_elements_for_time

[time-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[time-link-to-timezone-definitions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md#timezone_definitions

[time-to-string]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#cast

<!-- mdlint on -->

