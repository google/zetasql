

# Time functions

ZetaSQL supports the following `TIME` functions.

### CURRENT_TIME
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

### TIME
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

### EXTRACT
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

### TIME_ADD
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

### TIME_SUB
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

### TIME_DIFF
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

### TIME_TRUNC

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

### FORMAT_TIME

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

### PARSE_TIME

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

### Supported format elements for TIME

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

[time-link-to-timezone-definitions]: functions-and-operators#timezone-definitions
[time-link-to-supported-format-elements-for-time]: functions-and-operators#supported-format-elements-for-time

