

# Timestamp functions

ZetaSQL supports the following `TIMESTAMP` functions.

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined date and timestamp min/max values.

### CURRENT_TIMESTAMP

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

### EXTRACT

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

### STRING

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

### TIMESTAMP

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

### TIMESTAMP_ADD

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

### TIMESTAMP_SUB

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

### TIMESTAMP_DIFF

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

### TIMESTAMP_TRUNC

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

### FORMAT_TIMESTAMP

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

### PARSE_TIMESTAMP

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

### TIMESTAMP_SECONDS

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

### TIMESTAMP_MILLIS

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

### TIMESTAMP_MICROS

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

### UNIX_SECONDS

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

### UNIX_MILLIS

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

### UNIX_MICROS

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

### TIMESTAMP_FROM_UNIX_SECONDS

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

### TIMESTAMP_FROM_UNIX_MILLIS

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

### TIMESTAMP_FROM_UNIX_MICROS

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

### Supported format elements for TIMESTAMP

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

### Timezone definitions

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

