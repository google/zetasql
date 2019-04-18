

# DateTime functions

ZetaSQL supports the following `DATETIME` functions.

### CURRENT_DATETIME
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

### DATETIME
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

### EXTRACT
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
+ `ISOWEEK`: Returns the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) week
  number of the `datetime_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Returns the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)
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

### DATETIME_ADD
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

### DATETIME_SUB
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

### DATETIME_DIFF
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
+ `ISOWEEK`: Uses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)
  week boundaries. ISO weeks begin on Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Uses the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)
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

### DATETIME_TRUNC

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
   [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) week boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Truncates `datetime_expression` to the preceding [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)
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

### FORMAT_DATETIME

```
FORMAT_DATETIME(format_string, datetime_expression)
```

**Description**

Formats a DATETIME object according to the specified `format_string`. See
[Supported Format Elements For DATETIME][datetime-functions-link-to-supported-format-elements-for-datetime]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Example**

```sql
SELECT
  FORMAT_DATETIME("%c", DATETIME "2008-12-25 15:30:00")
  AS formatted;
```

### PARSE_DATETIME

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

### Supported format elements for DATETIME

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
    decimal
number (00-99).</td>
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
    <td>The ISO 8601 year with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The ISO 8601 year without century as a decimal number (00-99). Each ISO
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
    decimal
number (00-53).</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (01-53).  If the week containing January 1 has four or more
    days in the new year, then it is week 1; otherwise it is week 53 of the
    previous year, and the next week is week 1.</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal
number (00-53).</td>
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

[datetime-link-to-timezone-definitions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md#timezone-definitions
[datetime-functions-link-to-supported-format-elements-for-datetime]: https://github.com/google/zetasql/blob/master/docs/datetime_functions.md#supported-format-elements-for-datetime

