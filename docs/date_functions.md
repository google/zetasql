

# Date functions

ZetaSQL supports the following `DATE` functions.

### CURRENT_DATE

```sql
CURRENT_DATE([time_zone])
```

**Description**

Returns the current date as of the specified or default timezone. Parentheses
are optional when called with no
arguments.

 This function supports an optional
`time_zone` parameter. This parameter is a string representing the timezone to
use. If no timezone is specified, the default timezone, which is implementation defined,
is used. See [Timezone definitions][date-functions-link-to-timezone-definitions]
for information on how to specify a time zone.

If the `time_zone` parameter evaluates to `NULL`, this function returns `NULL`.

**Return Data Type**

DATE

**Example**

```sql
SELECT CURRENT_DATE() AS the_date;

+--------------+
| the_date     |
+--------------+
| 2016-12-25   |
+--------------+
```

When a column named `current_date` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][date-functions-link-to-range-variables]. For example, the
following query will select the function in the `the_date` column and the table
column in the `current_date` column.

```sql
WITH t AS (SELECT 'column value' AS `current_date`)
SELECT current_date() AS the_date, t.current_date FROM t;

+------------+--------------+
| the_date   | current_date |
+------------+--------------+
| 2016-12-25 | column value |
+------------+--------------+
```

### EXTRACT

```sql
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
date part.

```sql
SELECT EXTRACT(DAY FROM DATE '2013-12-25') AS the_day;

+---------+
| the_day |
+---------+
| 25      |
+---------+
```

In the following example, `EXTRACT` returns values corresponding to different
date parts from a column of dates near the end of the year.

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

### DATE

```sql
1. DATE(year, month, day)
2. DATE(timestamp_expression[, timezone])
3. DATE(datetime_expression)
```

**Description**

1. Constructs a DATE from INT64 values representing
   the year, month, and day.
2. Extracts the DATE from a TIMESTAMP expression. It supports an
   optional parameter to [specify a timezone][date-functions-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Extracts the DATE from a DATETIME expression.

**Return Data Type**

DATE

**Example**

```sql
SELECT
  DATE(2016, 12, 25) AS date_ymd,
  DATE(DATETIME "2016-12-25 23:59:59") AS date_dt,
  DATE(TIMESTAMP "2016-12-25 05:30:00+07", "America/Los_Angeles") AS date_tstz;

+------------+------------+------------+
| date_ymd   | date_dt    | date_tstz  |
+------------+------------+------------+
| 2016-12-25 | 2016-12-25 | 2016-12-24 |
+------------+------------+------------+
```

### DATE_ADD

```sql
DATE_ADD(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds a specified time interval to a DATE.

`DATE_ADD` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_ADD(DATE "2008-12-25", INTERVAL 5 DAY) AS five_days_later;

+--------------------+
| five_days_later    |
+--------------------+
| 2008-12-30         |
+--------------------+
```

### DATE_SUB

```sql
DATE_SUB(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts a specified time interval from a DATE.

`DATE_SUB` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_SUB(DATE "2008-12-25", INTERVAL 5 DAY) AS five_days_ago;

+---------------+
| five_days_ago |
+---------------+
| 2008-12-20    |
+---------------+
```

### DATE_DIFF

```sql
DATE_DIFF(date_expression_a, date_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
`DATE` objects (`date_expression_a` - `date_expression_b`).
If the first `DATE` is earlier than the second one,
the output is negative.

`DATE_DIFF` supports the following `date_part` values:

+  `DAY`
+  `WEEK` This date part begins on Sunday.
+  `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
   `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
   boundaries. ISO weeks begin on Monday.
+  `MONTH`, except when the first two arguments are `TIMESTAMP` objects.
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
SELECT DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY) AS days_diff;

+-----------+
| days_diff |
+-----------+
| 559       |
+-----------+
```

```sql
SELECT
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', DAY) AS days_diff,
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', WEEK) AS weeks_diff;

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

```sql
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
Sunday. `DATE_DIFF` with the date part `WEEK` returns 0 because this date part
uses weeks that begin on Sunday. `DATE_DIFF` with the date part `WEEK(MONDAY)`
returns 1. `DATE_DIFF` with the date part `ISOWEEK` also returns 1 because
ISO weeks begin on Monday.

```sql
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

### DATE_TRUNC

```sql
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

```sql
SELECT DATE_TRUNC(DATE '2008-12-25', MONTH) AS month;

+------------+
| month      |
+------------+
| 2008-12-01 |
+------------+
```

In the following example, the original date falls on a Sunday. Because
the `date_part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATE` for the
preceding Monday.

```sql
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

```sql
SELECT
  DATE_TRUNC('2015-06-15', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATE '2015-06-15') AS isoyear_number;

+------------------+----------------+
| isoyear_boundary | isoyear_number |
+------------------+----------------+
| 2014-12-29       | 2015           |
+------------------+----------------+
```

### DATE_FROM_UNIX_DATE

```sql
DATE_FROM_UNIX_DATE(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of days since 1970-01-01.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_FROM_UNIX_DATE(14238) AS date_from_epoch;

+-----------------+
| date_from_epoch |
+-----------------+
| 2008-12-25      |
+-----------------+
```

### FORMAT_DATE

```sql
FORMAT_DATE(format_string, date_expr)
```

**Description**

Formats the `date_expr` according to the specified `format_string`.

See [Supported Format Elements For DATE][date-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Examples**

```sql
SELECT FORMAT_DATE("%x", DATE "2008-12-25") AS US_format;

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

### LAST_DAY

```sql
LAST_DAY(date_expression[, date_part])
```

**Description**

Returns the last day from a date expression. This is commonly used to return
the last day of the month.

You can optionally specify the date part for which the last day is returned.
If this parameter is not used, the default value is `MONTH`.
`LAST_DAY` supports the following values for `date_part`:

+  `YEAR`
+  `QUARTER`
+  `MONTH`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `WEEK(<WEEKDAY>)`. `<WEEKDAY>` represents the starting day of the week.
   Valid values are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`. Uses [ISO 8601][ISO-8601-week] week boundaries. ISO weeks begin
   on Monday.
+  `ISOYEAR`. Uses the [ISO 8601][ISO-8601] week-numbering year boundary.
   The ISO year boundary is the Monday of the first week whose Thursday belongs
   to the corresponding Gregorian calendar year.

**Return Data Type**

`DATE`

**Example**

These both return the last day of the month:

```sql
SELECT LAST_DAY(DATE '2008-11-25', MONTH) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

```sql
SELECT LAST_DAY(DATE '2008-11-25') AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

This returns the last day of the year:

```sql
SELECT LAST_DAY(DATE '2008-11-25', YEAR) AS last_day

+------------+
| last_day   |
+------------+
| 2008-12-31 |
+------------+
```

This returns the last day of the week for a week that starts on a Sunday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(SUNDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-15 |
+------------+
```

This returns the last day of the week for a week that starts on a Monday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(MONDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-16 |
+------------+
```

### PARSE_DATE

```sql
PARSE_DATE(format_string, date_string)
```

**Description**

Converts a [string representation of date][date-format] to a
`DATE` object.

`format_string` contains the [format elements][date-format-elements]
that define how `date_string` is formatted. Each element in
`date_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `date_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_DATE("%A %b %e %Y", "Thursday Dec 25 2008")

-- This doesn't work because the year element is in different locations.
SELECT PARSE_DATE("%Y %A %b %e", "Thursday Dec 25 2008")

-- This doesn't work because one of the year elements is missing.
SELECT PARSE_DATE("%A %b %e", "Thursday Dec 25 2008")

-- This works because %F can find all matching elements in date_string.
SELECT PARSE_DATE("%F", "2000-12-30")
```

The format string fully supports most format elements except for
`%a`, `%A`, `%g`, `%G`, `%j`, `%u`, `%U`, `%V`, `%w`, and `%W`.

When using `PARSE_DATE`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01`.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the date string. In
  addition, leading and trailing white spaces in the date string are always
  allowed -- even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones.

**Return Data Type**

DATE

**Examples**

This example converts a `MM/DD/YY` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE("%x", "12/25/08") AS parsed;

+------------+
| parsed     |
+------------+
| 2008-12-25 |
+------------+
```

This example converts a `YYYYMMDD` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE("%Y%m%d", "20081225") AS parsed;

+------------+
| parsed     |
+------------+
| 2008-12-25 |
+------------+
```

### UNIX_DATE

```sql
UNIX_DATE(date_expression)
```

**Description**

Returns the number of days since 1970-01-01.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_DATE(DATE "2008-12-25") AS days_from_epoch;

+-----------------+
| days_from_epoch |
+-----------------+
| 14238           |
+-----------------+
```

### Supported format elements for DATE

Unless otherwise noted, DATE functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
    <td>Wednesday</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
    <td>Wed</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
    <td>January</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
    <td>Jan</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal
number (00-99).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
    <td>2021-01-20</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
    <td>020</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
    <td>01</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
    <td>1</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
   <td>3</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
    <td>3</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y produces as many
    characters as it takes to fully render the year.</td>
    <td>2021</td>
 </tr>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[date-format]: #format_date

[date-format-elements]: #supported_format_elements_for_date

[date-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[date-functions-link-to-timezone-definitions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md#timezone_definitions

<!-- mdlint on -->

