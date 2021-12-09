

# Timestamp functions

ZetaSQL supports the following `TIMESTAMP` functions.

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined [date][data-types-link-to-date_type]
and [timestamp][data-types-link-to-timestamp_type] min/max values.

### CURRENT_TIMESTAMP

```sql
CURRENT_TIMESTAMP()
```

**Description**

`CURRENT_TIMESTAMP()` produces a TIMESTAMP value that is continuous,
non-ambiguous, has exactly 60 seconds per minute and does not repeat values over
the leap second. Parentheses are optional.

This function handles leap seconds by smearing them across a window of 20 hours
around the inserted leap second.

**Supported Input Types**

Not applicable

**Result Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT CURRENT_TIMESTAMP() as now;

+---------------------------------------------+
| now                                         |
+---------------------------------------------+
| 2020-06-02 17:00:53.110 America/Los_Angeles |
+---------------------------------------------+
```

When a column named `current_timestamp` is present, the column name and the
function call without parentheses are ambiguous. To ensure the function call,
add parentheses; to ensure the column name, qualify it with its
[range variable][timestamp-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_timestamp` column.

```sql
WITH t AS (SELECT 'column value' AS `current_timestamp`)
SELECT current_timestamp() AS now, t.current_timestamp FROM t;

+---------------------------------------------+-------------------+
| now                                         | current_timestamp |
+---------------------------------------------+-------------------+
| 2020-06-02 17:00:53.110 America/Los_Angeles | column value      |
+---------------------------------------------+-------------------+
```

### EXTRACT

```sql
EXTRACT(part FROM timestamp_expression [AT TIME ZONE timezone])
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `timestamp_expression`. This function supports an optional
`timezone` parameter. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day of
   of the week.
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

**Return Data Type**

`INT64`, except when:

+ `part` is `DATE`, returns a `DATE` object.
+ `part` is `DATETIME`, returns a `DATETIME` object.
+ `part` is `TIME`, returns a `TIME` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
time part.

```sql
WITH Input AS (SELECT TIMESTAMP("2008-12-25 05:30:00+00") AS timestamp_value)
SELECT
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "UTC") AS the_day_utc,
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "America/Los_Angeles") AS the_day_california
FROM Input

+-------------+--------------------+
| the_day_utc | the_day_california |
+-------------+--------------------+
| 25          | 24                 |
+-------------+--------------------+
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of timestamps.

```sql
WITH Timestamps AS (
  SELECT TIMESTAMP("2005-01-03 12:34:56+00") AS timestamp_value UNION ALL
  SELECT TIMESTAMP("2007-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-01-01 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-01-02 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-05-26 12:00:00+00")
)
SELECT
  timestamp_value,
  EXTRACT(ISOYEAR FROM timestamp_value) AS isoyear,
  EXTRACT(ISOWEEK FROM timestamp_value) AS isoweek,
  EXTRACT(YEAR FROM timestamp_value) AS year,
  EXTRACT(WEEK FROM timestamp_value) AS week
FROM Timestamps
ORDER BY timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------+---------+------+------+
| timestamp_value                             | isoyear | isoweek | year | week |
+---------------------------------------------+---------+---------+------+------+
| 2005-01-03 04:34:56.000 America/Los_Angeles | 2005    | 1       | 2005 | 1    |
| 2007-12-31 04:00:00.000 America/Los_Angeles | 2008    | 1       | 2007 | 52   |
| 2009-01-01 04:00:00.000 America/Los_Angeles | 2009    | 1       | 2009 | 0    |
| 2009-12-31 04:00:00.000 America/Los_Angeles | 2009    | 53      | 2009 | 52   |
| 2017-01-02 04:00:00.000 America/Los_Angeles | 2017    | 1       | 2017 | 1    |
| 2017-05-26 05:00:00.000 America/Los_Angeles | 2017    | 21      | 2017 | 21   |
+---------------------------------------------+---------+---------+------+------+
```

In the following example, `timestamp_expression` falls on a Monday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT TIMESTAMP("2017-11-06 00:00:00+00") AS timestamp_value)
SELECT
  timestamp_value,
  EXTRACT(WEEK(SUNDAY) FROM timestamp_value) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM timestamp_value) AS week_monday
FROM table;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+-------------+---------------+
| timestamp_value                             | week_sunday | week_monday   |
+---------------------------------------------+-------------+---------------+
| 2017-11-05 16:00:00.000 America/Los_Angeles | 45          | 44            |
+---------------------------------------------+-------------+---------------+
```

### STRING

```sql
STRING(timestamp_expression[, timezone])
```

**Description**

Converts a `timestamp_expression` to a STRING data type. Supports an optional
parameter to specify a time zone. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS string;

+-------------------------------+
| string                        |
+-------------------------------+
| 2008-12-25 15:30:00+00        |
+-------------------------------+
```

### TIMESTAMP

```sql
TIMESTAMP(string_expression[, timezone])
TIMESTAMP(date_expression[, timezone])
TIMESTAMP(datetime_expression[, timezone])
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

`TIMESTAMP`

**Examples**

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00+00") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 15:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00 UTC") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP(DATETIME "2008-12-25 15:30:00") AS timestamp_datetime;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_datetime                          |
+---------------------------------------------+
| 2008-12-25 15:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP(DATE "2008-12-25") AS timestamp_date;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_date                              |
+---------------------------------------------+
| 2008-12-25 00:00:00.000 America/Los_Angeles |
+---------------------------------------------+
```

### TIMESTAMP_ADD

```sql
TIMESTAMP_ADD(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds `int64_expression` units of `date_part` to the timestamp, independent of
any time zone.

`TIMESTAMP_ADD` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Types**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS later;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| original                                    | later                                       |
+---------------------------------------------+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:40:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

### TIMESTAMP_SUB

```sql
TIMESTAMP_SUB(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts `int64_expression` units of `date_part` from the timestamp,
independent of any time zone.

`TIMESTAMP_SUB` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS earlier;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| original                                    | earlier                                     |
+---------------------------------------------+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:20:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

### TIMESTAMP_DIFF

```sql
TIMESTAMP_DIFF(timestamp_expression_a, timestamp_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
`TIMESTAMP` objects (`timestamp_expression_a` - `timestamp_expression_b`).
If the first `TIMESTAMP` is earlier than the second one,
the output is negative. Throws an error if the computation overflows the
result type, such as if the difference in
nanoseconds
between the two `TIMESTAMP` objects would overflow an
`INT64` value.

`TIMESTAMP_DIFF` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIMESTAMP("2010-07-07 10:20:00+00") AS later_timestamp,
  TIMESTAMP("2008-12-25 15:30:00+00") AS earlier_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00+00", TIMESTAMP "2008-12-25 15:30:00+00", HOUR) AS hours;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+-------+
| later_timestamp                             | earlier_timestamp                           | hours |
+---------------------------------------------+---------------------------------------------+-------+
| 2010-07-07 03:20:00.000 America/Los_Angeles | 2008-12-25 07:30:00.000 America/Los_Angeles | 13410 |
+---------------------------------------------+---------------------------------------------+-------+
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

In this example, the result is 0 because only the number of whole specified
`HOUR` intervals are included.

```sql
SELECT TIMESTAMP_DIFF("2001-02-01 01:00:00", "2001-02-01 00:00:01", HOUR)

+---------------+
| negative_diff |
+---------------+
| 0             |
+---------------+
```

### TIMESTAMP_TRUNC

```sql
TIMESTAMP_TRUNC(timestamp_expression, date_part[, timezone])
```

**Description**

Truncates a timestamp to the granularity of `date_part`.

`TIMESTAMP_TRUNC` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
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

`TIMESTAMP_TRUNC` function supports an optional `timezone` parameter. This
parameter applies to the following `date_parts`:

+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`
+ `ISOWEEK`
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`

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

`TIMESTAMP`

**Examples**

```sql
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC") AS utc,
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "America/Los_Angeles") AS la;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| utc                                         | la                                          |
+---------------------------------------------+---------------------------------------------+
| 2008-12-24 16:00:00.000 America/Los_Angeles | 2008-12-25 00:00:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

In the following example, `timestamp_expression` has a time zone offset of +12.
The first column shows the `timestamp_expression` in UTC time. The second
column shows the output of `TIMESTAMP_TRUNC` using weeks that start on Monday.
Because the `timestamp_expression` falls on a Sunday in UTC, `TIMESTAMP_TRUNC`
truncates it to the preceding Monday. The third column shows the same function
with the optional [Time zone definition][timestamp-link-to-timezone-definitions]
argument 'Pacific/Auckland'. Here the function truncates the
`timestamp_expression` using New Zealand Daylight Time, where it falls on a
Monday.

```sql
SELECT
  timestamp_value AS timestamp_value,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "UTC") AS utc_truncated,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "Pacific/Auckland") AS nzdt_truncated
FROM (SELECT TIMESTAMP("2017-11-06 00:00:00+12") AS timestamp_value);

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
| timestamp_value                             | utc_truncated                               | nzdt_truncated                              |
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
| 2017-11-05 04:00:00.000 America/Los_Angeles | 2017-10-29 17:00:00.000 America/Los_Angeles | 2017-11-05 03:00:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
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
  TIMESTAMP_TRUNC("2015-06-15 00:00:00+00", ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM TIMESTAMP "2015-06-15 00:00:00+00") AS isoyear_number;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+----------------+
| isoyear_boundary                            | isoyear_number |
+---------------------------------------------+----------------+
| 2014-12-29 00:00:00.000 America/Los_Angeles | 2015           |
+---------------------------------------------+----------------+
```

### FORMAT_TIMESTAMP

```sql
FORMAT_TIMESTAMP(format_string, timestamp[, timezone])
```

**Description**

Formats a timestamp according to the specified `format_string`.

See [Supported Format Elements For TIMESTAMP][timestamp-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS formatted;

+--------------------------+
| formatted                |
+--------------------------+
| Thu Dec 25 15:30:00 2008 |
+--------------------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00+00") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2008-12-25 15:30:00+00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

### PARSE_TIMESTAMP

```sql
PARSE_TIMESTAMP(format_string, timestamp_string[, timezone])
```

**Description**

Converts a [string representation of a timestamp][timestamp-format] to a
`TIMESTAMP` object.

`format_string` contains the [format elements][timestamp-format-elements]
that define how `timestamp_string` is formatted. Each element in
`timestamp_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `timestamp_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because the year element is in different locations.
SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because one of the year elements is missing.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This works because %c can find all matching elements in timestamp_string.
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008")
```

The format string fully
supports most format elements, except for
`%a`, `%A`, `%g`, `%G`, `%j`, `%P`, `%u`, `%U`, `%V`, `%w`, and `%W`.

When using `PARSE_TIMESTAMP`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01
  00:00:00.0`. This initialization value uses the time zone specified by the
  function's time zone argument, if present. If not, the initialization value
  uses the default time zone, which is implementation defined.  For instance, if the year
  is unspecified then it defaults to `1970`, and so on.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the timestamp string. In
  addition, leading and trailing white spaces in the timestamp string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones, with some exceptions (see the
  descriptions of `%s`, `%C`, and `%y`).
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008") AS parsed;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| parsed                                      |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

### TIMESTAMP_SECONDS

```sql
TIMESTAMP_SECONDS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since 1970-01-01 00:00:00
UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
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
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
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
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
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

`INT64`

**Example**

```sql
SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00") AS seconds;

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

`INT64`

**Example**

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00") AS millis;

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

`INT64`

**Example**

```sql
SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00") AS micros;

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

```sql
TIMESTAMP_FROM_UNIX_SECONDS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_FROM_UNIX_MILLIS

```sql
TIMESTAMP_FROM_UNIX_MILLIS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MILLIS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_FROM_UNIX_MICROS

```sql
TIMESTAMP_FROM_UNIX_MICROS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MICROS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
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
    <td>%c</td>
    <td>The date and time representation in the format %a %b %e %T %Y.</td>
    <td>Wed Jan 20 16:47:00 2021</td>
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
    <td>The
    <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
    <td>2021</td>
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
    <td>21</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
    <td>16</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
    <td>04</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
    <td>020</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
    <td>16</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
    <td>11</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
    <td>47</td>
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
    <td>%P</td>
    <td>Either am or pm.</td>
    <td>am</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
    <td>AM</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
    <td>1</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
    <td>16:47</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
    <td>04:47:00 PM</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
    <td>00</td>
 </tr>
 <tr>
    <td>%s</td>
    <td>The number of seconds since 1970-01-01 00:00:00 UTC. Always overrides all
    other format elements, independent of where %s appears in the string.
    If multiple %s elements appear, then the last one takes precedence.</td>
    <td>1611179220</td>
</tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
    <td>16:47:00</td>
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
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
    <td>16:47:00</td>
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
    <td>%Z</td>
    <td>The time zone name.</td>
    <td>UTC-5</td>
 </tr>
 <tr>
    <td>%z</td>
    <td>The offset from the Prime Meridian in the format +HHMM or -HHMM as
    appropriate,
with positive values representing locations east of Greenwich.</td>
    <td>-0500</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
    <td>%</td>
 </tr>
 <tr>
    <td>%Ez</td>
    <td>RFC 3339-compatible numeric time zone (+HH:MM or -HH:MM).</td>
    <td>-05:00</td>
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
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y
    produces as many characters as it takes to fully render the
year.</td>
    <td>2021</td>
 </tr>
</table>

### Time zone definitions 
<a id="timezone_definitions"></a>

Certain date and timestamp functions allow you to override the default time zone
and specify a different one. You can specify a time zone by either supplying
the [time zone name][timezone-by-name] (for example, `America/Los_Angeles`)
or time zone offset from UTC (for example, -08).

If you choose to use a time zone offset, use this format:

```sql
(+|-)H[H][:M[M]]
```

The following timestamps are equivalent because the time zone offset
for `America/Los_Angeles` is `-08` for the specified date and time.

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00 America/Los_Angeles") as millis;
```

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00-08:00") as millis;
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[timezone-by-name]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[timestamp-link-to-timezone-definitions]: #timezone_definitions

[timestamp-format]: #format_timestamp

[timestamp-format-elements]: #supported_format_elements_for_timestamp

[timestamp-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[data-types-link-to-date_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#date_type

[data-types-link-to-timestamp_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#timestamp_type

[timestamp-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#timestamp_literals

<!-- mdlint on -->

