

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Timestamp functions

ZetaSQL supports the following timestamp functions.

IMPORTANT: Before working with these functions, you need to understand
the difference between the formats in which timestamps are stored and displayed,
and how time zones are used for the conversion between these formats.
To learn more, see
[How time zones work with timestamp functions][timestamp-link-to-timezone-definitions].

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined [`DATE` range][data-types-link-to-date_type]
and [`TIMESTAMP` range][data-types-link-to-timestamp_type].

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#current_timestamp"><code>CURRENT_TIMESTAMP</code></a>

</td>
  <td>
    Returns the current date and time as a <code>TIMESTAMP</code> object.
  </td>
</tr>

<tr>
  <td><a href="#extract"><code>EXTRACT</code></a>

</td>
  <td>
    Extracts part of a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#format_timestamp"><code>FORMAT_TIMESTAMP</code></a>

</td>
  <td>
    Formats a <code>TIMESTAMP</code> value according to the specified
    format string.
  </td>
</tr>

<tr>
  <td><a href="#parse_timestamp"><code>PARSE_TIMESTAMP</code></a>

</td>
  <td>
    Converts a <code>STRING</code> value to a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#string"><code>STRING</code></a>

</td>
  <td>
    Converts a <code>TIMESTAMP</code> value to a <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#timestamp"><code>TIMESTAMP</code></a>

</td>
  <td>
    Constructs a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_add"><code>TIMESTAMP_ADD</code></a>

</td>
  <td>
    Adds a specified time interval to a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_diff"><code>TIMESTAMP_DIFF</code></a>

</td>
  <td>
    Gets the number of unit boundaries between two <code>TIMESTAMP</code> values
    at a particular time granularity.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_from_unix_micros"><code>TIMESTAMP_FROM_UNIX_MICROS</code></a>

</td>
  <td>
    Similar to <code>TIMESTAMP_MICROS</code>, except that additionally, a
    <code>TIMESTAMP</code> value can be passed in.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_from_unix_millis"><code>TIMESTAMP_FROM_UNIX_MILLIS</code></a>

</td>
  <td>
    Similar to <code>TIMESTAMP_MILLIS</code>, except that additionally, a
    <code>TIMESTAMP</code> value can be passed in.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_from_unix_seconds"><code>TIMESTAMP_FROM_UNIX_SECONDS</code></a>

</td>
  <td>
    Similar to <code>TIMESTAMP_SECONDS</code>, except that additionally, a
    <code>TIMESTAMP</code> value can be passed in.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_micros"><code>TIMESTAMP_MICROS</code></a>

</td>
  <td>
    Converts the number of microseconds since
    1970-01-01 00:00:00 UTC to a <code>TIMESTAMP</value>.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_millis"><code>TIMESTAMP_MILLIS</code></a>

</td>
  <td>
    Converts the number of milliseconds since
    1970-01-01 00:00:00 UTC to a <code>TIMESTAMP</value>.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_seconds"><code>TIMESTAMP_SECONDS</code></a>

</td>
  <td>
    Converts the number of seconds since
    1970-01-01 00:00:00 UTC to a <code>TIMESTAMP</value>.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_sub"><code>TIMESTAMP_SUB</code></a>

</td>
  <td>
    Subtracts a specified time interval from a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#timestamp_trunc"><code>TIMESTAMP_TRUNC</code></a>

</td>
  <td>
    Truncates a <code>TIMESTAMP</code> value.
  </td>
</tr>

<tr>
  <td><a href="#unix_micros"><code>UNIX_MICROS</code></a>

</td>
  <td>
    Converts a <code>TIMESTAMP</code> value to the number of microseconds since
    1970-01-01 00:00:00 UTC.
  </td>
</tr>

<tr>
  <td><a href="#unix_millis"><code>UNIX_MILLIS</code></a>

</td>
  <td>
    Converts a <code>TIMESTAMP</code> value to the number of milliseconds
    since 1970-01-01 00:00:00 UTC.
  </td>
</tr>

<tr>
  <td><a href="#unix_seconds"><code>UNIX_SECONDS</code></a>

</td>
  <td>
    Converts a <code>TIMESTAMP</code> value to the number of seconds since
    1970-01-01 00:00:00 UTC.
  </td>
</tr>

  </tbody>
</table>

### `CURRENT_TIMESTAMP`

```sql
CURRENT_TIMESTAMP()
```

```sql
CURRENT_TIMESTAMP
```

**Description**

Returns the current date and time as a timestamp object. The timestamp is
continuous, non-ambiguous, has exactly 60 seconds per minute and does not repeat
values over the leap second. Parentheses are optional.

This function handles leap seconds by smearing them across a window of 20 hours
around the inserted leap second.

The current date and time is recorded at the start of the query
statement which contains this function, not when this specific function is
evaluated.

**Supported Input Types**

Not applicable

**Result Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT CURRENT_TIMESTAMP() AS now;

/*---------------------------------------------*
 | now                                         |
 +---------------------------------------------+
 | 2020-06-02 17:00:53.110 America/Los_Angeles |
 *---------------------------------------------*/
```

[timestamp-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

### `EXTRACT`

```sql
EXTRACT(part FROM timestamp_expression [AT TIME ZONE time_zone])
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `timestamp_expression`. This function supports an optional
`time_zone` parameter. See
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
+ <code>DATETIME</code>
+ <code>TIME</code>

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`, except in the following cases:

+ If `part` is `DATE`, the function returns a `DATE` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
time part.

```sql
SELECT
  EXTRACT(
    DAY
    FROM TIMESTAMP('2008-12-25 05:30:00+00') AT TIME ZONE 'UTC')
    AS the_day_utc,
  EXTRACT(
    DAY
    FROM TIMESTAMP('2008-12-25 05:30:00+00') AT TIME ZONE 'America/Los_Angeles')
    AS the_day_california

/*-------------+--------------------*
 | the_day_utc | the_day_california |
 +-------------+--------------------+
 | 25          | 24                 |
 *-------------+--------------------*/
```

In the following examples, `EXTRACT` returns values corresponding to different
time parts from a column of type `TIMESTAMP`.

```sql
SELECT
  EXTRACT(ISOYEAR FROM TIMESTAMP("2005-01-03 12:34:56+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2005-01-03 12:34:56+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2005-01-03 12:34:56+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2005-01-03 12:34:56+00")) AS week

-- Display of results may differ, depending upon the environment and
-- time zone where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2005    | 1       | 2005 | 1    |
 *---------+---------+------+------*/
```

```sql
SELECT
  TIMESTAMP("2007-12-31 12:00:00+00") AS timestamp_value,
  EXTRACT(ISOYEAR FROM TIMESTAMP("2007-12-31 12:00:00+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2007-12-31 12:00:00+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2007-12-31 12:00:00+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2007-12-31 12:00:00+00")) AS week

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2008    | 1       | 2007 | 52    |
 *---------+---------+------+------*/
```

```sql
SELECT
  TIMESTAMP("2009-01-01 12:00:00+00") AS timestamp_value,
  EXTRACT(ISOYEAR FROM TIMESTAMP("2009-01-01 12:00:00+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2009-01-01 12:00:00+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2009-01-01 12:00:00+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2009-01-01 12:00:00+00")) AS week

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2009    | 1       | 2009 | 0    |
 *---------+---------+------+------*/
```

```sql
SELECT
  TIMESTAMP("2009-12-31 12:00:00+00") AS timestamp_value,
  EXTRACT(ISOYEAR FROM TIMESTAMP("2009-12-31 12:00:00+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2009-12-31 12:00:00+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2009-12-31 12:00:00+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2009-12-31 12:00:00+00")) AS week

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2009    | 53      | 2009 | 52   |
 *---------+---------+------+------*/
```

```sql
SELECT
  TIMESTAMP("2017-01-02 12:00:00+00") AS timestamp_value,
  EXTRACT(ISOYEAR FROM TIMESTAMP("2017-01-02 12:00:00+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2017-01-02 12:00:00+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2017-01-02 12:00:00+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2017-01-02 12:00:00+00")) AS week

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2017    | 1       | 2017 | 1    |
 *---------+---------+------+------*/
```

```sql
SELECT
  TIMESTAMP("2017-05-26 12:00:00+00") AS timestamp_value,
  EXTRACT(ISOYEAR FROM TIMESTAMP("2017-05-26 12:00:00+00")) AS isoyear,
  EXTRACT(ISOWEEK FROM TIMESTAMP("2017-05-26 12:00:00+00")) AS isoweek,
  EXTRACT(YEAR FROM TIMESTAMP("2017-05-26 12:00:00+00")) AS year,
  EXTRACT(WEEK FROM TIMESTAMP("2017-05-26 12:00:00+00")) AS week

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*---------+---------+------+------*
 | isoyear | isoweek | year | week |
 +---------+---------+------+------+
 | 2017    | 21      | 2017 | 21   |
 *---------+---------+------+------*/
```

In the following example, `timestamp_expression` falls on a Monday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
SELECT
  EXTRACT(WEEK(SUNDAY) FROM TIMESTAMP("2017-11-06 00:00:00+00")) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM TIMESTAMP("2017-11-06 00:00:00+00")) AS week_monday

-- Display of results may differ, depending upon the environment and time zone
-- where this query was executed.
/*-------------+---------------*
 | week_sunday | week_monday   |
 +-------------+---------------+
 | 45          | 44            |
 *-------------+---------------*/
```

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `FORMAT_TIMESTAMP`

```sql
FORMAT_TIMESTAMP(format_string, timestamp[, time_zone])
```

**Description**

Formats a timestamp according to the specified `format_string`.

See [Format elements for date and time parts][timestamp-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2050-12-25 15:30:55+00", "UTC")
  AS formatted;

/*--------------------------*
 | formatted                |
 +--------------------------+
 | Sun Dec 25 15:30:55 2050 |
 *--------------------------*/
```

```sql
SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2050-12-25 15:30:55+00")
  AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec-25-2050 |
 *-------------*/
```

```sql
SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2050-12-25 15:30:55+00")
  AS formatted;

/*-------------*
 | formatted   |
 +-------------+
 | Dec 2050    |
 *-------------*/
```

```sql
SELECT FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", TIMESTAMP "2050-12-25 15:30:55", "UTC")
  AS formatted;

/*+---------------------*
 |      formatted       |
 +----------------------+
 | 2050-12-25T15:30:55Z |
 *----------------------*/
```

[timestamp-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `PARSE_TIMESTAMP`

```sql
PARSE_TIMESTAMP(format_string, timestamp_string[, time_zone])
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
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008");

-- This produces an error because the year element is in different locations.
SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008");

-- This produces an error because one of the year elements is missing.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008");

-- This works because %c can find all matching elements in timestamp_string.
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008");
```

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
/*---------------------------------------------*
 | parsed                                      |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

[timestamp-format]: #format_timestamp

[timestamp-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

### `STRING`

```sql
STRING(timestamp_expression[, time_zone])
```

**Description**

Converts a timestamp to a string. Supports an optional
parameter to specify a time zone. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS string;

/*-------------------------------*
 | string                        |
 +-------------------------------+
 | 2008-12-25 15:30:00+00        |
 *-------------------------------*/
```

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `TIMESTAMP`

```sql
TIMESTAMP(string_expression[, time_zone])
TIMESTAMP(date_expression[, time_zone])
TIMESTAMP(datetime_expression[, time_zone])
```

**Description**

+  `string_expression[, time_zone]`: Converts a string to a
   timestamp. `string_expression` must include a
   timestamp literal.
   If `string_expression` includes a time zone in the timestamp literal, do
   not include an explicit `time_zone`
   argument.
+  `date_expression[, time_zone]`: Converts a date to a timestamp.
   The value returned is the earliest timestamp that falls within
   the given date.
+  `datetime_expression[, time_zone]`: Converts a
   datetime to a timestamp.

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
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 15:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00 UTC") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_str                               |
 +---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP(DATETIME "2008-12-25 15:30:00") AS timestamp_datetime;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_datetime                          |
 +---------------------------------------------+
 | 2008-12-25 15:30:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

```sql
SELECT TIMESTAMP(DATE "2008-12-25") AS timestamp_date;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------*
 | timestamp_date                              |
 +---------------------------------------------+
 | 2008-12-25 00:00:00.000 America/Los_Angeles |
 *---------------------------------------------*/
```

[timestamp-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#timestamp_literals

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `TIMESTAMP_ADD`

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
+ `HOUR`. Equivalent to 60 `MINUTE` parts.
+ `DAY`. Equivalent to 24 `HOUR` parts.

**Return Data Types**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS later;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | original                                    | later                                       |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:40:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

### `TIMESTAMP_DIFF`

```sql
TIMESTAMP_DIFF(end_timestamp, start_timestamp, granularity)
```

**Description**

Gets the number of unit boundaries between two `TIMESTAMP` values
(`end_timestamp` - `start_timestamp`) at a particular time granularity.

**Definitions**

+   `start_timestamp`: The starting `TIMESTAMP` value.
+   `end_timestamp`: The ending `TIMESTAMP` value.
+   `granularity`: The timestamp part that represents the granularity. If
    you passed in `TIMESTAMP` values for the first arguments, `granularity` can
    be:

    
    + `NANOSECOND`
      (if the SQL engine supports it)
    + `MICROSECOND`
    + `MILLISECOND`
    + `SECOND`
    + `MINUTE`
    + `HOUR`. Equivalent to 60 `MINUTE`s.
    + `DAY`. Equivalent to 24 `HOUR`s.

**Details**

If `end_timestamp` is earlier than `start_timestamp`, the output is negative.
Produces an error if the computation overflows, such as if the difference
in nanoseconds
between the two `TIMESTAMP` values overflows.

Note: The behavior of the this function follows the type of arguments passed in.
For example, `TIMESTAMP_DIFF(DATE, DATE, PART)`
behaves like `DATE_DIFF(DATE, DATE, PART)`.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIMESTAMP("2010-07-07 10:20:00+00") AS later_timestamp,
  TIMESTAMP("2008-12-25 15:30:00+00") AS earlier_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00+00", TIMESTAMP "2008-12-25 15:30:00+00", HOUR) AS hours;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------+-------*
 | later_timestamp                             | earlier_timestamp                           | hours |
 +---------------------------------------------+---------------------------------------------+-------+
 | 2010-07-07 03:20:00.000 America/Los_Angeles | 2008-12-25 07:30:00.000 America/Los_Angeles | 13410 |
 *---------------------------------------------+---------------------------------------------+-------*/
```

In the following example, the first timestamp occurs before the
second timestamp, resulting in a negative output.

```sql
SELECT TIMESTAMP_DIFF(TIMESTAMP "2018-08-14", TIMESTAMP "2018-10-14", DAY) AS negative_diff;

/*---------------*
 | negative_diff |
 +---------------+
 | -61           |
 *---------------*/
```

In this example, the result is 0 because only the number of whole specified
`HOUR` intervals are included.

```sql
SELECT TIMESTAMP_DIFF("2001-02-01 01:00:00", "2001-02-01 00:00:01", HOUR) AS diff;

/*---------------*
 | diff          |
 +---------------+
 | 0             |
 *---------------*/
```

### `TIMESTAMP_FROM_UNIX_MICROS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_FROM_UNIX_MILLIS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_FROM_UNIX_SECONDS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_MICROS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_MILLIS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_SECONDS`

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
/*------------------------*
 | timestamp_value        |
 +------------------------+
 | 2008-12-25 15:30:00+00 |
 *------------------------*/
```

### `TIMESTAMP_SUB`

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
+ `HOUR`. Equivalent to 60 `MINUTE` parts.
+ `DAY`. Equivalent to 24 `HOUR` parts.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS earlier;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | original                                    | earlier                                     |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:20:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

### `TIMESTAMP_TRUNC`

```sql
TIMESTAMP_TRUNC(timestamp_expression, granularity[, time_zone])
```

**Description**

Truncates a `TIMESTAMP` value at a particular time granularity. The `TIMESTAMP`
value is always rounded to the beginning of `granularity`.

**Definitions**

+ `timestamp_expression`: The `TIMESTAMP` value to truncate.
+ `granularity`: The datetime part that represents the granularity. If
  you passed in a `TIMESTAMP` value for the first argument, `granularity` can
  be:

  + `NANOSECOND`: If used, nothing is truncated from the value.

  + `MICROSECOND`: The nearest lesser than or equal microsecond.

  + `MILLISECOND`: The nearest lesser than or equal millisecond.

  + `SECOND`: The nearest lesser than or equal second.

  + `MINUTE`: The nearest lesser than or equal minute.

  + `HOUR`: The nearest lesser than or equal hour.

  + `DAY`: The day in the Gregorian calendar year that contains the
    `TIMESTAMP` value.

  + `WEEK`: The first day in the week that contains the
    `TIMESTAMP` value. Weeks begin on Sundays. `WEEK` is equivalent to
    `WEEK(SUNDAY)`.

  + `WEEK(WEEKDAY)`: The first day in the week that contains the
    `TIMESTAMP` value. Weeks begin on `WEEKDAY`. `WEEKDAY` must be one of the
     following: `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`,
     or `SATURDAY`.

  + `ISOWEEK`: The first day in the [ISO 8601 week][ISO-8601-week] that contains
    the `TIMESTAMP` value. The ISO week begins on
    Monday. The first ISO week of each ISO year contains the first Thursday of the
    corresponding Gregorian calendar year.

  + `MONTH`: The first day in the month that contains the
    `TIMESTAMP` value.

  + `QUARTER`: The first day in the quarter that contains the
    `TIMESTAMP` value.

  + `YEAR`: The first day in the year that contains the
    `TIMESTAMP` value.

  + `ISOYEAR`: The first day in the [ISO 8601][ISO-8601] week-numbering year
    that contains the `TIMESTAMP` value. The ISO year is the
    Monday of the first week where Thursday belongs to the corresponding
    Gregorian calendar year.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

<!-- mdlint on -->

+ `time_zone`: Use this parameter if you want to use a time zone other than
  the default time zone, which is implementation defined, as part of the
  truncate operation. This can be:

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

When truncating a timestamp to `MINUTE`
or`HOUR` parts, `TIMESTAMP_TRUNC` determines the civil time of the
timestamp in the specified (or default) time zone
and subtracts the minutes and seconds (when truncating to `HOUR`) or the seconds
(when truncating to `MINUTE`) from that timestamp.
While this provides intuitive results in most cases, the result is
non-intuitive near daylight savings transitions that are not hour-aligned.

**Return Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC") AS utc,
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "America/Los_Angeles") AS la;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------*
 | utc                                         | la                                          |
 +---------------------------------------------+---------------------------------------------+
 | 2008-12-24 16:00:00.000 America/Los_Angeles | 2008-12-25 00:00:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------*/
```

In the following example, `timestamp_expression` has a time zone offset of +12.
The first column shows the `timestamp_expression` in UTC time. The second
column shows the output of `TIMESTAMP_TRUNC` using weeks that start on Monday.
Because the `timestamp_expression` falls on a Sunday in UTC, `TIMESTAMP_TRUNC`
truncates it to the preceding Monday. The third column shows the same function
with the optional [Time zone definition][timestamp-link-to-timezone-definitions]
argument 'Pacific/Auckland'. Here, the function truncates the
`timestamp_expression` using New Zealand Daylight Time, where it falls on a
Monday.

```sql
SELECT
  timestamp_value AS timestamp_value,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "UTC") AS utc_truncated,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "Pacific/Auckland") AS nzdt_truncated
FROM (SELECT TIMESTAMP("2017-11-06 00:00:00+12") AS timestamp_value);

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
/*---------------------------------------------+---------------------------------------------+---------------------------------------------*
 | timestamp_value                             | utc_truncated                               | nzdt_truncated                              |
 +---------------------------------------------+---------------------------------------------+---------------------------------------------+
 | 2017-11-05 04:00:00.000 America/Los_Angeles | 2017-10-29 17:00:00.000 America/Los_Angeles | 2017-11-05 03:00:00.000 America/Los_Angeles |
 *---------------------------------------------+---------------------------------------------+---------------------------------------------*/
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
/*---------------------------------------------+----------------*
 | isoyear_boundary                            | isoyear_number |
 +---------------------------------------------+----------------+
 | 2014-12-29 00:00:00.000 America/Los_Angeles | 2015           |
 *---------------------------------------------+----------------*/
```

[timestamp-link-to-timezone-definitions]: #timezone_definitions

### `UNIX_MICROS`

```sql
UNIX_MICROS(timestamp_expression)
```

**Description**

Returns the number of microseconds since `1970-01-01 00:00:00 UTC`.
Truncates higher levels of precision by
rounding down to the beginning of the microsecond.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00") AS micros;

/*------------------*
 | micros           |
 +------------------+
 | 1230219000000000 |
 *------------------*/
```

```sql
SELECT UNIX_MICROS(TIMESTAMP "1970-01-01 00:00:00.0000018+00") AS micros;

/*------------------*
 | micros           |
 +------------------+
 | 1                |
 *------------------*/
```

### `UNIX_MILLIS`

```sql
UNIX_MILLIS(timestamp_expression)
```

**Description**

Returns the number of milliseconds since `1970-01-01 00:00:00 UTC`. Truncates
higher levels of precision by rounding down to the beginning of the millisecond.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00") AS millis;

/*---------------*
 | millis        |
 +---------------+
 | 1230219000000 |
 *---------------*/
```

```sql
SELECT UNIX_MILLIS(TIMESTAMP "1970-01-01 00:00:00.0018+00") AS millis;

/*---------------*
 | millis        |
 +---------------+
 | 1             |
 *---------------*/
```

### `UNIX_SECONDS`

```sql
UNIX_SECONDS(timestamp_expression)
```

**Description**

Returns the number of seconds since `1970-01-01 00:00:00 UTC`. Truncates higher
levels of precision by rounding down to the beginning of the second.

**Return Data Type**

`INT64`

**Examples**

```sql
SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00") AS seconds;

/*------------*
 | seconds    |
 +------------+
 | 1230219000 |
 *------------*/
```

```sql
SELECT UNIX_SECONDS(TIMESTAMP "1970-01-01 00:00:01.8+00") AS seconds;

/*------------*
 | seconds    |
 +------------+
 | 1          |
 *------------*/
```

### How time zones work with timestamp functions 
<a id="timezone_definitions"></a>

A timestamp represents an absolute point in time, independent of any time
zone. However, when a timestamp value is displayed, it is usually converted to
a human-readable format consisting of a civil date and time
(YYYY-MM-DD HH:MM:SS)
and a time zone. This is not the internal representation of the
`TIMESTAMP`; it is only a human-understandable way to describe the point in time
that the timestamp represents.

Some timestamp functions have a time zone argument. A time zone is needed to
convert between civil time (YYYY-MM-DD HH:MM:SS) and the absolute time
represented by a timestamp.
A function like `PARSE_TIMESTAMP` takes an input string that represents a
civil time and returns a timestamp that represents an absolute time. A
time zone is needed for this conversion. A function like `EXTRACT` takes an
input timestamp (absolute time) and converts it to civil time in order to
extract a part of that civil time. This conversion requires a time zone.
If no time zone is specified, the default time zone, which is implementation defined,
is used.

Certain date and timestamp functions allow you to override the default time zone
and specify a different one. You can specify a time zone by either supplying
the time zone name (for example, `America/Los_Angeles`)
or time zone offset from UTC (for example, -08).

To learn more about how time zones work with the `TIMESTAMP` type, see
[Time zones][data-types-timezones].

[timezone-by-name]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[data-types-timezones]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time_zones

[timestamp-link-to-timezone-definitions]: #timezone_definitions

[data-types-link-to-date_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#date_type

[data-types-link-to-timestamp_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#timestamp_type

