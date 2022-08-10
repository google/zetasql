

# Time functions

ZetaSQL supports the following `TIME` functions.

### CURRENT_TIME

```sql
CURRENT_TIME([time_zone])
```

**Description**

Returns the current time as a `TIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `time_zone` parameter.
See [Time zone definitions][time-link-to-timezone-definitions] for information
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
2. TIME(timestamp, [time_zone])
3. TIME(datetime)
```

**Description**

1. Constructs a `TIME` object using `INT64`
   values representing the hour, minute, and second.
2. Constructs a `TIME` object using a `TIMESTAMP` object. It supports an
   optional
   parameter to [specify a time zone][time-link-to-timezone-definitions]. If no
   time zone is specified, the default time zone, which is implementation defined, is
   used.
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
TIME_TRUNC(time_expression, time_part)
```

**Description**

Truncates a `TIME` value to the granularity of `time_part`. The `TIME` value
is always rounded to the beginning of `time_part`, which can be one of the
following:

+ `NANOSECOND`: If used, nothing is truncated from the value.
+ `MICROSECOND`: The nearest lessor or equal microsecond.
+ `MILLISECOND`: The nearest lessor or equal millisecond.
+ `SECOND`: The nearest lessor or equal second.
+ `MINUTE`: The nearest lessor or equal minute.
+ `HOUR`: The nearest lessor or equal hour.

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

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[time-format]: #format_time

[time-format-elements]: https://github.com/google/zetasql/blob/master/docs/format-elements.md#format_elements_date_time

[time-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[time-link-to-timezone-definitions]: https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md#timezone_definitions

[time-to-string]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#cast

<!-- mdlint on -->

