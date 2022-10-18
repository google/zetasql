

# Debugging functions

ZetaSQL supports the following debugging functions.

### ERROR

```sql
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
    ELSE ERROR(CONCAT('Found unexpected value: ', value))
  END AS new_value
FROM (
  SELECT 'foo' AS value UNION ALL
  SELECT 'bar' AS value UNION ALL
  SELECT 'baz' AS value);

-- Found unexpected value: baz
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
WHERE IF(x > 0, true, ERROR(FORMAT('Error: x must be positive but is %t', x)));

-- Error: x must be positive but is -1
```

### IFERROR

```sql
IFERROR(try_expression, catch_expression)
```

**Description**

Evaluates `try_expression`.

When `try_expression` is evaluated:

+ If the evaluation of `try_expression` does not produce an error, then
  `IFERROR` returns the result of `try_expression` without evaluating
  `catch_expression`.
+ If the evaluation of `try_expression` produces a system error, then `IFERROR`
  produces that system error.
+ If the evaluation of `try_expression` produces an evaluation error, then
  `IFERROR` suppresses that evaluation error and evaluates `catch_expression`.

If `catch_expression` is evaluated:

+ If the evaluation of `catch_expression` does not produce an error, then
  `IFERROR` returns the result of `catch_expression`.
+ If the evaluation of `catch_expression` produces any error, then `IFERROR`
  produces that error.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.
+ `catch_expression`: An expression that returns a scalar value.

The results of `try_expression` and `catch_expression` must share a
[supertype][supertype].

**Return Data Type**

The [supertype][supertype] for `try_expression` and
`catch_expression`.

**Example**

In the following examples, the query successfully evaluates `try_expression`.

```sql
SELECT IFERROR('a', 'b') AS result

+--------+
| result |
+--------+
| a      |
+--------+
```

```sql
SELECT IFERROR((SELECT [1,2,3][OFFSET(0)]), -1) AS result

+--------+
| result |
+--------+
| 1      |
+--------+
```

In the following examples, `IFERROR` catches an evaluation error in the
`try_expression` and successfully evaluates `catch_expression`.

```sql
SELECT IFERROR(ERROR('a'), 'b') AS result

+--------+
| result |
+--------+
| b      |
+--------+
```

```sql
SELECT IFERROR((SELECT [1,2,3][OFFSET(9)]), -1) AS result

+--------+
| result |
+--------+
| -1     |
+--------+
```

In the following query, the error is handled by the innermost `IFERROR`
operation, `IFERROR(ERROR('a'), 'b')`.

```sql
SELECT IFERROR(IFERROR(ERROR('a'), 'b'), 'c') AS result

+--------+
| result |
+--------+
| b      |
+--------+
```

In the following query, the error is handled by the outermost `IFERROR`
operation, `IFERROR(..., 'c')`.

```sql
SELECT IFERROR(IFERROR(ERROR('a'), ERROR('b')), 'c') AS result

+--------+
| result |
+--------+
| c      |
+--------+
```

In the following example, an evaluation error is produced because the subquery
passed in as the `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT IFERROR((SELECT e FROM UNNEST([1, 2]) AS e), 3) AS result

+--------+
| result |
+--------+
| 3      |
+--------+
```

In the following example, `IFERROR` catches an evaluation error in `ERROR('a')`
and then evaluates `ERROR('b')`. Because there is also an evaluation error in
`ERROR('b')`, `IFERROR` produces an evaluation error for `ERROR('b')`.

```sql
SELECT IFERROR(ERROR('a'), ERROR('b')) AS result

--ERROR: OUT_OF_RANGE 'b'
```

[supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

### ISERROR

```sql
ISERROR(try_expression)
```

**Description**

Evaluates `try_expression`.

+ If the evaluation of `try_expression` does not produce an error, then
  `ISERROR` returns `FALSE`.
+ If the evaluation of `try_expression` produces a system error, then `ISERROR`
  produces that system error.
+ If the evaluation of `try_expression` produces an evaluation error, then
  `ISERROR` returns `TRUE`.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.

**Return Data Type**

`BOOL`

**Example**

In the following examples, `ISERROR` successfully evaluates `try_expression`.

```sql
SELECT ISERROR('a') AS is_error

+----------+
| is_error |
+----------+
| false    |
+----------+
```

```sql
SELECT ISERROR(2/1) AS is_error

+----------+
| is_error |
+----------+
| false    |
+----------+
```

```sql
SELECT ISERROR((SELECT [1,2,3][OFFSET(0)])) AS is_error

+----------+
| is_error |
+----------+
| false    |
+----------+
```

In the following examples, `ISERROR` catches an evaluation error in
`try_expression`.

```sql
SELECT ISERROR(ERROR('a')) AS is_error

+----------+
| is_error |
+----------+
| true     |
+----------+
```

```sql
SELECT ISERROR(2/0) AS is_error

+----------+
| is_error |
+----------+
| true     |
+----------+
```

```sql
SELECT ISERROR((SELECT [1,2,3][OFFSET(9)])) AS is_error

+----------+
| is_error |
+----------+
| true     |
+----------+
```

In the following example, an evaluation error is produced because the subquery
passed in as `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT ISERROR((SELECT e FROM UNNEST([1, 2]) AS e)) AS is_error

+----------+
| is_error |
+----------+
| true     |
+----------+
```

### NULLIFERROR

```sql
NULLIFERROR(try_expression)
```
**Description**

Evaluates `try_expression`.

+ If the evaluation of `try_expression` does not produce an error, then
  `NULLIFERROR` returns the result of `try_expression`.
+ If the evaluation of `try_expression` produces a system error, then
 `NULLIFERROR` produces that system error.

+ If the evaluation of `try_expression` produces an evaluation error, then
  `NULLIFERROR` returns `NULL`.

**Arguments**

+ `try_expression`: An expression that returns a scalar value.

**Return Data Type**

The data type for `try_expression` or `NULL`

**Example**

In the following examples, `NULLIFERROR` successfully evaluates
`try_expression`.

```sql
SELECT NULLIFERROR('a') AS result

+--------+
| result |
+--------+
| a      |
+--------+
```

```sql
SELECT NULLIFERROR((SELECT [1,2,3][OFFSET(0)])) AS result

+--------+
| result |
+--------+
| 1      |
+--------+
```

In the following examples, `NULLIFERROR` catches an evaluation error in
`try_expression`.

```sql
SELECT NULLIFERROR(ERROR('a')) AS result

+--------+
| result |
+--------+
| NULL   |
+--------+
```

```sql
SELECT NULLIFERROR((SELECT [1,2,3][OFFSET(9)])) AS result

+--------+
| result |
+--------+
| NULL   |
+--------+
```

In the following example, an evaluation error is produced because the subquery
passed in as `try_expression` evaluates to a table, not a scalar value.

```sql
SELECT NULLIFERROR((SELECT e FROM UNNEST([1, 2]) AS e)) AS result

+--------+
| result |
+--------+
| NULL   |
+--------+
```

