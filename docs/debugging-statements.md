

## ASSERT
```
ASSERT expression [AS description]
```

**Description**

Evaluates `expression`.

If `expression` evaluates to `TRUE`, the statement returns successfully
without any result.

If `expression` evaluates to `FALSE` or `NULL`, the statement generates an
error. If `AS description` is present, `description` will appear in the error
message.

`expression` must evaluate to a `BOOL`.

`description` must be a `STRING` literal.

**Examples**

The following example asserts that the data source contains over five rows. The
statement succeeds.

```sql
ASSERT (
  (SELECT COUNT(*) FROM UNNEST([1, 2, 3, 4, 5, 6])) > 5
) AS 'Table must contain more than 5 rows.';
```

The following example asserts that the source table contains a particular value.

```sql
ASSERT
  EXISTS(
    SELECT X
    FROM UNNEST([7877, 7879, 7883, 7901, 7907]) AS X
    WHERE X = 7919
  )
AS 'Column X must contain the value 7919';
```

The above statement generates this error:

```
Assertion failed: Column X must contain the value 7919
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

