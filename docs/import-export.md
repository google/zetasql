

# Data export

ZetaSQL specifies the syntax for the `EXPORT` statement.

## EXPORT DATA

```
EXPORT DATA
  [OPTIONS (key=value, ...)]
AS query;
```

The `EXPORT DATA` statement writes out a query result. You can use this
statement to write a result to a file or stream the result into another system.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

