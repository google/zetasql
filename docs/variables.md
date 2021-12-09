

# Variables

ZetaSQL specifies the syntax for the variable statements `SET` and
`UNSET`.

## VARIABLE TYPES

There are three primary variable types: runtime variables, query parameters, and
system variables. Each can be set and unset with the `SET` and `UNSET` commands.
Additionally, query parameters are prefixed with a single @ symbol and system
variables are prefixed with @@.

The variable names must be valid sql [identifiers][link-to-sql-identifiers].

## SET
Sets a variable.

```
SET runtime_variable = constant_value;
SET @query_parameter = constant_value;
SET @@system_variable = constant_value;
```

## UNSET
Unsets a variable.

```
UNSET runtime_variable;
UNSET @query_parameter;
UNSET @@system_variable;
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[link-to-sql-identifiers]: https://github.com/google/zetasql/blob/master/docs/lexical.md#identifiers

<!-- mdlint on -->

