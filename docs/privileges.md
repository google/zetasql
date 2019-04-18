<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Privileges

ZetaSQL specifies the syntax for the privilege statements `GRANT` and
`REVOKE`.

Where possible, this topic provides a link to the engine-specific documentation
for each statement.

## GRANT and REVOKE

```
GRANT privileges ON [object_type] object TO grantees;

REVOKE privileges ON [object_type] object FROM grantees;
```

The `GRANT` and `REVOKE` statements have the same syntax. `GRANT` uses the `TO`
keyword to assign privileges to a grantee, while `REVOKE` uses the `FROM`
keyword to remove privileges from a grantee.

Both of these statements use the following variables:

+ `privileges` is either a comma-separated list of identifiers, or
  `ALL PRIVILEGES`.
+ `object_type` is an optional identifier for the object (for example,
  `TABLE`).
+ `object` identifies the object and supports an optional comma-separated
  list of identifiers in parentheses. These identifiers are usually column
  names.
+ `grantees` is a comma-separated list of strings.

<!-- END CONTENT -->

