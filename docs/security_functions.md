

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Security functions

ZetaSQL supports the following security functions.

### `SESSION_USER`

```
SESSION_USER()
```

**Description**

For first-party users, returns the email address of the user that is running the
query.
For third-party users, returns the
[principal identifier](https://cloud.google.com/iam/docs/principal-identifiers)
of the user that is running the query.
For more information about identities, see
[Principals](https://cloud.google.com/docs/authentication#principal).

**Return Data Type**

`STRING`

**Example**

```sql
SELECT SESSION_USER() as user;

/*----------------------*
 | user                 |
 +----------------------+
 | jdoe@example.com     |
 *----------------------*/
```

