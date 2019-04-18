

# Security functions

ZetaSQL supports the following security functions.

### SESSION_USER
```
SESSION_USER()
```

**Description**

Returns the email address of the user that is running the query.

**Return Data Type**

STRING

**Example**

```sql
SELECT SESSION_USER() as user;

+----------------------+
| user                 |
+----------------------+
| jdoe@example.com     |
+----------------------+

```

