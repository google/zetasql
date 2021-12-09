

# Transactions

ZetaSQL specifies the syntax for transaction statements, such as
`BEGIN`.

Where possible, this topic provides a link to the engine-specific documentation
for each statement.

## BEGIN

```
BEGIN [TRANSACTION][ISOLATION LEVEL isolation_level];
```

Begins a transaction.

This statement supports an optional `ISOLATION LEVEL` clause. Following are the
standard values for `isolation_level`:

+ `READ UNCOMMITTED`
+ `READ COMMITTED`
+ `REPEATABLE READ`
+ `SERIALIZABLE`

**Example**

The following example begins a transaction using the `READ COMMITTED` isolation
level.

```
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

## START TRANSACTION

```
START TRANSACTION [ISOLATION LEVEL isolation_level]
```

Synonymous with [`BEGIN`][begin-transaction].

## COMMIT

```
COMMIT [TRANSACTION];
```

Commits a transaction.

## ROLLBACK

```
ROLLBACK [TRANSACTION];
```

Rolls back a transaction.

[begin-transaction]: #begin

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

