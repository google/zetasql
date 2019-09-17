<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Data Definition Language statements

ZetaSQL specifies the syntax for Data Definition Language (DDL)
statements.

Where possible, this topic provides a link to the product-specific documentation
for each statement.

## CREATE DATABASE

<pre>
CREATE
  DATABASE
  database_name
  [OPTIONS (key=value, ...)]
</pre>

**Description**

The `CREATE DATABASE` statement creates a database. If you have schema
options, you can add them when you create the database. These options are
system-specific and follow the ZetaSQL
[`HINT` syntax](lexical.md#hints).

**Example**

```sql
CREATE DATABASE library OPTIONS(
  base_dir=`/city/orgs`,
  owner='libadmin'
);

+--------------------+
| Database           |
+--------------------+
| library            |
+--------------------+
```

## CREATE TABLE

<pre>
CREATE
   [ OR REPLACE ]
   [ TEMP | TEMPORARY ]
   TABLE
   [ IF NOT EXISTS ]
   <span class="var">path_expression</span> [ ( <span class="var">table_element</span>, ...) ]
   [ OPTIONS (...) ]
   [ AS <span class="var">query</span> ];

<span class="var">table_element:</span>
   <span class="var">column_definition</span> | <span class="var">primary_key_spec</span>

<span class="var">column_definition:</span>
   <span class="var">column_name</span> <span class="var">column_type</span> [ PRIMARY KEY ] [ OPTIONS (...) ]

<span class="var">primary_key_spec:</span>
   PRIMARY KEY (column_name [ ASC | DESC ], ...) [ OPTIONS (...) ]
</pre>

**Description**

The `CREATE TABLE` statement creates a table and adds any columns defined in the
column definition list `(table_element, ...)`. If the `AS query` clause is
absent, the column definition list must be present and contain at least one
column definition. The value of `column_name` must be unique for each column in
the table. If both the column definition list and the `AS query` clause are
present, then the number of columns in the column definition list must match the
number of columns selected in `query`, and the type of each column selected in
`query` must be coercible to the column type in the corresponding position of
the column definition list. The `column_type` can be any valid {{ product_name
}} [data type](https://github.com/google/zetasql/blob/master/docs/data-types.md).

You can define a primary key on a table by providing a `primary_key_spec`
clause, or by providing the `PRIMARY KEY` keyword in the `column_definition`.
The optional `ASC` or `DESC` keyword within `primary_key_spec` specifies the
sort order for any index the database system builds on the primary key.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    system-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.
+   `AS query`: Materializes the result of `query` into the new table.

## CREATE VIEW

```
CREATE
  [OR REPLACE]
  [TEMP | TEMPORARY]
  VIEW
  [IF NOT EXISTS]
  view_name
  [OPTIONS (key=value, ...)]
AS query;
```

**Description**

The `CREATE VIEW` statement creates a view based on a specific query.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    system-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.

## CREATE EXTERNAL TABLE

```
CREATE
  [OR REPLACE]
  [TEMP | TEMPORARY]
  EXTERNAL TABLE
  [IF NOT EXISTS]
  table_name
  [OPTIONS (key=value, ...)];
```

**Description**

The `CREATE EXTERNAL TABLE` creates a table from external data. `CREATE EXTERNAL
TABLE` also supports creating persistent definitions.

The `CREATE EXTERNAL TABLE` does not build a new table; instead, it creates a
pointer to data that exists outside of the database.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    system-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.

## CREATE INDEX

```
CREATE
  [OR REPLACE]
  [UNIQUE]
  INDEX
  [IF NOT EXISTS]
  index_name
  ON
  table_name [[AS] alias]
  [UNNEST(array_expression) [[AS] alias] [WITH OFFSET [[AS] alias]] ...]
  (key_expression [ASC|DESC], ...)
  [STORING (stored_expression, ...)]
  [OPTIONS (key=value, ...)];
```

**Description**

The `CREATE INDEX` statement creates a secondary index for one or more
values computed from expressions in a table.

**Expressions**

+  `array_expression`: An immutable expression that is used to produce an array
   value from each row of an indexed table.
+  `key_expression`: An immutable expression that is used to produce an index
    key value. The expression must have a type that satisfies the requirement of
    an index key column.
+  `stored_expression`: An immutable expression that is used to produce a value
    stored in the index.

**Optional Clauses**

+  `OR REPLACE`: If the index already exists, replace it. This cannot
    appear with `IF NOT EXISTS`.
+  `UNIQUE`: Do not index the same key multiple times. Systems can choose how to
    resolve conflicts in case the index is over a non-unique key.
+  `IF NOT EXISTS`: Do not create an index if it already exists. This cannot
    appear with `OR REPLACE`.
+  `UNNEST(array_name)`: Create an index for the elements in an array.
+  `WITH OFFSET`: Return a separate column containing the offset value
    for each row produced by the `UNNEST` operation.
+  `ASC | DESC`: Sort the indexed values in ascending or descending
    order. `ASC` is the default value with respect to the sort defined by any
    key parts to the left.
+  `STORING`: Specify additional values computed from the indexed base table row
    to materialize with the index entry.
+  `OPTIONS`: If you have schema options, you can add them when you create the
    index. These options are system-specific and follow the
    ZetaSQL[`HINT` syntax](lexical.md#hints).

**Examples**

Create an index on a column in a table.

```sql
CREATE INDEX i1 ON KeyValue (Key);
```

Create an index on multiple columns in a table.

```sql
CREATE INDEX i1 ON KeyValue (Key, Value);
```

If the index already exists, replace it.

```sql
CREATE OR REPLACE INDEX i1 ON KeyValue (Key, Value);
```

If the index already exists, don't replace it.

```sql
CREATE INDEX IF NOT EXISTS i1 ON KeyValue (Key, Value);
```

Create an index that contains unique values.

```sql
CREATE UNIQUE INDEX i1 ON Books (Title);
```

Create an index that contains a schema option.

```sql
CREATE INDEX i1 ON KeyValue (Value) OPTIONS (page_count=1);
```

Reference the table name for a column.

```sql
CREATE INDEX i1 ON KeyValue (KeyValue.Key, KeyValue.Value);
```

```sql
CREATE INDEX i1 on KeyValue AS foo (foo.Key, foo.Value);
```

Use the path expression for a key.

```sql
CREATE INDEX i1 ON KeyValue (Key.sub_field1.sub_field2);
```

Choose the sort order for the columns assigned to an index.

```sql
CREATE INDEX i1 ON KeyValue (Key DESC, Value ASC);
```

Create an index on an array, but not the elements in an array.

```sql
CREATE INDEX i1 ON Books (BookList);
```

Create an index for the elements in an array.

```sql
CREATE INDEX i1 ON Books UNNEST (BookList) (BookList);
```

```sql
CREATE INDEX i1 ON Books UNNEST (BookListA) AS a UNNEST (BookListB) AS b (a, b);
```

Create an index for the elements in an array using an offset.

```sql
CREATE index i1 on Books UNNEST(BookList) WITH OFFSET (BookList, offset);
```

```sql
CREATE index i1 on Books UNNEST(BookList) WITH OFFSET AS foo (BookList, foo);
```

Store an additional column but don't sort it.

```sql
CREATE INDEX i1 ON KeyValue (Value) STORING (Key);
```

Store multiple additional columns and don't sort them.

```sql
CREATE INDEX i1 ON Books (Title) STORING (First_Name, Last_Name);
```

Store a column but don't sort it. Reference a table name.

```sql
CREATE INDEX i1 ON Books (InStock) STORING (Books.Title);
```

Use an expression in the `STORING` clause.

```sql
CREATE INDEX i1 ON KeyValue (Key) STORING (Key+1);
```

Use an implicit alias in the `STORING` clause.

```sql
CREATE INDEX i1 ON KeyValue (Key) STORING (KeyValue);
```

## DEFINE TABLE

```
DEFINE TABLE table_name (options);
```

**Description**

The `DEFINE TABLE` statement allows queries to run against an exported data
source.

## ALTER

```
ALTER TABLE table_name SET OPTIONS (key=value, ...);
```

**Description**

The `ALTER` statement modifies schema options for a table. Because {{
product_name }} does not define general DDL syntax, it only supports `ALTER` for
changing table options which typically appear in the `OPTIONS` clause of a
`CREATE TABLE` statement.

`table_name` is any identifier or dotted path.

The option entries are system-specific. These follow the ZetaSQL
[`HINT` syntax](lexical.md#hints).

This statement raises an error under these conditions:

+   The table does not exist.
+   A key is not recognized.

The following semantics apply:

+   The value is updated for each key in the `SET OPTIONS` clause.
+   Keys not in the `SET OPTIONS` clause remain unchanged.
+   Setting a key value to `NULL` clears it.

## RENAME

```
RENAME object_type old_name_path TO new_name_path;
```

**Description**

The `RENAME` object renames an object. `object_type` indicates what type of
object to rename.

## DROP

```
DROP object_type [IF EXISTS] object_path;
```

**Description**

The `DROP` statement drops an object. `object_type` indicates what type of
object to drop.

**Optional Clauses**

+   `IF EXISTS`: If no object exists at `object_path`, the `DROP` statement will
    have no effect.

 <!-- END CONTENT -->

