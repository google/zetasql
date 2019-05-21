<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Data Definition Language statements

ZetaSQL specifies the syntax for Data Definition Language (DDL)
statements.

Where possible, this topic provides a link to the product-specific documentation
for each statement.

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
sort order for any index the database engine builds on the primary key.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    engine-specific.
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

The `CREATE VIEW` statement creates a view based on a specific query.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    engine-specific.
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

The `CREATE EXTERNAL TABLE` creates a table from external data. `CREATE EXTERNAL
TABLE` also supports creating persistent definitions.

The `CREATE EXTERNAL TABLE` does not build a new table; instead, it creates a
pointer to data that exists outside of the database.

**Optional Clauses**

+   `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
    appear with `IF NOT EXISTS`.
+   `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
    engine-specific.
+   `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
    statement will have no effect. Cannot appear with `OR REPLACE`.

## DEFINE TABLE

```
DEFINE TABLE table_name (options);
```

The `DEFINE TABLE` statement allows queries to run against an exported data
source.

## ALTER

```
ALTER TABLE table_name SET OPTIONS (key=value, ...);
```

The `ALTER` statement modifies schema options for a table. Because {{
product_name }} does not define general DDL syntax, it only supports `ALTER` for
changing table options which typically appear in the `OPTIONS` clause of a
`CREATE TABLE` statement.

`table_name` is any identifier or dotted path.

The option entries are engine specific. These follow the ZetaSQL
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

The `RENAME` object renames an object. `object_type` indicates what type of
object to rename.

## DROP

```
DROP object_type [IF EXISTS] object_path;
```

The `DROP` statement drops an object. `object_type` indicates what type of
object to drop.

**Optional Clauses**

+   `IF EXISTS`: If no object exists at `object_path`, the `DROP` statement will
    have no effect.

 <!-- END CONTENT -->

