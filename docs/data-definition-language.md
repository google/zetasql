<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Data Definition Language statements

<!-- BEGIN CONTENT -->

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
[`HINT` syntax][hints].

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
   table_name [ ( <span class="var">table_element</span>, ... ) ]
   [ PARTITION [ hints ] BY partition_expression, ... ]
   [ CLUSTER [ hints ] BY cluster_expression, ... ]
   [ OPTIONS (key=value, ...) ]
   [ AS query ];

<span class="var">table_element:</span>
   <span class="var"><a href="#defining-columns">column_definition</a></span> | <span class="var"><a href="#defining-table-constraints">constraint_definition</a></span>
</pre>

**Description**

The `CREATE TABLE` statement creates a table and adds any table elements
defined in the table element list `(table_element, ...)`. If the `AS query`
clause is absent, the table element list must be present and contain at
least one column definition. A table element can be a column definition
or constraint definition. To learn more about column definitions, see
[Defining Columns][defining-columns]. To learn more about constraint
definitions, see [Defining Constraints][defining-constraints].

In a table, if both the table element list and the `AS query` clause
are present, then the number of columns in the table element list must
match the number of columns selected in `query`, and the type of each
column selected in `query` must be assignable to the column type in the
corresponding position of the table element list.

**Optional Clauses**

+  `OR REPLACE`: Replaces any table with the same name if it exists. Cannot
   appear with `IF NOT EXISTS`.
+  `TEMP | TEMPORARY`: Creates a temporary table. The lifetime of the table is
   system-specific.
+  `IF NOT EXISTS`: If any table exists with the same name, the `CREATE`
   statement will have no effect. Cannot appear with `OR REPLACE`.
+  `PARTITION BY`: Creates partitions of a table. The expression cannot
   contain floating point types, non-groupable types, constants,
   aggregate functions, or analytic functions.
+  `CLUSTER BY`: Co-locates rows if the rows are not distinct for the values
   produced by the provided list of expressions.
   If the table was partitioned, co-location occurs within each partition.
   If the table was not partitioned, co-location occurs within the table.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the table. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]
+  `AS query`: Materializes the result of `query` into the new table.

**Examples**

Create a table.

```sql
CREATE TABLE books (title STRING, author STRING);
```

Create a table in a schema called `library`.

```sql
CREATE TABLE library.books (title STRING, author STRING);
```

Create a table that contains schema options.

```sql
CREATE TABLE books (title STRING, author STRING) OPTIONS (storage_kind=FLASH_MEMORY);
```

Partition a table.

```sql
CREATE TABLE books (title STRING, author STRING, publisher STRING, release_date DATE)
PARTITION BY publisher, author;
```

Partition a table with a pseudocolumn. In the example below, SYSDATE represents
the date when the book was added to the database. Replace SYSDATE with a
psuedocolumn supported by your SQL service.

```sql
CREATE TABLE books (title STRING, author STRING)
PARTITION BY sysdate;
```

Cluster a table.

```sql
CREATE TABLE books (
  title STRING,
  first_name STRING,
  last_name STRING
)
CLUSTER BY last_name, first_name;
```

### Defining Columns

<pre>
<span class="var">column_definition:</span>
   column_name
   [ column_type ]
   [ <span class="var">generation_clause</span> ]
   [ <span class="var">column_attribute</span>, ... ]
   [ OPTIONS (...) ]

<span class="var">column_attribute:</span>
  PRIMARY KEY
  | NOT NULL
  | HIDDEN
  | [ CONSTRAINT constraint_name ] <span class="var"><a href="#defining-foreign-references">foreign_reference</a></span>

<span class="var">generation_clause:</span>
  AS generation_expression
</pre>

**Description**

A column exists within a table. Each column describes one attribute of the
rows that belong to the table.

+  The name of a column must be unique within a table.
+  Columns in a table are ordered. The order will have consequences on
   some sorts of SQL statements such as `SELECT * FROM Table` and
   `INSERT INTO Table VALUES (...)`.
+  A column can be generated. A generated column is a column in a base
   table whose value is defined as a function of other columns in the
   same row. To use a generated column, include the `generation_clause`
   in the column definition.
+  If a column is not generated, `column_type` is required.

**Attribute Kinds**

+  `PRIMARY KEY`: Defines this column as the [primary key][primary-key]
   of the table.
+  `NOT NULL`: A value on a column cannot be null. More specifically, this is
   shorthand for a constraint of the shape `CHECK [column_name] IS NOT NULL`.
+  `HIDDEN`: Hides a column if it should not appear in `SELECT * expansions`
   or in structifications of a row variable, such as `SELECT t FROM Table t`.
+  `[ CONSTRAINT constraint_name ] foreign_reference`: The column in a
   [foreign table to reference][defining-foreign-reference].
   You can optionally name the constraint.
   A constraint name must be unique within its schema; it cannot share the name
   of other constraints.
   If a constraint name is not provided, one is generated by the
   implementing engine. Users can use INFORMATION_SCHEMA to look up
   the generated names of table constraints.

**Optional Clauses**

+  `column_type`: The  data type of the column.
    This is optional for generated columns, but is required for non-generated
    columns.
+  `generation_clause`: The function that describes a generation expression.
   A generation expression must be a scalar expression. Subqueries are not
   allowed.
+  `column_attribute`: A characteristic of the column.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the column. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]

**Examples**

Create a table with a primary key that can't be null.

```sql
CREATE TABLE books (title STRING, author STRING, isbn INT64 PRIMARY KEY NOT NULL);
```

Create a table with a generated column. In this example,
the generated column holds the first and last name of an author.

```sql
CREATE TABLE authors(
  firstName STRING HIDDEN,
  lastName STRING HIDDEN,
  fullName STRING CONCAT(firstName, " ", lastName)
);
```

Create a table that contains schema options on column definitions.

```sql
CREATE TABLE books (
  title STRING NOT NULL PRIMARY KEY,
  author STRING
      OPTIONS (is_deprecated=true, comment="Replaced by authorId column"),
  authorId INT64 REFERENCES authors (id),
  category STRING OPTIONS (description="LCC Subclass")
)
```

### Defining Table Constraints

<pre>
<span class="var">constraint_definition:</span>
   <span class="var">primary_key</span>
   | <span class="var">foreign_key</span>
   | <span class="var">check_constraint</span>

<span class="var">primary_key:</span>
  PRIMARY KEY (column_name [ ASC | DESC ], ... )
  [ OPTIONS (...) ]

<span class="var">foreign_key:</span>
  [ CONSTRAINT constraint_name ]
  FOREIGN KEY (column_name, ... )
  <span class="var"><a href="#defining-foreign-references">foreign_reference</a></span>
  [ OPTIONS (...) ]

<span class="var">check_constraint:</span>
  [ CONSTRAINT constraint_name ]
  CHECK ( boolean_expression )
  [ ENFORCED | NOT ENFORCED ]
  [ OPTIONS (...) ]
</pre>

**Description**

A `constraint_definition` is a rule enforced on the columns of a table.

**Constraint Kinds**

+  `primary_key`: Defines the [primary key][primary-key] for a table.
+  `foreign_key`:  Defines a foreign key for a table. A foreign key links
   two tables together.
+  `check_constraint`: Restricts the data that can be added to certain
   columns used by the expressions of the constraints.

**Optional Clauses**

+  `CONSTRAINT`: Names the constraint.
   A constraint name must be unique within its schema; it cannot share the name
   of other constraints.
   If a constraint name is not provided, one is generated by the
   implementing engine. Users can use INFORMATION_SCHEMA to look up
   the generated names of table constraints.
+  `ASC | DESC`: Specifies that the engine should optimize reading the
   index records in ascending or descending order by this key part.
   `ASC` is the default. Each key part will be sorted with respect to
   the sort defined by any key parts to the left.
+  `ENFORCED | NOT ENFORCED`: Specifies whether or not the constraint
   is enforced.
+  `OPTIONS`: If you have schema options, you can add them when you create
   the constraint. These options are system-specific and follow the
   ZetaSQL[`HINT` syntax][hints]

**Examples**

Create a primary key constraint, using the `title` and `author` columns
in a table called `books`.

```sql
CREATE TABLE books (title STRING, author STRING, PRIMARY KEY (title ASC, author ASC));
```

Create a foreign key constraint. When data in the `top_authors` table
is updated or deleted, make the same change in the `authors` table.

```sql
CREATE TABLE top_authors (
  author_first_name STRING,
  author_last_name STRING,
  CONSTRAINT fk_top_authors_name
    FOREIGN KEY (author_first_name, author_last_name)
    REFERENCES authors (first_name, last_name)
);
```

Create a check constraint. A row that contains values for `words_per_chapter`
and `words_per_book` can only only be inserted into the `page_count_average`
table if the `words_per_chapter` value is less than the `words_per_book` value.

```sql
CREATE TABLE page_count_average (
  words_per_chapter INT64,
  words_per_book INT64,
  CHECK (words_per_chapter < words_per_book)
);
```

### Defining Foreign References

<pre>
<span class="var">foreign_reference:</span>
  REFERENCES table_name (column_name, ... )
  [ MATCH { SIMPLE | FULL | NOT DISTINCT } ]
  [ ON UPDATE <span class="var">referential_action</span> ]
  [ ON DELETE <span class="var">referential_action</span> ]
  [ ENFORCED | NOT ENFORCED ]

<span class="var">referential_action:</span>
  NO ACTION | RESTRICT | CASCADE | SET NULL
</pre>

**Description**

A foreign key is used to define a relationship between the rows in two tables.
The foreign key in one table can reference one or more columns in
another table. A foreign reference can be used to assign constraints
to a foreign key and to give a foreign key a unique name.

**Optional Clauses**

+  `MATCH`: Specifies when a constraint validation for a referencing row
   passes or fails. Your choices are:
   +  `SIMPLE`:
      +  Passes if *any column* of the local key is `NULL`.
      +  Passes if the columns of the local key are pairwise-equal to the
         columns of the referenced key for some row of the referenced table.
   +  `FULL`:
      +  Passes if *all columns* of the local key are `NULL`.
      +  Passes if the columns of the local key are pairwise-equal to the
         columns of the referenced key for some row of the referenced table.
   +  `NOT DISTINCT`:
      +  Passes if the columns of the local key are pairwise-not-distinct from
         the columns of the referenced key for some row of the referenced table.
+  `ON UPDATE`: If data in the referenced table updates, your choices are:
   +  `RESTRICT`: Fail the transaction.
   +  `NO ACTION`: Don't update the referencing table. If the data in the
      referencing table does not satisfy the constraint before it is
      checked, the transaction will fail.
   +  `CASCADE`: For each updated row in the referenced table,
      update all matching rows in the referencing table so that
      they continue to match the same row after the transaction.
   +  `SET NULL`: Any change to a referenced column in the referenced table
      causes the corresponding referencing column in matching rows of the
      referencing table to be set to null.
+  `ON DELETE`: If data in the referenced table is deleted, your choices are:
   +  `RESTRICT`: Fail the transaction.
   +  `NO ACTION`: Don't delete the data in the referencing table. If the
       data in the referencing table does not satisfy the constraint before it
       is checked, the transaction will fail.
   +  `CASCADE`: For each deleted row in the referenced table,
      delete all matching rows in the referencing table so that
      they continue to match the same row after the transaction.
   +  `SET NULL`: If a row of the referenced table is deleted, then all
       referencing columns in all matching rows of the referencing table
       are set to null.
+  `ENFORCED | NOT ENFORCED`: Specifies whether or not the constraint
   is enforced.

**Examples**

When data in the `top_books` table is updated or deleted, make the
same change in the `books` table.

```sql
CREATE TABLE top_books (
  book_name STRING,
  CONSTRAINT fk_top_books_name
    FOREIGN KEY (book_name)
    REFERENCES books (title)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);
```

### Using CREATE TABLE AS

```
CREATE TABLE table_definition AS query
```

**Description**

The `CREATE TABLE AS` statement creates a table and materializes the result
of a query into this new table. To learn more about the syntax for this
statement, see [`CREATE TABLE`][create-table].

**Examples**

Copy all rows from a table called `old_books` to a new table called
`books`.

```sql
CREATE TABLE books AS (SELECT * FROM old_books);
```

Copy rows from a table called `old_books` to a new table called `books`.
If a book was published before 1900, do not add it to `books`.

```sql
CREATE TABLE books
AS (SELECT * FROM old_books where year >= 1900)
```

Copy rows from a table called `old_books` to a new table called `books`.
Only copy the `title` column into `books`.

```sql
CREATE TABLE books AS (SELECT title FROM old_books);
```

Copy rows from `old_books` and `ancient_books` to a new table called
`books`.

```sql
CREATE TABLE books AS (
  SELECT * FROM old_books UNION ALL
  SELECT * FROM ancient_books);
```

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
    ZetaSQL[`HINT` syntax][hints].

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

## CREATE CONSTANT

Documentation is pending for this feature.

## CREATE FUNCTION

Documentation is pending for this feature.

## CREATE PROCEDURE

Documentation is pending for this feature.

## CREATE ROW POLICY

Documentation is pending for this feature.

## CREATE TABLE FUNCTION

Documentation is pending for this feature.

## DEFINE TABLE

```
DEFINE TABLE table_name (options);
```

**Description**

The `DEFINE TABLE` statement allows queries to run against an exported data
source.

## ALTER

<pre>
ALTER <span class="var">object_type</span> <span class="var">alter_action</span>, ...

<span class="var">object_type</span>:
  TABLE
  | VIEW

<span class="var">alter_action</span>:
  SET OPTIONS (key=value, ...)
  | ADD COLUMN [ IF NOT EXISTS ]  column_definition
  | DROP COLUMN [ IF EXISTS ]  column_name
</pre>

**Description**

The `ALTER` statement modifies metadata for a table or a view.

`table_name` is any identifier or dotted path.

The option entries are system-specific. These follow the ZetaSQL
[`HINT` syntax][hints].

SET OPTIONS action raises an error under these conditions:

+   The table does not exist.
+   A key is not recognized.

The following semantics apply:

+   The value is updated for each key in the `SET OPTIONS` clause.
+   Keys not in the `SET OPTIONS` clause remain unchanged.
+   Setting a key value to `NULL` clears it.

ADD and DROP COLUMN actions raise an error under these conditions:

+   ADD COLUMN or DROP COLUMN are applied to a view.
+   ADD COLUMN specified without IF NOT EXIST and the column already exists.
+   DROP COLUMN specified without IF EXIST and the column does not exist.

**Examples**

The following examples illustrate ways to use the `ALTER SET OPTIONS` statement:

Update table description.

```sql
ALTER TABLE my_dataset.my_table
SET OPTIONS (description='my table');
```

Remove table description.

```sql
ALTER TABLE my_dataset.my_table
SET OPTIONS (description=NULL);
```

The following example illustrates using the `ALTER ADD COLUMN` statement:

```sql
ALTER TABLE mydataset.mytable
    ADD COLUMN A STRING,
    ADD COLUMN IF NOT EXISTS B GEOGRAPHY,
    ADD COLUMN C ARRAY<NUMERIC>,
    ADD COLUMN D DATE OPTIONS(description="my description")
```

Add column A of type STRUCT.

```sql
ALTER TABLE mydataset.mytable
ADD COLUMN A STRUCT<
               B GEOGRAPHY,
               C ARRAY<INT64>,
               D INT64 NOT NULL,
               E TIMESTAMP OPTIONS(description="creation time")
             >
```

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

## Terminology

### Primary key

A primary key constraint is an attribute of a table. A table can have at most
one primary key constraint that includes one or more columns of that table.
Each row in the table has a tuple that is the row's primary key. The primary key
tuple has values that correspond to the columns in the primary key constraint.
The primary key of all rows must be unique among the primary keys of all rows in
the table.

**Examples**

In this example, a column in a table called `books` is assigned to a primary
key. For each row in this table, the value in the `id` column must be distinct
from the value in the `id` column of all other rows in the table.

```sql
CREATE TABLE books (title STRING, id STRING, PRIMARY KEY (id));
```

In this example, multiple columns in a table called `books` are assigned to a
primary key. For each row in this table, the tuple of values in the `title` and
`name` columns must together be distinct from the values in the respective
`title` and `name` columns of all other rows in the table.

```sql
CREATE TABLE books (title STRING, name STRING, PRIMARY KEY (title, name));
```

[primary-key]: #primary-key
[create-table]: #create-table
[hints]: lexical#hints
[defining-columns]: #defining-columns
[defining-constraints]: #defining-table-constraints
[defining-foreign-reference]: #defining-foreign-references

<!-- END CONTENT -->

