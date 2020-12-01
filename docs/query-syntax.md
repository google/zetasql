
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Query syntax

Query statements scan one or more tables or expressions and return the computed
result rows. This topic describes the syntax for SQL queries in
ZetaSQL.

## SQL syntax

<pre class="lang-sql prettyprint">
<span class="var">query_statement</span>:
    <span class="var">query_expr</span>

<span class="var">query_expr</span>:
    [ <a href="#with_clause">WITH</a> <span class="var"><a href="#with_query_name">with_query_name</a></span> AS ( <span class="var">query_expr</span> ) [, ...] ]
    { <span class="var">select</span> | ( <span class="var">query_expr</span> ) | <span class="var">query_expr</span> <span class="var">set_op</span> <span class="var">query_expr</span> }
    [ <a href="#order_by_clause">ORDER</a> BY <span class="var">expression</span> [{ ASC | DESC }] [, ...] ]
    [ <a href="#limit_and_offset_clause">LIMIT</a> <span class="var">count</span> [ OFFSET <span class="var">skip_rows</span> ] ]

<span class="var">select</span>:
    <a href="#select_list">SELECT</a> [ AS { <span class="var"><a href="https://github.com/google/zetasql/blob/master/docs/protocol-buffers#select_as_typename">typename</a></span> | <a href="#select_as_struct">STRUCT</a> | <a href="#select_as_value">VALUE</a> } ] [{ ALL | DISTINCT }]
        { [ <span class="var">expression</span>. ]* [ <a href="#select_except">EXCEPT</a> ( <span class="var">column_name</span> [, ...] ) ]<br>            [ <a href="#select_replace">REPLACE</a> ( <span class="var">expression</span> [ AS ] <span class="var">column_name</span> [, ...] ) ]<br>        | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
    [ <a href="#from_clause">FROM</a> <span class="var">from_item</span> [ <span class="var">tablesample_type</span> ] [, ...] ]
    [ <a href="#where_clause">WHERE</a> <span class="var">bool_expression</span> ]
    [ <a href="#group_by_clause">GROUP</a> BY { <span class="var">expression</span> [, ...] | ROLLUP ( <span class="var">expression</span> [, ...] ) } ]
    [ <a href="#having_clause">HAVING</a> <span class="var">bool_expression</span> ]
    [ <a href="#window_clause">WINDOW</a> <span class="var">named_window_expression</span> AS { <span class="var">named_window</span> | ( [ <span class="var">window_definition</span> ] ) } [, ...] ]

<span class="var">set_op</span>:
    <a href="#union">UNION</a> { ALL | DISTINCT } | <a href="#intersect">INTERSECT</a> { ALL | DISTINCT } | <a href="#except">EXCEPT</a> { ALL | DISTINCT }

<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">field_path</span> |
    { <a href="#unnest">UNNEST</a>( <span class="var">array_expression</span> ) | UNNEST( <span class="var">array_path</span> ) | <span class="var">array_path</span> }
        [ [ AS ] <span class="var">alias</span> ] [ WITH OFFSET [ [ AS ] <span class="var">alias</span> ] ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}

<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] <a href="#join_types">JOIN</a> <span class="var">from_item</span>
    [ { <a href="#on_clause">ON</a> <span class="var">bool_expression</span> | <a href="#using_clause">USING</a> ( <span class="var">join_column</span> [, ...] ) } ]

<span class="var">join_type</span>:
    { <a href="#inner_join">INNER</a> | <a href="#cross_join">CROSS</a> | <a href="#full_outer_join">FULL [OUTER]</a> | <a href="#left_outer_join">LEFT [OUTER]</a> | <a href="#right_outer_join">RIGHT [OUTER]</a> }

<span class="var">tablesample_type</span>:
    <a href="#tablesample_operator">TABLESAMPLE</a> <span class="var">sample_method</span> (<span class="var">sample_size</span> <span class="var">percent_or_rows</span> )

<span class="var">sample_method</span>:
    { BERNOULLI | SYSTEM | RESERVOIR }

<span class="var">sample_size</span>:
    <span class="var">numeric_value_expression</span>

<span class="var">percent_or_rows</span>:
    { PERCENT | ROWS }
</pre>

**Notation rules**

+ Square brackets "[ ]" indicate optional clauses.
+ Parentheses "( )" indicate literal parentheses.
+ The vertical bar "|" indicates a logical OR.
+ Curly braces "{ }" enclose a set of options.
+ A comma followed by an ellipsis within square brackets "[, ... ]" indicates that
  the preceding item can repeat in a comma-separated list.

### Sample tables 
<a id="sample_tables"></a>

The following tables are used to illustrate the behavior of different
query clauses in this reference.

#### Roster table

The `Roster` table includes a list of player names (`LastName`) and the
unique ID assigned to their school (`SchoolID`). It looks like this:

```sql
+-----------------------+
| LastName   | SchoolID |
+-----------------------+
| Adams      | 50       |
| Buchanan   | 52       |
| Coolidge   | 52       |
| Davis      | 51       |
| Eisenhower | 77       |
+-----------------------+
```

You can use this `WITH` clause to emulate a temporary table name for the
examples in this reference:

```sql
WITH Roster AS
 (SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL
  SELECT 'Buchanan', 52 UNION ALL
  SELECT 'Coolidge', 52 UNION ALL
  SELECT 'Davis', 51 UNION ALL
  SELECT 'Eisenhower', 77)
SELECT * FROM Roster
```

#### PlayerStats table

The `PlayerStats` table includes a list of player names (`LastName`) and the
unique ID assigned to the opponent they played in a given game (`OpponentID`)
and the number of points scored by the athlete in that game (`PointsScored`).

```sql
+----------------------------------------+
| LastName   | OpponentID | PointsScored |
+----------------------------------------+
| Adams      | 51         | 3            |
| Buchanan   | 77         | 0            |
| Coolidge   | 77         | 1            |
| Davis      | 52         | 4            |
| Eisenhower | 50         | 13           |
+----------------------------------------+
```

You can use this `WITH` clause to emulate a temporary table name for the
examples in this reference:

```sql
WITH PlayerStats AS
 (SELECT 'Adams' as LastName, 51 as OpponentID, 3 as PointsScored UNION ALL
  SELECT 'Buchanan', 77, 0 UNION ALL
  SELECT 'Coolidge', 77, 1 UNION ALL
  SELECT 'Adams', 52, 4 UNION ALL
  SELECT 'Buchanan', 50, 13)
SELECT * FROM PlayerStats
```

#### TeamMascot table

The `TeamMascot` table includes a list of unique school IDs (`SchoolID`) and the
mascot for that school (`Mascot`).

```sql
+---------------------+
| SchoolID | Mascot   |
+---------------------+
| 50       | Jaguars  |
| 51       | Knights  |
| 52       | Lakers   |
| 53       | Mustangs |
+---------------------+
```

You can use this `WITH` clause to emulate a temporary table name for the
examples in this reference:

```sql
WITH TeamMascot AS
 (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL
  SELECT 51, 'Knights' UNION ALL
  SELECT 52, 'Lakers' UNION ALL
  SELECT 53, 'Mustangs')
SELECT * FROM TeamMascot
```

## SELECT list

<pre>
SELECT [ AS { <span class="var">typename</span> | STRUCT | VALUE } ] [{ ALL | DISTINCT }]
    { [ <span class="var">expression</span>. ]* [ EXCEPT ( <span class="var">column_name</span> [, ...] ) ]<br>        [ REPLACE ( <span class="var">expression</span> [ AS ] <span class="var">column_name</span> [, ...] ) ]<br>    | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
</pre>

The `SELECT` list defines the columns that the query will return. Expressions in
the `SELECT` list can refer to columns in any of the `from_item`s in its
corresponding `FROM` clause.

Each item in the `SELECT` list is one of:

+  `*`
+  `expression`
+  `expression.*`

### SELECT *

`SELECT *`, often referred to as *select star*, produces one output column for
each column that is visible after executing the full query.

```sql
SELECT * FROM (SELECT "apple" AS fruit, "carrot" AS vegetable);

+-------+-----------+
| fruit | vegetable |
+-------+-----------+
| apple | carrot    |
+-------+-----------+
```

### SELECT `expression`

Items in a `SELECT` list can be expressions. These expressions evaluate to a
single value and produce one output column, with an optional explicit `alias`.

If the expression does not have an explicit alias, it receives an implicit alias
according to the rules for [implicit aliases][implicit-aliases], if possible.
Otherwise, the column is anonymous and you cannot refer to it by name elsewhere
in the query.

### SELECT `expression.*`

An item in a `SELECT` list can also take the form of `expression.*`. This
produces one output column for each column or top-level field of `expression`.
The expression must either be a table alias or evaluate to a single value of a
data type with fields, such as a STRUCT.

The following query produces one output column for each column in the table
`groceries`, aliased as `g`.

```sql
WITH groceries AS
  (SELECT "milk" AS dairy,
   "eggs" AS protein,
   "bread" AS grain)
SELECT g.*
FROM groceries AS g;

+-------+---------+-------+
| dairy | protein | grain |
+-------+---------+-------+
| milk  | eggs    | bread |
+-------+---------+-------+
```

More examples:

```sql
WITH locations AS
  (SELECT STRUCT("Seattle" AS city, "Washington" AS state) AS location
  UNION ALL
  SELECT STRUCT("Phoenix" AS city, "Arizona" AS state) AS location)
SELECT l.location.*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
| Phoenix | Arizona    |
+---------+------------+
```

```sql
WITH locations AS
  (SELECT ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),
    ("Phoenix", "Arizona")] AS location)
SELECT l.LOCATION[offset(0)].*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
+---------+------------+
```

### SELECT modifiers

You can modify the results returned from a `SELECT` query, as follows.

#### SELECT DISTINCT

A `SELECT DISTINCT` statement discards duplicate rows and returns only the
remaining rows. `SELECT DISTINCT` cannot return columns of the following types:

+  `PROTO`

#### SELECT * EXCEPT

A `SELECT * EXCEPT` statement specifies the names of one or more columns to
exclude from the result. All matching column names are omitted from the output.

```sql
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * EXCEPT (order_id)
FROM orders;

+-----------+----------+
| item_name | quantity |
+-----------+----------+
| sprocket  | 200      |
+-----------+----------+
```

Note: `SELECT * EXCEPT` does not exclude columns that do not have names.

#### SELECT * REPLACE

A `SELECT * REPLACE` statement specifies one or more
`expression AS identifier` clauses. Each identifier must match a column name
from the `SELECT *` statement. In the output column list, the column that
matches the identifier in a `REPLACE` clause is replaced by the expression in
that `REPLACE` clause.

A `SELECT * REPLACE` statement does not change the names or order of columns.
However, it can change the value and the value type.

```sql
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE ("widget" AS item_name)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | widget    | 200      |
+----------+-----------+----------+

WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE (quantity/2 AS quantity)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | sprocket  | 100      |
+----------+-----------+----------+
```

Note: `SELECT * REPLACE` does not replace columns that do not have names.

#### SELECT ALL
A `SELECT ALL` statement returns all rows, including duplicate rows.
`SELECT ALL` is the default behavior of `SELECT`.

### Value tables

In ZetaSQL, a value table is a table where the row type is a single
value.  In a regular table, each row is made up of columns, each of which has a
name and a type.  In a value table, the row type is just a single value, and
there are no column names.

Most commonly, value tables are used for protocol buffer value tables, where the
table contains a stream of protocol buffer values. In this case, the top-level
protocol buffer fields can be used in the same way that column names are used
when querying a regular table.

In contexts where a query with exactly one column is expected, a value table
query can be used instead.  For example, scalar subqueries and array subqueries
(see [Subqueries][subquery-concepts]) normally require a single-column query,
but in ZetaSQL, they also allow using a value table query.

A query will produce a value table if it uses `SELECT AS`, using one of the
syntaxes below:

#### SELECT AS STRUCT

```sql
SELECT AS STRUCT expr1 [struct_field_name1] [,... ]
```

This produces a value table with a STRUCT row type,
where the STRUCT field names and types match the
column names and types produced in the `SELECT` list.

Example:

```sql
SELECT
  ARRAY(SELECT AS STRUCT t.f1, t.f2 WHERE t.f3=true)
FROM
  Table t
```

`SELECT AS STRUCT` can be used in a scalar or array subquery to produce a single
STRUCT type grouping multiple values together. Scalar
and array subqueries (see [Subqueries][subquery-concepts]) are normally not allowed to
return multiple columns.

Anonymous columns and duplicate columns
are allowed.

Example:

```sql
SELECT AS STRUCT 1 x, 2, 3 x
```

The query above produces STRUCT values of type
`STRUCT<int64 x, int64, int64 x>.` The first and third fields have the same name
`x`, and the second field is anonymous.

The example above produces the same result as this `SELECT AS VALUE` query
using a struct constructor:

```sql
SELECT AS VALUE STRUCT(1 AS x, 2, 3 AS x)
```

#### SELECT AS VALUE

`SELECT AS VALUE` produces a value table from any `SELECT` list that
produces exactly one column. Instead of producing an output table with one
column, possibly with a name, the output will be a value table where the row
type is just the value type that was produced in the one `SELECT` column.  Any
alias the column had will be discarded in the value table.

Example:

```sql
SELECT AS VALUE Int64Column FROM Table;
```

The query above produces a table with row type INT64.

Example:

```sql
SELECT AS VALUE STRUCT(1 a, 2 b) xyz FROM Table;
```

The query above produces a table with row type `STRUCT<a int64, b int64>`.

Example:

```sql
SELECT AS VALUE v FROM ValueTable v WHERE v.field=true;
```

Given a value table `v` as input, the query above filters out certain values in
the `WHERE` clause, and then produces a value table using the exact same value
that was in the input table. If the query above did not use `SELECT AS VALUE`,
then the output table schema would differ from the input table schema because
the output table would be a regular table with a column named `v` containing the
input value.

### Aliases

See [Using Aliases][using-aliases] for information on syntax and visibility for
`SELECT` list aliases.

## FROM clause

<pre>
<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">field_path</span> |
    { <a href="#unnest">UNNEST</a>( <span class="var">array_expression</span> ) | UNNEST( <span class="var">array_path</span> ) | <span class="var">array_path</span> }
        [ [ AS ] <span class="var">alias</span> ] [ WITH OFFSET [ [ AS ] <span class="var">alias</span> ] ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}
</pre>

The `FROM` clause indicates the table or tables from which to retrieve rows, and
specifies how to join those rows together to produce a single stream of
rows for processing in the rest of the query.

#### table_name

The name (optionally qualified) of an existing table.

<pre>
SELECT * FROM Roster;
SELECT * FROM db.Roster;
</pre>

#### join

See [JOIN Types][query-joins].

#### select

`( select ) [ [ AS ] alias ]` is a [table subquery][table-subquery-concepts].

#### field_path

In the `FROM` clause, `field_path` is any path that
resolves to a field within a data type. `field_path` can go
arbitrarily deep into a nested data structure.

Some examples of valid `field_path` values include:

```sql
SELECT * FROM T1 t1, t1.array_column;

SELECT * FROM T1 t1, t1.struct_column.array_field;

SELECT (SELECT ARRAY_AGG(c) FROM t1.array_column c) FROM T1 t1;

SELECT a.struct_field1 FROM T1 t1, t1.array_of_structs a;

SELECT (SELECT STRING_AGG(a.struct_field1) FROM t1.array_of_structs a) FROM T1 t1;
```

Field paths in the `FROM` clause must end in an
array or a repeated field. In
addition, field paths cannot contain arrays
or repeated fields before the end of the path. For example, the path
`array_column.some_array.some_array_field` is invalid because it
contains an array before the end of the path.

Note: If a path has only one name, it is interpreted as a table.
To work around this, wrap the path using `UNNEST`, or use the
fully-qualified path.

Note: If a path has more than one name, and it matches a field
name, it is interpreted as a field name. To force the path to be interpreted as
a table name, wrap the path using <code>`</code>.

#### UNNEST

The `UNNEST` operator takes an `ARRAY` and returns a
table, with one row for each element in the `ARRAY`.
You can also use `UNNEST` outside of the `FROM` clause with the
[`IN` operator][in-operator].

For input `ARRAY`s of most element types, the output of `UNNEST` generally has
one column. This single column has an optional `alias`, which you can use to
refer to the column elsewhere in the query. `ARRAYS` with these element types
return multiple columns:

 + STRUCT
 + PROTO

`UNNEST` destroys the order of elements in the input
`ARRAY`. Use the optional `WITH OFFSET` clause to
return a second column with the array element indexes (see below).

For an input `ARRAY` of `STRUCT`s, `UNNEST`
returns a row for each `STRUCT`, with a separate column for each field in the
`STRUCT`. The alias for each column is the name of the corresponding `STRUCT`
field.

**Example**

```sql
SELECT *
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')]);

+---+-----+
| x | y   |
+---+-----+
| 3 | bar |
| 1 | foo |
+---+-----+
```

Because the `UNNEST` operator returns a
[value table][query-value-tables],
you can alias `UNNEST` to define a range variable that you can reference
elsewhere in the query. If you reference the range variable in the `SELECT`
list, the query returns a `STRUCT` containing all of the fields of the original
`STRUCT` in the input table.

Example:

```sql
SELECT *, struct_value
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')])
       AS struct_value;

+---+-----+--------------+
| x | y   | struct_value |
+---+-----+--------------+
| 3 | bar | {3, bar}     |
| 1 | foo | {1, foo}     |
+---+-----+--------------+
```

For an input `ARRAY` of `PROTO`s, `UNNEST`
returns a row for each `PROTO`, with a separate column for each field in the
`PROTO`. The alias for each column is the name of the corresponding `PROTO`
field.

Example:

```sql
SELECT *
FROM UNNEST(
  ARRAY<zetasql.examples.music.Album>[
    NEW zetasql.examples.music.Album (
      'The Goldberg Variations' AS album_name,
      ['Aria', 'Variation 1', 'Variation 2'] AS song
    )
  ]
);
+-------------------------+--------+----------------------------------+
| album_name              | singer | song                             |
+-------------------------+--------+----------------------------------+
| The Goldberg Variations | NULL   | [Aria, Variation 1, Variation 2] |
+-------------------------+--------+----------------------------------+
```

As with `STRUCT`s, you can alias `UNNEST` here to define a range variable. You
can reference this alias in the `SELECT` list to return a value table where each
row is a `PROTO` element from the `ARRAY`.

```sql
SELECT proto_value
FROM UNNEST(
  ARRAY<zetasql.examples.music.Album>[
    NEW zetasql.examples.music.Album (
      'The Goldberg Variations' AS album_name,
      ['Aria', 'Var. 1'] AS song
    )
  ]
) AS proto_value;
+---------------------------------------------------------------------+
| proto_value                                                         |
+---------------------------------------------------------------------+
| {album_name: "The Goldberg Variations" song: "Aria" song: "Var. 1"} |
+---------------------------------------------------------------------+
```

`ARRAY` unnesting can be either explicit or implicit.
In explicit unnesting, `array_expression` must return an
`ARRAY` value but does not need to resolve to an `ARRAY`, and the `UNNEST`
keyword is required.

Example:

```sql
SELECT * FROM UNNEST ([1, 2, 3]);
```

In implicit unnesting, `array_path` must resolve to an `ARRAY` and the
`UNNEST` keyword is optional.

Example:

```sql
SELECT x
FROM mytable AS t,
  t.struct_typed_column.array_typed_field1 AS x;
```

In this scenario, `array_path` can go arbitrarily deep into a data
structure, but the last field must be `ARRAY`-typed. No previous field in the
expression can be `ARRAY`-typed because it is not possible to extract a named
field from an `ARRAY`.

`UNNEST` treats NULLs as follows:

+  NULL and empty arrays produces zero rows.</li>
+  An array containing NULLs produces rows containing NULL values.

The optional `WITH OFFSET` clause returns a separate
column containing the "offset" value (i.e. counting starts at zero) for each row
produced by the `UNNEST` operation. This column has an optional
`alias`; the default alias is offset.

Example:

```sql
SELECT * FROM UNNEST ( ) WITH OFFSET AS num;
```

See the [`Arrays topic`][working-with-arrays]
for more ways to use `UNNEST`, including construction, flattening, and
filtering.

#### with_query_name

The query names in a `WITH` clause (see [WITH Clause][with_clause]) act like
names of temporary tables that you can reference anywhere in the `FROM` clause.
In the example below, `subQ1` and `subQ2` are `with_query_names`.

Example:

```sql
WITH
  subQ1 AS (SELECT * FROM Roster WHERE SchoolID = 52),
  subQ2 AS (SELECT SchoolID FROM subQ1)
SELECT DISTINCT * FROM subQ2;
```

The `WITH` clause hides any permanent tables with the same name
for the duration of the query, unless you qualify the table name, for example:

 `db.Roster`.

### TABLESAMPLE operator

```sql
tablesample_type:
    TABLESAMPLE sample_method (sample_size percent_or_rows [ partition_by ])
    [ REPEATABLE(repeat_argument) ]
    [ WITH WEIGHT [AS alias] ]

sample_method:
    { BERNOULLI | SYSTEM | RESERVOIR }

sample_size:
    numeric_value_expression

percent_or_rows:
    { PERCENT | ROWS }

partition_by:
    PARTITION BY partition_expression [, ...]
```

**Description**

You can use the `TABLESAMPLE` operator to select a random sample of a data
set. This operator is useful when working with tables that have large amounts of
data and precise answers are not required.

+  `sample_method`: When using the `TABLESAMPLE` operator, you must specify the
   sampling algorithm to use:
   + `BERNOULLI`: Each row is independently selected with the probability
     given in the `percent` clause. As a result, you get approximately
     `N * percent/100` rows.
   + `SYSTEM`: Produces a sample using an
     unspecified engine-dependent method, which may be more efficient but less
     probabilistically random than other methods.  For example, it could choose
     random disk blocks and return data from those blocks.
   + `RESERVOIR`: Takes as parameter an actual sample size
     K (expressed as a number of rows). If the input is smaller than K, it
     outputs the entire input relation. If the input is larger than K,
     reservoir sampling outputs a sample of size exactly K, where any sample of
     size K is equally likely.
+  `sample_size`: The size of the sample.
+  `percent_or_rows`: The `TABLESAMPLE` operator requires that you choose either
   `ROWS` or `PERCENT`. If you choose `PERCENT`, the value must be between
   0 and 100. If you choose `ROWS`, the value must be greater than or equal
   to 0.
+  `partition_by`: Optional. Perform [stratefied sampling][stratefied-sampling]
   for each distinct group identified by the `PARTITION BY` clause. That is,
   if the number of rows in a particular group is less than the specified row
   count, all rows in that group are assigned to the sample. Otherwise, it
   randomly selects the specified number of rows for each group, where for a
   particular group, every sample of that size is equally
   likely.
+  `REPEATABLE`: Optional. When it is used, repeated
   executions of the sampling operation return a result table with identical
   rows for a given repeat argument, as long as the underlying data does
   not change. `repeat_argument` represents a sampling seed
   and must be a positive value of type `INT64`.
+  `WITH WEIGHT`: Optional. Retrieves [scaling weight][scaling-weight]. If
   specified, the `TableSample` operator outputs one extra column of type
   `DOUBLE` that is greater than or equal 1.0 to represent the actual scaling
   weight. If an alias is not provided, the default name _weight_ is used.
   +  In Bernoulli sampling, the weight is `1 / provided sampling probability`.
      For example, `TABLESAMPLE BERNOULLI (1 percent)` will expose the weight
      of `1 / 0.01`.
   +  In System sampling, the weight is approximated or computed exactly in
      some engine-defined way, as long as its type and value range is
      specified.
   +  In non-stratified fixed row count sampling,
      (RESERVOIR without the PARTITION BY clause), the weight is equal to the
      total number of input rows divided by the count of sampled rows.
   +  In stratified sampling,
      (RESERVOIR with the PARTITION BY clause), the weight for rows from a
      particular group is equal to the group cardinality divided by the count
      of sampled rows for that group.

**Examples**

The following examples illustrate the use of the `TABLESAMPLE` operator.

Select from a table using the `RESERVOIR` sampling method:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE RESERVOIR (100 ROWS);
```

Select from a table using the `BERNOULLI` sampling method:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE BERNOULLI (0.1 PERCENT);
```

Use `TABLESAMPLE` with a repeat argument:

```sql
SELECT MessageId
FROM Messages TABLESAMPLE RESERVOIR (100 ROWS) REPEATABLE(10);
```

Use `TABLESAMPLE` with a subquery:

```sql
SELECT Subject FROM
(SELECT MessageId, Subject FROM Messages WHERE ServerId="test")
TABLESAMPLE BERNOULLI(50 PERCENT)
WHERE MessageId > 3;
```

Use a `TABLESAMPLE` operation with a join to another table.

```sql
SELECT S.Subject
FROM
(SELECT MessageId, ThreadId FROM Messages WHERE ServerId="test") AS R
TABLESAMPLE RESERVOIR(5 ROWS),
Threads AS S
WHERE S.ServerId="test" AND R.ThreadId = S.ThreadId;
```

Group results by country, using stratefied sampling:

```sql
SELECT country, SUM(click_cost) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 GROUP BY country;
```

Add scaling weight to stratefied sampling:

```sql
SELECT country, SUM(click_cost * sampling_weight) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 WITH WEIGHT AS sampling_weight
 GROUP BY country;
```

This is equivelant to the previous example. Note that you don't have to use
an alias after `WITH WEIGHT`. If you don't, the default alias `weight` is used.

```sql
SELECT country, SUM(click_cost * weight) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 WITH WEIGHT
 GROUP BY country;
```

#### Stratefied sampling 
<a id="stratefied_sampling"></a>

If you want better quality generated samples for under-represented groups,
you can use stratefied sampling. Stratefied sampling helps you
avoid samples with missing groups. To allow stratified sampling per
distinct group, use `PARTITION BY` with `RESERVOIR` in the `TABLESAMPLE` clause.

Stratified sampling performs `RESERVOIR` sampling for each distinct group
identified by the `PARTITION BY` clause. If the number of rows in a particular
group is less than the specified row count, all rows in that group are assigned
to the sample. Otherwise, it randomly selects the specified number of rows for
each group, where for a particular group, every sample of that size is equally
likely.

**Example**

Let’s consider a table named `ClickEvents` representing a stream of
click events, each of which has two fields: `country` and `click_cost`.
`country` represents the country from which the click was originated
and `click_cost` represents how much the click costs. In this example,
100 rows are randomly selected for each country.

```sql
SELECT click_cost, country FROM ClickEvents
TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
```

#### Scaling weight 
<a id="scaling_weight"></a>

With scaling weight, you can perform fast and reasonable population estimates
from generated samples or estimate the aggregate results from samples. You can
capture scaling weight for a tablesample with the `WITH WEIGHT` clause.

Scaling weight represents the reciprocal of the actual, observed sampling
rate for a tablesample, making it easier to estimate aggregate results for
samples. The exposition of scaling weight generally applies to all variations
of `TABLESAMPLE`, including stratified Reservoir, non-stratified Reservoir,
Bernoulli, and System.

Let’s consider a table named `ClickEvents` representing a stream of
click events, each of which has two fields: `country` and `click_cost`.
`country` represents the country from which the click was originated
and `click_cost` represents how much the click costs. To calculate the
total click cost per country, you can use the following query:

```sql
SELECT country, SUM(click_cost)
FROM ClickEvents
GROUP BY country;
```

You can leverage the existing uniform sampling with fixed probability, using
Bernoulli sampling and run this query to estimate the result of the previous
query:

```sql
SELECT country, SUM(click_cost * weight)
FROM ClickEvents TABLESAMPLE BERNOULLI (1 PERCENT)
WITH WEIGHT
GROUP BY country;
```

You can break the second query into two steps:

1. Materialize a sample for reuse.
1. Perform aggregate estimates of the materialized sample.

Instead of aggregating the entire table, you use a 1% uniform sample to
aggregate a fraction of the original table and to compute the total click cost.
Because only 1% of the rows flow into the aggregation operator, you need to
scale the aggregate with a certain weight. Specifically, we multiply the
aggregate with 100, the reciprocal of the provided sampling probability, for
each group. And because we use uniform sampling, the scaling weight for each
group is effectively equal to the scaling weight for each row of the table,
which is 100.

Even though this sample provides a statistically accurate representation
of the original table, it might miss an entire group of rows, such as countries
in the running example, with small cardinality. For example, suppose that
the `ClickEvents` table contains 10000 rows, with 9990 rows of value `US`
and 10 rows of value `VN`. The number of distinct countries in this example
is two. With 1% uniform sampling, it is statistically probable that all the
sampled rows are from the `US` and none of them are from the `VN` partition.
As a result, the output of the second query does not contain the `SUM` estimate
for the group `VN`. We refer to this as the _missing-group problem_, which
can be solved with [stratefied sampling][stratefied-sampling].

### Aliases

See [Using Aliases][using-aliases] for information on syntax and visibility for
`FROM` clause aliases.

## JOIN types 
<a id="join_types"></a>

<pre>
<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] JOIN <span class="var">from_item</span>
    [ <a href="#on_clause">ON</a> <span class="var">bool_expression</span> | <a href="#using_clause">USING</a> ( <span class="var">join_column</span> [, ...] ) ]

<span class="var">join_type</span>:
    { <a href="#inner_join">INNER</a> | <a href="#cross_join">CROSS</a> | <a href="#full_outer_join">FULL [OUTER]</a> | <a href="#left_outer_join">LEFT [OUTER]</a> | <a href="#right_outer_join">RIGHT [OUTER]</a> }
</pre>

The `JOIN` clause merges two `from_item`s so that the `SELECT` clause can
query them as one source. The `join_type` and `ON` or `USING` clause (a
"join condition") specify how to combine and discard rows from the two
`from_item`s to form a single source.

All `JOIN` clauses require a `join_type`.

A `JOIN` clause requires a join condition unless one of the following conditions
is true:

+  `join_type` is `CROSS`.
+  One or both of the `from_item`s is not a table, e.g. an
   `array_path` or `field_path`.

### [INNER] JOIN

An `INNER JOIN`, or simply `JOIN`, effectively calculates the Cartesian product
of the two `from_item`s and discards all rows that do not meet the join
condition. "Effectively" means that it is possible to implement an `INNER JOIN`
without actually calculating the Cartesian product.

```sql
FROM A INNER JOIN B ON A.w = B.y

Table A       Table B       Result
+-------+     +-------+     +---------------+
| w | x |  *  | y | z |  =  | w | x | y | z |
+-------+     +-------+     +---------------+
| 1 | a |     | 2 | k |     | 2 | b | 2 | k |
| 2 | b |     | 3 | m |     | 3 | c | 3 | m |
| 3 | c |     | 3 | n |     | 3 | c | 3 | n |
| 3 | d |     | 4 | p |     | 3 | d | 3 | m |
+-------+     +-------+     | 3 | d | 3 | n |
                            +---------------+
```

```sql
FROM A INNER JOIN B USING (x)

Table A       Table B       Result
+-------+     +-------+     +-----------+
| x | y |  *  | x | z |  =  | x | y | z |
+-------+     +-------+     +-----------+
| 1 | a |     | 2 | k |     | 2 | b | k |
| 2 | b |     | 3 | m |     | 3 | c | m |
| 3 | c |     | 3 | n |     | 3 | c | n |
| 3 | d |     | 4 | p |     | 3 | d | m |
+-------+     +-------+     | 3 | d | n |
                            +-----------+
```

**Example**

This query performs an `INNER JOIN` on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Buchanan   | Lakers       |
| Coolidge   | Lakers       |
| Davis      | Knights      |
+---------------------------+
```

### CROSS JOIN

`CROSS JOIN` returns the Cartesian product of the two `from_item`s. In other
words, it combines each row from the first `from_item` with each row from the
second `from_item`.

If the rows of the two `from_item`s are independent, then the result has *M* *
*N* rows, given *M* rows in one `from_item` and *N* in the other. Note that this
still holds for the case when either `from_item` has zero rows.

```sql
FROM A CROSS JOIN B

Table A       Table B       Result
+-------+     +-------+     +---------------+
| w | x |  *  | y | z |  =  | w | x | y | z |
+-------+     +-------+     +---------------+
| 1 | a |     | 2 | c |     | 1 | a | 2 | c |
| 2 | b |     | 3 | d |     | 1 | a | 3 | d |
+-------+     +-------+     | 2 | b | 2 | c |
                            | 2 | b | 3 | d |
                            +---------------+
```

You can use *correlated* `CROSS JOIN`s to
[flatten `ARRAY` columns][flattening-arrays]. In this case, the rows of the
second `from_item` vary for each row of the first `from_item`.

```sql
FROM A CROSS JOIN A.y

Table A                    Result
+-------------------+      +-----------+
| w | x | y         |  ->  | w | x | y |
+-------------------+      +-----------+
| 1 | a | [P, Q]    |      | 1 | a | P |
| 2 | b | [R, S, T] |      | 1 | a | Q |
+-------------------+      | 2 | b | R |
                           | 2 | b | S |
                           | 2 | b | T |
                           +-----------+
```

`CROSS JOIN`s can be written explicitly like this:

```sql
FROM a CROSS JOIN b
```

Or implicitly as a comma cross join like this:

```sql
FROM a, b
```

You cannot write comma cross joins inside parentheses:

```sql {.bad}
FROM a CROSS JOIN (b, c)  // INVALID
```

See [Sequences of JOINs][sequences-of-joins] for details on how a comma cross
join behaves in a sequence of JOINs.

**Examples**

This query performs an explicit `CROSS JOIN` on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster CROSS JOIN TeamMascot;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Adams      | Knights      |
| Adams      | Lakers       |
| Adams      | Mustangs     |
| Buchanan   | Jaguars      |
| Buchanan   | Knights      |
| Buchanan   | Lakers       |
| Buchanan   | Mustangs     |
| ...                       |
+---------------------------+
```

This query performs a comma cross join that produces the same results as the
explicit `CROSS JOIN` above:

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster, TeamMascot;
```

### FULL [OUTER] JOIN

A `FULL OUTER JOIN` (or simply `FULL JOIN`) returns all fields for all rows in
both `from_item`s that meet the join condition.

`FULL` indicates that _all rows_ from both `from_item`s are
returned, even if they do not meet the join condition.

`OUTER` indicates that if a given row from one `from_item` does not
join to any row in the other `from_item`, the row will return with NULLs
for all columns from the other `from_item`.

```sql
FROM A FULL OUTER JOIN B ON A.w = B.y

Table A       Table B       Result
+-------+     +-------+     +---------------------------+
| w | x |  *  | y | z |  =  | w    | x    | y    | z    |
+-------+     +-------+     +---------------------------+
| 1 | a |     | 2 | k |     | 1    | a    | NULL | NULL |
| 2 | b |     | 3 | m |     | 2    | b    | 2    | k    |
| 3 | c |     | 3 | n |     | 3    | c    | 3    | m    |
| 3 | d |     | 4 | p |     | 3    | c    | 3    | n    |
+-------+     +-------+     | 3    | d    | 3    | m    |
                            | 3    | d    | 3    | n    |
                            | NULL | NULL | 4    | p    |
                            +---------------------------+
```

```sql
FROM A FULL OUTER JOIN B USING (x)

Table A       Table B       Result
+-------+     +-------+     +--------------------+
| x | y |  *  | x | z |  =  | x    | y    | z    |
+-------+     +-------+     +--------------------+
| 1 | a |     | 2 | k |     | 1    | a    | NULL |
| 2 | b |     | 3 | m |     | 2    | b    | k    |
| 3 | c |     | 3 | n |     | 3    | c    | m    |
| 3 | d |     | 4 | p |     | 3    | c    | n    |
+-------+     +-------+     | 3    | d    | m    |
                            | 3    | d    | n    |
                            | 4    | NULL | p    |
                            +--------------------+
```

**Example**

This query performs a `FULL JOIN` on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster FULL JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Buchanan   | Lakers       |
| Coolidge   | Lakers       |
| Davis      | Knights      |
| Eisenhower | NULL         |
| NULL       | Mustangs     |
+---------------------------+
```

### LEFT [OUTER] JOIN

The result of a `LEFT OUTER JOIN` (or simply `LEFT JOIN`) for two
`from_item`s always retains all rows of the left `from_item` in the
`JOIN` clause, even if no rows in the right `from_item` satisfy the join
predicate.

`LEFT` indicates that all rows from the _left_ `from_item` are
returned; if a given row from the left `from_item` does not join to any row
in the _right_ `from_item`, the row will return with NULLs for all
columns from the right `from_item`.  Rows from the right `from_item` that
do not join to any row in the left `from_item` are discarded.

```sql
FROM A LEFT OUTER JOIN B ON A.w = B.y

Table A       Table B       Result
+-------+     +-------+     +---------------------------+
| w | x |  *  | y | z |  =  | w    | x    | y    | z    |
+-------+     +-------+     +---------------------------+
| 1 | a |     | 2 | k |     | 1    | a    | NULL | NULL |
| 2 | b |     | 3 | m |     | 2    | b    | 2    | k    |
| 3 | c |     | 3 | n |     | 3    | c    | 3    | m    |
| 3 | d |     | 4 | p |     | 3    | c    | 3    | n    |
+-------+     +-------+     | 3    | d    | 3    | m    |
                            | 3    | d    | 3    | n    |
                            +---------------------------+
```

```sql
FROM A LEFT OUTER JOIN B USING (x)

Table A       Table B       Result
+-------+     +-------+     +--------------------+
| x | y |  *  | x | z |  =  | x    | y    | z    |
+-------+     +-------+     +--------------------+
| 1 | a |     | 2 | k |     | 1    | a    | NULL |
| 2 | b |     | 3 | m |     | 2    | b    | k    |
| 3 | c |     | 3 | n |     | 3    | c    | m    |
| 3 | d |     | 4 | p |     | 3    | c    | n    |
+-------+     +-------+     | 3    | d    | m    |
                            | 3    | d    | n    |
                            +--------------------+
```

**Example**

This query performs a `LEFT JOIN` on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster LEFT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Buchanan   | Lakers       |
| Coolidge   | Lakers       |
| Davis      | Knights      |
| Eisenhower | NULL         |
+---------------------------+
```

### RIGHT [OUTER] JOIN

The result of a `RIGHT OUTER JOIN` (or simply `RIGHT JOIN`) is similar and
symmetric to that of `LEFT OUTER JOIN`.

```sql
FROM A RIGHT OUTER JOIN B ON A.w = B.y

Table A       Table B       Result
+-------+     +-------+     +---------------------------+
| w | x |  *  | y | z |  =  | w    | x    | y    | z    |
+-------+     +-------+     +---------------------------+
| 1 | a |     | 2 | k |     | 2    | b    | 2    | k    |
| 2 | b |     | 3 | m |     | 3    | c    | 3    | m    |
| 3 | c |     | 3 | n |     | 3    | c    | 3    | n    |
| 3 | d |     | 4 | p |     | 3    | d    | 3    | m    |
+-------+     +-------+     | 3    | d    | 3    | n    |
                            | NULL | NULL | 4    | p    |
                            +---------------------------+
```

```sql
FROM A RIGHT OUTER JOIN B USING (x)

Table A       Table B       Result
+-------+     +-------+     +--------------------+
| x | y |  *  | x | z |  =  | x    | y    | z    |
+-------+     +-------+     +--------------------+
| 1 | a |     | 2 | k |     | 2    | b    | k    |
| 2 | b |     | 3 | m |     | 3    | c    | m    |
| 3 | c |     | 3 | n |     | 3    | c    | n    |
| 3 | d |     | 4 | p |     | 3    | d    | m    |
+-------+     +-------+     | 3    | d    | n    |
                            | 4    | NULL | p    |
                            +--------------------+
```

**Example**

This query performs a `RIGHT JOIN` on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster RIGHT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Buchanan   | Lakers       |
| Coolidge   | Lakers       |
| Davis      | Knights      |
| NULL       | Mustangs     |
+---------------------------+
```

### ON clause 
<a id="on_clause"></a>

The `ON` clause contains a `bool_expression`. A combined row (the result of
joining two rows) meets the join condition if `bool_expression` returns
TRUE.

```sql
FROM A JOIN B ON A.x = B.x

Table A   Table B   Result (A.x, B.x)
+---+     +---+     +-------+
| x |  *  | x |  =  | x | x |
+---+     +---+     +-------+
| 1 |     | 2 |     | 2 | 2 |
| 2 |     | 3 |     | 3 | 3 |
| 3 |     | 4 |     +-------+
+---+     +---+
```

**Example**

This query performs an `INNER JOIN` on the
[`Roster`][roster-table] and [`TeamMascot`][teammascot-table] table.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID;

+---------------------------+
| LastName   | Mascot       |
+---------------------------+
| Adams      | Jaguars      |
| Buchanan   | Lakers       |
| Coolidge   | Lakers       |
| Davis      | Knights      |
+---------------------------+
```

### USING clause 
<a id="using_clause"></a>

The `USING` clause requires a `column_list` of one or more columns which
occur in both input tables. It performs an equality comparison on that column,
and the rows meet the join condition if the equality comparison returns TRUE.

```sql
FROM A JOIN B USING (x)

Table A   Table B   Result
+---+     +---+     +---+
| x |  *  | x |  =  | x |
+---+     +---+     +---+
| 1 |     | 2 |     | 2 |
| 2 |     | 3 |     | 3 |
| 3 |     | 4 |     +---+
+---+     +---+
```

**NOTE**: The `USING` keyword is not supported in
strict
mode.

**Example**

This query performs an `INNER JOIN` on the
[`Roster`][roster-table] and [`TeamMascot`][teammascot-table] table.

This statement returns the rows from `Roster` and `TeamMascot` where
`Roster.SchooldID` is the same as `TeamMascot.SchooldID`.  The results include
a single `SchooldID` column.

```sql
SELECT * FROM Roster INNER JOIN TeamMascot USING (SchoolID);

+----------------------------------------+
| SchoolID   | LastName   | Mascot       |
+----------------------------------------+
| 50         | Adams      | Jaguars      |
| 52         | Buchanan   | Lakers       |
| 52         | Coolidge   | Lakers       |
| 51         | Davis      | Knights      |
+----------------------------------------+
```

### ON and USING equivalency

The `ON` and `USING` keywords are not equivalent, but they are similar.
`ON` returns multiple columns, and `USING` returns one.

```sql
FROM A JOIN B ON A.x = B.x
FROM A JOIN B USING (x)

Table A   Table B   Result ON     Result USING
+---+     +---+     +-------+     +---+
| x |  *  | x |  =  | x | x |     | x |
+---+     +---+     +-------+     +---+
| 1 |     | 2 |     | 2 | 2 |     | 2 |
| 2 |     | 3 |     | 3 | 3 |     | 3 |
| 3 |     | 4 |     +-------+     +---+
+---+     +---+
```

Although `ON` and `USING` are not equivalent, they can return the same results
if you specify the columns you want to return.

```sql
SELECT x FROM A JOIN B USING (x);
SELECT A.x FROM A JOIN B ON A.x = B.x;

Table A   Table B   Result
+---+     +---+     +---+
| x |  *  | x |  =  | x |
+---+     +---+     +---+
| 1 |     | 2 |     | 2 |
| 2 |     | 3 |     | 3 |
| 3 |     | 4 |     +---+
+---+     +---+
```

### Sequences of JOINs 
<a id="sequences_of_joins"></a>

The `FROM` clause can contain multiple `JOIN` clauses in a sequence.
`JOIN`s are bound from left to right. For example:

```sql
FROM A JOIN B USING (x) JOIN C USING (x)

-- A JOIN B USING (x)        = result_1
-- result_1 JOIN C USING (x) = result_2
-- result_2                  = return value
```

You can also insert parentheses to group `JOIN`s:

```sql
FROM ( (A JOIN B USING (x)) JOIN C USING (x) )

-- A JOIN B USING (x)        = result_1
-- result_1 JOIN C USING (x) = result_2
-- result_2                  = return value
```

With parentheses, you can group `JOIN`s so that they are bound in a different
order:

```sql
FROM ( A JOIN (B JOIN C USING (x)) USING (x) )

-- B JOIN C USING (x)       = result_1
-- A JOIN result_1          = result_2
-- result_2                 = return value
```

When comma cross joins are present in a query with a sequence of JOINs, they
group from left to right like other `JOIN` types:

```sql
FROM A JOIN B USING (x) JOIN C USING (x), D

-- A JOIN B USING (x)        = result_1
-- result_1 JOIN C USING (x) = result_2
-- result_2 CROSS JOIN D     = return value
```

There cannot be a `RIGHT JOIN` or `FULL JOIN` after a comma join:

```sql {.bad}
FROM A, B RIGHT JOIN C ON TRUE // INVALID
```

```sql {.bad}
FROM A, B FULL JOIN C ON TRUE  // INVALID
```

```sql
FROM A, B JOIN C ON TRUE       // VALID
```

## WHERE clause 
<a id="where_clause"></a>

```sql
WHERE bool_expression
```

The `WHERE` clause filters out rows by evaluating each row against
`bool_expression`, and discards all rows that do not return TRUE (that is,
rows that return FALSE or NULL).

Example:

```sql
SELECT * FROM Roster
WHERE SchoolID = 52;
```

The `bool_expression` can contain multiple sub-conditions.

Example:

```sql
SELECT * FROM Roster
WHERE STARTS_WITH(LastName, "Mc") OR STARTS_WITH(LastName, "Mac");
```

You cannot reference column aliases from the `SELECT` list in the `WHERE`
clause.

Expressions in an `INNER JOIN` have an equivalent expression in the
`WHERE` clause. For example, a query using `INNER` `JOIN` and `ON` has an
equivalent expression using `CROSS JOIN` and `WHERE`.

Example - this query:

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster INNER JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

is equivalent to:

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster CROSS JOIN TeamMascot
WHERE Roster.SchoolID = TeamMascot.SchoolID;
```

## GROUP BY clause 
<a id="group_by_clause"></a>

<pre>
GROUP BY { <span class="var">expression</span> [, ...] | ROLLUP ( <span class="var">expression</span> [, ...] ) }
</pre>

The `GROUP BY` clause groups together rows in a table with non-distinct values
for the `expression` in the `GROUP BY` clause. For multiple rows in the
source table with non-distinct values for `expression`, the
`GROUP BY` clause produces a single combined row. `GROUP BY` is commonly used
when aggregate functions are present in the `SELECT` list, or to eliminate
redundancy in the output. The data type of `expression` must be [groupable][data-type-properties].

Example:

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY LastName;
```

The `GROUP BY` clause can refer to expression names in the `SELECT` list. The
`GROUP BY` clause also allows ordinal references to expressions in the `SELECT`
list using integer values. `1` refers to the first expression in the
`SELECT` list, `2` the second, and so forth. The expression list can combine
ordinals and expression names.

Example:

```sql
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY LastName, FirstName;
```

The query above is equivalent to:

```sql
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY 2, FirstName;
```

`GROUP BY` clauses may also refer to aliases. If a query contains aliases in
the `SELECT` clause, those aliases override names in the corresponding `FROM`
clause.

Example:

```sql
SELECT SUM(PointsScored), LastName as last_name
FROM PlayerStats
GROUP BY last_name;
```

`GROUP BY` can group rows by the value of an `ARRAY`.
`GROUP BY` will group two arrays if they have the same number of elements and
all corresponding elements are in the same groups, or if both arrays are null.

`GROUP BY ROLLUP` returns the results of `GROUP BY` for
prefixes of the expressions in the `ROLLUP` list, each of which is known as a
*grouping set*.  For the `ROLLUP` list `(a, b, c)`, the grouping sets are
`(a, b, c)`, `(a, b)`, `(a)`, `()`. When evaluating the results of `GROUP BY`
for a particular grouping set, `GROUP BY ROLLUP` treats expressions that are not
in the grouping set as having a `NULL` value. A `SELECT` statement like this
one:

```sql
SELECT a, b, SUM(c) FROM Input GROUP BY ROLLUP(a, b);
```

uses the rollup list `(a, b)`. The result will include the
results of `GROUP BY` for the grouping sets `(a, b)`, `(a)`, and `()`, which
includes all rows. This returns the same rows as:

```sql
SELECT NULL, NULL, SUM(c) FROM Input               UNION ALL
SELECT a,    NULL, SUM(c) FROM Input GROUP BY a    UNION ALL
SELECT a,    b,    SUM(c) FROM Input GROUP BY a, b;
```

This allows the computation of aggregates for the grouping sets defined by the
expressions in the `ROLLUP` list and the prefixes of that list.

Example:

```sql
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(day);
```

The query above outputs a row for each day in addition to the rolled up total
across all days, as indicated by a `NULL` day:

```sql
+------+-------+
| day  | total |
+------+-------+
| NULL | 39.77 |
|    1 | 23.54 |
|    2 |  9.99 |
|    3 |  6.24 |
+------+-------+
```

Example:

```sql
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  sku,
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(sku, day)
ORDER BY sku, day;
```

The query above returns rows grouped by the following grouping sets:

+ sku and day
+ sku (day is `NULL`)
+ The empty grouping set (day and sku are `NULL`)

The sums for these grouping sets correspond to the total for each
distinct sku-day combination, the total for each sku across all days, and the
grand total:

```sql
+------+------+-------+
| sku  | day  | total |
+------+------+-------+
| NULL | NULL | 39.77 |
|  123 | NULL | 28.97 |
|  123 |    1 | 18.98 |
|  123 |    2 |  9.99 |
|  456 | NULL |  8.81 |
|  456 |    1 |  4.56 |
|  456 |    3 |  4.25 |
|  789 |    3 |  1.99 |
|  789 | NULL |  1.99 |
+------+------+-------+
```

## HAVING clause 
<a id="having_clause"></a>

```sql
HAVING bool_expression
```

The `HAVING` clause is similar to the `WHERE` clause: it filters out rows that
do not return TRUE when they are evaluated against the `bool_expression`.

As with the `WHERE` clause, the `bool_expression` can be any expression
that returns a boolean, and can contain multiple sub-conditions.

The `HAVING` clause differs from the `WHERE` clause in that:

  * The `HAVING` clause requires `GROUP BY` or aggregation to be present in the
     query.
  * The `HAVING` clause occurs after `GROUP BY` and aggregation, and before
     `ORDER BY`. This means that the `HAVING` clause is evaluated once for every
     aggregated row in the result set. This differs from the `WHERE` clause,
     which is evaluated before `GROUP BY` and aggregation.

The `HAVING` clause can reference columns available via the `FROM` clause, as
well as `SELECT` list aliases. Expressions referenced in the `HAVING` clause
must either appear in the `GROUP BY` clause or they must be the result of an
aggregate function:

```sql
SELECT LastName
FROM Roster
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

If a query contains aliases in the `SELECT` clause, those aliases override names
in a `FROM` clause.

```sql
SELECT LastName, SUM(PointsScored) AS ps
FROM Roster
GROUP BY LastName
HAVING ps > 0;
```

### Mandatory aggregation 
<a id="mandatory_aggregation"></a>

Aggregation does not have to be present in the `HAVING` clause itself, but
aggregation must be present in at least one of the following forms:

#### Aggregation function in the `SELECT` list.

```sql
SELECT LastName, SUM(PointsScored) AS total
FROM PlayerStats
GROUP BY LastName
HAVING total > 15;
```

#### Aggregation function in the 'HAVING' clause.

```sql
SELECT LastName
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

#### Aggregation in both the `SELECT` list and `HAVING` clause.

When aggregation functions are present in both the `SELECT` list and `HAVING`
clause, the aggregation functions and the columns they reference do not need
to be the same. In the example below, the two aggregation functions,
`COUNT()` and `SUM()`, are different and also use different columns.

```sql
SELECT LastName, COUNT(*)
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

## ORDER BY clause 
<a id="order_by_clause"></a>

<pre>
ORDER BY expression
  [{ ASC | DESC }]
  [, ...]
</pre>

The `ORDER BY` clause specifies a column or expression as the sort criterion for
the result set. If an ORDER BY clause is not present, the order of the results
of a query is not defined. Column aliases from a `FROM` clause or `SELECT` list
are allowed. If a query contains aliases in the `SELECT` clause, those aliases
override names in the corresponding `FROM` clause.

**Optional Clauses**

+  `ASC | DESC`: Sort the results in ascending or descending
    order of `expression` values. `ASC` is the default value. 

**Examples**

Use the default sort order (ascending).

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true)
ORDER BY x;
+------+-------+
| x    | y     |
+------+-------+
| 1    | true  |
| 9    | true  |
+------+-------+
```

Use descending sort order.

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true)
ORDER BY x DESC;
+------+-------+
| x    | y     |
+------+-------+
| 9    | true  |
| 1    | true  |
+------+-------+
```

It is possible to order by multiple columns. In the example below, the result
set is ordered first by `SchoolID` and then by `LastName`:

```sql
SELECT LastName, PointsScored, OpponentID
FROM PlayerStats
ORDER BY SchoolID, LastName;
```

The following rules apply when ordering values:

+  NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
   possible value; that is, NULLs appear first in `ASC` sorts and last in `DESC`
   sorts.
+  Floating point data types: see
   [Floating Point Semantics][floating-point-semantics]
   on ordering and grouping.

When used in conjunction with [set operators][set-operators], the `ORDER BY` clause applies to the result set of the entire query; it does not
apply only to the closest `SELECT` statement. For this reason, it can be helpful
(though it is not required) to use parentheses to show the scope of the `ORDER
BY`.

This query without parentheses:

```sql
SELECT * FROM Roster
UNION ALL
SELECT * FROM TeamMascot
ORDER BY SchoolID;
```

is equivalent to this query with parentheses:

```sql
( SELECT * FROM Roster
  UNION ALL
  SELECT * FROM TeamMascot )
ORDER BY SchoolID;
```

but is not equivalent to this query, where the `ORDER BY` clause applies only to
the second `SELECT` statement:

```sql
SELECT * FROM Roster
UNION ALL
( SELECT * FROM TeamMascot
  ORDER BY SchoolID );
```

You can also use integer literals as column references in `ORDER BY` clauses. An
integer literal becomes an ordinal (for example, counting starts at 1) into
the `SELECT` list.

Example - the following two queries are equivalent:

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY LastName;
```

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY 2;
```

### COLLATE

You can use the `COLLATE` clause to refine how a data is ordered from an `ORDER
BY` clause. *Collation* refers to a set of rules that determine how
STRINGs are compared according to the conventions and
standards of a particular language, region or country. These rules might define
the correct character sequence, with options for specifying case-insensitivity.

Note: You can use `COLLATE` only on columns of type
STRING.

You add collation to your statement as follows:

```sql
SELECT ...
FROM ...
ORDER BY value COLLATE collation_string
```

A `collation_string` contains a `collation_name` and can have an optional
`collation_attribute` as a suffix, separated by a colon. The `collation_string`
is a literal or a parameter.  Usually, this name is two letters that represent
the language optionally followed by an underscore and two letters that
represent the region&mdash;for example, `en_US`. These names are defined by the
[Common Locale Data Repository (CLDR)][language-territory-information].
A statement can also have a `collation_name` of `unicode`. This value means that
the statement should return data using the default unicode collation.

In addition to the `collation_name`, a `collation_string` can have an optional
`collation_attribute` as a suffix, separated by a colon. This attribute
specifies if the data comparisons should be case sensitive. Allowed values are
`cs`, for case sensitive, and `ci`, for case insensitive. If a
`collation_attribute` is not supplied, the
[CLDR defaults][tr35-collation-settings]
are used.

**Examples**

Collate results using English - Canada:

```sql
SELECT Place
FROM Locations
ORDER BY Place COLLATE "en_CA"
```

Collate results using a parameter:

```sql
#@collate_param = "arg_EG"
SELECT Place
FROM Locations
ORDER BY Place COLLATE @collate_param
```

Using multiple `COLLATE` clauses in a statement:

```sql
SELECT APlace, BPlace, CPlace
FROM Locations
ORDER BY APlace COLLATE "en_US" ASC,
         BPlace COLLATE "ar_EG" DESC,
         CPlace COLLATE "en" DESC
```

Case insensitive collation:

```sql
SELECT Place
FROM Locations
ORDER BY Place COLLATE "en_US:ci"
```

Default Unicode case-insensitive collation:

```sql
SELECT Place
FROM Locations
ORDER BY Place COLLATE "unicode:ci"
```

## WINDOW clause 
<a id="window_clause"></a>

<pre>
WINDOW named_window_expression [, ...]

named_window_expression:
  named_window AS { named_window | ( [ window_specification ] ) }
</pre>

A `WINDOW` clause defines a list of named windows.
A named window represents a group of rows in a table upon which to use an
[analytic function][analytic-concepts]. A named window can be defined with
a [window specification][query-window-specification] or reference another
named window. If another named window is referenced, the definition of the
referenced window must precede the referencing window.

**Examples**

These examples reference a table called [`Produce`][produce-table].
They all return the same [result][named-window-example]. Note the different
ways you can combine named windows and use them in an analytic function's
`OVER` clause.

```sql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (item_window) AS most_popular
FROM Produce
WINDOW item_window AS (
  PARTITION BY category
  ORDER BY purchases
  ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
```

```sql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (d) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  c AS (b ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),
  d AS (c)
```

```sql
SELECT item, purchases, category, LAST_VALUE(item)
  OVER (c ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular
FROM Produce
WINDOW
  a AS (PARTITION BY category),
  b AS (a ORDER BY purchases),
  c AS b
```

## Set operators 
<a id="set_operators"></a>

<pre>
UNION { ALL | DISTINCT } | <a href="#intersect">INTERSECT</a> { ALL | DISTINCT } | <a href="#except">EXCEPT</a> { ALL | DISTINCT }
</pre>

Set operators combine results from two or
more input queries into a single result set. You
must specify `ALL` or `DISTINCT`; if you specify `ALL`, then all rows are
retained.  If `DISTINCT` is specified, duplicate rows are discarded.

If a given row R appears exactly m times in the first input query and n times
in the second input query (m >= 0, n >= 0):

+  For `UNION ALL`, R appears exactly m + n times in the
   result.
+  For `INTERSECT ALL`, R will appear exactly `MIN(m, n)` in the
   result.
+  For `EXCEPT ALL`, R appears exactly `MAX(m - n, 0)` in the
   result.
+  For `UNION DISTINCT`, the `DISTINCT`
   is computed after the `UNION` is computed, so R appears exactly
   one time.
+  For `INTERSECT DISTINCT`, the `DISTINCT` is computed
   after the result above is computed.
+  For `EXCEPT DISTINCT`, row R appears once in the output if
   m > 0 and n = 0.
+  If there are more than two input queries, the above operations generalize
   and the output is the same as if the inputs were combined incrementally from
   left to right.

The following rules apply:

+  For set operations other than `UNION
   ALL`, all column types must support
   equality comparison.
+  The input queries on each side of the operator must return the same
   number of columns.
+  The operators pair the columns returned by each input query according to
   the columns' positions in their respective `SELECT` lists. That is, the first
   column in the first input query is paired with the first column in the second
   input query.
+  The result set always uses the column names from the first input query.
+  The result set always uses the supertypes of input types in corresponding
   columns, so paired columns must also have either the same data type or a
   common supertype.
+  You must use parentheses to separate different set
   operations; for this purpose, set operations such as `UNION ALL` and
   `UNION DISTINCT` are different. If the statement only repeats
   the same set operation, parentheses are not necessary.

Examples:

```sql
query1 UNION ALL (query2 UNION DISTINCT query3)
query1 UNION ALL query2 UNION ALL query3
```

Invalid:

```sql {.bad}
query1 UNION ALL query2 UNION DISTINCT query3
query1 UNION ALL query2 INTERSECT ALL query3;  // INVALID.
```

### UNION 
<a id="union"></a>

The `UNION` operator combines the result sets of two or more input queries by
pairing columns from the result set of each query and vertically concatenating
them.

### INTERSECT 
<a id="intersect"></a>

The `INTERSECT` operator returns rows that are found in the result sets of both
the left and right input queries. Unlike `EXCEPT`, the positioning of the input
queries (to the left versus right of the `INTERSECT` operator) does not matter.

### EXCEPT 
<a id="except"></a>

The `EXCEPT` operator returns rows from the left input query that are
not present in the right input query.

Example:

```sql
SELECT * FROM UNNEST(ARRAY<int64>[1, 2, 3]) AS number
EXCEPT DISTINCT SELECT 1;

+--------+
| number |
+--------+
| 2      |
| 3      |
+--------+
```

## LIMIT and OFFSET clauses 
<a id="limit_and_offset_clause"></a>

```sql
LIMIT count [ OFFSET skip_rows ]
```

`LIMIT` specifies a non-negative `count` of type INT64,
and no more than `count` rows will be returned. `LIMIT` `0` returns 0 rows.

If there is a set operation, `LIMIT` is applied after the set operation is
evaluated.

`OFFSET` specifies a non-negative number of rows to skip before applying
`LIMIT`. `skip_rows` is of type INT64.

These clauses accept only literal or parameter values. The rows that are
returned by `LIMIT` and `OFFSET` are unspecified unless these
operators are used after `ORDER BY`.

Examples:

```sql
SELECT *
FROM UNNEST(ARRAY<STRING>['a', 'b', 'c', 'd', 'e']) AS letter
ORDER BY letter ASC LIMIT 2

+---------+
| letter  |
+---------+
| a       |
| b       |
+---------+
```

```sql
SELECT *
FROM UNNEST(ARRAY<STRING>['a', 'b', 'c', 'd', 'e']) AS letter
ORDER BY letter ASC LIMIT 3 OFFSET 1

+---------+
| letter  |
+---------+
| b       |
| c       |
| d       |
+---------+
```

## WITH clause 
<a id="with_clause"></a>

The `WITH` clause binds the results of one or more named
[subqueries][subquery-concepts] to temporary table names.  Each introduced
table name is visible in subsequent `SELECT` expressions within the same
query expression. This includes the following kinds of `SELECT` expressions:

+ Any `SELECT` expressions in subsequent `WITH` bindings
+ Top level `SELECT` expressions in the query expression on both sides of a set
  operator such as `UNION`
+ `SELECT` expressions inside subqueries within the same query expression

Example:

```sql
WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2;
```

You can use `WITH` to break up more complex queries into a `WITH` `SELECT`
statement and `WITH` clauses, where the less desirable alternative is writing
nested table subqueries. For example:

```sql
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1) SELECT * FROM q2)
```

Note: If a `WITH` clause contains multiple subqueries, the subquery names cannot
repeat.

The following are scoping rules for `WITH` clauses:

+ Aliases are scoped so that the aliases introduced in a `WITH` clause are
  visible only in the later subqueries in the same `WITH` clause, and in the
  query under the `WITH` clause.
+ Aliases introduced in the same `WITH` clause must be unique, but the same
  alias can be used in multiple `WITH` clauses in the same query.  The local
  alias overrides any outer aliases anywhere that the local alias is visible.
+ Aliased subqueries in a `WITH` clause can never be correlated. No columns from
  outside the query are visible.  The only names from outside that are visible
  are other `WITH` aliases that were introduced earlier in the same `WITH`
  clause.

Here's an example of a statement that uses aliases in `WITH` subqueries:

```sql
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
        q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery
                                   # on the previous line.
    SELECT * FROM q1)  # q1 resolves to the third inner WITH subquery.
```

`WITH RECURSIVE` is not supported.

## Using aliases 
<a id="using_aliases"></a>

An alias is a temporary name given to a table, column, or expression present in
a query. You can introduce explicit aliases in the `SELECT` list or `FROM`
clause, or ZetaSQL will infer an implicit alias for some expressions.
Expressions with neither an explicit nor implicit alias are anonymous and the
query cannot reference them by name.

### Explicit aliases 
<a id="explicit_alias_syntax"></a>

You can introduce explicit aliases in either the `FROM` clause or the `SELECT`
list.

In a `FROM` clause, you can introduce explicit aliases for any item, including
tables, arrays, subqueries, and `UNNEST` clauses, using `[AS] alias`.  The `AS`
keyword is optional.

Example:

```sql
SELECT s.FirstName, s2.SongName
FROM Singers AS s, (SELECT * FROM Songs) AS s2;
```

You can introduce explicit aliases for any expression in the `SELECT` list using
`[AS] alias`. The `AS` keyword is optional.

Example:

```sql
SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname
FROM Singers s;
```

### Implicit aliases 
<a id="implicit_aliases"></a>

In the `SELECT` list, if there is an expression that does not have an explicit
alias, ZetaSQL assigns an implicit alias according to the following
rules. There can be multiple columns with the same alias in the `SELECT` list.

+  For identifiers, the alias is the identifier. For example, `SELECT abc`
   implies `AS abc`.
+  For path expressions, the alias is the last identifier in the path. For
   example, `SELECT abc.def.ghi` implies `AS ghi`.
+  For field access using the "dot" member field access operator, the alias is
   the field name. For example, `SELECT (struct_function()).fname` implies `AS
   fname`.

In all other cases, there is no implicit alias, so the column is anonymous and
cannot be referenced by name. The data from that column will still be returned
and the displayed query results may have a generated label for that column, but
the label cannot be used like an alias.

In a `FROM` clause, `from_item`s are not required to have an alias. The
following rules apply:

<ul>
  <li>
    If there is an expression that does not have an explicit alias,
    ZetaSQL assigns an implicit alias in these cases:
  </li>
<ul>
  <li>
    For identifiers, the alias is the identifier. For example,
    <code>FROM abc</code> implies <code>AS abc</code>.
  </li>
  <li>
    For path expressions, the alias is the last identifier in the path. For
    example, <code>FROM abc.def.ghi</code> implies <code>AS ghi</code>
  </li>
  <li>
    The column produced using <code>WITH OFFSET</code> has the implicit alias
    <code>offset</code>.
  </li>
</ul>
  <li>
    Table subqueries do not have implicit aliases.
  </li>
  <li>
    <code>FROM UNNEST(x)</code> does not have an implicit alias.
  </li>
</ul>

### Alias visibility 
<a id="alias_visibility"></a>

After you introduce an explicit alias in a query, there are restrictions on
where else in the query you can reference that alias. These restrictions on
alias visibility are the result of ZetaSQL's name scoping rules.

#### Visibility in the FROM clause 
<a id="from_clause_aliases"></a>

ZetaSQL processes aliases in a `FROM` clause from left to right,
and aliases are visible only to subsequent path expressions in a `FROM`
clause.

Example:

Assume the `Singers` table had a `Concerts` column of `ARRAY` type.

```sql
SELECT FirstName
FROM Singers AS s, s.Concerts;
```

Invalid:

```sql {.bad}
SELECT FirstName
FROM s.Concerts, Singers AS s;  // INVALID.
```

`FROM` clause aliases are **not** visible to subqueries in the same `FROM`
clause. Subqueries in a `FROM` clause cannot contain correlated references to
other tables in the same `FROM` clause.

Invalid:

```sql {.bad}
SELECT FirstName
FROM Singers AS s, (SELECT (2020 - ReleaseDate) FROM s)  // INVALID.
```

You can use any column name from a table in the `FROM` as an alias anywhere in
the query, with or without qualification with the table name.

Example:

```sql
SELECT FirstName, s.ReleaseDate
FROM Singers s WHERE ReleaseDate = 1975;
```

If the `FROM` clause contains an explicit alias, you must use the explicit alias
instead of the implicit alias for the remainder of the query (see
[Implicit Aliases][implicit-aliases]). A table alias is useful for brevity or
to eliminate ambiguity in cases such as self-joins, where the same table is
scanned multiple times during query processing.

Example:

```sql
SELECT * FROM Singers as s, Songs as s2
ORDER BY s.LastName
```

Invalid &mdash; `ORDER BY` does not use the table alias:

```sql {.bad}
SELECT * FROM Singers as s, Songs as s2
ORDER BY Singers.LastName;  // INVALID.
```

#### Visibility in the SELECT list 
<a id="select-list_aliases"></a>

Aliases in the `SELECT` list are visible only to the following clauses:

+  `GROUP BY` clause
+  `ORDER BY` clause
+  `HAVING` clause

Example:

```sql
SELECT LastName AS last, SingerID
FROM Singers
ORDER BY last;
```

#### Visibility in the GROUP BY, ORDER BY, and HAVING clauses 
<a id="aliases_clauses"></a>

These three clauses, `GROUP BY`, `ORDER BY`, and `HAVING`, can refer to only the
following values:

+  Tables in the `FROM` clause and any of their columns.
+  Aliases from the `SELECT` list.

`GROUP BY` and `ORDER BY` can also refer to a third group:

+  Integer literals, which refer to items in the `SELECT` list. The integer `1`
   refers to the first item in the `SELECT` list, `2` refers to the second item,
   etc.

Example:

```sql
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY 1
ORDER BY 2 DESC;
```

The previous query is equivalent to:

```sql
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY sid
ORDER BY s2id DESC;
```

### Duplicate aliases 
<a id="duplicate_aliases"></a>

A `SELECT` list or subquery containing multiple explicit or implicit aliases
of the same name is allowed, as long as the alias name is not referenced
elsewhere in the query, since the reference would be
[ambiguous][ambiguous-aliases].

Example:

```sql
SELECT 1 AS a, 2 AS a;

+---+---+
| a | a |
+---+---+
| 1 | 2 |
+---+---+
```

### Ambiguous aliases 
<a id="ambiguous_aliases"></a>

ZetaSQL provides an error if accessing a name is ambiguous, meaning
it can resolve to more than one unique object in the query or in a table schema,
including the schema of a destination table.

Examples:

This query contains column names that conflict between tables, since both
`Singers` and `Songs` have a column named `SingerID`:

```sql
SELECT SingerID
FROM Singers, Songs;
```

This query contains aliases that are ambiguous in the `GROUP BY` clause because
they are duplicated in the `SELECT` list:

```sql {.bad}
SELECT FirstName AS name, LastName AS name,
FROM Singers
GROUP BY name;
```

This query contains aliases that are ambiguous in the `SELECT` list and `FROM`
clause because they share the same name. Assume `table` has columns `x`, `y`,
and `z`. `z` is of type STRUCT and has fields
`v`, `w`, and `x`.

Example:

```sql {.bad}
SELECT x, z AS T
FROM table AS T
GROUP BY T.x;
```

The alias `T` is ambiguous and will produce an error because `T.x` in the `GROUP
BY` clause could refer to either `table.x` or `table.z.x`.

A name is **not** ambiguous in `GROUP BY`, `ORDER BY` or `HAVING` if it is both
a column name and a `SELECT` list alias, as long as the name resolves to the
same underlying object.

Example:

```sql
SELECT LastName, BirthYear AS BirthYear
FROM Singers
GROUP BY BirthYear;
```

The alias `BirthYear` is not ambiguous because it resolves to the same
underlying column, `Singers.BirthYear`.

### Implicit aliases 
<a id="implicit_aliases"></a>

In the `SELECT` list, if there is an expression that does not have an explicit
alias, ZetaSQL assigns an implicit alias according to the following
rules. There can be multiple columns with the same alias in the `SELECT` list.

+  For identifiers, the alias is the identifier. For example, `SELECT abc`
   implies `AS abc`.
+  For path expressions, the alias is the last identifier in the path. For
   example, `SELECT abc.def.ghi` implies `AS ghi`.
+  For field access using the "dot" member field access operator, the alias is
   the field name. For example, `SELECT (struct_function()).fname` implies `AS
   fname`.

In all other cases, there is no implicit alias, so the column is anonymous and
cannot be referenced by name. The data from that column will still be returned
and the displayed query results may have a generated label for that column, but
the label cannot be used like an alias.

In a `FROM` clause, `from_item`s are not required to have an alias. The
following rules apply:

+  If there is an expression that does not have an explicit alias,
   ZetaSQL assigns an implicit alias in these cases:
   +  For identifiers, the alias is the identifier. For example, `FROM abc`
         implies `AS abc`.
   +  For path expressions, the alias is the last identifier in the path. For
      example, `FROM abc.def.ghi` implies `AS ghi`
   +  The column produced using `WITH OFFSET` has the implicit
      alias `offset`.
+  Table subqueries do not have implicit aliases.
+  `FROM UNNEST(x)` does not have an implicit alias.

### Range variables 
<a id="range_variables"></a>

In ZetaSQL, a range variable is a table expression alias in the
`FROM` clause. Sometimes a range variable is known as a `table alias`. A
range variable lets you reference rows being scanned from a table expression.
A table expression represents an item in the `FROM` clause that returns a table.
Common items that this expression can represent include
tables, [value tables][query-value-tables], [subqueries][subquery-concepts],
[table value functions (TVFs)][tvf-concepts], [joins][query-joins], and [parenthesized joins][query-joins].

In general, a range variable provides a reference to the rows of a table
expression. A range variable can be used to qualify a column reference and
unambiguously identify the related table, for example `range_variable.column_1`.

When referencing a range variable on its own without a specified column suffix,
the result of a table expression is the row type of the related table.
Value tables have explicit row types, so for range variables related
to value tables, the result type is the value table's row type. Other tables
do not have explicit row types, and for those tables, the range variable
type is a dynamically defined `STRUCT` that includes all of the
columns in the table.

**Examples**

In these examples, the `WITH` clause is used to emulate a temporary table
called `Grid`. This table has columns `x` and `y`. A range variable called
`Coordinate` refers to the current row as the table is scanned. `Coordinate`
can be used to access the entire row or columns in the row.

The following example selects column `x` from range variable `Coordinate`,
which in effect selects column `x` from table `Grid`.

```sql
WITH Grid AS (SELECT 1 x, 2 y)
SELECT Coordinate.x FROM Grid AS Coordinate;

+---+
| x |
+---+
| 1 |
+---+
```

The following example selects all columns from range variable `Coordinate`,
which in effect selects all columns from table `Grid`.

```sql
WITH Grid AS (SELECT 1 x, 2 y)
SELECT Coordinate.* FROM Grid AS Coordinate;

+---+---+
| x | y |
+---+---+
| 1 | 2 |
+---+---+
```

The following example selects the range variable `Coordinate`, which is a
reference to rows in table `Grid`.  Since `Grid` is not a value table,
the result type of `Coordinate` is a `STRUCT` that contains all the columns
from `Grid`.

```sql
WITH Grid AS (SELECT 1 x, 2 y)
SELECT Coordinate FROM Grid AS Coordinate;

+--------------+
| Coordinate   |
+--------------+
| {x: 1, y: 2} |
+--------------+
```

## Appendix A: examples with sample data 
<a id="appendix_a_examples_with_sample_data"></a>

These examples include statements which perform queries on the
[`Roster`][roster-table] and [`TeamMascot`][teammascot-table],
and [`PlayerStats`][playerstats-table] tables.

### GROUP BY clause 
<a id="group_by_clause"></a>

Example:

```sql
SELECT LastName, SUM(PointsScored)
FROM PlayerStats
GROUP BY LastName;
```

<table>
<thead>
<tr>
<th>LastName</th>
<th>SUM</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>7</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
</tbody>
</table>

### UNION 
<a id="union"></a>

The `UNION` operator combines the result sets of two or more `SELECT` statements
by pairing columns from the result set of each `SELECT` statement and vertically
concatenating them.

Example:

```sql
SELECT Mascot AS X, SchoolID AS Y
FROM TeamMascot
UNION ALL
SELECT LastName, PointsScored
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>X</th>
<th>Y</th>
</tr>
</thead>
<tbody>
<tr>
<td>Jaguars</td>
<td>50</td>
</tr>
<tr>
<td>Knights</td>
<td>51</td>
</tr>
<tr>
<td>Lakers</td>
<td>52</td>
</tr>
<tr>
<td>Mustangs</td>
<td>53</td>
</tr>
<tr>
<td>Adams</td>
<td>3</td>
</tr>
<tr>
<td>Buchanan</td>
<td>0</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
<tr>
<td>Adams</td>
<td>4</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
</tbody>
</table>

### INTERSECT 
<a id="intersect"></a>

This query returns the last names that are present in both Roster and
PlayerStats.

```sql
SELECT LastName
FROM Roster
INTERSECT ALL
SELECT LastName
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
</tr>
<tr>
<td>Coolidge</td>
</tr>
<tr>
<td>Buchanan</td>
</tr>
</tbody>
</table>

### EXCEPT 
<a id="except"></a>

The query below returns last names in Roster that are **not** present in
PlayerStats.

```sql
SELECT LastName
FROM Roster
EXCEPT DISTINCT
SELECT LastName
FROM PlayerStats;
```

Results:

<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Eisenhower</td>
</tr>
<tr>
<td>Davis</td>
</tr>
</tbody>
</table>

Reversing the order of the `SELECT` statements will return last names in
PlayerStats that are **not** present in Roster:

```sql
SELECT LastName
FROM PlayerStats
EXCEPT DISTINCT
SELECT LastName
FROM Roster;
```

Results:

```sql
(empty)
```

[language-territory-information]: http://www.unicode.org/cldr/charts/latest/supplemental/language_territory_information.html
[tr35-collation-settings]: http://www.unicode.org/reports/tr35/tr35-collation.html#Setting_Options

[implicit-aliases]: #implicit_aliases
[using-aliases]: #using_aliases
[sequences-of-joins]: #sequences_of_joins
[set-operators]: #set_operators
[union-syntax]: #union
[join-hints]: #join_hints
[query-value-tables]: #value_tables
[roster-table]: #roster_table
[playerstats-table]: #playerstats_table
[teammascot-table]: #teammascot_table
[stratefied-sampling]: #stratefied_sampling
[scaling-weight]: #scaling_weight
[query-joins]: #join_types
[ambiguous-aliases]: #ambiguous_aliases
[with_clause]: #with_clause
[analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[query-window-specification]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#def_window_spec
[named-window-example]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#def_use_named_window
[produce-table]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#produce-table
[tvf-concepts]: https://github.com/google/zetasql/blob/master/docs/user-defined-functions.md#tvfs
[flattening-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays#flattening_arrays
[working-with-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays
[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types#data-type-properties
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating-point-semantics
[subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries
[table-subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries#table_subquery_concepts
[expression-subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries#expression_subquery_concepts

[in-operator]: https://github.com/google/zetasql/blob/master/docs/operators#in_operators
[expression-subqueries]: https://github.com/google/zetasql/blob/master/docs/expression_subqueries

