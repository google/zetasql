

# Query syntax

Query statements scan one or more tables or expressions and return the computed
result rows. This topic describes the syntax for SQL queries in
ZetaSQL.

## SQL syntax

<pre class="lang-sql prettyprint">
<span class="var">query_statement</span>:
    <span class="var">query_expr</span>

<span class="var">query_expr</span>:
    [ <a href="#with_clause">WITH</a> [ <a href="#recursive_keyword">RECURSIVE</a> ] { <a href="#simple_cte"><span class="var">non_recursive_cte</span></a> | <a href="#recursive_cte"><span class="var">recursive_cte</span></a> }[, ...] ]
    { <span class="var">select</span> | ( <span class="var">query_expr</span> ) | <a href="#set_clause"><span class="var">set_operation</span></a> }
    [ <a href="#order_by_clause">ORDER</a> BY <span class="var">expression</span> [{ ASC | DESC }] [, ...] ]
    [ <a href="#limit_and_offset_clause">LIMIT</a> <span class="var">count</span> [ OFFSET <span class="var">skip_rows</span> ] ]

<span class="var">select</span>:
    <a href="#select_list">SELECT</a> [ AS { <span class="var"><a href="https://github.com/google/zetasql/blob/master/docs/protocol-buffers#select_as_typename">typename</a></span> | <a href="#select_as_struct">STRUCT</a> | <a href="#select_as_value">VALUE</a> } ] [{ ALL | DISTINCT }]
        { [ <span class="var">expression</span>. ]* [ <a href="#select_except">EXCEPT</a> ( <span class="var">column_name</span> [, ...] ) ]<br>            [ <a href="#select_replace">REPLACE</a> ( <span class="var">expression</span> [ AS ] <span class="var">column_name</span> [, ...] ) ]<br>        | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
        [ <a href="#anon_clause">WITH ANONYMIZATION</a> OPTIONS( privacy_parameters ) ]
    [ <a href="#from_clause">FROM</a> <a href="#from_clause"><span class="var">from_clause</span></a>[, ...] ]
    [ <a href="#where_clause">WHERE</a> <span class="var">bool_expression</span> ]
    [ <a href="#group_by_clause">GROUP</a> BY { <span class="var">expression</span> [, ...] | ROLLUP ( <span class="var">expression</span> [, ...] ) } ]
    [ <a href="#having_clause">HAVING</a> <span class="var">bool_expression</span> ]
    [ <a href="#qualify_clause">QUALIFY</a> <span class="var">bool_expression</span> ]
    [ <a href="#window_clause">WINDOW</a> <a href="#window_clause"><span class="var">window_clause</span></a> ]

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
| Adams      | 52         | 4            |
| Buchanan   | 50         | 13           |
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

### Modifiers for * operator

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

### Duplicate row handling

You can modify the results returned from a `SELECT` query, as follows.

#### SELECT DISTINCT

A `SELECT DISTINCT` statement discards duplicate rows and returns only the
remaining rows. `SELECT DISTINCT` cannot return columns of the following types:

+  `PROTO`

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
SELECT AS STRUCT expr [[AS] struct_field_name1] [,...]
```

This produces a value table with a STRUCT row type,
where the STRUCT field names and types match the
column names and types produced in the `SELECT` list.

Example:

```sql
SELECT ARRAY(SELECT AS STRUCT 1 a, 2 b)
```

`SELECT AS STRUCT` can be used in a scalar or array subquery to produce a single
STRUCT type grouping multiple values together. Scalar
and array subqueries (see [Subqueries][subquery-concepts]) are normally not
allowed to return multiple columns, but can return a single column with
STRUCT type.

Anonymous columns are allowed.

Example:

```sql
SELECT AS STRUCT 1 x, 2, 3
```

The query above produces STRUCT values of type
`STRUCT<int64 x, int64, int64>.` The first field has the name `x` while the
second and third fields are anonymous.

The example above produces the same result as this `SELECT AS VALUE` query using
a struct constructor:

```sql
SELECT AS VALUE STRUCT(1 AS x, 2, 3)
```

Duplicate columns are allowed.

Example:

```sql
SELECT AS STRUCT 1 x, 2 y, 3 x
```

The query above produces STRUCT values of type
`STRUCT<int64 x, int64 y, int64 x>.` The first and third fields have the same
name `x` while the second field has the name `y`.

The example above produces the same result as this `SELECT AS VALUE` query
using a struct constructor:

```sql
SELECT AS VALUE STRUCT(1 AS x, 2 AS y, 3 AS x)
```

#### SELECT AS VALUE

`SELECT AS VALUE` produces a value table from any `SELECT` list that
produces exactly one column. Instead of producing an output table with one
column, possibly with a name, the output will be a value table where the row
type is just the value type that was produced in the one `SELECT` column.  Any
alias the column had will be discarded in the value table.

Example:

```sql
SELECT AS VALUE 1
```

The query above produces a table with row type INT64.

Example:

```sql
SELECT AS VALUE STRUCT(1 AS a, 2 AS b) xyz
```

The query above produces a table with row type `STRUCT<a int64, b int64>`.

Example:

```sql
SELECT AS VALUE v FROM (SELECT AS STRUCT 1 a, true b) v WHERE v.b
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
FROM <span class="var">from_clause</span>[, ...]

<span class="var">from_clause</span>:
    <span class="var">from_item</span>
    [ <span class="var">unpivot_operator</span> ]
    [ <a href="#tablesample_operator"><span class="var">tablesample_operator</span></a> ]

<span class="var">from_item</span>:
    {
      <span class="var">table_name</span> [ <span class="var">as_alias</span> ]
      | { <a href="#join_types"><span class="var">join_operation</span></a> | ( <a href="#join_types"><span class="var">join_operation</span></a> ) }
      | ( <span class="var">query_expr</span> ) [ <span class="var">as_alias</span> ]
      | <span class="var">field_path</span>
      | <a href="#unnest_operator"><span class="var">unnest_operator</span></a>
      | <span class="var"><a href="#cte_name">cte_name</a></span> [ <span class="var">as_alias</span> ]
    }

<span class="var">as_alias</span>:
    [ AS ] <span class="var">alias</span>
</pre>

The `FROM` clause indicates the table or tables from which to retrieve rows,
and specifies how to join those rows together to produce a single stream of
rows for processing in the rest of the query.

#### unpivot_operator 
<a id="unpivot_operator_stub"></a>

See [UNPIVOT operator][unpivot-operator].

#### tablesample_operator 
<a id="tablesample_operator_clause"></a>

See [TABLESAMPLE operator][tablesample-operator].

#### table_name

The name (optionally qualified) of an existing table.

<pre>
SELECT * FROM Roster;
SELECT * FROM db.Roster;
</pre>

#### join_operation

See [JOIN operation][query-joins].

#### query_expr

`( query_expr ) [ [ AS ] alias ]` is a [table subquery][table-subquery-concepts].

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

#### unnest_operator 
<a id="unnest_operator_clause"></a>

See [UNNEST operator][unnest-operator].

#### cte_name

Common table expressions (CTEs) in a [`WITH` Clause][with-clause] act like
temporary tables that you can reference anywhere in the `FROM` clause.
In the example below, `subQ1` and `subQ2` are CTEs.

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

## UNNEST operator 
<a id="unnest_operator"></a>

<pre>
<span class="var">unnest_operator</span>:
    {
      <a href="#unnest">UNNEST</a>( <span class="var">array_expression</span> )
      | UNNEST( <span class="var">array_path</span> )
      | <span class="var">array_path</span>
    }
    [ <span class="var">as_alias</span> ]
    [ WITH OFFSET [ <span class="var">as_alias</span> ] ]
</pre>

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
return a second column with the array element indexes.

For several ways to use `UNNEST`, including construction, flattening, and
filtering, see [`Working with arrays`][working-with-arrays].

### UNNEST and STRUCTs
For an input `ARRAY` of `STRUCT`s, `UNNEST`
returns a row for each `STRUCT`, with a separate column for each field in the
`STRUCT`. The alias for each column is the name of the corresponding `STRUCT`
field.

Example:

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

### UNNEST and PROTOs
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

### Explicit and implicit UNNEST

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

### UNNEST and FLATTEN

The `UNNEST` operator can accept an array with nested data and flatten a
specific part of the data into a table with one row for each element. To do
this, use the [array elements field access operator][array-el-field-operator]
as the argument for `array_path` in the `UNNEST` operation.

When you use the array elements field access operator with `UNNEST`, the
[`FLATTEN` operator][flatten-operator] is used implicitly.

These are equivalent:

```sql
-- In UNNEST (FLATTEN used explicitly):
SELECT * FROM T, UNNEST(FLATTEN(T.x.y.z));

-- In UNNEST (FLATTEN used implicitly):
SELECT * FROM T, UNNEST(T.x.y.z);

-- In the FROM clause (UNNEST used implicitly):
SELECT * FROM T, T.x.y.z;
```

You can use `UNNEST` with the array elements field access operator implicitly
in the `FROM` clause, but only if the
[array subscript operator][array-subscript-operator] is not included.

This works:

```sql
SELECT * FROM T, UNNEST(T.x.y.z[SAFE_OFFSET(1)]);
```

This produces an error:

```sql
SELECT * FROM T, T.x.y.z[SAFE_OFFSET(1)];
```

When you use the array elements field access operator with explicit `UNNEST`,
you can optionally prepend it with a table name.

These work:

```sql
SELECT * FROM T, UNNEST(x.y.z);
```

```sql
SELECT * FROM T, UNNEST(T.x.y.z);
```

With implicit `UNNEST`, the table name is also optional.

These work:

```sql
SELECT * FROM T, T.x.y.z;
```

```sql
SELECT * FROM T, x.y.z;
```

To learn more about the ways you can use `UNNEST` explicitly and implicitly
to flatten nested data into a table, see
[Flattening nested data into a table][flattening-trees-into-table].

### UNNEST and NULLs

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

## UNPIVOT operator 
<a id="unpivot_operator"></a>

<pre class="lang-sql prettyprint">
FROM <span class="var">from_item</span>[, ...] <span class="var">unpivot_operator</span>

<span class="var">unpivot_operator</span>:
    UNPIVOT [ { INCLUDE NULLS | EXCLUDE NULLS } ] (
        { <span class="var">single_column_unpivot</span> | <span class="var">multi_column_unpivot</span> }
    ) [<span class="var">unpivot_alias</span>]

<span class="var">single_column_unpivot</span>:
    <span class="var">values_column</span>
    FOR <span class="var">name_column</span>
    IN (<span class="var">columns_to_unpivot</span>)

<span class="var">multi_column_unpivot</span>:
    <span class="var">values_column_set</span>
    FOR <span class="var">name_column</span>
    IN (<span class="var">column_sets_to_unpivot</span>)

<span class="var">values_column_set</span>:
    (<span class="var">values_column</span>[, ...])

<span class="var">columns_to_unpivot</span>:
    <span class="var">unpivot_column</span> [<span class="var">row_value_alias</span>][, ...]

<span class="var">column_sets_to_unpivot</span>:
    (<span class="var">unpivot_column</span> [<span class="var">row_value_alias</span>][, ...])

<span class="var">unpivot_alias</span> and <span class="var">row_value_alias</span>:
    [AS] <span class="var">alias</span>
</pre>

The `UNPIVOT` operator rotates columns into rows. `UNPIVOT` is part of the
`FROM` clause.

+ `UNPIVOT` can be used to modify any table
  expression.
+ A `WITH OFFSET` clause immediately preceding the `UNPIVOT` operator is not
  allowed.

Conceptual example:

```sql
-- Before UNPIVOT is used to rotate Q1, Q2, Q3, Q4 into sales and quarter columns:
+---------+----+----+----+----+
| product | Q1 | Q2 | Q3 | Q4 |
+---------+----+----+----+----+
| Kale    | 51 | 23 | 45 | 3  |
| Apple   | 77 | 0  | 25 | 2  |
+---------+----+----+----+----+

-- After UNPIVOT is used to rotate Q1, Q2, Q3, Q4 into sales and quarter columns:
+---------+-------+---------+
| product | sales | quarter |
+---------+-------+---------+
| Kale    | 51    | Q1      |
| Kale    | 23    | Q2      |
| Kale    | 45    | Q3      |
| Kale    | 3     | Q4      |
| Apple   | 77    | Q1      |
| Apple   | 0     | Q2      |
| Apple   | 25    | Q3      |
| Apple   | 2     | Q4      |
+---------+-------+---------+
```

**Definitions**

Top-level definitions:

+ `from_item`: The table, subquery, or
  table-valued function (TVF) on which
  to perform a pivot operation. The `from_item` must
  [follow these rules](#rules_for_unpivot_from_item).
+ `unpivot_operator`: The pivot operation to perform on a `from_item`.

`unpivot_operator` definitions:

+ `INCLUDE NULLS`: Add rows with `NULL` values to the result.
+ `EXCLUDE NULLS`: Do not add rows with `NULL` values to the result.
  By default, `UNPIVOT` excludes rows with `NULL` values.
+ `single_column_unpivot`: Rotates columns into one `values_column`
  and one `name_column`.
+ `multi_column_unpivot`: Rotates columns into multiple
  `values_column`s and one `name_column`.
+ `unpivot_alias`: An alias for the results of the `UNPIVOT` operation. This
  alias can be referenced elsewhere in the query.

`single_column_unpivot` definitions:

+ `values_column`: A column to contain the row values from `columns_to_unpivot`.
  [Follow these rules](#rules_for_values_column) when creating a values column.
+ `name_column`: A column to contain the column names from `columns_to_unpivot`.
  [Follow these rules](#rules_for_name_column) when creating a name column.
+ `columns_to_unpivot`: The columns from the `from_item` to populate
  `values_column` and `name_column`.
  [Follow these rules](#rules_for_unpivot_column) when creating an unpivot
  column.
  + `row_value_alias`: An optional alias for a column that is displayed for the
    column in `name_column`. If not specified, the string value of the
    column name is used.
    [Follow these rules](#rules_for_row_value_alias) when creating a
    row value alias.

`multi_column_unpivot` definitions:

+ `values_column_set`: A set of columns to contain the row values from
  `columns_to_unpivot`. [Follow these rules](#rules_for_values_column) when
   creating a values column.
+ `name_column`: A set of columns to contain the column names from
  `columns_to_unpivot`. [Follow these rules](#rules_for_name_column) when
  creating a name column.
+ `column_sets_to_unpivot`: The columns from the `from_item` to unpivot.
  [Follow these rules](#rules_for_unpivot_column) when creating an unpivot
  column.
  + `row_value_alias`: An optional alias for a column set that is displayed
    for the column set in `name_column`. If not specified, a string value for
    the column set is used and each column in the string is separated with an
    underscore (`_`). For example, `(col1, col2)` outputs `col1_col2`.
    [Follow these rules](#rules_for_row_value_alias) when creating a
    row value alias.

**Rules**

<a id="rules_for_unpivot_from_item"></a>
Rules for a `from_item` passed to `UNPIVOT`:

+ The `from_item` may consist of any
  table, subquery, or table-valued function
  (TVF) result.
+ The `from_item` may not produce a value table.
+ Duplicate columns in a `from_item` cannot be referenced in the `UNPIVOT`
  clause.

<a id="rules_for_unpivot_operator"></a>
Rules for `unpivot_operator`:

+ Expressions are not permitted.
+ Qualified names are not permitted. For example, `mytable.mycolumn` is not
  allowed.
+ In the case where the `UNPIVOT` result has duplicate column names:
    + `SELECT *` is allowed.
    + `SELECT values_column` causes ambiguity.

<a id="rules_for_values_column"></a>
Rules for `values_column`:

+ It cannot be a name used for a `name_column` or an `unpivot_column`.
+ It can be the same name as a column from the `from_item`.

<a id="rules_for_name_column"></a>
Rules for `name_column`:

+ It cannot be a name used for a `values_column` or an `unpivot_column`.
+ It can be the same name as a column from the `from_item`.

<a id="rules_for_unpivot_column"></a>
Rules for `unpivot_column`:

+ Must be a column name from the `from_item`.
+ It cannot reference duplicate `from_item` column names.
+ All columns in a column set must have equivalent data types.
  + Data types cannot be coerced to a common supertype.
  + If the data types are exact matches (for example, a struct with
    different field names), the data type of the first input is
    the data type of the output.
+ You cannot have the same name in the same column set. For example,
  `(emp1, emp1)` results in an error.
+ You can have a the same name in different column sets. For example,
  `(emp1, emp2), (emp1, emp3)` is valid.

<a id="rules_for_row_value_alias"></a>
Rules for `row_value_alias`:

+ This can be a `STRING` or an `INT64` literal.
+ The data type for all `row_value_alias` clauses must be the same.
+ If the value is an `INT64`, the `row_value_alias` for each `unpivot_column`
  must be specified.

**Examples**

The following examples reference a table called `Produce` that looks like this:

```sql
WITH Produce AS (
  SELECT 'Kale' as product, 51 as Q1, 23 as Q2, 45 as Q3, 3 as Q4 UNION ALL
  SELECT 'Apple', 77, 0, 25, 2)
SELECT * FROM Produce

+---------+----+----+----+----+
| product | Q1 | Q2 | Q3 | Q4 |
+---------+----+----+----+----+
| Kale    | 51 | 23 | 45 | 3  |
| Apple   | 77 | 0  | 25 | 2  |
+---------+----+----+----+----+
```

With the `UNPIVOT` operator, the columns `Q1`, `Q2`, `Q3`, and `Q4` are
rotated. The values of these columns now populate a new column called `Sales`
and the names of these columns now populate a new column called `Quarter`.
This is a single-column unpivot operation.

```sql
SELECT * FROM Produce
UNPIVOT(sales FOR quarter IN (Q1, Q2, Q3, Q4))

+---------+-------+---------+
| product | sales | quarter |
+---------+-------+---------+
| Kale    | 51    | Q1      |
| Kale    | 23    | Q2      |
| Kale    | 45    | Q3      |
| Kale    | 3     | Q4      |
| Apple   | 77    | Q1      |
| Apple   | 0     | Q2      |
| Apple   | 25    | Q3      |
| Apple   | 2     | Q4      |
+---------+-------+---------+
```

In this example, we `UNPIVOT` four quarters into two semesters.
This is a multi-column unpivot operation.

```sql
SELECT * FROM Produce
UNPIVOT(
  (first_half_sales, second_half_sales)
  FOR semesters
  IN ((Q1, Q2) AS 'semester_1', (Q3, Q4) AS 'semester_2'))

+---------+------------------+-------------------+------------+
| product | first_half_sales | second_half_sales | semesters  |
+---------+------------------+-------------------+------------+
| Kale    | 51               | 23                | semester_1 |
| Kale    | 45               | 3                 | semester_2 |
| Apple   | 77               | 0                 | semester_1 |
| Apple   | 25               | 2                 | semester_2 |
+---------+------------------+-------------------+------------+
```

## TABLESAMPLE operator

```sql
tablesample_clause:
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

You can use the `TABLESAMPLE` operator to select a random sample of a dataset.
This operator is useful when you're working with tables that have large
amounts of data and you don't need precise answers.

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
+  `partition_by`: Optional. Perform [stratified sampling][stratified-sampling]
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
   specified, the `TABLESAMPLE` operator outputs one extra column of type
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

Group results by country, using stratified sampling:

```sql
SELECT country, SUM(click_cost) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 GROUP BY country;
```

Add scaling weight to stratified sampling:

```sql
SELECT country, SUM(click_cost * sampling_weight) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 WITH WEIGHT AS sampling_weight
 GROUP BY country;
```

This is equivalent to the previous example. Note that you don't have to use
an alias after `WITH WEIGHT`. If you don't, the default alias `weight` is used.

```sql
SELECT country, SUM(click_cost * weight) FROM ClickEvents
 TABLESAMPLE RESERVOIR (100 ROWS PARTITION BY country)
 WITH WEIGHT
 GROUP BY country;
```

### Stratified sampling 
<a id="stratified_sampling"></a>

If you want better quality generated samples for under-represented groups,
you can use stratified sampling. Stratified sampling helps you
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

### Scaling weight 
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
can be solved with [stratified sampling][stratified-sampling].

## JOIN operation 
<a id="join_types"></a>

<pre class="lang-sql prettyprint">
<span class="var">join_operation</span>:
    { <span class="var">cross_join_operation</span> | <span class="var">condition_join_operation</span> }

<span class="var">cross_join_operation</span>:
    <span class="var"><a href="#from_clause">from_item</a></span> <span class="var">cross_join_operator</span> <span class="var"><a href="#from_clause">from_item</a></span>

<span class="var">condition_join_operation</span>:
    <span class="var"><a href="#from_clause">from_item</a></span> <span class="var">condition_join_operator</span> <span class="var"><a href="#from_clause">from_item</a></span> <span class="var">join_condition</span>

<span class="var">cross_join_operator</span>:
    { <a href="#cross_join">CROSS JOIN</a> | <a href="#comma_cross_join">,</a> }

<span class="var">condition_join_operator</span>:
    {
      <a href="#inner_join">[INNER] JOIN</a>
      | <a href="#full_outer_join">FULL [OUTER] JOIN</a>
      | <a href="#left_outer_join">LEFT [OUTER] JOIN</a>
      | <a href="#right_outer_join">RIGHT [OUTER] JOIN</a>
    }

<span class="var">join_condition</span>:
    { <a href="#on_clause"><span class="var">on_clause</span></a> | <a href="#using_clause"><span class="var">using_clause</span></a> }

<span class="var">on_clause</span>:
    ON <span class="var">bool_expression</span>

<span class="var">using_clause</span>:
    USING ( <span class="var">join_column</span> [, ...] )
</pre>

The `JOIN` operation merges two `from_item`s so that the `SELECT` clause can
query them as one source. The `join_type` and `ON` or `USING` clause (a
"join condition") specify how to combine and discard rows from the two
`from_item`s to form a single source.

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

In a `FROM` clause, a `CROSS JOIN` can be written like this:

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

You can use a [correlated][correlated-join] cross join to convert or
flatten an `ARRAY` into a set of rows. To learn more, see
[Convert elements in an array to rows in a table][flattening-arrays].

**Examples**

This query performs an `CROSS JOIN` on the [`Roster`][roster-table]
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

### Comma cross join (,) 
<a id="comma_cross_join"></a>

[`CROSS JOIN`][cross-join]s can be written implicitly with a comma. This is
called a comma cross join.

A comma cross join looks like this in a `FROM` clause:

```sql
FROM A, B

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

You cannot write comma cross joins inside parentheses. To learn more, see
[Join operations in a sequence][sequences-of-joins].

```sql {.bad}
FROM (A, B)  // INVALID
```

You can use a [correlated][correlated-join] comma cross join to convert or
flatten an `ARRAY` into a set of rows. To learn more, see
[Convert elements in an array to rows in a table][flattening-arrays].

**Examples**

This query performs a comma cross join on the [`Roster`][roster-table]
and [`TeamMascot`][teammascot-table] tables.

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster, TeamMascot;

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
`JOIN` operation, even if no rows in the right `from_item` satisfy the join
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

A combined row (the result of joining two rows) meets the `ON` join condition
if join condition returns `TRUE`.

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

The `USING` clause requires a column list of one or more columns which
occur in both input tables. It performs an equality comparison on that column,
and the rows meet the join condition if the equality comparison returns `TRUE`.

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
`Roster.SchoolID` is the same as `TeamMascot.SchoolID`. The results include a
single `SchoolID` column.

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

### Join operations in a sequence 
<a id="sequences_of_joins"></a>

The `FROM` clause can contain multiple `JOIN` operations in a sequence.
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

A `FROM` clause can have multiple joins. Provided there are no comma cross joins
in the `FROM` clause, joins do not require parenthesis, though parenthesis can
help readability:

```sql
FROM A JOIN B JOIN C JOIN D USING (w) ON B.x = C.y ON A.z = B.x
```

If your clause contains comma cross joins, you must use parentheses:

```sql {.bad}
FROM A, B JOIN C JOIN D ON C.x = D.y ON B.z = C.x    // INVALID
```

```sql
FROM A, B JOIN (C JOIN D ON C.x = D.y) ON B.z = C.x  // VALID
```

When comma cross joins are present in a query with a sequence of JOINs, they
group from left to right like other `JOIN` types:

```sql
FROM A JOIN B USING (x) JOIN C USING (x), D

-- A JOIN B USING (x)        = result_1
-- result_1 JOIN C USING (x) = result_2
-- result_2 CROSS JOIN D     = return value
```

There cannot be a `RIGHT JOIN` or `FULL JOIN` after a comma cross join unless it
is parenthsized:

```sql {.bad}
FROM A, B RIGHT JOIN C ON TRUE // INVALID
```

```sql {.bad}
FROM A, B FULL JOIN C ON TRUE  // INVALID
```

```sql
FROM A, B JOIN C ON TRUE       // VALID
```

```sql
FROM A, (B RIGHT JOIN C ON TRUE) // VALID
```

```sql
FROM A, (B FULL JOIN C ON TRUE)  // VALID
```

### Correlated join operation 
<a id="correlated_join"></a>

A join operation is _correlated_ when the right `from_item` contains a
reference to at least one range variable or
column name introduced by the left `from_item`.

In a correlated join operation, rows from the right `from_item` are determined
by a row from the left `from_item`. Consequently, `RIGHT OUTER` and `FULL OUTER`
joins cannot be correlated because right `from_item` rows cannot be determined
in the case when there is no row from the left `from_item`.

All correlated join operations must reference an array in the right `from_item`.

This is a conceptual example of a correlated join operation that includes
a [correlated subquery][correlated-subquery]:

```sql
FROM A JOIN UNNEST(ARRAY(SELECT AS STRUCT * FROM B WHERE A.ID = B.ID)) AS C
```

+ Left `from_item`: `A`
+ Right `from_item`: `UNNEST(...) AS C`
+ A correlated subquery: `(SELECT AS STRUCT * FROM B WHERE A.ID = B.ID)`

This is another conceptual example of a correlated join operation.
`array_of_IDs` is part of the left `from_item` but is referenced in the
right `from_item`.

```sql
FROM A JOIN UNNEST(A.array_of_IDs) AS C
```

The [`UNNEST` operator][unnest-operator] can be explicit or implicit.
These are both allowed:

```sql
FROM A JOIN UNNEST(A.array_of_IDs) AS IDs
```

```sql
FROM A JOIN A.array_of_IDs AS IDs
```

In a correlated join operation, the right `from_item` is re-evaluated
against each distinct row from the left `from_item`. In the following
conceptual example, the correlated join operation first
evaluates `A` and `B`, then `A` and `C`:

```sql
FROM
  A
  JOIN
  UNNEST(ARRAY(SELECT AS STRUCT * FROM B WHERE A.ID = B.ID)) AS C
  ON A.Name = C.Name
```

**Caveats**

+   In a correlated `LEFT JOIN`, when the input table on the right side is empty
    for some row from the left side, it is as if no rows from the right side
    satisfied the join condition in a regular `LEFT JOIN`. When there are no
    joining rows, a row with `NULL` values for all columns on the right side is
    generated to join with the row from the left side.
+   In a correlated `CROSS JOIN`, when the input table on the right side is
    empty for some row from the left side, it is as if no rows from the right
    side satisfied the join condition in a regular correlated `INNER JOIN`. This
    means that the row is dropped from the results.

**Examples**

This is an example of a correlated join, using the
[Roster][roster-table] and [PlayerStats][playerstats-table] tables:

```sql
SELECT *
FROM
  Roster
JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT *
      FROM PlayerStats
      WHERE PlayerStats.OpponentID = Roster.SchoolID
    )) AS PlayerMatches
  ON PlayerMatches.LastName = 'Buchanan'

+------------+----------+----------+------------+--------------+
| LastName   | SchoolID | LastName | OpponentID | PointsScored |
+------------+----------+----------+------------+--------------+
| Adams      | 50       | Buchanan | 50         | 13           |
| Eisenhower | 77       | Buchanan | 77         | 0            |
+------------+----------+----------+------------+--------------+
```

A common pattern for a correlated `LEFT JOIN` is to have an `UNNEST` operation
on the right side that references an array from some column introduced by
input on the left side. For rows where that array is empty or `NULL`,
the `UNNEST` operation produces no rows on the right input. In that case, a row
with a `NULL` entry in each column of the right input is created to join with
the row from the left input. For example:

```sql
SELECT A.name, item, ARRAY_LENGTH(A.items) item_count_for_name
FROM
  UNNEST(
    [
      STRUCT(
        'first' AS name,
        [1, 2, 3, 4] AS items),
      STRUCT(
        'second' AS name,
        [] AS items)]) AS A
LEFT JOIN
  A.items AS item;

+--------+------+---------------------+
| name   | item | item_count_for_name |
+--------+------+---------------------+
| first  | 1    | 4                   |
| first  | 2    | 4                   |
| first  | 3    | 4                   |
| first  | 4    | 4                   |
| second | NULL | 0                   |
+--------+------+---------------------+
```

In the case of a correlated `CROSS JOIN`, when the input on the right side
is empty for some row from the left side, the final row is dropped from the
results. For example:

```sql
SELECT A.name, item
FROM
  UNNEST(
    [
      STRUCT(
        'first' AS name,
        [1, 2, 3, 4] AS items),
      STRUCT(
        'second' AS name,
        [] AS items)]) AS A
CROSS JOIN
  A.items AS item;

+-------+------+
| name  | item |
+-------+------+
| first | 1    |
| first | 2    |
| first | 3    |
| first | 4    |
+-------+------+
```

## WHERE clause 
<a id="where_clause"></a>

```sql
WHERE bool_expression
```

The `WHERE` clause filters the results of the `FROM` clause.

Only rows whose `bool_expression` evaluates to `TRUE` are included. Rows
whose `bool_expression` evaluates to `NULL` or `FALSE` are
discarded.

The evaluation of a query with a `WHERE` clause is typically completed in this
order:

+ `FROM`
+ `WHERE`
+ `GROUP BY` and aggregation
+ `HAVING`
+ `WINDOW`
+ `QUALIFY`
+ `DISTINCT`
+ `ORDER BY`
+ `LIMIT`

The `WHERE` clause can only reference columns available via the `FROM` clause;
it cannot reference `SELECT` list aliases.

**Examples**

This query returns returns all rows from the [`Roster`][roster-table] table
where the `SchoolID` column has the value `52`:

```sql
SELECT * FROM Roster
WHERE SchoolID = 52;
```

The `bool_expression` can contain multiple sub-conditions:

```sql
SELECT * FROM Roster
WHERE STARTS_WITH(LastName, "Mc") OR STARTS_WITH(LastName, "Mac");
```

Expressions in an `INNER JOIN` have an equivalent expression in the
`WHERE` clause. For example, a query using `INNER` `JOIN` and `ON` has an
equivalent expression using `CROSS JOIN` and `WHERE`. For example,
the following two queries are equivalent:

```sql
SELECT Roster.LastName, TeamMascot.Mascot
FROM Roster INNER JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

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
redundancy in the output. The data type of `expression` must be
[groupable][data-type-properties].

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

The `HAVING` clause filters the results produced by `GROUP BY` or
aggregation. `GROUP BY` or aggregation must be present in the query. If
aggregation is present, the `HAVING` clause is evaluated once for every
aggregated row in the result set.

Only rows whose `bool_expression` evaluates to `TRUE` are included. Rows
whose `bool_expression` evaluates to `NULL` or `FALSE` are
discarded.

The evaluation of a query with a `HAVING` clause is typically completed in this
order:

+ `FROM`
+ `WHERE`
+ `GROUP BY` and aggregation
+ `HAVING`
+ `WINDOW`
+ `QUALIFY`
+ `DISTINCT`
+ `ORDER BY`
+ `LIMIT`

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

<a id="collate_clause"></a>
## ORDER BY clause 
<a id="order_by_clause"></a>

<pre>
ORDER BY expression
  [COLLATE collation_specification]
  [{ ASC | DESC }]
  [{ NULLS FIRST | NULLS LAST }]
  [, ...]

collation_specification:
  language_tag[:collation_attribute]
</pre>

The `ORDER BY` clause specifies a column or expression as the sort criterion for
the result set. If an ORDER BY clause is not present, the order of the results
of a query is not defined. Column aliases from a `FROM` clause or `SELECT` list
are allowed. If a query contains aliases in the `SELECT` clause, those aliases
override names in the corresponding `FROM` clause. The data type of
`expression` must be [orderable][orderable-data-types].

**Optional Clauses**

+ `COLLATE`: You can use the `COLLATE` clause to refine how data is ordered
  by an `ORDER BY` clause. _Collation_ refers to a set of rules that determine
  how strings are compared according to the conventions and
  standards of a particular written language, region, or country. These rules
  might define the correct character sequence, with options for specifying
  case-insensitivity. You can use `COLLATE` only on columns of type `STRING`.

  `collation_specification` represents the collation specification for the
  `COLLATE` clause. The collation specification can be a `STRING` literal or
   a query parameter. To learn more see
   [collation specification details][collation-spec].
+  `NULLS FIRST | NULLS LAST`:
    +  `NULLS FIRST`: Sort null values before non-null values.
    +  `NULLS LAST`. Sort null values after non-null values.
+  `ASC | DESC`: Sort the results in ascending or descending
    order of `expression` values. `ASC` is the
    default value. If null ordering is not specified
    with `NULLS FIRST` or `NULLS LAST`:
    +  `NULLS FIRST` is applied by default if the sort order is ascending.
    +  `NULLS LAST` is applied by default if the sort order is descending.
    

**Examples**

Use the default sort order (ascending).

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true UNION ALL
      SELECT NULL, false)
ORDER BY x;
+------+-------+
| x    | y     |
+------+-------+
| NULL | false |
| 1    | true  |
| 9    | true  |
+------+-------+
```

Use the default sort order (ascending), but return null values last.

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true UNION ALL
      SELECT NULL, false)
ORDER BY x NULLS LAST;
+------+-------+
| x    | y     |
+------+-------+
| 1    | true  |
| 9    | true  |
| NULL | false |
+------+-------+
```

Use descending sort order.

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true UNION ALL
      SELECT NULL, false)
ORDER BY x DESC;
+------+-------+
| x    | y     |
+------+-------+
| 9    | true  |
| 1    | true  |
| NULL | false |
+------+-------+
```

Use descending sort order, but return null values first.

```sql
SELECT x, y
FROM (SELECT 1 AS x, true AS y UNION ALL
      SELECT 9, true UNION ALL
      SELECT NULL, false)
ORDER BY x DESC NULLS FIRST;
+------+-------+
| x    | y     |
+------+-------+
| NULL | false |
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

When used in conjunction with
[set operators][set-operators],
the `ORDER BY` clause applies to the result set of the entire query; it does not
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
GROUP BY LastName
ORDER BY LastName;
```

```sql
SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY 2
ORDER BY 2;
```

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
ORDER BY Place COLLATE "und:ci"
```

## QUALIFY clause

```sql
QUALIFY bool_expression
```

The `QUALIFY` clause filters the results of analytic functions.
An analytic function is required to be present in the `QUALIFY` clause or the
`SELECT` list.

Only rows whose `bool_expression` evaluates to `TRUE` are included. Rows
whose `bool_expression` evaluates to `NULL` or `FALSE` are
discarded.

The evaluation of a query with a `QUALIFY` clause is typically completed in this
order:

+ `FROM`
+ `WHERE`
+ `GROUP BY` and aggregation
+ `HAVING`
+ `WINDOW`
+ `QUALIFY`
+ `DISTINCT`
+ `ORDER BY`
+ `LIMIT`

**Limitations**

The `QUALIFY` clause has an implementation limitation in that it must be used in
conjunction with at least one of these clauses:

+ `WHERE`
+ `GROUP BY`
+ `HAVING`

**Examples**

The following query returns the most popular vegetables in the
[`Produce`][produce-table] table and their rank.

```sql
SELECT
  item,
  RANK() OVER (PARTITION BY category ORDER BY purchases DESC) as rank
FROM Produce
WHERE Produce.category = 'vegetable'
QUALIFY rank <= 3

+---------+------+
| item    | rank |
+---------+------+
| kale    | 1    |
| lettuce | 2    |
| cabbage | 3    |
+---------+------+
```

You don't have to include an analytic function in the `SELECT` list to use
`QUALIFY`. The following query returns the most popular vegetables in the
[`Produce`][produce-table] table.

```sql
SELECT item
FROM Produce
WHERE Produce.category = 'vegetable'
QUALIFY RANK() OVER (PARTITION BY category ORDER BY purchases DESC) <= 3

+---------+
| item    |
+---------+
| kale    |
| lettuce |
| cabbage |
+---------+
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

<a id="set_clause"></a>

## Set operators 
<a id="set_operators"></a>

<pre>
<span class="var">set_operation</span>:
  <a href="#sql_syntax">query_expr</a> set_operator <a href="#sql_syntax">query_expr</a>

<span class="var">set_operator</span>:
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

<pre class="lang-sql prettyprint">
WITH [ <a href="#recursive_keyword">RECURSIVE</a> ] { <a href="#simple_cte"><span class="var">non_recursive_cte</span></a> | <a href="#recursive_cte"><span class="var">recursive_cte</span></a> }[, ...]
</pre>

A `WITH` clause contains one or more common table expressions (CTEs).
A CTE acts like a temporary table that you can reference within a single
query expression. Each CTE binds the results of a [subquery][subquery-concepts]
to a table name, which can be used elsewhere in the same query expression,
but [rules apply][cte-rules].

CTEs can be [non-recursive][non-recursive-cte] or
[recursive][recursive-cte] and you can include both of these in your
`WITH` clause. A recursive CTE references itself, where a
non-recursive CTE does not. If a recursive CTE is included in the `WITH` clause,
the `RECURSIVE` keyword must also be included.

You can include the `RECURSIVE` keyword in a `WITH` clause even if no
recursive CTEs are present. You can learn more about the `RECURSIVE` keyword
[here][recursive-keyword].

### RECURSIVE keyword 
<a id="recursive_keyword"></a>

A `WITH` clause can optionally include the `RECURSIVE` keyword, which does
two things:

+ Enables recursion in the `WITH` clause. If this keyword is not present,
  you can only include non-recursive common table expressions (CTEs).
  If this keyword is present, you can use both [recursive][recursive-cte] and
  [non-recursive][non-recursive-cte] CTEs.
+ [Changes the visibility][cte-visibility] of CTEs in the `WITH` clause. If this
  keyword is not present, a CTE is only visible to CTEs defined after it in the
  `WITH` clause. If this keyword is present, a CTE is visible to all CTEs in the
  `WITH` clause where it was created.

### Non-recursive CTEs 
<a id="simple_cte"></a>

<pre class="lang-sql prettyprint">
non_recursive_cte:
    <a href="#cte_name">cte_name</a> AS ( <a href="#sql_syntax">query_expr</a> )
</pre>

A non-recursive common table expression (CTE) contains
a non-recursive [subquery][subquery-concepts]
and a name associated with the CTE.

+ A non-recursive CTE cannot reference itself.
+ A non-recursive CTE can be referenced by the query expression that
  contains the `WITH` clause, but [rules apply][cte-rules].

##### Examples

In this example, a `WITH` clause defines two non-recursive CTEs that
are referenced in the related set operation, where one CTE is referenced by
each of the set operation's input query expressions:

```sql
WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2
```

You can break up more complex queries into a `WITH` clause and
`WITH` `SELECT` statement instead of writing nested table subqueries.
For example:

```sql
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1) SELECT * FROM q2)
```

```sql
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
        q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery on the previous line.
    SELECT * FROM q1)              # q1 resolves to the third inner WITH subquery.
```

### Recursive CTEs 
<a id="recursive_cte"></a>

<pre class="lang-sql prettyprint">
recursive_cte:
    <a href="#cte_name">cte_name</a> AS ( recursive_union_operation )

recursive_union_operation:
    base_term union_operator recursive_term

base_term:
    <a href="#sql_syntax">query_expr</a>

recursive_term:
    <a href="#sql_syntax">query_expr</a>

union_operator:
    { UNION ALL | UNION DISTINCT }
</pre>

A recursive common table expression (CTE) contains a
recursive [subquery][subquery-concepts] and a name associated with the CTE.

+ A recursive CTE references itself.
+ A recursive CTE can be referenced in the query expression that contains the
  `WITH` clause, but [rules apply][cte-rules].
+ When a recursive CTE is present in a WITH clause, the
  [`RECURSIVE`][recursive-keyword] keyword must be present.

A recursive CTE is defined by a _recursive union operation_. The
recursive union operation defines how input is recursively processed
to produce the final table result. The recursive union operation has the
following parts:

+ `base_term`: Runs the first iteration of the
  recursive union operation. This term must follow the
  [base term rules][base-term-rules].
+ `union_operator`: The `UNION` operator returns the rows that are from
  the union of the base term and recursive term. With `UNION ALL`,
  each row produced in iteration `N` becomes part of the final query output and
  input for iteration `N+1`. With
  `UNION DISTINCT`, only distinct rows become part of the final query output,
  and only new distinct rows move into iteration `N+1`. Iteration
  stops when an iteration produces no rows to move into the next iteration.
+ `recursive_term`: Runs the remaining iterations.
  It must include a self-reference to the recursive CTE. All recursive
  references must be in this term. This term
  must follow the [recursive term rules][recursive-cte-rules].

A recursive CTE looks like this:

```sql
WITH RECURSIVE
  T1 AS ( (SELECT 1 AS n) UNION ALL (SELECT n + 1 AS n FROM T1 WHERE n < 3) )
SELECT n FROM T1

+---+
| n |
+---+
| 1 |
| 2 |
| 3 |
+---+
```

The first iteration of a recursive union operation runs the base term.
Then, each subsequent iteration runs the recursive term and produces
_new rows_ which are unioned with the previous iteration. The recursive
union operation terminates when an recursive term iteration produces no new
rows.

If recursion does not terminate, the query will not terminate.

To avoid a non-terminating iteration in a recursive union operation, you can
use the `LIMIT` clause in a query.

A recursive CTE can include nested `WITH` clauses, however, you can't reference
`recursive_term` inside of an inner `WITH` clause. An inner `WITH`
clause can't be recursive unless it includes its own `RECURSIVE` keyword.
The `RECURSIVE` keyword affects only the particular `WITH` clause to which it
belongs.

##### Examples of allowed recursive CTEs

This is a simple recursive CTE:

```sql
WITH RECURSIVE
  T1 AS (
    (SELECT 1 AS n) UNION ALL
    (SELECT n + 2 FROM T1 WHERE n < 4))
SELECT * FROM T1

+---+
| n |
+---+
| 1 |
| 3 |
| 5 |
+---+
```

Multiple subqueries in the same recursive CTE are okay, as
long as each recursion has a cycle length of 1. It is also okay for recursive
entries to depend on non-recursive entries and vice-versa:

```sql
WITH RECURSIVE
  T0 AS (SELECT 1 AS n),
  T1 AS ((SELECT * FROM T0) UNION ALL (SELECT n + 1 FROM T1 WHERE n < 4)),
  T2 AS ((SELECT 1 AS n) UNION ALL (SELECT n + 1 FROM T2 WHERE n < 4)),
  T3 AS (SELECT * FROM T1 INNER JOIN T2 USING (n))
SELECT * FROM T3

+---+
| n |
+---+
| 1 |
| 2 |
| 3 |
| 4 |
+---+
```

Aggregate functions can be invoked in subqueries, as long as they are not
aggregating on the table being defined:

```sql
WITH RECURSIVE
  T0 AS (SELECT * FROM UNNEST ([60, 20, 30])),
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT n + (SELECT COUNT(*) FROM T0) FROM T1 WHERE n < 4))
SELECT * FROM T1

+---+
| n |
+---+
| 1 |
| 4 |
+---+
```

`INNER JOIN` can be used inside subqueries:

```sql
WITH RECURSIVE
  T0 AS (SELECT 1 AS n),
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT n + 1 FROM T1 INNER JOIN T0 USING (n)))
SELECT * FROM T1;

+---+
| n |
+---+
| 1 |
| 2 |
+---+
```

`CROSS JOIN` can be used inside subqueries:

```sql
WITH RECURSIVE
  T0 AS (SELECT 2 AS p),
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT T1.n + T0.p FROM T1 CROSS JOIN T0 WHERE T1.n < 4))
SELECT * FROM T1 CROSS JOIN T0;

+---+---+
| n | p |
+---+---+
| 1 | 2 |
| 3 | 2 |
| 5 | 2 |
+---+---+
```

In the following query, if `UNION DISTINCT` was replaced with `UNION ALL`,
this query would never terminate; it would keep generating rows
`0, 1, 2, 3, 4, 0, 1, 2, 3, 4...`. With `UNION DISTINCT`, however, the only row
produced by iteration `5` is a duplicate, so the query terminates.

```sql
WITH RECURSIVE
  T1 AS ( (SELECT 0 AS n) UNION DISTINCT (SELECT MOD(n + 1, 5) FROM T1) )
SELECT * FROM T1;

+---+
| n |
+---+
| 0 |
| 1 |
| 2 |
| 3 |
| 4 |
+---+
```

##### Examples of disallowed recursive CTEs

The following recursive CTE is disallowed because the
self-reference does not include a set operator, base term, and
recursive term.

```sql {.bad}
WITH RECURSIVE
  T1 AS (SELECT * FROM T1)
SELECT * FROM T1
```

The following recursive CTE is disallowed because the
self-reference to `T1` is in the base term. Self references are only allowed in
the recursive term.

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT * FROM T1) UNION ALL (SELECT 1))
SELECT * FROM T1
```

The following recursive CTE is disallowed because there are multiple
self-references in the recursive term when there must only be one.

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL ((SELECT * FROM T1) UNION ALL (SELECT * FROM T1)))
SELECT * FROM T1
```

The following recursive CTE is disallowed because the self-reference is
inside an [expression subquery][expression-subquery-concepts]

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT (SELECT n FROM T1)))
SELECT * FROM T1
```

The following recursive CTE is disallowed because there is a
self-reference as an argument to a table-valued function (TVF).

```sql {.bad}
WITH RECURSIVE
  T1 AS (
    (SELECT 1 AS n) UNION ALL
    (SELECT * FROM MY_TVF(T1)))
SELECT * FROM T1;
```

The following recursive CTE is disallowed because there is a
self-reference as input to an outer join.

```sql {.bad}
WITH RECURSIVE
  T0 AS (SELECT 1 AS n),
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT * FROM T1 FULL OUTER JOIN T0 USING (n)))
SELECT * FROM T1;
```

The following recursive CTE is disallowed due to multiple
self-references.

```sql {.bad}
WITH RECURSIVE
  T1 AS (
    (SELECT 1 AS n) UNION ALL
    (SELECT n + 1 FROM T1 CROSS JOIN T1))
SELECT * FROM T1;
```

The following recursive CTE is disallowed due to illegal
aggregation.

```sql {.bad}
WITH RECURSIVE
  T1 AS (
    (SELECT 1 AS n) UNION ALL
    (SELECT COUNT(*) FROM T1))
SELECT * FROM T1;
```

The following recursive CTE is disallowed due to an illegal
analytic function `OVER` clause.

```sql {.bad}
WITH RECURSIVE
  T1(n) AS (
    VALUES (1.0) UNION ALL
    SELECT 1 + AVG(n) OVER(ROWS between 2 PRECEDING and 0 FOLLOWING) FROM T1 WHERE n < 10)
SELECT n FROM T1;
```

The following recursive CTE is disallowed due to an illegal
`LIMIT` clause.

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT n FROM T1 LIMIT 3))
SELECT * FROM T1;
```

The following recursive CTE is disallowed due to an illegal
`ORDER BY` clause.

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT n + 1 FROM T1 ORDER BY n))
SELECT * FROM T1;
```

The following recursive CTE is disallowed due to an illegal
`ORDER BY` clause.

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL (SELECT n + 1 FROM T1) ORDER BY n)
SELECT * FROM T1;
```

The following recursive CTE is disallowed because table `T1` can't be
recursively referenced from inside an inner `WITH` clause

```sql {.bad}
WITH RECURSIVE
  T1 AS ((SELECT 1 AS n) UNION ALL (WITH t AS (SELECT n FROM T1) SELECT * FROM t))
SELECT * FROM T1
```

### CTE rules and constraints 
<a id="cte_rules"></a>

Common table expressions (CTEs) can be referenced inside the query expression
that contains the `WITH` clause.

##### General rules 
<a id="cte_general_rules"></a>

Here are some general rules and constraints to consider when working with CTEs:

+ Each CTE in the same `WITH` clause must have a unique name.
+ You must include the [`RECURSIVE` keyword][recursive-keyword] keyword if the
  `WITH` clause contains a recursive CTE.
+ The [`RECURSIVE` keyword][recursive-keyword] in
  the `WITH` clause changes the visibility of CTEs to other CTEs in the
  same `WITH` clause. You can learn more [here][recursive-keyword].
+ A local CTE overrides an outer CTE or table with the same name.
+ A CTE on a subquery may not reference correlated columns from the outer query.

##### Base term rules 
<a id="base_term_rules"></a>

The following rules apply to the base term in a recursive CTE:

+ The base term is required to be non-recursive.
+ The base term determines the names and types of all of the
  table columns.

##### Recursive term rules 
<a id="recursive_cte_rules"></a>

The following rules apply to the recursive term in a recursive CTE:

+ The recursive term must include exactly one reference to the
  recursively-defined table in the base term.
+ The recursive term must contain the same number of columns as the
  base term, and the type of each column must be implicitly coercible to
  the type of the corresponding column in the base term.
+ A recursive table reference cannot be used as an operand to a `FULL JOIN`,
  a right operand to a `LEFT JOIN`, or a left operand to a `RIGHT JOIN`.
+ A recursive table reference cannot be used with the `TABLESAMPLE` operator.
+ A recursive table reference cannot be used as an operand to a
  table-valued function (TVF).

The following rules apply to a subquery inside an recursive term:

+ A subquery with a recursive table reference must be a `SELECT` expression,
  not a set operation, such as `UNION ALL`.
+ A subquery cannot contain, directly or indirectly, a
  recursive table reference anywhere outside of its `FROM` clause.
+ A subquery with a recursive table reference cannot contain an `ORDER BY` or
  `LIMIT` clause.
+ A subquery with a recursive table reference cannot invoke aggregate functions.
+ A subquery with a recursive table reference cannot invoke analytic functions.
+ A subquery with a recursive table reference cannot contain the
  `DISTINCT` keyword or
  `GROUP BY` clause. To filter
  duplicates, use `UNION DISTINCT` in the top-level set operation,
  instead.

### CTE visibility 
<a id="cte_visibility"></a>

The visibility of a common table expression (CTE) within a query expression
is determined by whether or not you add the `RECURSIVE` keyword to the
`WITH` clause where the CTE was defined. You can learn more about these
differences in the following sections.

#### Visibility of CTEs in a `WITH` clause with the `RECURSIVE` keyword

When you include the `RECURSIVE` keyword, references between CTEs in the `WITH`
clause can go backwards and forwards. Cycles are not allowed.

This is what happens when you have two CTEs that reference
themselves or each other in a `WITH` clause with the `RECURSIVE`
keyword. Assume that `A` is the first CTE and `B` is the second
CTE in the clause:

+ A references A = Valid
+ A references B = Valid
+ B references A = Valid
+ A references B references A = Invalid (cycles are not allowed)

`A` can reference itself because self-references are supported:

```sql
WITH RECURSIVE
  A AS (SELECT 1 AS n UNION ALL (SELECT n + 1 FROM A WHERE n < 3))
SELECT * FROM A

+---+
| n |
+---+
| 1 |
| 2 |
| 3 |
+---+
```

`A` can reference `B` because references between CTEs can go forwards:

```sql
WITH RECURSIVE
  A AS (SELECT * FROM B),
  B AS (SELECT 1 AS n)
SELECT * FROM B

+---+
| n |
+---+
| 1 |
+---+
```

`B` can reference `A` because references between CTEs can go backwards:

```sql
WITH RECURSIVE
  A AS (SELECT 1 AS n),
  B AS (SELECT * FROM A)
SELECT * FROM B

+---+
| n |
+---+
| 1 |
+---+
```

This produces an error. `A` and `B` reference each other, which creates a cycle:

```sql
WITH RECURSIVE
  A AS (SELECT * FROM B),
  B AS (SELECT * FROM A)
SELECT * FROM B

-- Error
```

#### Visibility of CTEs in a `WITH` clause without the `RECURSIVE` keyword

When you don't include the `RECURSIVE` keyword in the `WITH` clause,
references between CTEs in the clause can go backward but not forward.

This is what happens when you have two CTEs that reference
themselves or each other in a `WITH` clause without
the `RECURSIVE` keyword. Assume that `A` is the first CTE and `B`
is the second CTE in the clause:

+ A references A = Invalid
+ A references B = Invalid
+ B references A = Valid
+ A references B references A = Invalid (cycles are not allowed)

This produces an error. `A` cannot reference itself because self-references are
not supported:

```sql
WITH
  A AS (SELECT 1 AS n UNION ALL (SELECT n + 1 FROM A WHERE n < 3))
SELECT * FROM A

-- Error
```

This produces an error. `A` cannot reference `B` because references between
CTEs can go backwards but not forwards:

```sql
WITH
  A AS (SELECT * FROM B),
  B AS (SELECT 1 AS n)
SELECT * FROM B

-- Error
```

`B` can reference `A` because references between CTEs can go backwards:

```sql
WITH
  A AS (SELECT 1 AS n),
  B AS (SELECT * FROM A)
SELECT * FROM B

+---+
| n |
+---+
| 1 |
+---+
```

This produces an error. `A` and `B` reference each other, which creates a
cycle:

```sql
WITH
  A AS (SELECT * FROM B),
  B AS (SELECT * FROM A)
SELECT * FROM B

-- Error
```

## WITH ANONYMIZATION clause 
<a id="anon_clause"></a>

<pre class="lang-sql prettyprint">
WITH ANONYMIZATION OPTIONS( privacy_parameters )
</pre>

This clause lets you anonymize the results of a query with differentially
private aggregations. To learn more about this clause, see
[Anonymization and Differential Privacy][anon-concepts].

Note: the `WITH ANONYMIZATION` clause cannot be used with the `WITH` clause.
Support for this clause in query patterns is limited.

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
<a id="group_by_clause_example"></a>

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
<a id="union_example"></a>

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
<a id="intersect_example"></a>

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
<a id="except_example"></a>

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

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[iana-language-subtag-registry]: https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry

[unicode-locale-identifier]: https://www.unicode.org/reports/tr35/#Unicode_locale_identifier

[tr35-collation-settings]: http://www.unicode.org/reports/tr35/tr35-collation.html#Setting_Options

[tr10-collation-algorithm]: http://www.unicode.org/reports/tr10/

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

[stratified-sampling]: #stratified_sampling

[scaling-weight]: #scaling_weight

[query-joins]: #join_types

[ambiguous-aliases]: #ambiguous_aliases

[with-clause]: #with_clause

[cte-rules]: #cte_rules

[non-recursive-cte]: #simple_cte

[unnest-operator]: #unnest_operator

[cte-visibility]: #cte_visibility

[comma-cross-join]: #comma_cross_join

[cross-join]: #cross_join

[correlated-join]: #correlated_join

[unpivot-operator]: #unpivot_operator

[tablesample-operator]: #tablesample_operator

[recursive-keyword]: #recursive_keyword

[base-term-rules]: #base_term_rules

[recursive-cte-rules]: #recursive_cte_rules

[recursive-cte]: #recursive_cte

[analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[query-window-specification]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#def_window_spec

[named-window-example]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#def_use_named_window

[produce-table]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#produce_table

[tvf-concepts]: https://github.com/google/zetasql/blob/master/docs/user-defined-functions.md#tvfs

[anon-concepts]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md

[flattening-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#flattening_arrays

[working-with-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#working_with_arrays

[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

[orderable-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#orderable_data_types

[subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries.md

[correlated-subquery]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#correlated_subquery_concepts

[table-subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#table_subquery_concepts

[expression-subquery-concepts]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#expression_subquery_concepts

[in-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#in_operators

[expression-subqueries]: https://github.com/google/zetasql/blob/master/docs/expression_subqueries.md

[array-subscript-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#array_subscript_operator

[flattening-trees-into-table]: https://github.com/google/zetasql/blob/master/docs/arrays.md#flattening_nested_data_into_table

[flatten-operator]: https://github.com/google/zetasql/blob/master/docs/array_functions.md#flatten

[array-el-field-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#array_el_field_operator

[collation-spec]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_spec_details

<!-- mdlint on -->

