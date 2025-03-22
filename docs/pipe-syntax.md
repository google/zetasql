

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Pipe query syntax

Pipe query syntax is an extension to ZetaSQL that's simpler and more
concise than [standard query syntax][query-syntax]. Pipe syntax supports the
same operations as standard syntax, and improves some areas of SQL query
functionality and usability.

For more background and details on pipe syntax design, see the research paper
[SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL][pipe-syntax-paper]{: .external}.

## Pipe syntax 
<a id="pipe_syntax"></a>

Pipe syntax has the following key characteristics:

+   Each pipe operator in pipe syntax consists of the pipe symbol, `|>`,
   an operator name, and any arguments: \
    `|> operator_name argument_list`
+   Pipe operators can be added to the end of any valid query.
+   Pipe operators can be applied in any order, any number of times.
+   Pipe syntax works anywhere standard syntax is supported: in queries, views,
    table-valued functions (TVFs), and other contexts.
+   Pipe syntax can be mixed with standard syntax in the same query. For
    example, subqueries can use different syntax from the parent query.
+   A pipe operator can see every alias that exists in the table
    preceding the pipe.
+   A query can [start with a `FROM` clause][from-queries], and pipe
    operators can optionally be added after the `FROM` clause.

### Query comparison

Consider the following table called `Produce`:

```zetasql
CREATE OR REPLACE TABLE Produce AS (
  SELECT 'apples' AS item, 2 AS sales, 'fruit' AS category
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales, 'vegetable' AS category
  UNION ALL
  SELECT 'apples' AS item, 7 AS sales, 'fruit' AS category
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales, 'fruit' AS category
);

SELECT * FROM Produce;

/*---------+-------+-----------+
 | item    | sales | category  |
 +---------+-------+-----------+
 | apples  | 2     | fruit     |
 | carrots | 8     | vegetable |
 | apples  | 7     | fruit     |
 | bananas | 5     | fruit     |
 +---------+-------+-----------*/
```

Compare the following equivalent queries that compute the number and total
amount of sales for each item in the `Produce` table:

**Standard syntax**

```zetasql
SELECT item, COUNT(*) AS num_items, SUM(sales) AS total_sales
FROM Produce
WHERE
  item != 'bananas'
  AND category IN ('fruit', 'nut')
GROUP BY item
ORDER BY item DESC;

/*--------+-----------+-------------+
 | item   | num_items | total_sales |
 +--------+-----------+-------------+
 | apples | 2         | 9           |
 +--------+-----------+-------------*/
```

**Pipe syntax**

```zetasql
FROM Produce
|> WHERE
    item != 'bananas'
    AND category IN ('fruit', 'nut')
|> AGGREGATE COUNT(*) AS num_items, SUM(sales) AS total_sales
   GROUP BY item
|> ORDER BY item DESC;

/*--------+-----------+-------------+
 | item   | num_items | total_sales |
 +--------+-----------+-------------+
 | apples | 2         | 9           |
 +--------+-----------+-------------*/
```

## Pipe operator semantics 
<a id="pipe_semantics"></a>

Pipe operators have the following semantic behavior:

+   Each pipe operator performs a self-contained operation.
+   A pipe operator consumes the input table passed to it through the pipe
    symbol, `|>`, and produces a new table as output.
+   A pipe operator can reference only columns from its immediate input table.
    Columns from earlier in the same query aren't visible. Inside subqueries,
    correlated references to outer columns are still allowed.

## `FROM` queries 
<a id="from_queries"></a>

In pipe syntax, a query can start with a standard [`FROM` clause][from-clause]
and use any standard `FROM` syntax, including tables, joins, subqueries,
`UNNEST` operations, and
table-valued functions (TVFs). Table aliases can be
assigned to each input item using the [`AS alias` clause][using-aliases].

A query with only a `FROM` clause, like `FROM table_name`, is allowed in pipe
syntax and returns all rows from the table. For tables with columns,
`FROM table_name` in pipe syntax is similar to
[`SELECT * FROM table_name`][select-star] in standard syntax.
 For [value tables][value-tables], `FROM table_name` in
pipe syntax returns the row values without expanding fields, similar to
[`SELECT value FROM table_name AS value`][select-as-value] in standard
syntax.

**Examples**

The following queries use the [`Produce` table][query-comparison]:

```zetasql
FROM Produce;

/*---------+-------+-----------+
 | item    | sales | category  |
 +---------+-------+-----------+
 | apples  | 2     | fruit     |
 | carrots | 8     | vegetable |
 | apples  | 7     | fruit     |
 | bananas | 5     | fruit     |
 +---------+-------+-----------*/
```

```zetasql
-- Join tables in the FROM clause and then apply pipe operators.
FROM
  Produce AS p1
  JOIN Produce AS p2
    USING (item)
|> WHERE item = 'bananas'
|> SELECT p1.item, p2.sales;

/*---------+-------+
 | item    | sales |
 +---------+-------+
 | bananas | 5     |
 +---------+-------*/
```

## Pipe operators

ZetaSQL supports the following pipe operators. For operators that
correspond or relate to similar operations in standard syntax, the operator
descriptions highlight similarities and differences and link to more detailed
documentation on the corresponding syntax.

### Pipe operator list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#select_pipe_operator"><code>SELECT</code></a>
</td>
  <td>Produces a new table with the listed columns.</td>
</tr>

<tr>
  <td><a href="#extend_pipe_operator"><code>EXTEND</code></a>
</td>
  <td>Propagates the existing table and adds computed columns.</td>
</tr>

<tr>
  <td><a href="#set_pipe_operator"><code>SET</code></a>
</td>
  <td>Replaces the values of columns in the current table.</td>
</tr>

<tr>
  <td><a href="#drop_pipe_operator"><code>DROP</code></a>
</td>
  <td>Removes listed columns from the current table.</td>
</tr>

<tr>
  <td><a href="#rename_pipe_operator"><code>RENAME</code></a>
</td>
  <td>Renames specified columns.</td>
</tr>

<tr>
  <td><a href="#as_pipe_operator"><code>AS</code></a>
</td>
  <td>Introduces a table alias for the input table.</td>
</tr>

<tr>
  <td><a href="#where_pipe_operator"><code>WHERE</code></a>
</td>
  <td>Filters the results of the input table.</td>
</tr>

<tr>
  <td><a href="#limit_pipe_operator"><code>LIMIT</code></a>
</td>
  <td>
    Limits the number of rows to return in a query, with an optional
    <code>OFFSET</code> clause to skip over rows.
  </td>
</tr>

<tr>
  <td><a href="#aggregate_pipe_operator"><code>AGGREGATE</code></a>
</td>
  <td>
    Performs aggregation on data across groups of rows or the full
    input table.
  </td>
</tr>

<tr>
  <td><a href="#distinct_pipe_operator"><code>DISTINCT</code></a>
</td>
  <td>
    Returns distinct rows from the input table, while preserving table aliases.
  </td>
</tr>

<tr>
  <td><a href="#order_by_pipe_operator"><code>ORDER BY</code></a>
</td>
  <td>Sorts results by a list of expressions.</td>
</tr>

<tr>
  <td><a href="#union_pipe_operator"><code>UNION</code></a>
</td>
  <td>
    Returns the combined results of the input queries to the left and right of the pipe operator.
  </td>
</tr>

<tr>
  <td><a href="#intersect_pipe_operator"><code>INTERSECT</code></a>
</td>
  <td>
    Returns rows that are found in the results of both the input query to the left
    of the pipe operator and all input queries to the right of the pipe
    operator.
  </td>
</tr>

<tr>
  <td><a href="#except_pipe_operator"><code>EXCEPT</code></a>
</td>
  <td>
    Returns rows from the input query to the left of the pipe operator that
    aren't present in any input queries to the right of the pipe operator.
  </td>
</tr>

<tr>
  <td><a href="#join_pipe_operator"><code>JOIN</code></a>
</td>
  <td>
    Joins rows from the input table with rows from a second table provided as an
    argument.
  </td>
</tr>

<tr>
  <td><a href="#call_pipe_operator"><code>CALL</code></a>
</td>
  <td>
    Calls a table-valued function (TVF), passing the pipe input table as a
    table argument.
  </td>
</tr>

<!-- disableFinding(LINK_ID) -->

<tr>
  <td><a href="#window_pipe_operator"><code>WINDOW</code></a>
</td>
  <td>
    Deprecated. Adds columns with the result of computing the function
    over some window of existing rows.
  </td>
</tr>

<tr>
  <td><a href="#tablesample_pipe_operator"><code>TABLESAMPLE</code></a>
</td>
  <td>Selects a random sample of rows from the input table.</td>
</tr>

<tr>
  <td><a href="#pivot_pipe_operator"><code>PIVOT</code></a>
</td>
  <td>Rotates rows into columns.</td>
</tr>

<tr>
  <td><a href="#unpivot_pipe_operator"><code>UNPIVOT</code></a>
</td>
  <td>Rotates columns into rows.</td>
</tr>

<tr>
  <td><a href="#assert_pipe_operator"><code>ASSERT</code></a>
</td>
  <td>
    Evaluates that an expression is true for all input rows, raising an error
    if not.
  </td>
</tr>

  </tbody>
</table>

### `SELECT` pipe operator 
<a id="select_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> SELECT expression [[AS] alias] [, ...]
</pre>

**Description**

Produces a new table with the listed columns, similar to the outermost
[`SELECT` clause][select-clause] in a table subquery in standard syntax.
Supports standard output modifiers like `SELECT AS STRUCT`, and supports
[window functions][window-functions]. Doesn't support aggregations or
anonymization.

In pipe syntax, the `SELECT` operator in a query is optional. The `SELECT`
operator can be used near the end of a query to specify the list of output
columns. The final query result contains the columns returned from the last pipe
operator. If the `SELECT` operator isn't used to select specific columns, the
output includes the full row, similar to what the
[`SELECT *` statement][select-star] in standard syntax produces.
 For [value tables][value-tables], the result is the
row value, without field expansion.

In pipe syntax, the `SELECT` clause doesn't perform aggregation. Use the
[`AGGREGATE` operator][aggregate-pipe-operator] instead.

For cases where `SELECT` would be used in standard syntax to rearrange columns,
pipe syntax supports other operators:

+   The [`EXTEND` operator][extend-pipe-operator] adds columns.
+   The [`SET` operator][set-pipe-operator] updates the value of an existing
    column.
+   The [`DROP` operator][drop-pipe-operator] removes columns.
+   The [`RENAME` operator][rename-pipe-operator] renames columns.

**Example**

```zetasql
FROM (SELECT 'apples' AS item, 2 AS sales)
|> SELECT item AS fruit_name;

/*------------+
 | fruit_name |
 +------------+
 | apples     |
 +------------*/
```

[select-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_list

[window-functions]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

[select-star]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_

[aggregate-pipe-operator]: #aggregate_pipe_operator

[extend-pipe-operator]: #extend_pipe_operator

[set-pipe-operator]: #set_pipe_operator

[drop-pipe-operator]: #drop_pipe_operator

[rename-pipe-operator]: #rename_pipe_operator

[value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

### `EXTEND` pipe operator 
<a id="extend_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">EXTEND</span> expression [[AS] alias] [, ...]
</pre>

**Description**

Propagates the existing table and adds computed columns, similar to
[`SELECT *, new_column`][select-star] in standard syntax. Supports
[window functions][window-functions].

**Examples**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> EXTEND item IN ('carrots', 'oranges') AS is_orange;

/*---------+-------+------------+
 | item    | sales | is_orange  |
 +---------+-------+------------+
 | apples  | 2     | FALSE      |
 | carrots | 8     | TRUE       |
 +---------+-------+------------*/
```

```zetasql
-- Window function, with `OVER`
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> EXTEND SUM(sales) OVER() AS total_sales;

/*---------+-------+-------------+
 | item    | sales | total_sales |
 +---------+-------+-------------+
 | apples  | 2     | 15          |
 | bananas | 5     | 15          |
 | carrots | 8     | 15          |
 +---------+-------+-------------*/
```

[select-star]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_

[window-functions]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

### `SET` pipe operator 
<a id="set_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> SET column_name = expression [, ...]
</pre>

**Description**

Replaces the value of a column in the current table, similar to
[`SELECT * REPLACE (expression AS column)`][select-replace] in standard syntax.
Each referenced column must exist exactly once in the input table.

After a `SET` operation, the referenced top-level columns (like `x`) are
updated, but table aliases (like `t`) still refer to the original row values.
Therefore, `t.x` will still refer to the original value.

**Example**

```zetasql
(
  SELECT 1 AS x, 11 AS y
  UNION ALL
  SELECT 2 AS x, 22 AS y
)
|> SET x = x * x, y = 3;

/*---+---+
 | x | y |
 +---+---+
 | 1 | 3 |
 | 4 | 3 |
 +---+---*/
```

```zetasql
FROM (SELECT 2 AS x, 3 AS y) AS t
|> SET x = x * x, y = 8
|> SELECT t.x AS original_x, x, y;

/*------------+---+---+
 | original_x | x | y |
 +------------+---+---+
 | 2          | 4 | 8 |
 +------------+---+---*/
```

[select-replace]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_replace

### `DROP` pipe operator 
<a id="drop_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> DROP column_name [, ...]
</pre>

**Description**

Removes listed columns from the current table, similar to
[`SELECT * EXCEPT (column)`][select-except] in standard syntax. Each
referenced column must exist at least once in the input table.

After a `DROP` operation, the referenced top-level columns (like `x`) are
removed, but table aliases (like `t`) still refer to the original row values.
Therefore, `t.x` will still refer to the original value.

The `DROP` operator doesn't correspond to the
[`DROP` statement][drop-statement] in data definition language (DDL), which
deletes persistent schema objects.

**Example**

```zetasql
SELECT 'apples' AS item, 2 AS sales, 'fruit' AS category
|> DROP sales, category;

/*--------+
 | item   |
 +--------+
 | apples |
 +--------*/
```

```zetasql
FROM (SELECT 1 AS x, 2 AS y) AS t
|> DROP x
|> SELECT t.x AS original_x, y;

/*------------+---+
 | original_x | y |
 +------------+---+
 | 1          | 2 |
 +------------+---*/
```

[select-except]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_except

[drop-statement]: https://github.com/google/zetasql/blob/master/docs/data-definition-language.md#drop

### `RENAME` pipe operator 
<a id="rename_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">RENAME</span> old_column_name [AS] new_column_name [, ...]
</pre>

**Description**

Renames specified columns. Each column to be renamed must exist exactly once in
the input table. The `RENAME` operator can't rename value table fields,
pseudo-columns, range variables, or objects that aren't columns in the input
table.

After a `RENAME` operation, the referenced top-level columns (like `x`) are
renamed, but table aliases (like `t`) still refer to the original row
values. Therefore, `t.x` will still refer to the original value.

**Example**

```zetasql
SELECT 1 AS x, 2 AS y, 3 AS z
|> AS t
|> RENAME y AS renamed_y
|> SELECT *, t.y AS t_y;

/*---+-----------+---+-----+
 | x | renamed_y | z | t_y |
 +---+-----------+---+-----+
 | 1 | 2         | 3 | 2   |
 +---+-----------+---+-----*/
```

### `AS` pipe operator 
<a id="as_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> AS alias
</pre>

**Description**

Introduces a table alias for the input table, similar to applying the
[`AS alias` clause][using-aliases] on a table subquery in standard syntax. Any
existing table aliases are removed and the new alias becomes the table alias for
all columns in the row.

The `AS` operator can be useful after operators like
[`SELECT`][select-pipe-operator], [`EXTEND`][extend-pipe-operator], or
[`AGGREGATE`][aggregate-pipe-operator] that add columns but can't give table
aliases to them. You can use the table alias to disambiguate columns after the
`JOIN` operator.

**Example**

```zetasql
(
  SELECT "000123" AS id, "apples" AS item, 2 AS sales
  UNION ALL
  SELECT "000456" AS id, "bananas" AS item, 5 AS sales
) AS sales_table
|> AGGREGATE SUM(sales) AS total_sales GROUP BY id, item
-- The sales_table alias is now out of scope. We must introduce a new one.
|> AS t1
|> JOIN (SELECT 456 AS id, "yellow" AS color) AS t2
   ON CAST(t1.id AS INT64) = t2.id
|> SELECT t2.id, total_sales, color;

/*-----+-------------+--------+
 | id  | total_sales | color  |
 +-----+-------------+--------+
 | 456 | 5           | yellow |
 +-----+-------------+--------*/
```

[using-aliases]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#using_aliases

[select-pipe-operator]: #select_pipe_operator

[extend-pipe-operator]: #extend_pipe_operator

[aggregate-pipe-operator]: #aggregate_pipe_operator

### `WHERE` pipe operator 
<a id="where_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> WHERE boolean_expression
</pre>

**Description**

Filters the results of the input table. The `WHERE` operator behaves the same
as the [`WHERE` clause][where-clause] in standard syntax.

In pipe syntax, the `WHERE` operator also replaces the
[`HAVING` clause][having-clause] and [`QUALIFY` clause][qualify-clause] in
standard syntax. For example, after performing aggregation with the
[`AGGREGATE` operator][aggregate-pipe-operator], use the `WHERE` operator
instead of the `HAVING` clause. For [window functions][window-functions] inside
a `QUALIFY` clause, use window functions inside a `WHERE` clause instead.

**Example**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> WHERE sales >= 3;

/*---------+-------+
 | item    | sales |
 +---------+-------+
 | bananas | 5     |
 | carrots | 8     |
 +---------+-------*/
```

[where-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#where_clause

[having-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#having_clause

[qualify-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#qualify_clause

[aggregate-pipe-operator]: #aggregate_pipe_operator

[window-functions]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

### `LIMIT` pipe operator 
<a id="limit_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">LIMIT</span> count [OFFSET skip_rows]
</pre>

**Description**

Limits the number of rows to return in a query, with an optional `OFFSET` clause
to skip over rows. The `LIMIT` operator behaves the same as the
[`LIMIT` and `OFFSET` clause][limit-offset-clause] in standard syntax.

**Examples**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> ORDER BY item
|> LIMIT 1;

/*---------+-------+
 | item    | sales |
 +---------+-------+
 | apples  | 2     |
 +---------+-------*/
```

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> ORDER BY item
|> LIMIT 1 OFFSET 2;

/*---------+-------+
 | item    | sales |
 +---------+-------+
 | carrots | 8     |
 +---------+-------*/
```

[limit-offset-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_and_offset_clause

### `AGGREGATE` pipe operator 
<a id="aggregate_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
-- Full-table aggregation
|> <span class="kwd">AGGREGATE</span> aggregate_expression [[AS] alias] [, ...]
</pre>
<pre class="lang-sql prettyprint no-copy">
-- Aggregation with grouping
|> <span class="kwd">AGGREGATE</span> [aggregate_expression [[AS] alias] [, ...]]
   GROUP BY groupable_items [[AS] alias] [, ...]
</pre>
<pre class="lang-sql prettyprint">
-- Aggregation with grouping and shorthand ordering syntax
|> <span class="kwd">AGGREGATE</span> [aggregate_expression [[AS] alias] [<span class="var">order_suffix</span>] [, ...]]
   GROUP [AND ORDER] BY groupable_item [[AS] alias] [<span class="var">order_suffix</span>] [, ...]

<span class="var">order_suffix</span>: {ASC | DESC} [{<span class="var">NULLS FIRST</span> | <span class="var">NULLS LAST</span>}]
</pre>

**Description**

Performs aggregation on data across grouped rows or an entire table. The
`AGGREGATE` operator is similar to a query in standard syntax that contains a
[`GROUP BY` clause][group-by-clause] or a `SELECT` list with
[aggregate functions][aggregate-functions] or both. In pipe syntax, the
`GROUP BY` clause is part of the `AGGREGATE` operator. Pipe syntax
doesn't support a standalone `GROUP BY` operator.

Without the `GROUP BY` clause, the `AGGREGATE` operator performs full-table
aggregation and produces one output row.

With the `GROUP BY` clause, the `AGGREGATE` operator performs aggregation with
grouping, producing one row for each set of distinct values for the grouping
expressions.

The `AGGREGATE` expression list corresponds to the aggregated expressions in a
`SELECT` list in standard syntax. Each expression in the `AGGREGATE` list must
include an aggregate function. Aggregate expressions can also include scalar
expressions (for example, `sqrt(SUM(x*x))`). Column aliases can be assigned
using the [`AS` operator][as-pipe-operator]. Window functions aren't allowed,
but the [`EXTEND` operator][extend-pipe-operator] can be used before the
`AGGREGATE` operator to compute window functions.

The `GROUP BY` clause in the `AGGREGATE` operator corresponds to the `GROUP BY`
clause in standard syntax. Unlike in standard syntax, aliases can be assigned to
`GROUP BY` items. Standard grouping operators like `GROUPING SETS`, `ROLLUP`,
and `CUBE` are supported.

The output columns from the `AGGREGATE` operator include all grouping columns
first, followed by all aggregate columns, using their assigned aliases as the
column names.

Unlike in standard syntax, grouping expressions aren't repeated across `SELECT`
and `GROUP BY` clauses. In pipe syntax, the grouping expressions are listed
once, in the `GROUP BY` clause, and are automatically included as output columns
for the `AGGREGATE` operator.

Because output columns are fully specified by the `AGGREGATE` operator, the
`SELECT` operator isn't needed after the `AGGREGATE` operator unless
you want to produce a list of columns different from the default.

**Standard syntax**

<pre class="lang-sql prettyprint">
-- Aggregation in standard syntax
SELECT SUM(col1) AS total, col2, col3, col4...
FROM table1
GROUP BY col2, col3, col4...
</pre>

**Pipe syntax**

<pre class="lang-sql prettyprint">
-- The same aggregation in pipe syntax
FROM table1
|> <span class="kwd">AGGREGATE</span> SUM(col1) AS total
   GROUP BY col2, col3, col4...
</pre>

**Examples**

```zetasql
-- Full-table aggregation
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'apples' AS item, 7 AS sales
)
|> AGGREGATE COUNT(*) AS num_items, SUM(sales) AS total_sales;

/*-----------+-------------+
 | num_items | total_sales |
 +-----------+-------------+
 | 3         | 14          |
 +-----------+-------------*/
```

```zetasql
-- Aggregation with grouping
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'apples' AS item, 7 AS sales
)
|> AGGREGATE COUNT(*) AS num_items, SUM(sales) AS total_sales
   GROUP BY item;

/*---------+-----------+-------------+
 | item    | num_items | total_sales |
 +---------+-----------+-------------+
 | apples  | 2         | 9           |
 | bananas | 1         | 5           |
 +---------+-----------+-------------*/
```

#### Shorthand ordering syntax with `AGGREGATE` 
<a id="shorthand_order_pipe_syntax"></a>

The `AGGREGATE` operator supports a shorthand ordering syntax, which is
equivalent to applying the [`ORDER BY` operator][order-by-pipe-operator] as part
of the `AGGREGATE` operator without repeating the column list:

<pre class="lang-sql prettyprint">
-- Aggregation with grouping and shorthand ordering syntax
|> <span class="kwd">AGGREGATE</span> [aggregate_expression [[AS] alias] [<span class="var">order_suffix</span>] [, ...]]
   GROUP [AND ORDER] BY groupable_item [[AS] alias] [<span class="var">order_suffix</span>] [, ...]

<span class="var">order_suffix</span>: {ASC | DESC} [{NULLS FIRST | NULLS LAST}]
</pre>

The `GROUP AND ORDER BY` clause is equivalent to an `ORDER BY` clause on all
`groupable_items`. By default, each `groupable_item` is sorted in ascending
order with `NULL` values first. Other ordering suffixes like `DESC` or `NULLS
LAST` can be used for other orders.

Without the `GROUP AND ORDER BY` clause, the `ASC` or `DESC` suffixes can be
added on individual columns in the `GROUP BY` list or `AGGREGATE` list or both.
The `NULLS FIRST` and `NULLS LAST` suffixes can be used to further modify `NULL`
sorting.

Adding these suffixes is equivalent to adding an `ORDER BY` clause that includes
all of the suffixed columns with the suffixed grouping columns first, matching
the left-to-right output column order.

**Examples**

Consider the following table called `Produce`:

```zetasql
/*---------+-------+-----------+
 | item    | sales | category  |
 +---------+-------+-----------+
 | apples  | 2     | fruit     |
 | carrots | 8     | vegetable |
 | apples  | 7     | fruit     |
 | bananas | 5     | fruit     |
 +---------+-------+-----------*/
```

The following two equivalent examples show you how to order by all grouping
columns using the `GROUP AND ORDER BY` clause or a separate `ORDER BY` clause:

```zetasql
-- Order by all grouping columns using GROUP AND ORDER BY.
FROM Produce
|> AGGREGATE SUM(sales) AS total_sales
   GROUP AND ORDER BY category, item DESC;

/*-----------+---------+-------------+
 | category  | item    | total_sales |
 +-----------+---------+-------------+
 | fruit     | bananas | 5           |
 | fruit     | apples  | 9           |
 | vegetable | carrots | 8           |
 +-----------+---------+-------------*/
```

```zetasql
--Order by columns using ORDER BY after performing aggregation.
FROM Produce
|> AGGREGATE SUM(sales) AS total_sales
   GROUP BY category, item
|> ORDER BY category, item DESC;
```

You can add an ordering suffix to a column in the `AGGREGATE` list. Although the
`AGGREGATE` list appears before the `GROUP BY` list in the query, ordering
suffixes on columns in the `GROUP BY` list are applied first.

```zetasql
FROM Produce
|> AGGREGATE SUM(sales) AS total_sales ASC
   GROUP BY item, category DESC;

/*---------+-----------+-------------+
 | item    | category  | total_sales |
 +---------+-----------+-------------+
 | carrots | vegetable | 8           |
 | bananas | fruit     | 5           |
 | apples  | fruit     | 9           |
 +---------+-----------+-------------*/
```

The previous query is equivalent to the following:

```zetasql
-- Order by specified grouping and aggregate columns.
FROM Produce
|> AGGREGATE SUM(sales) AS total_sales
   GROUP BY item, category
|> ORDER BY category DESC, total_sales;
```

[group-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[aggregate-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

[as-pipe-operator]: #as_pipe_operator

[extend-pipe-operator]: #extend_pipe_operator

[order-by-pipe-operator]: #order_by_pipe_operator

### `DISTINCT` pipe operator 
<a id="distinct_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">DISTINCT</span>
</pre>

**Description**

Returns distinct rows from the input table, while preserving table aliases.

Using the `DISTINCT` operator after a `SELECT` or `UNION ALL` clause is similar
to using a [`SELECT DISTINCT` clause][select-distinct] or
[`UNION DISTINCT` clause][union-operator] in standard syntax, but the `DISTINCT`
pipe operator can be applied anywhere. The `DISTINCT` operator computes distinct
rows based on the values of all visible columns. Pseudo-columns are ignored
while computing distinct rows and are dropped from the output.

The `DISTINCT` operator is similar to using a `|> SELECT DISTINCT *` clause, but
doesn't expand value table fields, and preserves table aliases from the input.

**Examples**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> DISTINCT
|> WHERE sales >= 3;

/*---------+-------+
 | item    | sales |
 +---------+-------+
 | bananas | 5     |
 | carrots | 8     |
 +---------+-------*/
```

In the following example, the table alias `Produce` can be used in
expressions after the `DISTINCT` pipe operator.

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> AS Produce
|> DISTINCT
|> SELECT Produce.item;

/*---------+
 | item    |
 +---------+
 | apples  |
 | bananas |
 | carrots |
 +---------*/
```

By contrast, the table alias isn't visible after a `|> SELECT DISTINCT *`
clause.

```zetasql {.bad}
-- Error, unrecognnized name: Produce
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> AS Produce
|> SELECT DISTINCT *
|> SELECT Produce.item;
```

In the following examples, the `DISTINCT` operator doesn't expand value table
fields and retains the `STRUCT` type in the result. By contrast, the
`|> SELECT DISTINCT *` clause expands the `STRUCT` type into two columns.

```zetasql
SELECT AS STRUCT 1 x, 2 y
|> DISTINCT;

/*---------+
 | $struct |
 +---------+
  {
    x: 1,
    y: 2
  }
 +----------*/
```

```zetasql
SELECT AS STRUCT 1 x, 2 y
|> SELECT DISTINCT *;

/*---+---+
 | x | y |
 +---+---+
 | 1 | 2 |
 +---+---*/
```

The following examples show equivalent ways to generate the same results with
distinct values from columns `a`, `b`, and `c`.

```zetasql
FROM table
|> SELECT DISTINCT a, b, c;

FROM table
|> SELECT a, b, c
|> DISTINCT;

FROM table
|> AGGREGATE
   GROUP BY a, b, c;
```

[select-distinct]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_distinct

[union-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#union

[aggregate-pipe-operator]: https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md#aggregate_pipe_operator

[using-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#using_clause

[full-join]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#full_join

[inner-join]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#inner_join

### `ORDER BY` pipe operator 
<a id="order_by_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> ORDER BY expression [sort_options] [, ...]
</pre>

**Description**

Sorts results by a list of expressions. The `ORDER BY` operator behaves the same
as the [`ORDER BY` clause][order-by-clause] in standard syntax. Suffixes like
`ASC`, `DESC`, and `NULLS LAST` are supported for customizing the ordering for
each expression.

In pipe syntax, the [`AGGREGATE` operator][aggregate-pipe-operator] also
supports [shorthand ordering suffixes][shorthand-order-pipe-syntax] to
apply `ORDER BY` behavior more concisely as part of aggregation.

**Example**

```zetasql
(
  SELECT 1 AS x
  UNION ALL
  SELECT 3 AS x
  UNION ALL
  SELECT 2 AS x
)
|> ORDER BY x DESC;

/*---+
 | x |
 +---+
 | 3 |
 | 2 |
 | 1 |
 +---*/
```

[order-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#order_by_clause

[aggregate-pipe-operator]: #aggregate_pipe_operator

[shorthand-order-pipe-syntax]: #shorthand_order_pipe_syntax

### `UNION` pipe operator 
<a id="union_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
query
|> <span class="kwd">UNION</span> {ALL | DISTINCT} (query) [, (query), ...]
</pre>

**Description**

Returns the combined results of the input queries to the left and right of the
pipe operator. Columns are matched and rows are concatenated vertically.

The `UNION` pipe operator behaves the same as the
[`UNION` set operator][union-operator] in standard syntax. However, in pipe
syntax, the `UNION` pipe operator can include multiple comma-separated queries
without repeating the `UNION` syntax. Queries following the operator
are enclosed in parentheses.

For example, compare the following equivalent queries:

```zetasql
-- Standard syntax
SELECT * FROM ...
UNION ALL
SELECT 1
UNION ALL
SELECT 2;

-- Pipe syntax
SELECT * FROM ...
|> UNION ALL
    (SELECT 1),
    (SELECT 2);
```

The `UNION` pipe operator supports the same modifiers as the
`UNION` set operator in standard syntax, such as the
`BY NAME` modifier (or `CORRESPONDING`) and `LEFT | FULL [OUTER]` mode prefixes.

**Examples**

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|> UNION ALL (SELECT 1);

/*--------+
 | number |
 +--------+
 | 1      |
 | 2      |
 | 3      |
 | 1      |
 +--------*/
```

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|> UNION DISTINCT (SELECT 1);

/*--------+
 | number |
 +--------+
 | 1      |
 | 2      |
 | 3      |
 +--------*/
```

The following example shows multiple input queries to the right of the pipe
operator:

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|> UNION DISTINCT
    (SELECT 1),
    (SELECT 2);

/*--------+
 | number |
 +--------+
 | 1      |
 | 2      |
 | 3      |
 +--------*/
```

The following example uses the `BY NAME`
modifier to match results by column name instead of in the
order that the columns are given in the input queries.

```zetasql
SELECT 1 AS one_digit, 10 AS two_digit
|> UNION ALL BY NAME
    (SELECT 20 AS two_digit, 2 AS one_digit);

/*-----------+-----------+
 | one_digit | two_digit |
 +-----------+-----------+
 | 1         | 10        |
 | 2         | 20        |
 +-----------+-----------*/
```

Without the `BY NAME` modifier,
the results are matched by column position in the input query and the column
names are ignored.

```zetasql
SELECT 1 AS one_digit, 10 AS two_digit
|> UNION ALL
    (SELECT 20 AS two_digit, 2 AS one_digit);

-- Results follow column order from queries and ignore column names.
/*-----------+-----------+
 | one_digit | two_digit |
 +-----------+-----------+
 | 1         | 10        |
 | 20        | 2         |
 +-----------+-----------*/
```

[union-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#union

### `INTERSECT` pipe operator 
<a id="intersect_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
query
|> <span class="kwd">INTERSECT</span> {ALL | DISTINCT} (query) [, (query), ...]
</pre>

**Description**

Returns rows that are found in the results of both the input query to the left
of the pipe operator and all input queries to the right of the pipe
operator.

The `INTERSECT` pipe operator behaves the same as the
[`INTERSECT` set operator][intersect-operator] in standard syntax. However, in
pipe syntax, the `INTERSECT` pipe operator can include multiple
comma-separated queries without repeating the `INTERSECT` syntax. Queries
following the operator are enclosed in parentheses.

For example, compare the following equivalent queries:

```zetasql
-- Standard syntax
SELECT * FROM ...
INTERSECT ALL
SELECT 1
INTERSECT ALL
SELECT 2;

-- Pipe syntax
SELECT * FROM ...
|> INTERSECT ALL
    (SELECT 1),
    (SELECT 2);
```

The `INTERSECT` pipe operator supports the same modifiers as the
`INTERSECT` set operator in standard syntax, such as the
`BY NAME` modifier (or `CORRESPONDING`)
 and `LEFT | FULL [OUTER]` mode prefixes.

**Examples**

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> INTERSECT ALL
    (SELECT * FROM UNNEST(ARRAY<INT64>[2, 3, 3, 5]) AS number);

/*--------+
 | number |
 +--------+
 | 2      |
 | 3      |
 | 3      |
 +--------*/
```

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> INTERSECT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[2, 3, 3, 5]) AS number);

/*--------+
 | number |
 +--------+
 | 2      |
 | 3      |
 +--------*/
```

The following example shows multiple input queries to the right of the pipe
operator:

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> INTERSECT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[2, 3, 3, 5]) AS number),
    (SELECT * FROM UNNEST(ARRAY<INT64>[3, 3, 4, 5]) AS number);

/*--------+
 | number |
 +--------+
 | 3      |
 +--------*/
```

The following example uses the `BY NAME`
modifier to return the intersecting row from the columns despite the differing
column order in the input queries.

```zetasql
WITH
  NumbersTable AS (
    SELECT 1 AS one_digit, 10 AS two_digit
    UNION ALL
    SELECT 2, 20
    UNION ALL
    SELECT 3, 30
  )
SELECT one_digit, two_digit FROM NumbersTable
|> INTERSECT ALL BY NAME
    (SELECT 10 AS two_digit, 1 AS one_digit);

/*-----------+-----------+
 | one_digit | two_digit |
 +-----------+-----------+
 | 1         | 10        |
 +-----------+-----------*/
```

Without the `BY NAME` modifier, the same
columns in differing order are considered different columns, so the query
doesn't detect any intersecting row values.

```zetasql
WITH
  NumbersTable AS (
    SELECT 1 AS one_digit, 10 AS two_digit
    UNION ALL
    SELECT 2, 20
    UNION ALL
    SELECT 3, 30
  )
SELECT one_digit, two_digit FROM NumbersTable
|> INTERSECT ALL
    (SELECT 10 AS two_digit, 1 AS one_digit);

-- No intersecting values detected because columns aren't recognized as the same.
/*-----------+-----------+

 +-----------+-----------*/
```

[intersect-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#intersect

### `EXCEPT` pipe operator 
<a id="except_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
query
|> <span class="kwd">EXCEPT</span> {ALL | DISTINCT} (query) [, (query), ...]
</pre>

**Description**

Returns rows from the input query to the left of the pipe operator that aren't
present in any input queries to the right of the pipe operator.

The `EXCEPT` pipe operator behaves the same as the
[`EXCEPT` set operator][except-operator] in standard syntax. However, in pipe
syntax, the `EXCEPT` pipe operator can include multiple comma-separated
queries without repeating the `EXCEPT` syntax. Queries following the
operator are enclosed in parentheses.

For example, compare the following equivalent queries:

```zetasql
-- Standard syntax
SELECT * FROM ...
EXCEPT ALL
SELECT 1
EXCEPT ALL
SELECT 2;

-- Pipe syntax
SELECT * FROM ...
|> EXCEPT ALL
    (SELECT 1),
    (SELECT 2);
```

Parentheses can be used to group set operations and control order of operations.
In `EXCEPT` set operations, query results can vary depending on the operation
grouping.

```zetasql
-- Default operation grouping
(
  SELECT * FROM ...
  EXCEPT ALL
  SELECT 1
)
EXCEPT ALL
SELECT 2;

-- Modified operation grouping
SELECT * FROM ...
EXCEPT ALL
(
  SELECT 1
  EXCEPT ALL
  SELECT 2
);

-- Same modified operation grouping in pipe syntax
SELECT * FROM ...
|> EXCEPT ALL
(
  SELECT 1
  |> EXCEPT ALL (SELECT 2)
);
```

The `EXCEPT` pipe operator supports the same modifiers as the
`EXCEPT` set operator in standard syntax, such as the
`BY NAME` modifier (or `CORRESPONDING`) and `LEFT | FULL [OUTER]` mode prefixes.

**Examples**

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> EXCEPT ALL
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number);

/*--------+
 | number |
 +--------+
 | 3      |
 | 3      |
 | 4      |
 +--------*/
```

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> EXCEPT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number);

/*--------+
 | number |
 +--------+
 | 3      |
 | 4      |
 +--------*/
```

The following example shows multiple input queries to the right of the pipe
operator:

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> EXCEPT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number),
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 4]) AS number);

/*--------+
 | number |
 +--------+
 | 3      |
 +--------*/
```

The following example groups the set operations to modify the order of
operations. The first input query is used against the result of the last two
queries instead of the values of the last two queries individually.

```zetasql
SELECT * FROM UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|> EXCEPT DISTINCT
(
  SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number
  |> EXCEPT DISTINCT
      (SELECT * FROM UNNEST(ARRAY<INT64>[1, 4]) AS number)
);

/*--------+
 | number |
 +--------+
 | 1      |
 | 3      |
 | 4      |
 +--------*/
```

The following example uses the `BY NAME`
modifier to return unique rows from the input query to the left of the pipe
operator despite the differing column order in the input queries.

```zetasql
WITH
  NumbersTable AS (
    SELECT 1 AS one_digit, 10 AS two_digit
    UNION ALL
    SELECT 2, 20
    UNION ALL
    SELECT 3, 30
  )
SELECT one_digit, two_digit FROM NumbersTable
|> EXCEPT ALL BY NAME
    (SELECT 10 AS two_digit, 1 AS one_digit);

/*-----------+-----------+
 | one_digit | two_digit |
 +-----------+-----------+
 | 2         | 20        |
 | 3         | 30        |
 +-----------+-----------*/
```

Without the `BY NAME` modifier, the same columns in
differing order are considered different columns, so the query doesn't detect
any common rows that should be excluded.

```zetasql
WITH
  NumbersTable AS (
    SELECT 1 AS one_digit, 10 AS two_digit
    UNION ALL
    SELECT 2, 20
    UNION ALL
    SELECT 3, 30
  )
SELECT one_digit, two_digit FROM NumbersTable
|> EXCEPT ALL
    (SELECT 10 AS two_digit, 1 AS one_digit);

-- No values excluded because columns aren't recognized as the same.
/*-----------+-----------+
 | one_digit | two_digit |
 +-----------+-----------+
 | 1         | 10        |
 | 2         | 20        |
 | 3         | 30        |
 +-----------+-----------*/
```

[except-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#except

### `JOIN` pipe operator 
<a id="join_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> [join_type] JOIN from_item [[AS] alias] [{on_clause | using_clause}]
</pre>

**Description**

Joins rows from the input table with rows from a second table provided as an
argument. The `JOIN` operator behaves the same as the
[`JOIN` operation][join-operation] in standard syntax. The input table is the
left side of the join and the `JOIN` argument is the right side of the join.
Standard join inputs are supported, including tables, subqueries, `UNNEST`
operations, and table-valued function (TVF) calls. Standard join modifiers like
`LEFT`, `INNER`, and `CROSS` are allowed before the `JOIN` keyword.

An alias can be assigned to the input table on the right side of the join, but
not to the input table on the left side of the join. If an alias on the
input table is needed, perhaps to disambiguate columns in an
[`ON` expression][on-clause], then an alias can be added using the
[`AS` operator][as-pipe-operator] before the `JOIN` arguments.

**Example**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
)
|> AS produce_sales
|> LEFT JOIN
     (
       SELECT "apples" AS item, 123 AS id
     ) AS produce_data
   ON produce_sales.item = produce_data.item
|> SELECT produce_sales.item, sales, id;

/*---------+-------+------+
 | item    | sales | id   |
 +---------+-------+------+
 | apples  | 2     | 123  |
 | bananas | 5     | NULL |
 +---------+-------+------*/
```

[join-operation]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#join_types

[on-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#on_clause

[as-pipe-operator]: #as_pipe_operator

### `CALL` pipe operator 
<a id="call_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">CALL</span> table_function (argument [, ...]) [[AS] alias]
</pre>

**Description**

Calls a [table-valued function][tvf] (TVF), similar to
[table function calls][table-function-calls] in standard syntax.

TVFs in standard syntax can be called in the `FROM` clause or in a `JOIN`
operation. These are both allowed in pipe syntax as well.

In pipe syntax, TVFs that take a table argument can also be called with the
`CALL` operator. The first table argument comes from the input table and
must be omitted in the arguments. An optional table alias can be added for the
output table.

Multiple TVFs can be called sequentially without using nested subqueries.

**Examples**

Suppose you have TVFs with the following parameters:

+  `tvf1(inputTable1 ANY TABLE, arg1 ANY TYPE)` and
+  `tvf2(arg2 ANY TYPE, arg3 ANY TYPE, inputTable2 ANY TABLE)`.

The following examples compare calling both TVFs on an input table
by using standard syntax and by using the `CALL` pipe operator:

```zetasql
-- Call the TVFs without using the CALL operator.
SELECT *
FROM
  tvf2(arg2, arg3, TABLE tvf1(TABLE input_table, arg1));
```

```zetasql
-- Call the same TVFs with the CALL operator.
FROM input_table
|> CALL tvf1(arg1)
|> CALL tvf2(arg2, arg3);
```

[tvf]: https://github.com/google/zetasql/blob/master/docs/table-functions.md#tvfs

[table-function-calls]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#table_function_calls

<!-- disableFinding(LINK_ID) -->

### `WINDOW` pipe operator (DEPRECATED) 
<a id="window_pipe_operator"></a>

Warning: `WINDOW` pipe operator has been deprecated. Use
`EXTEND` pipe operator instead.

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">WINDOW</span> window_expression [[AS] alias] [, ...]
</pre>

**Description**

Adds a column with the result of computing the function over some window of
existing rows, similar to calling [window functions][window-functions] in a
`SELECT` list in standard syntax. Existing rows and columns are unchanged. The
window expression must include a window function with an
[`OVER` clause][over-clause].

Alternatively, you can use the [`EXTEND` operator][extend-pipe-operator] for
window functions.

**Example**

```zetasql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
  UNION ALL
  SELECT 'carrots' AS item, 8 AS sales
)
|> WINDOW SUM(sales) OVER() AS total_sales;

/*---------+-------+-------------+
 | item    | sales | total_sales |
 +---------+-------+-------------+
 | apples  | 2     | 15          |
 | bananas | 5     | 15          |
 | carrots | 8     | 15          |
 +---------+-------+-------------*/
```

[window-functions]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

[over-clause]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md#def_over_clause

[extend-pipe-operator]: #extend_pipe_operator

### `TABLESAMPLE` pipe operator 
<a id="tablesample_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> TABLESAMPLE sample_method (sample_size {PERCENT | ROWS}) [, ...]
</pre>

**Description**

Selects a random sample of rows from the input table. The `TABLESAMPLE` pipe
operator behaves the same as [`TABLESAMPLE` operator][tablesample-operator] in
standard syntax.

**Example**

The following example samples approximately 1% of data from a table called
`LargeTable`:

```zetasql
FROM LargeTable
|> TABLESAMPLE SYSTEM (1 PERCENT);
```

[tablesample-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#tablesample_operator

### `PIVOT` pipe operator 
<a id="pivot_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">PIVOT</span> (aggregate_expression FOR input_column IN (pivot_column [, ...])) [[AS] alias]
</pre>

**Description**

Rotates rows into columns. The `PIVOT` pipe operator behaves the same as the
[`PIVOT` operator][pivot-operator] in standard syntax.

**Example**

```zetasql
(
  SELECT "kale" AS product, 51 AS sales, "Q1" AS quarter
  UNION ALL
  SELECT "kale" AS product, 4 AS sales, "Q1" AS quarter
  UNION ALL
  SELECT "kale" AS product, 45 AS sales, "Q2" AS quarter
  UNION ALL
  SELECT "apple" AS product, 8 AS sales, "Q1" AS quarter
  UNION ALL
  SELECT "apple" AS product, 10 AS sales, "Q2" AS quarter
)
|> PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2'));

/*---------+----+------+
 | product | Q1 | Q2   |
 +---------+-----------+
 | kale    | 55 | 45   |
 | apple   | 8  | 10   |
 +---------+----+------*/
```

[pivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#pivot_operator

### `UNPIVOT` pipe operator 
<a id="unpivot_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> <span class="kwd">UNPIVOT</span> (values_column FOR name_column IN (column_to_unpivot [, ...])) [[AS] alias]
</pre>

**Description**

Rotates columns into rows. The `UNPIVOT` pipe operator behaves the same as the
[`UNPIVOT` operator][unpivot-operator] in standard syntax.

**Example**

```zetasql
(
  SELECT 'kale' as product, 55 AS Q1, 45 AS Q2
  UNION ALL
  SELECT 'apple', 8, 10
)
|> UNPIVOT(sales FOR quarter IN (Q1, Q2));

/*---------+-------+---------+
 | product | sales | quarter |
 +---------+-------+---------+
 | kale    | 55    | Q1      |
 | kale    | 45    | Q2      |
 | apple   | 8     | Q1      |
 | apple   | 10    | Q2      |
 +---------+-------+---------*/
```

[unpivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unpivot_operator

### `ASSERT` pipe operator 
<a id="assert_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> ASSERT expression [, payload_expression [, ...]]
</pre>

**Description**

Evaluates an expression over all rows of an input table to verify that the
expression is true or raise an assertion error if it's false.

The expression must evaluate to a boolean value. When the expression evaluates
to `TRUE`, the input table passes through the `ASSERT` operator unchanged. When
the expression evaluates to `FALSE` or `NULL`, the query fails with an
`Assertion failed` error.

One or more optional payload expressions can be provided. If the assertion
fails, the payload expression values are computed, converted to strings, and
included in the error message, separated by spaces.

If no payload is provided, the error message includes the SQL text of the
assertion expression.

The `ASSERT` operator has no equivalent operation in standard syntax.
 The [`ASSERT` statement][assert-statement] is
a related feature that verifies that a single expression is true.

**Example**

```zetasql
FROM table
|> ASSERT count != 0, "Count is zero for user", userId
|> SELECT total / count AS average
```

[assert-statement]: https://github.com/google/zetasql/blob/master/docs/debugging-statements.md#assert

[query-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md

[pipe-syntax-paper]: https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/

[from-queries]: #from_queries

[from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[using-aliases]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#using_aliases

[select-star]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_

[query-comparison]: #query_comparison

[value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

[select-as-value]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_as_value

