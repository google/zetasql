

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Pipe query syntax

ZetaSQL supports pipe query syntax, which is a simpler and more
concise alternative to [standard query syntax][query-syntax]. Pipe syntax
supports many of the same operators as standard syntax, and improves some areas
of SQL query functionality.

## Pipe syntax 
<a id="pipe_syntax"></a>

Pipe syntax has the following key characteristics:

+   Pipe syntax consists of a pipe and an angle bracket `|>`, an operator name,
    and any arguments: \
    `|> operator_name argument_list`
+   Pipe operators can be added to the end of any valid query.
+   Pipe operators can be applied in any order, any number of times.
+   Pipe syntax works anywhere standard syntax is supported: in queries, views,
    table-valued functions (TVFs), and other contexts.
+   Pipe syntax can be mixed with standard syntax in the same query. For
    example, subqueries can use different syntax from the parent query.
+   A query can [start with a `FROM` clause][from-queries], and pipe
    operators can optionally be added after the `FROM` clause.

Compare the following equivalent queries that count open tickets
assigned to a user:

**Standard syntax**

```sql
SELECT component_id, COUNT(*)
FROM ticketing_system_table
WHERE
  assignee_user.email = 'username@email.com'
  AND status IN ('NEW', 'ASSIGNED', 'ACCEPTED')
GROUP BY component_id
ORDER BY component_id DESC;
```

**Pipe syntax**

```sql
FROM ticketing_system_table
|> WHERE
    assignee_user.email = 'username@email.com'
    AND status IN ('NEW', 'ASSIGNED', 'ACCEPTED')
|> AGGREGATE COUNT(*)
   GROUP AND ORDER BY component_id DESC;
```

## Pipe operator semantics 
<a id="pipe_semantics"></a>

Pipe operators have the following semantic behavior:

+   Each pipe operator performs a self-contained operation.
+   A pipe operator consumes the input table passed to it through the pipe
    character and produces a new table as output.
+   A pipe operator can reference only columns from its immediate input table.
    Columns from earlier in the same query aren't visible. Inside subqueries,
    correlated references to outer columns are still allowed.

## `FROM` queries 
<a id="from_queries"></a>

In pipe syntax, a query can start with a standard [`FROM` clause][from-clause]
and use any standard `FROM` syntax, including tables, joins, subqueries,
`UNNEST` operations, and table-valued functions (TVFs). Table aliases can be
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

<pre class="lang-sql prettyprint">
-- Return a table row that matches a condition.
FROM table_name
|> WHERE value_column IS NULL
|> LIMIT 1;
</pre>

<pre class="lang-sql prettyprint">
-- Join tables in the FROM clause and then apply pipe operators.
FROM Table1 AS t1 JOIN Table2 AS t2 USING (key)
|> AGGREGATE SUM(t2.value)
   GROUP BY t1.key;
</pre>

## Pipe operators

ZetaSQL supports the following pipe operators. For operators that
correspond or relate to similar operations in standard syntax, the operator
descriptions highlight similarities and differences and link to more detailed
documentation on the corresponding syntax.

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

<pre class="lang-sql prettyprint">
|> SELECT account_id AS Account
</pre>

### `EXTEND` pipe operator 
<a id="extend_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> EXTEND expression [[AS] alias] [, ...]
</pre>

**Description**

Propagates the existing table and adds a computed column, similar to
[`SELECT *, new_column`][select-star] in standard syntax. Supports
[window functions][window-functions].

**Examples**

<pre class="lang-sql prettyprint">
|> EXTEND status IN ('NEW', 'ASSIGNED', 'ACCEPTED') AS is_open
</pre>

<pre class="lang-sql prettyprint">
-- Window function, with OVER
|> EXTEND SUM(val) OVER (ORDER BY k) AS val_over_k
</pre>

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

<pre class="lang-sql prettyprint">
|> SET x = 5, y = CAST(y AS INT32)
</pre>

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

<pre class="lang-sql prettyprint">
|> DROP account_id, user_id
</pre>

### `RENAME` pipe operator 
<a id="rename_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> RENAME old_column_name [AS] new_column_name [, ...]
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

<pre class="lang-sql prettyprint">
|> RENAME last_name AS surname
</pre>

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
aliases to them.

**Example**

<pre class="lang-sql prettyprint">
|> SELECT x, y, z
|> AS table_alias
|> WHERE table_alias.y = 10
</pre>

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

<pre class="lang-sql prettyprint">
|> WHERE assignee_user.email = 'username@email.com'
</pre>

### `LIMIT` pipe operator 
<a id="limit_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> LIMIT count [OFFSET skip_rows]
</pre>

**Description**

Limits the number of rows to return in a query, with an optional `OFFSET` clause
to skip over rows. The `LIMIT` operator behaves the same as the
[`LIMIT` and `OFFSET` clause][limit-offset-clause] in standard syntax.

**Examples**

<pre class="lang-sql prettyprint">
|> LIMIT 10
</pre>

<pre class="lang-sql prettyprint">
|> LIMIT 10 OFFSET 2
</pre>

### `AGGREGATE` pipe operator 
<a id="aggregate_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
-- Full-table aggregation
|> AGGREGATE aggregate_expression [[AS] alias] [, ...]
</pre>
<pre class="lang-sql prettyprint no-copy">
-- Aggregation with grouping
|> AGGREGATE [aggregate_expression [[AS] alias] [, ...]]
   GROUP BY groupable_items [[AS] alias] [, ...]
</pre>
<pre class="lang-sql prettyprint">
-- Aggregation with grouping and shorthand ordering syntax
|> AGGREGATE [aggregate_expression [<span class="var">order_suffix</span>] [[AS] alias] [, ...]]
   GROUP [AND ORDER] BY groupable_item [<span class="var">order_suffix</span>] [[AS] alias] [, ...]

<span class="var">order_suffix</span>: {ASC | DESC} [{NULLS FIRST | NULLS LAST}]
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

**Examples**

<pre class="lang-sql prettyprint">
-- Full-table aggregation
|> AGGREGATE COUNT(*) AS row_count, SUM(num_users) AS total_users
</pre>
<pre class="lang-sql prettyprint">
-- Aggregation with grouping
|> AGGREGATE COUNT(*) AS row_count, SUM(num_users) AS total_users,
   GROUP BY org_site, date
</pre>

The following examples compare aggregation in standard syntax and in pipe
syntax:

<pre class="lang-sql prettyprint">
-- Aggregation in standard syntax
SELECT id, EXTRACT(MONTH FROM date) AS month, SUM(value) AS total
FROM table
GROUP BY id, month
</pre>

<pre class="lang-sql prettyprint">
-- The same aggregation in pipe syntax
FROM table
|> AGGREGATE SUM(value) AS total
   GROUP BY id, EXTRACT(MONTH FROM date) AS month
</pre>

#### Shorthand ordering syntax with `AGGREGATE` 
<a id="shorthand_order_pipe_syntax"></a>

The `AGGREGATE` operator supports a shorthand ordering syntax, which is
equivalent to applying the [`ORDER BY` operator][order-by-pipe-operator] as part
of the `AGGREGATE` operator without repeating the column list:

<pre class="lang-sql prettyprint">
-- Aggregation with grouping and shorthand ordering syntax
|> AGGREGATE [aggregate_expression [<span class="var">order_suffix</span>] [[AS] alias] [, ...]]
   GROUP [AND ORDER] BY groupable_item [<span class="var">order_suffix</span>] [[AS] alias] [, ...]

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

<pre class="lang-sql prettyprint">
-- Order by all grouping columns.
|> AGGREGATE COUNT(*)
   GROUP AND ORDER BY first_name, last_name DESC
</pre>

The ordering in the previous example is equivalent to using
`|> ORDER BY first_name, last_name DESC`.

<pre class="lang-sql prettyprint">
-- Order by specified grouping and aggregate columns.
|> AGGREGATE COUNT(*) DESC
   GROUP BY first_name, last_name ASC
</pre>

The ordering in the previous example is equivalent to using
`|> ORDER BY last_name ASC, COUNT(*) DESC`.

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

<pre class="lang-sql prettyprint">
|> ORDER BY last_name DESC
</pre>

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

<pre class="lang-sql prettyprint">
|> JOIN ticketing_system_table AS components
     ON bug_table.component_id = CAST(components.component_id AS int64)
</pre>

### `CALL` pipe operator 
<a id="call_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> CALL table_function (argument [, ...]) [[AS] alias]
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

<pre class="lang-sql prettyprint">
|> CALL AddSuffix('*')
|> CALL AddSuffix2(arg1, arg2, arg3)
</pre>

The following examples compare a TVF call in standard syntax and in pipe syntax:

<pre class="lang-sql prettyprint">
-- Call a TVF in standard syntax.
FROM tvf( (SELECT * FROM table), arg1, arg2 )
</pre>

<pre class="lang-sql prettyprint">
-- Call the same TVF in pipe syntax.
SELECT * FROM table
|> CALL tvf(arg1, arg2)
</pre>

### `WINDOW` pipe operator 
<a id="window_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> WINDOW window_expression [[AS] alias] [, ...]
</pre>

**Description**

Adds a column with the result of computing the function over some window of
existing rows, similar to calling [window functions][window-functions] in a
`SELECT` list in standard syntax. Existing rows and columns are unchanged. The
window expression must include a window function with an
[`OVER` clause][over-clause].

The [`EXTEND` operator][extend-pipe-operator] is recommended for window
functions instead of the `WINDOW` operator because it also supports window
expressions and covers the same use cases.

**Example**

<pre class="lang-sql prettyprint">
|> WINDOW SUM(val) OVER (ORDER BY k)
</pre>

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

<pre class="lang-sql prettyprint">
|> TABLESAMPLE BERNOULLI (0.1 PERCENT)
</pre>

### `PIVOT` pipe operator 
<a id="pivot_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> PIVOT (aggregate_expression FOR input_column IN (pivot_column [, ...])) [[AS] alias]
</pre>

**Description**

Rotates rows into columns. The `PIVOT` pipe operator behaves the same as the
[`PIVOT` operator][pivot-operator] in standard syntax.

**Example**

<pre class="lang-sql prettyprint">
|> SELECT year, username, num_users
|> PIVOT (SUM(num_users) FOR username IN ('Jeff', 'Jeffrey', 'Jeffery'))
</pre>

### `UNPIVOT` pipe operator 
<a id="unpivot_pipe_operator"></a>

<pre class="lang-sql prettyprint no-copy">
|> UNPIVOT (values_column FOR name_column IN (column_to_unpivot [, ...])) [[AS] alias]
</pre>

**Description**

Rotates columns into rows. The `UNPIVOT` pipe operator behaves the same as the
[`UNPIVOT` operator][unpivot-operator] in standard syntax.

**Example**

<pre class="lang-sql prettyprint">
|> UNPIVOT (count FOR user_location IN (London, Bangalore, Madrid))
|> ORDER BY year, cnt
</pre>

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

<pre class="lang-sql prettyprint">
FROM table
|> ASSERT count != 0, "Count is zero for user", userId
|> SELECT total / count AS average
</pre>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[query-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md

[from-queries]: #from_queries

[from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[select-as-value]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_as_value

[select-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_list

[select-star]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_

[select-replace]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_replace

[select-except]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_except

[set-operators]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#set_operators

[drop-statement]: https://github.com/google/zetasql/blob/master/docs/data-definition-language.md#drop

[where-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#where_clause

[aggregate-pipe-operator]: #aggregate_pipe_operator

[extend-pipe-operator]: #extend_pipe_operator

[select-pipe-operator]: #select_pipe_operator

[set-pipe-operator]: #set_pipe_operator

[drop-pipe-operator]: #drop_pipe_operator

[rename-pipe-operator]: #rename_pipe_operator

[as-pipe-operator]: #as_pipe_operator

[order-by-pipe-operator]: #order_by_pipe_operator

[shorthand-order-pipe-syntax]: #shorthand_order_pipe_syntax

[limit-offset-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_and_offset_clause

[having-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#having_clause

[qualify-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#qualify_clause

[aggregate-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

[group-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[order-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#order_by_clause

[join-operation]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#join_types

[on-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#on_clause

[window-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#window_clause

[window-functions]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

[over-clause]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md#def_over_clause

[window-pipe-operator]: #window_pipe_operator

[table-function-calls]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#table-function-calls

[tvf]: https://github.com/google/zetasql/blob/master/docs/table-functions.md#tvfs

[tablesample-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#tablesample_operator

[using-aliases]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#using_aliases

[pivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#pivot_operator

[unpivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unpivot_operator

[assert-statement]: https://github.com/google/zetasql/blob/master/docs/debugging-statements.md#assert

[value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

<!-- mdlint on -->

