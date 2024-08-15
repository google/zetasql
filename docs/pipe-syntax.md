

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Pipe query syntax

ZetaSQL supports pipe query syntax, which is a simpler and more
concise alternative to [standard query syntax][query-syntax]. Pipe syntax
supports the same operators as standard syntax, but follows these rules.

Syntax:

*   Pipe syntax consists of a pipe and an angle bracket `|>`, an operator name,
    and any arguments: \
    `|> operator_name argument_list`
*   Pipe operators can be added on the end of any valid query.
*   Pipe operators can be applied in any order, any number of times.
*   Pipe syntax works anywhere standard syntax is supported, in queries, views,
    table-valued functions (TVFs), and other contexts.
*   Pipe syntax can be mixed with standard syntax in the same query. For
    example, subqueries can use different syntax from the parent query.

`FROM` queries:

*   A query can start with a [`FROM` clause][from-clause] and use any standard
    `FROM` syntax, including tables, joins, subqueries, table aliases with `AS`,
    and other usual elements.
*   Pipe operators can optionally be added after the `FROM` clause.

Semantics:

*   Each pipe operator is self-contained and can only reference columns
    available from its immediate input table. A pipe operator can't reference
    columns from earlier in the same query.
*   Each pipe operator produces a new result table as its output.

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

## Pipe operators

ZetaSQL supports the following pipe operators. Each operator
description includes a link to documentation for the corresponding operation in
standard syntax, where applicable. That documentation describes syntax features
and behavior in more detail. This pipe operator documentation highlights any
differences between the pipe syntax and the standard syntax for each operator.

### `SELECT`

<pre class="lang-sql prettyprint no-copy">
|> SELECT expression [[AS] alias] [, ...]
</pre>

**Description**

Produces a new table with exactly the listed columns, similar to the outermost
[`SELECT` clause][select-clause] in a table subquery in standard syntax.
Supports standard output modifiers like `SELECT AS STRUCT`, and supports
[window functions][window-functions]. Doesn't support aggregations or
`WITH ANONYMIZATION`.

In pipe syntax, using `SELECT` operators in a query is optional. `SELECT` can be
used near the end of a query to specify the list of output columns. The final
query result includes all columns from the output of the last pipe operator. If
`SELECT` isn't used, the output is similar to what would be produced by
[`SELECT *`][select-star].

For some operations normally done with `SELECT` in standard syntax, pipe syntax
supports other operators that might be more convenient. For example, aggregation
is always done with [`AGGREGATE`][aggregate-pipes-operator]. To compute new
columns, you can use [`EXTEND`][extend-pipes-operator]. To update specific
columns, you can use [`SET`][set-pipes-operator] or
[`DROP`][drop-pipes-operator].

**Example**

<pre class="lang-sql prettyprint">
|> SELECT account_id AS Account
</pre>

### `EXTEND`

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
-- Window function, with `OVER`
|> EXTEND SUM(val) OVER (ORDER BY k) AS val_over_k
</pre>

### `SET`

<pre class="lang-sql prettyprint no-copy">
|> SET column_name = expression [, ...]
</pre>

**Description**

Replaces the value of a column in the current table, similar to
[`SELECT * REPLACE (expression AS column)`][select-replace] in standard syntax.
Each reference column must exist exactly once in the input table. Table aliases
(like `t.x`) still point to the original column.

This pipe operator doesn't correspond to the [set operators][set-operators] in
standard syntax, which combine input query results.

**Example**

<pre class="lang-sql prettyprint">
|> SET x = 5, y = CAST(y AS INT32)
</pre>

### `DROP`

<pre class="lang-sql prettyprint no-copy">
|> DROP column_name [, ...]
</pre>

**Description**

Removes listed columns from the current table, similar to
[`SELECT * EXCEPT (column)`][select-except] in standard syntax. Each reference
column must exist at least once in the input table.

This pipe operator doesn't correspond to [`DROP` statements][drop-statement] in
data definition language (DDL), which delete persistent schema objects.

**Example**

<pre class="lang-sql prettyprint">
|> DROP account_id, user_id
</pre>

### `RENAME`

<pre class="lang-sql prettyprint no-copy">
|> RENAME old_column_name [AS] new_column_name [, ...]
</pre>

**Description**

Renames specified columns. Each old column name must exist exactly once in the
input table. The `RENAME` operator can't rename value table fields,
pseudo-columns, range variables, or objects that aren't columns on the pipe
input table.

**Example**

<pre class="lang-sql prettyprint">
|> RENAME last_name AS surname
</pre>

### `AS`

<pre class="lang-sql prettyprint no-copy">
|> AS alias
</pre>

**Description**

Introduces a table alias for the pipe input table, similar to applying
[`AS alias`][using-aliases] on a table subquery in standard syntax. Any existing
table aliases are removed and the new alias becomes the table alias for all
columns in the row.

This operator can be useful after operators like
[`SELECT`][select-pipes-operator], [`EXTEND`][extend-pipes-operator], or
[`AGGREGATE`][aggregate-pipes-operator] that add columns but can't give table
aliases to them.

**Example**

<pre class="lang-sql prettyprint">
|> SELECT x, y, z
|> AS table_alias
|> WHERE table_alias.y = 10
</pre>

### `WHERE`

<pre class="lang-sql prettyprint no-copy">
|> WHERE boolean_expression
</pre>

**Description**

Same behavior as the [`WHERE` clause][where-clause] in standard syntax: Filters
the results of the `FROM` clause.

In pipe syntax, `WHERE` also replaces the [`HAVING` clause][having-clause] and
[`QUALIFY` clause][qualify-clause]. For example, after performing aggregation
with [`AGGREGATE`][aggregate-pipes-operator], use `WHERE` instead of `HAVING`.
For [window functions][window-functions] inside a `QUALIFY` clause, use window
functions inside a `WHERE` clause instead.

**Example**

<pre class="lang-sql prettyprint">
|> WHERE assignee_user.email = 'username@email.com'
</pre>

### `LIMIT...OFFSET`

<pre class="lang-sql prettyprint no-copy">
|> LIMIT count [OFFSET skip_rows]
</pre>

**Description**

Same behavior as the [`LIMIT` and `OFFSET` clause][limit-offset-clause] in
standard syntax: Limits the number of rows to return in a query, and optionally
skips over rows. In pipe syntax, the `OFFSET` clause is used only with the
`LIMIT` clause.

**Example**

<pre class="lang-sql prettyprint">
|> LIMIT 10 OFFSET 2
</pre>

### `AGGREGATE...GROUP BY`

<pre class="lang-sql prettyprint no-copy">
-- Full-table aggregation
|> AGGREGATE aggregate_expression [[AS] alias] [, ...]
</pre>
<pre class="lang-sql prettyprint no-copy">
-- Aggregation with grouping
|> AGGREGATE [aggregate_expression [[AS] alias] [, ...]]
   GROUP BY groupable_items [[AS] alias] [, ...]
</pre>

**Description**

Performs aggregation on data across grouped rows or an entire table. The
`AGGREGATE` operator is similar to a query in standard syntax that contains a
[`GROUP BY` clause][group-by-clause] or a `SELECT` list with
[aggregate functions][aggregate-functions] or both.

Without the `GROUP BY` clause, the `AGGREGATE` operator performs full-table
aggregation and always produces exactly one output row.

With the `GROUP BY` clause, the `AGGREGATE` operator performs aggregation with
grouping, producing one row for each set of distinct values for the grouping
expressions.

The `AGGREGATE` list corresponds to the aggregated expressions in a `SELECT`
list in standard syntax. Each expression in the `AGGREGATE` list must include
aggregation. Aggregate expressions can also include scalar expressions (for
example, `sqrt(SUM(x*x))`). Aliases are allowed. Window functions aren't
allowed, but [`EXTEND`][extend-pipes-operator] can be used before `AGGREGATE` to
compute window functions.

The `GROUP BY` list corresponds to the `GROUP BY` clause in standard syntax.
Unlike in standard syntax, aliases can be assigned on `GROUP BY` items. Standard
modifiers like `GROUPING SETS`, `ROLLUP`, and `CUBE` are supported.

The output columns from `AGGREGATE` include all grouping columns first, followed
by all aggregate columns, using the aliases assigned in either list as the
column names.

Unlike in standard syntax, grouping expressions aren't repeated across `SELECT`
and `GROUP BY`. In pipe syntax, the grouping expressions are listed only once,
in `GROUP BY`, and are automatically included as output columns.

Because output columns are fully specified by this operator, `SELECT` isn't usually
needed after `AGGREGATE` to specify the columns to return.

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
-- Aggregation in pipe syntax
FROM table
|> AGGREGATE SUM(value) AS total
   GROUP BY id, EXTRACT(MONTH FROM date) AS month
</pre>

#### Shorthand syntax for `AGGREGATE` with `ORDER BY`

Pipe syntax supports the following additional shorthand syntax for the
[`ORDER BY`][order-by-pipes-operator] pipe operator as part of `AGGREGATE`
without repeating the column list:

<pre class="lang-sql prettyprint">
|> AGGREGATE [
     aggregate_expression [{ASC | DESC} {NULLS FIRST | NULLS LAST}] [[AS] alias]
     [, ...]]
   GROUP [AND ORDER] BY
     groupable_item [{ASC | DESC} {NULLS FIRST| NULLS LAST}] [[AS] alias]
     [, ...]
</pre>

The `GROUP AND ORDER BY` clause is equivalent to an `ORDER BY` on all groupable
items. By default, the groupable items are sorted in ascending order with null
values first. Ordering suffixes like `ASC`, `DESC`, `NULLS FIRST`, and
`NULLS LAST` can be used for other orders.

Without `GROUP AND ORDER BY`, the `ASC` or `DESC` suffixes can be added on
individual columns in the `GROUP BY` or `AGGREGATE` lists or both. The
`NULLS FIRST` or `NULLS LAST` suffixes can be used to further modify sorting.

If any of these ordering suffixes are present, the syntax behavior is equivalent
to adding an `ORDER BY` that includes all of the suffixed columns, with the
grouping columns first, matching the left-to-right output column order.

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

### `ORDER BY`

<pre class="lang-sql prettyprint no-copy">
|> ORDER BY expression [sort_options] [, ...]
</pre>

**Description**

Same behavior as the [`ORDER BY` clause][order-by-clause] in standard syntax:
Sorts results by a list of expressions. Suffixes like `ASC`, `DESC`, and
`NULLS LAST` are supported for customizing the ordering for each expression.

For queries with [`AGGREGATE...GROUP BY`][aggregate-pipes-operator] and
`ORDER BY` together, pipe syntax also supports a `GROUP AND ORDER BY` clause and
other [shorthand ordering suffixes][order-by-with-aggregate] for aggregation.

**Example**

<pre class="lang-sql prettyprint">
|> ORDER BY last_name DESC
</pre>

### `JOIN`

<pre class="lang-sql prettyprint no-copy">
|> [join_type] JOIN from_item [[AS] alias] [on_clause | using_clause]
</pre>

**Description**

Same behavior as the [`JOIN` operation][join-operation] in standard syntax:
Merges two items so that they can be queried as one source. The join operation
treats the pipe input table as the left side of the join and the `JOIN` argument
as the right side of the join. Standard join inputs are supported, including
tables, subqueries, `UNNEST`, and table-valued function (TVF) calls. Standard
join modifiers like `LEFT`, `INNER`, and `CROSS` are allowed before `JOIN`.

An alias can be assigned to the input item on the right side of the join, but
not to the pipe input table on the left side of the join. If an alias on the
pipe input table is needed, perhaps to disambiguate columns in an
[`ON` expression][on-clause], then an alias can be added using
[`AS`][as-pipes-operator] before the `JOIN`.

**Example**

<pre class="lang-sql prettyprint">
|> JOIN ticketing_system_table AS components
     ON bug_table.component_id = CAST(components.component_id AS int64)
</pre>

### `CALL`

<pre class="lang-sql prettyprint no-copy">
|> CALL table_function (argument [, ...]) [[AS] alias]
</pre>

**Description**

Calls a [table-valued function][tvf] (TVF), similar to [table function
calls][table-function-calls] in standard syntax. TVFs in standard syntax can be
called in the `FROM` clause or in a `JOIN` operation. These are both allowed in
pipe syntax as well.

In pipe syntax, TVFs that take a table argument can also be called with the
`CALL` operator. The first table argument comes from the pipe input table and
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

### `WINDOW`

<pre class="lang-sql prettyprint no-copy">
|> WINDOW window_expression [[AS] alias] [, ...]
</pre>

**Description**

Adds a column with the result of computing the function over some window of
existing rows, similar to calling [window functions][window-functions] in a
`SELECT` list in standard syntax. Existing rows and columns are unchanged. The
window expression must include a window function with an
[`OVER` clause][over-clause].

The [`EXTEND`][extend-pipes-operator] pipe operator also supports window
expressions. Using `EXTEND` with window functions is usually preferred instead
of `WINDOW`.

**Example**

<pre class="lang-sql prettyprint">
|> WINDOW SUM(val) OVER (ORDER BY k)
</pre>

### `TABLESAMPLE`

<pre class="lang-sql prettyprint no-copy">
|> TABLESAMPLE sample_method (sample_size {PERCENT | ROWS}) [, ...]
</pre>

**Description**

Same behavior as the [`TABLESAMPLE` operator][tablesample-operator] in standard
syntax: Selects a random sample of rows from the input table.

**Example**

<pre class="lang-sql prettyprint">
|> TABLESAMPLE BERNOULLI (0.1 PERCENT)
</pre>

### `PIVOT`

<pre class="lang-sql prettyprint no-copy">
|> PIVOT (aggregate_expression FOR input_column IN (pivot_column [, ...])) [[AS] alias]
</pre>

**Description**

Same behavior as the [`PIVOT` operator][pivot-operator] in standard syntax:
Rotates rows into columns.

**Example**

<pre class="lang-sql prettyprint">
|> SELECT year, username, num_users
|> PIVOT (SUM(num_users) FOR username IN ('Jeff', 'Jeffrey', 'Jeffery'))
</pre>

### `UNPIVOT`

<pre class="lang-sql prettyprint no-copy">
|> UNPIVOT (values_column FOR name_column IN (column_to_unpivot [, ...])) [[AS] alias]
</pre>

**Description**

Same behavior as the [`UNPIVOT` operator][unpivot-operator] in standard syntax:
Rotates columns into rows.

**Example**

<pre class="lang-sql prettyprint">
|> UNPIVOT (count FOR user_location IN (London, Bangalore, Madrid))
|> ORDER BY year, cnt
</pre>

### `ASSERT`

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
a related feature that verifies that a single expression
is true.

**Example**

<pre class="lang-sql prettyprint">
FROM table
|> ASSERT count != 0, "Count is zero for user", userId
|> SELECT total / count AS average
</pre>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[query-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md

[from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[select-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_list

[select-star]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_

[select-replace]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_replace

[select-except]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_except

[set-operators]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#set_operators

[drop-statement]: https://github.com/google/zetasql/blob/master/docs/data-definition-language.md#drop

[where-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#where_clause

[aggregate-pipes-operator]: #aggregategroup_by

[extend-pipes-operator]: #extend

[select-pipes-operator]: #select

[set-pipes-operator]: #set

[drop-pipes-operator]: #drop

[as-pipes-operator]: #as

[order-by-pipes-operator]: #order_by

[order-by-with-aggregate]: #shorthand_syntax_for_aggregate_with_order_by

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

[window-pipes-operator]: #window

[table-function-calls]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#table-function-calls

[tvf]: https://github.com/google/zetasql/blob/master/docs/table-functions.md#tvfs

[tablesample-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#tablesample_operator

[using-aliases]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#using_aliases

[pivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#pivot_operator

[unpivot-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unpivot_operator

[assert-statement]: https://github.com/google/zetasql/blob/master/docs/debugging-statements.md#assert

<!-- mdlint on -->

