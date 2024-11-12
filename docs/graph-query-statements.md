

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# GQL query statements

Graph Query Language (GQL) lets you execute multiple linear
graph queries in one query. Each linear graph query generates results
(the working table) and then passes those results to the next.

GQL supports the following building blocks, which can be composited into a
GQL query based on the [syntax rules][gql_syntax].

## Language list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#gql_syntax">GQL syntax</a>
</td>
  <td>Creates a graph query with the GQL syntax.</td>
</tr>

<tr>
  <td><a href="https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#graph_query"><code>GRAPH</code> clause</a>
</td>
  <td>
    Specifies a property graph to query.
  </td>
</tr>

<tr>
  <td><a href="#gql_filter"><code>FILTER</code> statement</a>
</td>
  <td>
    Filters out rows in the query results that do not satisfy a specified
    condition.
  </td>
</tr>

<tr>
  <td><a href="#gql_for"><code>FOR</code> statement</a>
</td>
  <td>
    Unnests an <code>ARRAY</code>-typed expression.
  </td>
</tr>

<tr>
  <td><a href="#gql_let"><code>LET</code> statement</a>
</td>
  <td>
    Defines variables and assigns values for later use in the current linear
    query statement.
  </td>
</tr>

<tr>
  <td><a href="#gql_limit"><code>LIMIT</code> statement</a>
</td>
  <td>Limits the number of query results.</td>
</tr>

<tr>
  <td><a href="#gql_match"><code>MATCH</code> statement</a>
</td>
  <td>Matches data described by a graph pattern.</td>
</tr>

<tr>
  <td><a href="#gql_next"><code>NEXT</code> statement</a>
</td>
  <td>Chains multiple linear query statements together.</td>
</tr>

<tr>
  <td><a href="#gql_offset"><code>OFFSET</code> statement</a>
</td>
  <td>Skips a specified number of rows in the query results.</td>
</tr>

<tr>
  <td><a href="#gql_order_by"><code>ORDER BY</code> statement</a>
</td>
  <td>Orders the query results.</td>
</tr>

<tr>
  <td><a href="#gql_return"><code>RETURN</code> statement</a>
</td>
  <td>Marks the end of a linear query statement and returns the results.</td>
</tr>

<tr>
  <td><a href="#gql_skip"><code>SKIP</code> statement</a>
</td>
  <td>Synonym for the <code>OFFSET</code> statement.</td>
</tr>

<tr>
  <td><a href="#gql_with"><code>WITH</code> statement</a>
</td>
  <td>
    Passes on the specified columns, optionally filtering, renaming, and
    transforming those results.
  </td>
</tr>

<tr>
  <td><a href="#gql_set">Set operation</a>
</td>
  <td>Combines a sequence of linear query statements with a set operation.</td>
</tr>

  </tbody>
</table>

## GQL syntax
<a id="gql_syntax"></a>

<pre>
<span class="var">graph_query</span>:
  <span class="var">GRAPH clause</span>
  <span class="var">multi_linear_query_statement</span>

<span class="var">multi_linear_query_statement</span>:
  <span class="var">linear_query_statement</span>
  [
    NEXT
    <span class="var">linear_query_statement</span>
  ]
  [...]

<span class="var">linear_query_statement</span>:
  {
    <span class="var">simple_linear_query_statement</span>
    | <span class="var">composite_linear_query_statement</span>
  }

<span class="var">composite_linear_query_statement</span>:
  <span class="var">simple_linear_query_statement</span>
  <span class="var">set_operator</span> <span class="var">simple_linear_query_statement</span>
  [...]

<span class="var">simple_linear_query_statement</span>:
  <span class="var">primitive_query_statement</span>
  [...]
</pre>

#### Description

Creates a graph query with the GQL syntax. The syntax rules define how
to composite the building blocks of GQL into a query.

#### Definitions

+   `primitive_query_statement`: A statement in [Query statements][language-list]
    except for the `NEXT` statement.
+   `simple_linear_query_statement`: A list of `primitive_query_statement`s that
    ends with a [`RETURN` statement][return].
+   `composite_linear_query_statement`: A list of
    `simple_linear_query_statement`s composited with the [set operators][set-op].
+   `linear_query_statement`: A statement that is either a
    `simple_linear_query_statement` or a `composite_linear_query_statement`.
+   `multi_linear_query_statement`: A list of `linear_query_statement`s chained
    together with the [`NEXT` statement][next].
+   `graph_query`: A GQL query that starts with a [`GRAPH` clause][graph-clause],
    then follows with a `multi_linear_query_statement`.

[language-list]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#language-list

[graph-clause]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#graph_query

[set-op]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_set

[return]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_return

[next]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_next

[graph-clause]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#graph_query

## `GRAPH` clause 
<a id="graph_query"></a>

<pre>
GRAPH <span class="var">property_graph_name</span>
<span class="var">multi_linear_query_statement</span>
</pre>

#### Description

Specifies a property graph to query. This clause must be added before the first
linear query statement in a graph query.

#### Definitions

+ `property_graph_name`: The name of the property graph to query.
+ `multi_linear_query_statement`: A multi linear query statement. For more
  information, see `multi_linear_query_statement` in [GQL syntax][gql_syntax].

#### Examples

The following example queries the [`FinGraph`][fin-graph] property graph to find
accounts with incoming transfers and looks up their owners:

```sql
GRAPH FinGraph
MATCH (:Account)-[:Transfers]->(account:Account)
RETURN account, COUNT(*) AS num_incoming_transfers
GROUP BY account

NEXT

MATCH (account:Account)<-[:Owns]-(owner:Person)
RETURN
  account.id AS account_id, owner.name AS owner_name,
  num_incoming_transfers

/*--------------------------------------------------+
 | account_id | owner_name | num_incoming_transfers |
 +--------------------------------------------------+
 | 7          | Alex       | 1                      |
 | 20         | Dana       | 1                      |
 | 6          | Lee        | 3                      |
 +--------------------------------------------------*/
```

[graph-query-statements]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md

[gql_syntax]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_syntax

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

[next]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_next

## `FILTER` statement 
<a id="gql_filter"></a>

<pre>
FILTER [ WHERE ] <span class="var">bool_expression</span>
</pre>

#### Description

Filters out rows in the query results that do not satisfy a specified condition.

#### Definitions

+   `bool_expression`: A boolean expression. Only rows whose `bool_expression`
    evaluates to `TRUE` are included. Rows whose `bool_expression` evaluates to
    `NULL` or `FALSE` are discarded.

#### Details

The `FILTER` statement can reference columns in the working table.

The syntax for the `FILTER` statement is similar to the syntax for the
[graph pattern `WHERE` clause][graph-pattern-definition], but they are evaluated
differently. The `FILTER` statement is evaluated after the previous statement.
The `WHERE` clause is evaluated as part of the containing statement.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

In the following query, only people who were born before `1990-01-10`
are included in the results table:

```sql
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FILTER p.birthday < '1990-01-10'
RETURN p.name

/*------+
 | name |
 +------+
 | Dana |
 | Lee  |
 +------*/
```

`WHERE` is an optional keyword that you can include in a `FILTER` statement.
The following query is semantically identical to the previous query:

```sql
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FILTER WHERE p.birthday < '1990-01-10'
RETURN p.name

/*------+
 | name |
 +------+
 | Dana |
 | Lee  |
 +------*/
```

In the following example, `FILTER` follows an aggregation step with
grouping. Semantically, it's similar to the `HAVING` clause in SQL:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(dest:Account)
RETURN source, dest, SUM(e.amount) AS total_amount
GROUP BY source, dest

NEXT

FILTER WHERE total_amount < 400
RETURN source.id AS source_id, dest.id AS destination_id, total_amount

/*-------------------------------------------+
 | source_id | destination_id | total_amount |
 +-------------------------------------------+
 | 16        | 20             | 300          |
 | 20        | 16             | 200          |
 +-------------------------------------------*/
```

In the following example, an error is produced because `FILTER` references
`m`, which is not in the working table:

```sql {.bad}
-- Error: m does not exist
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FILTER WHERE m.birthday < '1990-01-10'
RETURN p.name
```

In the following example, an error is produced because even though `p` is in the
working table, `p` does not have a property called `date_of_birth`:

```sql {.bad}
-- ERROR: date_of_birth is not a property of p
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FILTER WHERE p.date_of_birth < '1990-01-10'
RETURN p.name
```

[where-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#where_clause

[having-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#having_clause

[graph-pattern-definition]: https://github.com/google/zetasql/blob/master/docs/graph-patterns.md#graph_pattern_definition

[horizontal-aggregation]: https://github.com/google/zetasql/blob/master/docs/graph-gql-functions.md

## `FOR` statement 
<a id="gql_for"></a>

<pre>
FOR <span class="var">element_name</span> IN <span class="var">array_expression</span>
  [ <span class="var">with_offset_clause</span> ]

<span class="var">with_offset_clause</span>:
  WITH OFFSET [ AS <span class="var">offset_name</span> ]
</pre>

#### Description

Unnests an `ARRAY`-typed expression and joins the result with the current working table.

#### Definitions

+   `array_expression`: An `ARRAY`-typed expression.
+   `element_name`: The name of the element column. The name can't be the name
    of a column that already exists in the current linear query statement.
+   `offset_name`: The name of the offset column. The name can't be the name of
    a column that already exists in the current linear query statement. If not
    specified, the default is `offset`.

#### Details

The `FOR` statement expands the working table by defining a new column for the
elements of `array_expression`, with an optional offset column. The cardinality
of the working table might change as a result.

The `FOR` statement can reference columns in the working table.

The `FOR` statement evaluation is similar to the [`UNNEST`][unnest-operator] operator.

The `FOR` statement does not preserve order.

And empty or `NULL` `array_expression` produces zero rows.

The keyword `WITH` following the `FOR` statement is always interpreted as the
beginning of `with_offset_clause`. If you want to use the `WITH` statement
following the `FOR` statement, you should fully qualify the `FOR` statement with
`with_offset_clause`, or use the `RETURN` statement instead of the `WITH`
statement.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

In the following query, there are three rows in the working table prior to the
`FOR` statement. After the `FOR` statement, each row is expanded into two rows,
one per `element` value from the array.

```sql
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FOR element in ["all","some"] WITH OFFSET
RETURN p.name, element as alert_type, offset
ORDER BY p.name, element, offset

/*----------------------------+
 | name | alert_type | offset |
 +----------------------------+
 | Alex | all        | 0      |
 | Alex | some       | 1      |
 | Dana | all        | 0      |
 | Dana | some       | 1      |
 | Lee  | all        | 0      |
 | Lee  | some       | 1      |
 +----------------------------*/
```

In the following query, there are two rows in the working table prior to the
`FOR` statement. After the `FOR` statement, each row is expanded into a
different number of rows, based on the value of `array_expression` for that row.

```sql
GRAPH FinGraph
MATCH (p:Person)-[o:Owns]->(a:Account)
FILTER p.name != "Alex"
FOR element in GENERATE_ARRAY(1,LENGTH(p.name))
RETURN p.name, element
ORDER BY p.name, element

/*----------------+
 | name | element |
 +----------------+
 | Dana | 1       |
 | Dana | 2       |
 | Dana | 3       |
 | Dana | 4       |
 | Lee  | 1       |
 | Lee  | 2       |
 | Lee  | 3       |
 +----------------*/
```

In the following query, there are three rows in the working table prior to the
`FOR` statement. After the `FOR` statement, no row is produced because
`array_expression` is an empty array.

```sql
-- No rows produced
GRAPH FinGraph
MATCH (p:Person)
FOR element in [] WITH OFFSET AS off
RETURN p.name, element, off
```

In the following query, there are three rows in the working table prior to the
`FOR` statement. After the `FOR` statement, no row is produced because
`array_expression` is a `NULL` array.

```sql
-- No rows produced
GRAPH FinGraph
MATCH (p:Person)
FOR element in CAST(NULL AS ARRAY<STRING>) WITH OFFSET
RETURN p.name, element, offset
```

In the following example, an error is produced because `WITH` is used directly
After the `FOR` statement. The query can be fixed by adding `WITH OFFSET` after
the `FOR` statement, or by using `RETURN` directly instead of `WITH`.

```sql {.bad}
-- Error: Expected keyword OFFSET but got identifier "element"
GRAPH FinGraph
FOR element in [1,2,3]
WITH element as col
RETURN col
ORDER BY col
```

```sql
GRAPH FinGraph
FOR element in [1,2,3] WITH OFFSET
WITH element as col
RETURN col
ORDER BY col

/*-----+
 | col |
 +-----+
 | 1   |
 | 2   |
 | 3   |
 +-----*/
```

[unnest-operator]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unnest_operator

## `LET` statement 
<a id="gql_let"></a>

<pre>
LET <span class="var">linear_graph_variable</span>[, ...]

<span class="var">linear_graph_variable</span>:
  <span class="var">variable_name</span> = <span class="var">value</span>
</pre>

#### Description

Defines variables and assigns values to them for later use in the current
linear query statement.

#### Definitions

+   `linear_graph_variable`: The variable to define.
+   `variable_name`: The name of the variable.
+   `value`: A scalar expression that represents the value of the variable.
    The names referenced by this expression must be in the incoming working
    table.

#### Details

`LET` does not change the cardinality of the working table nor modify its
existing columns.

The variable can only be used in the current linear query statement. To use it
in a following linear query statement, you must include it in the `RETURN`
statement as a column.

You can't define and reference a variable within the same `LET` statement.

You can't redefine a variable with the same name.

You can use horizontal aggregate functions in this statement. To learn more, see
[Horizontal aggregate function calls in GQL][horizontal-aggregation].

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

In the following graph query, the variable `a` is defined and then referenced
later:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
RETURN a.id AS a_id

/*------+
 | a_id |
 +------+
 | 20   |
 | 7    |
 | 7    |
 | 20   |
 | 16   |
 +------*/
```

The following `LET` statement in the second linear query statement is valid
because `a` is defined and returned from the first linear query statement:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
RETURN a

NEXT

LET b = a -- Valid: 'a' is defined and returned from the linear query statement above.
RETURN b.id AS b_id

/*------+
 | b_id |
 +------+
 | 20   |
 | 7    |
 | 7    |
 | 20   |
 | 16   |
 +------*/
```

The following `LET` statement in the second linear query statement is invalid
because `a` is not returned from the first linear query statement.

```sql {.bad}
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
RETURN source.id

NEXT

LET b = a  -- ERROR: 'a' does not exist.
RETURN b.id AS b_id
```

The following `LET` statement is invalid because `a` is defined and then
referenced in the same `LET` statement:

```sql {.bad}
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source, b = a -- ERROR: Can't define and reference 'a' in the same operation.
RETURN a
```

The following `LET` statement is valid because `a` is defined first and then
referenced afterwards:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
LET b = a
RETURN b.id AS b_id

/*------+
 | b_id |
 +------+
 | 20   |
 | 7    |
 | 7    |
 | 20   |
 | 16   |
 +------*/
```

In the following examples, the `LET` statements are invalid because `a` is
redefined:

```sql {.bad}
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source, a = destination -- ERROR: 'a' has already been defined.
RETURN a.id AS a_id
```

```sql {.bad}
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
LET a = destination -- ERROR: 'a' has already been defined.
RETURN a.id AS a_id
```

In the following examples, the `LET` statements are invalid because `b` is
redefined:

```sql {.bad}
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
LET b = destination
RETURN a, b

NEXT

MATCH (a)
LET b = a -- ERROR: 'b' has already been defined.
RETURN b.id
```

The following `LET` statement is valid because although `b` is defined in the
first linear query statement, it's not passed to the second linear query
statement:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
LET a = source
LET b = destination
RETURN a

NEXT

MATCH (a)
LET b = a
RETURN b.id

/*------+
 | b_id |
 +------+
 | 20   |
 | 7    |
 | 7    |
 | 20   |
 | 16   |
 +------*/
```

[horizontal-aggregation]: https://github.com/google/zetasql/blob/master/docs/graph-gql-functions.md

## `LIMIT` statement 
<a id="gql_limit"></a>

<pre>
LIMIT <span class="var">count</span>
</pre>

#### Description

Limits the number of query results.

#### Definitions

+   `count`: A non-negative `INT64` value that represents the number of
    results to produce. For more information,
    see the [`LIMIT` and `OFFSET` clauses][limit-and-offset-clause].

#### Details

The `LIMIT` statement can appear before the `RETURN` statement. You can also use
it as a qualifying clause in the [`RETURN` statement][gql-return].

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following example uses the `LIMIT` statement to limit the query results to
three rows:

```sql
GRAPH FinGraph
MATCH (source:Account)-[e:Transfers]->(destination:Account)
ORDER BY source.nick_name
LIMIT 3
RETURN source.nick_name

/*----------------+
 | nick_name      |
 +----------------+
 | Rainy day fund |
 | Rainy day fund |
 | Vacation fund  |
 +----------------*/
```

The following query finds the account and its owner with the largest outgoing
transfer to a blocked account:

```sql
GRAPH FinGraph
MATCH (src_account:Account)-[transfer:Transfers]->(dst_account:Account)
WHERE dst_account.is_blocked
ORDER BY transfer.amount DESC
LIMIT 1
MATCH (src_account:Account)<-[owns:Owns]-(owner:Person)
RETURN src_account.id AS account_id, owner.name AS owner_name

/*-------------------------+
 | account_id | owner_name |
 +-------------------------+
 | 7          | Alex       |
 +-------------------------*/
```

[gql-return]: #gql_return

[limit-and-offset-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_and_offset_clause

## `MATCH` statement 
<a id="gql_match"></a>

<pre>
[ OPTIONAL ] MATCH <span class="var">graph_pattern</span>
</pre>

#### Description

Matches data described by a graph pattern. You can have zero or more `MATCH`
statements in a linear query statement.

#### Definitions

+ `MATCH graph_pattern`: The graph pattern to match. For more information,
  see [`MATCH` graph pattern definition][graph-pattern-definition].
+ `OPTIONAL MATCH graph_pattern`: The graph pattern to optionally match. If there
  are missing parts in the pattern, the missing parts are represented by `NULL`
  values. For more information, see
  [`OPTIONAL MATCH` graph pattern definition][graph-pattern-definition].

#### Details

The `MATCH` statement joins the incoming working table with the matched
result with either `INNER JOIN` or `CROSS JOIN` semantics.

The `INNER JOIN` semantics is used when the working table and matched result
have variables in common. In the following example, the `INNER JOIN`
semantics is used because `friend` is produced by both `MATCH` statements:

```sql
MATCH (person:Person)-[:knows]->(friend:Person)
MATCH (friend)-[:knows]->(otherFriend:Person)
```

The `CROSS JOIN` semantics is used when the incoming working table and matched
result have no variables in common. In the following example, the `CROSS JOIN`
semantics is used because `person1` and `friend` exist in the result of the
first `MATCH` statement, but not the second one:

```sql
MATCH (person1:Person)-[:knows]->(friend:Person)
MATCH (person2:Person)-[:knows]->(otherFriend:Person)
```

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following query matches all `Person` nodes and returns the name and ID of
each person:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id

/*-----------+
 | name | id |
 +-----------+
 | Alex | 1  |
 | Dana | 2  |
 | Lee  | 3  |
 +-----------*/
```

The following query matches all `Person` and `Account` nodes and returns their
labels and ID:

```sql
GRAPH FinGraph
MATCH (n:Person|Account)
RETURN LABELS(n) AS label, n.id

/*----------------+
 | label     | id |
 +----------------+
 | [Account] | 7  |
 | [Account] | 16 |
 | [Account] | 20 |
 | [Person]  | 1  |
 | [Person]  | 2  |
 | [Person]  | 3  |
 +----------------*/
```

The following query matches all `Account` nodes that are not blocked:

```sql
GRAPH FinGraph
MATCH (a:Account {is_blocked: false})
RETURN a.id

/*----+
 | id |
 +----+
 | 7  |
 | 20 |
 +----*/
```

The following query matches all `Person` nodes that have a `birthday` less than
`1990-01-10`:

```sql
GRAPH FinGraph
MATCH (p:Person WHERE p.birthday < '1990-01-10')
RETURN p.name

/*------+
 | name |
 +------+
 | Dana |
 | Lee  |
 +------*/
```

The following query matches all `Owns` edges:

```sql
GRAPH FinGraph
MATCH -[e:Owns]->
RETURN e.id

/*----+
 | id |
 +----+
 | 1  |
 | 3  |
 | 2  |
 +----*/
```

The following query matches all `Owns` edges created within a specific period of
time:

```sql
GRAPH FinGraph
MATCH -[e:Owns WHERE e.create_time > '2020-01-14' AND e.create_time < '2020-05-14']->
RETURN e.id

/*----+
 | id |
 +----+
 | 2  |
 | 3  |
 +----*/
```

The following query matches all `Transfers` edges where a blocked account is
involved in any direction:

```sql
GRAPH FinGraph
MATCH (account:Account)-[transfer:Transfers]-(:Account)
WHERE account.is_blocked
RETURN transfer.order_number, transfer.amount

/*--------------------------+
 | order_number    | amount |
 +--------------------------+
 | 304330008004315 | 300    |
 | 304120005529714 | 100    |
 | 103650009791820 | 300    |
 | 302290001255747 | 200    |
 +--------------------------*/
```

The following query matches all `Transfers` initiated from an `Account` owned by
`Person` with `id` equal to `2`:

```sql
GRAPH FinGraph
MATCH
  (p:Person {id: 2})-[:Owns]->(account:Account)-[t:Transfers]->
  (to_account:Account)
RETURN p.id AS sender_id, to_account.id AS to_id

/*-------------------+
 | sender_id | to_id |
 +-------------------+
 | 2         | 7     |
 | 2         | 16    |
 +-------------------*/
```

The following query matches all the destination `Accounts` one to three
transfers away from a source `Account` with `id` equal to `7`, other than the
source itself:

```sql
GRAPH FinGraph
MATCH (src:Account {id: 7})-[e:Transfers]->{1, 3}(dst:Account)
WHERE src != dst
RETURN ARRAY_LENGTH(e) AS hops, dst.id AS destination_account_id

/*-------------------------------+
 | hops | destination_account_id |
 +-------------------------------+
 | 1    | 16                     |
 | 3    | 16                     |
 | 3    | 16                     |
 | 1    | 16                     |
 | 2    | 20                     |
 | 2    | 20                     |
 +-------------------------------*/
```

The following query matches paths between `Account` nodes with one to two
`Transfers` edges through intermediate accounts that are blocked:

```sql
GRAPH FinGraph
MATCH
  (src:Account)
  ((:Account)-[:Transfers]->(interm:Account) WHERE interm.is_blocked){1,2}
  -[:Transfers]->(dst:Account)
RETURN src.id AS source_account_id, dst.id AS destination_account_id

/*--------------------------------------------+
 | source_account_id | destination_account_id |
 +--------------------------------------------+
 | 7                 | 20                     |
 | 7                 | 20                     |
 | 20                | 20                     |
 +--------------------------------------------*/
```

The following query finds unique reachable accounts which are one or two
transfers away from a given `Account` node:

```sql
GRAPH FinGraph
MATCH ANY (src:Account {id: 7})-[e:Transfers]->{1,2}(dst:Account)
LET ids_in_path = ARRAY(SELECT e.to_id FROM UNNEST(e) AS e)
RETURN src.id AS source_account_id, dst.id AS destination_account_id, ids_in_path

/*----------------------------------------------------------+
 | source_account_id | destination_account_id | ids_in_path |
 +----------------------------------------------------------+
 | 7                 | 16                     | 16          |
 | 7                 | 20                     | 16,20       |
 +----------------------------------------------------------*/
```

The following query matches all `Person` nodes and optionally matches the
blocked `Account` owned by the `Person`. The missing blocked `Account` is
represented as `NULL`:

```sql
GRAPH FinGraph
MATCH (n:Person)
OPTIONAL MATCH (n:Person)-[:Owns]->(a:Account {is_blocked: TRUE})
RETURN n.name, a.id AS blocked_account_id

/*--------------+
 | name  | id   |
 +--------------+
 | Lee   | 16   |
 | Alex  | NULL |
 | Dana  | NULL |
 +--------------*/
```

[graph-pattern-definition]: https://github.com/google/zetasql/blob/master/docs/graph-patterns.md#graph_pattern_definition

## `NEXT` statement 
<a id="gql_next"></a>

<pre>
NEXT
</pre>

##### Description

Chains multiple linear query statements together.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following linear query statements are chained by the `NEXT` statement:

```sql
GRAPH FinGraph
MATCH (:Account)-[:Transfers]->(account:Account)
RETURN account, COUNT(*) AS num_incoming_transfers
GROUP BY account

NEXT

MATCH (account:Account)<-[:Owns]-(owner:Person)
RETURN
  account.id AS account_id, owner.name AS owner_name,
  num_incoming_transfers

NEXT

FILTER num_incoming_transfers < 2
RETURN account_id, owner_name
UNION ALL
RETURN "Bob" AS owner_name, 100 AS account_id

/*-------------------------+
 | account_id | owner_name |
 +-------------------------+
 | 7          | Alex       |
 | 20         | Dana       |
 | 100        | Bob        |
 | 100        | Bob        |
 | 100        | Bob        |
 +-------------------------*/
```

## `OFFSET` statement 
<a id="gql_offset"></a>

<pre>
OFFSET <span class="var">count</span>
</pre>

#### Description

Skips a specified number of rows in the query results.

#### Definitions

+   `count`: A non-negative `INT64` value that represents the number of
    rows to skip. For more information,
    see the [`LIMIT` and `OFFSET` clauses][limit-and-offset-clause].

#### Details

The `OFFSET` statement can appear anywhere in a linear query statement before
the `RETURN` statement.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

In the following example, the first two rows are not included in the results:

```sql
GRAPH FinGraph
MATCH (p:Person)
OFFSET 2
RETURN p.name, p.id

/*-----------+
 | name | id |
 +-----------+
 | Lee  | 3  |
 +-----------*/
```

[limit-and-offset-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_and_offset_clause

## `ORDER BY` statement 
<a id="gql_order_by"></a>

<pre>
ORDER BY <span class="var">order_by_specification</span>[, ...]

<span class="var">order_by_specification</span>:
  <span class="var">expression</span>
  [ COLLATE <span class="var">collation_specification</span> ]
  [ { ASC | ASCENDING | DESC | DESCENDING } ]
  [ { NULLS FIRST | NULLS LAST } ]
</pre>

#### Description

Orders the query results.

#### Definitions

+   `expression`: The sort criterion for the result set. For more information,
    see the [`ORDER BY` clause][order-by-clause].
+   `COLLATE collation_specification`: The collation specification for
    `expression`. For more information, see the
    [`ORDER BY` clause][order-by-clause].
+   `ASC | ASCENDING | DESC | DESCENDING`: The sort order, which can be either
    ascending or descending. The following options are synonymous:

    + `ASC` and `ASCENDING`

    + `DESC` and `DESCENDING`

    For more information about sort order, see the
    [`ORDER BY` clause][order-by-clause].
+   `NULLS FIRST | NULLS LAST`: Determines how `NULL` values are sorted for
    `expression`. For more information, see the
    [`ORDER BY` clause][order-by-clause].

#### Details

Ordinals are not supported in the `ORDER BY` statement.

The `ORDER BY` statement is ignored unless it is immediately followed by the
`LIMIT` or `OFFSET` statement.

If you would like to apply `ORDER BY` to what is in `RETURN` statement, use the
`ORDER BY` clause in `RETURN` statement. For more information, see
[`RETURN` statement][return-statement].

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following query sorts the results by the `transfer.amount`
values in descending order:

```sql
GRAPH FinGraph
MATCH (src_account:Account)-[transfer:Transfers]->(dst_account:Account)
ORDER BY transfer.amount DESC
LIMIT 3
RETURN src_account.id AS account_id, transfer.amount AS transfer_amount

/*------------------------------+
 | account_id | transfer_amount |
 +------------------------------+
 | 20         | 500             |
 | 7          | 300             |
 | 16         | 300             |
 +------------------------------*/
```

```sql
GRAPH FinGraph
MATCH (src_account:Account)-[transfer:Transfers]->(dst_account:Account)
ORDER BY transfer.amount DESC
OFFSET 1
RETURN src_account.id AS account_id, transfer.amount AS transfer_amount

/*------------------------------+
 | account_id | transfer_amount |
 +------------------------------+
 | 7          | 300             |
 | 16         | 300             |
 | 20         | 200             |
 | 7          | 100             |
 +------------------------------*/
```

If you don't include the `LIMIT` or `OFFSET` statement right after the
`ORDER BY` statement, the effect of `ORDER BY` is discarded and the result is
unordered.

```sql
-- Warning: The transfer.amount values are not sorted because the
-- LIMIT statement is missing.
GRAPH FinGraph
MATCH (src_account:Account)-[transfer:Transfers]->(dst_account:Account)
ORDER BY transfer.amount DESC
RETURN src_account.id AS account_id, transfer.amount AS transfer_amount

/*------------------------------+
 | account_id | transfer_amount |
 +------------------------------+
 | 7          | 300             |
 | 7          | 100             |
 | 16         | 300             |
 | 20         | 500             |
 | 20         | 200             |
 +------------------------------*/
```

```sql
-- Warning: Using the LIMIT clause in the RETURN statement, but not immediately
-- after the ORDER BY statement, also returns the unordered transfer.amount
-- values.
GRAPH FinGraph
MATCH (src_account:Account)-[transfer:Transfers]->(dst_account:Account)
ORDER BY transfer.amount DESC
RETURN src_account.id AS account_id, transfer.amount AS transfer_amount
LIMIT 10

/*------------------------------+
 | account_id | transfer_amount |
 +------------------------------+
 | 7          | 300             |
 | 7          | 100             |
 | 16         | 300             |
 | 20         | 500             |
 | 20         | 200             |
 +------------------------------*/
```

[order-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#order_by_clause

[return-statement]: #gql_return

## `RETURN` statement 
<a id="gql_return"></a>

<pre>
RETURN <span class="var">*</span>
</pre>

<pre>
RETURN
  [ { ALL | DISTINCT } ]
  <span class="var">return_item</span>[, ... ]
  [ <span class="var">group_by_clause</span> ]
  [ <span class="var">order_by_clause</span> ]
  [ <span class="var">limit_and_offset_clauses</span> ]

<span class="var">return_item</span>:
  { <span class="var">expression</span> [ AS <span class="var">alias</span> ] | <span class="var">*</span> }

<span class="var">limit_and_offset_clauses</span>:
  {
    <span class="var">limit_clause</span>
    | <span class="var">offset_clause</span>
    | <span class="var">offset_clause</span> <span class="var">limit_clause</span>
  }
</pre>

#### Description

Marks the end of a linear query statement and returns the results. Only one `RETURN`
statement is allowed in a linear query statement.

#### Definitions

+   `*`: Returns all columns in the current working table.
+   `return_item`: A column to include in the results.
+   `ALL`: Returns all rows. This is equivalent to not using any prefix.
+   `DISTINCT`: Duplicate rows are discarded and only the remaining distinct
    rows are returned. This deduplication takes place after any aggregation
    is performed.
+   `expression`: An expression that represents a column to produce.
    Aggregation is supported.
+   `alias`: An alias for `expression`.
+   `group_by_clause`: Groups the current rows of the working table, using the
    [`GROUP BY` clause][group-by-clause]. If
    `GROUP BY ALL` is applied, the groupable items from the
    `return_item` list are used to group the rows.
+   `order_by_clause`: Orders the current rows in a
    linear query statement, using the [`ORDER BY` clause][order-by-clause].
+   `limit_clause`: Limits the number of current rows in a
    linear query statement, using the [`LIMIT` clause][limit-and-offset-clause].
+   `offset_clause`: Skips a specified number of rows in a linear query statement,
    using the [`OFFSET` clause][limit-and-offset-clause].

#### Details

If any expression performs aggregation, and no `GROUP BY` clause is
specified, all groupable items from the return list are used implicitly as
grouping keys (This is
equivalent to `GROUP BY ALL`).

Ordinals are not supported in the `ORDER BY` and `GROUP BY` clauses.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following query returns `p.name` and `p.id`:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id

/*-----------+
 | name | id |
 +-----------+
 | Alex | 1  |
 | Dana | 2  |
 | Lee  | 3  |
 +-----------*/
```

In the following example, the first linear query statement returns all columns
including `p`, `a`, `b`, and `c`. The second linear query statement returns the
specified `p.name` and `d` columns:

```sql
GRAPH FinGraph
MATCH (p:Person)
LET a = 1, b = 2, c = 3
RETURN *

NEXT

RETURN p.name, (a + b + c) AS d

/*----------+
 | name | d |
 +----------+
 | Alex | 6 |
 | Dana | 6 |
 | Lee  | 6 |
 +----------*/
```

The following query returns distinct rows:

```sql
GRAPH FinGraph
MATCH (src:Account {id: 7})-[e:Transfers]->{1, 3}(dst:Account)
RETURN DISTINCT ARRAY_LENGTH(e) AS hops, dst.id AS destination_account_id

/*-------------------------------+
 | hops | destination_account_id |
 +-------------------------------+
 | 3    | 7                      |
 | 1    | 16                     |
 | 3    | 16                     |
 | 2    | 20                     |
 +-------------------------------*/
```

In the following example, the first linear query statement returns `account` and
aggregated `num_incoming_transfers` per account. The second statement returns
sorted result.

```sql
GRAPH FinGraph
MATCH (:Account)-[:Transfers]->(account:Account)
RETURN account, COUNT(*) AS num_incoming_transfers
GROUP BY account

NEXT

MATCH (account:Account)<-[:Owns]-(owner:Person)
RETURN owner.name AS owner_name, num_incoming_transfers
ORDER BY num_incoming_transfers DESC

/*-------------------------------------+
 | owner_name | num_incoming_transfers |
 +-------------------------------------+
 | Lee        | 3                      |
 | Alex       | 1                      |
 | Dana       | 1                      |
 +-------------------------------------*/
```

In the following example, the `GROUP BY ALL` clause groups rows by inferring
grouping keys from the return items in the `RETURN` statement.

```sql
GRAPH FinGraph
MATCH (:Account)-[:Transfers]->(account:Account)
RETURN account, COUNT(*) AS num_incoming_transfers
GROUP BY ALL
ORDER BY num_incoming_transfers DESC

NEXT

MATCH (account:Account)<-[:Owns]-(owner:Person)
RETURN owner.name AS owner_name, num_incoming_transfers

/*-------------------------------------+
 | owner_name | num_incoming_transfers |
 +-------------------------------------+
 | Alex       | 1                      |
 | Dana       | 1                      |
 | Lee        | 3                      |
 +-------------------------------------*/
```

In the following example, the `LIMIT` clause in the `RETURN` statement
reduces the results to one row:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id
LIMIT 1

/*-----------+
 | name | id |
 +-----------+
 | Alex | 1  |
 +-----------*/
```

In the following example, the `OFFSET` clause in the `RETURN` statement
skips the first row:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id
OFFSET 1

/*-----------+
 | name | id |
 +-----------+
 | Dana | 2  |
 | Lee  | 3  |
 +-----------*/
```

In the following example, the `OFFSET` clause in the `RETURN` statement
skips the first row, then the `LIMIT` clause reduces the
results to one row:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id
OFFSET 1
LIMIT 1

/*-----------+
 | name | id |
 +-----------+
 | Dana | 2  |
 +-----------*/
```

In the following example, an error is produced because the `OFFSET` clause must
come before the `LIMIT` clause when they are both used in the
`RETURN` statement:

```sql {.bad}
-- Error: The LIMIT clause must come after the OFFSET clause in a
-- RETURN operation.
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, p.id
LIMIT 1
OFFSET 1
```

In the following example, the `ORDER BY` clause in the `RETURN` statement sorts
the results by `hops` and then `destination_account_id`:

```sql
GRAPH FinGraph
MATCH (src:Account {id: 7})-[e:Transfers]->{1, 3}(dst:Account)
RETURN DISTINCT ARRAY_LENGTH(e) AS hops, dst.id AS destination_account_id
ORDER BY hops, destination_account_id

/*-------------------------------+
 | hops | destination_account_id |
 +-------------------------------+
 | 1    | 16                     |
 | 2    | 20                     |
 | 3    | 7                      |
 | 3    | 16                     |
 +-------------------------------*/
```

[group-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[order-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#order_by_clause

[limit-and-offset-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_and_offset_clause

## `SKIP` statement 
<a id="gql_skip"></a>

<pre>
SKIP <span class="var">count</span>
</pre>

#### Description

Synonym for the [`OFFSET` statement][gql-offset].

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

`SKIP` is a synonym for `OFFSET`. Therefore, these queries are equivalent:

```sql
GRAPH FinGraph
MATCH (p:Person)
SKIP 2
RETURN p.name, p.id

/*-----------+
 | name | id |
 +-----------+
 | Lee  | 3  |
 +-----------*/
```

```sql
GRAPH FinGraph
MATCH (p:Person)
OFFSET 2
RETURN p.name, p.id

/*-----------+
 | name | id |
 +-----------+
 | Lee  | 3  |
 +-----------*/
```

[gql-offset]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_offset

## `WITH` statement 
<a id="gql_with"></a>

<pre>
WITH
  [ { ALL | DISTINCT } ]
  <span class="var">return_item</span>[, ... ]
  [ <span class="var">group_by_clause</span> ]

<span class="var">return_item</span>:
  { <span class="var">expression</span> [ AS <span class="var">alias</span> ] | <span class="var">*</span> }
</pre>

#### Description

Passes on the specified columns, optionally filtering, renaming, and
transforming those results.

#### Definitions

+   `*`: Returns all columns in the current working table.
+   `ALL`: Returns all rows. This is equivalent to not using any prefix.
+   `DISTINCT`: Returns distinct rows. Deduplication takes place after
    aggregations are performed.
+   `return_item`: A column to include in the results.
+   `expression`: An expression that represents a column to produce. Aggregation
    is supported.
+   `alias`: An alias for `expression`.
+   `group_by_clause`: Groups the current rows of the working table, using the
    [`GROUP BY` clause][group-by-clause]. If `GROUP BY ALL`
    is applied, the groupable items from the `return_item` list are used to
    group the rows.

#### Details

If any expression performs aggregation, and no `GROUP BY` clause is
specified, all groupable items from the return list are implicitly used as
grouping keys (This is equivalent to `GROUP BY ALL`).

Window functions are not supported in `expression`.

Ordinals are not supported in the `GROUP BY` clause.

#### Examples

Note: The examples in this section reference a property graph called
[`FinGraph`][fin-graph].

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

The following query returns all distinct destination account IDs:

```sql
GRAPH FinGraph
MATCH (src:Account)-[transfer:Transfers]->(dst:Account)
WITH DISTINCT dst
RETURN dst.id AS destination_id

/*----------------+
 | destination_id |
 +----------------+
 | 7              |
 | 16             |
 | 20             |
 +----------------*/
```

The following query uses `*` to carry over the existing columns of
the working table in addition to defining a new one for the destination
account id.

```sql
GRAPH FinGraph
MATCH (src:Account)-[transfer:Transfers]->(dst:Account)
WITH *, dst.id
RETURN dst.id AS destination_id

/*----------------+
 | destination_id |
 +----------------+
 | 7              |
 | 16             |
 | 16             |
 | 16             |
 | 20             |
 +----------------*/
```

In the following example, aggregation is performed implicitly because the
`WITH` statement has an aggregate expression but does not specify a `GROUP BY`
clause. All groupable items from the return item list are used as grouping keys
 (This is equivalent to `GROUP BY ALL`).
In this case, the grouping keys inferred are `src.id` and `dst.id`.
Therefore, this query returns the number of transfers for each
distinct combination of `src.id` and `dst.id`.

```sql
GRAPH FinGraph
MATCH (src:Account)-[transfer:Transfers]->(dst:Account)
WITH COUNT(*) AS transfer_total, src.id AS source_id, dst.id AS destination_id
RETURN transfer_total, destination_id, source_id

/*---------------------------------------------+
 | transfer_total | destination_id | source_id |
 +---------------------------------------------+
 | 2              | 16             | 7         |
 | 1              | 20             | 16        |
 | 1              | 7              | 20        |
 | 1              | 16             | 20        |
 +---------------------------------------------*/
```

In the following example, an error is produced because the `WITH` statement only
contains `dst`. `src` is not visible after the `WITH` statement in the `RETURN`
statement.

```sql {.bad}
-- Error: src does not exist
GRAPH FinGraph
MATCH (src:Account)-[transfer:Transfers]->(dst:Account)
WITH dst
RETURN src.id AS source_id
```

[group-by-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

## Set operation 
<a id="gql_set"></a>

<pre>
<span class="var">linear_query_statement</span>
<span class="var">set_operator</span>
<span class="var">linear_query_statement</span>
[
  <span class="var">set_operator</span>
  <span class="var">linear_graph_query</span>
][...]

<span class="var">set_operator</span>:
  { 
    UNION ALL
    | UNION DISTINCT
    | INTERSECT ALL
    | INTERSECT DISTINCT
    | EXCEPT ALL
    | EXCEPT DISTINCT
  }
</pre>

#### Description

Combines a sequence of linear query statements with a set operation.
Only one type of set operation is allowed per set operation.

#### Definitions

+   `linear_query_statement`: A [linear query statement][gql_syntax] to
    include in the set operation.

#### Details

Each linear query statement in the same set operation shares the same working table.

Most of the rules for GQL set operators are the same as those for
SQL [set operators][set-op], but there are some differences:

+   A GQL set operator does not support hints, or the `CORRESPONDING` keyword.
    Since each set operation input (a linear query statement) only
    produces columns with names, the default behavior of GQL set operations
    requires all inputs to have the same set of column names and all
    paired columns to share the same [supertype][supertypes].
+   GQL does not allow chaining different kinds of set operations in the same
    set operation.
+   GQL does not allow using parentheses to separate different set operations.
+   The results produced by the linear query statements are combined in a left
    associative order.

#### Examples

A set operation between two linear query statements with the same set of
output column names and types but with different column orders is supported.
For example:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, 1 AS group_id
UNION ALL
MATCH (p:Person)
RETURN 2 AS group_id, p.name

/*------+----------+
 | name | group_id |
 +------+----------+
 | Alex |    1     |
 | Dana |    1     |
 | Lee  |    1     |
 | Alex |    2     |
 | Dana |    2     |
 | Lee  |    2     |
 +------+----------*/
```

In a set operation, chaining the same kind of set operation is supported, but
chaining different kinds of set operations is not.
For example:

```sql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, 1 AS group_id
UNION ALL
MATCH (p:Person)
RETURN 2 AS group_id, p.name
UNION ALL
MATCH (p:Person)
RETURN 3 AS group_id, p.name

/*------+----------+
 | name | group_id |
 +------+----------+
 | Alex |    1     |
 | Dana |    1     |
 | Lee  |    1     |
 | Alex |    2     |
 | Dana |    2     |
 | Lee  |    2     |
 | Alex |    3     |
 | Dana |    3     |
 | Lee  |    3     |
 +------+----------*/
```

```sql {.bad}
-- ERROR: GQL does not allow chaining EXCEPT DISTINCT with UNION ALL
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name, 1 AS group_id
UNION ALL
MATCH (p:Person)
RETURN 2 AS group_id, p.name
EXCEPT DISTINCT
MATCH (p:Person)
RETURN 3 AS group_id, p.name
```

[set-op]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#set_operators

[supertypes]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[gql_syntax]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md#gql_syntax

