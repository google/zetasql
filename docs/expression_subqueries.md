

# Expression subqueries

There are four types of expression subqueries, i.e. subqueries that are used as
expressions.  Expression subqueries return `NULL` or a single value, as opposed to
a column or table, and must be surrounded by parentheses. For a fuller
discussion of subqueries, see
[Subqueries][exp-sub-link-to-subqueries].

<table>
<thead>
<tr>
<th>Type of Subquery</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Scalar</td>
<td>Any type T</td>
<td>A subquery in parentheses inside an expression (e.g. in the
<code>SELECT</code> list or <code>WHERE</code> clause) is interpreted as a
scalar subquery. The <code>SELECT</code> list in a scalar subquery must have
exactly one field. If the subquery returns exactly one row, that single value is
the scalar subquery result. If the subquery returns zero rows, the scalar
subquery value is <code>NULL</code>. If the subquery returns more than one row, the query
fails with a runtime error. When the subquery is written with <code>SELECT AS
STRUCT</code>  or <code>SELECT AS ProtoName</code>, it can include multiple
columns, and the returned value is the constructed STRUCT or PROTO. Selecting
multiple columns without using <code>SELECT AS</code> is an error.</td>
</tr>
<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>Can use <code>SELECT AS STRUCT</code> or <code>SELECT AS ProtoName</code> to
build arrays of structs or PROTOs, and conversely, selecting multiple columns
without using <code>SELECT AS</code> is an error. Returns an empty ARRAY if the
subquery returns zero rows. Never returns a <code>NULL</code> ARRAY.</td>
</tr>

<tr>
<td>IN</td>
<td>BOOL</td>
<td>Occurs in an expression following the IN operator. The subquery must produce
a single column whose type is equality-compatible with the expression on the
left side of the IN operator. Returns FALSE if the subquery returns zero rows.
<code>x IN ()</code> is equivalent to <code>x IN (value, value, ...)</code>
See the <code>IN</code> operator in
<a href="functions-and-operators.md#comparison-operators">
  Comparison Operators</a>

for full semantics.</td>

</tr>

<tr>
<td>EXISTS</td>
<td>BOOL</td>
<td>Returns TRUE if the subquery produced one or more rows. Returns FALSE if the
subquery produces zero rows. Never returns <code>NULL</code>. Unlike all other expression
subqueries, there are no rules about the column list. Any number of columns may
be selected and it will not affect the query result.</td>

</tr>
</tbody>
</table>

**Examples**

The following examples of expression subqueries assume that `t.int_array` has
type `ARRAY<INT64>`.

<table>
<thead>
<tr>
<th>Type</th>
<th>Subquery</th>
<th>Result Data Type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td rowspan="8" style="vertical-align:top">Scalar</td>
<td><code>(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>INT64</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT DISTINCT i FROM t.int_array i)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT i FROM t.int_array i WHERE i=5)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT ARRAY_AGG(i) FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Uses the ARRAY_AGG aggregation function to return an ARRAY.</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a)</code></td>
<td>STRING</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a, 123 b)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a, 123 b)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="7" style="vertical-align:top">ARRAY</td>
<td><code>ARRAY(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>ARRAY of size 1</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT x FROM t)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT i FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Makes an ARRAY of one-field STRUCTs</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>ARRAY</td>
<td>Returns an ARRAY of STRUCTs with anonymous or duplicate fields.</td>
</tr>
<tr>
<td><code>ARRAY(SELECT  AS TypeName SUM(x) a, SUM(y) b, SUM(z) c from t)</code></td>
<td>array&lt;TypeName></td>
<td>Selecting into a named type. Assume TypeName is a STRUCT type with fields
a,b,c.</td>
</tr>
<tr>
<td style="vertical-align:top">STRUCT</td>
<td><code>(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>STRUCT</td>
<td>Constructs a STRUCT with anonymous or duplicate fields.</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">EXISTS</td>
<td><code>EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>NOT EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">IN</td>
<td><code>x IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>x NOT IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

[exp-sub-link-to-subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#subqueries
