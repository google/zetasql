

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# GQL data types

Graph Query Language (GQL) supports all ZetaSQL [data types][data-types],
including the following GQL-specific data type:

## Graph data types list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#graph_element_type">Graph element type</a>
</td>
  <td>
    An element in a property graph.<br/>
    SQL type name: <code>GRAPH_ELEMENT</code>
  </td>
</tr>

  </tbody>
</table>

## Graph element type 
<a id="graph_element_type"></a>

<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>GRAPH_ELEMENT</code></td>
<td>
  An element in a property graph.
</td>
</tr>
</tbody>
</table>

A variable with a `GRAPH_ELEMENT` type is produced by a graph query.
The generated type has this format:

```
GRAPH_ELEMENT<T>
```

A graph element can be one of two kinds: a node or edge.
A graph element is similar to the struct type, except that fields are
graph properties, and you can only access graph properties by name.
A graph element can represent nodes or edges from multiple node or edge tables
if multiple such tables match the given label expression.

**Example**

In the following example, `n` represents a graph element in the
[`FinGraph`][fin-graph] property graph:

```sql
GRAPH FinGraph
MATCH (n:Person)
RETURN n.name
```

[graph-query]: https://github.com/google/zetasql/blob/master/docs/graph-intro.md

[fin-graph]: https://github.com/google/zetasql/blob/master/docs/graph-schema-statements.md#fin_graph

[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

