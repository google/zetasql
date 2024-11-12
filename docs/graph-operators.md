

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# GQL operators

Graph Query Language (GQL) supports all ZetaSQL [operators][operators],
including the following GQL-specific operators:

## Graph operators list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#graph_concatenation_operator">Graph concatenation operator</a>
</td>
  <td>
    Combines multiple graph paths into one and preserves the original order of
    the nodes and edges.
  </td>
</tr>

<tr>
  <td><a href="#graph_logical_operators">Graph logical operators</a>
</td>
  <td>
    Tests for the truth of a condition in a graph and produces either
    <code>TRUE</code> or <code>FALSE</code>.
  </td>
</tr>

<tr>
  <td><a href="#graph_predicates">Graph predicates</a>
</td>
  <td>
    Tests for the truth of a condition for a graph element and produces
    <code>TRUE</code>, <code>FALSE</code>, or <code>NULL</code>.
  </td>
</tr>

<tr>
  <td><a href="#is_destination_predicate"><code>IS DESTINATION</code> predicate</a>
</td>
  <td>In a graph, checks to see if a node is or isn't the destination of an edge.</td>
</tr>

<tr>
  <td><a href="#is_source_predicate"><code>IS SOURCE</code> predicate</a>
</td>
  <td>In a graph, checks to see if a node is or isn't the source of an edge.</td>
</tr>

<tr>
  <td><a href="#property_exists_predicate"><code>PROPERTY_EXISTS</code> predicate</a>
</td>
  <td>In a graph, checks to see if a property exists for an element.</td>
</tr>

<tr>
  <td><a href="#same_predicate"><code>SAME</code> predicate</a>
</td>
  <td>
    In a graph, determines if all graph elements in a list bind to the same node or edge.
  </td>
</tr>

  </tbody>
</table>

## Graph concatenation operator 
<a id="graph_concatenation_operator"></a>

```sql
graph_path || graph_path [ || ... ]
```

**Description**

Combines multiple graph paths into one and preserves the original order of the
nodes and edges.

Arguments:

+ `graph_path`: A `GRAPH_PATH` value that represents a graph path to
  concatenate.

**Details**

This operator produces an error if the last node in the first path isn't the
same as the first node in the second path.

```sql
-- This successfully produces the concatenated path called `full_path`.
MATCH
  p=(src:Account)-[t1:Transfers]->(mid:Account),
  q=(mid)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
```

```sql
-- This produces an error because the first node of the path to be concatenated
-- (mid2) is not equal to the last node of the previous path (mid1).
MATCH
  p=(src:Account)-[t1:Transfers]->(mid1:Account),
  q=(mid2:Account)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
```

The first node in each subsequent path is removed from the
concatenated path.

```sql
-- The concatenated path called `full_path` contains these elements:
-- src, t1, mid, t2, dst.
MATCH
  p=(src:Account)-[t1:Transfers]->(mid:Account),
  q=(mid)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
```

If any `graph_path` is `NULL`, produces `NULL`.

**Example**

In the following query, a path called `p` and `q` are concatenated. Notice that
`mid` is used at the end of the first path and at the beginning of the
second path. Also notice that the duplicate `mid` is removed from the
concatenated path called `full_path`:

```sql
GRAPH FinGraph
MATCH
  p=(src:Account)-[t1:Transfers]->(mid:Account),
  q = (mid)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
RETURN
  JSON_QUERY(TO_JSON(full_path)[0], '$.labels') AS element_a,
  JSON_QUERY(TO_JSON(full_path)[1], '$.labels') AS element_b,
  JSON_QUERY(TO_JSON(full_path)[2], '$.labels') AS element_c,
  JSON_QUERY(TO_JSON(full_path)[3], '$.labels') AS element_d,
  JSON_QUERY(TO_JSON(full_path)[4], '$.labels') AS element_e,
  JSON_QUERY(TO_JSON(full_path)[5], '$.labels') AS element_f

/*-------------------------------------------------------------------------------------*
 | element_a   | element_b     | element_c   | element_d     | element_e   | element_f |
 +-------------------------------------------------------------------------------------+
 | ["Account"] | ["Transfers"] | ["Account"] | ["Transfers"] | ["Account"] |           |
 | ...         | ...           | ...         | ...           | ...         | ...       |
 *-------------------------------------------------------------------------------------/*
```

The following query produces an error because the last node for `p` must
be the first node for `q`:

```sql
-- Error: `mid1` and `mid2` are not equal.
GRAPH FinGraph
MATCH
  p=(src:Account)-[t1:Transfers]->(mid1:Account),
  q=(mid2:Account)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
RETURN TO_JSON(full_path) AS results
```

The following query produces an error because the path called `p` is `NULL`:

```sql
-- Error: a graph path is NULL.
GRAPH FinGraph
MATCH
  p=NULL,
  q=(mid:Account)-[t2:Transfers]->(dst:Account)
LET full_path = p || q
RETURN TO_JSON(full_path) AS results
```

## Graph logical operators 
<a id="graph_logical_operators"></a>

ZetaSQL supports the following logical operators in
[element pattern label expressions][element-pattern-definition]:

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Syntax</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>NOT</code></td>
      <td style="white-space:nowrap"><code>!X</code></td>
      <td>
        Returns <code>TRUE</code> if <code>X</code> is not included, otherwise,
        returns <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>OR</code></td>
      <td style="white-space:nowrap"><code>X | Y</code></td>
      <td>
        Returns <code>TRUE</code> if either <code>X</code> or <code>Y</code> is
        included, otherwise, returns <code>FALSE</code>.
      </td>
    </tr>
    <tr>
      <td><code>AND</code></td>
      <td style="white-space:nowrap"><code>X & Y</code></td>
      <td>
        Returns <code>TRUE</code> if both <code>X</code> and <code>Y</code> are
        included, otherwise, returns <code>FALSE</code>.
      </td>
    </tr>
  </tbody>
</table>

[element-pattern-definition]: https://github.com/google/zetasql/blob/master/docs/graph-patterns.md#element_pattern_definition

## Graph predicates 
<a id="graph_predicates"></a>

ZetaSQL supports the following graph-specific predicates in
graph expressions. A predicate can produce `TRUE`, `FALSE`, or `NULL`.

+   [`PROPERTY_EXISTS` predicate][property-exists-predicate]
+   [`IS SOURCE` predicate][is-source-predicate]
+   [`IS DESTINATION` predicate][is-destination-predicate]
+   [`SAME` predicate][same-predicate]

[property-exists-predicate]: #property_exists_predicate

[is-source-predicate]: #is_source_predicate

[is-destination-predicate]: #is_destination_predicate

[same-predicate]: #same_predicate

## `IS DESTINATION` predicate 
<a id="is_destination_predicate"></a>

```sql
node IS [ NOT ] DESTINATION [ OF ] edge
```

**Description**

In a graph, checks to see if a node is or isn't the destination of an edge.
Can produce `TRUE`, `FALSE`, or `NULL`.

Arguments:

+ `node`: The graph pattern variable for the node element.
+ `edge`: The graph pattern variable for the edge element.

**Examples**

```sql
GRAPH FinGraph
MATCH (a:Account)-[transfer:Transfers]-(b:Account)
WHERE a IS DESTINATION of transfer
RETURN a.id AS a_id, b.id AS b_id

/*-------------+
 | a_id | b_id |
 +-------------+
 | 16   | 7    |
 | 16   | 7    |
 | 20   | 16   |
 | 7    | 20   |
 | 16   | 20   |
 +-------------*/
```

```sql
GRAPH FinGraph
MATCH (a:Account)-[transfer:Transfers]-(b:Account)
WHERE b IS DESTINATION of transfer
RETURN a.id AS a_id, b.id AS b_id

/*-------------+
 | a_id | b_id |
 +-------------+
 | 7    | 16   |
 | 7    | 16   |
 | 16   | 20   |
 | 20   | 7    |
 | 20   | 16   |
 +-------------*/
```

## `IS SOURCE` predicate 
<a id="is_source_predicate"></a>

```sql
node IS [ NOT ] SOURCE [ OF ] edge
```

**Description**

In a graph, checks to see if a node is or isn't the source of an edge.
Can produce `TRUE`, `FALSE`, or `NULL`.

Arguments:

+ `node`: The graph pattern variable for the node element.
+ `edge`: The graph pattern variable for the edge element.

**Examples**

```sql
GRAPH FinGraph
MATCH (a:Account)-[transfer:Transfers]-(b:Account)
WHERE a IS SOURCE of transfer
RETURN a.id AS a_id, b.id AS b_id

/*-------------+
 | a_id | b_id |
 +-------------+
 | 20   | 7    |
 | 7    | 16   |
 | 7    | 16   |
 | 20   | 16   |
 | 16   | 20   |
 +-------------*/
```

```sql
GRAPH FinGraph
MATCH (a:Account)-[transfer:Transfers]-(b:Account)
WHERE b IS SOURCE of transfer
RETURN a.id AS a_id, b.id AS b_id

/*-------------+
 | a_id | b_id |
 +-------------+
 | 7    | 20   |
 | 16   | 7    |
 | 16   | 7    |
 | 16   | 20   |
 | 20   | 16   |
 +-------------*/
```

## `PROPERTY_EXISTS` predicate 
<a id="property_exists_predicate"></a>

```sql
PROPERTY_EXISTS(element, element_property)
```

**Description**

In a graph, checks to see if a property exists for an element.
Can produce `TRUE`, `FALSE`, or `NULL`.

Arguments:

+ `element`: The graph pattern variable for a node or edge element.
+ `element_property`: The name of the property to look for in `element`.
  The property name must refer to a property in the graph. If the property
  does not exist in the graph, an error is produced. The property name is
  resolved in a case-insensitive manner.

**Example**

```sql
GRAPH FinGraph
MATCH (n:Person|Account WHERE PROPERTY_EXISTS(n, name))
RETURN n.name

/*------+
 | name |
 +------+
 | Alex |
 | Dana |
 | Lee  |
 +------*/
```

## `SAME` predicate 
<a id="same_predicate"></a>

```sql
SAME (element, element[, element])
```

**Description**

In a graph, determines if all graph elements in a list bind to the same node or
edge. Can produce `TRUE`, `FALSE`, or `NULL`.

Arguments:

+ `element`: The graph pattern variable for a node or edge element.

**Example**

The following query checks to see if `a` and `b` are not the same person.

```sql
GRAPH FinGraph
MATCH (src:Account)<-[transfer:Transfers]-(dest:Account)
WHERE NOT SAME(src, dest)
RETURN src.id AS source_id, dest.id AS destination_id

/*----------------------------+
 | source_id | destination_id |
 +----------------------------+
 | 7         | 20             |
 | 16        | 7              |
 | 16        | 7              |
 | 16        | 20             |
 | 20        | 16             |
 +----------------------------*/
```

[operators]: https://github.com/google/zetasql/blob/master/docs/operators.md

