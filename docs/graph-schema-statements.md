

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# GQL schema statements

Graph Query Language (GQL) supports all ZetaSQL DDL statements,
including the following GQL-specific DDL statements:

## Statement list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#gql_create_graph"><code>CREATE PROPERTY GRAPH</code> statement</a>
</td>
  <td>Creates a property graph.</td>
</tr>

<tr>
  <td><a href="#gql_drop_graph"><code>DROP PROPERTY GRAPH</code> statement</a>
</td>
  <td>Deletes a property graph.</td>
</tr>

  </tbody>
</table>

## `CREATE PROPERTY GRAPH` statement {: #gql_create_graph}

### Property graph definition 
<a id="property_graph_definition"></a>

<pre>
CREATE
  [ OR REPLACE ]
  PROPERTY GRAPH
  [ IF NOT EXISTS ]
  <span class="var">property_graph_name</span>
  <span class="var">property_graph_content</span>
  [ OPTIONS (<span class="var">key</span>=<span class="var">value</span>, ...) ];

<span class="var">property_graph_content</span>:
  <span class="var">node_tables</span>
  [ <span class="var">edge_tables</span> ]

<span class="var">node_tables</span>:
  NODE TABLES <span class="var">element_list</span>

<span class="var">edge_tables</span>:
  EDGE TABLES <span class="var">element_list</span>

<span class="var">element_list</span>:
  (<span class="var">element</span>[, ...])
</pre>

**Description**

Creates a property graph.

Note: all GQL examples in the GQL reference use the
[`FinGraph`][fin-graph] property graph example.

**Definitions**

+ `OR REPLACE`: Replaces any property graph with the same name if it exists.
  If the property graph doesn't exist, creates the property graph. Can't
  appear with `IF NOT EXISTS`.
+ `IF NOT EXISTS`: If any property graph exists with the same name, the
  `CREATE` statement has no effect. Can't appear with `OR REPLACE`.
+ `OPTIONS`: If you have schema options, you can add them when you create
  the property graph. These options are system-specific and follow the
  ZetaSQL [`HINT` syntax][hints]
+ `property_graph_name`: The name of the property graph. This name can be a
  path expression. This name must not conflict with the name of an existing
  table, view, or property graph.
+ `property_graph_content`: Add the definitions for the nodes and edges in the
  property graph.
+ `node_tables`: A collection of node definitions. A node definition defines a
  new type of node in the graph.

  The following example represents three node definitions:
  `Account`, `Customer`, and `GeoLocation`.

  ```zetasql
  NODE TABLES (
    Account,
    Customer
      LABEL Client
      PROPERTIES (cid, name),
    Location AS GeoLocation
      DEFAULT LABEL
      PROPERTIES ALL COLUMNS
  )
  ```
+ `edge_tables`: A collection of edge definitions. An edge definition defines
  a new type of edge in the graph. An edge is directed and connects a source and
  a destination node.

  The following example represents two edge definitions:
  `Own` and `Transfer`.

  ```zetasql
  EDGE TABLES (
    Own
      SOURCE KEY (cid) REFERENCES Customer (cid)
      DESTINATION KEY (aid) REFERENCES Account
      NO PROPERTIES,
    Transfer
      SOURCE KEY (from_id) REFERENCES Account (aid)
      DESTINATION KEY (to_id) REFERENCES Account (aid)
      LABEL Transfer NO PROPERTIES
  )
  ```
+ `element_list`: A list of element (node or edge) definitions.
+ `element`: Refer to [Element definition][element-definition] for details.

### Element definition 
<a id="element_definition"></a>

<pre>
<span class="var">element</span>:
  <span class="var">element_name</span>
  [ AS <span class="var">element_alias</span> ]
  [ <span class="var">element_keys</span> ]
  [ { <span class="var">label_and_properties_list</span> | <span class="var">element_properties</span> } ]

<span class="var">element_keys</span>:
  { <span class="var">node_element_key</span> | <span class="var">edge_element_keys</span> }

<span class="var">node_element_key</span>:
  <span class="var">element_key</span>

<span class="var">edge_element_keys</span>:
  <span class="var">element_key</span>
  <span class="var">source_key</span>
  <span class="var">destination_key</span>

<span class="var">element_key</span>:
  KEY <span class="var">column_name_list</span>

<span class="var">source_key</span>:
  SOURCE KEY <span class="var">edge_column_name_list</span>
  REFERENCES <span class="var">element_alias_reference</span> [ <span class="var">node_column_name_list</span> ]

<span class="var">destination_key</span>:
  DESTINATION KEY <span class="var">edge_column_name_list</span>
  REFERENCES <span class="var">element_alias_reference</span> [ <span class="var">node_column_name_list</span> ]

<span class="var">edge_column_name_list</span>:
  <span class="var">column_name_list</span>

<span class="var">node_column_name_list</span>:
  <span class="var">column_name_list</span>

<span class="var">column_name_list</span>:
  (<span class="var">column_name</span>[, ...])
</pre>

**Description**

Adds an element definition to the property graph. For example:

```zetasql
Customer
  LABEL Client
    PROPERTIES (cid, name)
```

In a graph, labels and properties are uniquely identified by their names. Labels
and properties with the same name can appear in multiple node or edge
definitions. However, labels and properties with the same name must follow these
rules:

+ Properties with the same name must have the same value type.
+ Labels with the same name must expose the same set of properties.

**Definitions**

+ `element_name`: The name of the input table from which elements are created.
+ `element_alias`: An optional alias. You must use an alias if you use an input
  table for more than one element definition.
+ `element_keys`: The key for a graph element. This uniquely identifies a graph
  element.

  + By default, the element key is the primary key of the input table.

  + Element keys can be explicitly defined with the `KEY` clause.

  
+ `node_element_key`: The element key for a node.

  ```zetasql
  KEY (item1_column, item2_column)
  ```
+ `edge_element_keys`: The element key, source key, and destination key
  for an edge.

  ```zetasql
  KEY (item1_column, item2_column)
  SOURCE KEY (item1_column) REFERENCES item_node (item_node_column)
  DESTINATION KEY (item2_column) REFERENCES item_node (item_node_column)
  ```
+ `element_key`: The key that identifies the node or edge element.

  ```zetasql
  KEY (item1_column, item2_column)
  ```
+ `source_key`: The key for the source node of the edge.

  ```zetasql
  SOURCE KEY (item1_column) REFERENCES item_node (item_node_column)
  ```
+ `destination_key`: The key for the destination node of the edge.

  ```zetasql
  DESTINATION KEY (item2_column) REFERENCES item_node (item_node_column)
  ```
+ `column_name_list`: One or more columns to assign to a key.

  In `column_name_list`, column names must be unique.
+ Reference column name lists:

  * `node_column_name_list`: One or more columns referenced from the node tables.

  * `edge_column_name_list`: One or more columns referenced from the edge tables.

  Referenced columns must exist in the corresponding node or edge table.

  If `node_column_name_list` doesn't exist in `source_key` or
  `destination_key`, then the `element_keys` of the referenced node are used.
  In this case, the column order in the `element_keys` must match the column
  order in the `edge_column_name_list`.
+ `element_alias_reference`: The alias of another element to reference.
+ `label_and_property_list`: The list of labels and properties to add to
  an element. For more information, see
  [Label and properties list definition][label-property-definition].

### Label and properties list definition 
<a id="label_property_definition"></a>

<pre>
<span class="var">label_and_properties_list</span>:
  <span class="var">label_and_properties</span>[...]

<span class="var">label_and_properties</span>:
  <span class="var">element_label</span>
  [ <span class="var">element_properties</span> ]

<span class="var">element_label</span>:
  {
    LABEL <span class="var">label_name</span> |
    DEFAULT LABEL
  }

</pre>

**Description**

Adds a list of labels and properties to an element.

**Definitions**

+ `label_and_properties`: The label to add to the element and the properties
  exposed by that label. For example:

  ```zetasql
  LABEL Tourist PROPERTIES (home_city, home_country)
  ```

  When `label_and_properties` isn't specified, the following is
  applied implicitly:

  ```zetasql
  DEFAULT LABEL PROPERTIES ARE ALL COLUMNS
  ```

  A property must be unique in `label_and_properties`.
+ `element_label`: Add a custom label or use the default label for the
  element. `label_name` must be unique in `element`.

  If you use `DEFAULT LABEL`, `label_name` is the same as `element_table_alias`.
+ `element_properties`: The properties associated with a label. A property
  can't be used more than once for a specific label. For more information, see
  [Element properties definition][element-table-property-definition].

### Element properties definition 
<a id="element_table_property_definition"></a>

<pre>
<span class="var">element_properties</span>:
  {
    NO PROPERTIES |
    <span class="var">properties_are</span> |
    <span class="var">derived_property_list</span>
  }

<span class="var">properties_are</span>:
  PROPERTIES [ ARE ] ALL COLUMNS [ EXCEPT <span class="var">column_name_list</span> ]

<span class="var">column_name_list</span>:
  (<span class="var">column_name</span>[, ...])

<span class="var">derived_property_list</span>:
  PROPERTIES <span class="var">derived_property</span>[, ...]

<span class="var">derived_property</span>:
  <span class="var">value_expression</span> [ AS <span class="var">property_name</span> ]
</pre>

**Description**

Adds properties associated with a label.

**Definitions**

+ `NO PROPERTIES`: The element doesn't have properties.
+ `properties_are`: Define which columns to include as element
  properties.

  If you don't include this definition, all columns are included by
  default, and the following definition is applied implicitly:

  ```zetasql
  PROPERTIES ARE ALL COLUMNS
  ```

  In the following examples, all columns in a table are included as
  element properties:

  ```zetasql
  PROPERTIES ARE ALL COLUMNS
  ```

  ```zetasql
  PROPERTIES ALL COLUMNS
  ```

  In the following example, all columns in a table except for `home_city` and
  `home_country` are included as element properties:

  ```zetasql
  PROPERTIES ARE ALL COLUMNS EXCEPT (home_city, home_country)
  ```
+ `column_name_list`: A list of columns to exclude as element properties.

  Column names in the `EXCEPT column_name_list` must be unique.
+ `derived_property_list`: A list of element property definitions.
+ `derived_property`: An expression that defines a property and can optionally
  reference the input table columns.

  In the following example, the `id` and `name` columns are included as
  properties. Additionally, the result of the `salary + bonus` expression are
  included as the `income` property:

  ```zetasql
  PROPERTIES (id, name, salary + bonus AS income)
  ```

  A derived property includes:

  + `value_expression`: An expression that can be represented by simple constructs
    such as column references and functions. Subqueries are excluded.

  + `AS property_name`: Alias to assign to the value expression. This is
    optional unless `value_expression` is a function.

  If `derived_property` has any column reference in `value_expression`, that
  column reference must refer to a column of the underlying table.

  If `derived_property` doesn't define `property_name`, `value_expression`
  must be a column reference and the implicit `property_name` is the
  column name.

### `FinGraph` Example 
<a id="fin_graph"></a>

The following property graph, `FinGraph`, contains two node
definitions (`Account` and `Person`) and two edge definitions
(`PersonOwnAccount` and `AccountTransferAccount`).

Note: all GQL examples in the GQL reference use the
[`FinGraph`][fin-graph] property graph example.

```zetasql
CREATE OR REPLACE PROPERTY GRAPH FinGraph
  NODE TABLES (
    Account,
    Person
  )
  EDGE TABLES (
    PersonOwnAccount
      SOURCE KEY (id) REFERENCES Person (id)
      DESTINATION KEY (account_id) REFERENCES Account (id)
      LABEL Owns,
    AccountTransferAccount
      SOURCE KEY (id) REFERENCES Account (id)
      DESTINATION KEY (to_id) REFERENCES Account (id)
      LABEL Transfers
  );
```

Once the property graph is created, you can use it in [GQL][gql] queries. For
example, the following query matches all nodes labeled `Person` and then returns
the `name` values in the results.

```zetasql
GRAPH FinGraph
MATCH (p:Person)
RETURN p.name

/*---------+
 | name    |
 +---------+
 | Alex    |
 | Dana    |
 | Lee     |
 +---------*/
```

[hints]: https://github.com/google/zetasql/blob/master/docs/lexical.md#hints

[element-definition]: #element_definition

[label-property-definition]: #label_property_definition

[element-table-property-definition]: #element_table_property_definition

[fin-graph]: #fin_graph

[gql]: https://github.com/google/zetasql/blob/master/docs/graph-query-statements.md

## `DROP PROPERTY GRAPH` statement 
<a id="gql_drop_graph"></a>

<pre>
DROP PROPERTY GRAPH [ IF EXISTS ] <span class="var">property_graph_name</span>;
</pre>

**Description**

Deletes a property graph.

**Definitions**

+ `IF EXISTS`: If a property graph of the specified name doesn't exist, then the
  DROP statement has no effect and no error is generated.
+ `property_graph_name`: The name of the property graph to drop.

**Example**

```zetasql
DROP PROPERTY GRAPH FinGraph;
```

