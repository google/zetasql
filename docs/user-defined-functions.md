

<!-- This file is auto-generated. DO NOT EDIT.                               -->

# User-Defined Functions

<!-- BEGIN CONTENT -->

ZetaSQL supports
user-defined functions (UDFs). A UDF enables you to create a function using
another SQL expression or another programming
language, such as JavaScript. These functions accept columns of input and perform actions,
returning the result of those actions as a value.

##  General UDF and TVF  Syntax

User-defined functions and table-valued
functions in ZetaSQL use the
following general syntax:

<pre>
CREATE [TEMPORARY | TEMP] [TABLE | AGGREGATE] FUNCTION
  <span class="var">function_name</span> ([<span class="var">function_parameter</span>[, ...]])
  [RETURNS { <span class="var">data_type</span> | TABLE&lt;<span class="var">argument_name data_type</span> [, ...]*&gt; }]
  { [LANGUAGE <span class="var">language</span> AS <span class="var">"""body"""</span>] | [AS (<span class="var">function_definition</span>)] };

<span class="var">function_parameter</span>:
  <span class="var">param_name param_type</span> [NOT AGGREGATE]
</pre>

This syntax consists of the following components:

+   **CREATE [TEMPORARY | TEMP]
    [TABLE | AGGREGATE] FUNCTION**.
    Creates a new function. A function can contain zero or more
    `function_parameter`s. To make the function
    temporary, use the `TEMP` or `TEMPORARY` keyword. To
    make a [table-valued function][table-valued function], use the `TABLE`
    keyword. To make
    an [aggregate function][aggregate-udf-parameters], use the
    `AGGREGATE` keyword.
*   **function_parameter**. Consists of a comma-separated `param_name` and
    `param_type` pair. The value of `param_type` is a ZetaSQL
    [data type][data-types].
    The value of `param_type` may also be `ANY TYPE` for a
    SQL user-defined function or `ANY TABLE` for a table-valued function.
+   **[RETURNS data_type]**. Specifies the data type
    that the function returns. If the function is defined in SQL, then the
    `RETURNS` clause is optional and ZetaSQL infers the result type
    of the function from the SQL function body. If the
    function is defined in an external language, then the `RETURNS` clause is
    required. See [Supported UDF data types][supported-external-udf-data-types]
    for more information about allowed values for
    `data_type`.
+   **[RETURNS TABLE]**. Specifies the schema of the table that a table-valued
    function returns as a comma-separated list of `argument_name` and `TYPE`
    pairs. Can only appear after `CREATE TEMP TABLE FUNCTION`. If
    `RETURNS TABLE` is absent, ZetaSQL infers the output schema from
    the `AS SELECT...` statement in the function body.
    
*   **[LANGUAGE language AS """body"""]**. Specifies the external language
    for the function and the code that defines the function.
*   **AS (function_definition)**. Specifies the SQL code that defines the
    function. For user-defined functions (UDFs),
    `function_definition` is a SQL expression. For table-valued functions
    (TVFs), `function_definition` is a SQL query.
*   **[NOT AGGREGATE]**. Specifies that a parameter is not
    aggregate. Can only appear after `CREATE AGGREGATE FUNCTION`. A
    non-aggregate parameter [can appear anywhere in the function
    definition.][aggregate-udf-parameters]

## External UDF structure

Create external UDFs using the following structure.

```sql
CREATE [TEMPORARY | TEMP] FUNCTION function_name ([function_parameter[, ...]])
  [RETURNS data_type]
  [LANGUAGE language]
  AS external_code
```

## External UDF examples

The following example creates a UDF.

```sql
CREATE FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, multiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can make this function temporary by using the `TEMP` or `TEMPORARY`
keyword.

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, multiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can create multiple UDFs before a query. For example:

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION divideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x / 2;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x,
  y,
  multiplyInputs(x, y) as product,
  divideByTwo(x) as half_x,
  divideByTwo(y) as half_y
FROM numbers;

+-----+-----+--------------+--------+--------+
| x   | y   | product      | half_x | half_y |
+-----+-----+--------------+--------+--------+
| 1   | 5   | 5            | 0.5    | 2.5    |
| 2   | 10  | 20           | 1      | 5      |
| 3   | 15  | 45           | 1.5    | 7.5    |
+-----+-----+--------------+--------+--------+
```

You can pass the result of a UDF as input to another UDF. For example:

```sql
CREATE TEMP FUNCTION multiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION divideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x/2;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x,
  y,
  multiplyInputs(divideByTwo(x), divideByTwo(y)) as half_product
FROM numbers;

+-----+-----+--------------+
| x   | y   | half_product |
+-----+-----+--------------+
| 1   | 5   | 1.25         |
| 2   | 10  | 5            |
| 3   | 15  | 11.25        |
+-----+-----+--------------+
```

 The following example sums the values of all
fields named "foo" in the given JSON string.

```sql
CREATE TEMP FUNCTION SumFieldsNamedFoo(json_row STRING)
  RETURNS FLOAT64
  LANGUAGE js AS """
function SumFoo(obj) {
  var sum = 0;
  for (var field in obj) {
    if (obj.hasOwnProperty(field) && obj[field] != null) {
      if (typeof obj[field] == "object") {
        sum += SumFoo(obj[field]);
      } else if (field == "foo") {
        sum += obj[field];
      }
    }
  }
  return sum;
}
var row = JSON.parse(json_row);
return SumFoo(row);
""";

WITH Input AS (
  SELECT STRUCT(1 AS foo, 2 AS bar, STRUCT('foo' AS x, 3.14 AS foo) AS baz) AS s, 10 AS foo UNION ALL
  SELECT NULL, 4 AS foo UNION ALL
  SELECT STRUCT(NULL, 2 AS bar, STRUCT('fizz' AS x, 1.59 AS foo) AS baz) AS s, NULL AS foo
)
SELECT
  TO_JSON_STRING(t) AS json_row,
  SumFieldsNamedFoo(TO_JSON_STRING(t)) AS foo_sum
FROM Input AS t;
+---------------------------------------------------------------------+---------+
| json_row                                                            | foo_sum |
+---------------------------------------------------------------------+---------+
| {"s":{"foo":1,"bar":2,"baz":{"x":"foo","foo":3.14}},"foo":10}       | 14.14   |
| {"s":null,"foo":4}                                                  | 4       |
| {"s":{"foo":null,"bar":2,"baz":{"x":"fizz","foo":1.59}},"foo":null} | 1.59    |
+---------------------------------------------------------------------+---------+
```

### Supported external UDF languages

External UDFs support code written in JavaScript, which you specify using `js`
as the `LANGUAGE`. For example:

```sql
CREATE TEMP FUNCTION greeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
  return "Hello, " + a + "!";
  """;
SELECT greeting(name) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS name;

+----------------+
| everyone       |
+----------------+
| Hello, Hannah! |
| Hello, Max!    |
| Hello, Jakob!  |
+----------------+
```

See [SQL type encodings in JavaScript](#sql-type-encodings-in-javascript) for
information on how ZetaSQL data types map to JavaScript types.

### Supported external UDF data types

For external UDFs, ZetaSQL supports the following data types:

<ul>
<li>ARRAY</li><li>BOOL</li><li>BYTES</li><li>DATE</li><li>DOUBLE</li><li>FLOAT</li><li>INT32</li><li>NUMERIC</li><li>STRING</li><li>STRUCT</li><li>TIMESTAMP</li>
</ul>

### SQL type encodings in JavaScript

Some SQL types have a direct mapping to JavaScript types, but others do not.

Because JavaScript does not support a 64-bit integer type,
`INT64` is unsupported as an input type for JavaScript
UDFs. Instead, use `DOUBLE` to represent integer
values as a number, or `STRING` to represent integer
values as a string.

ZetaSQL does support `INT64` as a return type
in JavaScript UDFs. In this case, the JavaScript function body can return either
a JavaScript Number or a String. ZetaSQL then converts either of
these types to `INT64`.

ZetaSQL represents types in the following manner:

<table>
<tr>
<th>ZetaSQL Data Type</th>
<th>JavaScript Data Type</th>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
</tr>

<tr>
<td>BOOL</td>
<td>BOOLEAN</td>
</tr>

<tr>
<td>BYTES</td>
<td>base64-encoded STRING</td>
</tr>

<tr>
<td>DOUBLE</td>
<td>NUMBER</td>
</tr>

<tr>
<td>FLOAT</td>
<td>NUMBER</td>
</tr>

<tr>
<td>NUMERIC</td>
<td>If a NUMERIC value can be represented exactly as an
  <a href="https://en.wikipedia.org/wiki/Floating-point_arithmetic#IEEE_754:_floating_point_in_modern_computers">IEEE 754 floating-point</a>
  value and has no fractional part, it is encoded as a Number. These values are
  in the range [-2<sup>53</sup>, 2<sup>53</sup>]. Otherwise, it is encoded as a
  String.</td>
</tr>

<tr>
<td>INT32</td>
<td>NUMBER</td>
</tr>

<tr>
<td>STRING</td>
<td>STRING</td>
</tr>

<tr>
<td>STRUCT</td>
<td>OBJECT where each STRUCT field is a named field</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td>DATE with a microsecond field containing the <code>microsecond</code>
fraction of the timestamp</td>
</tr>

<tr>
<td>DATE</td>
<td>DATE</td>
</tr>

<tr>
<td>UINT32</td>
<td>NUMBER</td>
</tr>

</table>

### Quoting rules

You must enclose external code in quotes. For simple, one line code snippets,
you can use a standard quoted string:

```sql
CREATE TEMP FUNCTION plusOne(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js
AS "return x+1;";
SELECT val, plusOne(val) AS result
FROM UNNEST([1, 2, 3, 4, 5]) AS val;

+-----------+-----------+
| val       | result    |
+-----------+-----------+
| 1         | 2         |
| 2         | 3         |
| 3         | 4         |
| 4         | 5         |
| 5         | 6         |
+-----------+-----------+
```

In cases where the snippet contains quotes, or consists of multiple lines, use
triple-quoted blocks:

```sql
CREATE TEMP FUNCTION customGreeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
  var d = new Date();
  if (d.getHours() < 12) {
    return 'Good Morning, ' + a + '!';
  } else {
    return 'Good Evening, ' + a + '!';
  }
  """;
SELECT customGreeting(names) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS names;
+-----------------------+
| everyone              |
+-----------------------+
| Good Morning, Hannah! |
| Good Morning, Max!    |
| Good Morning, Jakob!  |
+-----------------------+
```

## SQL UDF structure

Create SQL UDFs using the following syntax:

<pre>
CREATE [TEMPORARY | TEMP] [AGGREGATE] FUNCTION <span class="var">function_name</span> ([<span class="var">function_parameter</span>[, ...]])
  [RETURNS <span class="var">data_type</span>]
  AS (<span class="var">sql_expression</span>)

<span class="var">function_parameter</span>:
  <span class="var">param_name param_type</span> [NOT AGGREGATE]
</pre>

### Aggregate UDF parameters

An aggregate UDF can take aggregate or non-aggregate parameters. Like other
[aggregate functions][aggregate-fns-link], aggregate UDFs normally aggregate
parameters across all rows in a [group][group-by-link]. However, you can specify
a parameter as non-aggregate with the `NOT AGGREGATE` keyword. A non-aggregate
parameter is a scalar argument with a constant value for all rows in a group;
for example, literals or grouping expressions are valid non-aggregate
parameters. Inside the UDF definition, aggregate parameters can only appear as
arguments to aggregate function calls. Non-aggregate parameters can appear
anywhere in the UDF definition.

### Templated SQL UDF parameters

A templated parameter can match more than one argument type at function call
time. If a function signature includes a templated parameter, ZetaSQL
allows function calls to pass one of several argument types to the function.

SQL user-defined function signatures can contain the following templated
`param_type` value:

+ `ANY TYPE`. The function will accept an input of any type for this argument.
  If more than one parameter has the type `ANY TYPE`, ZetaSQL does
  not enforce any relationship between these arguments at the time of function
  creation. However, passing the function arguments of types that are
  incompatible with the function definition will result in an error at call
  time.

## SQL UDF examples

The following example shows a UDF that employs a SQL function.

```sql
CREATE TEMP FUNCTION addFourAndDivide(x INT64, y INT64) AS ((x + 4) / y);
WITH numbers AS
  (SELECT 1 as val
  UNION ALL
  SELECT 3 as val
  UNION ALL
  SELECT 4 as val
  UNION ALL
  SELECT 5 as val)
SELECT val, addFourAndDivide(val, 2) AS result
FROM numbers;

+-----+--------+
| val | result |
+-----+--------+
| 1   | 2.5    |
| 3   | 3.5    |
| 4   | 4      |
| 5   | 4.5    |
+-----+--------+
```

The following example shows an aggregate UDF that uses a non-aggregate
parameter. Inside the function definition, the aggregate `SUM` method takes the
aggregate parameter `dividend`, while the non-aggregate division operator
( `/` ) takes the non-aggregate parameter `divisor`.

```sql
CREATE TEMP AGGREGATE FUNCTION scaled_sum(dividend DOUBLE, divisor DOUBLE NOT AGGREGATE)
AS (SUM(dividend) / divisor);
SELECT scaled_sum(col1, 2) AS scaled_sum
FROM (SELECT 1 AS col1 UNION ALL
      SELECT 3 AS col1 UNION ALL
      SELECT 5 AS col1
);

+------------+
| scaled_sum |
+------------+
| 4.5        |
+------------+
```

The following example shows a SQL UDF that uses a
[templated parameter][templated-parameters]. The resulting function
accepts arguments of various types.

```sql
CREATE TEMP FUNCTION addFourAndDivideAny(x ANY TYPE, y ANY TYPE) AS (
  (x + 4) / y
);
SELECT addFourAndDivideAny(3, 4) AS integer_output,
       addFourAndDivideAny(1.59, 3.14) AS floating_point_output;

+----------------+-----------------------+
| integer_output | floating_point_output |
+----------------+-----------------------+
| 1.75           | 1.7802547770700636    |
+----------------+-----------------------+
```

The following example shows a SQL UDF that uses a
[templated parameter][templated-parameters] to return the last
element of an array of any type.

```sql
CREATE TEMP FUNCTION lastArrayElement(arr ANY TYPE) AS (
  arr[ORDINAL(ARRAY_LENGTH(arr))]
);
SELECT
  names[OFFSET(0)] AS first_name,
  lastArrayElement(names) AS last_name
FROM (
  SELECT ['Fred', 'McFeely', 'Rogers'] AS names UNION ALL
  SELECT ['Marie', 'Skłodowska', 'Curie']
);

+------------+-----------+
| first_name | last_name |
+------------+-----------+
| Fred       | Rogers    |
| Marie      | Curie     |
+------------+-----------+
```

## Table-valued functions {#tvfs}

A *table-valued function* (TVF) is a function that returns an entire output
table instead of a single scalar value, and appears in the FROM clause like a
table subquery.

### TVF structure

You create a TVF using the following structure.

```sql
CREATE TEMPORARY | TEMP TABLE FUNCTION function_name
  ([argument_name data_type][, ...]*)
  [RETURNS TABLE<argument_name data_type [, ...]*>]
  [[LANGUAGE language AS """body"""] | [AS SELECT...]];
```

### Specifying TVF arguments {#tvf-arguments}

An argument to a TVF can be of any supported ZetaSQL type or a table.

Specify a table argument the same way you specify the fields of a
[STRUCT][data-types-struct].

```sql
argument_name TABLE<column_name data_type , column_name data_type [, ...]*>
```

The table argument can specify a [value table][datamodel-value-tables],
in which each row
is a single column of a specific type. To specify a value table as an argument,
include only the `data_type`, leaving out the `column_name`:

```sql
argument_name TABLE<data_type>
```

In many cases, the `data_type` of the single column in the value table is a
protocol buffer; for example:

```sql
CREATE TEMP TABLE FUNCTION AggregatedMovieLogs(
  TicketPurchases TABLE<analysis_conduit.codelab.MovieTicketPurchase>)
```

The function body can refer directly to fields within the proto.

You have the option to specify the input table using the templated type `ANY
TABLE` in place of `TABLE<column_name data_type [, ...]*>`. This option enables
you to create a polymorphic TVF that accepts any table as input.

**Example**

The following example implements a pair of TVFs that define parameterized views
of a range of rows from the Customer table. The first returns all rows for a
range of CustomerIds; the second calls the first function and applies an
additional filter based on CustomerType.

```sql
CREATE TEMP TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)
  AS
    SELECT *
    FROM Customer
    WHERE CustomerId >= MinId AND CustomerId <= MaxId;

CREATE TEMP TABLE FUNCTION CustomerRangeWithCustomerType(
    MinId INT64,
    MaxId INT64,
    customer_type ads.boulder.schema.CustomerType)
  AS
    SELECT * FROM CustomerRange(MinId, MaxId)
    WHERE Info.type = customer_type;
```

#### Templated SQL TVF Parameters

SQL table-valued function signatures can contain the following [templated
parameter][templated-parameters] values for `param_type`:

+ `ANY TYPE`. The function will accept an input of any type for this argument.
  If more than one parameter has the type `ANY TYPE`, ZetaSQL does
  not enforce any relationship between these arguments at the time of function
  creation. However, passing the function arguments of types that are
  incompatible with the function definition will result in an error at call
  time.
+ `ANY TABLE`. The function will accept an
argument of any relation type for this argument. However, passing the function
arguments of types that are incompatible with the function definition will
result in an error at call time.

**Examples**

The following function returns all rows from the input table if the first
argument is greater than the second argument; otherwise it returns no rows.

```sql
CREATE TEMP TABLE FUNCTION MyFunction(
     first ANY TYPE,
     second ANY TYPE,
     third ANY TABLE)
   AS
     SELECT *
     FROM third
     WHERE first > second;

```

The following function accepts two integers and a table with any set of columns
and returns rows from the table where the predicate evaluates to true. The input
table `selected_customers` must contain a column named `Info` that has a field
named `creation_time`, and `creation_time` must be a numeric type, or the
function will return an error.

```sql
CREATE TEMP TABLE FUNCTION CustomerInfoProtoCreationTimeRange(
    min_creation_time INT64,
    max_creation_time INT64,
    selected_customers ANY TABLE)
  AS
    SELECT *
    FROM selected_customers
    WHERE Info.creation_time >= min_creation_time
    AND Info.creation_time <= max_creation_time;
```

### Calling TVFs

To call a TVF, use the function call in place of the table name in a `FROM`
clause.

**Example**

The following query calls the `CustomerRangeWithCustomerType` function to
return a table with rows for customers with a CustomerId between 100
and 200.

```sql
SELECT CustomerId, Info
FROM CustomerRangeWithCustomerType(100, 200, 'CUSTOMER_TYPE_ADVERTISER');
```

[table-valued function]: #tvfs
[aggregate-udf-parameters]: #aggregate-udf-parameters
[templated-parameters]: #templated-sql-udf-parameters
[supported-external-udf-data-types]: #supported-external-udf-data-types
[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types
[data-types-struct]: https://github.com/google/zetasql/blob/master/docs/data-types#struct_type
[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model#value-tables
[group-by-link]: https://github.com/google/zetasql/blob/master/docs/query-syntax#group_by_clause
[aggregate-fns-link]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions

<!-- END CONTENT -->

