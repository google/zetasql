

# User-defined functions

ZetaSQL
supports user-defined functions (UDFs). A UDF enables you to create a function
using another SQL expression or another programming
language, such as JavaScript or Lua. These functions accept columns of input
and perform actions, returning the result of those actions as a value.

## Scalar SQL UDFs 
<a id="sql_udfs"></a>

A scalar SQL UDF is a user-defined function that returns a single value.

### Scalar SQL UDF structure 
<a id="sql_udf_structure"></a>

Create a scalar SQL UDF using the following syntax:

```sql
CREATE
  [ { TEMPORARY | TEMP } ] FUNCTION
  function_name ( [ function_parameter [, ...] ] )
  [ RETURNS data_type ]
  AS ( sql_expression )

function_parameter:
  parameter_name { data_type | ANY TYPE }
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function. A function can have zero
    or more function parameters.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
     exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function.

    + `parameter_name`: The name of the parameter.

    + `data_type`: A ZetaSQL [data type][data-types].

    + `ANY TYPE`: The function will accept an argument of any type for this
      function parameter. If more than one parameter includes `ANY TYPE`,
      a relationship is not enforced between these parameters when the function
      is defined. However, if the type of argument passed into the function at
      call time is incompatible with the function definition, this will
      result in an error.

      `ANY TYPE` is a [_templated function parameter_][templated-parameters].
+   `RETURNS data_type`: Optional clause that specifies the data type
    that the function returns. ZetaSQL infers the result type
    of the function from the SQL function body when the `RETURN` clause is
    omitted.
+   `AS (sql_expression)`: Specifies the SQL code that defines the
    function.

### Scalar SQL UDF examples 
<a id="sql_udf_structure"></a>

The following example shows a UDF that employs a SQL function.

```sql
CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
RETURNS DOUBLE
AS (
  (x + 4) / y
);

WITH
  numbers AS (
    SELECT 1 AS val UNION ALL
    SELECT 3 AS val UNION ALL
    SELECT 4 AS val UNION ALL
    SELECT 5 AS val
  )
SELECT val, AddFourAndDivide(val, 2) AS result
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

The following example shows a scalar SQL UDF that uses the
templated function parameter, `ANY TYPE`. The resulting function accepts
arguments of various types.

```sql
CREATE TEMP FUNCTION AddFourAndDivideAny(x ANY TYPE, y ANY TYPE)
AS (
  (x + 4) / y
);

SELECT
  AddFourAndDivideAny(3, 4) AS integer_output,
  AddFourAndDivideAny(1.59, 3.14) AS floating_point_output;

+----------------+-----------------------+
| integer_output | floating_point_output |
+----------------+-----------------------+
| 1.75           | 1.7802547770700636    |
+----------------+-----------------------+
```

The following example shows a scalar SQL UDF that uses the
templated function parameter, `ANY TYPE`, to return the last element of an
array of any type.

```sql
CREATE TEMP FUNCTION LastArrayElement(arr ANY TYPE)
AS (
  arr[ORDINAL(ARRAY_LENGTH(arr))]
);

SELECT
  names[OFFSET(0)] AS first_name,
  LastArrayElement(names) AS last_name
FROM
  (
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

## Aggregate SQL UDFs

An aggregate UDF is a user-defined function that performs a calculation on
multiple values and returns the result of that calculation as a single value.

### Aggregate SQL UDF structure

Create an aggregate SQL UDF using the following syntax:

```sql
CREATE
  [ { TEMPORARY | TEMP } ] [ AGGREGATE ] FUNCTION
  function_name ( [ function_parameter [, ...] ] )
  [ RETURNS data_type ]
  AS ( sql_expression )

function_parameter:
  parameter_name data_type [ NOT AGGREGATE ]
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function. A function can have zero
    or more function parameters.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
    exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function.

    + `parameter_name`: The name of the function parameter.

    + `data_type`: A ZetaSQL [data type][data-types].

    + `NOT AGGREGATE`: Specifies that a function parameter is not an
      aggregate. A non-aggregate function parameter can appear anywhere in the
      function definition. You can learn more about
      [function parameters for aggregate UDFS][aggregate-udf-parameters]
      in the next section.
+   `RETURNS data_type`: Optional clause that specifies the data type
    that the function returns. ZetaSQL infers the result type
    of the function from the SQL function body when the `RETURN` clause is
    omitted.
+   `AS (sql_expression)`: Specifies the SQL code that defines the
    function.

### Aggregate UDF function parameters 
<a id="aggregate_udf_parameters"></a>

An aggregate UDF can include aggregate or non-aggregate function parameters.
Like other [aggregate functions][aggregate-fns-link], aggregate UDFs normally
aggregate function parameters across all rows in a [group][group-by-link].
However, you can specify a function parameter as non-aggregate with the
`NOT AGGREGATE` keyword. A non-aggregate function parameter is a
scalar function parameter with a constant value for all rows in a group;
for example, literals or grouping expressions are valid non-aggregate
function parameters. Inside the UDF definition, aggregate function parameters
can only appear as function parameters to aggregate function calls.
Non-aggregate function parameters can appear anywhere in the UDF definition.

### Aggregate SQL UDF examples

The following example shows an aggregate UDF that uses a non-aggregate
function parameter. Inside the function definition, the aggregate `SUM` method
takes the aggregate function parameter `dividend`, while the non-aggregate
division operator ( `/` ) takes the non-aggregate function parameter `divisor`.

```sql
CREATE TEMP AGGREGATE FUNCTION ScaledSum(
  dividend DOUBLE,
  divisor DOUBLE NOT AGGREGATE)
RETURNS DOUBLE
AS (
  SUM(dividend) / divisor
);

SELECT ScaledSum(col1, 2) AS scaled_sum
FROM (
  SELECT 1 AS col1 UNION ALL
  SELECT 3 AS col1 UNION ALL
  SELECT 5 AS col1
);

+------------+
| scaled_sum |
+------------+
| 4.5        |
+------------+
```

## JavaScript UDFs 
<a id="javascript_udfs"></a>

A JavaScript UDF is a SQL user-defined function that executes
JavaScript code and returns the result as a single value.

### JavaScript UDF structure 
<a id="javascript_structure"></a>

Create a JavaScript UDF using the following syntax.

```sql
CREATE
  [ { TEMPORARY | TEMP } ] FUNCTION
  function_name ( [ function_parameter [, ...] ] )
  RETURNS data_type
  [ determinism_specifier ]
  LANGUAGE js AS string_literal

function_parameter:
  parameter_name data_type

determinism_specifier:
  { IMMUTABLE | DETERMINISTIC | NOT DETERMINISTIC | VOLATILE | STABLE }
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function. A function can contain zero
    or more function parameters.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
    exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function. A parameter includes a
    name and a data type. The value of `data_type` is a ZetaSQL
    [data type][data-types].
+   `determinism_specifier`: Identifies the determinism property of the
    function, which impacts query semantics and planning. Your choices are:

    +   `IMMUTABLE` or `DETERMINISTIC`: The function always returns the same
        result when passed the same arguments. For example, if the function
        `add_one(i)` always returns `i + 1`, the function is deterministic.

    +   `NOT DETERMINISTIC`: The function does not always return the same result
        when passed the same arguments. The `VOLATILE` and `STABLE` keywords are
        subcategories of `NOT DETERMINISTIC`.

    +   `VOLATILE`: The function does not always return the same result when
        passed the same arguments, even within the same run of a query
        statement. For example if `add_random(i)` returns `i + rand()`, the
        function is volatile, because every call to the function can return a
        different result.

    +   `STABLE`: Within one execution of a query statement, the function will
        consistently return the same result for the same argument values.
        However, the result could change for different executions of the
        same query. For example if you invoke the function `CURRENT_TIMESTAMP`
        within a single query, it will return the same result, but it will
        return different results in different query executions.
+   `RETURNS data_type`: Specifies the data type that the function returns.
+   `LANGUAGE ... AS`: Specify the language and code to use. `js`
    represents the name of the language. `string_literal` represents the code
    that defines the function body.

<a id="quoting_rules"></a>
You must enclose JavaScript in quotes. There are a few options:

+ `"..."`: For simple, one line code snippets that don't contain quotes or
  escaping, you can use a standard quoted string.
+ `"""..."""`: If the snippet contains quotes or multiple lines, use
  triple-quoted blocks.
+ `R"""..."""`: If the snippet contains escaping, prefix a triple-quoted
  block with an `R` to indicate that this is a raw string that should ignore
  escaping rules. If you are not sure which quoting style to use, this one
  will provide the most consistent results.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

<!-- mdlint on -->

### SQL type encodings in JavaScript

ZetaSQL represents types in the following manner:

<table>
  <tr>
  <th>SQL Data Type</th>
  <th>JavaScript Data Type</th>
  <th>Notes</th>
  </tr>

  
  <tr>
    <td>ARRAY</td>
    <td>Array</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>BOOL</td>
    <td>Boolean</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>BYTES</td>
    <td>String</td>
    <td>Base64-encoded String.</td>
  </tr>
  

  
  <tr>
    <td>DOUBLE</td>
    <td>Number</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>FLOAT</td>
    <td>Number</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>
      NUMERIC
    </td>
    <td>
      Number or String
    </td>
    <td>
      If a NUMERIC value can be represented exactly as an
      <a href="https://en.wikipedia.org/wiki/Floating-point_arithmetic#IEEE_754:_floating_point_in_modern_computers">IEEE 754 floating-point</a>
      value and has no fractional part, it is encoded as a Number. These values
      are in the range [-2<sup>53</sup>, 2<sup>53</sup>]. Otherwise, it is
      encoded as a String.
    </td>
  </tr>
  

  
  <tr>
    <td>
      BIGNUMERIC
    </td>
    <td>
      Number or String
    </td>
    <td>
      Same as NUMERIC.
    </td>
  </tr>
  

  
  <tr>
    <td>INT32</td>
    <td>Number</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>UINT32</td>
    <td>Number</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>INT64</td>
    <td>
      
      See notes
      
    </td>
    <td>
      
      See the documentation for your database engine.
      
    </td>
  </tr>
  

  
  <tr>
    <td>
      UINT64
    </td>
    <td>
      
      See notes
      
    </td>
    <td>
      Same as INT64.
    </td>
  </tr>
  

  
  <tr>
    <td>STRING</td>
    <td>String</td>
    <td></td>
  </tr>
  

  
  <tr>
    <td>STRUCT</td>
    <td>Object</td>
    <td>
      
      See the documentation for your database engine.
      
    </td>
  </tr>
  

  
  <tr>
    <td>TIMESTAMP</td>
    <td>Date object</td>
    <td>
      
      See the documentation for your database engine.
      
    </td>
  </tr>
  

  
  <tr>
    <td>DATE</td>
    <td>Date object</td>
    <td></td>
  </tr>
  

</table>

### JavaScript UDF examples 
<a id="javascript_examples"></a>

The following example creates a persistent JavaScript UDF.

```sql
CREATE FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
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
SELECT x, y, MultiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

The following example creates a temporary JavaScript UDF.

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
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
SELECT x, y, MultiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can create multiple JavaScript UDFs before a query. For example:

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION DivideByTwo(x DOUBLE)
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
  MultiplyInputs(x, y) as product,
  DivideByTwo(x) as half_x,
  DivideByTwo(y) as half_y
FROM numbers;

+-----+-----+--------------+--------+--------+
| x   | y   | product      | half_x | half_y |
+-----+-----+--------------+--------+--------+
| 1   | 5   | 5            | 0.5    | 2.5    |
| 2   | 10  | 20           | 1      | 5      |
| 3   | 15  | 45           | 1.5    | 7.5    |
+-----+-----+--------------+--------+--------+
```

You can pass the result of a JavaScript UDF as input to another UDF.
For example:

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE js AS """
  return x*y;
""";
CREATE TEMP FUNCTION DivideByTwo(x DOUBLE)
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
  MultiplyInputs(DivideByTwo(x), DivideByTwo(y)) as half_product
FROM numbers;

+-----+-----+--------------+
| x   | y   | half_product |
+-----+-----+--------------+
| 1   | 5   | 1.25         |
| 2   | 10  | 5            |
| 3   | 15  | 11.25        |
+-----+-----+--------------+
```

The following provides an example of a simple, single statement JavaScript UDF:

```sql
CREATE TEMP FUNCTION PlusOne(x DOUBLE)
RETURNS DOUBLE
LANGUAGE js
AS "return x+1;";
SELECT val, PlusOne(val) AS result
FROM UNNEST([1, 2, 3]) AS val;

+-----------+-----------+
| val       | result    |
+-----------+-----------+
| 1         | 2         |
| 2         | 3         |
| 3         | 4         |
+-----------+-----------+
```

The following example illustrates a more complex, multi-statement
JavaScript UDF.  Note that a triple-quoted multi-line string is used in this
example for readability.

```sql
CREATE TEMP FUNCTION CustomGreeting(a STRING)
RETURNS STRING
LANGUAGE js
AS """
  var d = new Date();
  if (d.getHours() < 12) {
    return 'Good Morning, ' + a + '!';
  } else {
    return 'Good Evening, ' + a + '!';
  }
  """;
SELECT CustomGreeting(names) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS names;

+-----------------------+
| everyone              |
+-----------------------+
| Good Morning, Hannah! |
| Good Morning, Max!    |
| Good Morning, Jakob!  |
+-----------------------+
```

The following example demonstrates how to utilize JavaScript escaping within the
triple-quoted multi-line string.

```sql
CREATE TEMP FUNCTION PlusOne(x STRING)
RETURNS STRING
LANGUAGE js
AS R"""
var re = /[a-z]/g;
return x.match(re);
""";

SELECT val, PlusOne(val) AS result
FROM UNNEST(['ab-c', 'd_e', '!']) AS val;

+---------+
| result  |
+---------+
| [a,b,c] |
| [d,e]   |
| NULL    |
+---------+
```

The following example sums the values of all
fields named `foo` in the given JSON string.

```sql
CREATE TEMP FUNCTION SumFieldsNamedFoo(json_row STRING)
RETURNS FLOAT64
LANGUAGE js
AS """
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

WITH
  Input AS (
    SELECT
      STRUCT(1 AS foo, 2 AS bar, STRUCT('foo' AS x, 3.14 AS foo) AS baz) AS s,
      10 AS foo
    UNION ALL
    SELECT NULL, 4 AS foo
    UNION ALL
    SELECT
      STRUCT(NULL, 2 AS bar, STRUCT('fizz' AS x, 1.59 AS foo) AS baz) AS s,
      NULL AS foo
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

## LUA UDFs 
<a id="lua_udfs"></a>

A LUA UDF is a SQL user-defined function that executes
LUA code and returns the result as a single value.

### LUA UDF structure 
<a id="lua_structure"></a>

Create a LUA UDF using the following syntax.

```sql
CREATE
  [ { TEMPORARY | TEMP } ] FUNCTION
  function_name ( [ function_parameter [, ...] ] )
  RETURNS data_type
  [ determinism_specifier ]
  LANGUAGE lua AS string_literal

function_parameter:
  parameter_name data_type

determinism_specifier:
  { IMMUTABLE | DETERMINISTIC | NOT DETERMINISTIC | VOLATILE | STABLE }
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function. A function can contain zero
    or more function parameters.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
    exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function. A parameter includes a
    name and a data type. The value of `data_type` is a ZetaSQL
    [data type][data-types].
+   `determinism_specifier`: Identifies the determinism property of the
    function, which impacts query semantics and planning. Your choices are:

    +   `IMMUTABLE` or `DETERMINISTIC`: The function always returns the same
        result when passed the same arguments. For example, if the function
        `add_one(i)` always returns `i + 1`, the function is deterministic.

    +   `NOT DETERMINISTIC`: The function does not always return the same result
        when passed the same arguments. The `VOLATILE` and `STABLE` keywords are
        subcategories of `NOT DETERMINISTIC`.

    +   `VOLATILE`: The function does not always return the same result when
        passed the same arguments, even within the same run of a query
        statement. For example if `add_random(i)` returns `i + rand()`, the
        function is volatile, because every call to the function can return a
        different result.

    +   `STABLE`: Within one execution of a query statement, the function will
        consistently return the same result for the same argument values.
        However, the result could change for different executions of the
        same query. For example if you invoke the function `CURRENT_TIMESTAMP`
        within a single query, it will return the same result, but it will
        return different results in different query executions.
+   `RETURNS data_type`: Specifies the data type that the function returns.
+   `LANGUAGE ... AS`: Specify the language and code to use. `lua`
    represents the name of the language. `string_literal` represents the code
    that defines the function body.

<a id="quoting_rules"></a>
You must enclose LUA in quotes. There are a few options:

+ `"..."`: For simple, one line code snippets that don't contain quotes or
  escaping, you can use a standard quoted string.
+ `"""..."""`: If the snippet contains quotes or multiple lines, use
  triple-quoted blocks.
+ `R"""..."""`: If the snippet contains escaping, prefix a triple-quoted
  block with an `R` to indicate that this is a raw string that should ignore
  escaping rules. If you are not sure which quoting style to use, this one
  will provide the most consistent results.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

<!-- mdlint on -->

### LUA UDF examples 
<a id="lua_examples"></a>

The following example creates a persistent LUA UDF.

```sql
CREATE FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, MultiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

The following example creates a temporary LUA UDF.

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
  return x*y;
""";
WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, MultiplyInputs(x, y) as product
FROM numbers;

+-----+-----+--------------+
| x   | y   | product      |
+-----+-----+--------------+
| 1   | 5   | 5            |
| 2   | 10  | 20           |
| 3   | 15  | 45           |
+-----+-----+--------------+
```

You can create multiple LUA UDFs before a query. For example:

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
  return x*y;
""";
CREATE TEMP FUNCTION DivideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
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
  MultiplyInputs(x, y) as product,
  DivideByTwo(x) as half_x,
  DivideByTwo(y) as half_y
FROM numbers;

+-----+-----+--------------+--------+--------+
| x   | y   | product      | half_x | half_y |
+-----+-----+--------------+--------+--------+
| 1   | 5   | 5            | 0.5    | 2.5    |
| 2   | 10  | 20           | 1      | 5      |
| 3   | 15  | 45           | 1.5    | 7.5    |
+-----+-----+--------------+--------+--------+
```

You can pass the result of a LUA UDF as input to another UDF.
For example:

```sql
CREATE TEMP FUNCTION MultiplyInputs(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
  return x*y;
""";
CREATE TEMP FUNCTION DivideByTwo(x DOUBLE)
RETURNS DOUBLE
LANGUAGE lua AS """
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
  MultiplyInputs(DivideByTwo(x), DivideByTwo(y)) as half_product
FROM numbers;

+-----+-----+--------------+
| x   | y   | half_product |
+-----+-----+--------------+
| 1   | 5   | 1.25         |
| 2   | 10  | 5            |
| 3   | 15  | 11.25        |
+-----+-----+--------------+
```

The following provides an example of a simple, single statement LUA UDF:

```sql
CREATE TEMP FUNCTION PlusOne(x DOUBLE)
RETURNS DOUBLE
LANGUAGE lua
AS "return x+1;";

SELECT val, PlusOne(val) AS result
FROM UNNEST([1, 2, 3]) AS val;

+-----------+-----------+
| val       | result    |
+-----------+-----------+
| 1         | 2         |
| 2         | 3         |
| 3         | 4         |
+-----------+-----------+
```

The following example illustrates a more complex, multi-statement
LUA UDF.  Note that a triple-quoted multi-line string is used in this
example for readability.

```sql
CREATE TEMP FUNCTION CustomGreeting(i INT32)
RETURNS STRING
LANGUAGE lua
AS """
  if i < 12 then
    return 'Good Morning!'
  else
    return 'Good Evening!'
  end
  """;

SELECT CustomGreeting(13) AS message;

+---------------+
| message       |
+---------------+
| Good Evening! |
+---------------+
```

The following example demonstrates how to utilize LUA escaping within the
triple-quoted multi-line string.

```sql
CREATE TEMP FUNCTION Alphabet()
RETURNS STRING
LANGUAGE lua
AS R"""
  return 'A\tB\tC'
  """;

SELECT Alphabet() AS result;

+---------+
| result  |
+---------+
| A  B  C |
+---------+
```

## TVFs {#tvfs}

A TVF is a table-valued function that returns an entire output table instead of
a single scalar value, and appears in the `FROM` clause like a table subquery.

### TVF structure

You create a TVF using the following structure.

```sql
CREATE
  { TEMPORARY | TEMP } TABLE FUNCTION
  function_name ( [ function_parameter  [, ...] ] )
  [ RETURNS TABLE < column_declaration [, ...] > ]
  [ { AS query | LANGUAGE language_name AS string_literal } ]

function_parameter:
  parameter_name { data_type | ANY TYPE | ANY TABLE }

column_declaration:
  column_name data_type
```

+   `CREATE ... TABLE FUNCTION`: Creates a new
    [table-valued function][table-valued function] function.
    A function can have zero or more function parameters.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
     exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function.

    + `parameter_name`: The name of the parameter.

    + `data_type`: A ZetaSQL [data type][data-types].

    + `ANY TYPE`: The function will accept an argument of any type for this
      function parameter. If more than one parameter includes `ANY TYPE`,
      a relationship is not enforced between these parameters when the function
      is defined. However, if the type of argument passed into the function at
      call time is incompatible with the function definition, this will
      result in an error.

      `ANY TYPE` is a [_templated function parameter_][templated-parameters].

    
    + `ANY TABLE`. The function will accept an argument of any relation type for
      this argument. However, passing the function arguments of types that are
      incompatible with the function definition will result in an error at
      call time.

      `ANY TABLE` is a [_templated function parameter_][templated-parameters].
    
+   `RETURNS TABLE`: Specifies the schema of the table that a table-valued
    function returns as a comma-separated list of `column_name` and `TYPE`
    pairs. If `RETURNS TABLE` is absent, ZetaSQL infers the
    output schema from the `AS query` statement in the function body.
+   `AS query`: If you want to create a SQL TVF, specifies the SQL query to run.
+   `LANGUAGE ... AS`: If you want to create an external TVF, specifies the
    language and code to use.
    `language_name` represents the name of the language, such
    as `js` for JavaScript. `string_literal` represents the code that defines
    the function body.

### Specifying TVF arguments {#tvf_arguments}

When a TVF with function parameters is called, arguments must be passed in for
all function parameters that do not have defaults. An argument can be of any
supported ZetaSQL type or table, but must be coercible to the related
function parameter's type.

Specify a table argument the same way you specify the fields of a
[STRUCT][data-types-struct].

```sql
parameter_name TABLE<column_name data_type [, ...]>
```

The table argument can specify a [value table][datamodel-value-tables],
in which each row
is a single column of a specific type. To specify a value table as an argument,
include only the `data_type`, leaving out the `column_name`:

```sql
parameter_name TABLE<data_type>
```

In many cases, the `data_type` of the single column in the value table is a
protocol buffer; for example:

```sql
CREATE TEMP TABLE FUNCTION AggregatedMovieLogs(
  TicketPurchases TABLE<analysis_conduit.codelab.MovieTicketPurchase>)
```

The function body can refer directly to fields within the proto.

You have the option to specify the input table using the templated type `ANY
TABLE` in place of `TABLE<column_name data_type [, ...]>`. This option enables
you to create a polymorphic TVF that accepts any table as input.

**Example**

The following example implements a pair of TVFs that define parameterized views
of a range of rows from the Customer table. The first returns all rows for a
range of `CustomerIds`; the second calls the first function and applies an
additional filter based on `CustomerType`.

```sql
CREATE TEMP TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)
AS (
  SELECT *
  FROM Customer
  WHERE CustomerId >= MinId AND CustomerId <= MaxId
);

CREATE TEMP TABLE FUNCTION CustomerRangeWithCustomerType(
  MinId INT64,
  MaxId INT64,
  customer_type ads.boulder.schema.CustomerType)
AS (
  SELECT *
  FROM CustomerRange(MinId, MaxId)
  WHERE type = customer_type
);
```

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
table `selected_customers` must contain a column named `creation_time`, and
`creation_time` must be a numeric type, or the function will return an error.

```sql
CREATE TEMP TABLE FUNCTION CustomerCreationTimeRange(
    min_creation_time INT64,
    max_creation_time INT64,
    selected_customers ANY TABLE)
  AS
    SELECT *
    FROM selected_customers
    WHERE creation_time >= min_creation_time
    AND creation_time <= max_creation_time;
```

### Calling TVFs

To call a TVF, use the function call in place of the table name in a `FROM`
clause.

There are two ways to pass a table as an argument to a TVF. You can use a
subquery for the table argument, or you can use the name of a table, preceded by
the keyword `TABLE`.

**Examples**

The following query calls the `CustomerRangeWithCustomerType` function to
return a table with rows for customers with a CustomerId between 100
and 200.

```sql
SELECT CustomerId, Info
FROM CustomerRangeWithCustomerType(100, 200, 'CUSTOMER_TYPE_ADVERTISER');
```

The following query calls the `CustomerCreationTimeRange` function defined
previously, passing the result of a subquery as the table argument.

```sql
SELECT *
FROM
  CustomerCreationTimeRange(
    1577836800,  -- 2020-01-01 00:00:00 UTC
    1609459199,  -- 2020-12-31 23:59:59 UTC
    (
      SELECT customer_id, customer_name, creation_time
      FROM MyCustomerTable
      WHERE customer_name LIKE '%Hernández'
    ))
```

The following query calls `CustomerCreationTimeRange`, passing the table
`MyCustomerTable` as an argument.

```sql
SELECT *
FROM
  CustomerCreationTimeRange(
    1577836800,  -- 2020-01-01 00:00:00 UTC
    1609459199,  -- 2020-12-31 23:59:59 UTC
    TABLE MyCustomerTable)
```

## Templated function parameters

A templated function parameter can match more than one argument type at
function call time. If a function signature includes a
templated function parameter, ZetaSQL allows function calls
to pass to the function any argument type as long as the function body is
valid for that argument type.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[table-valued function]: #tvfs

[tvf-syntax]: #tvf_structure

[templated-parameters]: #templated_function_parameters

[ext-udf-syntax]: #external_udf_structure

[sql-udf-syntax]: #sql_udf_structure

[javascript-data-types]: #javascript_udf_data_types

[data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

[data-types-struct]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

[aggregate-udf-parameters]: #aggregate_udf_parameters

[group-by-link]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[aggregate-fns-link]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

<!-- mdlint on -->

