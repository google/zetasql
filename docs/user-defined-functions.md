

# User-defined functions

ZetaSQL
supports user-defined functions (UDFs). A UDF enables you to create a function
using another SQL expression or another programming
language, such as JavaScript or Lua. These functions accept columns of input
and perform actions, returning the result of those actions as a value.

##  General UDF and TVF  syntax

User-defined functions and table-valued
functions in ZetaSQL use the
following general syntax:

+  [SQL UDF syntax][sql-udf-syntax]
+  [External UDF syntax][ext-udf-syntax]
+  [TVF syntax][tvf-syntax]

## SQL UDFs

An SQL UDF lets you specify a SQL expression for the UDF.

### SQL UDF structure

Create SQL UDFs using the following syntax:

```sql
CREATE
  [ { TEMPORARY | TEMP } ] [ AGGREGATE ] FUNCTION
  function_name ( [ function_parameter [ NOT AGGREGATE ] [, ...] ] )
  [ RETURNS data_type ]
  AS ( sql_expression )

function_parameter:
  parameter_name { data_type | ANY TYPE }
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function.
     A function can contain zero or more
    `function_parameter`s.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
     exists for the lifetime of the session.
+   `AGGREGATE`: Indicates that this is an [aggregate function][aggregate-udf-parameters].
+   `function_parameter`: A parameter for the function. A parameter
    includes a name and a data type. The value of `data_type`
    is a ZetaSQL [data type][data-types] or may also be `ANY TYPE`.
+   `RETURNS data_type`: Specifies the data type
    that the function returns. The
    `RETURNS` clause is optional and ZetaSQL infers the result type
    of the function from the SQL function body.
+   `AS (sql_expression)`: Specifies the SQL code that defines the
    function.
+   `NOT AGGREGATE`: Specifies that a parameter is not
    aggregate. Can only appear after `CREATE AGGREGATE FUNCTION`. A
    non-aggregate parameter can appear anywhere in the
    [function definition][aggregate-udf-parameters].

### Aggregate UDF parameters

An aggregate UDF can include aggregate or non-aggregate parameters. Like other
[aggregate functions][aggregate-fns-link], aggregate UDFs normally aggregate
parameters across all rows in a [group][group-by-link]. However, you can specify
a parameter as non-aggregate with the `NOT AGGREGATE` keyword. A non-aggregate
parameter is a scalar parameter with a constant value for all rows in a group;
for example, literals or grouping expressions are valid non-aggregate
parameters. Inside the UDF definition, aggregate parameters can only appear as
parameters to aggregate function calls. Non-aggregate parameters can appear
anywhere in the UDF definition.

### Templated SQL UDF parameters

A templated parameter can match more than one argument type at function call
time. If a function signature includes a templated parameter, ZetaSQL
allows function calls to pass one of several argument types to the function.

SQL user-defined function signatures can contain the following templated
parameter value:

+ `ANY TYPE`. The function will accept an argument of any type for this
  parameter. If more than one parameter includes `ANY TYPE`,
  a relationship is not enforced between these parameters
  when the function is defined. However, if the type of argument passed into the
  function at call time is incompatible with the function definition, this will
  result in an error.

### SQL UDF examples

The following example shows a UDF that employs a SQL function.

```sql
CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
RETURNS DOUBLE
AS ((x + 4) / y);
WITH numbers AS
  (SELECT 1 as val UNION ALL
   SELECT 3 as val UNION ALL
   SELECT 4 as val UNION ALL
   SELECT 5 as val)
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

The following example shows an aggregate UDF that uses a non-aggregate
parameter. Inside the function definition, the aggregate `SUM` method takes the
aggregate parameter `dividend`, while the non-aggregate division operator
( `/` ) takes the non-aggregate parameter `divisor`.

```sql
CREATE TEMP AGGREGATE FUNCTION ScaledSum(dividend DOUBLE, divisor DOUBLE NOT AGGREGATE)
RETURNS DOUBLE
AS (SUM(dividend) / divisor);
SELECT ScaledSum(col1, 2) AS scaled_sum
FROM (SELECT 1 AS col1 UNION ALL
      SELECT 3 AS col1 UNION ALL
      SELECT 5 AS col1);

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
CREATE TEMP FUNCTION AddFourAndDivideAny(x ANY TYPE, y ANY TYPE)
AS (
  (x + 4) / y
);

SELECT AddFourAndDivideAny(3, 4) AS integer_output,
       AddFourAndDivideAny(1.59, 3.14) AS floating_point_output;

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
CREATE TEMP FUNCTION LastArrayElement(arr ANY TYPE)
AS (
  arr[ORDINAL(ARRAY_LENGTH(arr))]
);
SELECT
  names[OFFSET(0)] AS first_name,
  LastArrayElement(names) AS last_name
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

## External UDFs

An external UDF lets you specify external code libraries for the UDF.

### External UDF structure

Create external UDFs using the following syntax.

```sql
CREATE
  [ { TEMPORARY | TEMP } ] FUNCTION
  function_name ( [ function_parameter [, ...] ] )
  RETURNS data_type
  [ determinism_specifier ]
  LANGUAGE language_name AS string_literal

function_parameter:
  parameter_name data_type

determinism_specifier:
  { IMMUTABLE | DETERMINISTIC | NOT DETERMINISTIC | VOLATILE | STABLE }
```

This syntax consists of the following components:

+   `CREATE ... FUNCTION`: Creates a new function. A function can contain zero
    or more `function_parameter`s.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
    exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function. A parameter includes a
    name and a data type. The value of `data_type` is a ZetaSQL
    [data type][data-types].
+   `determinism_specifier`: Determines if the results of the function can be
    cached or not. Your choices are:
    +   `IMMUTABLE` or `DETERMINISTIC`: The function always returns the same
        result when passed the same arguments. The query result is potentially
        cacheable. For example, if the function `add_one(i)` always returns `i +
        1`, the function is deterministic.
    +   `NOT DETERMINISTIC`: The function does not always return the same result
        when passed the same arguments. The `VOLATILE` and `STABLE` keywords are
        subcategories of `NOT DETERMINISTIC`.
    +   `VOLATILE`: The function does not always return the same result when
        passed the same arguments, even within the same run of a query
        statement. For example if `add_random(i)` returns `i + rand()`, the
        function is volatile, because every call to the function can return a
        different result.
    +   `STABLE`: Within one run of a query statement, the function will
        consistently return the same result for the same argument values.
        However, the result could change across two runs. For example, a
        function that returns the current session user is stable, because the
        value would not change within the same run.
+   `RETURNS data_type`: Specifies the data type that the function returns. See
    [Supported UDF data types][supported-external-udf-data-types] for more
    information about allowed values for `data_type`.
+   `LANGUAGE ... AS`: Specify the language and code to use. `language_name`
    represents the name of the language, such as `js` for JavaScript.
    `string_literal` represents the code that defines the function body.

### External UDF examples

The following example creates a persistent UDF.

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

The following example creates a temporary UDF.

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

You can create multiple UDFs before a query. For example:

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

You can pass the result of a UDF as input to another UDF. For example:

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

### Quoting rules

You must enclose external code in quotes. There are a few options:

+ `"..."`: For simple, one line code snippets that don't contain quotes or
  escaping, you can use a standard quoted string.
+ `"""..."""`: If the snippet contains quotes or multiple lines, use
  triple-quoted blocks.
+ `R"""..."""`: If the snippet contains escaping, prefix a triple-quoted
  block with an `R` to indicate that this is a raw string that should ignore
  escaping rules. If you are not sure which quoting style to use, this one
  will provide the most consistent results.

**Examples**

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

```sql
CREATE TEMP FUNCTION CustomGreeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
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

```sql
CREATE TEMP FUNCTION PlusOne(x STRING)
RETURNS STRING
LANGUAGE js AS R"""
var re = /[a-z]/g;
return x.match(re);
""";

SELECT val, PlusOne(val) AS result
FROM UNNEST(["ab-c", "d_e", "!"]) AS val;

+---------+
| result  |
+---------+
| [a,b,c] |
| [d,e]   |
| NULL    |
+---------+
```

### Supported external UDF types 
<a id="supported_external_udf_data_types"></a>

## TVFs {#tvfs}

A *table-valued function* (TVF) is a function that returns an entire output
table instead of a single scalar value, and appears in the FROM clause like a
table subquery.

### TVF structure

You create a TVF using the following structure.

```sql
CREATE
  { TEMPORARY | TEMP } TABLE FUNCTION
  function_name ( [ function_parameter  [, ...] ] )
  [ RETURNS TABLE < column_declaration [, ...] > ]
  [ { AS query | LANGUAGE language_name AS string_literal } ]

function_parameter:
  parameter_name { data_type | ANY TABLE }

column_declaration:
  column_name data_type
```

+   `CREATE ... TABLE FUNCTION`: Creates a new
    [table-valued function][table-valued function] function.
    A function can contain zero or more
    `function_parameter`s.
+   `TEMPORARY` or `TEMP`: Indicates that the function is temporary; that is it
     exists for the lifetime of the session.
+   `function_parameter`: A parameter for the function. A parameter
    includes a name and a data type. The value of `data_type`
    is a ZetaSQL [data type][data-types].
    The value of `data_type` may also be `ANY TABLE`.
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

When a TVF with parameters is called, arguments must be passed in for all
parameters that do not have defaults. An argument can be of any supported
ZetaSQL type or table, but must be coercible to the related
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
    WHERE type = customer_type;
```

#### Templated SQL TVF parameters

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

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[table-valued function]: #tvfs

[tvf-syntax]: #tvf_structure

[templated-parameters]: #templated_sql_udf_parameters

[supported-external-udf-data-types]: #supported_external_udf_data_types

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

