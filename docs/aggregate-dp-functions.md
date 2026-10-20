

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Differentially private aggregate functions 
<a id="aggregate-dp-functions"></a>

GoogleSQL supports differentially private aggregate functions.
For an explanation of how aggregate functions work, see
[Aggregate function calls][agg-function-calls].

You can only use differentially private aggregate functions with
[differentially private queries][dp-guide] in a
[differential privacy clause][dp-syntax].

Note: In this topic, the privacy parameters in the examples aren't
recommendations. You should work with your privacy or security officer to
determine the optimal privacy parameters for your dataset and organization.

## Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#dp_avg"><code>AVG</code> (Differential Privacy)</a>
</td>
  <td>
    <code>DIFFERENTIAL_PRIVACY</code>-supported <code>AVG</code>.<br/><br/>
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
    
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#dp_count"><code>COUNT</code> (Differential Privacy)</a>
</td>
  <td>
    <code>DIFFERENTIAL_PRIVACY</code>-supported <code>COUNT</code>.<br/><br/>
    Signature 1: Gets the differentially-private count of rows in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
    <br/>
    <br/>
    Signature 2: Gets the differentially-private count of rows with a
    non-<code>NULL</code> expression in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
    
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#dp_percentile_cont"><code>PERCENTILE_CONT</code> (Differential Privacy)</a>
</td>
  <td>
    <code>DIFFERENTIAL_PRIVACY</code>-supported <code>PERCENTILE_CONT</code>.<br/><br/>
    Computes a differentially-private percentile across privacy unit columns
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
    
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#dp_sum"><code>SUM</code> (Differential Privacy)</a>
</td>
  <td>
    <code>DIFFERENTIAL_PRIVACY</code>-supported <code>SUM</code>.<br/><br/>
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
    
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#dp_var_pop"><code>VAR_POP</code> (Differential Privacy)</a>
</td>
  <td>
    <code>DIFFERENTIAL_PRIVACY</code>-supported <code>VAR_POP</code> (Differential Privacy).<br/><br/>
    Computes the differentially-private population (biased) variance of values
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
    
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_avg"><code>ANON_AVG</code></a>
</td>
  <td>
    Deprecated.
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_count"><code>ANON_COUNT</code></a>
</td>
  <td>
    Deprecated.
    <br/>
    <br/>
    Signature 1: Gets the differentially-private count of rows in a query
    with an <code>ANONYMIZATION</code> clause.
    <br/>
    <br/>
    Signature 2: Gets the differentially-private count of rows with a
    non-<code>NULL</code> expression in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_percentile_cont"><code>ANON_PERCENTILE_CONT</code></a>
</td>
  <td>
    Deprecated.
    Computes a differentially-private percentile across privacy unit columns
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_quantiles"><code>ANON_QUANTILES</code></a>
</td>
  <td>
    Deprecated.
    Produces an array of differentially-private quantile boundaries
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_stddev_pop"><code>ANON_STDDEV_POP</code></a>
</td>
  <td>
    Deprecated.
    Computes a differentially-private population (biased) standard deviation of
    values in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_sum"><code>ANON_SUM</code></a>
</td>
  <td>
    Deprecated.
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#anon_var_pop"><code>ANON_VAR_POP</code></a>
</td>
  <td>
    Deprecated.
    Computes a differentially-private population (biased) variance of values
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

  </tbody>
</table>

## `AVG` (`DIFFERENTIAL_PRIVACY`) 
<a id="dp_avg"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  AVG(
    expression,
    [ contribution_bounds_per_group => (lower_bound, upper_bound) ],
    [ report_format => { 'JSON' | 'PROTO' } ]
  )
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `contribution_bounds_per_group`: A named argument with a
  [contribution bound][dp-clamped-named].
  Performs clamping for each group separately before performing intermediate
  grouping on the privacy unit column.
+ `report_format`: A named argument that produces a
  [`JSON`-formatted report][json-report-format] or [`PROTO`-formatted report][proto-report-format]
   with the results.

**Return type**

+ Without a report format: `DOUBLE`
+ With a `JSON` report format: `JSON`
+ With a `PROTO` report format: `PROTO`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a table called [`professors`][dp-example-tables].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 +----------+------------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity) average_quantity
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 +----------+------------------*/
```

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity, contribution_bounds_per_group=>(0, 100)) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 +----------+------------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 +----------+------------------*/
```

The following differentially private query produces a JSON-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity, report_format=>'JSON') average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | average_quantity                                                 |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   "bounding_report":{"lower_bound":4.0,"num_inputs":1.0,         |
 |          |   "num_outside":0.0,"upper_bound":8.0},                          |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":8.0,                     |
 |          |   "upper_bound":8.0},"value":8.0}                                |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   "bounding_report":{"lower_bound":64.0,"num_inputs":1.0,        |
 |          |   "num_outside":0.0,"upper_bound":128.0},                        |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":72.0,                    |
 |          |   "upper_bound":72.0},"value":72.0}                              |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   "bounding_report":{"lower_bound":8.0,"num_inputs":2.0,         |
 |          |   "num_outside":0.0,"upper_bound":32.0},                         |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":18.5,                    |
 |          |   "upper_bound":18.5},"value":18.5}                              |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

The following differentially private query produces a PROTO-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity, report_format=>'PROTO') average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | average_quantity                                                 |
 +----------+------------------------------------------------------------------+
 | scissors | value { float_value: 8.0 noise_confidence_interval {             |
 |          | lower_bound: 8.0 upper_bound: 8.0 confidence_level: 0.95 } }     |
 |          | bounding_report { lower_bound { float_value: 4.0 }               |
 |          | upper_bound { float_value: 8.0 } num_inputs: 1 num_outside: 0 }  |
 +----------+------------------------------------------------------------------+
 | pencil   | value { float_value: 72.0 noise_confidence_interval {            |
 |          | lower_bound: 72.0 upper_bound: 72.0 confidence_level: 0.95 } }   |
 |          | bounding_report { lower_bound { float_value: 64.0 }              |
 |          | upper_bound { float_value: 128.0 } num_inputs: 1 num_outside: 0 }|
 +----------+------------------------------------------------------------------+
 | pen      | value { float_value: 18.5 noise_confidence_interval {            |
 |          | lower_bound: 18.5 upper_bound: 18.5 confidence_level: 0.95 } }   |
 |          | bounding_report { lower_bound { float_value: 8.0 }               |
 |          | upper_bound { float_value: 32.0 } num_inputs: 2 num_outside: 0 } |
 +----------+------------------------------------------------------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-example-tables]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[json-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#json_report_format

[proto-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#proto_report_format

## `COUNT` (`DIFFERENTIAL_PRIVACY`) 
<a id="dp_count"></a>

+ [Signature 1](#dp_count_signature1): Returns the number of rows in a
  differentially private `FROM` clause.
+ [Signature 2](#dp_count_signature2): Returns the number of non-`NULL`
  values in an expression.

#### Signature 1 
<a id="dp_count_signature1"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(
    *,
    [ contribution_bounds_per_group => (lower_bound, upper_bound) ],
    [ report_format => { 'JSON' | 'PROTO' } ]
  )
```

**Description**

Returns the number of rows in the
[differentially private][dp-from-clause] `FROM` clause. The final result
is an aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following arguments:

+ `contribution_bounds_per_group`: A named argument with a
  [contribution bound][dp-clamped-named].
  Performs clamping for each group separately before performing intermediate
  grouping on the privacy unit column.
+ `report_format`: A named argument that produces a
  [`JSON`-formatted report][json-report-format] or [`PROTO`-formatted report][proto-report-format]
   with the results.

**Return type**

+ Without a report format: `INT64`
+ With a `JSON` report format: `JSON`
+ With a `PROTO` report format: `PROTO`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a table called
[`professors`][dp-example-tables].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100)) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100)) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

The following differentially private query counts the number of requests for
each item. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100)) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100)) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

The following differentially private query produces a JSON-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100), report_format=>'JSON') times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | times_requested                                                  |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":1.0,                     |
 |          |   "upper_bound":1.0},"value":1}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":4.0,                     |
 |          |   "upper_bound":4.0},"value":4}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":3.0,                     |
 |          |   "upper_bound":3.0},"value":3}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

The following differentially private query produces a PROTO-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(*, contribution_bounds_per_group=>(0, 100), report_format=>'PROTO') times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | times_requested                                                  |
 +----------+------------------------------------------------------------------+
 | scissors | value { int_value: 1 noise_confidence_interval {                 |
 |          | lower_bound: 1.0 upper_bound: 1.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------+
 | pencil   | value { int_value: 4 noise_confidence_interval {                 |
 |          | lower_bound: 4.0 upper_bound: 4.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------+
 | pen      | value { int_value: 3 noise_confidence_interval {                 |
 |          | lower_bound: 3.0 upper_bound: 3.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------*/
```

#### Signature 2 
<a id="dp_count_signature2"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(
    expression,
    [ contribution_bounds_per_group => (lower_bound, upper_bound) ],
    [ report_format => { 'JSON' | 'PROTO' } ]
  )
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This expression can be any
  numeric input type, such as `INT64`.
+ `contribution_bounds_per_group`: A named argument with a
  [contribution bound][dp-clamped-named].
  Performs clamping per each group separately before performing intermediate
  grouping on the privacy unit column.
+ `report_format`: A named argument that produces a
  [`JSON`-formatted report][json-report-format] or [`PROTO`-formatted report][proto-report-format]
   with the results.

**Return type**

+ Without a report format: `INT64`
+ With a `JSON` report format: `JSON`
+ With a `PROTO` report format: `PROTO`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a table called
[`professors`][dp-example-tables].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

The following differentially private query counts the number of requests made
for each type of item. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    COUNT(item, contribution_bounds_per_group=>(0, 100)) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(item, contribution_bounds_per_group=>(0, 100)) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

The following differentially private query produces a JSON-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(item, contribution_bounds_per_group=>(0, 100), report_format=>'JSON') times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | times_requested                                                  |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":1.0,                     |
 |          |   "upper_bound":1.0},"value":1}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":4.0,                     |
 |          |   "upper_bound":4.0},"value":4}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":3.0,                     |
 |          |   "upper_bound":3.0},"value":3}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

The following differentially private query produces a PROTO-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    COUNT(item, contribution_bounds_per_group=>(0, 100), report_format=>'PROTO') times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | times_requested                                                  |
 +----------+------------------------------------------------------------------+
 | scissors | value { int_value: 1 noise_confidence_interval {                 |
 |          | lower_bound: 1.0 upper_bound: 1.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------+
 | pencil   | value { int_value: 4 noise_confidence_interval {                 |
 |          | lower_bound: 4.0 upper_bound: 4.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------+
 | pen      | value { int_value: 3 noise_confidence_interval {                 |
 |          | lower_bound: 3.0 upper_bound: 3.0 confidence_level: 0.95 } }     |
 +----------+------------------------------------------------------------------*/
```

[dp-clamp-implicit]: #dp_implicit_clamping

[dp-from-clause]: https://github.com/google/googlesql/blob/master/docs/differential-privacy.md#dp_from_rules

[dp-example-tables]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[dp-clamped-named]: #dp_clamped_named

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[json-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#json_report_format

[proto-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#proto_report_format

## `PERCENTILE_CONT` (`DIFFERENTIAL_PRIVACY`) 
<a id="dp_percentile_cont"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  PERCENTILE_CONT(
    expression,
    percentile,
    contribution_bounds_per_row => (lower_bound, upper_bound)
  )
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across privacy unit columns.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL` values are always ignored.
+ `percentile`: The percentile to compute. The percentile must be a literal in
  the range `[0, 1]`.
+ `contribution_bounds_per_row`: A named argument with a
  [contribution bounds][dp-clamped-named].
  Performs clamping for each row separately before performing intermediate
  grouping on the privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`professors`][dp-example-tables].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    PERCENTILE_CONT(quantity, 0.5, contribution_bounds_per_row => (0,100)) percentile_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
 /*----------+----------------------+
  | item     | percentile_requested |
  +----------+----------------------+
  | pencil   | 72.00011444091797    |
  | scissors | 8.000175476074219    |
  | pen      | 23.001075744628906   |
  +----------+----------------------*/
```

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    PERCENTILE_CONT(quantity, 0.5, contribution_bounds_per_row=>(0, 100)) percentile_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+----------------------+
 | item     | percentile_requested |
 +----------+----------------------+
 | pencil   | 72.00011444091797    |
 | scissors | 8.000175476074219    |
 | pen      | 23.001075744628906   |
 +----------+----------------------*/
```

[dp-example-tables]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

## `SUM` (`DIFFERENTIAL_PRIVACY`) 
<a id="dp_sum"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  SUM(
    expression,
    [ contribution_bounds_per_group => (lower_bound, upper_bound) ],
    [ report_format => { 'JSON' | 'PROTO' } ]
  )
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across privacy unit columns.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL` values are always ignored.
+ `contribution_bounds_per_group`: A named argument with a
  [contribution bound][dp-clamped-named]. Performs clamping for each group
  separately before performing intermediate grouping on the privacy unit column.
+ `report_format`: A named argument that produces a
  [`JSON`-formatted report][json-report-format] or [`PROTO`-formatted report][proto-report-format]
   with the results.

**Return type**

+ Without a report format, one of the following [supertypes][dp-supertype]:

  + `INT64`
  + `UINT64`
  + `DOUBLE`
+ With a `JSON` report format: `JSON`
+ With a `PROTO` report format: `PROTO`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`professors`][dp-example-tables].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    SUM(quantity, contribution_bounds_per_group => (0,100)) quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------+
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 +----------+-----------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    SUM(quantity) quantity
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+----------+
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 +----------+----------*/
```

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    SUM(quantity, contribution_bounds_per_group=>(0, 100)) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------+
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 +----------+-----------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    SUM(quantity) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+----------+
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 +----------+----------*/
```

Note: For more information about when and when not to use
noise, see [Use differential privacy][dp-noise].

The following differentially private query produces a JSON-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    SUM(quantity, report_format=>'JSON') quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | quantity                                                         |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   "bounding_report":{"lower_bound":4,"num_inputs":1.0,           |
 |          |   "num_outside":0.0,"upper_bound":8},                            |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":8.0,                     |
 |          |   "upper_bound":8.0},"value":8}                                  |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   "bounding_report":{"lower_bound":64,"num_inputs":1.0,          |
 |          |   "num_outside":0.0,"upper_bound":256},                          |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":144.0,                   |
 |          |   "upper_bound":144.0},"value":144}                              |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   "bounding_report":{"lower_bound":16,"num_inputs":2.0,          |
 |          |   "num_outside":0.0,"upper_bound":64},                           |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":58.0,                    |
 |          |   "upper_bound":58.0},"value":58}                                |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

The following differentially private query produces a PROTO-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    SUM(quantity, report_format=>'PROTO') quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | quantity                                                         |
 +----------+------------------------------------------------------------------+
 | scissors | value { int_value: 8 noise_confidence_interval {                 |
 |          | lower_bound: 8.0 upper_bound: 8.0 confidence_level: 0.95 } }     |
 |          | bounding_report { lower_bound { int_value: 4 }                   |
 |          | upper_bound { int_value: 8 } num_inputs: 1 num_outside: 0 }      |
 +----------+------------------------------------------------------------------+
 | pencil   | value { int_value: 144 noise_confidence_interval {               |
 |          | lower_bound: 144.0 upper_bound: 144.0 confidence_level: 0.95 } } |
 |          | bounding_report { lower_bound { int_value: 64 }                  |
 |          | upper_bound { int_value: 256 } num_inputs: 1 num_outside: 0 }    |
 +----------+------------------------------------------------------------------+
 | pen      | value { int_value: 58 noise_confidence_interval {                |
 |          | lower_bound: 58.0 upper_bound: 58.0 confidence_level: 0.95 } }   |
 |          | bounding_report { lower_bound { int_value: 16 }                  |
 |          | upper_bound { int_value: 64 } num_inputs: 2 num_outside: 0 }     |
 +----------+------------------------------------------------------------------*/
```

[dp-example-tables]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[json-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#json_report_format

[proto-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#proto_report_format

## `VAR_POP` (`DIFFERENTIAL_PRIVACY`) 
<a id="dp_var_pop"></a>

```googlesql
WITH DIFFERENTIAL_PRIVACY ...
  VAR_POP(
    expression,
    [ contribution_bounds_per_row => (lower_bound, upper_bound) ]
  )
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][dp_clamped_named] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual user values.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `contribution_bounds_per_row`: A named argument with a
  [contribution bound][dp-clamped-named].
  Performs clamping for each row separately before performing intermediate
  grouping on individual user values.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations may not
be included. This query references a view called
[`professors`][dp-example-tables].

```googlesql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    VAR_POP(quantity, contribution_bounds_per_row => (0,100)) pop_variance
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
/*----------+-----------------+
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 +----------+-----------------*/
```

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations might not
be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    VAR_POP(quantity, contribution_bounds_per_row=>(0, 100)) pop_variance
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 +----------+-----------------*/
```

[dp-example-tables]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

## `ANON_AVG` (DEPRECATED) 
<a id="anon_avg"></a>

Warning: This function has been deprecated. Use
`AVG` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_AVG(expression [clamped_between_clause] [with_report_clause])

clamped_between_clause:
  CLAMPED BETWEEN lower_bound AND upper_bound

with_report_clause:
  WITH REPORT(FORMAT={JSON|PROTO})
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `clamped_between_clause`: Perform [clamping][dp-clamping] per
  privacy unit column averages.
+ `with_report_clause`: Produce a [`JSON`-formatted report][json-report-format]
  or [`PROTO`-formatted report][proto-report-format] with the results.

**Return type**

+ Without a report: `DOUBLE`
+ With a `JSON`-formatted report: `JSON`
+ With a `PROTO`-formatted report: `PROTO`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 +----------+------------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 +----------+------------------*/
```

The following differentially private query produces a JSON-formatted report.
This query references a view called [`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity WITH REPORT(FORMAT=JSON)) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | average_quantity                                                 |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   {"bounding_report":{"lower_bound":4.0,"num_inputs":1.0,        |
 |          |   "num_outside":0.0,"upper_bound":8.0},                          |
 |          |   "result":{"noise_confidence_interval":{"confidence_level":0.95,|
 |          |   "lower_bound":8.0,"upper_bound":8.0},"value":8.0}}             |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   {"bounding_report":{"lower_bound":64.0,"num_inputs":1.0,       |
 |          |   "num_outside":0.0,"upper_bound":128.0},                        |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":72.0,                    |
 |          |   "upper_bound":72.0},"value":72.0}}                             |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   {"bounding_report":{"lower_bound":8.0,"num_inputs":2.0,        |
 |          |   "num_outside":0.0,"upper_bound":32.0},                         |
 |          |   "result":{"noise_confidence_interval":{"confidence_level":0.95,|
 |          |   "lower_bound":18.5,"upper_bound":18.5},"value":18.5}}.         |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamping]: #dp_clamping

[json-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#json_report_format

[proto-report-format]: https://github.com/google/googlesql/blob/master/docs/aggregate-dp-functions.md#proto_report_format

## `ANON_COUNT` (DEPRECATED) 
<a id="anon_count"></a>

Warning: This function has been deprecated. Use
`COUNT` (differential privacy) instead.

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```googlesql
WITH ANONYMIZATION ...
  ANON_COUNT(*)
```

**Description**

Returns the number of rows in the
[differentially private][dp-from-clause] `FROM` clause. The final result
is an aggregation across privacy unit columns.
[Input values are clamped implicitly][dp-clamp-implicit]. Clamping is
performed per privacy unit column.

This function must be used with the `ANONYMIZATION` clause.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

#### Signature 2 
<a id="anon_count_signature2"></a>

```googlesql
WITH ANONYMIZATION ...
  ANON_COUNT(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 +----------+-----------------*/
```

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------+
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 +----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-clamp-implicit]: #dp_clamped_named_implicit

[dp-from-clause]: https://github.com/google/googlesql/blob/master/docs/differential-privacy.md#dp_from_rules

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamping]: #dp_clamping

## `ANON_PERCENTILE_CONT` (DEPRECATED) 
<a id="anon_percentile_cont"></a>

Warning: This function has been deprecated. Use
`PERCENTILE_CONT` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_PERCENTILE_CONT(expression, percentile [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `percentile`: The percentile to compute. The percentile must be a literal in
  the range [0, 1]
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_PERCENTILE_CONT(quantity, 0.5 CLAMPED BETWEEN 0 AND 100) percentile_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+----------------------+
 | item     | percentile_requested |
 +----------+----------------------+
 | pencil   | 72.00011444091797    |
 | scissors | 8.000175476074219    |
 | pen      | 23.001075744628906   |
 +----------+----------------------*/
```

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamping]: #dp_clamping

## `ANON_QUANTILES` (DEPRECATED) 
<a id="anon_quantiles"></a>

Warning: This function has been deprecated. Use
`QUANTILES` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_QUANTILES(expression, number CLAMPED BETWEEN lower_bound AND upper_bound)
```

**Description**

Returns an array of differentially private quantile boundaries for values in
`expression`. The first element in the return value is the
minimum quantile boundary and the last element is the maximum quantile boundary.
The returned results are aggregations across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `number`: The number of quantiles to create. This must be an `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`ARRAY`\<`DOUBLE`>

**Examples**

The following differentially private query gets the five quantile boundaries of
the four quartiles of the number of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_QUANTILES(quantity, 4 CLAMPED BETWEEN 0 AND 100) quantiles_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+----------------------------------------------------------------------+
 | item     | quantiles_requested                                                  |
 +----------+----------------------------------------------------------------------+
 | pen      | [6.409375,20.647684733072918,41.40625,67.30848524305556,99.80078125] |
 | pencil   | [6.849259,44.010416666666664,62.64204,65.83806818181819,98.59375]    |
 +----------+----------------------------------------------------------------------*/
```

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamping]: #dp_clamping

## `ANON_STDDEV_POP` (DEPRECATED) 
<a id="anon_stddev_pop"></a>

Warning: This function has been deprecated. Use
`STDDEV_POP` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_STDDEV_POP(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes the population (biased) standard deviation of
the values in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) standard deviation of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_STDDEV_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_standard_deviation
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+------------------------+
 | item     | pop_standard_deviation |
 +----------+------------------------+
 | pencil   | 25.350871122442054     |
 | scissors | 50                     |
 | pen      | 2                      |
 +----------+------------------------*/
```

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamping]: #dp_clamping

## `ANON_SUM` (DEPRECATED) 
<a id="anon_sum"></a>

Warning: This function has been deprecated. Use
`SUM` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_SUM(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across privacy unit columns.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per privacy unit column.

**Return type**

One of the following [supertypes][dp-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity CLAMPED BETWEEN 0 AND 100) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------+
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 +----------+-----------*/
```

```googlesql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+----------+
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 +----------+----------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamping]: #dp_clamping

## `ANON_VAR_POP` (DEPRECATED) 
<a id="anon_var_pop"></a>

Warning: This function has been deprecated. Use
`VAR_POP` (differential privacy) instead.

```googlesql
WITH ANONYMIZATION ...
  ANON_VAR_POP(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][dp-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual entity values.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][dp-clamping] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations might not
be included. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- With noise, using the epsilon parameter.
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_VAR_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_variance
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------+
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 +----------+-----------------*/
```

[dp-clamp-explicit]: #dp_clamped_named

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamping]: #dp_clamping

## Supplemental materials

### Clamp values in a differentially private aggregate function 
<a id="dp_clamping"></a>

In [differentially private queries][dp-syntax],
aggregation clamping is used to limit the contribution of outliers. You can
clamp explicitly or implicitly as follows:

+ [Clamp explicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named].
+ [Clamp implicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named-imp].

#### Implicitly clamp values 
<a id="dp_clamped_named_implicit"></a>

If you don't include the contribution bounds named argument with the
`DIFFERENTIAL_PRIVACY` clause, clamping is implicit, which
means bounds are derived from the data itself in a differentially private way.

Implicit bounding works best when computed using large datasets. For more
information, see
[Implicit bounding limitations for small datasets][implicit-limits].

**Details**

In differentially private aggregate functions, explicit clamping is optional.
If you don't include this clause, clamping is implicit,
which means bounds are derived from the data itself in a differentially
private way. The process is somewhat random, so aggregations with identical
ranges can have different bounds.

Implicit bounds are determined for each aggregation. So if some
aggregations have a wide range of values, and others have a narrow range of
values, implicit bounding can identify different bounds for different
aggregations as appropriate. Implicit bounds might be an advantage or a
disadvantage depending on your use case. Different bounds for different
aggregations can result in lower error. Different bounds also means that
different aggregations have different levels of uncertainty, which might not be
directly comparable. [Explicit bounds][dp-clamped-named], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

**Example**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity) average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 +----------+------------------*/
```

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 +----------+------------------*/
```

#### Explicitly clamp values 
<a id="dp_clamped_named"></a>

```googlesql
contribution_bounds_per_group => (lower_bound,upper_bound)
```

```googlesql
contribution_bounds_per_row => (lower_bound,upper_bound)
```

Use the contribution bounds named argument to explicitly clamp
values per group or per row between a lower and upper bound in a
`DIFFERENTIAL_PRIVACY` clause.

Input values:

+ `contribution_bounds_per_row`: Contributions per privacy unit are clamped
  on a per-row (per-record) basis. This means the following:
  +  Upper and lower bounds are applied to column values in individual
    rows produced by the input subquery independently.
  +  The maximum possible contribution per privacy unit (and per grouping set)
    is the product of the per-row contribution limit and `max_groups_contributed`
    differential privacy parameter.
+ `contribution_bounds_per_group`: Contributions per privacy unit are clamped
  on a unique set of entity-specified `GROUP BY` keys. The upper and lower
  bounds are applied to values per group after the values are aggregated per
  privacy unit.
+ `lower_bound`: Numeric literal that represents the smallest value to
  include in an aggregation.
+ `upper_bound`: Numeric literal that represents the largest value to
  include in an aggregation.

`NUMERIC` and `BIGNUMERIC` arguments aren't allowed.

**Details**

In differentially private aggregate functions, clamping explicitly clamps the
total contribution from each privacy unit column to within a specified
range.

Explicit bounds are uniformly applied to all aggregations. So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them. On the other hand, when
[implicit bounds][dp-clamped-named-imp] are inferred from the data, the bounds
applied to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because the age of most people generally falls within this range.

Important: The results of the query reveal the explicit bounds. Don't use
explicit bounds based on the entity data; explicit bounds should be based on
public information.

**Examples**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(0,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 +----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```googlesql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(50,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 +----------+------------------*/
```

The following differentially private query clamps each aggregate contribution
for each privacy unit column and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results will be
accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(0,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 +----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```googlesql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(50,100)) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------+
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 +----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[dp-example-views]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#eliminate_noise

[implicit-limits]: https://github.com/google/googlesql/blob/master/docs/differential-privacy.md#implicit_limits

[dp-clamped-named]: #dp_clamped_named

[dp-clamped-named-imp]: #dp_clamped_named_implicit

### `JSON` report format 
<a id="json_report_format"></a>

Some differentially private aggregate functions can return a
`JSON`-formatted report that has the following values:

```json
{
  "bounding_report": {
    "lower_bound": number,
    "num_inputs": number,
    "num_outside": number,
    "upper_bound": number
  },
  "result": {
    "noise_confidence_interval": {
      "confidence_level": number,
      "lower_bound": number,
      "upper_bound": number
    },
    "value": number
  }
}
```

Definitions:

+ `result`: The value produced by the function and its
  noise confidence interval.

  + `noise_confidence_interval`: The probability that the results will
    fall between a set of values.

    + `confidence_level`: The percentage of probability that the
      confidence interval contains the true results.

    + `lower_bound`: The lower bound of the confidence interval.

    + `upper_bound`: The upper bound of the confidence interval.

  + `value`: The value produced by the function.
+ `bounding_report`: Bounding report that's only generated when
  [implicit clamping][dp-clamped-named-imp] is used.

  + `lower_bound`: The inferred lower bound used for clamping.

  + `upper_bound`: The inferred upper bound used for clamping.

  + `num_inputs`: Total noisy count of inputs.

  + `num_outside`: Noisy number of inputs lying outside of bounds.

**Example**

With the following query, you can include a `JSON`-formatted report with your
results. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity, report_format=>'JSON') average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | average_quantity                                                 |
 +----------+------------------------------------------------------------------+
 | scissors | {                                                                |
 |          |   "bounding_report":{"lower_bound":4.0,"num_inputs":1.0,         |
 |          |   "num_outside":0.0,"upper_bound":8.0},                          |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":8.0,                     |
 |          |   "upper_bound":8.0},"value":8.0}                                |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pencil   | {                                                                |
 |          |   "bounding_report":{"lower_bound":64.0,"num_inputs":1.0,        |
 |          |   "num_outside":0.0,"upper_bound":128.0},                        |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":72.0,                    |
 |          |   "upper_bound":72.0},"value":72.0}                              |
 |          | }                                                                |
 +----------+------------------------------------------------------------------+
 | pen      | {                                                                |
 |          |   "bounding_report":{"lower_bound":8.0,"num_inputs":2.0,         |
 |          |   "num_outside":0.0,"upper_bound":32.0},                         |
 |          |   "result":{"noise_confidence_interval":{                        |
 |          |   "confidence_level":0.95,"lower_bound":18.5,                    |
 |          |   "upper_bound":18.5},"value":18.5}                              |
 |          | }                                                                |
 +----------+------------------------------------------------------------------*/
```

### `PROTO` report format 
<a id="proto_report_format"></a>

Some differentially private aggregate functions can return a
`PROTO`-formatted report that has the following values:

```proto
{
  {
    value {
      [float|int]_value: double or int64
      noise_confidence_interval {
        lower_bound: double
        upper_bound: double
        confidence_level: double
      }
    }
    bounding_report {
      lower_bound {
        [float|int]_value: double or int64
      }
      upper_bound {
        [float|int]_value: double or int64
      }
      num_inputs: double
      num_outside: double
    }
  }
}
```

Definitions:

+ `value`: The value produced by the function and its
  noise confidence interval.

  + `float_value` or `int_value`: The value produced by the function.

  + `noise_confidence_interval`: The probability that the results will
    fall between a set of values.

    + `lower_bound`: The lower bound of the confidence interval.

    + `upper_bound`: The upper bound of the confidence interval.

    + `confidence_level`: The percentage of probability that the
      confidence interval contains the true results.
+ `bounding_report`: Bounding report that's only generated when
  [implicit clamping][dp-clamped-named-imp] is used.

  + `lower_bound`: The inferred lower bound used for clamping.

  + `upper_bound`: The inferred upper bound used for clamping.

  + `num_inputs`: Total noisy count of inputs.

  + `num_outside`: Noisy number of inputs lying outside of bounds.

**Example**

With the following query, you can include a `PROTO`-formatted report with your
results. This query references a view called
[`view_on_professors`][dp-example-views].

```googlesql
-- Without noise, using the epsilon parameter.
-- This un-noised version is for demonstration only.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    AVG(quantity, report_format=>'PROTO') average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

/*----------+------------------------------------------------------------------+
 | item     | average_quantity                                                 |
 +----------+------------------------------------------------------------------+
 | scissors | value { float_value: 8.0 noise_confidence_interval {             |
 |          | lower_bound: 8.0 upper_bound: 8.0 confidence_level: 0.95 } }     |
 |          | bounding_report { lower_bound { float_value: 4.0 }               |
 |          | upper_bound { float_value: 8.0 } num_inputs: 1 num_outside: 0 }  |
 +----------+------------------------------------------------------------------+
 | pencil   | value { float_value: 72.0 noise_confidence_interval {            |
 |          | lower_bound: 72.0 upper_bound: 72.0 confidence_level: 0.95 } }   |
 |          | bounding_report { lower_bound { float_value: 64.0 }              |
 |          | upper_bound { float_value: 128.0 } num_inputs: 1 num_outside: 0 }|
 +----------+------------------------------------------------------------------+
 | pen      | value { float_value: 18.5 noise_confidence_interval {            |
 |          | lower_bound: 18.5 upper_bound: 18.5 confidence_level: 0.95 } }   |
 |          | bounding_report { lower_bound { float_value: 8.0 }               |
 |          | upper_bound { float_value: 32.0 } num_inputs: 2 num_outside: 0 } |
 +----------+------------------------------------------------------------------*/
```

[dp-guide]: https://github.com/google/googlesql/blob/master/docs/differential-privacy.md

[dp-syntax]: https://github.com/google/googlesql/blob/master/docs/query-syntax.md#dp_clause

[agg-function-calls]: https://github.com/google/googlesql/blob/master/docs/aggregate-function-calls.md

