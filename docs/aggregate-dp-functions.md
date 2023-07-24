

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Differentially private aggregate functions 
<a id="aggregate-dp-functions"></a>

ZetaSQL supports differentially private aggregate functions.
For an explanation of how aggregate functions work, see
[Aggregate function calls][agg-function-calls].

You can only use differentially private aggregate functions with
[differentially private queries][dp-guide] in a
[differential privacy clause][dp-syntax].

Note: In this topic, the privacy parameters in the examples are not
recommendations. You should work with your privacy or security officer to
determine the optimal privacy parameters for your dataset and organization.

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#anon_avg"><code>ANON_AVG</code></a>

</td>
  <td>
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_count"><code>ANON_COUNT</code></a>

</td>
  <td>
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
  <td><a href="#anon_percentile_cont"><code>ANON_PERCENTILE_CONT</code></a>

</td>
  <td>
    Computes a differentially-private percentile across privacy unit columns
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_quantiles"><code>ANON_QUANTILES</code></a>

</td>
  <td>
    Produces an array of differentially-private quantile boundaries
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_stddev_pop"><code>ANON_STDDEV_POP</code></a>

</td>
  <td>
    Computes a differentially-private population (biased) standard deviation of
    values in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_sum"><code>ANON_SUM</code></a>

</td>
  <td>
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with an
    <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#anon_var_pop"><code>ANON_VAR_POP</code></a>

</td>
  <td>
    Computes a differentially-private population (biased) variance of values
    in a query with an <code>ANONYMIZATION</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_avg"><code>AVG</code> (differential privacy)</a>

</td>
  <td>
    Gets the differentially-private average of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_count"><code>COUNT</code> (differential privacy)</a>

</td>
  <td>
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
  <td><a href="#dp_percentile_cont"><code>PERCENTILE_CONT</code> (differential privacy)</a>

</td>
  <td>
    Computes a differentially-private percentile across privacy unit columns
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_sum"><code>SUM</code> (differential privacy)</a>

</td>
  <td>
    Gets the differentially-private sum of non-<code>NULL</code>,
    non-<code>NaN</code> values in a query with a
    <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

<tr>
  <td><a href="#dp_var_pop"><code>VAR_POP</code> (differential privacy)</a>

</td>
  <td>
    Computes the differentially-private population (biased) variance of values
    in a query with a <code>DIFFERENTIAL_PRIVACY</code> clause.
  </td>
</tr>

  </tbody>
</table>

### `ANON_AVG` 
<a id="anon_avg"></a>

```sql
WITH ANONYMIZATION ...
  ANON_AVG(expression [clamped_between_clause])

clamped_between_clause:
  CLAMPED BETWEEN lower_bound AND upper_bound
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `clamped_between_clause`: Perform [clamping][dp-clamp-between] per
  privacy unit column averages.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a view called [`view_on_professors`][dp-example-views].

```sql
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
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 *----------+------------------*/
```

```sql
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
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamp-between]: #dp_clamp_between

### `ANON_COUNT` 
<a id="anon_count"></a>

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```sql
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

```sql
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
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
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
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

#### Signature 2 
<a id="anon_count_signature2"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-clamp-implicit]: #dp_implicit_clamping

[dp-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#dp_from_rules

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamp-between]: #dp_clamp_between

### `ANON_PERCENTILE_CONT` 
<a id="anon_percentile_cont"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`view_on_professors`][dp-example-views].

```sql
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
/*----------+----------------------*
 | item     | percentile_requested |
 +----------+----------------------+
 | pencil   | 72.00011444091797    |
 | scissors | 8.000175476074219    |
 | pen      | 23.001075744628906   |
 *----------+----------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_QUANTILES` 
<a id="anon_quantiles"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`ARRAY`<`DOUBLE`>

**Examples**

The following differentially private query gets the five quantile boundaries of
the four quartiles of the number of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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
/*----------+----------------------------------------------------------------------*
 | item     | quantiles_requested                                                  |
 +----------+----------------------------------------------------------------------+
 | pen      | [6.409375,20.647684733072918,41.40625,67.30848524305556,99.80078125] |
 | pencil   | [6.849259,44.010416666666664,62.64204,65.83806818181819,98.59375]    |
 *----------+----------------------------------------------------------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_STDDEV_POP` 
<a id="anon_stddev_pop"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) standard deviation of items requested. Smaller aggregations
might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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
/*----------+------------------------*
 | item     | pop_standard_deviation |
 +----------+------------------------+
 | pencil   | 25.350871122442054     |
 | scissors | 50                     |
 | pen      | 2                      |
 *----------+------------------------*/
```

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `ANON_SUM` 
<a id="anon_sum"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per privacy unit column.

**Return type**

One of the following [supertypes][dp-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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
/*----------+-----------*
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 *----------+-----------*/
```

```sql
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
/*----------+----------*
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 *----------+----------*/
```

Note: You can learn more about when and when not to use
noise [here][dp-noise].

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamp-between]: #dp_clamp_between

### `ANON_VAR_POP` 
<a id="anon_var_pop"></a>

```sql
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
  Perform [clamping][dp-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations might not
be included. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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
/*----------+-----------------*
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 *----------+-----------------*/
```

[dp-clamp-explicit]: #dp_explicit_clamping

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clamp-between]: #dp_clamp_between

### `AVG` (differential privacy) 
<a id="dp_avg"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  AVG(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations might not be included. This query
references a table called [`professors`][dp-example-tables].

```sql
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
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | pencil   | 38.5038356810269 |
 | pen      | 13.4725028762032 |
 *----------+------------------*/
```

```sql
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
/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `COUNT` (differential privacy) 
<a id="dp_count"></a>

+ [Signature 1](#dp_count_signature1): Returns the number of rows in a
  differentially private `FROM` clause.
+ [Signature 2](#dp_count_signature2): Returns the number of non-`NULL`
  values in an expression.

#### Signature 1 
<a id="dp_count_signature1"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(* [, contribution_bounds_per_group => (lower_bound, upper_bound)]))
```

**Description**

Returns the number of rows in the
[differentially private][dp-from-clause] `FROM` clause. The final result
is an aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support the following argument:

+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a table called
[`professors`][dp-example-tables].

```sql
-- With noise, using the epsilon parameter.
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations might be removed.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
-- Without noise, using the epsilon parameter.
-- (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### Signature 2 
<a id="dp_count_signature2"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across a privacy unit column.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This expression can be any
  numeric input type, such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a table called
[`professors`][dp-example-tables].

```sql
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
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | pencil   | 5               |
 | pen      | 2               |
 *----------+-----------------*/
```

```sql
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
/*----------+-----------------*
 | item     | times_requested |
 +----------+-----------------+
 | scissors | 1               |
 | pencil   | 4               |
 | pen      | 3               |
 *----------+-----------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

[dp-clamp-implicit]: #dp_implicit_clamping

[dp-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#dp_from

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

[dp-clamped-named]: #dp_clamped_named

### `PERCENTILE_CONT` (differential privacy) 
<a id="dp_percentile_cont"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  PERCENTILE_CONT(expression, percentile, contribution_bounds_per_row => (lower_bound, upper_bound))
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
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on the privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations might not be included. This query references a
view called [`professors`][dp-example-tables].

```sql
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
 /*----------+----------------------*
  | item     | percentile_requested |
  +----------+----------------------+
  | pencil   | 72.00011444091797    |
  | scissors | 8.000175476074219    |
  | pen      | 23.001075744628906   |
  *----------+----------------------*/
```

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `SUM` (differential privacy) 
<a id="dp_sum"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  SUM(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across privacy unit columns.

This function must be used with the [`DIFFERENTIAL_PRIVACY` clause][dp-syntax]
and can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL` values are always ignored.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

One of the following [supertypes][dp-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations might not be included. This query references a view called
[`professors`][dp-example-tables].

```sql
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
/*----------+-----------*
 | item     | quantity  |
 +----------+-----------+
 | pencil   | 143       |
 | pen      | 59        |
 *----------+-----------*/
```

```sql
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
/*----------+----------*
 | item     | quantity |
 +----------+----------+
 | scissors | 8        |
 | pencil   | 144      |
 | pen      | 58       |
 *----------+----------*/
```

Note: For more information about when and when not to use
noise, see [Use differential privacy][dp-noise].

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[dp-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[dp-clamped-named]: #dp_clamped_named

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

### `VAR_POP` (differential privacy) 
<a id="dp_var_pop"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  VAR_POP(expression[, contribution_bounds_per_row => (lower_bound, upper_bound)])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][dp-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual user values.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][dp-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on individual user values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them as the
`DOUBLE` data type first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations may not
be included. This query references a view called
[`professors`][dp-example-tables].

```sql
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
/*----------+-----------------*
 | item     | pop_variance    |
 +----------+-----------------+
 | pencil   | 642             |
 | pen      | 2.6666666666665 |
 | scissors | 2500            |
 *----------+-----------------*/
```

[dp-clamp-explicit]: #dp_explicit_clamping

[dp-example-tables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_tables

[dp-clamped-named]: #dp_clamped_named

### Clamp values in a differentially private aggregate function 
<a id="dp_clamping"></a>

In [differentially private queries][dp-syntax],
aggregation clamping is used to limit the contribution of outliers. You can
clamp explicitly or implicitly as follows:

+ [Clamp explicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named].
+ [Clamp implicitly in the `DIFFERENTIAL_PRIVACY` clause][dp-clamped-named-imp].
+ [Clamp explicitly in the `ANONYMIZATION` clause][dp-clamp-between].
+ [Clamp implicitly in the `ANONYMIZATION` clause][dp-clamp-between-imp].

To learn more about explicit and implicit clamping, see the following:

+ [About implicit clamping][dp-imp-clamp].
+ [About explicit clamping][dp-exp-clamp].

#### Implicitly clamp values in the `DIFFERENTIAL_PRIVACY` clause 
<a id="dp_clamped_named_implicit"></a>

If you don't include the contribution bounds named argument with the
`DIFFERENTIAL_PRIVACY` clause, clamping is [implicit][dp-imp-clamp], which
means bounds are derived from the data itself in a differentially private way.

Implicit bounding works best when computed using large datasets. For more
information, see [Implicit bounding limitations for small datasets][implicit-limits].

**Example**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 *----------+------------------*/
```

#### Explicitly clamp values in the `DIFFERENTIAL_PRIVACY` clause 
<a id="dp_clamped_named"></a>

```sql
contribution_bounds_per_group => (lower_bound,upper_bound)
```

```sql
contribution_bounds_per_row => (lower_bound,upper_bound)
```

Use the contribution bounds named argument to [explicitly clamp][dp-exp-clamp]
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

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.

**Examples**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
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

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```sql {.bad}
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

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### Implicitly clamp values in the `ANONYMIZATION` clause 
<a id="dp_clamp_between_implicit"></a>

If you don't include the `CLAMPED BETWEEN` clause with the
`ANONYMIZATION` clause, clamping is [implicit][dp-imp-clamp], which means bounds
are derived from the data itself in a differentially private way.

Implicit bounding works best when computed using large datasets. For more
information, see [Implicit bounding limitations for small datasets][implicit-limits].

**Example**

The following anonymized query clamps each aggregate contribution for each
differential privacy ID and within a derived range from the data itself.
As long as all or most values fall within this range, your results
will be accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 72               |
 | pen      | 18.5             |
 *----------+------------------*/
```

#### Explicitly clamp values in the `ANONYMIZATION` clause 
<a id="dp_clamp_between"></a>

```sql
CLAMPED BETWEEN lower_bound AND upper_bound
```

Use the `CLAMPED BETWEEN` clause to [explicitly clamp][dp-exp-clamp] values
between a lower and an upper bound in an `ANONYMIZATION` clause.

Input values:

+ `lower_bound`: Numeric literal that represents the smallest value to
  include in an aggregation.
+ `upper_bound`: Numeric literal that represents the largest value to
  include in an aggregation.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.

Note: This is a legacy feature. If possible, use the `contribution_bounds`
named argument instead.

**Examples**

The following differentially private query clamps each aggregate contribution
for each privacy unit column and within a specified range (`0` and `100`).
As long as all or most values fall within this range, your results will be
accurate. This query references a view called
[`view_on_professors`][dp-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 8                |
 | pencil   | 40               |
 | pen      | 18.5             |
 *----------+------------------*/
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible, and that most inputs are between the upper and
lower bound.

```sql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  ANON_AVG(quantity CLAMPED BETWEEN 50 AND 100) AS average_quantity
FROM view_on_professors
GROUP BY item;

/*----------+------------------*
 | item     | average_quantity |
 +----------+------------------+
 | scissors | 54               |
 | pencil   | 58               |
 | pen      | 51               |
 *----------+------------------*/
```

Note: For more information about when and when not to use
noise, see [Remove noise][dp-noise].

#### About explicit clamping 
<a id="dp_explicit_clamping"></a>

In differentially private aggregate functions, clamping explicitly clamps the
total contribution from each privacy unit column to within a specified
range.

Explicit bounds are uniformly applied to all aggregations.  So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them.  On the other hand, when
[implicit bounds][dp-imp-clamp] are inferred from the data, the bounds applied
to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because the age of most people generally falls within this range.

Important: The results of the query reveal the explicit bounds. Do not use
explicit bounds based on the entity data; explicit bounds should be based on
public information.

#### About implicit clamping 
<a id="dp_implicit_clamping"></a>

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
directly comparable. [Explicit bounds][dp-exp-clamp], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

[dp-guide]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md

[dp-syntax]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

[dp-exp-clamp]: #dp_explicit_clamping

[dp-imp-clamp]: #dp_implicit_clamping

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[implicit-limits]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#implicit_limits

[dp-clamp-between]: #dp_clamp_between

[dp-clamp-between-imp]: #dp_clamp_between_implicit

[dp-clamped-named]: #dp_clamped_named

[dp-clamped-named-imp]: #dp_clamped_named_implicit

