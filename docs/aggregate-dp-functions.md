

# Differentially private aggregate functions 
<a id="aggregate-dp-functions"></a>

ZetaSQL supports differentially private aggregate functions.
For an explanation of how aggregate functions work, see
[Aggregate function calls][agg-function-calls].

Differentially private aggregate functions can only be
used with [differentially private queries][anon-syntax].

### `ANON_AVG` 
<a id="anon_avg"></a>

```sql
WITH ANONYMIZATION ...
  ANON_AVG(expression [CLAMPED BETWEEN lower_bound AND upper_bound])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][anon-clamp-between] per privacy unit column averages.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations may not be included. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 38.5038356810269 |
| pen      | 13.4725028762032 |
+----------+------------------+
```

```sql
-- Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-clamp-between]: #anon_clamp_between

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
[differentially private][anon-from-clause] `FROM` clause. The final result
is an aggregation across privacy unit columns.
[Input values are clamped implicitly][anon-clamp-implicit]. Clamping is
performed per privacy unit column.

This function must be used with the `ANONYMIZATION` clause.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_COUNT(*) times_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

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
  Perform [clamping][anon-clamp-between] per privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a view called
[`view_on_professors`][anon-example-views].

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
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
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
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

[anon-clamp-implicit]: #anon_implicit_clamping

[anon-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_from

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-clamp-between]: #anon_clamp_between

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
  Perform [clamping][anon-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations may not be included. This query references a
view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_PERCENTILE_CONT(quantity, 0.5 CLAMPED BETWEEN 0 AND 100) percentile_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+----------------------+
| item     | percentile_requested |
+----------+----------------------+
| pencil   | 72.00011444091797    |
| scissors | 8.000175476074219    |
| pen      | 23.001075744628906   |
+----------+----------------------+
```

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-clamp-between]: #anon_clamp_between

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
  Perform [clamping][anon-clamp-between] per privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`ARRAY`<`DOUBLE`>

**Examples**

The following differentially private query gets the five quantile boundaries of
the four quartiles of the number of items requested. Smaller aggregations
may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_QUANTILES(quantity, 4 CLAMPED BETWEEN 0 AND 100) quantiles_requested
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+----------------------------------------------------------------------+
| item     | quantiles_requested                                                  |
+----------+----------------------------------------------------------------------+
| pen      | [6.409375,20.647684733072918,41.40625,67.30848524305556,99.80078125] |
| pencil   | [6.849259,44.010416666666664,62.64204,65.83806818181819,98.59375]    |
+----------+----------------------------------------------------------------------+
```

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-clamp-between]: #anon_clamp_between

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
  Perform [clamping][anon-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) standard deviation of items requested. Smaller aggregations
may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_STDDEV_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_standard_deviation
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+------------------------+
| item     | pop_standard_deviation |
+----------+------------------------+
| pencil   | 25.350871122442054     |
| scissors | 50                     |
| pen      | 2                      |
+----------+------------------------+
```

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-clamp-between]: #anon_clamp_between

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
  Perform [clamping][anon-clamp-between] per privacy unit column.

**Return type**

One of the following [supertypes][anon-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following differentially private query gets the sum of items requested.
Smaller aggregations may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity CLAMPED BETWEEN 0 AND 100) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------+
| item     | quantity  |
+----------+-----------+
| pencil   | 143       |
| pen      | 59        |
+----------+-----------+
```

```sql
-- Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_SUM(quantity) quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+----------+
| item     | quantity |
+----------+----------+
| scissors | 8        |
| pencil   | 144      |
| pen      | 58       |
+----------+----------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

[anon-clamp-between]: #anon_clamp_between

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
[clamp the input values][anon-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual entity values.

This function must be used with the `ANONYMIZATION` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `CLAMPED BETWEEN` clause:
  Perform [clamping][anon-clamp-between] per individual entity values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations may not
be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1)
    item,
    ANON_VAR_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_variance
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | pop_variance    |
+----------+-----------------+
| pencil   | 642             |
| pen      | 2.6666666666665 |
| scissors | 2500            |
+----------+-----------------+
```

[anon-clamp-explicit]: #anon_explicit_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-clamp-between]: #anon_clamp_between

### `AVG` (differential privacy) 
<a id="anon_avg"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  AVG(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per privacy unit column, and then
computes the final result by averaging these averages.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][anon-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the average number of each item
requested per professor. Smaller aggregations may not be included. This query
references a table called [`professors`][anon-example-tables].

```sql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 38.5038356810269 |
| pen      | 13.4725028762032 |
+----------+------------------+
```

```sql
-- Without noise (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    AVG(quantity) average_quantity
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

[anon-example-tables]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_tables

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-clamped-named]: #anon_clamped_named

### `COUNT` (differential privacy) 
<a id="anon_count"></a>

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(* [, contribution_bounds_per_group => (lower_bound, upper_bound)]))
```

**Description**

Returns the number of rows in the
[differentially private][anon-from-clause] `FROM` clause. The final result
is an aggregation across privacy unit columns.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support the following argument:

+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][anon-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests for
each item. This query references a table called
[`professors`][anon-example-tables].

```sql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(*) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

#### Signature 2 
<a id="anon_count_signature2"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  COUNT(expression[, contribution_bounds_per_group => (lower_bound, upper_bound)])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across privacy unit columns.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`.
+ `contribution_bounds_per_group`: The
  [contribution bounds named argument][anon-clamped-named].
  Perform clamping per each group separately before performing intermediate
  grouping on the privacy unit column.

**Return type**

`INT64`

**Examples**

The following differentially private query counts the number of requests made
for each type of item. This query references a table called
[`professors`][anon-example-tables].

```sql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    COUNT(item, contribution_bounds_per_group => (0,100)) times_requested
FROM professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

[anon-clamp-implicit]: #anon_implicit_clamping

[anon-from-clause]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_from

[anon-example-tables]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_tables

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-clamped-named]: #anon_clamped_named

### `PERCENTILE_CONT` (differential privacy) 
<a id="anon_percentile_cont"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  PERCENTILE_CONT(expression, percentile, contribution_bounds_per_row => (lower_bound, upper_bound))
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across privacy unit columns.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be most numeric input types,
  such as `INT64`. `NULL`s are always ignored.
+ `percentile`: The percentile to compute. The percentile must be a literal in
  the range [0, 1]
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][anon-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on the privacy unit column.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the percentile of items
requested. Smaller aggregations may not be included. This query references a
view called [`professors`][anon-example-tables].

```sql
-- With noise
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=1, privacy_unit_column=id)
    item,
    PERCENTILE_CONT(quantity, 0.5, contribution_bounds_per_row => (0,100)) percentile_requested
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+----------------------+
| item     | percentile_requested |
+----------+----------------------+
| pencil   | 72.00011444091797    |
| scissors | 8.000175476074219    |
| pen      | 23.001075744628906   |
+----------+----------------------+
```

[anon-example-tables]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_tables

[anon-clamped-named]: #anon_clamped_named

### `VAR_POP` (differential privacy) 
<a id="anon_var_pop"></a>

```sql
WITH DIFFERENTIAL_PRIVACY ...
  VAR_POP(expression[, contribution_bounds_per_row => (lower_bound, upper_bound)])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
privacy unit columns between `0` and `+Inf`. You can
[clamp the input values][anon-clamp-explicit] explicitly, otherwise input values
are clamped implicitly. Clamping is performed per individual user values.

This function must be used with the `DIFFERENTIAL_PRIVACY` clause and
can support these arguments:

+ `expression`: The input expression. This can be any numeric input type,
  such as `INT64`. `NULL`s are always ignored.
+ `contribution_bounds_per_row`: The
  [contribution bounds named argument][anon-clamped-named].
  Perform clamping per each row separately before performing intermediate
  grouping on individual user values.

`NUMERIC` and `BIGNUMERIC` arguments are not allowed.
 If you need them, cast them to
`DOUBLE` first.

**Return type**

`DOUBLE`

**Examples**

The following differentially private query gets the
population (biased) variance of items requested. Smaller aggregations may not
be included. This query references a view called
[`professors`][anon-example-tables].

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
+----------+-----------------+
| item     | pop_variance    |
+----------+-----------------+
| pencil   | 642             |
| pen      | 2.6666666666665 |
| scissors | 2500            |
+----------+-----------------+
```

[anon-clamp-explicit]: #anon_explicit_clamping

[anon-example-tables]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_tables

[anon-clamped-named]: #anon_clamped_named

### Clamp values in a differentially private aggregate function 
<a id="anon_clamping"></a>

In [differentially private queries][anon-differential-privacy],
aggregation clamping is used to limit the contribution of outliers. You can
clamp [implicitly][anon-imp-clamp] or [explicitly][anon-exp-clamp].

If you clamp explicitly, you can clamp values with the
[contribution bounds named argument][anon-clamped-named] (recommended) or
the [`CLAMPED BETWEEN`][anon-clamp-between] clause.

#### Clamp with the contribution bounds named argument 
<a id="anon_clamped_named"></a>

```sql
contribution_bounds_per_group => (lower_bound,upper_bound)
```

```sql
contribution_bounds_per_row => (lower_bound,upper_bound)
```

Use this named argument to clamp values per group or
per row between a lower and upper bound.

+ `contribution_bounds_per_row`: Contributions per privacy unit are clamped
  on a per-row (per-record) basis. This means that:
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
[`view_on_professors`][anon-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    privacy_unit_column=id
  )
  item,
  AVG(quantity, contribution_bounds_per_group=>(0,100)) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
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
  AVG(quantity, contribution_bounds_per_group=>(50,100)) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 54               |
| pencil   | 58               |
| pen      | 51               |
+----------+------------------+
```

To learn more about when and when not to use noise in
differentially private queries, see [Differentially privacy][anon-noise].

#### Clamp with the `CLAMPED BETWEEN` clause 
<a id="anon_clamp_between"></a>

```sql
CLAMPED BETWEEN lower_bound AND upper_bound
```

Use this clause to clamp values between a lower and an upper bound.

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
[`view_on_professors`][anon-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT WITH ANONYMIZATION
  OPTIONS (
    epsilon = 1e20,
    delta = .01,
    max_groups_contributed = 1
  )
  item,
  AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
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
  AVG(quantity CLAMPED BETWEEN 50 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 54               |
| pencil   | 58               |
| pen      | 51               |
+----------+------------------+
```

To learn more about when and when not to use noise in
differentially private queries, see [Differentially privacy][anon-noise].

#### Explicit clamping 
<a id="anon_explicit_clamping"></a>

In differentially private aggregate functions, clamping explicitly clamps the
total contribution from each privacy unit column to within a specified
range.

Explicit bounds are uniformly applied to all aggregations.  So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them.  On the other hand, when
[implicit bounds][anon-imp-clamp] are inferred from the data, the bounds applied
to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because the age of most people generally falls within this range.

Important: The results of the query reveal the explicit bounds. Do not use
explicit bounds based on the entity data; explicit bounds should be based on
public information.

#### Implicit clamping 
<a id="anon_implicit_clamping"></a>

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
directly comparable. [Explicit bounds][anon-exp-clamp], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

[anon-syntax]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

[anon-differential-privacy]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md

[anon-exp-clamp]: #anon_explicit_clamping

[anon-imp-clamp]: #anon_implicit_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/differential-privacy.md#eliminate_noise

[anon-clamp-between]: #anon_clamp_between

[anon-clamped-named]: #anon_clamped_named

