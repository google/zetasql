

# Anonymization aggregate functions 
<a id="aggregate_anonymization_functions"></a>

The following anonymization aggregate functions are available in
ZetaSQL. For an explanation of how aggregate functions work, see
[Aggregate function calls][agg-function-calls].

Anonymization aggregate functions can transform user data into anonymous
information. This is done in such a way that it is not reasonably likely that
anyone with access to the data can identify or re-identify an individual user
from the anonymized data. Anonymization aggregate functions can only be used
with [anonymization-enabled queries][anon-syntax].

### ANON_AVG

```sql
ANON_AVG(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per anonymization ID, and then computes
the final result by averaging these averages.

You can [clamp the input values explicitly][anon-clamp], otherwise
input values are clamped implicitly. Clamping is done to the
per-anonymization ID averages.

`expression` can be any numeric input type, such as
INT64.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the average number of each item requested
per professor. Smaller aggregations may not be included. This query references
a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
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
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity) average_quantity
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#eliminate_noise

### ANON_COUNT

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```sql
ANON_COUNT(*)
```

**Description**

Returns the number of rows in the [anonymization-enabled][anon-from-clause]
`FROM` clause. The final result is an aggregation across anonymization IDs.
[Input values are clamped implicitly][anon-clamp]. Clamping is performed per
anonymization ID.

**Return type**

`INT64`

**Examples**

The following anonymized query counts the number of requests for each item.
This query references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_COUNT(*) times_requested
FROM view_on_professors
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
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_COUNT(*) times_requested
FROM view_on_professors
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
ANON_COUNT(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across anonymization IDs.

You can [clamp the input values explicitly][anon-clamp], otherwise
input values are clamped implicitly. Clamping is performed per anonymization ID.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Return type**

`INT64`

**Examples**

The following anonymized query counts the number of requests made for each
type of item. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM view_on_professors
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
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM view_on_professors
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

[anon-from-clause]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_from

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#eliminate_noise

### ANON_PERCENTILE_CONT

```sql
ANON_PERCENTILE_CONT(expression, percentile [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across anonymization IDs. The percentile must be a literal in the
range [0, 1]. You can [clamp the input values][anon-clamp] explicitly,
otherwise input values are clamped implicitly. Clamping is performed per
anonymization ID.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the percentile of items requested. Smaller
aggregations may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_PERCENTILE_CONT(quantity, 0.5 CLAMPED BETWEEN 0 AND 100) percentile_requested
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

### ANON_QUANTILES

```sql
ANON_QUANTILES(expression, number CLAMPED BETWEEN lower AND upper)
```

**Description**

Returns an array of anonymized quantile boundaries for values in `expression`.
`number` represents the number of quantiles to create and must be an
`INT64`. The first element in the return value is the
minimum quantile boundary and the last element is the maximum quantile boundary.
`lower` and `upper` are the explicit bounds wherein the
[input values are clamped][anon-clamp]. The returned results are aggregations
across anonymization IDs.

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`ARRAY`<`DOUBLE`>

**Examples**

The following anonymized query gets the five quantile boundaries of the four
quartiles of the number of items requested. Smaller aggregations may not be
included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_QUANTILES(quantity, 4 CLAMPED BETWEEN 0 AND 100) quantiles_requested
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

### ANON_STDDEV_POP

```sql
ANON_STDDEV_POP(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes the population (biased) standard deviation of
the values in the expression. The final result is an aggregation across
anonymization IDs between `0` and `+Inf`. You can
[clamp the input values][anon-clamp] explicitly, otherwise input values are
clamped implicitly. Clamping is performed per individual user values.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the population (biased) standard deviation
of items requested. Smaller aggregations may not be included. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_STDDEV_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_standard_deviation
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

### ANON_SUM

```sql
ANON_SUM(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across anonymization IDs. You can optionally
[clamp the input values][anon-clamp]. Clamping is performed per
anonymization ID.

The expression can be any numeric input type, such as
`INT64`.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Return type**

One of the following [supertypes][anon-supertype]:

+ `INT64`
+ `UINT64`
+ `DOUBLE`

**Examples**

The following anonymized query gets the sum of items requested. Smaller
aggregations may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_SUM(quantity CLAMPED BETWEEN 0 AND 100) quantity
FROM view_on_professors
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
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_SUM(quantity) quantity
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#eliminate_noise

[anon-supertype]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md#supertypes

### ANON_VAR_POP

```sql
ANON_VAR_POP(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
anonymization IDs between `0` and `+Inf`. You can
[clamp the input values][anon-clamp] explicitly, otherwise input values are
clamped implicitly. Clamping is performed per individual user values.

To learn more about the optional arguments in this function and how to use them,
see [Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the population (biased) variance
of items requested. Smaller aggregations may not be included. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_VAR_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_variance
FROM view_on_professors
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

[anon-clamp]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md#anon_clamping

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-syntax]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

