

# Anonymization aggregate functions

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

**Return type**

Matches the type in the expression.

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

### CLAMPED BETWEEN clause 
<a id="anon_clamping"></a>

```sql
CLAMPED BETWEEN lower_bound AND upper_bound
```

Clamping of aggregations is done to avoid the re-identifiability
of outliers. The `CLAMPED BETWEEN` clause [explicitly clamps][anon-exp-clamp]
each aggregate contribution per anonymization ID within the specified range.
This clause is optional. If you do not include it in an anonymization aggregate
function, [clamping is implicit][anon-imp-clamp].

**Examples**

The following anonymized query clamps each aggregate contribution per
anonymization ID and within a specified range (`0` and `100`). As long as all
or most values fall within this range, your results will be accurate. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
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
bound is as small as possible while most inputs are between the upper and lower
bound.

```sql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 50 AND 100) average_quantity
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

Note: You can learn more about when and when not to use
noise [here][anon-noise].

### Explicit clamping 
<a id="anon_explicit_clamping"></a>

In an anonymization aggregate function, the [`CLAMPED BETWEEN`][anon-clamp]
clause explicitly clamps the total contribution from each anonymization ID to
within a specified range.

Explicit bounds are uniformly applied to all aggregations.  So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them.  On the other hand, when
[implicit bounds][anon-imp-clamp] are inferred from the data, the bounds applied
to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because in general, the age of most people falls within this range.

Important: The results of the query reveal the explicit bounds. Do not use
explicit bounds based on the user data; explicit bounds should be based on
public information.

### Implicit clamping 
<a id="anon_implicit_clamping"></a>

In an anonymization aggregate function, the [`CLAMPED BETWEEN`][anon-clamp]
clause is optional. If you do not include this clause, clamping is implicit,
which means bounds are derived from the data itself in a differentially
private way. The process is somewhat random, so aggregations with identical
ranges can have different bounds.

Implicit bounds are determined per aggregation. So if some
aggregations have a wide range of values, and others have a narrow range of
values, implicit bounding can identify different bounds for different
aggregations as appropriate. This may be an advantage or a disadvantage
depending on your use case: different bounds for different aggregations
can result in lower error, but this also means that different
aggregations have different levels of uncertainty, which may not be
directly comparable. [Explicit bounds][anon-exp-clamp], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[anon-clamp]: #anon_clamping

[anon-exp-clamp]: #anon_explicit_clamping

[anon-imp-clamp]: #anon_implicit_clamping

[anon-syntax]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-from-clause]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_from

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#eliminate_noise

<!-- mdlint on -->

