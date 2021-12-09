

# Statistical aggregate functions

ZetaSQL supports the following statistical aggregate functions.

### CORR
```sql
CORR(
  X1, X2
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the [Pearson coefficient][stat-agg-link-to-pearson-coefficient]
of correlation of a set of number pairs. For each number pair, the first number
is the dependent variable and the second number is the independent variable.
The return result is between `-1` and `1`. A result of `0` indicates no
correlation.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more NULL values. If
there are fewer than two input pairs without NULL values, this function returns
NULL.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### COVAR_POP
```sql
COVAR_POP(
  X1, X2
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the population [covariance][stat-agg-link-to-covariance] of
a set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more NULL values. If
there is no input pair without NULL values, this function returns NULL. If there
is exactly one input pair without NULL values, this function returns 0.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### COVAR_SAMP
```sql
COVAR_SAMP(
  X1, X2
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the sample [covariance][stat-agg-link-to-covariance] of a
set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any input pairs that contain one or more NULL values. If
there are fewer than two input pairs without NULL values, this function returns
NULL.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### STDDEV_POP
```sql
STDDEV_POP(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the population (biased) standard deviation of the values. The return
result is between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any NULL inputs. If all inputs are ignored, this function
returns NULL.

If this function receives a single non-NULL input, it returns `0`.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### STDDEV_SAMP
```sql
STDDEV_SAMP(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the sample (unbiased) standard deviation of the values. The return
result is between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any NULL inputs. If there are fewer than two non-NULL
inputs, this function returns NULL.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### STDDEV
```sql
STDDEV(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

An alias of [STDDEV_SAMP][stat-agg-link-to-stddev-samp].

### VAR_POP
```sql
VAR_POP(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the population (biased) variance of the values. The return result is
between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any NULL inputs. If all inputs are ignored, this function
returns NULL.

If this function receives a single non-NULL input, it returns `0`.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### VAR_SAMP
```sql
VAR_SAMP(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the sample (unbiased) variance of the values. The return result is
between `0` and `+Inf`.

All numeric types are supported. If the
input is `NUMERIC` or `BIGNUMERIC` then the internal aggregation is
stable with the final output converted to a `DOUBLE`.
Otherwise the input is converted to a `DOUBLE`
before aggregation, resulting in a potentially unstable result.

This function ignores any NULL inputs. If there are fewer than two non-NULL
inputs, this function returns NULL.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Type**

`DOUBLE`

### VARIANCE
```sql
VARIANCE(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

An alias of [VAR_SAMP][stat-agg-link-to-var-samp].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[stat-agg-link-to-pearson-coefficient]: https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient

[stat-agg-link-to-covariance]: https://en.wikipedia.org/wiki/Covariance

[stat-agg-link-to-stddev-samp]: #stddev_samp

[stat-agg-link-to-var-samp]: #var_samp

<!-- mdlint on -->

