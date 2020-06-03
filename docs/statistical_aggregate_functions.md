
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Statistical Aggregate Functions

ZetaSQL supports the following statistical aggregate functions.

### CORR
```
CORR(X1, X2 [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the [Pearson coefficient][stat-agg-link-to-pearson-coefficient]
of correlation of a set of number pairs. For each number pair, the first number
is the dependent variable and the second number is the independent variable.
The return result is between `-1` and `1`. A result of `0` indicates no
correlation.

This function ignores any input pairs that contain one or more NULL values. If
there are fewer than two input pairs without NULL values, this function returns
NULL.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### COVAR_POP
```
COVAR_POP(X1, X2 [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the population [covariance][stat-agg-link-to-covariance] of
a set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

This function ignores any input pairs that contain one or more NULL values. If
there is no input pair without NULL values, this function returns NULL. If there
is exactly one input pair without NULL values, this function returns 0.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### COVAR_SAMP
```
COVAR_SAMP(X1, X2 [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the sample [covariance][stat-agg-link-to-covariance] of a
set of number pairs. The first number is the dependent variable; the second
number is the independent variable. The return result is between `-Inf` and
`+Inf`.

This function ignores any input pairs that contain one or more NULL values. If
there are fewer than two input pairs without NULL values, this function returns
NULL.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### STDDEV_POP
```
STDDEV_POP([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the population (biased) standard deviation of the values. The return
result is between `0` and `+Inf`.

This function ignores any NULL inputs. If all inputs are ignored, this function
returns NULL.

If this function receives a single non-NULL input, it returns `0`.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### STDDEV_SAMP
```
STDDEV_SAMP([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the sample (unbiased) standard deviation of the values. The return
result is between `0` and `+Inf`.

This function ignores any NULL inputs. If there are fewer than two non-NULL
inputs, this function returns NULL.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### STDDEV
```
STDDEV([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

An alias of [STDDEV_SAMP][stat-agg-link-to-stddev-samp].

### VAR_POP
```
VAR_POP([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the population (biased) variance of the values. The return result is
between `0` and `+Inf`.

This function ignores any NULL inputs. If all inputs are ignored, this function
returns NULL.

If this function receives a single non-NULL input, it returns `0`.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### VAR_SAMP
```
VAR_SAMP([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

Returns the sample (unbiased) variance of the values. The return result is
between `0` and `+Inf`.

This function ignores any NULL inputs. If there are fewer than two non-NULL
inputs, this function returns NULL.

**Supported Input Types**

DOUBLE

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates to those having a value for `expression2` equal to the
    maximum or minimum value for `expression2`. The  maximum or minimum value is
    equal to the result of `MAX(expression2)` or `MIN(expression2)`. This clause
    ignores `NULL` values when computing the maximum or minimum value unless
    `expression2` evaluates to `NULL` for all rows.
    This clause is not compatible with the `OVER` clause. This clause
    does not support the following data types:
    `ARRAY`
    `STRUCT`
    `PROTO`

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types#floating_point_semantics

**Return Data Type**

DOUBLE

### VARIANCE
```
VARIANCE([DISTINCT] expression [HAVING (MAX | MIN) expression2])  [OVER (...)]
```

**Description**

An alias of [VAR_SAMP][stat-agg-link-to-var-samp].

[stat-agg-link-to-pearson-coefficient]: https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
[stat-agg-link-to-covariance]: https://en.wikipedia.org/wiki/Covariance
[stat-agg-link-to-stddev-samp]: #stddev_samp
[stat-agg-link-to-var-samp]: #var_samp

