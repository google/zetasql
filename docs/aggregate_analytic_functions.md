
<!-- This file is auto-generated. DO NOT EDIT.                               -->

# Aggregate Analytic Functions

The following sections describe the aggregate analytic functions that
ZetaSQL supports. For an explanation of how analytic functions work,
see [Analytic Function Concepts][analytic-function-concepts]. For an explanation
of how aggregate analytic functions work, see
[Aggregate Analytic Function Concepts][aggregate-analytic-concepts].

ZetaSQL supports the following
[aggregate functions][analytic-functions-link-to-aggregate-functions]
as analytic functions:

* [ANY_VALUE](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#any_value)
* [ARRAY_AGG](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#array_agg)
* [AVG](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#avg)
* [CORR](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#corr)
* [COUNT](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#count)
* [COUNTIF](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#countif)
* [COVAR_POP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#covar_pop)
* [COVAR_SAMP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#covar_samp)
* [LOGICAL_AND](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#logical_and)
* [LOGICAL_OR](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#logical_or)
* [MAX](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#max)
* [MIN](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#min)
* [STDDEV_POP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#stddev_pop)
* [STDDEV_SAMP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#stddev_samp)
* [STRING_AGG](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#string_agg)
* [SUM](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#sum)
* [VAR_POP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#var_pop)
* [VAR_SAMP](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#var_samp)

`OVER` clause requirements:

+ `PARTITION BY`: Optional.
+ `ORDER BY`: Optional. Disallowed if `DISTINCT` is present.
+ `window_frame_clause`: Optional. Disallowed if `DISTINCT` is present.

Example:

```
COUNT(*) OVER (ROWS UNBOUNDED PRECEDING)
```

```
SUM(DISTINCT x) OVER ()
```

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts
[aggregate-analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts#aggregate_analytic_function_concepts

[analytic-functions-link-to-aggregate-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions

