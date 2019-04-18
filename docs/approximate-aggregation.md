

<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Approximate Aggregation

This topic explains the concepts behind approximate
aggregation in ZetaSQL.

## What is approximate aggregation?

Approximate aggregation is the estimation of aggregate function
outputs, such as cardinality and quantiles. Approximate
aggregation requires less memory than normal aggregation functions like
`COUNT(DISTINCT ...)`, but also introduces statistical uncertainty. This makes it
appropriate for large data streams for which linear memory usage is impractical,
as well as for data that is already approximate. Where exact results are
necessary, use exact [aggregate functions][exact-aggregate-fns]. This topic
describes the concepts behind approximate aggregation in ZetaSQL.

## Fixed-precision approximate aggregation

ZetaSQL supports
[aggregate functions with fixed precision][link-to-APPROX_] for estimating
cardinality and quantiles. These functions work directly on the input data,
rather than an intermediate estimation of the data. These functions do not allow
users to specify the precision for the estimation.

## Storing estimated aggregate values as sketches

ZetaSQL supports
[HyperLogLog++ cardinality estimation functions][hll-functions] for estimating
the number of distinct values in a large dataset.

These functions operate on sketches that compress an arbitrary set into a
fixed-memory representation. ZetaSQL stores these sketches as
`BYTES`. You can merge the sketches to produce a new sketch that represents the
union of the input sketches, before you extract a final numeric estimate from
the sketch.

For example, consider the following table, which contains ice-cream flavors and
the number of people who claimed to enjoy that flavor:

Flavor            | People
------------------|--------
Vanilla           | 3945
Chocolate         | 1728
Strawberry        | 2051

If this table is the result of aggregation, then it may not be possible to use
the table to calculate cardinality. If you wanted to know the number of unique
respondents, you couldn't use `SUM` to aggregate the `People` column, because
some respondents may have responded positively to more than one flavor. On the
other hand, performing an aggregate function over the underlying raw
data can consume large amounts of time and memory.

One solution is to store an approximate aggregate or **sketch** of the raw
data. A sketch is a summary of the raw data. Sketches require less memory than
the raw data, and you can extract an estimate, such as the estimated number of
unique users, from the sketch.

### Specifying approximation precision

ZetaSQL's sketch-based approximate aggregation functions
allow you to specify the precision of the sketch when you create it. The
precision of the sketch affects the accuracy of the estimate you can extract
from the sketch. Higher precision requires additional memory to process the
sketches or store them on disk, and reduces the relative error of any estimate
you extract from the sketch. Once you have created the sketch, you can only
merge it with other sketches of the same precision.

### Merging sketches

You can merge two or more sketches to produce a new sketch which represents an
estimate of the union of the data underlying the different sketches. The merge
function, such as `HLL_COUNT.MERGE`,
returns the estimate as a number, whereas the partial merge
function, such as
`HLL_COUNT.MERGE_PARTIAL`, returns the new sketch in `BYTES`.
A partial merge is useful if you want to reduce a table that already contains
sketches, but do not yet want to extract an estimate. For example, use this
function if you want to create a sketch that you will merge with another sketch
later.

### Extracting estimates from sketches

Once you have stored a sketch or merged two or more sketches into a new sketch,
you can use the extract function, such as
`HLL_COUNT.EXTRACT`, to return an estimate of the underlying data,
such as the estimated number of unique users, as a number.

## Algorithms

 This section describes the
approximate aggregate algorithms that ZetaSQL supports.

### HyperLogLog++

The [HyperLogLog++][HyperLogLogPlusPlus-paper] algorithm is an improvement on
the [HyperLogLog][hll-link-to-hyperloglog-wikipedia] algorithm for estimating
distinct values in a data set. In ZetaSQL, the
[HyperLogLog++ cardinality estimation functions][hll-functions] use this
algorithm. The HyperLogLog++ algorithm improves on the HyperLogLog algorithm by
using bias correction to reduce error for an important range of cardinalities.

[hll-link-to-hyperloglog-wikipedia]: https://en.wikipedia.org/wiki/HyperLogLog
[hll-link-to-approx-count-distinct]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#approx_count_distinct
[link-to-APPROX_]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#approximate-aggregate-functions
[hll-functions]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#hyperloglog-functions
[HyperLogLogPlusPlus-paper]: https://research.google.com/pubs/pub40671.html
[exact-aggregate-fns]: https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#aggregate-functions

<!-- END CONTENT -->

