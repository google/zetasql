

# HyperLogLog++ functions

The [HyperLogLog++ algorithm (HLL++)][hll-algorithm] estimates
[cardinality][cardinality] from [sketches][hll-sketches]. If you do not want
to work with sketches and do not need customized precision, consider
using [approximate aggregate functions with system-defined precision][approx-functions-reference].

HLL++ functions are approximate aggregate functions.
Approximate aggregation typically requires less
memory than [exact aggregation functions][aggregate-functions-reference],
like `COUNT(DISTINCT)`, but also introduces statistical uncertainty.
This makes HLL++ functions appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

Note: While `APPROX_COUNT_DISTINCT` is also returning approximate count results,
the functions from this section allow for partial aggregations and
re-aggregations.

ZetaSQL supports the following HLL++ functions:

### HLL_COUNT.INIT
```
HLL_COUNT.INIT(input [, precision])
```

**Description**

An aggregate function that takes one or more `input` values and aggregates them
into a [HLL++][hll-link-to-research-whitepaper] sketch. Each sketch
is represented using the `BYTES` data type. You can then merge sketches using
`HLL_COUNT.MERGE` or `HLL_COUNT.MERGE_PARTIAL`. If no merging is needed,
you can extract the final count of distinct values from the sketch using
`HLL_COUNT.EXTRACT`.

This function supports an optional parameter, `precision`. This parameter
defines the accuracy of the estimate at the cost of additional memory required
to process the sketches or store them on disk. The following table shows the
allowed precision values, the maximum sketch size per group, and confidence
interval (CI) of typical precisions:

|   Precision  | Max. Sketch Size (KiB) | 65% CI | 95% CI | 99% CI |
|--------------|------------------------|--------|--------|--------|
| 10           | 1                      | ±3.25% | ±6.50% | ±9.75% |
| 11           | 2                      | ±2.30% | ±4.60% | ±6.89% |
| 12           | 4                      | ±1.63% | ±3.25% | ±4.88% |
| 13           | 8                      | ±1.15% | ±2.30% | ±3.45% |
| 14           | 16                     | ±0.81% | ±1.63% | ±2.44% |
| 15 (default) | 32                     | ±0.57% | ±1.15% | ±1.72% |
| 16           | 64                     | ±0.41% | ±0.81% | ±1.22% |
| 17           | 128                    | ±0.29% | ±0.57% | ±0.86% |
| 18           | 256                    | ±0.20% | ±0.41% | ±0.61% |
| 19           | 512                    | ±0.14% | ±0.29% | ±0.43% |
| 20           | 1024                   | ±0.10% | ±0.20% | ±0.30% |
| 21           | 2048                   | ±0.07% | ±0.14% | ±0.22% |
| 22           | 4096                   | ±0.05% | ±0.10% | ±0.15% |
| 23           | 8192                   | ±0.04% | ±0.07% | ±0.11% |
| 24           | 16384                  | ±0.03% | ±0.05% | ±0.08% |

If the input is `NULL`, this function returns `NULL`.

For more information, see
[HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm][hll-link-to-research-whitepaper].

**Supported input types**

+ `INT64`
+ `UINT64`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`
+ `BYTES`

**Return type**

`BYTES`

**Example**

```sql
SELECT
  HLL_COUNT.INIT(respondent) AS respondents_hll,
  flavor,
  country
FROM UNNEST([
  STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
  (1, "Chocolate", "CH"),
  (2, "Chocolate", "US"),
  (2, "Strawberry", "US")])
GROUP BY flavor, country;
```

### HLL_COUNT.MERGE
```
HLL_COUNT.MERGE(sketch)
```

**Description**

An aggregate function that returns the cardinality of several
[HLL++][hll-link-to-research-whitepaper] set sketches by computing their union.

Each `sketch` must be initialized on the same type. Attempts to merge sketches
for different types results in an error. For example, you cannot merge a sketch
initialized from `INT64` data with one initialized from `STRING` data.

If the merged sketches were initialized with different precisions, the precision
will be downgraded to the lowest precision involved in the merge.

This function ignores `NULL` values when merging sketches. If the merge happens
over zero rows or only over `NULL` values, the function returns `0`.

**Supported input types**

`BYTES`

**Return type**

`INT64`

**Example**

```sql
SELECT HLL_COUNT.MERGE(respondents_hll) AS num_respondents, flavor
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country)
GROUP BY flavor;
```

### HLL_COUNT.MERGE_PARTIAL
```
HLL_COUNT.MERGE_PARTIAL(sketch)
```

**Description**

An aggregate function that takes one or more
[HLL++][hll-link-to-research-whitepaper] `sketch`
inputs and merges them into a new sketch.

Each `sketch` must be initialized on the same type. Attempts to merge sketches
for different types results in an error. For example, you cannot merge a sketch
initialized from `INT64` data with one initialized from `STRING` data.

If the merged sketches were initialized with different precisions, the precision
will be downgraded to the lowest precision involved in the merge. For example,
if `MERGE_PARTIAL` encounters sketches of precision 14 and 15, the returned new
sketch will have precision 14.

This function returns `NULL` if there is no input or all inputs are `NULL`.

**Supported input types**

`BYTES`

**Return type**

`BYTES`

**Example**

```sql
SELECT HLL_COUNT.MERGE_PARTIAL(respondents_hll) AS num_respondents, flavor
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country)
GROUP BY flavor;
```

### HLL_COUNT.EXTRACT
```
HLL_COUNT.EXTRACT(sketch)
```

**Description**

A scalar function that extracts a cardinality estimate of a single
[HLL++][hll-link-to-research-whitepaper] sketch.

If `sketch` is `NULL`, this function returns a cardinality estimate of `0`.

**Supported input types**

`BYTES`

**Return type**

`INT64`

**Example**

```sql
SELECT
  flavor,
  country,
  HLL_COUNT.EXTRACT(respondents_hll) AS num_respondents
FROM (
  SELECT
    HLL_COUNT.INIT(respondent) AS respondents_hll,
    flavor,
    country
  FROM UNNEST([
    STRUCT(1 AS respondent, "Vanilla" AS flavor, "CH" AS country),
    (1, "Chocolate", "CH"),
    (2, "Chocolate", "US"),
    (2, "Strawberry", "US")])
  GROUP BY flavor, country);

+------------+---------+-----------------+
| flavor     | country | num_respondents |
+------------+---------+-----------------+
| Vanilla    | CH      | 1               |
| Chocolate  | CH      | 1               |
| Chocolate  | US      | 1               |
| Strawberry | US      | 1               |
+------------+---------+-----------------+
```

### About the HLL++ algorithm 
<a id="about_hll_alg"></a>

The [HLL++ algorithm][hll-link-to-research-whitepaper]
improves on the [HLL][hll-link-to-hyperloglog-wikipedia]
algorithm by more accurately estimating very small and large cardinalities.
The HLL++ algorithm includes a 64-bit hash function, sparse
representation to reduce memory requirements for small cardinality estimates,
and empirical bias correction for small cardinality estimates.

### About sketches 
<a id="sketches_hll"></a>

A sketch is a summary of a large data stream. You can extract statistics
from a sketch to estimate particular statistics of the original data, or
merge sketches to summarize multiple data streams. A sketch has these features:

+ It compresses raw data into a fixed-memory representation.
+ It's asymptotically smaller than the input.
+ It's the serialized form of an in-memory, sublinear data structure.
+ It typically requires less memory than the input used to create it.

Sketches allow integration with other systems. For example, it is possible to
build sketches in external applications, like [Cloud Dataflow][dataflow], or
[Apache Spark][spark] and consume them in ZetaSQL or
vice versa. Sketches also allow building intermediate aggregations for
non-additive functions like `COUNT(DISTINCT)`.

[spark]: https://spark.apache.org
[dataflow]: https://cloud.google.com/dataflow

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[cardinality]: https://en.wikipedia.org/wiki/Cardinality

[hll-link-to-hyperloglog-wikipedia]: https://en.wikipedia.org/wiki/HyperLogLog

[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html

[hll-link-to-approx-count-distinct]: #approx_count_distinct

[hll-sketches]: #sketches_hll

[hll-algorithm]: #about_hll_alg

[approx-functions-reference]: https://github.com/google/zetasql/blob/master/docs/approximate_aggregate_functions.md

[aggregate-functions-reference]: https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md

<!-- mdlint on -->

