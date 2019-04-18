

# HyperLogLog++ Functions

ZetaSQL supports the following approximate aggregate functions using
the HyperLogLog++ algorithm. For an explanation of how approximate aggregate
functions work, see [Approximate Aggregation][approximate-aggregation-concept].

### HLL_COUNT.INIT
```
HLL_COUNT.INIT(input [, precision])
```

**Description**

A scalar function that takes one or more `input` values and aggregates them into
a [HyperLogLog++][hll-link-to-hyperloglog-wikipedia] sketch. Each sketch
is represented using the `BYTES` data type. You can then merge sketches using
`HLL_COUNT.MERGE` or `HLL_COUNT.MERGE_PARTIAL`. If no merging is needed,
you can extract the final count of distinct values from the sketch using
`HLL_COUNT.EXTRACT`.

An `input` can be one of the following:

<ul>
<li>INT64</li>
<li>UINT64</li>
<li>STRING</li>
<li>BYTES</li>
</ul>

This function supports an optional parameter, `precision`. This parameter
defines the accuracy of the estimate at the cost of additional memory required
to process the sketches or store them on disk. The following table shows the
allowed precision values, the maximum sketch size per group, and confidence
interval (CI) of typical precisions:

|   Precision  | Max. Sketch Size (KiB) | 65% CI | 95% CI | 99% CI |
|--------------|------------------------|--------|--------|--------|
| 10           | 1                      | ±1.63% | ±3.25% | ±6.50% |
| 11           | 2                      | ±1.15% | ±2.30% | ±4.60% |
| 12           | 4                      | ±0.81% | ±1.63% | ±3.25% |
| 13           | 8                      | ±0.57% | ±1.15% | ±1.72% |
| 14           | 16                     | ±0.41% | ±0.81% | ±1.22% |
| 15 (default) | 32                     | ±0.29% | ±0.57% | ±0.86% |
| 16           | 64                     | ±0.20% | ±0.41% | ±0.61% |
| 17           | 128                    | ±0.14% | ±0.29% | ±0.43% |
| 18           | 256                    | ±0.10% | ±0.20% | ±0.41% |
| 19           | 512                    | ±0.07% | ±0.14% | ±0.29% |
| 20           | 1024                   | ±0.05% | ±0.10% | ±0.20% |
| 21           | 2048                   | ±0.04% | ±0.07% | ±0.14% |
| 22           | 4096                   | ±0.03% | ±0.05% | ±0.10% |
| 23           | 8192                   | ±0.02% | ±0.04% | ±0.07% |
| 24           | 16384                  | ±0.01% | ±0.03% | ±0.05% |

If the input is NULL, this function returns NULL.

For more information, see
[HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm][hll-link-to-research-whitepaper].

**Supported input type**

BYTES

**Return type**

BYTES

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
[HyperLogLog++][hll-link-to-research-whitepaper] set sketches by computing their union.

Each `sketch` must have the same precision and be initialized on the same type.
Attempts to merge sketches with different precisions or for different types
results in an error. For example, you cannot merge a sketch initialized
from INT64 data with one initialized from STRING data.

This function ignores NULL values when merging sketches. If the merge happens
over zero rows or only over NULL values, the function returns `0`.

**Supported input types**

BYTES

**Return type**

INT64

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
[HyperLogLog++][hll-link-to-research-whitepaper] `sketch`
inputs and merges them into a new sketch.

This function returns NULL if there is no input or all inputs are NULL.

**Supported input types**

BYTES

**Return type**

BYTES

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

A scalar function that extracts an cardinality estimate of a single
[HyperLogLog++][hll-link-to-research-whitepaper] sketch.

If `sketch` is NULL, this function returns a cardinality estimate of `0`.

**Supported input types**

BYTES

**Return type**

INT64

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

[hll-link-to-hyperloglog-wikipedia]: https://en.wikipedia.org/wiki/HyperLogLog
[hll-link-to-approx-count-distinct]: #approx_count_distinct
[hll-link-to-research-whitepaper]: https://research.google.com/pubs/pub40671.html
[approximate-aggregation-concept]: https://github.com/google/zetasql/blob/master/docs/approximate-aggregation.md

