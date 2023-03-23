

# Differential privacy 
<a id="differential-privacy"></a>

<!-- BEGIN CONTENT -->

This document provides information about differential privacy for
ZetaSQL. This includes an overview, syntax and examples. For a list
of functions you can use with this syntax, see
[differentially private aggregate functions][anonymization-functions].

## Overview

The goal of [differential privacy][wiki-diff-privacy] is mitigating disclosure
risk: the risk that an attacker can extract sensitive information of individuals
from a dataset. Differential privacy balances this need to safeguard privacy
against the need for statistical accuracy. As privacy increases, statistical
utility decreases, and vice versa.

With ZetaSQL, you can transform the results of a query with
differentially private aggregations. When the query is executed, it:

1.  Computes per-entity aggregations for each group if groups are specified with
    a `GROUP BY` clause. If `kappa` or `max_groups_contributed` is specified, limits
    the number of groups each entity can contribute to.
1.  [Clamps][dp-clamping] each per-entity aggregate contribution to be within
    the clamping bounds. If the clamping bounds are not specified they are
    implicitly calculated in a differentially private way.
1.  Aggregates the clamped per-entity aggregate contributions for each group.
1.  Adds noise to the final aggregate value for each group. The scale of
    random noise is a function of all of the clamped bounds and privacy
    parameters.
1.  Computes a noisy entity count for each group and eliminates groups with
    few entities. A noisy entity count helps eliminate a non-deterministic set
    of groups.

The final result is a dataset where each group has noisy aggregate results
and small groups have been eliminated.

For additional context on what differential privacy is and its use cases, see
the following articles:

+ [A friendly, non-technical introduction to differential privacy][friendly-dp]
+ [Differentially private SQL with bounded user contribution][dp-paper]
+ [Differential privacy on Wikipedia][wiki-diff-privacy]

## Differential privacy syntax 
<a id="dp_query_syntax"></a>

**Anonymization clause**

<pre class="lang-sql notranslate prettyprint">
WITH ANONYMIZATION OPTIONS( privacy_parameters )

privacy_parameters:
  epsilon = expression,
  { delta = expression | k_threshold = expression },
  [ { kappa = expression | max_groups_contributed = expression } ]
</pre>

**Differential privacy clause**

<pre class="lang-sql notranslate prettyprint">
WITH DIFFERENTIAL_PRIVACY OPTIONS( privacy_parameters )

privacy_parameters:
  epsilon = expression,
  delta = expression,
  [ max_groups_contributed = expression ],
  [ privacy_unit_column = column_name ]
</pre>

**Notation rules**

+ Square brackets `[ ]` indicate optional clauses.
+ Curly braces `{ }` with vertical bars `|` represent logical `OR` options.

**Description**

If you want to use this syntax, add it after the `SELECT` keyword with one or
more [differentially private aggregate functions][anonymization-functions] in
the `SELECT` list.

Both the anonymization and differential privacy clause indicate that you are
adding differential privacy to your query. If possible, use
the differential privacy clause as the anonymization clause contains
legacy syntax.

You can use the following privacy parameters to control how the results are
transformed:

+  [`epsilon`][dp-epsilon]: Controls the amount of noise added to the results.
   A higher epsilon means less noise. `1e20` is large enough to add no
   noise. `expression` must be a literal and return a
   `DOUBLE`.
+  [`delta`][dp-delta]: The probability that any row in the result fails to
   be epsilon-differentially private. `expression` must be a literal and return
   a `DOUBLE`.
+  [`k_threshold`][dp-k-threshold]: The number of entities that must
   contribute to a group in order for the group to be exposed in the results.
   `expression` must return an `INT64`.
+  [`kappa` or `max_groups_contributed`][dp-kappa]: A positive integer identifying the limit on
   the number of groups that an entity is allowed to contribute to. This number
   is also used to scale the noise for each group. `expression` must be a
   literal and return an `INT64`.
+ [`privacy_unit_column`][dp-privacy-unit-id]: The column that represents the
  privacy unit column. Replace `column_name` with the path expression for the
  column. The first identifier in the path can start with either a table name
  or a column name that is visible in the `FROM` clause.

Important: Avoid using `kappa` as it is soon to be depreciated. Instead, use
`max_groups_contributed`.

Note: `delta` and `k_threshold` are mutually exclusive; `delta` is preferred
over `k_threshold`.

## Privacy parameters

Privacy parameters control how the results of a query are transformed.
Appropriate values for these settings can depend on many things such
as the characteristics of your data, the exposure level, and the
privacy level.

### `epsilon` 
<a id="dp_epsilon"></a>

Noise is added primarily based on the specified `epsilon`.
The higher the epsilon the less noise is added. More noise corresponding to
smaller epsilons equals more privacy protection.

Noise can be eliminated by setting `epsilon` to `1e20`, which can be
useful during initial data exploration and experimentation with
differential privacy. Extremely large `epsilon` values, such as `1e308`,
cause query failure.

ZetaSQL splits `epsilon` between the differentially private
aggregates in the query. In addition to the explicit
differentially private aggregate functions, the differential privacy process
will also inject an implicit differentially private aggregate into the plan for
removing small groups that computes a noisy entity count per group. If you have
`n` explicit differentially private aggregate functions in your query, then each
aggregate individually gets `epsilon/(n+1)` for its computation. If used with
`kappa` or `max_groups_contributed`, the effective `epsilon` per function per groups is further
split by `kappa` or `max_groups_contributed`. Additionally, if implicit clamping is used for an
aggregate differentially private function, then half of the function's epsilon
is applied towards computing implicit bounds, and half of the function's epsilon
is applied towards the differentially private aggregation itself.

### `delta` 
<a id="dp_delta"></a>

`delta` represents the probability that any row fails to be
`epsilon`-differentially private in the result of a
differentially private query.

If you have to choose between `delta` and `k_threshold`, use `delta`.
When supporting `delta`, the specification of `epsilon/delta` must be evaluated
to determine `k_threshold`, and the specification of `epsilon/k_threshold` must
be evaluated to determine `delta`. This allows a user to specify either
(`epsilon`,`delta`) or (`epsilon`, `k_threshold`) in their
differentially private query.

While doing testing or initial data exploration, it is often useful to set
`delta` to a value where all groups, including small groups, are
preserved. This removes privacy protection and should only be done when it is
not necessary to protect query results, such as when working with non-private
test data. `delta` roughly corresponds to the probability of keeping a small
group.  In order to avoid losing small groups, set `delta` very close to 1,
for example `0.99999`.

### `k_threshold` 
<a id="dp_k_threshold"></a>

Important: `k_threshold` is discouraged. If possible, use `delta` instead.

Tip: We recommend that engines implementing this specification don't allow
entities to specify `k_threshold`.

`k_threshold` computes a noisy entity count for each group and eliminates groups
with few entities from the output. Use this parameter to define how many unique
entities must be included in the group for the value to be included in the
output.

### `kappa` or `max_groups_contributed` 
<a id="dp_kappa"></a>

Important: Avoid using `kappa` as it is soon to be depreciated. Instead, use
`max_groups_contributed`.

`kappa` or `max_groups_contributed` is a positive integer that, if specified, scales the noise and
limits the number of groups that each entity can contribute to.

The default values for `kappa` and `max_groups_contributed` are determined by the
query engine.

If `kappa` or `max_groups_contributed` is unspecified, then there is no limit to the number of
groups that each entity can contribute to.

If `kappa` or `max_groups_contributed` is unspecified, the language can't guarantee that the
results will be differentially private. We recommend `kappa` or `max_groups_contributed` to be
specified. Without `kappa` or `max_groups_contributed` specified, the results may still be
differentially private if certain preconditions are met. For example, if you
know that the privacy unit column in a table or view is unique in the
`FROM` clause, the entity can't contribute to more than one group and therefore
the results will be the same regardless of whether `kappa` or `max_groups_contributed` is set.

Tip: We recommend that engines require `kappa` or `max_groups_contributed` to be set.

### `privacy_unit_column` 
<a id="dp_privacy_unit_id"></a>

To learn about the privacy unit and how to define a privacy unit column, see
[Define a privacy unit column][dp-define-privacy-unit-id].

## Produce a valid differentially private query 
<a id="dp_rules"></a>

The following rules must be met for the differentially private query to be
valid.

### Define a privacy unit column 
<a id="dp_define_privacy_unit_id"></a>

A privacy unit is an entity that we're trying to protect with
differential privacy. Often, this refers to a single individual.
A differentially private query must include one and only one
_privacy unit column_. A privacy unit column is a unique identifier for a
privacy unit. The data type for the privacy unit column must be
[groupable][data-types-groupable].

You can define a privacy unit column in the `OPTIONS` clause of a view or
differential privacy clause with one of the following unique identifiers:

+ In views: `anonymization_userid_column`
+ In the differential privacy clause: `privacy_unit_column`

In the following example, a privacy unit column,
`anonymization_userid_column='id'`, is added to a view. `id` represents a
privacy unit column in a table called `students`.

```sql
CREATE OR REPLACE VIEW view_on_students
OPTIONS(anonymization_userid_column='id')
AS (SELECT * FROM students);
```

In the following examples, a privacy unit column is added to a
differential privacy clause. `id` represents a column that originates from a
table called `students`.

```sql
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (epsilon=1.09, delta=1e5, privacy_unit_column=id)
  item,
  COUNT(*)
FROM students;
```

```sql
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (epsilon=1.09, delta=1e5, privacy_unit_column=members.id)
  item,
  COUNT(*)
FROM (SELECT * FROM students) AS members;
```

The following query is invalid because
[`view_on_students`][dp-example-views] contains a
privacy unit column and so does
[`view_on_professors`][dp-example-views]. To learn more about
`FROM` clause rules, see [Review `FROM` clause rules for differentially private
table expressions][dp-from].

```sql {.bad}
-- This produces an error
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, max_groups_contributed=2)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors, {{USERNAME}}.view_on_students
GROUP BY item;
```

### Review FROM clause rules for differentially private table expressions 
<a id="dp_from"></a>

If a query contains a
differential privacy clause and the
privacy unit column is not defined in that clause, these rules apply:

+ The query must contain at least one table expression or table path in the
  `FROM` clause that has a defined privacy unit column. The
  privacy unit column for a table expression is defined in a
  [view][dp-example-views].
+ If a `FROM` subquery contains a differentially private table expression,
  the subquery must produce a privacy unit column in its output or
  an error is returned.
+ In the `FROM` clause, joins are allowed between tables with and without a
  defined privacy unit column.
+ If the `FROM` clause contains multiple
  differentially private table expressions,
  then all joins between those relations must include the
  privacy unit column name in the join predicate or an error
  is returned.
+ Cross joins are disallowed between two differentially private
  table expressions, since they are not joined on the privacy unit column.

### Only use differentially private aggregate functions 
<a id="dp_aggregate_functions"></a>

If a `SELECT` list contains a differentially private clause, you can only
use [differentially private aggregate functions][anonymization-functions]
in that `SELECT` list.

## Performance implications of differential privacy

Performance of similar differentially private and non-differentially private
queries can't be expected to be equivalent. For example, the performance
profiles of the following two queries are not the same:

```sql
SELECT
  WITH DIFFERENTIAL_PRIVACY OPTIONS(epsilon=1, delta=1e-10, privacy_unit_column=id)
  column_a, COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

```sql
SELECT column_a, COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

The reason for the performance difference is that an additional
finer-granularity level of grouping is performed for
differentially private queries, since per-entity aggregation must also be
performed. The performance profiles of these queries should be similar:

```sql
SELECT
  WITH DIFFERENTIAL_PRIVACY OPTIONS(epsilon=1, delta=1e-10, privacy_unit_column=id)
  column_a, COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

```sql
SELECT column_a, id, COUNT(column_b)
FROM table_a
GROUP BY column_a, id;
```

This implies that if the data being transformed has a high number of
distinct values for the privacy unit column, query
performance can suffer.

## Examples

This section contains examples which illustrate how to work with
differential privacy in ZetaSQL.

### Tables for examples 
<a id="dp_example_tables"></a>

The examples in this section reference these tables:

```sql
CREATE OR REPLACE TABLE professors AS (
  SELECT 101 id, "pencil" item, 24 quantity UNION ALL
  SELECT 123, "pen", 16 UNION ALL
  SELECT 123, "pencil", 10 UNION ALL
  SELECT 123, "pencil", 38 UNION ALL
  SELECT 101, "pen", 19 UNION ALL
  SELECT 101, "pen", 23 UNION ALL
  SELECT 130, "scissors", 8 UNION ALL
  SELECT 150, "pencil", 72);
```

```sql
CREATE OR REPLACE TABLE students AS (
  SELECT 1 id, "pencil" item, 5 quantity UNION ALL
  SELECT 1, "pen", 2 UNION ALL
  SELECT 2, "pen", 1 UNION ALL
  SELECT 3, "pen", 4);
```

### Views for examples 
<a id="dp_example_views"></a>

The examples in this section reference these views:

```sql
CREATE OR REPLACE VIEW view_on_professors
OPTIONS(anonymization_userid_column='id')
AS (SELECT * FROM professors);
```

```sql
CREATE OR REPLACE VIEW view_on_students
OPTIONS(anonymization_userid_column='id')
AS (SELECT * FROM students);
```

These views reference the [professors][dp-example-tables] and
[students][dp-example-tables] example tables.

### Remove noise 
<a id="eliminate_noise"></a>

Removing noise removes privacy protection. Only remove noise for
testing queries on non-private data. When `epsilon` is high, noise is removed
from the results.

```sql
-- This gets the average number of items requested per professor and removes
-- noise from the results
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=2)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 40               |
| pen      | 18.5             |
| scissors | 8                |
+----------+------------------+
```

```sql
-- This gets the average number of items requested per professor and removes
-- noise from the results
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=2, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 40               |
| pen      | 18.5             |
| scissors | 8                |
+----------+------------------+
```

### Add noise

You can add noise to a differentially private query. Smaller groups may not be
included. Smaller epsilons and more noise will provide greater
privacy protection.

```sql
-- This gets the average number of items requested per professor and adds
-- noise to the results
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=2)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- The scissors group was removed this time, but may not be
-- removed the next time.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 38.5038356810269 |
| pen      | 13.4725028762032 |
+----------+------------------+
```

```sql
-- This gets the average number of items requested per professor and adds
-- noise to the results
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=10, delta=.01, max_groups_contributed=2, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

-- These results will change each time you run the query.
-- The scissors group was removed this time, but may not be
-- removed the next time.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 38.5038356810269 |
| pen      | 13.4725028762032 |
+----------+------------------+
```

### Limit the groups in which a privacy unit ID can exist

A privacy unit column can exist within multiple groups. For example, in the
`professors` table, the privacy unit column `123` exists in the `pencil` and
`pen` group. You can set `kappa` or `max_groups_contributed` to different values to limit how many
groups each privacy unit column will be included in.

```sql
SELECT
  WITH ANONYMIZATION
    OPTIONS(epsilon=1e20, delta=.01, max_groups_contributed=1)
    item,
    ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors
GROUP BY item;

-- privacy unit ID 123 was only included in the pen group in this example.
-- Noise was removed from this query for demonstration purposes only.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 40               |
| pen      | 18.5             |
| scissors | 8                |
+----------+------------------+
```

```sql
SELECT
  WITH DIFFERENTIAL_PRIVACY
    OPTIONS(epsilon=1e20, delta=.01, privacy_unit_column=id)
    item,
    AVG(quantity, contribution_bounds_per_group => (0,100)) average_quantity
FROM professors
GROUP BY item;

-- privacy unit column 123 was only included in the pen group in this example.
-- Noise was removed from this query for demonstration purposes only.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 40               |
| pen      | 18.5             |
| scissors | 8                |
+----------+------------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[wiki-diff-privacy]: https://en.wikipedia.org/wiki/Differential_privacy

[friendly-dp]: https://desfontain.es/privacy/friendly-intro-to-differential-privacy.html

[dp-paper]: https://arxiv.org/abs/1909.01917

[dp-example-views]: #dp_example_views

[dp-example-tables]: #dp_example_tables

[dp-k-threshold]: #dp_k_threshold

[dp-epsilon]: #dp_epsilon

[dp-kappa]: #dp_kappa

[dp-delta]: #dp_delta

[dp-privacy-unit-id]: #dp_privacy_unit_id

[dp-define-privacy-unit-id]: #dp_define_privacy_unit_id

[dp-from]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[data-types-groupable]: https://github.com/google/zetasql/blob/master/docs/data-types.md#groupable_data_types

[anonymization-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md

[dp-clamping]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md#dp_clamping

<!-- mdlint on -->

