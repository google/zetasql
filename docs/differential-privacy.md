

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Work with differential privacy 
<a id="differential-privacy"></a>

<!-- BEGIN CONTENT -->

This document provides general information about differential privacy for
ZetaSQL. For syntax, see the [differential privacy clause][dp-clause].
For a list of functions you can use with this syntax, see
[differentially private aggregate functions][dp-functions].

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

## Produce a valid differentially private query 
<a id="dp_rules"></a>

The following rules must be met for the differentially private query to be
valid:

+ A [privacy unit column][dp-define-privacy-unit-id] has been defined.
+ The `SELECT` list contains a [differentially private clause][dp-clause].
+ Only [differentially private aggregate functions][dp-functions] are
  in the `SELECT` list with the differentially private clause.
+ [`FROM` clause rules][dp-from] are used for
  differentially private table expressions.

## Define a privacy unit column 
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
  OPTIONS (epsilon=1.09, delta=1e-5, privacy_unit_column=id)
  item,
  COUNT(*)
FROM students;
```

```sql
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (epsilon=1.09, delta=1e-5, privacy_unit_column=members.id)
  item,
  COUNT(*)
FROM (SELECT * FROM students) AS members;
```

The following query is invalid because
[`view_on_students`][dp-example-views] contains a
privacy unit column and so does
[`view_on_professors`][dp-example-views]. To learn more about
`FROM` clause rules, see [Review `FROM` clause rules for differentially private
table expressions][dp-from-rules].

```sql {.bad}
-- This produces an error
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, max_groups_contributed=2)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM {{USERNAME}}.view_on_professors, {{USERNAME}}.view_on_students
GROUP BY item;
```

## Remove noise from a differentially private query 
<a id="remove_noise"></a>

In the Query syntax reference, see [Remove noise][qs-remove-noise].

## Add noise to a differentially private query 
<a id="add_noise"></a>

In the Query syntax reference, see [Add noise][qs-add-noise].

## Limit the groups in which a privacy unit ID can exist 
<a id="limit_groups"></a>

In the Query syntax reference, see
[Limit the groups in which a privacy unit ID can exist][qs-limit-groups].

## `FROM` clause rules for differentially private table expressions 
<a id="dp_from_rules"></a>

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

## Performance implications of differential privacy

Performance of similar differentially private and non-differentially private
queries can't be expected to be equivalent. For example, the performance
profiles of the following queries are not similar:

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
performed. The performance profiles of the following queries should be similar,
although the differentially private query will be slightly slower:

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

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[wiki-diff-privacy]: https://en.wikipedia.org/wiki/Differential_privacy

[friendly-dp]: https://desfontain.es/privacy/friendly-intro-to-differential-privacy.html

[dp-paper]: https://arxiv.org/abs/1909.01917

[dp-define-privacy-unit-id]: #dp_define_privacy_unit_id

[dp-from-rules]: #dp_from_rules

[dp-example-views]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_example_views

[dp-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#dp_clause

[dp-from]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[qs-add-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#add_noise

[qs-remove-noise]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#eliminate_noise

[qs-limit-groups]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#limit_groups

[data-types-groupable]: https://github.com/google/zetasql/blob/master/docs/data-types.md#groupable_data_types

[dp-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md

[dp-clamping]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md#dp_clamping

<!-- mdlint on -->

