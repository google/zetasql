

# Anonymization and Differential Privacy 
<a id="anonymization_syntax"></a>

<!-- BEGIN CONTENT -->

Anonymization is the process of transforming user data into anonymous
information. This is done in such a way that it is not reasonably likely that
anyone with access to the data can identify or re-identify an individual user
from the anonymized data.

The anonymization definition supported by ZetaSQL is
[differential privacy][wiki-diff-privacy]. The goal of differential privacy
is mitigating disclosure risk: the risk that an attacker can extract sensitive
information of individuals from a dataset. Differential privacy balances
this need to safeguard privacy against the need for statistical accuracy.
As privacy increases, statistical utility decreases, and vice versa.

With ZetaSQL, you can anonymize the results of a query with
differentially private aggregations. When the query is executed, it:

1.  Computes per-user aggregations for each group if groups are specified with
    a `GROUP BY` clause. If `kappa` is specified, limits the
    number of groups each user can contribute to.
1.  [Clamps][anon-clamping] each per-user aggregate contribution to be within
    the clamping bounds. If the clamping bounds are not specified they are
    implicitly calculated in a differentially private way.
1.  Aggregates the clamped per-user aggregate contributions for each group.
1.  Adds noise to the final aggregate value for each group. The scale of
    random noise is a function of all of the clamped bounds and privacy
    parameters.
1.  Computes a noisy user count for each group and eliminates groups with
    few users. A noisy user count helps eliminate a non-deterministic set
    of groups.

The final result is a dataset where each group has noisy aggregate results
and small groups have been eliminated.

## Anonymization clause syntax 
<a id="anon_query_syntax"></a>

<pre>
WITH ANONYMIZATION OPTIONS( privacy_parameters )

privacy_parameters:
  epsilon = expression,
  { delta = expression | k_threshold = expression },
  [ kappa = expression ]
</pre>

**Description**

This clause indicates that you want to anonymize the results of a query with
differentially private aggregations. If you want to use this clause, add it to
the `SELECT` list with one or more
[anonymization aggregate functions][anonymization-functions].

Optionally, you can include privacy parameters to control how the results are
anonymized.

+  [`epsilon`][anon-epsilon]: Controls the amount of noise added to the results.
   A higher epsilon means less noise. `1e20` is usually large enough to add no
   noise. `expression` must be constant and return a
   `DOUBLE`.
+  [`delta`][anon-delta]: The probability the any row in the result fails to
   be epsilon-differentially private. `expression` must return a
   `DOUBLE`.
+  [`k_threshold`][anon-k-threshold]: The number of users that must contribute
   to a group in order for the group to be exposed in the results.
   `expression` must return an `INT64`.
+  [`kappa`][anon-kappa]: A positive integer identifying the limit on the
   number of groups that a user is allowed to contribute to. This number is
   also used to scale the noise for each group. `expression` must return an
   `INT64`.

Note: `delta` and `k_threshold` are mutually exclusive; `delta` is preferred
over `k_threshold`.

## Privacy parameters

Privacy parameters control how the results of a query are anonymized.
Appropriate values for these settings can depend on many things such
as the characteristics of your data, the exposure level, and the
privacy level.

### epsilon 
<a id="anon_epsilon"></a>

Noise is added primarily based on the specified `epsilon`.
The higher the epsilon the less noise is added. More noise corresponding to
smaller epsilons equals more privacy protection.

Noise can usually be eliminated by setting `epsilon` to `1e20`, which can be
useful during initial data exploration and experimentation with anonymization.
Unusually large `epsilon` values, such as `1e308`, cause query
failure. Start large, and reduce the `epsilon` until the query succeeds, but not
so much that it returns noisy results.

ZetaSQL splits `epsilon` between the anonymization aggregates in the
query. In addition to the explicit anonymization aggregate functions, the
anonymization process will also inject an implicit anonymized aggregate into the
plan for removing small groups that computes a noisy user count per group. If
you have `n` explicit anonymization aggregate functions in your query, then each
aggregate individually gets `epsilon/(n+1)` for its computation. If used with
`kappa`, the effective `epsilon` per function per groups is further split by
`kappa`. Additionally, if implicit clamping is used for an aggregate
anonymization function, then half of the function's epsilon is applied towards
computing implicit bounds, and half of the function's epsilon is applied towards
the anonymized aggregation itself.

### delta 
<a id="anon_delta"></a>

`delta` represents the probability that any row fails to be
`epsilon`-differentially private in the result of an anonymized query. If you
have to choose between `delta` and `k_threshold`, use `delta`.

When supporting `delta`, the specification of `epsilon/delta` must be evaluated
to determine `k_threshold`, and the specification of `epsilon/k_threshold` must
be evaluated to determine `delta`. This allows a user to specify either
(`epsilon`,`delta`) or (`epsilon`, `k_threshold`) in their anonymization query.

While doing testing or initial data exploration, it is often useful to set
`delta` to a value where all groups, including small groups, are
preserved. This removes privacy protection and should only be done when it is
not necessary to protect query results, such as when working with non-private
test data. `delta` roughly corresponds to the probability of keeping a small
group.  In order to avoid losing small groups, set `delta` very close to 1,
for example `0.99999`.

### k_threshold 
<a id="anon_k_threshold"></a>

Important: `k_threshold` is discouraged. If possible, use `delta` instead.

Tip: We recommend that engines implementing this specification do not allow
users to specify `k_threshold`.

`k_threshold` computes a noisy user count for each group and eliminates groups
with few users from the output. Use this parameter to define how many unique
users must be included in the group for the value to be included in the output.

### kappa 
<a id="anon_kappa"></a>

`kappa` is a positive integer that, if specified, scales the noise and
limits the number of groups that each user can contribute to. If `kappa` is
unspecified, then there is no limit to the number of groups that each user
can contribute to.

If `kappa` is unset, the language cannot guarantee that the results will be
differentially private. We recommend kappa to be set. Without `kappa` the
results may still be differentially private if certain preconditions are met.
For example, if you know that the anonymization ID column in a table or view is
unique in the `FROM` clause, the user cannot contribute to more than one group
and therefore the results will be the same regardless of whether `kappa` is set.

Tip: We recommend that engines require kappa to be set.

## Rules for producing a valid query

The following rules must be met for the anonymized query to be valid.

###  Anonymization-enabled table expressions 
<a id="anon_expression"></a>

An anonymization-enabled table expression is a table expression that
produces a column that has been identified as an anonymization ID. If a query
contains an anonymization clause, it must also contain at least one
anonymization-enabled table expression in the `FROM` clause.

### FROM clause rules 
<a id="anon_from"></a>

The `FROM` clause must have at least one `from_item` that represents an
[anonymization-enabled table expression][anon-expression]. Not all
table expressions in the `FROM` clause need to be
anonymization-enabled table expressions.

If a `FROM` subquery contains an anonymization-enabled table expression,
the subquery must produce an anonymization ID column in its output or
an error is returned.

If the `FROM` clause contains multiple anonymization-enabled table expressions,
then all joins between those relations must include the anonymization ID column
name in the join predicate or an error is returned. Cross joins are disallowed
between two anonymization-enabled table expressions, since they are not joined
on the anonymization ID column.

### Aggregate function rules 
<a id="anon_aggregate_functions"></a>

An anonymization query cannot contain non-anonymized aggregate functions.
Only [anonymization aggregate functions][anonymization-functions] can be used.

## Performance implications of anonymization

Performance of similar anonymized and non-anonymized queries
cannot be expected to be equivalent. For example, the performance profiles
of the following two queries are not the same:

```sql
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1, delta=1e-10, kappa=1)
  column_a, ANON_COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

```sql
SELECT column_a, COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

The reason for the performance difference is that an additional
finer-granularity level of grouping is performed for anonymized queries,
since per-user aggregation must also be performed. The performance profiles
of these queries should be similar:

```sql
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1, delta=1e-10, kappa=1)
  column_a, ANON_COUNT(column_b)
FROM table_a
GROUP BY column_a;
```

```sql
SELECT column_a, id, COUNT(column_b)
FROM table_a
GROUP BY column_a, id;
```

This implies that if the data being anonymized has a high number of
distinct values for the anonymization ID column, anonymized query performance
can suffer.

## Examples

### Tables and views for examples 
<a id="anon_example_views"></a>

The examples in this section reference these table and views:

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

CREATE OR REPLACE VIEW view_on_professors
OPTIONS(anonymization_userid_column='id')
AS (SELECT * FROM professors);
```

```sql
CREATE OR REPLACE TABLE students AS (
  SELECT 1 id, "pencil" item, 5 quantity UNION ALL
  SELECT 1, "pen", 2 UNION ALL
  SELECT 2, "pen", 1 UNION ALL
  SELECT 3, "pen", 4);

CREATE OR REPLACE VIEW view_on_students
OPTIONS(anonymization_userid_column='id')
AS (SELECT * FROM students);
```

### Remove noise 
<a id="eliminate_noise"></a>

Removing noise removes privacy protection. Only remove noise for
testing queries on non-private data.

The following anonymized query gets the average number of items requested
per professor. For details on how the averages were computed, see
[ANON_AVG][anon-avg]. Because `epsilon` is high, noise is removed from the
results.

```sql
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=2)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
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

In this example, noise has been added to the anonymized query.
Smaller groups may not be included. Smaller epsilons and more noise will
provide greater privacy protection.

```sql
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
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

### Limit the groups in which an anonymization ID can exist

An anonymization ID can exist within multiple groups. For example, in the
`professors` table, the anonymization ID `123` exists in the `pencil` and `pen`
group. If you only want `123` to be used in the first group found, you can use a
query that looks like this:

```sql
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=2)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

-- Anonymization ID 123 was not included in the pencil group.
-- Noise was removed from this query for demonstration purposes only.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 72               |
| pen      | 18.5             |
| scissors | 8                |
+----------+------------------+
```

### Invalid query with two anonymization ID columns

The following query is invalid because `view_on_students` contains an
anonymization ID column and so does `view_on_professors`.
When the `FROM` clause contains multiple
anonymization-enabled table expressions, then those tables must be joined on
the anonymization ID column or an error is returned.

```sql {.bad}
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=2)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors, view_on_students
GROUP BY item;
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[anon-expression]: #anon_expression

[anon-resources]: #anon_resources

[anon-query]: #anon_query

[anon-k-threshold]: #anon_k_threshold

[anon-epsilon]: #anon_epsilon

[anon-kappa]: #anon_kappa

[anon-delta]: #anon_delta

[anon-from]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[anon-select-list]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_list

[anon-group-by]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[wiki-diff-privacy]: https://en.wikipedia.org/wiki/Differential_privacy

[anonymization-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate_anonymization_functions.md

[anon-clamping]: https://github.com/google/zetasql/blob/master/docs/aggregate_anonymization_functions.md#anon_clamping

[anon-exp-clamping]: https://github.com/google/zetasql/blob/master/docs/aggregate_anonymization_functions.md#anon_explicit_clamping

[anon-imp-clamping]: https://github.com/google/zetasql/blob/master/docs/aggregate_anonymization_functions.md#anon_implicit_clamping

[anon-avg]: https://github.com/google/zetasql/blob/master/docs/aggregate_anonymization_functions.md#anon_avg

<!-- mdlint on -->

