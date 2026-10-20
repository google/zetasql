

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Conditional expressions

GoogleSQL supports conditional expressions.

### Evaluation order and short-circuiting 
<a id="short_circuiting"></a>

In contrast to regular functions, where all inputs are evaluated before calling
the function, conditional expressions impose constraints on the evaluation
semantics of their inputs:

*   Conditional expressions behave as if input expressions are evaluated from
    left to right and only up to the chosen output expression. Evaluation errors
    in unchosen expressions are ignored. If an expression that must be evaluated
    to determine the result produces an evaluation error, the query fails with
    that error. You can use short-circuiting semantics for error handling, such
    as avoiding division-by-zero errors. For more information, see
    [short-circuiting][short-circuit-eval].
*   If the final result and error-handling behavior match short-circuiting
    semantics, input expressions might be evaluated in parallel or before the
    conditional expression is evaluated. These input expressions include scalar
    expressions and scalar subqueries in unchosen branches. Because all input
    expressions might still execute, don't rely solely on conditional
    expressions for performance tuning. To
    conditionally execute subqueries, use procedural control flow statements
    (such as `IF ... THEN` statements in procedural SQL) rather than conditional
    expressions. For more information, see
    [lazy physical execution][lazy-eval].

### Expression list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#case_expr"><code>CASE expr</code></a>
</td>
  <td>
    Compares the given expression to each successive <code>WHEN</code> clause
    and produces the first result where the values are equal.
  </td>
</tr>

<tr>
  <td><a href="#case"><code>CASE</code></a>
</td>
  <td>
    Evaluates the condition of each successive <code>WHEN</code> clause and
    produces the first result where the condition evaluates to
    <code>TRUE</code>.
  </td>
</tr>

<tr>
  <td><a href="#coalesce"><code>COALESCE</code></a>
</td>
  <td>
    Produces the value of the first non-<code>NULL</code> expression, if any,
    otherwise <code>NULL</code>.
  </td>
</tr>

<tr>
  <td><a href="#if"><code>IF</code></a>
</td>
  <td>
    If an expression evaluates to <code>TRUE</code>, produces a specified
    result, otherwise produces the evaluation for an <i>else result</i>.
  </td>
</tr>

<tr>
  <td><a href="#ifnull"><code>IFNULL</code></a>
</td>
  <td>
    If an expression evaluates to <code>NULL</code>, produces a specified
    result, otherwise produces the expression.
  </td>
</tr>

<tr>
  <td><a href="#nullif"><code>NULLIF</code></a>
</td>
  <td>
    Produces <code>NULL</code> if the first expression that matches another
    evaluates to <code>TRUE</code>, otherwise returns the first expression.
  </td>
</tr>

<tr>
  <td><a href="#nullifzero"><code>NULLIFZERO</code></a>
</td>
  <td>
    Produces <code>NULL</code> if an expression is <code>0</code>,
    otherwise produces the expression.
  </td>
</tr>

<tr>
  <td><a href="#zeroifnull"><code>ZEROIFNULL</code></a>
</td>
  <td>
    Produces <code>0</code> if an expression is <code>NULL</code>, otherwise
    produces the expression.
  </td>
</tr>

  </tbody>
</table>

### `CASE expr` 
<a id="case_expr"></a>

```googlesql
CASE expr
  WHEN expr_to_match THEN result
  [ ... ]
  [ ELSE else_result ]
  END
```

**Description**

Compares `expr` to `expr_to_match` of each successive `WHEN` clause and returns
the first result where this comparison evaluates to `TRUE`. The expression
behaves as if the remaining `WHEN` clauses and `else_result` aren't evaluated.
For more information, see
[Evaluation order and short-circuiting][case-expr-short-circuiting].

If the `expr = expr_to_match` comparison evaluates to `FALSE` or `NULL` for all
`WHEN` clauses, returns the evaluation of `else_result` if present; if
`else_result` isn't present, then returns `NULL`.

Consistent with [equality comparisons][logical-operators] elsewhere, if both
`expr` and `expr_to_match` are `NULL`, then `expr = expr_to_match` evaluates to
`NULL`, which returns `else_result`. If a CASE statement needs to distinguish a
`NULL` value, then the alternate [CASE][case] syntax should be used.

`expr` and `expr_to_match` can be any type. They must be implicitly
coercible to a common [supertype][cond-exp-supertype]; equality comparisons are
done on coerced values. There may be multiple `result` types. `result` and
`else_result` expressions must be coercible to a common supertype.

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/googlesql/blob/master/docs/collation-concepts.md

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```googlesql
WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 8 UNION ALL
  SELECT 60, 6 UNION ALL
  SELECT 50, 10
)
SELECT
  A,
  B,
  CASE A
    WHEN 90 THEN 'red'
    WHEN 50 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------+
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 8  | blue   |
 | 60 | 6  | green  |
 | 50 | 10 | blue   |
 +------------------*/
```

[case-expr-short-circuiting]: #short_circuiting

[logical-operators]: https://github.com/google/googlesql/blob/master/docs/operators.md#logical_operators

[case]: #case

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

### `CASE` 
<a id="case"></a>

```googlesql
CASE
  WHEN condition THEN result
  [ ... ]
  [ ELSE else_result ]
  END
```

**Description**

Evaluates the condition of each successive `WHEN` clause and returns the
first result where the condition evaluates to `TRUE`. The expression behaves
as if any remaining `WHEN` clauses and `else_result` aren't evaluated. For
more information, see
[Evaluation order and short-circuiting][case-short-circuiting].

If all conditions evaluate to `FALSE` or `NULL`, returns evaluation of
`else_result` if present; if `else_result` isn't present, then returns `NULL`.

For additional rules on how values are evaluated, see the
three-valued logic table in [Logical operators][logical-operators].

`condition` must be a boolean expression. There may be multiple `result` types.
`result` and `else_result` expressions must be implicitly coercible to a common
[supertype][cond-exp-supertype].

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/googlesql/blob/master/docs/collation-concepts.md

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```googlesql
WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 6 UNION ALL
  SELECT 20, 10
)
SELECT
  A,
  B,
  CASE
    WHEN A > 60 THEN 'red'
    WHEN B = 6 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------+
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 6  | blue   |
 | 20 | 10 | green  |
 +------------------*/
```

[case-short-circuiting]: #short_circuiting

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

[logical-operators]: https://github.com/google/googlesql/blob/master/docs/operators.md#logical_operators

### `COALESCE` 
<a id="coalesce"></a>

```googlesql
COALESCE(expr[, ...])
```

**Description**

Returns the value of the first non-`NULL` expression, if any, otherwise
`NULL`.

The expression behaves as if the remaining expressions aren't evaluated:
evaluation errors in an expression are ignored if any preceding expression
evaluates to a non-`NULL` value, even though all input expressions might still
be evaluated. If no preceding expression evaluates to a non-`NULL` value and an
expression produces an evaluation error, the query fails with that error. For
more information, see
[Evaluation order and short-circuiting][coalesce-short-circuiting].

An input expression can be any type. There can be multiple input expression
types. All input expressions must be implicitly coercible to a common
[supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr`[, ...].

**Examples**

```googlesql
SELECT COALESCE('A', 'B', 'C') as result

/*--------+
 | result |
 +--------+
 | A      |
 +--------*/
```

```googlesql
SELECT COALESCE(NULL, 'B', 'C') as result

/*--------+
 | result |
 +--------+
 | B      |
 +--------*/
```

```googlesql
-- The first expression is non-NULL, so it's used. The division-by-zero error
-- from the second expression isn't produced.
SELECT COALESCE(10, 1 / 0) as result

/*--------+
 | result |
 +--------+
 | 10     |
 +--------*/
```

In the following example, `COALESCE` includes scalar subqueries as fallback
arguments. Because conditional expressions don't guarantee lazy execution, all
three subqueries might be evaluated even if the first subquery returns a
non-`NULL` value:

```googlesql
SELECT COALESCE(
  (SELECT event_date FROM recent_events LIMIT 1),
  (SELECT MAX(event_date) FROM weekly_events),
  (SELECT MAX(event_date) FROM all_events)
)
```

[coalesce-short-circuiting]: #short_circuiting

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

### `IF` 
<a id="if"></a>

```googlesql
IF(expr, true_result, else_result)
```

**Description**

If `expr` evaluates to `TRUE`, returns `true_result`, else returns the
evaluation for `else_result`. The expression behaves as if `else_result` isn't
evaluated when `expr` evaluates to `TRUE`, and as if `true_result` isn't
evaluated when `expr` evaluates to `FALSE` or `NULL`. For more information,
see [Evaluation order and short-circuiting][if-short-circuiting].

`expr` must be a boolean expression. `true_result` and `else_result`
must be coercible to a common [supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `true_result` and `else_result`.

**Examples**

```googlesql
SELECT
  10 AS A,
  20 AS B,
  IF(10 < 20, 'true', 'false') AS result

/*------------------+
 | A  | B  | result |
 +------------------+
 | 10 | 20 | true   |
 +------------------*/
```

```googlesql
SELECT
  30 AS A,
  20 AS B,
  IF(30 < 20, 'true', 'false') AS result

/*------------------+
 | A  | B  | result |
 +------------------+
 | 30 | 20 | false  |
 +------------------*/
```

[if-short-circuiting]: #short_circuiting

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

### `IFNULL` 
<a id="ifnull"></a>

```googlesql
IFNULL(expr, null_result)
```

**Description**

If `expr` evaluates to `NULL`, returns `null_result`. Otherwise, returns
`expr`.

If `expr` is non-`NULL`, the expression behaves as if `null_result` isn't
evaluated: evaluation errors in `null_result` are ignored, even though both
input expressions might still be evaluated. If `expr` evaluates to `NULL` and
`null_result` produces an evaluation error, the query fails with that error.
For more information, see
[Evaluation order and short-circuiting][ifnull-short-circuiting].

`expr` and `null_result` can be any type and must be implicitly coercible to
a common [supertype][cond-exp-supertype]. Synonym for
`COALESCE(expr, null_result)`.

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` or `null_result`.

**Examples**

```googlesql
SELECT IFNULL(NULL, 0) as result

/*--------+
 | result |
 +--------+
 | 0      |
 +--------*/
```

```googlesql
SELECT IFNULL(10, 0) as result

/*--------+
 | result |
 +--------+
 | 10     |
 +--------*/
```

```googlesql
-- The first expression is non-NULL, so it's used. The division-by-zero error
-- from the second expression isn't produced.
SELECT IFNULL(10, 1 / 0) as result

/*--------+
 | result |
 +--------+
 | 10     |
 +--------*/
```

[ifnull-short-circuiting]: #short_circuiting

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

### `NULLIF` 
<a id="nullif"></a>

```googlesql
NULLIF(expr, expr_to_match)
```

**Description**

Returns `NULL` if `expr = expr_to_match` evaluates to `TRUE`, otherwise
returns `expr`.

`expr` and `expr_to_match` must be implicitly coercible to a
common [supertype][cond-exp-supertype], and must be comparable.

This expression supports specifying [collation][collation].

[collation]: https://github.com/google/googlesql/blob/master/docs/collation-concepts.md

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` and `expr_to_match`.

**Example**

```googlesql
SELECT NULLIF(0, 0) as result

/*--------+
 | result |
 +--------+
 | NULL   |
 +--------*/
```

```googlesql
SELECT NULLIF(10, 0) as result

/*--------+
 | result |
 +--------+
 | 10     |
 +--------*/
```

[cond-exp-supertype]: https://github.com/google/googlesql/blob/master/docs/conversion_rules.md#supertypes

### `NULLIFZERO` 
<a id="nullifzero"></a>

```googlesql
NULLIFZERO(expr)
```

**Description**

Returns `NULL` if the value of `expr` is `0`. Otherwise, returns `expr`. `expr`
must be a zeroable type.

**Return Data Type**

Type of `expr`.

**Example**

```googlesql
SELECT NULLIFZERO(0) AS result

/*--------+
 | result |
 +--------+
 | NULL   |
 +--------*/
```

### `ZEROIFNULL` 
<a id="zeroifnull"></a>

```googlesql
ZEROIFNULL(expr)
```

**Description**

Returns `0` if the value of `expr` is `NULL`. Otherwise, returns `expr`. `expr`
must be a zeroable type.

**Return Data Type**

Type of `expr`.

**Example**

```googlesql
SELECT ZEROIFNULL(NULL) AS result

/*--------+
 | result |
 +--------+
 | 0      |
 +--------*/
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[short-circuit-eval]: https://en.wikipedia.org/wiki/Short-circuit_evaluation

[lazy-eval]: https://en.wikipedia.org/wiki/Lazy_evaluation

<!-- mdlint on -->

