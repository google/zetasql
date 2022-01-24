

# Expressions, functions, and operators

This page explains ZetaSQL expressions, including functions and
operators.

## Function call rules

The following rules apply to all functions unless explicitly indicated otherwise
in the function description:

+ Integer types coerce to INT64.
+ For functions that accept numeric types, if one operand is a floating point
  operand and the other operand is another numeric type, both operands are
  converted to DOUBLE before the function is
  evaluated.
+ If an operand is `NULL`, the result is `NULL`, with the exception of the
  IS operator.
+ For functions that are time zone sensitive (as indicated in the function
  description), the default time zone, which is implementation defined, is used if a time
  zone is not specified.

## Lambdas 
<a id="lambdas"></a>

**Syntax:**

```sql
(arg[, ...]) -> body_expression
```

```sql
arg -> body_expression
```

**Description**

For some functions, ZetaSQL supports lambdas as builtin function
arguments. A lambda takes a list of arguments and an expression as the lambda
body.

+   `arg`:
    +   Name of the lambda argument is defined by the user.
    +   No type is specified for the lambda argument. The type is inferred from
        the context.
+   `body_expression`:
    +   The lambda body can be any valid scalar expression.

## SAFE. prefix

**Syntax:**

```
SAFE.function_name()
```

**Description**

If you begin a function with the `SAFE.` prefix, it will return `NULL` instead
of an error. The `SAFE.` prefix only prevents errors from the prefixed function
itself: it does not prevent errors that occur while evaluating argument
expressions. The `SAFE.` prefix only prevents errors that occur because of the
value of the function inputs, such as "value out of range" errors; other
errors, such as internal or system errors, may still occur. If the function
does not return an error, `SAFE.` has no effect on the output.

[Operators][link-to-operators], such as `+` and `=`, do not support the `SAFE.`
prefix. To prevent errors from a division
operation, use [SAFE_DIVIDE][link-to-SAFE_DIVIDE]. Some operators,
such as `IN`, `ARRAY`, and `UNNEST`, resemble functions, but do not support the
`SAFE.` prefix. The `CAST` and `EXTRACT` functions also do not support the
`SAFE.` prefix. To prevent errors from casting, use
[SAFE_CAST][link-to-SAFE_CAST].

**Example**

In the following example, the first use of the `SUBSTR` function would normally
return an error, because the function does not support length arguments with
negative values. However, the `SAFE.` prefix causes the function to return
`NULL` instead. The second use of the `SUBSTR` function provides the expected
output: the `SAFE.` prefix has no effect.

```sql
SELECT SAFE.SUBSTR('foo', 0, -2) AS safe_output UNION ALL
SELECT SAFE.SUBSTR('bar', 0, 2) AS safe_output;

+-------------+
| safe_output |
+-------------+
| NULL        |
| ba          |
+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[link-to-operators]: #operators

[link-to-SAFE_DIVIDE]: #safe_divide

[link-to-SAFE_CAST]: #safe_casting

<!-- mdlint on -->

## Conversion rules

Conversion includes, but is not limited to, casting, coercion, and
supertyping.

+ Casting is explicit conversion and uses the
  [`CAST()`][con-rules-link-to-cast] function.
+ Coercion is implicit conversion, which ZetaSQL performs
  automatically under the conditions described below.
+ A supertype is a common type to which two or more expressions can be coerced.

There are also conversions that have their own function names, such as
`PARSE_DATE()`. To learn more about these functions, see
[Conversion functions][con-rules-link-to-conversion-functions-other]

### Comparison of casting and coercion 
<a id="comparison_chart"></a>

The following table summarizes all possible cast and coercion possibilities for
ZetaSQL data types. The _Coerce to_ column applies to all
expressions of a given data type, (for example, a
column), but
literals and parameters can also be coerced. See
[literal coercion][con-rules-link-to-literal-coercion] and
[parameter coercion][con-rules-link-to-parameter-coercion] for details.

<table>
<thead>
<tr>
<th>From type</th>
<th>Cast to</th>
<th>Coerce to</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT32</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>INT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>INT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>UINT32</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>INT64</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>UINT64</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>ENUM</span><br /></td>
<td><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>NUMERIC</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>BIGNUMERIC</span><br /><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>BIGNUMERIC</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>FLOAT</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td><span>DOUBLE</span><br /></td>
</tr>

<tr>
<td>DOUBLE</td>
<td><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BOOL</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>STRING</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>STRING</td>
<td><span>BOOL</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>NUMERIC</span><br /><span>BIGNUMERIC</span><br /><span>FLOAT</span><br /><span>DOUBLE</span><br /><span>STRING</span><br /><span>BYTES</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /><span>ENUM</span><br /><span>PROTO</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>BYTES</td>
<td><span>STRING</span><br /><span>BYTES</span><br /><span>PROTO</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>DATE</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIMESTAMP</span><br /></td>
<td><span>DATETIME</span><br /></td>
</tr>

<tr>
<td>DATETIME</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIME</td>
<td><span>STRING</span><br /><span>TIME</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td><span>STRING</span><br /><span>DATE</span><br /><span>DATETIME</span><br /><span>TIME</span><br /><span>TIMESTAMP</span><br /></td>
<td>&nbsp;</td>
</tr>

<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>ENUM</td>
<td><span>
ENUM
(with the same ENUM name)
</span><br /><span>INT32</span><br /><span>INT64</span><br /><span>UINT32</span><br /><span>UINT64</span><br /><span>STRING</span><br /></td>
<td>ENUM
(with the same ENUM name)</td>
</tr>

<tr>
<td>STRUCT</td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>

<tr>
<td>PROTO</td>
<td><span>
PROTO
(with the same PROTO name)
</span><br /><span>STRING</span><br /><span>BYTES</span><br /></td>
<td>PROTO
(with the same PROTO name)</td>
</tr>

</tbody>
</table>

### Casting

Most data types can be cast from one type to another with the `CAST` function.
When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. If you want to protect your queries from these types of errors, you
can use `SAFE_CAST`. To learn more about the rules for `CAST`, `SAFE_CAST` and
other casting functions, see
[Conversion functions][con-rules-link-to-conversion-functions].

### Coercion

ZetaSQL coerces the result type of an argument expression to another
type if needed to match function signatures. For example, if function `func()`
is defined to take a single argument of type `DOUBLE`
and an expression is used as an argument that has a result type of
`INT64`, then the result of the expression will be
coerced to `DOUBLE` type before `func()` is computed.

#### Literal coercion

ZetaSQL supports the following literal coercions:

<table>
<thead>
<tr>
<th>Input data type</th>
<th>Result data type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>

<tr>
<td>Integer literal</td>
<td><span> INT32</span><br /><span> UINT32</span><br /><span> UINT64</span><br /><span> ENUM</span><br /></td>
<td>

Integer literals will implicitly coerce to ENUM type when necessary, or can
be explicitly CAST to a specific ENUM type name.

</td>
</tr>

<tr>
<td>DOUBLE literal</td>
<td>

<span> NUMERIC</span><br />

<span> FLOAT</span><br />

</td>
<td>Coercion may not be exact, and returns a close value.</td>
</tr>

<tr>
<td>STRING literal</td>
<td><span> DATE</span><br /><span> DATETIME</span><br /><span> TIME</span><br /><span> TIMESTAMP</span><br /><span> ENUM</span><br /><span> PROTO</span><br /></td>
<td>

String literals will implicitly coerce to PROTO
or ENUM type when necessary, or can
be explicitly CAST to a specific PROTO or
ENUM type name.

</td>
</tr>

<tr>
<td>BYTES literal</td>
<td>PROTO</td>
<td>&nbsp;</td>
</tr>

</tbody>
</table>

Literal coercion is needed when the actual literal type is different from the
type expected by the function in question. For
example, if function `func()` takes a DATE argument,
then the expression `func("2014-09-27")` is valid because the
string literal `"2014-09-27"` is coerced to
`DATE`.

Literal conversion is evaluated at analysis time, and gives an error if the
input literal cannot be converted successfully to the target type.

Note: String literals do not coerce to numeric types.

#### Parameter coercion

ZetaSQL supports the following parameter coercions:

<table>
<thead>
<tr>
<th>Input data type</th>
<th>Result data type</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT32 parameter</td>
<td>ENUM</td>
</tr>

<tr>
<td>INT64 parameter</td>
<td>ENUM</td>
</tr>

<tr>
<td>STRING parameter</td>
<td><span> DATE</span><br /><span> DATETIME</span><br /><span> TIME</span><br /><span> TIMESTAMP</span><br /><span> ENUM</span><br /><span> PROTO</span><br /></td>
</tr>

<tr>
<td>BYTES parameter</td>
<td>PROTO</td>
</tr>

</tbody>
</table>

If the parameter value cannot be coerced successfully to the target type, an
error is provided.

### Supertypes

A supertype is a common type to which two or more expressions can be coerced.
Supertypes are used with set operations such as `UNION ALL` and expressions such
as `CASE` that expect multiple arguments with matching types. Each type has one
or more supertypes, including itself, which defines its set of supertypes.

<table>
  <thead>
    <tr>
      <th>Input type</th>
      <th>Supertypes</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td>BOOL</td>
      <td>BOOL</td>
    </tr>
    
    
    <tr>
      <td>INT32</td>
      <td>

<span> INT32</span><br /><span> INT64</span><br /><span> FLOAT</span><br /><span> DOUBLE</span><br /><span> NUMERIC</span><br /><span> BIGNUMERIC</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>INT64</td>
      <td>

<span> INT64</span><br /><span> FLOAT</span><br /><span> DOUBLE</span><br /><span> NUMERIC</span><br /><span> BIGNUMERIC</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>UINT32</td>
      <td>

<span> UINT32</span><br /><span> INT64</span><br /><span> UINT64</span><br /><span> FLOAT</span><br /><span> DOUBLE</span><br /><span> NUMERIC</span><br /><span> BIGNUMERIC</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>UINT64</td>
      <td>

<span> UINT64</span><br /><span> FLOAT</span><br /><span> DOUBLE</span><br /><span> NUMERIC</span><br /><span> BIGNUMERIC</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>FLOAT</td>
      <td>

<span> FLOAT</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>DOUBLE</td>
      <td>

<span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>NUMERIC</td>
      <td>

<span> NUMERIC</span><br /><span> BIGNUMERIC</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>DECIMAL</td>
      <td>

<span> DECIMAL</span><br /><span> BIGDECIMAL</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>BIGNUMERIC</td>
      <td>

<span> BIGNUMERIC</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>BIGDECIMAL</td>
      <td>

<span> BIGDECIMAL</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
    
    
    <tr>
      <td>STRING</td>
      <td>STRING</td>
    </tr>
    
    
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    
    
    <tr>
      <td>TIME</td>
      <td>TIME</td>
    </tr>
    
    
    <tr>
      <td>DATETIME</td>
      <td>DATETIME</td>
    </tr>
    
    
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
    </tr>
    
    
    <tr>
      <td>ENUM</td>
      <td>
        ENUM with the same name. The resulting enum supertype is the one that
        occurred first.
      </td>
    </tr>
    
    
    <tr>
      <td>BYTES</td>
      <td>BYTES</td>
    </tr>
    
    
    <tr>
      <td>STRUCT</td>
      <td>
        STRUCT with the same field position types.
      </td>
    </tr>
    
    
    <tr>
      <td>ARRAY</td>
      <td>
        ARRAY with the same element types.
      </td>
    </tr>
    
    
    <tr>
      <td>PROTO</td>
      <td>
        PROTO with the same name. The resulting PROTO supertype is the one that
        occurred first. For example, the first occurrence could be in the
        first branch of a set operation or the first result expression in
        a CASE statement.
      </td>
    </tr>
    
    
  </tbody>
</table>

If you want to find the supertype for a set of input types, first determine the
intersection of the set of supertypes for each input type. If that set is empty
then the input types have no common supertype. If that set is non-empty, then
the common supertype is generally the
[most specific][con-supertype-specificity] type in that set. Generally,
the most specific type is the type with the most restrictive domain.

**Examples**

<table>
  <thead>
    <tr>
      <th>Input types</th>
      <th>Common supertype</th>
      <th>Returns</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td>

<span> INT64</span><br /><span> FLOAT</span><br />
</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>
        If you apply supertyping to INT64 and FLOAT, supertyping
        succeeds because they they share a supertype,
        DOUBLE.
      </td>
    </tr>
    
    <tr>
      <td>

<span> INT64</span><br /><span> DOUBLE</span><br />
</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>
        If you apply supertyping to INT64 and DOUBLE,
        supertyping succeeds because they they share a supertype,
        DOUBLE.
      </td>
    </tr>
    <tr>
      <td>

<span> INT64</span><br /><span> BOOL</span><br />
</td>
      <td>None</td>
      <td>Error</td>
      <td>
        If you apply supertyping to INT64 and BOOL, supertyping
        fails because they do not share a common supertype.
      </td>
    </tr>
  </tbody>
</table>

#### Exact and inexact types

Numeric types can be exact or inexact. For supertyping, if all of the
input types are exact types, then the resulting supertype can only be an
exact type.

The following table contains a list of exact and inexact numeric data types.

<table>
  <thead>
    <tr>
      <th>Exact types</th>
      <th>Inexact types</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>

<span> INT32</span><br /><span> UINT32</span><br /><span> INT64</span><br /><span> UINT64</span><br /><span> NUMERIC</span><br /><span> BIGNUMERIC</span><br />
</td>
      <td>

<span> FLOAT</span><br /><span> DOUBLE</span><br />
</td>
    </tr>
  </tbody>
</table>

**Examples**

<table>
  <thead>
    <tr>
      <th>Input types</th>
      <th>Common supertype</th>
      <th>Returns</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td>

<span> UINT64</span><br /><span> INT64</span><br />
</td>
      <td>DOUBLE</td>
      <td>Error</td>
      <td>
        If you apply supertyping to INT64 and UINT64, supertyping fails
        because they are both exact numeric types and the only shared supertype
        is DOUBLE, which is an inexact numeric type.
      </td>
    </tr>
    
    
    <tr>
      <td>

<span> UINT32</span><br /><span> INT32</span><br />
</td>
      <td>INT64</td>
      <td>INT64</td>
      <td>
        If you apply supertyping to INT32 and UINT32, supertyping
        succeeds because they are both exact numeric types and they share an
        exact supertype, INT64.
      </td>
    </tr>
    
    <tr>
      <td>

<span> INT64</span><br /><span> DOUBLE</span><br />
</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>
        If supertyping is applied to INT64 and DOUBLE, supertyping
        succeeds because there are exact and inexact numeric types being
        supertyped.
      </td>
    </tr>
    
    <tr>
      <td>

<span> UINT64</span><br /><span> INT64</span><br /><span> DOUBLE</span><br />
</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>
        If supertyping is applied to INT64, UINT64, and
        DOUBLE, supertyping succeeds because there are
        exact and inexact numeric types being supertyped.
      </td>
    </tr>
    
  </tbody>
</table>

#### Types specificity 
<a id="supertype_specificity"></a>

Each type has a domain of values that it supports. A type with a
narrow domain is more specific than a type with a wider domain. Exact types
are more specific than inexact types because inexact types have a wider range
of domain values that are supported than exact types. For example,
`INT64` is more specific than `DOUBLE`.

#### Supertypes and literals

Supertype rules for literals are more permissive than for normal expressions,
and are consistent with implicit coercion rules. The following algorithm is used
when the input set of types includes types related to literals:

+ If there exists non-literals in the set, find the set of common supertypes
  of the non-literals.
+ If there is at least one possible supertype, find the
  [most specific][con-supertype-specificity] type to
  which the remaining literal types can be implicitly coerced and return that
  supertype. Otherwise, there is no supertype.
+ If the set only contains types related to literals, compute the supertype of
  the literal types.
+ If all input types are related to `NULL` literals, then the resulting
  supertype is `INT64`.
+ If no common supertype is found, an error is produced.

**Examples**

<table>
  <thead>
    <tr>
      <th>Input types</th>
      <th>Common supertype</th>
      <th>Returns</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td>
        INT64 literal<br />
        INT32 expression<br />
      </td>
      <td>INT32</td>
      <td>INT32</td>
    </tr>
    
    
    <tr>
      <td>
        INT64 literal<br />
        UINT32 expression<br />
      </td>
      <td>UINT32</td>
      <td>UINT32</td>
    </tr>
    
    <tr>
      <td>
        INT64 literal<br />
        UINT64 expression<br />
      </td>
      <td>UINT64</td>
      <td>UINT64</td>
    </tr>
    
    <tr>
      <td>
        DOUBLE literal<br />
        FLOAT expression<br />
      </td>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    
    
    <tr>
      <td>
        INT64 literal<br />
        DOUBLE literal<br />
      </td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
    </tr>
    
    
    <tr>
      <td>
        INT64 expression<br />
        UINT64 expression<br />
        DOUBLE literal<br />
      </td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
    </tr>
    
    
    <tr>
      <td>
        TIMESTAMP expression<br />
        STRING literal<br />
      </td>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
    </tr>
    
    <tr>
      <td>
        NULL literal<br />
        NULL literal<br />
      </td>
      <td>INT64</td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>
        BOOL literal<br />
        TIMESTAMP literal<br />
      </td>
      <td>None</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[conversion-rules-table]: #conversion_rules

[con-rules-link-to-literal-coercion]: #literal_coercion

[con-rules-link-to-parameter-coercion]: #parameter_coercion

[con-rules-link-to-casting]: #casting

[con-supertype-specificity]: #supertype_specificity

[con-rules-link-to-time-zones]: https://github.com/google/zetasql/blob/master/docs/data-types.md#time_zones

[con-rules-link-to-conversion-functions]: #conversion_functions

[con-rules-link-to-cast]: #cast

[con-rules-link-to-conversion-functions-other]: #other_conv_functions

<!-- mdlint on -->

## Aggregate functions

An *aggregate function* is a function that summarizes the rows of a group into a
single value. `COUNT`, `MIN` and `MAX` are examples of aggregate functions.

```sql
SELECT COUNT(*) as total_count, COUNT(fruit) as non_null_count,
       MIN(fruit) as min, MAX(fruit) as max
FROM (SELECT NULL as fruit UNION ALL
      SELECT "apple" as fruit UNION ALL
      SELECT "pear" as fruit UNION ALL
      SELECT "orange" as fruit)

+-------------+----------------+-------+------+
| total_count | non_null_count | min   | max  |
+-------------+----------------+-------+------+
| 4           | 3              | apple | pear |
+-------------+----------------+-------+------+
```

When used in conjunction with a `GROUP BY` clause, the groups summarized
typically have at least one row. When the associated `SELECT` has no `GROUP BY`
clause or when certain aggregate function modifiers filter rows from the group
to be summarized it is possible that the aggregate function needs to summarize
an empty group. In this case, the `COUNT` and `COUNTIF` functions return `0`,
while all other aggregate functions return `NULL`.

The following sections describe the aggregate functions that ZetaSQL
supports.

### ANY_VALUE

```sql
ANY_VALUE(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
rows for which `expression` is `NULL` are considered and may be selected.

**Supported Argument Types**

Any

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

**Returned Data Types**

Matches the input data type.

**Examples**

```sql
SELECT ANY_VALUE(fruit) as any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+-----------+
| any_value |
+-----------+
| apple     |
+-----------+
```

```sql
SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

+--------+-----------+
| fruit  | any_value |
+--------+-----------+
| pear   | pear      |
| apple  | pear      |
| banana | apple     |
+--------+-----------+
```

### ARRAY_AGG
```sql
ARRAY_AGG(
  [DISTINCT]
  expression
  [{IGNORE|RESPECT} NULLS]
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
[OVER (...)]
```

**Description**

Returns an ARRAY of `expression` values.

**Supported Argument Types**

All data types except ARRAY.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified, the `NULL` values are excluded from the result. If
    `RESPECT NULLS` is specified, the `NULL` values are included in the
    result. If
    neither is specified, the `NULL` values are included in the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

ARRAY

If there are zero input rows, this function returns `NULL`.

**Examples**

```sql
SELECT ARRAY_AGG(x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [2, 1, -2, 3, -2, 1, 2] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+---------------+
| array_agg     |
+---------------+
| [2, 1, -2, 3] |
+---------------+
```

```sql
SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [1, -2, 3, -2, 1] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------------+
| array_agg               |
+-------------------------+
| [1, 1, 2, -2, -2, 2, 3] |
+-------------------------+
```

```sql
SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+-------------------+
| array_agg         |
+-------------------+
| [2, 1, -2, 3, -2] |
+-------------------+
```

```sql
SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x LIMIT 2) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

+-----------+
| array_agg |
+-----------+
| [-2, 1]   |
+-----------+
```

```sql
SELECT
  x,
  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

+----+-------------------------+
| x  | array_agg               |
+----+-------------------------+
| 1  | [1, 1]                  |
| 1  | [1, 1]                  |
| 2  | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| -2 | [1, 1, 2, -2, -2, 2]    |
| 2  | [1, 1, 2, -2, -2, 2]    |
| 3  | [1, 1, 2, -2, -2, 2, 3] |
+----+-------------------------+
```

### ARRAY_CONCAT_AGG

```sql
ARRAY_CONCAT_AGG(
  expression
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
```

**Description**

Concatenates elements from `expression` of type
ARRAY, returning a single
ARRAY as a result. This function ignores NULL input
arrays, but respects the NULL elements in non-NULL input arrays.

**Supported Argument Types**

ARRAY

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input arrays, not
    the number of elements in the arrays. An empty array counts as 1. A NULL
    array is not counted.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

ARRAY

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x)) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+-----------------------------------+
| array_concat_agg                  |
+-----------------------------------+
| [5, 6, 7, 8, 9, 1, 2, 3, 4]       |
+-----------------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+--------------------------+
| array_concat_agg         |
+--------------------------+
| [1, 2, 3, 4, 5, 6]       |
+--------------------------+
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

+------------------+
| array_concat_agg |
+------------------+
| [5, 6, 7, 8, 9]  |
+------------------+
```

### AVG
```sql
AVG(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the average of non-`NULL` input values, or `NaN` if the input contains a
`NaN`.

**Supported Argument Types**

Any numeric input type, such as  INT64. Note that, for
floating point input types, the return result is non-deterministic, which
means you might receive a different result each time you use this function.

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

**Returned Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT AVG(x) as avg
FROM UNNEST([0, 2, 4, 4, 5]) as x;

+-----+
| avg |
+-----+
| 3   |
+-----+
```

```sql
SELECT AVG(DISTINCT x) AS avg
FROM UNNEST([0, 2, 4, 4, 5]) AS x;

+------+
| avg  |
+------+
| 2.75 |
+------+
```

```sql
SELECT
  x,
  AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x;

+------+------+
| x    | avg  |
+------+------+
| NULL | NULL |
| 0    | 0    |
| 2    | 1    |
| 4    | 3    |
| 4    | 4    |
| 5    | 4.5  |
+------+------+
```

### BIT_AND
```sql
BIT_AND(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise AND operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

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

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x;

+---------+
| bit_and |
+---------+
| 1       |
+---------+
```

### BIT_OR
```sql
BIT_OR(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise OR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

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

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x;

+--------+
| bit_or |
+--------+
| 61601  |
+--------+
```

### BIT_XOR
```sql
BIT_XOR(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Performs a bitwise XOR operation on `expression` and returns the result.

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Optional Clauses**

The clauses are applied *in the following order*:

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

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 5678    |
+---------+
```

```sql
SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

+---------+
| bit_xor |
+---------+
| 4860    |
+---------+
```

### COUNT

1.

```sql
COUNT(*)  [OVER (...)]
```

2.

```sql
COUNT(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

**Supported Argument Types**

`expression` can be any data type. If
`DISTINCT` is present, `expression` can only be a data type that is
[groupable][agg-data-type-properties].

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

**Return Data Types**

INT64

**Examples**

You can use the `COUNT` function to return the number of rows in a table or the
number of distinct values of an expression. For example:

```sql
SELECT
  COUNT(*) AS count_star,
  COUNT(DISTINCT x) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------------+--------------+
| count_star | count_dist_x |
+------------+--------------+
| 4          | 3            |
+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

+------+------------+--------------+
| x    | count_star | count_dist_x |
+------+------------+--------------+
| 1    | 3          | 2            |
| 4    | 3          | 2            |
| 4    | 3          | 2            |
| 5    | 1          | 1            |
+------+------------+--------------+
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(x) OVER (PARTITION BY MOD(x, 3)) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

+------+------------+---------+
| x    | count_star | count_x |
+------+------------+---------+
| NULL | 1          | 0       |
| 1    | 3          | 3       |
| 4    | 3          | 3       |
| 4    | 3          | 3       |
| 5    | 1          | 1       |
+------+------------+---------+
```

If you want to count the number of distinct values of an expression for which a
certain condition is satisfied, this is one recipe that you can use:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Here, `IF` will return the value of `expression` if `condition` is `TRUE`, or
`NULL` otherwise. The surrounding `COUNT(DISTINCT ...)` will ignore the `NULL`
values, so it will count only the distinct values of `expression` for which
`condition` is `TRUE`.

For example, to count the number of distinct positive values of `x`:

```sql
SELECT COUNT(DISTINCT IF(x > 0, x, NULL)) AS distinct_positive
FROM UNNEST([1, -2, 4, 1, -5, 4, 1, 3, -6, 1]) AS x;

+-------------------+
| distinct_positive |
+-------------------+
| 3                 |
+-------------------+
```

Or to count the number of distinct dates on which a certain kind of event
occurred:

```sql
WITH Events AS (
  SELECT DATE '2021-01-01' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-03' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
)
SELECT
  COUNT(DISTINCT IF(event_type = 'FAILURE', event_date, NULL))
    AS distinct_dates_with_failures
FROM Events;

+------------------------------+
| distinct_dates_with_failures |
+------------------------------+
| 2                            |
+------------------------------+
```

### COUNTIF
```sql
COUNTIF(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the count of `TRUE` values for `expression`. Returns `0` if there are
zero input rows, or if `expression` evaluates to `FALSE` or `NULL` for all rows.

Since `expression` must be a `BOOL`, the form
`COUNTIF(DISTINCT ...)` is generally not useful: there is only one distinct
value of `TRUE`. So `COUNTIF(DISTINCT ...)` will return 1 if `expression`
evaluates to `TRUE` for one or more input rows, or 0 otherwise.
Usually when someone wants to combine `COUNTIF` and `DISTINCT`, they
want to count the number of distinct values of an expression for which a certain
condition is satisfied. One recipe to achieve this is the following:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Note that this uses `COUNT`, not `COUNTIF`; the `IF` part has been moved inside.
To learn more, see the examples for [`COUNT`](#count).

**Supported Argument Types**

BOOL

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

**Return Data Types**

INT64

**Examples**

```sql
SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive
FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x;

+--------------+--------------+
| num_negative | num_positive |
+--------------+--------------+
| 3            | 4            |
+--------------+--------------+
```

```sql
SELECT
  x,
  COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS num_negative
FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x;

+------+--------------+
| x    | num_negative |
+------+--------------+
| NULL | 0            |
| 0    | 1            |
| -2   | 1            |
| 3    | 1            |
| 4    | 0            |
| 5    | 0            |
| 6    | 1            |
| -7   | 2            |
| -10  | 2            |
+------+--------------+
```

### LOGICAL_AND
```sql
LOGICAL_AND(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the logical AND of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

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

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_AND(x) AS logical_and FROM UNNEST([true, false, true]) AS x;

+-------------+
| logical_and |
+-------------+
| false       |
+-------------+
```

### LOGICAL_OR
```sql
LOGICAL_OR(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the logical OR of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

BOOL

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

**Return Data Types**

BOOL

**Examples**

```sql
SELECT LOGICAL_OR(x) AS logical_or FROM UNNEST([true, false, true]) AS x;

+------------+
| logical_or |
+------------+
| true       |
+------------+
```

### MAX
```sql
MAX(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the maximum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties].

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

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MAX(x) AS max
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| max |
+-----+
| 55  |
+-----+
```

```sql
SELECT x, MAX(x) OVER (PARTITION BY MOD(x, 2)) AS max
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | max  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 8    |
| 4    | 8    |
| 37   | 55   |
| 55   | 55   |
+------+------+
```

### MIN
```sql
MIN(
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the minimum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.
Returns `NaN` if the input contains a `NaN`.

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties].

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

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```sql
SELECT MIN(x) AS min
FROM UNNEST([8, 37, 4, 55]) AS x;

+-----+
| min |
+-----+
| 4   |
+-----+
```

```sql
SELECT x, MIN(x) OVER (PARTITION BY MOD(x, 2)) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+------+------+
| x    | min  |
+------+------+
| NULL | NULL |
| NULL | NULL |
| 8    | 4    |
| 4    | 4    |
| 37   | 37   |
| 55   | 37   |
+------+------+
```

### STRING_AGG
```sql
STRING_AGG(
  [DISTINCT]
  expression [, delimiter]
  [HAVING {MAX | MIN} expression2]
  [ORDER BY key [{ASC|DESC}] [, ... ]]
  [LIMIT n]
)
[OVER (...)]
```

**Description**

Returns a value (either STRING or
BYTES) obtained by concatenating non-null values.
Returns `NULL` if there are zero input rows or `expression` evaluates to
`NULL` for all rows.

If a `delimiter` is specified, concatenated values are separated by that
delimiter; otherwise, a comma is used as a delimiter.

**Supported Argument Types**

STRING
BYTES

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `OVER`: Specifies a window. See
    [Analytic Functions][analytic-functions].
1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.
1.  `ORDER BY`: Specifies the order of the values.
    *   For each sort key, the default sort direction is `ASC`.
    *   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
        possible value; that is, NULLs appear first in `ASC` sorts and last in
        `DESC` sorts.
    *   Floating point data types: see
        [Floating Point Semantics][floating-point-semantics]
        on ordering and grouping.
    *   If `DISTINCT` is also specified, then
        the sort key must be the same as `expression`.
    *   If `ORDER BY` is not specified, the order of the elements in the output
        array is non-deterministic, which means you might receive a different
        result each time you use this function.
1.  `LIMIT`: Specifies the maximum number of `expression` inputs in the
    result.
    The limit applies to the number of input strings,
    not the number of characters or bytes in the inputs. An empty string counts
    as 1. A NULL string is not counted.
    The limit `n` must be a constant INT64.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Data Types**

STRING
BYTES

**Examples**

```sql
SELECT STRING_AGG(fruit) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+------------------------+
| string_agg             |
+------------------------+
| apple,pear,banana,pear |
+------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| apple & pear & banana & pear |
+------------------------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+-----------------------+
| string_agg            |
+-----------------------+
| apple & pear & banana |
+-----------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+------------------------------+
| string_agg                   |
+------------------------------+
| pear & pear & apple & banana |
+------------------------------+
```

```sql
SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+--------------+
| string_agg   |
+--------------+
| apple & pear |
+--------------+
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

+---------------+
| string_agg    |
+---------------+
| pear & banana |
+---------------+
```

```sql
SELECT
  fruit,
  STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

+--------+------------------------------+
| fruit  | string_agg                   |
+--------+------------------------------+
| NULL   | NULL                         |
| pear   | pear & pear                  |
| pear   | pear & pear                  |
| apple  | pear & pear & apple          |
| banana | pear & pear & apple & banana |
+--------+------------------------------+
```

### SUM
```sql
SUM(
  [DISTINCT]
  expression
  [HAVING {MAX | MIN} expression2]
)
[OVER (...)]
```

**Description**

Returns the sum of non-null values.

If the expression is a floating point value, the sum is non-deterministic, which
means you might receive a different result each time you use this function.

**Supported Argument Types**

Any supported numeric data types and INTERVAL.

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

**Return Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th><th>INTERVAL</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">INTERVAL</td></tr>
</tbody>

</table>

Special cases:

Returns `NULL` if the input contains only `NULL`s.

Returns `NULL` if the input contains no rows.

Returns `Inf` if the input contains `Inf`.

Returns `-Inf` if the input contains `-Inf`.

Returns `NaN` if the input contains a `NaN`.

Returns `NaN` if the input contains a combination of `Inf` and `-Inf`.

**Examples**

```sql
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 25  |
+-----+
```

```sql
SELECT SUM(DISTINCT x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 15  |
+-----+
```

```sql
SELECT
  x,
  SUM(x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
| x | sum |
+---+-----+
| 3 | 6   |
| 3 | 6   |
| 1 | 10  |
| 4 | 10  |
| 4 | 10  |
| 1 | 10  |
| 2 | 9   |
| 5 | 9   |
| 2 | 9   |
+---+-----+
```

```sql
SELECT
  x,
  SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+---+-----+
| x | sum |
+---+-----+
| 3 | 3   |
| 3 | 3   |
| 1 | 5   |
| 4 | 5   |
| 4 | 5   |
| 1 | 5   |
| 2 | 7   |
| 5 | 7   |
| 2 | 7   |
+---+-----+
```

```sql
SELECT SUM(x) AS sum
FROM UNNEST([]) AS x;

+------+
| sum  |
+------+
| NULL |
+------+
```

### Common clauses

#### HAVING MAX and HAVING MIN clause 
<a id="max_min_clause"></a>

Most aggregate functions support two optional clauses called `HAVING MAX` and
`HAVING MIN`, which restricts the set of rows that a function aggregates to
rows that have a maximal or minimal value in a particular column. The syntax
generally looks like this:

```sql
aggregate_function(expression1 [HAVING {MAX | MIN} expression2])
```

+ `HAVING MAX`: Restricts the set of rows that the
  function aggregates to those having a value for `expression2` equal to the
  maximum value for `expression2` within the group. The  maximum value is
  equal to the result of `MAX(expression2)`.
+ `HAVING MIN` Restricts the set of rows that the
  function aggregates to those having a value for `expression2` equal to the
  minimum value for `expression2` within the group. The minimum value is
  equal to the result of `MIN(expression2)`.

These clauses ignore `NULL` values when computing the maximum or minimum
value unless `expression2` evaluates to `NULL` for all rows.

 These clauses only support
[orderable data types][agg-data-type-properties].

**Example**

In this example, the average rainfall is returned for the most recent year,
2001.

```sql
WITH Precipitation AS
 (SELECT 2001 as year, 'spring' as season, 9 as inches UNION ALL
  SELECT 2001, 'winter', 1 UNION ALL
  SELECT 2000, 'fall', 3 UNION ALL
  SELECT 2000, 'summer', 5 UNION ALL
  SELECT 2000, 'spring', 7 UNION ALL
  SELECT 2000, 'winter', 2)
SELECT AVG(inches HAVING MAX year) as average FROM Precipitation

+---------+
| average |
+---------+
| 5       |
+---------+
```

First, the query gets the rows with the maximum value in the `year` column.
There are two:

```sql
+------+--------+--------+
| year | season | inches |
+------+--------+--------+
| 2001 | spring | 9      |
| 2001 | winter | 1      |
+------+--------+--------+
```

Finally, the query averages the values in the `inches` column (9 and 1) with
this result:

```sql
+---------+
| average |
+---------+
| 5       |
+---------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

<!-- mdlint on -->

## Statistical aggregate functions

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

## Anonymization aggregate functions

Anonymization aggregate functions can transform user data into anonymous
information. This is done in such a way that it is not reasonably likely that
anyone with access to the data can identify or re-identify an individual user
from the anonymized data. Anonymization aggregate functions can only be used
with [anonymization-enabled queries][anon-syntax].

### ANON_AVG

```sql
ANON_AVG(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the average of non-`NULL`, non-`NaN` values in the expression.
This function first computes the average per anonymization ID, and then computes
the final result by averaging these averages.

You can [clamp the input values explicitly][anon-clamp], otherwise
input values are clamped implicitly. Clamping is done to the
per-anonymization ID averages.

`expression` can be any numeric input type, such as
INT64.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the average number of each item requested
per professor. Smaller aggregations may not be included. This query references
a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| pencil   | 38.5038356810269 |
| pen      | 13.4725028762032 |
+----------+------------------+
```

```sql
-- Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity) average_quantity
FROM view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

### ANON_COUNT

+ [Signature 1](#anon_count_signature1)
+ [Signature 2](#anon_count_signature2)

#### Signature 1 
<a id="anon_count_signature1"></a>

```sql
ANON_COUNT(*)
```

**Description**

Returns the number of rows in the [anonymization-enabled][anon-from-clause]
`FROM` clause. The final result is an aggregation across anonymization IDs.
[Input values are clamped implicitly][anon-clamp]. Clamping is performed per
anonymization ID.

**Return type**

`INT64`

**Examples**

The following anonymized query counts the number of requests for each item.
This query references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_COUNT(*) times_requested
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_COUNT(*) times_requested
FROM view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

#### Signature 2 
<a id="anon_count_signature2"></a>

```sql
ANON_COUNT(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the number of non-`NULL` expression values. The final result is an
aggregation across anonymization IDs.

You can [clamp the input values explicitly][anon-clamp], otherwise
input values are clamped implicitly. Clamping is performed per anonymization ID.

**Return type**

`INT64`

**Examples**

The following anonymized query counts the number of requests made for each
type of item. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| pencil   | 5               |
| pen      | 2               |
+----------+-----------------+
```

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_COUNT(item CLAMPED BETWEEN 0 AND 100) times_requested
FROM view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+-----------------+
| item     | times_requested |
+----------+-----------------+
| scissors | 1               |
| pencil   | 4               |
| pen      | 3               |
+----------+-----------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

### ANON_PERCENTILE_CONT

```sql
ANON_PERCENTILE_CONT(expression, percentile [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes a percentile for it. The final result is an
aggregation across anonymization IDs. The percentile must be a literal in the
range [0, 1]. You can [clamp the input values][anon-clamp] explicitly,
otherwise input values are clamped implicitly. Clamping is performed per
anonymization ID.

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the percentile of items requested. Smaller
aggregations may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_PERCENTILE_CONT(quantity, 0.5 CLAMPED BETWEEN 0 AND 100) percentile_requested
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+----------------------+
| item     | percentile_requested |
+----------+----------------------+
| pencil   | 72.00011444091797    |
| scissors | 8.000175476074219    |
| pen      | 23.001075744628906   |
+----------+----------------------+
```

### ANON_STDDEV_POP

```sql
ANON_STDDEV_POP(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes the population (biased) standard deviation of
the values in the expression. The final result is an aggregation across
anonymization IDs between `0` and `+Inf`. You can
[clamp the input values][anon-clamp] explicitly, otherwise input values are
clamped implicitly. Clamping is performed per individual user values.

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the population (biased) standard deviation
of items requested. Smaller aggregations may not be included. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_STDDEV_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_standard_deviation
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+------------------------+
| item     | pop_standard_deviation |
+----------+------------------------+
| pencil   | 25.350871122442054     |
| scissors | 50                     |
| pen      | 2                      |
+----------+------------------------+
```

### ANON_SUM

```sql
ANON_SUM(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Returns the sum of non-`NULL`, non-`NaN` values in the expression. The final
result is an aggregation across anonymization IDs. You can optionally
[clamp the input values][anon-clamp]. Clamping is performed per
anonymization ID.

The expression can be any numeric input type, such as
`INT64`.

**Return type**

Matches the type in the expression.

**Examples**

The following anonymized query gets the sum of items requested. Smaller
aggregations may not be included. This query references a view called
[`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_SUM(quantity CLAMPED BETWEEN 0 AND 100) quantity
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------+
| item     | quantity  |
+----------+-----------+
| pencil   | 143       |
| pen      | 59        |
+----------+-----------+
```

```sql
-- Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_SUM(quantity) quantity
FROM view_on_professors
GROUP BY item;

-- These results will not change when you run the query.
+----------+----------+
| item     | quantity |
+----------+----------+
| scissors | 8        |
| pencil   | 144      |
| pen      | 58       |
+----------+----------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

### ANON_VAR_POP

```sql
ANON_VAR_POP(expression [CLAMPED BETWEEN lower AND upper])
```

**Description**

Takes an expression and computes the population (biased) variance of the values
in the expression. The final result is an aggregation across
anonymization IDs between `0` and `+Inf`. You can
[clamp the input values][anon-clamp] explicitly, otherwise input values are
clamped implicitly. Clamping is performed per individual user values.

Caveats:

+ `NUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `BIGNUMERIC` arguments are not allowed. If you need them, cast them to
  `DOUBLE` first.
+ `NULL`s are always ignored. If all inputs are ignored, this function returns
  `NULL`.

**Return type**

`DOUBLE`

**Examples**

The following anonymized query gets the population (biased) variance
of items requested. Smaller aggregations may not be included. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
-- With noise
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=10, delta=.01, kappa=1)
  item, ANON_VAR_POP(quantity CLAMPED BETWEEN 0 AND 100) pop_variance
FROM view_on_professors
GROUP BY item;

-- These results will change each time you run the query.
-- Smaller aggregations may be removed.
+----------+-----------------+
| item     | pop_variance    |
+----------+-----------------+
| pencil   | 642             |
| pen      | 2.6666666666665 |
| scissors | 2500            |
+----------+-----------------+
```

### CLAMPED BETWEEN clause 
<a id="anon_clamping"></a>

```sql
CLAMPED BETWEEN lower_bound AND upper_bound
```

Clamping of aggregations is done to avoid the re-identifiability
of outliers. The `CLAMPED BETWEEN` clause [explicitly clamps][anon-exp-clamp]
each aggregate contribution per anonymization ID within the specified range.
This clause is optional. If you do not include it in an anonymization aggregate
function, [clamping is implicit][anon-imp-clamp].

**Examples**

The following anonymized query clamps each aggregate contribution per
anonymization ID and within a specified range (`0` and `100`). As long as all
or most values fall within this range, your results will be accurate. This query
references a view called [`view_on_professors`][anon-example-views].

```sql
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 0 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 8                |
| pencil   | 40               |
| pen      | 18.5             |
+----------+------------------+
```

Notice what happens when most or all values fall outside of the clamped range.
To get accurate results, ensure that the difference between the upper and lower
bound is as small as possible while most inputs are between the upper and lower
bound.

```sql {.bad}
--Without noise (this un-noised version is for demonstration only)
SELECT
  WITH ANONYMIZATION OPTIONS(epsilon=1e20, delta=.01, kappa=1)
  item, ANON_AVG(quantity CLAMPED BETWEEN 50 AND 100) average_quantity
FROM view_on_professors
GROUP BY item;

+----------+------------------+
| item     | average_quantity |
+----------+------------------+
| scissors | 54               |
| pencil   | 58               |
| pen      | 51               |
+----------+------------------+
```

Note: You can learn more about when and when not to use
noise [here][anon-noise].

### Explicit clamping 
<a id="anon_explicit_clamping"></a>

In an anonymization aggregate function, the [`CLAMPED BETWEEN`][anon-clamp]
clause explicitly clamps the total contribution from each anonymization ID to
within a specified range.

Explicit bounds are uniformly applied to all aggregations.  So even if some
aggregations have a wide range of values, and others have a narrow range of
values, the same bounds are applied to all of them.  On the other hand, when
[implicit bounds][anon-imp-clamp] are inferred from the data, the bounds applied
to each aggregation can be different.

Explicit bounds should be chosen to reflect public information.
For example, bounding ages between 0 and 100 reflects public information
because in general, the age of most people falls within this range.

Important: The results of the query reveal the explicit bounds. Do not use
explicit bounds based on the user data; explicit bounds should be based on
public information.

### Implicit clamping 
<a id="anon_implicit_clamping"></a>

In an anonymization aggregate function, the [`CLAMPED BETWEEN`][anon-clamp]
clause is optional. If you do not include this clause, clamping is implicit,
which means bounds are derived from the data itself in a differentially
private way. The process is somewhat random, so aggregations with identical
ranges can have different bounds.

Implicit bounds are determined per aggregation. So if some
aggregations have a wide range of values, and others have a narrow range of
values, implicit bounding can identify different bounds for different
aggregations as appropriate. This may be an advantage or a disadvantage
depending on your use case: different bounds for different aggregations
can result in lower error, but this also means that different
aggregations have different levels of uncertainty, which may not be
directly comparable. [Explicit bounds][anon-exp-clamp], on the other hand,
apply uniformly to all aggregations and should be derived from public
information.

When clamping is implicit, part of the total epsilon is spent picking bounds.
This leaves less epsilon for aggregations, so these aggregations are noisier.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[anon-clamp]: #anon_clamping

[anon-exp-clamp]: #anon_explicit_clamping

[anon-imp-clamp]: #anon_implicit_clamping

[anon-syntax]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md

[anon-example-views]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_example_views

[anon-from-clause]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#anon_from

[anon-noise]: https://github.com/google/zetasql/blob/master/docs/anonymization_syntax.md#eliminate_noise

<!-- mdlint on -->

## Approximate aggregate functions

Approximate aggregate functions are scalable in terms of memory usage and time,
but produce approximate results instead of exact results. These functions
typically require less memory than [exact aggregation functions][aggregate-functions-reference]
like `COUNT(DISTINCT ...)`, but also introduce statistical uncertainty.
This makes approximate aggregation appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

The approximate aggregate functions in this section work directly on the
input data, rather than an intermediate estimation of the data. These functions
_do not allow_ users to specify the precision for the estimation with
sketches. If you would like specify precision with sketches, see:

+  [HyperLogLog++ functions][hll-functions] to estimate cardinality.
+  [KLL16 functions][kll-functions] to estimate quantile values.

### APPROX_COUNT_DISTINCT

```sql
APPROX_COUNT_DISTINCT(
  expression
)
```

**Description**

Returns the approximate result for `COUNT(DISTINCT expression)`. The value
returned is a statistical estimate&mdash;not necessarily the actual value.

This function is less accurate than `COUNT(DISTINCT expression)`, but performs
better on huge input.

**Supported Argument Types**

Any data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

INT64

**Examples**

```sql
SELECT APPROX_COUNT_DISTINCT(x) as approx_distinct
FROM UNNEST([0, 1, 1, 2, 3, 5]) as x;

+-----------------+
| approx_distinct |
+-----------------+
| 5               |
+-----------------+
```

### APPROX_QUANTILES

```sql
APPROX_QUANTILES(
  [DISTINCT]
  expression, number
  [{IGNORE|RESPECT} NULLS]
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate boundaries for a group of `expression` values, where
`number` represents the number of quantiles to create. This function returns
an array of `number` + 1 elements, where the first element is the approximate
minimum and the last element is the approximate maximum.

**Supported Argument Types**

`expression` can be any supported data type **except**:
`ARRAY`
`STRUCT`
`PROTO`

`number` must be INT64.

**Optional Clauses**

The clauses are applied *in the following order*:

1.  `DISTINCT`: Each distinct value of
    `expression` is aggregated only once into the result.
1.  `IGNORE NULLS` or `RESPECT NULLS`: If `IGNORE NULLS` is
    specified, the `NULL` values are excluded from the result. If
    `RESPECT NULLS` is specified, the `NULL` values are included in the
    result. If neither is specified, the `NULL`
    values are excluded from the result.
1.  `HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of the type specified by the `expression`
parameter.

Returns `NULL` if there are zero input
rows or `expression` evaluates to NULL for all rows.

**Examples**

```sql
SELECT APPROX_QUANTILES(x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 5, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 100)[OFFSET(90)] AS percentile_90
FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS x;

+---------------+
| percentile_90 |
+---------------+
| 9             |
+---------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2) AS approx_quantiles
FROM UNNEST([1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [1, 6, 10]       |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 4, 10]    |
+------------------+
```

```sql
SELECT APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS) AS approx_quantiles
FROM UNNEST([NULL, NULL, 1, 1, 1, 4, 5, 6, 7, 8, 9, 10]) AS x;

+------------------+
| approx_quantiles |
+------------------+
| [NULL, 6, 10]    |
+------------------+
```

### APPROX_TOP_COUNT

```sql
APPROX_TOP_COUNT(
  expression, number
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate top elements of `expression`. The `number` parameter
specifies the number of elements returned.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields. The first field
(named `value`) contains an input value. The second field (named `count`)
contains an INT64 specifying the number of times the
value was returned.

Returns `NULL` if there are zero input rows.

**Examples**

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST(["apple", "apple", "pear", "pear", "pear", "banana"]) as x;

+-------------------------+
| approx_top_count        |
+-------------------------+
| [{pear, 3}, {apple, 2}] |
+-------------------------+
```

**NULL handling**

APPROX_TOP_COUNT does not ignore NULLs in the input. For example:

```sql
SELECT APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM UNNEST([NULL, "pear", "pear", "pear", "apple", NULL]) as x;

+------------------------+
| approx_top_count       |
+------------------------+
| [{pear, 3}, {NULL, 2}] |
+------------------------+
```

### APPROX_TOP_SUM

```sql
APPROX_TOP_SUM(
  expression, weight, number
  [HAVING {MAX | MIN} expression2]
)
```

**Description**

Returns the approximate top elements of `expression`, based on the sum of an
assigned `weight`. The `number` parameter specifies the number of elements
returned.

If the `weight` input is negative or `NaN`, this function returns an error.

**Supported Argument Types**

`expression` can be of any data type that the `GROUP BY` clause supports.

`weight` must be one of the following:

<ul>
<li>INT64</li>

<li>UINT64</li>

<li>NUMERIC</li>

<li>BIGNUMERIC</li>

<li>DOUBLE</li>
</ul>

`number` must be INT64.

**Optional Clause**

`HAVING MAX` or `HAVING MIN`: Restricts the set of rows that the
    function aggregates by a maximum or minimum value. See
    [HAVING MAX and HAVING MIN clause][max_min_clause] for details.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Returned Data Types**

An ARRAY of type STRUCT.
The STRUCT contains two fields: `value` and `sum`.
The `value` field contains the value of the input expression. The `sum` field is
the same type as `weight`, and is the approximate sum of the input weight
associated with the `value` field.

Returns `NULL` if there are zero input rows.

**Examples**

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([
  STRUCT("apple" AS x, 3 AS weight),
  ("pear", 2),
  ("apple", 0),
  ("banana", 5),
  ("pear", 4)
]);

+--------------------------+
| approx_top_sum           |
+--------------------------+
| [{pear, 6}, {banana, 5}] |
+--------------------------+
```

**NULL handling**

APPROX_TOP_SUM does not ignore NULL values for the `expression` and `weight`
parameters.

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, NULL AS weight), ("pear", 0), ("pear", NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{pear, 0}, {apple, NULL}] |
+----------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, 2)]);

+-------------------------+
| approx_top_sum          |
+-------------------------+
| [{NULL, 2}, {apple, 0}] |
+-------------------------+
```

```sql
SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum FROM
UNNEST([STRUCT("apple" AS x, 0 AS weight), (NULL, NULL)]);

+----------------------------+
| approx_top_sum             |
+----------------------------+
| [{apple, 0}, {NULL, NULL}] |
+----------------------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[hll-functions]: #hyperloglog_functions

[kll-functions]: #kll16_quantile_functions

[aggregate-functions-reference]: #aggregate_functions

<!-- mdlint on -->

## HyperLogLog++ functions

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

[approx-functions-reference]: #approximate_aggregate_functions

[aggregate-functions-reference]: #aggregate_functions

<!-- mdlint on -->

## KLL16 quantile functions

The [KLL16 algorithm][kll-algorithm] estimates
[quantiles][kll-quantiles] from [sketches][kll-sketches]. If you do not want
to work with sketches and do not need customized precision, consider
using [approximate aggregate functions with system-defined precision][approx-functions-reference].

KLL16 functions are approximate aggregate functions.
Approximate aggregation requires significantly less memory than an exact
quantiles computation, but also introduces statistical uncertainty.
This makes approximate aggregation appropriate for large data streams for
which linear memory usage is impractical, as well as for data that is
already approximate.

Note: While `APPROX_QUANTILES` is also returning approximate quantile results,
the functions from this section allow for partial aggregations and
re-aggregations.

ZetaSQL supports the following KLL16 functions:

### KLL_QUANTILES.INIT_INT64

```sql
KLL_QUANTILES.INIT_INT64(input[, precision[, weight => input_weight]])
```

**Description**

Takes one or more `input` values and aggregates them into a
[KLL16][link-to-kll-paper] sketch. This function represents the output sketch
using the `BYTES` data type. This is an
aggregate function.

The `precision` argument defines the exactness of the returned approximate
quantile *q*. By default, the rank of the approximate quantile in the input can
be at most ±1/1000 * *n* off from ⌈Φ * *n*⌉, where *n* is the number of rows in
the input and ⌈Φ * *n*⌉ is the rank of the exact quantile. If you provide a
value for `precision`, the rank of the approximate quantile in the input can be
at most ±1/`precision` * *n* off from the rank of the exact quantile. The error
is within this error bound in 99.999% of cases. This error guarantee only
applies to the difference between exact and approximate ranks: the numerical
difference between the exact and approximated value for a quantile can be
arbitrarily large.

By default, values in an initialized KLL sketch are weighted equally as `1`.
If you would you like to weight values differently, use the
mandatory-named argument, `weight`, which assigns weight to each input in the
resulting KLL sketch. `weight` is a multiplier. For example, if you assign a
weight of `3` to an input value, it's as if three instances of the input value
are included in the generation of the KLL sketch.

**Supported Argument Types**

+ `input`: `INT64`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`BYTES`

**Example**

The following query takes a column of type `INT64` and
outputs a sketch as `BYTES` that allows you to retrieve values whose ranks
are within ±1/1000 * 5 = ±1/200 ≈ 0 ranks of their exact quantile.

```sql
SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
FROM (SELECT 1 AS x UNION ALL
      SELECT 2 AS x UNION ALL
      SELECT 3 AS x UNION ALL
      SELECT 4 AS x UNION ALL
      SELECT 5 AS x);

+----------------------------------------------------------------------+
| kll_sketch                                                           |
+----------------------------------------------------------------------+
| "\010q\020\005 \004\212\007\025\010\200                              |
| \020\350\007\032\001\001\"\001\005*\007\n\005\001\002\003\004\005"   |
+----------------------------------------------------------------------+
```

The following examples illustrate how weight works when you initialize a
KLL sketch. The results are converted to quantiles.

```sql
WITH points AS (
  SELECT 1 AS x, 1 AS y UNION ALL
  SELECT 2 AS x, 1 AS y UNION ALL
  SELECT 3 AS x, 1 AS y UNION ALL
  SELECT 4 AS x, 1 AS y UNION ALL
  SELECT 5 AS x, 1 AS y)
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM
  (
    SELECT KLL_QUANTILES.INIT_INT64(x, 1000,  weight=>y) AS kll_sketch
    FROM points
  );

+---------+
| median  |
+---------+
| [1,3,5] |
+---------+
```

```sql
WITH points AS (
  SELECT 1 AS x, 1 AS y UNION ALL
  SELECT 2 AS x, 3 AS y UNION ALL
  SELECT 3 AS x, 1 AS y UNION ALL
  SELECT 4 AS x, 1 AS y UNION ALL
  SELECT 5 AS x, 1 AS y)
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM
  (
    SELECT KLL_QUANTILES.INIT_INT64(x, 1000,  weight=>y) AS kll_sketch
    FROM points
  );

+---------+
| median  |
+---------+
| [1,2,5] |
+---------+
```

### KLL_QUANTILES.INIT_UINT64

```sql
KLL_QUANTILES.INIT_UINT64(input[, precision[, weight => input_weight]])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64), but accepts
`input` of type `UINT64`.

**Supported Argument Types**

+ `input`: `UINT64`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`BYTES`

### KLL_QUANTILES.INIT_DOUBLE

```sql
KLL_QUANTILES.INIT_DOUBLE(input[, precision[, weight => input_weight]])
```

**Description**

Like [`KLL_QUANTILES.INIT_INT64`](#kll-quantilesinit-int64), but accepts
`input` of type `DOUBLE`.

`KLL_QUANTILES.INIT_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ `input`: `DOUBLE`
+ `precision`: `INT64`
+ `input_weight`: `INT64`

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`BYTES`

### KLL_QUANTILES.MERGE_PARTIAL

```sql
KLL_QUANTILES.MERGE_PARTIAL(sketch)
```

**Description**

Takes KLL16 sketches of the same underlying type and merges them to return a new
sketch of the same underlying type. This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if two or more sketches don't have compatible underlying types,
such as one sketch of `INT64` values and another of
`DOUBLE` values.

Returns an error if one or more inputs are not a valid KLL16 quantiles sketch.

Ignores `NULL` sketches. If the input contains zero rows or only `NULL`
sketches, the function returns `NULL`.

You can initialize sketches with different optional clauses and merge them. For
example, you can initialize a sketch with the `DISTINCT` clause and another
sketch without any optional clauses, and then merge these two sketches.
However, if you initialize sketches with the `DISTINCT` clause and merge them,
the resulting sketch may still contain duplicates.

**Example**

```sql
SELECT KLL_QUANTILES.MERGE_PARTIAL(kll_sketch) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

+-----------------------------------------------------------------------------+
| merged_sketch                                                               |
+-----------------------------------------------------------------------------+
| "\010q\020\n \004\212\007\032\010\200 \020\350\007\032\001\001\"\001\n*     |
| \014\n\n\001\002\003\004\005\006\007\010\t\n"                               |
+-----------------------------------------------------------------------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches into a new sketch, also as
`BYTES`. Both input sketches have the same underlying
data type and precision.

**Supported Argument Types**

`BYTES`

**Return Types**

`BYTES`

### KLL_QUANTILES.MERGE_INT64

```sql
KLL_QUANTILES.MERGE_INT64(sketch, number)
```

**Description**

Takes KLL16 sketches as `BYTES` and merges them into
a new sketch, then
returns the quantiles that divide the input into `number` equal-sized
groups, along with the minimum and maximum values of the input. The output is
an `ARRAY` containing the exact minimum value from
the input data that you used
to initialize the sketches, each approximate quantile, and the exact maximum
value from the initial input data. This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```sql
SELECT KLL_QUANTILES.MERGE_INT64(kll_sketch, 2) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

+---------------+
| merged_sketch |
+---------------+
| [1,5,10]      |
+---------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches and returns an `ARRAY`
containing the minimum,
median, and maximum values in the input sketches.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `INT64`.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`ARRAY` of type `INT64`.

### KLL_QUANTILES.MERGE_UINT64

```sql
KLL_QUANTILES.MERGE_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64), but accepts
`input` of type `UINT64`.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `UINT64`.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`ARRAY` of type `UINT64`.

### KLL_QUANTILES.MERGE_DOUBLE

```sql
KLL_QUANTILES.MERGE_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.MERGE_INT64`](#kll-quantilesmerge-int64), but accepts
`input` of type `DOUBLE`.

`KLL_QUANTILES.MERGE_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

Takes KLL16 sketches as `BYTES`, initialized on data
of type `DOUBLE`.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`ARRAY` of type `DOUBLE`.

### KLL_QUANTILES.MERGE_POINT_INT64

```sql
KLL_QUANTILES.MERGE_POINT_INT64(sketch, phi)
```

**Description**

Takes KLL16 sketches as `BYTES` and merges them, then
extracts a single
quantile from the merged sketch. The `phi` argument specifies the quantile
to return as a fraction of the total number of rows in the input, normalized
between 0 and 1. This means that the function will return a value *v* such that
approximately Φ * *n* inputs are less than or equal to *v*, and a (1-Φ) / *n*
inputs are greater than or equal to *v*. This is an aggregate function.

If the merged sketches were initialized with different precisions, the precision
is downgraded to the lowest precision involved in the merge — except if the
aggregations are small enough to still capture the input exactly — then the
mergee's precision is maintained.

Returns an error if the underlying type of one or more input sketches is not
compatible with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```sql
SELECT KLL_QUANTILES.MERGE_POINT_INT64(kll_sketch, .9) AS merged_sketch
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5)
      UNION ALL
      SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 6 AS x UNION ALL
            SELECT 7 AS x UNION ALL
            SELECT 8 AS x UNION ALL
            SELECT 9 AS x UNION ALL
            SELECT 10 AS x));

+---------------+
| merged_sketch |
+---------------+
|             9 |
+---------------+
```

The query above initializes two KLL16 sketches from five rows of data each. Then
it merges these two sketches and returns the value of the ninth decile or 90th
percentile of the merged sketch.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `INT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`INT64`

### KLL_QUANTILES.MERGE_POINT_UINT64

```sql
KLL_QUANTILES.MERGE_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64), but
accepts `input` of type `UINT64`.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `UINT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`UINT64`

### KLL_QUANTILES.MERGE_POINT_DOUBLE

```sql
KLL_QUANTILES.MERGE_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.MERGE_POINT_INT64`](#kll-quantilesmerge-point-int64), but
accepts `input` of type `DOUBLE`.

`KLL_QUANTILES.MERGE_POINT_DOUBLE` orders values according to the ZetaSQL
[floating point sort order][sort-order]. For example, `NaN` orders before
<code>&#8209;inf</code>.

**Supported Argument Types**

+ Takes KLL16 sketches as `BYTES`, initialized on
  data of type `DOUBLE`.
+ `phi` is a `DOUBLE` between 0 and 1.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[max_min_clause]: #max_min_clause

[analytic-functions]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[floating-point-semantics]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_semantics

<!-- mdlint on -->

**Return Types**

`DOUBLE`

### KLL_QUANTILES.EXTRACT_INT64

```sql
KLL_QUANTILES.EXTRACT_INT64(sketch, number)
```

**Description**

Takes a single KLL16 sketch as `BYTES` and returns a
selected `number`
of quantiles. The output is an `ARRAY` containing the
exact minimum value from
the input data that you used to initialize the sketch, each approximate
quantile, and the exact maximum value from the initial input data. This is a
scalar function, similar to `KLL_QUANTILES.MERGE_INT64`, but scalar rather than
aggregate.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```sql
SELECT KLL_QUANTILES.EXTRACT_INT64(kll_sketch, 2) AS median
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

+---------+
| median  |
+---------+
| [1,3,5] |
+---------+
```

The query above initializes a KLL16 sketch from five rows of data. Then
it returns an `ARRAY` containing the minimum, median,
and maximum values in the input sketch.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `INT64`.

**Return Types**

`ARRAY` of type `INT64`.

### KLL_QUANTILES.EXTRACT_UINT64

```sql
KLL_QUANTILES.EXTRACT_UINT64(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64), but accepts
sketches initialized on data of type of type `UINT64`.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `UINT64`.

**Return Types**

`ARRAY` of type `UINT64`.

### KLL_QUANTILES.EXTRACT_DOUBLE

```sql
KLL_QUANTILES.EXTRACT_DOUBLE(sketch, number)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_INT64`](#kll-quantilesextract-int64), but accepts
sketches initialized on data of type of type `DOUBLE`.

**Supported Argument Types**

Takes a KLL16 sketch as `BYTES` initialized on data
of type `DOUBLE`.

**Return Types**

`ARRAY` of type `DOUBLE`.

### KLL_QUANTILES.EXTRACT_POINT_INT64

```sql
KLL_QUANTILES.EXTRACT_POINT_INT64(sketch, phi)
```

**Description**

Takes a single KLL16 sketch as `BYTES` and returns a
single quantile.
The `phi` argument specifies the quantile to return as a fraction of the total
number of rows in the input, normalized between 0 and 1. This means that the
function will return a value *v* such that approximately Φ * *n* inputs are less
than or equal to *v*, and a (1-Φ) / *n* inputs are greater than or equal to *v*.
This is a scalar function.

Returns an error if the underlying type of the input sketch is not compatible
with type `INT64`.

Returns an error if the input is not a valid KLL16 quantiles sketch.

**Example**

```sql
SELECT KLL_QUANTILES.EXTRACT_POINT_INT64(kll_sketch, .8) AS quintile
FROM (SELECT KLL_QUANTILES.INIT_INT64(x, 1000) AS kll_sketch
      FROM (SELECT 1 AS x UNION ALL
            SELECT 2 AS x UNION ALL
            SELECT 3 AS x UNION ALL
            SELECT 4 AS x UNION ALL
            SELECT 5 AS x));

+----------+
| quintile |
+----------+
|      4   |
+----------+
```

The query above initializes a KLL16 sketch from five rows of data. Then
it returns the value of the eighth decile or 80th percentile of the sketch.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `INT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`INT64`

### KLL_QUANTILES.EXTRACT_POINT_UINT64

```sql
KLL_QUANTILES.EXTRACT_POINT_UINT64(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts sketches initialized on data of type of type
`UINT64`.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `UINT64`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`UINT64`

### KLL_QUANTILES.EXTRACT_POINT_DOUBLE

```sql
KLL_QUANTILES.EXTRACT_POINT_DOUBLE(sketch, phi)
```

**Description**

Like [`KLL_QUANTILES.EXTRACT_POINT_INT64`](#kll-quantilesextract-point-int64),
but accepts sketches initialized on data of type of type
`DOUBLE`.

**Supported Argument Types**

+ Takes a KLL16 sketch as `BYTES`, initialized on
  data of type `DOUBLE`.
+ `phi` is a `DOUBLE` between 0 and 1.

**Return Types**

`DOUBLE`

### About quantiles 
<a id="about_kll_quantiles"></a>

[Quantiles][quantiles] can be defined in two ways. First, for a positive integer *q*,
*q-quantiles* are a set of values that partition an input set into *q* subsets
of nearly equal size; that is, there are *q*-1 of the *q*-quantiles. Some of
these have specific names: the single 2-quantile is the median; the 4-quantiles
are quartiles, the 100-quantiles are percentiles, etc.

To extract a set of *q*-quantiles, use the following functions, where *q* is the
`number` argument:

+ `KLL_QUANTILES.MERGE_INT64`
+ `KLL_QUANTILES.MERGE_UINT64`
+ `KLL_QUANTILES.MERGE_DOUBLE`
+ `KLL_QUANTILES.EXTRACT_INT64`
+ `KLL_QUANTILES.EXTRACT_UINT64`
+ `KLL_QUANTILES.EXTRACT_DOUBLE`

Alternatively, quantiles can be defined as individual *Φ-quantiles*, where Φ is
a real number with 0 <= Φ <= 1. The Φ-quantile *x* is an element of the input
such that a Φ fraction of the input is less than or equal to *x*, and a (1-Φ)
fraction is greater than or equal to *x*. In this notation, the median is the
0.5-quantile, and the 95th percentile is the 0.95-quantile.

To extract individual Φ-quantiles, use the following functions, where Φ is the
`phi` argument:

+ `KLL_QUANTILES.MERGE_POINT_INT64`
+ `KLL_QUANTILES.MERGE_POINT_UINT64`
+ `KLL_QUANTILES.MERGE_POINT_DOUBLE`
+ `KLL_QUANTILES.EXTRACT_POINT_INT64`
+ `KLL_QUANTILES.EXTRACT_POINT_UINT64`
+ `KLL_QUANTILES.EXTRACT_POINT_DOUBLE`

### About the KLL algorithm 
<a id="about_kll_alg"></a>

The [KLL16 algorithm][link-to-kll-paper] improves on the [MP80 algorithm][mp80]
by using variable-size buffers to reduce memory use for large data sets.

### About sketches 
<a id="sketches_kll"></a>

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

[quantiles]: https://en.wikipedia.org/wiki/Quantile

[link-to-kll-paper]: https://arxiv.org/pdf/1603.05346v2.pdf

[mp80]: https://polylogblog.files.wordpress.com/2009/08/80munro-median.pdf

[kll-sketches]: #sketches_kll

[kll-algorithm]: #anon_clamping

[kll-quantiles]: #anon_clamping

[sort-order]: https://github.com/google/zetasql/blob/master/docs/data-types.md#comparison_operator_examples

[approx-functions-reference]: #approximate_aggregate_functions

[aggregate-functions-reference]: #aggregate_functions

<!-- mdlint on -->

## Numbering functions

The following sections describe the numbering functions that ZetaSQL
supports. Numbering functions are a subset of analytic functions. For an
explanation of how analytic functions work, see
[Analytic Function Concepts][analytic-function-concepts]. For a
description of how numbering functions work, see the
[Numbering Function Concepts][numbering-function-concepts].

`OVER` clause requirements:

+ `PARTITION BY`: Optional.
+ `ORDER BY`: Required, except for `ROW_NUMBER()`.
+ `window_frame_clause`: Disallowed.

### RANK

```sql
RANK()
```

**Description**

Returns the ordinal (1-based) rank of each row within the ordered partition.
All peer rows receive the same rank value. The next row or set of peer rows
receives a rank value which increments by the number of peers with the previous
rank value, instead of `DENSE_RANK`, which always increments by 1.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 4           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

### DENSE_RANK

```sql
DENSE_RANK()
```

**Description**

Returns the ordinal (1-based) rank of each row within the window partition.
All peer rows receive the same rank value, and the subsequent rank value is
incremented by one.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  DENSE_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 3           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

### PERCENT_RANK

```sql
PERCENT_RANK()
```

**Description**

Return the percentile rank of a row defined as (RK-1)/(NR-1), where RK is
the `RANK` of the row and NR is the number of rows in the partition.
Returns 0 if NR=1.

**Return Type**

`DOUBLE`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  PERCENT_RANK() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+---------------------+
| name            | finish_time            | division | finish_rank         |
+-----------------+------------------------+----------+---------------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 0                   |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 0.33333333333333331 |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 0.33333333333333331 |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 1                   |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 0                   |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 0.33333333333333331 |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 0.66666666666666663 |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 1                   |
+-----------------+------------------------+----------+---------------------+
```

### CUME_DIST

```sql
CUME_DIST()
```

**Description**

Return the relative rank of a row defined as NP/NR. NP is defined to be the
number of rows that either precede or are peers with the current row. NR is the
number of rows in the partition.

**Return Type**

`DOUBLE`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  CUME_DIST() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 0.25        |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 0.75        |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 0.75        |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 1           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 0.25        |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 0.5         |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 0.75        |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 1           |
+-----------------+------------------------+----------+-------------+
```

### NTILE

```sql
NTILE(constant_integer_expression)
```

**Description**

This function divides the rows into `constant_integer_expression`
buckets based on row ordering and returns the 1-based bucket number that is
assigned to each row. The number of rows in the buckets can differ by at most 1.
The remainder values (the remainder of number of rows divided by buckets) are
distributed one for each bucket, starting with bucket 1. If
`constant_integer_expression` evaluates to NULL, 0 or negative, an
error is provided.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  NTILE(3) OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 1           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 3           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 1           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 2           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 3           |
+-----------------+------------------------+----------+-------------+
```

### ROW_NUMBER

```sql
ROW_NUMBER()
```

**Description**

Does not require the `ORDER BY` clause. Returns the sequential
row ordinal (1-based) of each row for each ordered partition. If the
`ORDER BY` clause is unspecified then the result is
non-deterministic.

**Return Type**

`INT64`

**Example**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 2:59:01', 'F30-34')
SELECT name,
  finish_time,
  division,
  ROW_NUMBER() OVER (PARTITION BY division ORDER BY finish_time ASC) AS finish_rank
FROM finishers;

+-----------------+------------------------+----------+-------------+
| name            | finish_time            | division | finish_rank |
+-----------------+------------------------+----------+-------------+
| Sophia Liu      | 2016-10-18 09:51:45+00 | F30-34   | 1           |
| Meghan Lederer  | 2016-10-18 09:59:01+00 | F30-34   | 2           |
| Nikki Leith     | 2016-10-18 09:59:01+00 | F30-34   | 3           |
| Jen Edwards     | 2016-10-18 10:06:36+00 | F30-34   | 4           |
| Lisa Stelzner   | 2016-10-18 09:54:11+00 | F35-39   | 1           |
| Lauren Matthews | 2016-10-18 10:01:17+00 | F35-39   | 2           |
| Desiree Berry   | 2016-10-18 10:05:42+00 | F35-39   | 3           |
| Suzy Slane      | 2016-10-18 10:06:24+00 | F35-39   | 4           |
+-----------------+------------------------+----------+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[numbering-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#numbering_function_concepts

<!-- mdlint on -->

## Bit functions

ZetaSQL supports the following bit functions.

### BIT_CAST_TO_INT32

```sql
BIT_CAST_TO_INT32(value)
```

**Description**

ZetaSQL supports bit casting to INT32. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

INT32

**Examples**

```sql
SELECT BIT_CAST_TO_UINT32(-1) as UINT32_value, BIT_CAST_TO_INT32(BIT_CAST_TO_UINT32(-1)) as bit_cast_value;

+---------------+----------------------+
| UINT32_value  | bit_cast_value       |
+---------------+----------------------+
| 4294967295    | -1                   |
+---------------+----------------------+
```

### BIT_CAST_TO_INT64

```sql
BIT_CAST_TO_INT64(value)
```

**Description**

ZetaSQL supports bit casting to INT64. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

INT64

**Example**

```sql
SELECT BIT_CAST_TO_UINT64(-1) as UINT64_value, BIT_CAST_TO_INT64(BIT_CAST_TO_UINT64(-1)) as bit_cast_value;

+-----------------------+----------------------+
| UINT64_value          | bit_cast_value       |
+-----------------------+----------------------+
| 18446744073709551615  | -1                   |
+-----------------------+----------------------+
```

### BIT_CAST_TO_UINT32

```sql
BIT_CAST_TO_UINT32(value)
```

**Description**

ZetaSQL supports bit casting to UINT32. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT32`
+ `UINT32`

**Return Data Type**

UINT32

**Examples**

```sql
SELECT -1 as UINT32_value, BIT_CAST_TO_UINT32(-1) as bit_cast_value;

+--------------+----------------------+
| UINT32_value | bit_cast_value       |
+--------------+----------------------+
| -1           | -4294967295          |
+--------------+----------------------+
```

### BIT_CAST_TO_UINT64

```sql
BIT_CAST_TO_UINT64(value)
```

**Description**

ZetaSQL supports bit casting to UINT64. A bit
cast is a cast in which the order of bits is preserved instead of the value
those bytes represent.

The `value` parameter can represent:

+ `INT64`
+ `UINT64`

**Return Data Type**

UINT64

**Example**

```sql
SELECT -1 as INT64_value, BIT_CAST_TO_UINT64(-1) as bit_cast_value;

+--------------+----------------------+
| INT64_value  | bit_cast_value       |
+--------------+----------------------+
| -1           | 18446744073709551615 |
+--------------+----------------------+
```

### BIT_COUNT
```
BIT_COUNT(expression)
```

**Description**

The input, `expression`, must be an
integer or BYTES.

Returns the number of bits that are set in the input `expression`.
For signed integers, this is the number of bits in two's complement form.

**Return Data Type**

INT64

**Example**

```sql
SELECT a, BIT_COUNT(a) AS a_bits, FORMAT("%T", b) as b, BIT_COUNT(b) AS b_bits
FROM UNNEST([
  STRUCT(0 AS a, b'' AS b), (0, b'\x00'), (5, b'\x05'), (8, b'\x00\x08'),
  (0xFFFF, b'\xFF\xFF'), (-2, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE'),
  (-1, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'),
  (NULL, b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
]) AS x;

+-------+--------+---------------------------------------------+--------+
| a     | a_bits | b                                           | b_bits |
+-------+--------+---------------------------------------------+--------+
| 0     | 0      | b""                                         | 0      |
| 0     | 0      | b"\x00"                                     | 0      |
| 5     | 2      | b"\x05"                                     | 2      |
| 8     | 1      | b"\x00\x08"                                 | 1      |
| 65535 | 16     | b"\xff\xff"                                 | 16     |
| -2    | 63     | b"\xff\xff\xff\xff\xff\xff\xff\xfe"         | 63     |
| -1    | 64     | b"\xff\xff\xff\xff\xff\xff\xff\xff"         | 64     |
| NULL  | NULL   | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" | 80     |
+-------+--------+---------------------------------------------+--------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

## Conversion functions

ZetaSQL supports the following conversion functions. These data type
conversions are explicit, but some conversions can happen implicitly. You can
learn more about implicit and explicit conversion [here][conversion-rules].

### CAST overview 
<a id="cast"></a>

<pre class="lang-sql prettyprint">
<code>CAST(expression AS typename [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

Cast syntax is used in a query to indicate that the result type of an
expression should be converted to some other type.

When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. If you want to protect your queries from these types of errors, you
can use [SAFE_CAST][con-func-safecast].

Casts between supported types that do not successfully map from the original
value to the target domain produce runtime errors. For example, casting
BYTES to STRING where the byte sequence is not valid UTF-8 results in a runtime
error.

Other examples include:

+ Casting INT64 to INT32 where the value overflows INT32.
+ Casting STRING to INT32 where the STRING contains non-digit characters.

Some casts can include a [format clause][formatting-syntax], which provides
instructions for how to conduct the
cast. For example, you could
instruct a cast to convert a sequence of bytes to a BASE64-encoded string
instead of a UTF-8-encoded string.

The structure of the format clause is unique to each type of cast and more
information is available in the section for that cast.

**Examples**

The following query results in `"true"` if `x` is `1`, `"false"` for any other
non-`NULL` value, and `NULL` if `x` is `NULL`.

```sql
CAST(x=1 AS STRING)
```

### CAST AS ARRAY

```sql
CAST(expression AS ARRAY<element_type>)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to ARRAY. The `expression`
parameter can represent an expression for these data types:

+ `ARRAY`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>ARRAY</td>
    <td>ARRAY</td>
    <td>
      
      The element types of the input
      <code>ARRAY</code> must be castable to the
      element types of the target <code>ARRAY</code>.
      For example, casting from type
      <code>ARRAY&lt;INT64&gt;</code> to
      <code>ARRAY&lt;DOUBLE&gt;</code> or
      <code>ARRAY&lt;STRING&gt;</code> is valid;
      casting from type <code>ARRAY&lt;INT64&gt;</code>
      to <code>ARRAY&lt;BYTES&gt;</code> is not valid.
      
    </td>
  </tr>
</table>

### CAST AS BIGNUMERIC 
<a id="cast_bignumeric"></a>

```sql
CAST(expression AS BIGNUMERIC)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to BIGNUMERIC. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td>BIGNUMERIC</td>
    <td>
      The floating point number will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">
      half away from zero</a>. Casting a <code>NaN</code>, <code>+inf</code> or
      <code>-inf</code> will return an error. Casting a value outside the range
      of
      
      <a href="data-types#bignumeric_type"><code>BIGNUMERIC</code></a>
      
      will return an overflow error.
    </td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>BIGNUMERIC</td>
    <td>
      The numeric literal contained in the <code>STRING</code> must not exceed
      the maximum precision or range of the
      
      <a href="data-types#bignumeric_type"><code>BIGNUMERIC</code></a>
      
      type, or an error will occur. If the number of digits
      after the decimal point exceeds 38, then the resulting
      <code>BIGNUMERIC</code> value will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">
      half away from zero</a> to have 38 digits after the decimal point.
    </td>
  </tr>
</table>

### CAST AS BOOL

```sql
CAST(expression AS BOOL)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to BOOL. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `BOOL`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Integer</td>
    <td>BOOL</td>
    <td>
      Returns <code>FALSE</code> if <code>x</code> is <code>0</code>,
      <code>TRUE</code> otherwise.
    </td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>BOOL</td>
    <td>
      Returns <code>TRUE</code> if <code>x</code> is <code>"true"</code> and
      <code>FALSE</code> if <code>x</code> is <code>"false"</code><br />
      All other values of <code>x</code> are invalid and throw an error instead
      of casting to BOOL.<br />
      STRINGs are case-insensitive when converting
      to BOOL.
    </td>
  </tr>
</table>

### CAST AS BYTES

<pre class="lang-sql prettyprint">
<code>CAST(expression AS BYTES [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to BYTES. The
`expression` parameter can represent an expression for these data types:

+ `BYTES`
+ `STRING`
+ `PROTO`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as bytes][format-string-as-bytes]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>BYTES</td>
    <td>
      STRINGs are cast to
      BYTES using UTF-8 encoding. For example,
      the STRING "&copy;", when cast to
      BYTES, would become a 2-byte sequence with the
      hex values C2 and A9.
    </td>
  </tr>
  
  <tr>
    <td>PROTO</td>
    <td>BYTES</td>
    <td>
      Returns the proto2 wire format BYTES
      of <code>x</code>.
    </td>
  </tr>
  
</table>

### CAST AS DATE

<pre class="lang-sql prettyprint">
<code>CAST(expression AS DATE [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to DATE. The `expression`
parameter can represent an expression for these data types:

+ `STRING`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>DATE</td>
    <td>
      When casting from string to date, the string must conform to
      the supported date literal format, and is independent of time zone. If the
      string expression is invalid or represents a date that is outside of the
      supported min/max range, then an error is produced.
    </td>
  </tr>
  
  <tr>
    <td>TIMESTAMP</td>
    <td>DATE</td>
    <td>
      Casting from a timestamp to date effectively truncates the timestamp as
      of the default time zone.
    </td>
  </tr>
  
</table>

### CAST AS DATETIME

<pre class="lang-sql prettyprint">
<code>CAST(expression AS DATETIME [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to DATETIME. The
`expression` parameter can represent an expression for these data types:

+ `STRING`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>DATETIME</td>
    <td>
      When casting from string to datetime, the string must conform to the
      supported datetime literal format, and is independent of time zone. If
      the string expression is invalid or represents a datetime that is outside
      of the supported min/max range, then an error is produced.
    </td>
  </tr>
  
  <tr>
    <td>TIMESTAMP</td>
    <td>DATETIME</td>
    <td>
      Casting from a timestamp to datetime effectively truncates the timestamp
      as of the default time zone.
    </td>
  </tr>
  
</table>

### CAST AS ENUM

```sql
CAST(expression AS ENUM)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to ENUM. The `expression`
parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `STRING`
+ `ENUM`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>ENUM</td>
    <td>ENUM</td>
    <td>Must have the same ENUM name.</td>
  </tr>
</table>

### CAST AS Floating Point 
<a id="cast_as_floating_point"></a>

```sql
CAST(expression AS DOUBLE)
```

```sql
CAST(expression AS FLOAT)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to floating point types.
The `expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Integer</td>
    <td>Floating Point</td>
    <td>
      Returns a close but potentially not exact floating point value.
    </td>
  </tr>
  
  <tr>
    <td>NUMERIC</td>
    <td>Floating Point</td>
    <td>
      NUMERIC will convert to the closest floating point number with a possible
      loss of precision.
    </td>
  </tr>
  
  
  <tr>
    <td>BIGNUMERIC</td>
    <td>Floating Point</td>
    <td>
      BIGNUMERIC will convert to the closest floating point number with a
      possible loss of precision.
    </td>
  </tr>
  
  <tr>
    <td>STRING</td>
    <td>Floating Point</td>
    <td>
      Returns <code>x</code> as a floating point value, interpreting it as
      having the same form as a valid floating point literal.
      Also supports casts from <code>"[+,-]inf"</code> to
      <code>[,-]Infinity</code>,
      <code>"[+,-]infinity"</code> to <code>[,-]Infinity</code>, and
      <code>"[+,-]nan"</code> to <code>NaN</code>.
      Conversions are case-insensitive.
    </td>
  </tr>
</table>

### CAST AS Integer 
<a id="cast_as_integer"></a>

```sql
CAST(expression AS INT32)
```

```sql
CAST(expression AS UINT32)
```

```sql
CAST(expression AS INT64)
```

```sql
CAST(expression AS UINT64)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to integer types.
The `expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `ENUM`
+ `BOOL`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  
  <tr>
    <td>
      Floating Point
    </td>
    <td>
      Integer
    </td>
    <td>
      Returns the closest integer value.<br />
      Halfway cases such as 1.5 or -0.5 round away from zero.
    </td>
  </tr>
  <tr>
    <td>BOOL</td>
    <td>Integer</td>
    <td>
      Returns <code>1</code> if <code>x</code> is <code>TRUE</code>,
      <code>0</code> otherwise.
    </td>
  </tr>
  
  <tr>
    <td>STRING</td>
    <td>Integer</td>
    <td>
      A hex string can be cast to an integer. For example,
      <code>0x123</code> to <code>291</code> or <code>-0x123</code> to
      <code>-291</code>.
    </td>
  </tr>
  
</table>

**Examples**

If you are working with hex strings (`0x123`), you can cast those strings as
integers:

```sql
SELECT '0x123' as hex_value, CAST('0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| 0x123     | 291        |
+-----------+------------+
```

```sql
SELECT '-0x123' as hex_value, CAST('-0x123' as INT64) as hex_to_int;

+-----------+------------+
| hex_value | hex_to_int |
+-----------+------------+
| -0x123    | -291       |
+-----------+------------+
```

### CAST AS INTERVAL

<pre class="lang-sql prettyprint">
<code>CAST(expression AS INTERVAL)</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to INTERVAL. The
`expression` parameter can represent an expression for these data types:

+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>INTERVAL</td>
    <td>
      When casting from string to interval, the string must conform to either
      <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO 8601 Duration</a> standard or to interval literal
      format 'Y-M D H:M:S.F'. Partial interval literal formats are also accepted
      when they are not ambiguous, for example 'H:M:S'.
      If the string expression is invalid or represents an interval that is
      outside of the supported min/max range, then an error is produced.
    </td>
  </tr>
</table>

**Examples**

```sql
SELECT input, CAST(input AS INTERVAL) AS output
FROM UNNEST([
  '1-2 3 10:20:30.456',
  '1-2',
  '10:20:30',
  'P1Y2M3D',
  'PT10H20M30,456S'
]) input

+--------------------+--------------------+
| input              | output             |
+--------------------+--------------------+
| 1-2 3 10:20:30.456 | 1-2 3 10:20:30.456 |
| 1-2                | 1-2 0 0:0:0        |
| 10:20:30           | 0-0 0 10:20:30     |
| P1Y2M3D            | 1-2 3 0:0:0        |
| PT10H20M30,456S    | 0-0 0 10:20:30.456 |
+--------------------+--------------------+
```

### CAST AS NUMERIC 
<a id="cast_numeric"></a>

```sql
CAST(expression AS NUMERIC)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to NUMERIC. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `STRING`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td>NUMERIC</td>
    <td>
      The floating point number will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">
      half away from zero</a>. Casting a <code>NaN</code>, <code>+inf</code> or
      <code>-inf</code> will return an error. Casting a value outside the range
      of
      <a href="data-types#numeric_type"><code>NUMERIC</code></a>
      
      will return an overflow error.
    </td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>NUMERIC</td>
    <td>
      The numeric literal contained in the <code>STRING</code> must not exceed
      the maximum precision or range of the
      <a href="data-types#numeric_type"><code>NUMERIC</code></a>
      
      type, or an error will occur. If the number of digits
      after the decimal point exceeds nine, then the resulting
      <code>NUMERIC</code> value will round
      <a href="https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero">
      half away from zero</a> to have nine digits after the decimal point.
    </td>
  </tr>
</table>

### CAST AS PROTO

```sql
CAST(expression AS PROTO)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to PROTO. The `expression`
parameter can represent an expression for these data types:

+ `STRING`
+ `BYTES`
+ `PROTO`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>PROTO</td>
    <td>
      Returns the PROTO that results from parsing
      from proto2 text format.<br />
      Throws an error if parsing fails, e.g. if not all required fields are set.
    </td>
  </tr>
  <tr>
    <td>BYTES</td>
    <td>PROTO</td>
    <td>
      Returns the PROTO that results from parsing
      <code>x</code> from the proto2 wire format.<br />
      Throws an error if parsing fails, e.g. if not all required fields are set.
    </td>
  </tr>
  <tr>
    <td>PROTO</td>
    <td>PROTO</td>
    <td>Must have the same PROTO name.</td>
  </tr>
</table>

**Example**

The example in this section references a protocol buffer called `Award`.

```proto
message Award {
  required int32 year = 1;
  optional int32 month = 2;
  repeated Type type = 3;

  message Type {
    optional string award_name = 1;
    optional string category = 2;
  }
}
```

```sql
SELECT
  CAST(
    '''
    year: 2001
    month: 9
    type { award_name: 'Best Artist' category: 'Artist' }
    type { award_name: 'Best Album' category: 'Album' }
    '''
    AS zetasql.examples.music.Award)
  AS award_col

+---------------------------------------------------------+
| award_col                                               |
+---------------------------------------------------------+
| {                                                       |
|   year: 2001                                            |
|   month: 9                                              |
|   type { award_name: "Best Artist" category: "Artist" } |
|   type { award_name: "Best Album" category: "Album" }   |
| }                                                       |
+---------------------------------------------------------+
```

### CAST AS STRING 
<a id="cast_as_string"></a>

<pre class="lang-sql prettyprint">
<code>CAST(expression AS STRING [<a href="#formatting_syntax">format_clause</a> [AT TIME ZONE timezone_expr]])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to STRING. The
`expression` parameter can represent an expression for these data types:

+ `INT32`
+ `UINT32`
+ `INT64`
+ `UINT64`
+ `FLOAT`
+ `DOUBLE`
+ `NUMERIC`
+ `BIGNUMERIC`
+ `ENUM`
+ `BOOL`
+ `BYTES`
+ `PROTO`
+ `TIME`
+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`
+ `STRING`

**Format clause**

When an expression of one type is cast to another type, you can use the
format clause to provide instructions for how to conduct the cast.
You can use the format clause in this section if `expression` is one of these
data types:

+ `BYTES`
+ `TIME`
+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

The format clause for `STRING` has an additional optional clause called
`AT TIME ZONE timezone_expr`, which you can use to specify a specific time zone
to use during formatting of a `TIMESTAMP`. If this optional clause is not
included when formatting a `TIMESTAMP`, your current time zone is used.

For more information, see the following topics:

+ [Format bytes as string][format-bytes-as-string]
+ [Format date and time as string][format-date-time-as-string]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>Floating Point</td>
    <td>STRING</td>
    <td>Returns an approximate string representation.<br />
    </td>
  </tr>
  <tr>
    <td>BOOL</td>
    <td>STRING</td>
    <td>
      Returns <code>"true"</code> if <code>x</code> is <code>TRUE</code>,
      <code>"false"</code> otherwise.</td>
  </tr>
  <tr>
    <td>BYTES</td>
    <td>STRING</td>
    <td>
      Returns <code>x</code> interpreted as a UTF-8 STRING.<br />
      For example, the BYTES literal
      <code>b'\xc2\xa9'</code>, when cast to STRING,
      is interpreted as UTF-8 and becomes the unicode character "&copy;".<br />
      An error occurs if <code>x</code> is not valid UTF-8.</td>
  </tr>
  
  <tr>
    <td>ENUM</td>
    <td>STRING</td>
    <td>
      Returns the canonical ENUM value name of
      <code>x</code>.<br />
      If an ENUM value has multiple names (aliases),
      the canonical name/alias for that value is used.</td>
  </tr>
  
  
  <tr>
    <td>PROTO</td>
    <td>STRING</td>
    <td>Returns the proto2 text format representation of <code>x</code>.</td>
  </tr>
  
  
  <tr>
    <td>TIME</td>
    <td>STRING</td>
    <td>
      Casting from a time type to a string is independent of time zone and
      is of the form <code>HH:MM:SS</code>.
    </td>
  </tr>
  
  
  <tr>
    <td>DATE</td>
    <td>STRING</td>
    <td>
      Casting from a date type to a string is independent of time zone and is
      of the form <code>YYYY-MM-DD</code>.
    </td>
  </tr>
  
  
  <tr>
    <td>DATETIME</td>
    <td>STRING</td>
    <td>
      Casting from a datetime type to a string is independent of time zone and
      is of the form <code>YYYY-MM-DD HH:MM:SS</code>.
    </td>
  </tr>
  
  
  <tr>
    <td>TIMESTAMP</td>
    <td>STRING</td>
    <td>
      When casting from timestamp types to string, the timestamp is interpreted
      using the default time zone, which is implementation defined. The number of
      subsecond digits produced depends on the number of trailing zeroes in the
      subsecond part: the CAST function will truncate zero, three, or six
      digits.
    </td>
  </tr>
  
  <tr>
    <td>INTERVAL</td>
    <td>STRING</td>
    <td>
      Casting from an interval to a string is of the form
      <code>Y-M D H:M:S</code>.
    </td>
  </tr>
  
</table>

**Examples**

```sql
SELECT CAST(CURRENT_DATE() AS STRING) AS current_date

+---------------+
| current_date  |
+---------------+
| 2021-03-09    |
+---------------+
```

```sql
SELECT CAST(CURRENT_DATE() AS STRING FORMAT 'DAY') AS current_day

+-------------+
| current_day |
+-------------+
| MONDAY      |
+-------------+
```

```sql
SELECT CAST(
  TIMESTAMP '2008-12-25 00:00:00+00:00'
  AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS date_time_to_string

-- Results depend upon where this query was executed.
+------------------------------+
| date_time_to_string          |
+------------------------------+
| 2008-12-24 16:00:00 -08:00   |
+------------------------------+
```

```sql
SELECT CAST(
  TIMESTAMP '2008-12-25 00:00:00+00:00'
  AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM'
  AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string

-- Because the time zone is specified, the result is always the same.
+------------------------------+
| date_time_to_string          |
+------------------------------+
| 2008-12-25 05:30:00 +05:30   |
+------------------------------+
```

### CAST AS STRUCT

```sql
CAST(expression AS STRUCT)
```

**Description**

ZetaSQL supports [casting][con-func-cast] to STRUCT. The `expression`
parameter can represent an expression for these data types:

+ `STRUCT`

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRUCT</td>
    <td>STRUCT</td>
    <td>
      Allowed if the following conditions are met:<br />
      <ol>
        <li>
          The two STRUCTs have the same number of
          fields.
        </li>
        <li>
          The original STRUCT field types can be
          explicitly cast to the corresponding target
          STRUCT field types (as defined by field
          order, not field name).
        </li>
      </ol>
    </td>
  </tr>
</table>

### CAST AS TIME

<pre class="lang-sql prettyprint">
<code>CAST(expression AS TIME [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to TIME. The `expression`
parameter can represent an expression for these data types:

+ `STRING`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>TIME</td>
    <td>
      When casting from string to time, the string must conform to
      the supported time literal format, and is independent of time zone. If the
      string expression is invalid or represents a time that is outside of the
      supported min/max range, then an error is produced.
    </td>
  </tr>
</table>

### CAST AS TIMESTAMP

<pre class="lang-sql prettyprint">
<code>CAST(expression AS TIMESTAMP [<a href="#formatting_syntax">format_clause</a> [AT TIME ZONE timezone_expr]])</code>
</pre>

**Description**

ZetaSQL supports [casting][con-func-cast] to TIMESTAMP. The
`expression` parameter can represent an expression for these data types:

+ `STRING`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Format clause**

When an expression of one type is cast to another type, you can use the
[format clause][formatting-syntax] to provide instructions for how to conduct
the cast. You can use the format clause in this section if `expression` is a
`STRING`.

+ [Format string as date and time][format-string-as-date-time]

The format clause for `TIMESTAMP` has an additional optional clause called
`AT TIME ZONE timezone_expr`, which you can use to specify a specific time zone
to use during formatting. If this optional clause is not included, your
current time zone is used.

**Conversion rules**

<table>
  <tr>
    <th>From</th>
    <th>To</th>
    <th>Rule(s) when casting <code>x</code></th>
  </tr>
  <tr>
    <td>STRING</td>
    <td>TIMESTAMP</td>
    <td>
      When casting from string to a timestamp, <code>string_expression</code>
      must conform to the supported timestamp literal formats, or else a runtime
      error occurs. The <code>string_expression</code> may itself contain a
      time zone.
      <br />
      If there is a time zone in the <code>string_expression</code>, that
      time zone is used for conversion, otherwise the default time zone,
      which is implementation defined, is used. If the string has fewer than six digits,
      then it is implicitly widened.
      <br />
      An error is produced if the <code>string_expression</code> is invalid,
      has more than six subsecond digits (i.e. precision greater than
      microseconds), or represents a time outside of the supported timestamp
      range.
    </td>
  </tr>
  
  <tr>
    <td>DATE</td>
    <td>TIMESTAMP</td>
    <td>
      Casting from a date to a timestamp interprets <code>date_expression</code>
      as of midnight (start of the day) in the default time zone,
      which is implementation defined.
    </td>
  </tr>
  
  
  <tr>
    <td>DATETIME</td>
    <td>TIMESTAMP</td>
    <td>
      Casting from a datetime to a timestamp interprets
      <code>datetime_expression</code> as of midnight (start of the day) in the
      default time zone, which is implementation defined.
    </td>
  </tr>
  
</table>

**Examples**

The following example casts a string-formatted timestamp as a timestamp:

```sql
SELECT CAST("2020-06-02 17:00:53.110+00:00" AS TIMESTAMP) AS as_timestamp

-- Results depend upon where this query was executed.
+----------------------------+
| as_timestamp               |
+----------------------------+
| 2020-06-03 00:00:53.110+00 |
+----------------------------+
```

The following examples cast a string-formatted date and time as a timestamp.
These examples return the same output as the previous example.

```sql
SELECT CAST("06/02/2020 17:00:53.110" AS TIMESTAMP FORMAT 'MM/DD/YYYY HH:MI:SS' AT TIME ZONE 'America/Los_Angeles') AS as_timestamp
```

```sql
SELECT CAST("06/02/2020 17:00:53.110" AS TIMESTAMP FORMAT 'MM/DD/YYYY HH:MI:SS' AT TIME ZONE '00') AS as_timestamp
```

```sql
SELECT CAST('06/02/2020 17:00:53.110 +00' AS TIMESTAMP FORMAT 'YYYY-MM-DD HH:MI:SS TZH') AS as_timestamp
```

### SAFE_CAST 
<a id="safe_casting"></a>

<pre class="lang-sql prettyprint">
<code>SAFE_CAST(expression AS typename [<a href="#formatting_syntax">format_clause</a>])</code>
</pre>

**Description**

When using `CAST`, a query can fail if ZetaSQL is unable to perform
the cast. For example, the following query generates an error:

```sql
SELECT CAST("apple" AS INT64) AS not_a_number;
```

If you want to protect your queries from these types of errors, you can use
`SAFE_CAST`. `SAFE_CAST` is identical to `CAST`, except it returns `NULL`
instead of raising an error.

```sql
SELECT SAFE_CAST("apple" AS INT64) AS not_a_number;

+--------------+
| not_a_number |
+--------------+
| NULL         |
+--------------+
```

If you are casting from bytes to strings, you can also use the
function, `SAFE_CONVERT_BYTES_TO_STRING`. Any invalid UTF-8 characters are
replaced with the unicode replacement character, `U+FFFD`. See
[SAFE_CONVERT_BYTES_TO_STRING][SC_BTS] for more
information.

### Other conversion functions 
<a id="other_conv_functions"></a>

You can learn more about these conversion functions elsewhere in the
documentation:

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

Conversion function                    | From               | To
-------                                | --------           | -------
[ARRAY_TO_STRING][ARRAY_STRING]        | ARRAY              | STRING
[BIT_CAST_TO_INT32][BIT_I32]           | UINT32             | INT32
[BIT_CAST_TO_INT64][BIT_I64]           | UINT64             | INT64
[BIT_CAST_TO_UINT32][BIT_U32]          | INT32              | UINT32
[BIT_CAST_TO_UINT64][BIT_U64]          | INT64              | UINT64
[BOOL][JSON_TO_BOOL]                   | JSON               | BOOL
[DATE][T_DATE]                         | Various data types | DATE
[DATETIME][T_DATETIME]                 | Various data types | DATETIME
[FLOAT64][JSON_TO_DOUBLE]              | JSON               | DOUBLE
[FROM_BASE32][F_B32]                   | STRING             | BYTEs
[FROM_BASE64][F_B64]                   | STRING             | BYTES
[FROM_HEX][F_HEX]                      | STRING             | BYTES
[FROM_PROTO][F_PROTO]                  | PROTO value        | Most data types
[INT64][JSON_TO_INT64]                 | JSON               | INT64
[PARSE_DATE][P_DATE]                   | STRING             | DATE
[PARSE_DATETIME][P_DATETIME]           | STRING             | DATETIME
[PARSE_JSON][P_JSON]                   | STRING             | JSON
[PARSE_TIME][P_TIME]                   | STRING             | TIME
[PARSE_TIMESTAMP][P_TIMESTAMP]         | STRING             | TIMESTAMP
[SAFE_CONVERT_BYTES_TO_STRING][SC_BTS] | BYTES              | STRING
[STRING][STRING_TIMESTAMP]             | TIMESTAMP          | STRING
[STRING][JSON_TO_STRING]               | JSON               | STRING
[TIME][T_TIME]                         | Various data types | TIME
[TIMESTAMP][T_TIMESTAMP]               | Various data types | TIMESTAMP
[TO_BASE32][T_B32]                     | BYTES              | STRING
[TO_BASE64][T_B64]                     | BYTES              | STRING
[TO_HEX][T_HEX]                        | BYTES              | STRING
[TO_JSON][T_JSON]                      | All data types     | JSON
[TO_JSON_STRING][T_JSON_STRING]        | All data types     | STRING
[TO_PROTO][T_PROTO]                    | Most data types    | PROTO value

<!-- mdlint on -->

## Format clause for CAST 
<a id="formatting_syntax"></a>

```sql
format_clause:
  FORMAT format_model

format_model:
  format_string_expression
```

The format clause can be used in some `CAST` functions. You use a format clause
to provide instructions for how to conduct a
cast. For example, you could
instruct a cast to convert a sequence of bytes to a BASE64-encoded string
instead of a UTF-8-encoded string.

The format clause includes a format model. The format model can contain
format elements combined together as a format string.

### Format bytes as string 
<a id="format_bytes_as_string"></a>

```sql
CAST(bytes_expression AS STRING FORMAT format_string_expression)
```

You can cast a sequence of bytes to a string with a format element in the
format string. If the bytes cannot be formatted with a
format element, an error is returned. If the sequence of bytes is `NULL`, the
result is `NULL`. Format elements are case-insensitive.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>HEX</td>
      <td>
        Converts a sequence of bytes into a hexadecimal string.
      </td>
      <td>
        Input: b'\x00\x01\xEF\xFF'<br />
        Output: 0001efff
      </td>
    </tr>
    <tr>
      <td>
        BASEX
      </td>
      <td>
        Converts a sequence of bytes into a
        <a href="#about_basex_encoding">BASEX</a> encoded string.
        X represents one of these numbers: 2, 8, 16, 32, 64.
      </td>
      <td>
        Input as BASE8: b'\x02\x11\x3B'<br />
        Output: 00410473
      </td>
    </tr>
    <tr>
      <td>BASE64M</td>
      <td>
        Converts a sequence of bytes into a
        <a href="#about_basex_encoding">BASE64</a>-encoded string based on
        <a href="https://tools.ietf.org/html/rfc2045#section-6.8">rfc 2045</a>
        for MIME. Generates a newline character ("\n") every 76 characters.
      </td>
      <td>
        Input: b'\xde\xad\xbe\xef'<br />
        Output: 3q2+7w==
      </td>
    </tr>
    <tr>
      <td>ASCII</td>
      <td>
        Converts a sequence of bytes that are ASCII values to a string. If the
        input contains bytes that are not a valid ASCII encoding, an error
        is returned.
      </td>
      <td>
        Input: b'\x48\x65\x6c\x6c\x6f'<br />
        Output: Hello
      </td>
    </tr>
    <tr>
      <td>UTF-8</td>
      <td>
        Converts a sequence of bytes that are UTF-8 values to a string.
        If the input contains bytes that are not a valid UTF-8 encoding,
        an error is returned.
      </td>
      <td>
        Input: b'\x24'<br />
        Output: $
      </td>
    </tr>
    <tr>
      <td>UTF8</td>
      <td>
        Same behavior as UTF-8.
      </td>
      <td>
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Example**

```sql
SELECT CAST(b'\x48\x65\x6c\x6c\x6f' AS STRING FORMAT 'ASCII') AS bytes_to_string;

+-----------------+
| bytes_to_string |
+-----------------+
| Hello           |
+-----------------+
```

### Format string as bytes 
<a id="format_string_as_bytes"></a>

```sql
CAST(string_expression AS BYTES FORMAT format_string_expression)
```

You can cast a string to bytes with a format element in the
format string. If the string cannot be formatted with the
format element, an error is returned. Format elements are case-insensitive.

In the string expression, whitespace characters, such as `\n`, are ignored
if the `BASE64` or `BASE64M` format element is used.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>HEX</td>
      <td>
        Converts a hexadecimal-encoded string to bytes. If the input
        contains characters that are not part of the HEX encoding alphabet
        (0~9, case-insensitive a~f), an error is returned.
      </td>
      <td>
        Input: '0001efff'<br />
        Output: b'\x00\x01\xEF\xFF'
      </td>
    </tr>
    <tr>
      <td>
        BASEX
      </td>
      <td>
        Converts a <a href="#about_basex_encoding">BASEX</a>-encoded string to
        bytes.  X represents one of these numbers: 2, 8, 16, 32, 64. An error
        is returned if the input contains characters that are not part of the
        BASEX encoding alphabet, except whitespace characters if the
        format element is BASE64.
      </td>
      <td>
        Input as BASE8: '00410473'<br />
        Output: b'\x02\x11\x3B'
      </td>
    </tr>
    <tr>
      <td>BASE64M</td>
      <td>
        Converts a <a href="#about_basex_encoding">BASE64</a>-encoded string to
        bytes. If the input contains characters that are not whitespace and not
        part of the BASE64 encoding alphabet defined at
        <a href="https://tools.ietf.org/html/rfc2045#section-6.8">rfc 2045</a>,
        an error is returned. BASE64M and BASE64 decoding have the same
        behavior.
      </td>
      <td>
        Input: '3q2+7w=='<br />
        Output: b'\xde\xad\xbe\xef'
      </td>
    </tr>
    <tr>
      <td>ASCII</td>
      <td>
        Converts a string with only ASCII characters to bytes. If the input
        contains characters that are not ASCII characters, an error is
        returned.
      </td>
      <td>
        Input: 'Hello'<br />
        Output: b'\x48\x65\x6c\x6c\x6f'
      </td>
    </tr>
    <tr>
      <td>UTF-8</td>
      <td>
        Converts a string to a sequence of UTF-8 bytes.
      </td>
      <td>
        Input: '$'<br />
        Output: b'\x24'
      </td>
    </tr>
    <tr>
      <td>UTF8</td>
      <td>
        Same behavior as UTF-8.
      </td>
      <td>
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`BYTES`

**Example**

```sql
SELECT CAST('Hello' AS BYTES FORMAT 'ASCII') AS string_to_bytes

+-------------------------+
| string_to_bytes         |
+-------------------------+
| b'\x48\x65\x6c\x6c\x6f' |
+-------------------------+
```

### Format date and time as string 
<a id="format_date_time_as_string"></a>

You can format these date and time parts as a string:

+ [Format year part as string][format-year-as-string]
+ [Format month part as string][format-month-as-string]
+ [Format day part as string][format-day-as-string]
+ [Format hour part as string][format-hour-as-string]
+ [Format minute part as string][format-minute-as-string]
+ [Format second part as string][format-second-as-string]
+ [Format meridian indicator as string][format-meridian-as-string]
+ [Format time zone as string][format-tz-as-string]
+ [Format literal as string][format-literal-as-string]

Case matching is supported when you format some date or time parts as a string
and the output contains letters. To learn more,
see [Case matching][case-matching-date-time].

#### Case matching 
<a id="case_matching_date_time"></a>

When the output of some format element contains letters, the letter cases of
the output is matched with the letter cases of the format element,
meaning the words in the output are capitalized according to how the
format element is capitalized. This is called case matching. The rules are:

+ If the first two letters of the element are both upper case, the words in
  the output are capitalized. For example `DAY` = `THURSDAY`.
+ If the first letter of the element is upper case, and the second letter is
  lowercase, the first letter of each word in the output is capitalized and
  other letters are lowercase. For example `Day` = `Thursday`.
+ If the first letter of the element is lowercase, then all letters in the
  output are lowercase. For example, `day` = `thursday`.

#### Format year part as string 
<a id="format_year_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the year part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the year
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the year format element.

These data types include a year part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>YYYY</td>
      <td>Year, 4 or more digits.</td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: 2018
        <hr />
        Input: DATE '76-01-30'<br />
        Output: 0076
        <hr />
        Input: DATE '10000-01-30'<br />
        Output: 10000
      </td>
    </tr>
    <tr>
      <td>YYY</td>
      <td>Year, last 3 digits only.</td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: 018
        <hr />
        Input: DATE '98-01-30'<br />
        Output: 098
      </td>
    </tr>
    <tr>
      <td>YY</td>
      <td>Year, last 2 digits only.</td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: 18
        <hr />
        Input: DATE '8-01-30'<br />
        Output: 08
      </td>
    </tr>
    <tr>
      <td>Y</td>
      <td>Year, last digit only.</td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: 8
      </td>
    </tr>
    <tr>
      <td>RRRR</td>
      <td>Same behavior as YYYY.</td>
      <td></td>
    </tr>
    <tr>
      <td>RR</td>
      <td>Same behavior as YY.</td>
      <td></td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Example**

```sql
SELECT CAST(DATE '2018-01-30' AS STRING FORMAT 'YYYY') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 2018                |
+---------------------+
```

#### Format month part as string 
<a id="format_month_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the month part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the month
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the month format element.

These data types include a month part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MM</td>
      <td>Month, 2 digits.</td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: 01
      </td>
    </tr>
    <tr>
      <td>MON</td>
      <td>
        Abbreviated, 3-character name of the month. The abbreviated month names
        for locale en-US are: JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT,
        NOV, DEC. <a href="#case_matching_date_time">Case matching</a> is
        supported.
      </td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: JAN
      </td>
    </tr>
    <tr>
      <td>MONTH</td>
      <td>
        Name of the month.
        <a href="#case_matching_date_time">Case matching</a> is supported.
      </td>
      <td>
        Input: DATE '2018-01-30'<br />
        Output: JANUARY
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Example**

```sql
SELECT CAST(DATE '2018-01-30' AS STRING FORMAT 'MONTH') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| JANUARY             |
+---------------------+
```

#### Format day part as string 
<a id="format_day_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the day part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the day
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the day format element.

These data types include a day part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>DAY</td>
      <td>
        Name of the day of the week, localized. Spaces are padded on the right
        side to make the output size exactly 9.
        <a href="#case_matching_date_time">Case matching</a> is supported.
      </td>
      <td>
        Input: DATE '2020-12-31'<br />
        Output: THURSDAY
      </td>
    </tr>
    <tr>
      <td>DY</td>
      <td>
        Abbreviated, 3-character name of the weekday, localized.
        The abbreviated weekday names for locale en-US are: MON, TUE, WED, THU,
        FRI, SAT, SUN.
        <a href="#case_matching_date_time">Case matching</a> is supported.
      </td>
      <td>
        Input: DATE '2020-12-31'<br />
        Output: THU
      </td>
    </tr>
    <tr>
      <td>D</td>
      <td>Day of the week (1 to 7), starting with Sunday as 1.</td>
      <td>
        Input: DATE '2020-12-31'<br />
        Output: 4
      </td>
    </tr>
    <tr>
      <td>DD</td>
      <td>2-digit day of the month.</td>
      <td>
        Input: DATE '2018-12-02'<br />
        Output: 02
      </td>
    </tr>
    <tr>
      <td>DDD</td>
      <td>3-digit day of the year.</td>
      <td>
        Input: DATE '2018-02-03'<br />
        Output: 034
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Example**

```sql
SELECT CAST(DATE '2018-02-15' AS STRING FORMAT 'DD') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 15                  |
+---------------------+
```

#### Format hour part as string 
<a id="format_hour_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the hour part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the hour
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the hour format element.

These data types include a hour part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>HH</td>
      <td>Hour of the day, 12-hour clock, 2 digits.</td>
      <td>
        Input: TIME '21:30:00'<br />
        Output: 09
      </td>
    </tr>
    <tr>
      <td>HH12</td>
      <td>
        Hour of the day, 12-hour clock.
      </td>
      <td>
        Input: TIME '21:30:00'<br />
        Output: 09
      </td>
    </tr>
    <tr>
      <td>HH24</td>
      <td>
        Hour of the day, 24-hour clock, 2 digits.
      </td>
      <td>
        Input: TIME '21:30:00'<br />
        Output: 21
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Examples**

```sql
SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'HH24') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 21                  |
+---------------------+
```

```sql
SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'HH12') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 09                  |
+---------------------+
```

#### Format minute part as string 
<a id="format_minute_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the minute part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the minute
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the minute format element.

These data types include a minute part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MI</td>
      <td>Minute, 2 digits.</td>
      <td>
        Input: TIME '01:02:03'<br />
        Output: 02
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Example**

```sql
SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'MI') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 30                  |
+---------------------+
```

#### Format second part as string 
<a id="format_second_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the second part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the second
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the second format element.

These data types include a second part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>SS</td>
      <td>Seconds of the minute, 2 digits.</td>
      <td>
        Input: TIME '01:02:03'<br />
        Output: 03
      </td>
    </tr>
    <tr>
      <td>SSSSS</td>
      <td>Seconds of the day, 5 digits.</td>
      <td>
        Input: TIME '01:02:03'<br />
        Output: 03723
      </td>
    </tr>
    <tr>
      <td>FFn</td>
      <td>
        Fractional part of the second, <code>n</code> digits long.
        Replace <code>n</code> with a value from 1 to 9. For example, FF5.
        The fractional part of the second is rounded
        to fit the size of the output.
      </td>
      <td>
        Input for FF1: TIME '01:05:07.16'<br />
        Output: 1
        <hr />
        Input for FF2: TIME '01:05:07.16'<br />
        Output: 16
        <hr />
        Input for FF3: TIME '01:05:07.16'<br />
        Output: 016
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Examples**

```sql
SELECT CAST(TIME '21:30:25.16' AS STRING FORMAT 'SS') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 25                  |
+---------------------+
```

```sql
SELECT CAST(TIME '21:30:25.16' AS STRING FORMAT 'FF2') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| 16                  |
+---------------------+
```

#### Format meridian indicator part as string 
<a id="format_meridian_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the meridian indicator part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the meridian indicator
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the meridian indicator format element.

These data types include a meridian indicator part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>A.M.</td>
      <td>
        A.M. if the time is less than 12, otherwise P.M.
        The letter case of the output is determined by the first letter case
        of the format element.
      </td>
      <td>
        Input for A.M.: TIME '01:02:03'<br />
        Output: A.M.
        <hr />
        Input for A.M.: TIME '16:02:03'<br />
        Output: P.M.
        <hr />
        Input for a.m.: TIME '01:02:03'<br />
        Output: a.m.
        <hr />
        Input for a.M.: TIME '01:02:03'<br />
        Output: a.m.
      </td>
    </tr>
    <tr>
      <td>AM</td>
      <td>
        AM if the time is less than 12, otherwise PM.
        The letter case of the output is determined by the first letter case
        of the format element.
      </td>
      <td>
        Input for AM: TIME '01:02:03'<br />
        Output: AM
        <hr />
        Input for AM: TIME '16:02:03'<br />
        Output: PM
        <hr />
        Input for am: TIME '01:02:03'<br />
        Output: am
        <hr />
        Input for aM: TIME '01:02:03'<br />
        Output: am
      </td>
    </tr>
    <tr>
      <td>P.M.</td>
      <td>Output is the same as A.M. format element.</td>
      <td></td>
    </tr>
    <tr>
      <td>PM</td>
      <td>Output is the same as AM format element.</td>
      <td></td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Examples**

```sql
SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'AM') AS date_time_to_string;
SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'PM') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| PM                  |
+---------------------+
```

```sql
SELECT CAST(TIME '01:30:00' AS STRING FORMAT 'AM') AS date_time_to_string;
SELECT CAST(TIME '01:30:00' AS STRING FORMAT 'PM') AS date_time_to_string;

+---------------------+
| date_time_to_string |
+---------------------+
| AM                  |
+---------------------+
```

#### Format time zone part as string 
<a id="format_tz_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

Casts a data type that contains the time zone part to a string. Includes
format elements, which provide instructions for how to conduct the cast.

+ `expression`: This expression contains the data type with the time zone
  that you need to format.
+ `format_string_expression`: A string which contains format elements, including
  the time zone format element.

These data types include a time zone part:

+ `DATE`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If `expression` or `format_string_expression` is `NULL` the return value is
`NULL`. If `format_string_expression` is an empty string, the output is an
empty string. An error is generated if a value that is not a supported
format element appears in `format_string_expression` or `expression` does not
contain a value specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>TZH</td>
      <td>
        Hour offset for a time zone. This includes the <code>+/-</code> sign and
        2-digit hour.
      </td>
      <td>
        Inputstamp: TIMESTAMP '2008-12-25 05:30:00+00'
        Output: −08
      </td>
    </tr>
    <tr>
      <td>TZM</td>
      <td>
        Minute offset for a time zone. This includes only the 2-digit minute.
      </td>
      <td>
        Inputstamp: TIMESTAMP '2008-12-25 05:30:00+00'
        Output: 00
      </td>
    </tr>
  </tbody>
</table>

**Return type**

`STRING`

**Examples**

```sql
SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH') AS date_time_to_string;

-- Results depend upon where this query was executed.
+---------------------+
| date_time_to_string |
+---------------------+
| -08                 |
+---------------------+
```

```sql
SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH' AT TIME ZONE 'Asia/Kolkata')
AS date_time_to_string;

-- Because the time zone is specified, the result is always the same.
+---------------------+
| date_time_to_string |
+---------------------+
| +05                 |
+---------------------+
```

```sql
SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZM') AS date_time_to_string;

-- Results depend upon where this query was executed.
+---------------------+
| date_time_to_string |
+---------------------+
| 00                  |
+---------------------+
```

```sql
SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZM' AT TIME ZONE 'Asia/Kolkata')
AS date_time_to_string;

-- Because the time zone is specified, the result is always the same.
+---------------------+
| date_time_to_string |
+---------------------+
| 30                  |
+---------------------+
```

#### Format literal as string 
<a id="format_literal_as_string"></a>

```sql
CAST(expression AS STRING FORMAT format_string_expression)
```

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>-</td>
      <td>Output is the same as the input.</td>
      <td>-</td>
    </tr>
    <tr>
      <td>.</td>
      <td>Output is the same as the input.</td>
      <td>.</td>
    </tr>
    <tr>
      <td>/</td>
      <td>Output is the same as the input.</td>
      <td>/</td>
    </tr>
    <tr>
      <td>,</td>
      <td>Output is the same as the input.</td>
      <td>,</td>
    </tr>
    <tr>
      <td>'</td>
      <td>Output is the same as the input.</td>
      <td>'</td>
    </tr>
    <tr>
      <td>;</td>
      <td>Output is the same as the input.</td>
      <td>;</td>
    </tr>
    <tr>
      <td>:</td>
      <td>Output is the same as the input.</td>
      <td>:</td>
    </tr>
    <tr>
      <td>Whitespace</td>
      <td>
        Output is the same as the input.
        Whitespace means the space character, ASCII 32. It does not mean
        other types of space like tab or new line. Any whitespace character
        that is not the ASCII 32 character in the format model generates
        an error.
      </td>
      <td></td>
    </tr>
    <tr>
      <td>"text"</td>
      <td>
        Output is the value within the double quotes. To preserve a double
        quote or backslash character, use the <code>\"</code> or <code>\\</code>
        escape sequence. Other escape sequences are not supported.
      </td>
      <td>
        Input: "abc"<br />
        Output: abc
        <hr />
        Input: "a\"b\\c"<br />
        Output: a"b\c
      </td>
    </tr>
  </tbody>
</table>

### Format string as date and time 
<a id="format_string_as_datetime"></a>

You can format a string with these date and time parts:

+ [Format string as year part][format-string-as-year]
+ [Format string as month part][format-string-as-month]
+ [Format string as day part][format-string-as-day]
+ [Format string as hour part][format-string-as-hour]
+ [Format string as minute part][format-string-as-minute]
+ [Format string as second part][format-string-as-second]
+ [Format string as meridian indicator part][format-string-as-meridian]
+ [Format string as time zone part][format-string-as-tz]
+ [Format string as literal part][format-string-as-literal]

When formatting a string with date and time parts, you must follow the
[format model rules][format-model-rules-date-time].

#### Format model rules 
<a id="format_model_rules_date_time"></a>

When casting a string to date and time parts, you must ensure the _format model_
is valid. The format model represents the elements passed into
`CAST(string_expression AS type FORMAT format_string_expression)` as the
`format_string_expression` and is validated according to the following
rules:

+ It contains at most one of each of the following parts:
  meridian indicator, year, month, day, hour.
+ A non-literal, non-whitespace format element cannot appear more than once.
+ If it contains the day of year format element, `DDD`,  then it cannot contain
  the month.
+ If it contains the 24-hour format element, `HH24`,  then it cannot contain the
  12-hour format element or a meridian indicator.
+ If it contains the 12-hour format element, `HH12` or `HH`,  then it must also
  contain a meridian indicator.
+ If it contains a meridian indicator, then it must also contain a 12-hour
  format element.
+ If it contains the second of the day format element, `SSSSS`,  then it cannot
  contain any of the following: hour, minute, second, or meridian indicator.
+ It cannot contain a format element such that the value it sets does not exist
  in the target type. For example, an hour format element such as `HH24` cannot
  appear in a string you are casting as a `DATE`.

#### Format string as year part 
<a id="format_string_as_year"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted year to a data type that contains
the year part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the year
  that you need to format.
+ `type`: The data type to which you are casting. Must include the year
  part.
+ `format_string_expression`: A string which contains format elements, including
  the year format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a year part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If the `YEAR` part is missing from `string_expression` and the return type
includes this part, `YEAR` is set to the current year.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>YYYY</td>
      <td>
        If it is delimited, matches 1 to 5 digits. If it is not delimited,
        matches 4 digits. Sets the year part to the matched number.
      </td>
      <td>
        Input for MM-DD-YYYY: '03-12-2018'<br />
        Output as DATE: 2018-12-03
        <hr />
        Input for YYYY-MMDD: '10000-1203'<br />
        Output as DATE: 10000-12-03
        <hr />
        Input for YYYY: '18'<br />
        Output as DATE: 2018-03-01 (Assume current date is March 23, 2021)
      </td>
    </tr>
    <tr>
      <td>YYY</td>
      <td>
        Matches 3 digits. Sets the last 3 digits of the year part to the
        matched number.
      </td>
      <td>
        Input for YYY-MM-DD: '018-12-03'<br />
        Output as DATE: 2018-12-03
        <hr />
        Input for YYY-MM-DD: '038-12-03'<br />
        Output as DATE: 2038-12-03
      </td>
    </tr>
    <tr>
      <td>YY</td>
      <td>
        Matches 2 digits. Sets the last 2 digits of the year part to the
        matched number.
      </td>
      <td>
        Input for YY-MM-DD: '18-12-03'<br />
        Output as DATE: 2018-12-03
        <hr />
        Input for YY-MM-DD: '38-12-03'<br />
        Output as DATE: 2038-12-03
      </td>
    </tr>
    <tr>
      <td>Y</td>
      <td>
        Matches 1 digit. Sets the last digit of the year part to the matched
        number.
      </td>
      <td>
        Input for Y-MM-DD: '8-12-03'<br />
        Output as DATE: 2008-12-03
      </td>
    </tr>
    <tr>
      <td>Y,YYY</td>
      <td>
        Matches the pattern of 1 to 2 digits, comma, then exactly 3 digits.
        Sets the year part to the matched number.
      </td>
      <td>
        Input for Y,YYY-MM-DD: '2,018-12-03'<br />
        Output as DATE: 2008-12-03
      </td>
    </tr>
    <tr>
    <tr>
      <td>RRRR</td>
      <td>Same behavior as YYYY.</td>
      <td></td>
    </tr>
    <tr>
      <td>RR</td>
      <td>
        <p>
          Matches 2 digits.
        </p>
        <p>
          If the 2 digits entered are between 00 and 49 and the
          last 2 digits of the current year are between 00 and 49, the
          returned year has the same first 2 digits as the current year.
          If the last 2 digits of the current year are between 50 and 99,
          the first 2 digits of the returned year is 1 greater than the first 2
          digits of the current year.
        </p>
        <p>
          If the 2 digits entered are between 50 and 99 and the
          last 2 digits of the current year are between 00 and 49, the first
          2 digits of the returned year are 1 less than the first 2 digits of
          the current year. If the last 2 digits of the current year are
          between 50 and 99, the returned year has the same first 2 digits
          as the current year.
        </p>
      </td>
      <td>
        Input for RR-MM-DD: '18-12-03'<br />
        Output as DATE: 2018-12-03 (executed in the year 2021)
        Output as DATE: 2118-12-03 (executed in the year 2050)
        <hr />
        Input for RR-MM-DD: '50-12-03'<br />
        Output as DATE: 2050-12-03 (executed in the year 2021)
        Output as DATE: 2050-12-03 (executed in the year 2050)
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('18-12-03' AS DATE FORMAT 'YY-MM-DD') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 2018-02-03          |
+---------------------+
```

#### Format string as month part 
<a id="format_string_as_month"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted month to a data type that contains
the month part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the month
  that you need to format.
+ `type`: The data type to which you are casting. Must include the month
  part.
+ `format_string_expression`: A string which contains format elements, including
  the month format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a month part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If the `MONTH` part is missing from `string_expression` and the return type
includes this part, `MONTH` is set to the current month.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MM</td>
      <td>
        Matches 2 digits. Sets the month part to the matched number.
      </td>
      <td>
        Input for MM-DD-YYYY: '03-12-2018'<br />
        Output as DATE: 2018-12-03
      </td>
    </tr>
    <tr>
      <td>MON</td>
      <td>
        Matches 3 letters. Sets the month part to the matched string interpreted
        as the abbreviated name of the month.
      </td>
      <td>
        Input for MON DD, YYYY: 'DEC 03, 2018'<br />
        Output as DATE: 2018-12-03
      </td>
    </tr>
    <tr>
      <td>MONTH</td>
      <td>
        Matches 9 letters. Sets the month part to the matched string interpreted
        as the name of the month.
      </td>
      <td>
        Input for MONTH DD, YYYY: 'DECEMBER 03, 2018'<br />
        Output as DATE: 2018-12-03
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('DEC 03, 2018' AS DATE FORMAT 'MON DD, YYYY') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 2018-12-03          |
+---------------------+
```

#### Format string as day part 
<a id="format_string_as_day"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted day to a data type that contains
the day part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the day
  that you need to format.
+ `type`: The data type to which you are casting. Must include the day
  part.
+ `format_string_expression`: A string which contains format elements, including
  the day format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a day part:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

If the `DAY` part is missing from `string_expression` and the return type
includes this part, `DAY` is set to `1`.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>DD</td>
      <td>Matches 2 digits. Sets the day part to the matched number.</td>
      <td>
        Input for MONTH DD, YYYY: 'DECEMBER 03, 2018'<br />
        Output as DATE: 2018-12-03
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `DATE`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('DECEMBER 03, 2018' AS DATE FORMAT 'MONTH DD, YYYY') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 2018-12-03          |
+---------------------+
```

#### Format string as hour part 
<a id="format_string_as_hour"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted hour to a data type that contains
the hour part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the hour
  that you need to format.
+ `type`: The data type to which you are casting. Must include the hour
  part.
+ `format_string_expression`: A string which contains format elements, including
  the hour format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a hour part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If the `HOUR` part is missing from `string_expression` and the return type
includes this part, `HOUR` is set to `0`.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>HH</td>
      <td>
        Matches 2 digits. If the matched number <code>n</code> is <code>12</code>,
        sets <code>temp = 0</code>; otherwise, sets <code>temp = n</code>. If
        the matched value of the A.M./P.M. format element is P.M., sets
        <code>temp = n + 12</code>. Sets the hour part to <code>temp</code>.
        A meridian indicator must be present in the format model, when
        HH is present.
      </td>
      <td>
        Input for HH:MI P.M.: '03:30 P.M.'<br />
        Output as TIME: 15:30:00
      </td>
    </tr>
    <tr>
      <td>HH12</td>
      <td>
        Same behavior as HH.
      </td>
      <td></td>
    </tr>
    <tr>
      <td>HH24</td>
      <td>
        Matches 2 digits. Sets the hour part to the matched number.
      </td>
      <td>
        Input for HH24:MI: '15:30'<br />
        Output as TIME: 15:30:00
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('15:30' AS TIME FORMAT 'HH24:MI') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 15:30:00            |
+---------------------+
```

#### Format string as minute part 
<a id="format_string_as_minute"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted minute to a data type that contains
the minute part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the minute
  that you need to format.
+ `type`: The data type to which you are casting. Must include the minute
  part.
+ `format_string_expression`: A string which contains format elements, including
  the minute format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a minute part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If the `MINUTE` part is missing from `string_expression` and the return type
includes this part, `MINUTE` is set to `0`.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MI</td>
      <td>
        Matches 2 digits. Sets the minute part to the matched number.
      </td>
      <td>
        Input for HH:MI P.M.: '03:30 P.M.'<br />
        Output as TIME: 15:30:00
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI P.M.') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 15:30:00            |
+---------------------+
```

#### Format string as second part 
<a id="format_string_as_second"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted second to a data type that contains
the second part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the second
  that you need to format.
+ `type`: The data type to which you are casting. Must include the second
  part.
+ `format_string_expression`: A string which contains format elements, including
  the second format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a second part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

If the `SECOND` part is missing from `string_expression` and the return type
includes this part, `SECOND` is set to `0`.

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>SS</td>
      <td>
        Matches 2 digits. Sets the second part to the matched number.
      </td>
      <td>
        Input for HH:MI:SS P.M.: '03:30:02 P.M.'<br />
        Output as TIME: 15:30:02
      </td>
    </tr>
    <tr>
      <td>SSSSS</td>
      <td>
        Matches 5 digits. Sets the hour, minute and second parts by interpreting
        the matched number as the number of seconds past midnight.
      </td>
      <td>
        Input for SSSSS: '03723'<br />
        Output as TIME: 01:02:03
      </td>
    </tr>
    <tr>
      <td>FFn</td>
      <td>
        Matches <code>n</code> digits, where <code>n</code> is the number
        following FF in the format element. Sets the fractional part of the
        second part to the matched number.
      </td>
      <td>
        Input for HH24:MI:SS.FF1: '01:05:07.16'<br />
        Output as TIME: 01:05:07.2
        <hr />
        Input for HH24:MI:SS.FF2: '01:05:07.16'<br />
        Output as TIME: 01:05:07.16
        <hr />
        Input for HH24:MI:SS.FF3: 'FF3: 01:05:07.16'<br />
        Output as TIME: 01:05:07.160
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('01:05:07.16' AS TIME FORMAT 'HH24:MI:SS.FF1') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 01:05:07.2          |
+---------------------+
```

#### Format string as meridian indicator part 
<a id="format_string_as_meridian"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted meridian indicator to a data type that contains
the meridian indicator part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the meridian indicator
  that you need to format.
+ `type`: The data type to which you are casting. Must include the meridian indicator
  part.
+ `format_string_expression`: A string which contains format elements, including
  the meridian indicator format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a meridian indicator part:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>A.M. or P.M.</td>
      <td>
        Matches using the regular expression <code>'(A|P)\.M\.'</code>.
      </td>
      <td>
        Input for HH:MI A.M.: '03:30 A.M.'<br />
        Output as TIME: 03:30:00
        <hr />
        Input for HH:MI P.M.: '03:30 P.M.'<br />
        Output as TIME: 15:30:00
        <hr />
        Input for HH:MI P.M.: '03:30 A.M.'<br />
        Output as TIME: 03:30:00
        <hr />
        Input for HH:MI A.M.: '03:30 P.M.'<br />
        Output as TIME: 15:30:00
        <hr />
        Input for HH:MI a.m.: '03:30 a.m.'<br />
        Output as TIME: 03:30:00
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('03:30 P.M.' AS TIME FORMAT 'HH:MI A.M.') AS string_to_date_time

+---------------------+
| string_to_date_time |
+---------------------+
| 15:30:00            |
+---------------------+
```

#### Format string as time zone part 
<a id="format_string_as_tz"></a>

```sql
CAST(string_expression AS type FORMAT format_string_expression)
```

Casts a string-formatted time zone to a data type that contains
the time zone part. Includes format elements, which provide instructions for how
to conduct the cast.

+ `string_expression`: This expression contains the string with the time zone
  that you need to format.
+ `type`: The data type to which you are casting. Must include the time zone
  part.
+ `format_string_expression`: A string which contains format elements, including
  the time zone format element. The formats elements in this string are
  defined collectively as the format model, which must follow
  [these rules][format-model-rules-date-time].

These data types include a time zone part:

+ `DATE`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

An error is generated if a value that is not a supported format element appears
in `format_string_expression` or `string_expression` does not contain a value
specified by a format element.

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>TZH</td>
      <td>
        Matches using the regular expression <code>'(\+|\-| )[0-9]{2}'</code>.
        Sets the time zone and hour parts to the matched sign and number.
        Sets the time zone sign to be the first letter of the matched string.
        The number 2 means matching up to 2 digits for non-exact matching, and
        exactly 2 digits for exact matching.
      </td>
      <td>
        Input for YYYY-MM-DD HH:MI:SSTZH: '2008-12-25 05:30:00-08'<br />
        Output as TIMESTAMP: 2008-12-25 05:30:00-08
      </td>
    </tr>
    <tr>
      <td>TZM</td>
      <td>
        Matches 2 digits. Let <code>n</code> be the matched number. If the
        time zone sign is the minus sign, sets the time zone minute part to
        <code>-n</code>. Otherwise, sets the time zone minute part to
        <code>n</code>.
      </td>
      <td>
        Input for YYYY-MM-DD HH:MI:SSTZH: '2008-12-25 05:30:00+05.30'<br />
        Output as TIMESTAMP: 2008-12-25 05:30:00+05.30
      </td>
    </tr>
  </tbody>
</table>

**Return type**

The data type to which the string was cast. This can be:

+ `DATE`
+ `TIME`
+ `DATETIME`
+ `TIMESTAMP`

**Examples**

```sql
SELECT CAST('2020.06.03 00:00:53+00' AS TIMESTAMP FORMAT 'YYYY.MM.DD HH:MI:SSTZH') AS string_to_date_time

+----------------------------+
| as_timestamp               |
+----------------------------+
| 2020-06-03 00:00:53.110+00 |
+----------------------------+
```

#### Format string as literal 
<a id="format_string_as_literal"></a>

```sql
CAST(string_expression AS data_type FORMAT format_string_expression)
```

<table>
  <thead>
    <tr>
      <th width='100px'>Format element</th>
      <th width='400px'>Returns</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>-</td>
      <td>Output is the same as the input.</td>
      <td></td>
    </tr>
    <tr>
      <td>.</td>
      <td>Output is the same as the input.</td>
      <td>.</td>
    </tr>
    <tr>
      <td>/</td>
      <td>Output is the same as the input.</td>
      <td>/</td>
    </tr>
    <tr>
      <td>,</td>
      <td>Output is the same as the input.</td>
      <td>,</td>
    </tr>
    <tr>
      <td>'</td>
      <td>Output is the same as the input.</td>
      <td>'</td>
    </tr>
    <tr>
      <td>;</td>
      <td>Output is the same as the input.</td>
      <td>;</td>
    </tr>
    <tr>
      <td>:</td>
      <td>Output is the same as the input.</td>
      <td>:</td>
    </tr>
    <tr>
      <td>Whitespace</td>
      <td>
        A consecutive sequence of one or more spaces in the format model
        is matched with one or more consecutive Unicode whitespace characters
        in the input. Space means the ASCII 32 space character.
        It does not mean the general whitespace such as a tab or new line.
        Any whitespace character that is not the ASCII 32 character in the
        format model generates an error.
      </td>
      <td></td>
    </tr>
    <tr>
      <td>"text"</td>
      <td>
        Output generated by the format element in formatting, using this
        regular expression, with <code>s</code> representing the string input:
        <code>regex.escape(s)</code>.
      </td>
      <td>
        Input: "abc"<br />
        Output: abc
        <hr />
        Input: "a\"b\\c"<br />
        Output: a"b\c
      </td>
    </tr>
  </tbody>
</table>

### About BASE encoding 
<a id="about_basex_encoding"></a>

BASE encoding translates binary data in string format into a radix-X
representation.

If X is 2, 8, or 16, Arabic numerals 0–9 and the Latin letters
a–z are used in the encoded string. So for example, BASE16/Hexadecimal encoding
results contain 0~9 and a~f).

If X is 32 or 64, the default character tables are defined in
[rfc 4648][rfc-4648]. When you decode a BASE string where X is 2, 8, or 16,
the Latin letters in the input string are case-insensitive. For example, both
"3a" and "3A" are valid input strings for BASE16/Hexadecimal decoding, and
will output the same result.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[formatting-syntax]: #formatting_syntax

[rfc-4648]: https://tools.ietf.org/html/rfc4648#section-3.3

[about-basex-encoding]: #about_basex_encoding

[format-string-as-bytes]: #format_string_as_bytes

[format-bytes-as-string]: #format_bytes_as_string

[format-date-time-as-string]: #format_date_time_as_string

[case-matching-date-time]: #case_matching_date_time

[format-year-as-string]: #format_year_as_string

[format-month-as-string]: #format_month_as_string

[format-day-as-string]: #format_day_as_string

[format-hour-as-string]: #format_hour_as_string

[format-minute-as-string]: #format_minute_as_string

[format-second-as-string]: #format_second_as_string

[format-meridian-as-string]: #format_meridian_as_string

[format-tz-as-string]: #format_tz_as_string

[format-literal-as-string]: #format_literal_as_string

[format-string-as-date-time]: #format_string_as_datetime

[format-model-rules-date-time]: #format_model_rules_date_time

[format-string-as-year]: #format_string_as_year

[format-string-as-month]: #format_string_as_month

[format-string-as-day]: #format_string_as_day

[format-string-as-hour]: #format_string_as_hour

[format-string-as-minute]: #format_string_as_minute

[format-string-as-second]: #format_string_as_second

[format-string-as-meridian]: #format_string_as_meridian

[format-string-as-tz]: #format_string_as_tz

[format-string-as-literal]: #format_string_as_literal

[con-func-cast]: #cast

[con-func-safecast]: #safe_casting

[cast-bignumeric]: #cast_bignumeric

[cast-numeric]: #cast_numeric

[conversion-rules]: https://github.com/google/zetasql/blob/master/docs/conversion_rules.md

[bignumeric-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#decimal_types

[numeric-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#decimal_types

[half-from-zero-wikipedia]: https://en.wikipedia.org/wiki/Rounding#Round_half_away_from_zero

[conversion-rules]: #conversion_rules

[ARRAY_STRING]: #array_to_string

[BIT_I32]: #bit_cast_to_int32

[BIT_U32]: #bit_cast_to_uint32

[BIT_I64]: #bit_cast_to_int64

[BIT_U64]: #bit_cast_to_uint64

[F_B32]: #from_base32

[F_B64]: #from_base64

[F_HEX]: #from_hex

[F_PROTO]: #from_proto

[P_DATE]: #parse_date

[P_DATETIME]: #parse_datetime

[P_JSON]: #parse_json

[P_TIME]: #parse_time

[P_TIMESTAMP]: #parse_timestamp

[SC_BTS]: #safe_convert_bytes_to_string

[STRING_TIMESTAMP]: #string

[T_B32]: #to_base32

[T_B64]: #to_base64

[T_HEX]: #to_hex

[T_JSON]: #to_json

[T_JSON_STRING]: #to_json_string

[T_PROTO]: #to_proto

[T_DATE]: #date

[T_DATETIME]: #datetime

[T_TIMESTAMP]: #timestamp

[T_TIME]: #time

<!-- mdlint on -->

## Mathematical functions

All mathematical functions have the following behaviors:

+  They return `NULL` if any of the input parameters is `NULL`.
+  They return `NaN` if any of the arguments is `NaN`.

### ABS

```
ABS(X)
```

**Description**

Computes absolute value. Returns an error if the argument is an integer and the
output value cannot be represented as the same type; this happens only for the
largest negative input value, which has no positive representation.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ABS(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>25</td>
      <td>25</td>
    </tr>
    <tr>
      <td>-25</td>
      <td>25</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT32</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### SIGN

```
SIGN(X)
```

**Description**

Returns `-1`, `0`, or `+1` for negative, zero and positive arguments
respectively. For floating point arguments, this function does not distinguish
between positive and negative zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SIGN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>25</td>
      <td>+1</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td>-25</td>
      <td>-1</td>
    </tr>
    <tr>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT32</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### IS_INF

```
IS_INF(X)
```

**Description**

Returns `TRUE` if the value is positive or negative infinity.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>IS_INF(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td>25</td>
      <td><code>FALSE</code></td>
    </tr>
  </tbody>
</table>

### IS_NAN

```
IS_NAN(X)
```

**Description**

Returns `TRUE` if the value is a `NaN` value.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>IS_NAN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>NaN</code></td>
      <td><code>TRUE</code></td>
    </tr>
    <tr>
      <td>25</td>
      <td><code>FALSE</code></td>
    </tr>
  </tbody>
</table>

### IEEE_DIVIDE

```
IEEE_DIVIDE(X, Y)
```

**Description**

Divides X by Y; this function never fails. Returns
`DOUBLE` unless
both X and Y are `FLOAT`, in which case it returns
`FLOAT`. Unlike the division operator (/),
this function does not generate errors for division by zero or overflow.</p>

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>IEEE_DIVIDE(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20.0</td>
      <td>4.0</td>
      <td>5.0</td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>25.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>25.0</td>
      <td>0.0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>-25.0</td>
      <td>0.0</td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>0.0</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td>0.0</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### RAND

```
RAND()
```

**Description**

Generates a pseudo-random value of type `DOUBLE` in
the range of [0, 1), inclusive of 0 and exclusive of 1.

### SQRT

```
SQRT(X)
```

**Description**

Computes the square root of X. Generates an error if X is less than 0.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SQRT(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>25.0</code></td>
      <td><code>5.0</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>X &lt; 0</code></td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### POW

```
POW(X, Y)
```

**Description**

Returns the value of X raised to the power of Y. If the result underflows and is
not representable, then the function returns a  value of zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>POW(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>3.0</td>
      <td>8.0</td>
    </tr>
    <tr>
      <td>1.0</td>
      <td>Any value including <code>NaN</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>Any value including <code>NaN</code></td>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>-1.0</td>
      <td><code>+inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>-1.0</td>
      <td><code>-inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td>ABS(X) &lt; 1</td>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>ABS(X) &gt; 1</td>
      <td><code>-inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>ABS(X) &lt; 1</td>
      <td><code>+inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>ABS(X) &gt; 1</td>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Y &lt; 0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Y &gt; 0</td>
      <td><code>-inf</code> if Y is an odd integer, <code>+inf</code> otherwise</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &lt; 0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &gt; 0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>Finite value &lt; 0</td>
      <td>Non-integer</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>0</td>
      <td>Finite value &lt; 0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### POWER

```
POWER(X, Y)
```

**Description**

Synonym of [`POW(X, Y)`](#pow).

### EXP

```
EXP(X)
```

**Description**

Computes *e* to the power of X, also called the natural exponential function. If
the result underflows, this function returns a zero. Generates an error if the
result overflows.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>EXP(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### LN

```
LN(X)
```

**Description**

Computes the natural logarithm of X. Generates an error if X is less than or
equal to zero.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>LN(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>X &lt; 0</code></td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### LOG

```
LOG(X [, Y])
```

**Description**

If only X is present, `LOG` is a synonym of `LN`. If Y is also present,
`LOG` computes the logarithm of X to base Y.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>LOG(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>100.0</td>
      <td>10.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Any value</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>Any value</td>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>0.0 &lt; Y &lt; 1.0</td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Y &gt; 1.0</td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td>X &lt;= 0</td>
      <td>Any value</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>Any value</td>
      <td>Y &lt;= 0</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>Any value</td>
      <td>1.0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### LOG10

```
LOG10(X)
```

**Description**

Similar to `LOG`, but computes logarithm to base 10.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>LOG10(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>100.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt;= 0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### GREATEST

```
GREATEST(X1,...,XN)
```

**Description**

Returns the largest value among X1,...,XN according to the &lt; comparison.
If any parts of X1,...,XN are `NULL`, the return value is `NULL`.

<table>
  <thead>
    <tr>
      <th>X1,...,XN</th>
      <th>GREATEST(X1,...,XN)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>3,5,1</td>
      <td>5</td>
    </tr>
  </tbody>
</table>

**Return Data Types**

Data type of the input values.

### LEAST

```
LEAST(X1,...,XN)
```

**Description**

Returns the smallest value among X1,...,XN according to the &gt; comparison.
If any parts of X1,...,XN are `NULL`, the return value is `NULL`.

<table>
  <thead>
    <tr>
      <th>X1,...,XN</th>
      <th>LEAST(X1,...,XN)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>3,5,1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>

**Return Data Types**

Data type of the input values.

### DIV

```
DIV(X, Y)
```

**Description**

Returns the result of integer division of X by Y. Division by zero returns
an error. Division by -1 may overflow.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>DIV(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>20</td>
      <td>4</td>
      <td>5</td>
    </tr>
    <tr>
      <td>0</td>
      <td>20</td>
      <td>0</td>
    </tr>
    <tr>
      <td>20</td>
      <td>0</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
</tbody>

</table>

### SAFE_DIVIDE

```
SAFE_DIVIDE(X, Y)
```

**Description**

Equivalent to the division operator (<code>X / Y</code>), but returns
<code>NULL</code> if an error occurs, such as a division by zero error.

<table><thead><tr><th>X</th><th>Y</th><th>SAFE_DIVIDE(X, Y)</th></tr></thead><tbody><tr><td>20</td><td>4</td><td>5</td></tr><tr><td>0</td><td>20</td><td><code>0</code></td></tr><tr><td>20</td><td>0</td><td><code>NULL</code></td></tr></tbody></table>

**Return Data Type**

<table style="font-size:small"><thead><tr><th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr></thead><tbody><tr><th>INT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>INT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr></tbody></table>

### SAFE_MULTIPLY

```
SAFE_MULTIPLY(X, Y)
```

**Description**

Equivalent to the multiplication operator (<code>*</code>), but returns
<code>NULL</code> if overflow occurs.

<table><thead><tr><th>X</th><th>Y</th><th>SAFE_MULTIPLY(X, Y)</th></tr></thead><tbody><tr><td>20</td><td>4</td><td>80</td></tr></tbody></table>

**Return Data Type**

<table style="font-size:small"><thead><tr><th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr></thead><tbody><tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr></tbody></table>

### SAFE_NEGATE

```
SAFE_NEGATE(X)
```

**Description**

Equivalent to the unary minus operator (<code>-</code>), but returns
<code>NULL</code> if overflow occurs.

<table><thead><tr><th>X</th><th>SAFE_NEGATE(X)</th></tr></thead><tbody><tr><td>+1</td><td>-1</td></tr><tr><td>-1</td><td>+1</td></tr><tr><td>0</td><td>0</td></tr></tbody></table>

**Return Data Type**

<table><thead><tr><th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr></thead><tbody><tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr></tbody></table>

### SAFE_ADD

```
SAFE_ADD(X, Y)
```

**Description**

Equivalent to the addition operator (<code>+</code>), but returns
<code>NULL</code> if overflow occurs.

<table><thead><tr><th>X</th><th>Y</th><th>SAFE_ADD(X, Y)</th></tr></thead><tbody><tr><td>5</td><td>4</td><td>9</td></tr></tbody></table>

**Return Data Type**

<table style="font-size:small"><thead><tr><th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr></thead><tbody><tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr></tbody></table>

### SAFE_SUBTRACT

```
SAFE_SUBTRACT(X, Y)
```

**Description**

Returns the result of Y subtracted from X.
Equivalent to the subtraction operator (<code>-</code>), but returns
<code>NULL</code> if overflow occurs.

<table><thead><tr><th>X</th><th>Y</th><th>SAFE_SUBTRACT(X, Y)</th></tr></thead><tbody><tr><td>5</td><td>4</td><td>1</td></tr></tbody></table>

**Return Data Type**

<table style="font-size:small"><thead><tr><th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th></tr></thead><tbody><tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr><tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr></tbody></table>

### MOD

```
MOD(X, Y)
```

**Description**

Modulo function: returns the remainder of the division of X by Y. Returned
value has the same sign as X. An error is generated if Y is 0.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>MOD(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>25</td>
      <td>12</td>
      <td>1</td>
    </tr>
    <tr>
      <td>25</td>
      <td>0</td>
      <td>Error</td>
    </tr>
</table>

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td></tr>
</tbody>

</table>

### ROUND

```
ROUND(X [, N])
```

**Description**

If only X is present, `ROUND` rounds X to the nearest integer. If N is present,
`ROUND` rounds X to N decimal places after the decimal point. If N is negative,
`ROUND` will round off digits to the left of the decimal point. Rounds halfway
cases away from zero. Generates an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ROUND(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### TRUNC

```
TRUNC(X [, N])
```

**Description**

If only X is present, `TRUNC` rounds X to the nearest integer whose absolute
value is not greater than the absolute value of X. If N is also present, `TRUNC`
behaves like `ROUND(X, N)`, but always rounds towards zero and never overflows.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TRUNC(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### CEIL

```
CEIL(X)
```

**Description**

Returns the smallest integral value that is not less than X.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>CEIL(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>3.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-2.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### CEILING

```
CEILING(X)
```

**Description**

Synonym of CEIL(X)

### FLOOR

```
FLOOR(X)
```

**Description**

Returns the largest integral value that is not greater than X.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>FLOOR(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.3</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.8</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>2.5</td>
      <td>2.0</td>
    </tr>
    <tr>
      <td>-2.3</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>-2.8</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>-2.5</td>
      <td>-3.0</td>
    </tr>
    <tr>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

**Return Data Type**

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### COS

```
COS(X)
```

**Description**

Computes the cosine of X where X is specified in radians. Never fails.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COS(X)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### COSH

```
COSH(X)
```

**Description**

Computes the hyperbolic cosine of X where X is specified in radians.
Generates an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>COSH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### ACOS

```
ACOS(X)
```

**Description**

Computes the principal value of the inverse cosine of X. The return value is in
the range [0,&pi;]. Generates an error if X is a value outside of the
range [-1, 1].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ACOS(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### ACOSH

```
ACOSH(X)
```

**Description**

Computes the inverse hyperbolic cosine of X. Generates an error if X is a value
less than 1.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ACOSH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### SIN

```
SIN(X)
```

**Description**

Computes the sine of X where X is specified in radians. Never fails.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SIN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### SINH

```
SINH(X)
```

**Description**

Computes the hyperbolic sine of X where X is specified in radians. Generates
an error if overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>SINH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### ASIN

```
ASIN(X)
```

**Description**

Computes the principal value of the inverse sine of X. The return value is in
the range [-&pi;/2,&pi;/2]. Generates an error if X is outside of
the range [-1, 1].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ASIN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### ASINH

```
ASINH(X)
```

**Description**

Computes the inverse hyperbolic sine of X. Does not fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ASINH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### TAN

```
TAN(X)
```

**Description**

Computes the tangent of X where X is specified in radians. Generates an error if
overflow occurs.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TAN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### TANH

```
TANH(X)
```

**Description**

Computes the hyperbolic tangent of X where X is specified in radians. Does not
fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>TANH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td>1.0</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>-1.0</td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### ATAN

```
ATAN(X)
```

**Description**

Computes the principal value of the inverse tangent of X. The return value is
in the range [-&pi;/2,&pi;/2]. Does not fail.

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ATAN(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td>&pi;/2</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>-&pi;/2</td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
  </tbody>
</table>

### ATANH

```
ATANH(X)
```

**Description**

Computes the inverse hyperbolic tangent of X. Generates an error if X is outside
of the range [-1, 1].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>ATANH(X)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>+inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>X &lt; -1</td>
      <td>Error</td>
    </tr>
    <tr>
      <td>X &gt; 1</td>
      <td>Error</td>
    </tr>
  </tbody>
</table>

### ATAN2

```
ATAN2(X, Y)
```

**Description**

Calculates the principal value of the inverse tangent of X/Y using the signs of
the two arguments to determine the quadrant. The return value is in the range
[-&pi;,&pi;].

<table>
  <thead>
    <tr>
      <th>X</th>
      <th>Y</th>
      <th>ATAN2(X, Y)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>NaN</code></td>
      <td>Any value</td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>Any value</td>
      <td><code>NaN</code></td>
      <td><code>NaN</code></td>
    </tr>
    <tr>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>Positive Finite value</td>
      <td><code>-inf</code></td>
      <td>&pi;</td>
    </tr>
    <tr>
      <td>Negative Finite value</td>
      <td><code>-inf</code></td>
      <td>-&pi;</td>
    </tr>
    <tr>
      <td>Finite value</td>
      <td><code>+inf</code></td>
      <td>0.0</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td>Finite value</td>
      <td>&pi;/2</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td>Finite value</td>
      <td>-&pi;/2</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>-inf</code></td>
      <td>&frac34;&pi;</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>-inf</code></td>
      <td>-&frac34;&pi;</td>
    </tr>
    <tr>
      <td><code>+inf</code></td>
      <td><code>+inf</code></td>
      <td>&pi;/4</td>
    </tr>
    <tr>
      <td><code>-inf</code></td>
      <td><code>+inf</code></td>
      <td>-&pi;/4</td>
    </tr>
  </tbody>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

<!-- mdlint on -->

## Navigation functions

The following sections describe the navigation functions that ZetaSQL
supports. Navigation functions are a subset of analytic functions. For an
explanation of how analytic functions work, see [Analytic
Function Concepts][analytic-function-concepts]. For an explanation of how
navigation functions work, see
[Navigation Function Concepts][navigation-function-concepts].

### FIRST_VALUE

```
FIRST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of the `value_expression` for the first row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the fastest time for each division.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', fastest_time) AS fastest_time,
  TIMESTAMP_DIFF(finish_time, fastest_time, SECOND) AS delta_in_seconds
FROM (
  SELECT name,
  finish_time,
  division,
  FIRST_VALUE(finish_time)
    OVER (PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fastest_time
  FROM finishers);

+-----------------+-------------+----------+--------------+------------------+
| name            | finish_time | division | fastest_time | delta_in_seconds |
+-----------------+-------------+----------+--------------+------------------+
| Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | 0                |
| Sophia Liu      | 02:51:45    | F30-34   | 02:51:45     | 0                |
| Nikki Leith     | 02:59:01    | F30-34   | 02:51:45     | 436              |
| Jen Edwards     | 03:06:36    | F30-34   | 02:51:45     | 891              |
| Meghan Lederer  | 03:07:41    | F30-34   | 02:51:45     | 956              |
| Lauren Reasoner | 03:10:14    | F30-34   | 02:51:45     | 1109             |
| Lisa Stelzner   | 02:54:11    | F35-39   | 02:54:11     | 0                |
| Lauren Matthews | 03:01:17    | F35-39   | 02:54:11     | 426              |
| Desiree Berry   | 03:05:42    | F35-39   | 02:54:11     | 691              |
| Suzy Slane      | 03:06:24    | F35-39   | 02:54:11     | 733              |
+-----------------+-------------+----------+--------------+------------------+
```

### LAST_VALUE

```
LAST_VALUE (value_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of the `value_expression` for the last row in the current
window frame.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

`value_expression` can be any data type that an expression can return.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the slowest time for each division.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', slowest_time) AS slowest_time,
  TIMESTAMP_DIFF(slowest_time, finish_time, SECOND) AS delta_in_seconds
FROM (
  SELECT name,
  finish_time,
  division,
  LAST_VALUE(finish_time)
    OVER (PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS slowest_time
  FROM finishers);

+-----------------+-------------+----------+--------------+------------------+
| name            | finish_time | division | slowest_time | delta_in_seconds |
+-----------------+-------------+----------+--------------+------------------+
| Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | 0                |
| Sophia Liu      | 02:51:45    | F30-34   | 03:10:14     | 1109             |
| Nikki Leith     | 02:59:01    | F30-34   | 03:10:14     | 673              |
| Jen Edwards     | 03:06:36    | F30-34   | 03:10:14     | 218              |
| Meghan Lederer  | 03:07:41    | F30-34   | 03:10:14     | 153              |
| Lauren Reasoner | 03:10:14    | F30-34   | 03:10:14     | 0                |
| Lisa Stelzner   | 02:54:11    | F35-39   | 03:06:24     | 733              |
| Lauren Matthews | 03:01:17    | F35-39   | 03:06:24     | 307              |
| Desiree Berry   | 03:05:42    | F35-39   | 03:06:24     | 42               |
| Suzy Slane      | 03:06:24    | F35-39   | 03:06:24     | 0                |
+-----------------+-------------+----------+--------------+------------------+

```

### NTH_VALUE

```
NTH_VALUE (value_expression, constant_integer_expression [{RESPECT | IGNORE} NULLS])
```

**Description**

Returns the value of `value_expression` at the Nth row of the current window
frame, where Nth is defined by `constant_integer_expression`. Returns NULL if
there is no such row.

This function includes `NULL` values in the calculation unless `IGNORE NULLS` is
present. If `IGNORE NULLS` is present, the function excludes `NULL` values from
the calculation.

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `constant_integer_expression` can be any constant expression that returns an
  integer.

**Return Data Type**

Same type as `value_expression`.

**Examples**

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  FORMAT_TIMESTAMP('%X', finish_time) AS finish_time,
  division,
  FORMAT_TIMESTAMP('%X', fastest_time) AS fastest_time,
  FORMAT_TIMESTAMP('%X', second_fastest) AS second_fastest
FROM (
  SELECT name,
  finish_time,
  division,finishers,
  FIRST_VALUE(finish_time)
    OVER w1 AS fastest_time,
  NTH_VALUE(finish_time, 2)
    OVER w1 as second_fastest
  FROM finishers
  WINDOW w1 AS (
    PARTITION BY division ORDER BY finish_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING));

+-----------------+-------------+----------+--------------+----------------+
| name            | finish_time | division | fastest_time | second_fastest |
+-----------------+-------------+----------+--------------+----------------+
| Carly Forte     | 03:08:58    | F25-29   | 03:08:58     | NULL           |
| Sophia Liu      | 02:51:45    | F30-34   | 02:51:45     | 02:59:01       |
| Nikki Leith     | 02:59:01    | F30-34   | 02:51:45     | 02:59:01       |
| Jen Edwards     | 03:06:36    | F30-34   | 02:51:45     | 02:59:01       |
| Meghan Lederer  | 03:07:41    | F30-34   | 02:51:45     | 02:59:01       |
| Lauren Reasoner | 03:10:14    | F30-34   | 02:51:45     | 02:59:01       |
| Lisa Stelzner   | 02:54:11    | F35-39   | 02:54:11     | 03:01:17       |
| Lauren Matthews | 03:01:17    | F35-39   | 02:54:11     | 03:01:17       |
| Desiree Berry   | 03:05:42    | F35-39   | 02:54:11     | 03:01:17       |
| Suzy Slane      | 03:06:24    | F35-39   | 02:54:11     | 03:01:17       |
+-----------------+-------------+----------+--------------+----------------+
```

### LEAD

```
LEAD (value_expression[, offset [, default_expression]])
```

**Description**

Returns the value of the `value_expression` on a subsequent row. Changing the
`offset` value changes which subsequent row is returned; the default value is
`1`, indicating the next row in the window frame. An error occurs if `offset` is
NULL or a negative value.

The optional `default_expression` is used if there isn't a row in the window
frame at the specified offset. This expression must be a constant expression and
its type must be implicitly coercible to the type of `value_expression`. If left
unspecified, `default_expression` defaults to NULL.

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example illustrates a basic use of the `LEAD` function.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS followed_by
FROM finishers;

+-----------------+-------------+----------+-----------------+
| name            | finish_time | division | followed_by     |
+-----------------+-------------+----------+-----------------+
| Carly Forte     | 03:08:58    | F25-29   | NULL            |
| Sophia Liu      | 02:51:45    | F30-34   | Nikki Leith     |
| Nikki Leith     | 02:59:01    | F30-34   | Jen Edwards     |
| Jen Edwards     | 03:06:36    | F30-34   | Meghan Lederer  |
| Meghan Lederer  | 03:07:41    | F30-34   | Lauren Reasoner |
| Lauren Reasoner | 03:10:14    | F30-34   | NULL            |
| Lisa Stelzner   | 02:54:11    | F35-39   | Lauren Matthews |
| Lauren Matthews | 03:01:17    | F35-39   | Desiree Berry   |
| Desiree Berry   | 03:05:42    | F35-39   | Suzy Slane      |
| Suzy Slane      | 03:06:24    | F35-39   | NULL            |
+-----------------+-------------+----------+-----------------+
```

This next example uses the optional `offset` parameter.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers;

+-----------------+-------------+----------+------------------+
| name            | finish_time | division | two_runners_back |
+-----------------+-------------+----------+------------------+
| Carly Forte     | 03:08:58    | F25-29   | NULL             |
| Sophia Liu      | 02:51:45    | F30-34   | Jen Edwards      |
| Nikki Leith     | 02:59:01    | F30-34   | Meghan Lederer   |
| Jen Edwards     | 03:06:36    | F30-34   | Lauren Reasoner  |
| Meghan Lederer  | 03:07:41    | F30-34   | NULL             |
| Lauren Reasoner | 03:10:14    | F30-34   | NULL             |
| Lisa Stelzner   | 02:54:11    | F35-39   | Desiree Berry    |
| Lauren Matthews | 03:01:17    | F35-39   | Suzy Slane       |
| Desiree Berry   | 03:05:42    | F35-39   | NULL             |
| Suzy Slane      | 03:06:24    | F35-39   | NULL             |
+-----------------+-------------+----------+------------------+
```

The following example replaces NULL values with a default value.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LEAD(name, 2, 'Nobody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_back
FROM finishers;

+-----------------+-------------+----------+------------------+
| name            | finish_time | division | two_runners_back |
+-----------------+-------------+----------+------------------+
| Carly Forte     | 03:08:58    | F25-29   | Nobody           |
| Sophia Liu      | 02:51:45    | F30-34   | Jen Edwards      |
| Nikki Leith     | 02:59:01    | F30-34   | Meghan Lederer   |
| Jen Edwards     | 03:06:36    | F30-34   | Lauren Reasoner  |
| Meghan Lederer  | 03:07:41    | F30-34   | Nobody           |
| Lauren Reasoner | 03:10:14    | F30-34   | Nobody           |
| Lisa Stelzner   | 02:54:11    | F35-39   | Desiree Berry    |
| Lauren Matthews | 03:01:17    | F35-39   | Suzy Slane       |
| Desiree Berry   | 03:05:42    | F35-39   | Nobody           |
| Suzy Slane      | 03:06:24    | F35-39   | Nobody           |
+-----------------+-------------+----------+------------------+
```

### LAG

```
LAG (value_expression[, offset [, default_expression]])
```

**Description**

Returns the value of the `value_expression` on a preceding row. Changing the
`offset` value changes which preceding row is returned; the default value is
`1`, indicating the previous row in the window frame. An error occurs if
`offset` is NULL or a negative value.

The optional `default_expression` is used if there isn't a row in the window
frame at the specified offset. This expression must be a constant expression and
its type must be implicitly coercible to the type of `value_expression`. If left
unspecified, `default_expression` defaults to NULL.

**Supported Argument Types**

+ `value_expression` can be any data type that can be returned from an
  expression.
+ `offset` must be a non-negative integer literal or parameter.
+ `default_expression` must be compatible with the value expression type.

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example illustrates a basic use of the `LAG` function.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS preceding_runner
FROM finishers;

+-----------------+-------------+----------+------------------+
| name            | finish_time | division | preceding_runner |
+-----------------+-------------+----------+------------------+
| Carly Forte     | 03:08:58    | F25-29   | NULL             |
| Sophia Liu      | 02:51:45    | F30-34   | NULL             |
| Nikki Leith     | 02:59:01    | F30-34   | Sophia Liu       |
| Jen Edwards     | 03:06:36    | F30-34   | Nikki Leith      |
| Meghan Lederer  | 03:07:41    | F30-34   | Jen Edwards      |
| Lauren Reasoner | 03:10:14    | F30-34   | Meghan Lederer   |
| Lisa Stelzner   | 02:54:11    | F35-39   | NULL             |
| Lauren Matthews | 03:01:17    | F35-39   | Lisa Stelzner    |
| Desiree Berry   | 03:05:42    | F35-39   | Lauren Matthews  |
| Suzy Slane      | 03:06:24    | F35-39   | Desiree Berry    |
+-----------------+-------------+----------+------------------+
```

This next example uses the optional `offset` parameter.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2)
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers;

+-----------------+-------------+----------+-------------------+
| name            | finish_time | division | two_runners_ahead |
+-----------------+-------------+----------+-------------------+
| Carly Forte     | 03:08:58    | F25-29   | NULL              |
| Sophia Liu      | 02:51:45    | F30-34   | NULL              |
| Nikki Leith     | 02:59:01    | F30-34   | NULL              |
| Jen Edwards     | 03:06:36    | F30-34   | Sophia Liu        |
| Meghan Lederer  | 03:07:41    | F30-34   | Nikki Leith       |
| Lauren Reasoner | 03:10:14    | F30-34   | Jen Edwards       |
| Lisa Stelzner   | 02:54:11    | F35-39   | NULL              |
| Lauren Matthews | 03:01:17    | F35-39   | NULL              |
| Desiree Berry   | 03:05:42    | F35-39   | Lisa Stelzner     |
| Suzy Slane      | 03:06:24    | F35-39   | Lauren Matthews   |
+-----------------+-------------+----------+-------------------+
```

The following example replaces NULL values with a default value.

```sql
WITH finishers AS
 (SELECT 'Sophia Liu' as name,
  TIMESTAMP '2016-10-18 2:51:45' as finish_time,
  'F30-34' as division
  UNION ALL SELECT 'Lisa Stelzner', TIMESTAMP '2016-10-18 2:54:11', 'F35-39'
  UNION ALL SELECT 'Nikki Leith', TIMESTAMP '2016-10-18 2:59:01', 'F30-34'
  UNION ALL SELECT 'Lauren Matthews', TIMESTAMP '2016-10-18 3:01:17', 'F35-39'
  UNION ALL SELECT 'Desiree Berry', TIMESTAMP '2016-10-18 3:05:42', 'F35-39'
  UNION ALL SELECT 'Suzy Slane', TIMESTAMP '2016-10-18 3:06:24', 'F35-39'
  UNION ALL SELECT 'Jen Edwards', TIMESTAMP '2016-10-18 3:06:36', 'F30-34'
  UNION ALL SELECT 'Meghan Lederer', TIMESTAMP '2016-10-18 3:07:41', 'F30-34'
  UNION ALL SELECT 'Carly Forte', TIMESTAMP '2016-10-18 3:08:58', 'F25-29'
  UNION ALL SELECT 'Lauren Reasoner', TIMESTAMP '2016-10-18 3:10:14', 'F30-34')
SELECT name,
  finish_time,
  division,
  LAG(name, 2, 'Nobody')
    OVER (PARTITION BY division ORDER BY finish_time ASC) AS two_runners_ahead
FROM finishers;

+-----------------+-------------+----------+-------------------+
| name            | finish_time | division | two_runners_ahead |
+-----------------+-------------+----------+-------------------+
| Carly Forte     | 03:08:58    | F25-29   | Nobody            |
| Sophia Liu      | 02:51:45    | F30-34   | Nobody            |
| Nikki Leith     | 02:59:01    | F30-34   | Nobody            |
| Jen Edwards     | 03:06:36    | F30-34   | Sophia Liu        |
| Meghan Lederer  | 03:07:41    | F30-34   | Nikki Leith       |
| Lauren Reasoner | 03:10:14    | F30-34   | Jen Edwards       |
| Lisa Stelzner   | 02:54:11    | F35-39   | Nobody            |
| Lauren Matthews | 03:01:17    | F35-39   | Nobody            |
| Desiree Berry   | 03:05:42    | F35-39   | Lisa Stelzner     |
| Suzy Slane      | 03:06:24    | F35-39   | Lauren Matthews   |
+-----------------+-------------+----------+-------------------+
```

### PERCENTILE_CONT

```
PERCENTILE_CONT (value_expression, percentile [{RESPECT | IGNORE} NULLS])
```

**Description**

Computes the specified percentile value for the value_expression, with linear
interpolation.

This function ignores NULL values if `RESPECT NULLS` is absent.  If `RESPECT
NULLS` is present:

+ Interpolation between two `NULL` values returns `NULL`.
+ Interpolation between a `NULL` value and a non-`NULL` value returns the
  non-`NULL` value.

**Supported Argument Types**

+ `value_expression` and `percentile` must have one of the following types:
   + `NUMERIC`
   + `BIGNUMERIC`
   + `DOUBLE`
+ `percentile` must be a literal in the range `[0, 1]`.

**Return Data Type**

The return data type is determined by the argument types with the following
table.
<table>

<thead>
<tr>
<th>INPUT</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```
SELECT
  PERCENTILE_CONT(x, 0) OVER() AS min,
  PERCENTILE_CONT(x, 0.01) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5) OVER() AS median,
  PERCENTILE_CONT(x, 0.9) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

+-----+-------------+--------+--------------+-----+
| min | percentile1 | median | percentile90 | max |
+-----+-------------+--------+--------------+-----+
| 0   | 0.03        | 1.5    | 2.7          | 3   |
+-----+-------------+--------+--------------+-----+
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```
SELECT
  PERCENTILE_CONT(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_CONT(x, 0.01 RESPECT NULLS) OVER() AS percentile1,
  PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_CONT(x, 0.9 RESPECT NULLS) OVER() AS percentile90,
  PERCENTILE_CONT(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST([0, 3, NULL, 1, 2]) AS x LIMIT 1;

+------+-------------+--------+--------------+-----+
| min  | percentile1 | median | percentile90 | max |
+------+-------------+--------+--------------+-----+
| NULL | 0           | 1      | 2.6          | 3   |
+------+-------------+--------+--------------+-----+
```

### PERCENTILE_DISC

```
PERCENTILE_DISC (value_expression, percentile [{RESPECT | IGNORE} NULLS])
```

**Description**

Computes the specified percentile value for a discrete `value_expression`. The
returned value is the first sorted value of `value_expression` with cumulative
distribution greater than or equal to the given `percentile` value.

This function ignores `NULL` values unless `RESPECT NULLS` is present.

**Supported Argument Types**

+ `value_expression` can be any orderable type.
+ `percentile` must be a literal in the range `[0, 1]`, with one of the
  following types:
   + `NUMERIC`
   + `BIGNUMERIC`
   + `DOUBLE`

**Return Data Type**

Same type as `value_expression`.

**Examples**

The following example computes the value for some percentiles from a column of
values while ignoring nulls.

```
SELECT
  x,
  PERCENTILE_DISC(x, 0) OVER() AS min,
  PERCENTILE_DISC(x, 0.5) OVER() AS median,
  PERCENTILE_DISC(x, 1) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

+------+-----+--------+-----+
| x    | min | median | max |
+------+-----+--------+-----+
| c    | a   | b      | c   |
| NULL | a   | b      | c   |
| b    | a   | b      | c   |
| a    | a   | b      | c   |
+------+-----+--------+-----+
```

The following example computes the value for some percentiles from a column of
values while respecting nulls.

```
SELECT
  x,
  PERCENTILE_DISC(x, 0 RESPECT NULLS) OVER() AS min,
  PERCENTILE_DISC(x, 0.5 RESPECT NULLS) OVER() AS median,
  PERCENTILE_DISC(x, 1 RESPECT NULLS) OVER() AS max
FROM UNNEST(['c', NULL, 'b', 'a']) AS x;

+------+------+--------+-----+
| x    | min  | median | max |
+------+------+--------+-----+
| c    | NULL | a      | c   |
| NULL | NULL | a      | c   |
| b    | NULL | a      | c   |
| a    | NULL | a      | c   |
+------+------+--------+-----+

```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[navigation-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#navigation_function_concepts

<!-- mdlint on -->

## Aggregate analytic functions

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

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[analytic-function-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md

[aggregate-analytic-concepts]: https://github.com/google/zetasql/blob/master/docs/analytic-function-concepts.md#aggregate_analytic_function_concepts

[analytic-functions-link-to-aggregate-functions]: #aggregate_functions

<!-- mdlint on -->

## Hash functions

### FARM_FINGERPRINT
```
FARM_FINGERPRINT(value)
```

**Description**

Computes the fingerprint of the `STRING` or `BYTES` input using the
`Fingerprint64` function from the
[open-source FarmHash library][hash-link-to-farmhash-github]. The output
of this function for a particular input will never change.

**Return type**

INT64

**Examples**

```sql
WITH example AS (
  SELECT 1 AS x, "foo" AS y, true AS z UNION ALL
  SELECT 2 AS x, "apple" AS y, false AS z UNION ALL
  SELECT 3 AS x, "" AS y, true AS z
)
SELECT
  *,
  FARM_FINGERPRINT(CONCAT(CAST(x AS STRING), y, CAST(z AS STRING)))
    AS row_fingerprint
FROM example;
+---+-------+-------+----------------------+
| x | y     | z     | row_fingerprint      |
+---+-------+-------+----------------------+
| 1 | foo   | true  | -1541654101129638711 |
| 2 | apple | false | 2794438866806483259  |
| 3 |       | true  | -4880158226897771312 |
+---+-------+-------+----------------------+
```

### FINGERPRINT

```
FINGERPRINT(input)
```

**Description**

Computes the fingerprint of the `STRING`
or `BYTES` input using Fingerprint.

**Return type**

UINT64

**Examples**

```sql
SELECT FINGERPRINT("Hello World") as fingerprint;

+----------------------+
| fingerprint          |
+----------------------+
| 4584092443788135411  |
+----------------------+
```

### MD5
```
MD5(input)
```

**Description**

Computes the hash of the input using the
[MD5 algorithm][hash-link-to-md5-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 16 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT MD5("Hello World") as md5;

+-------------------------------------------------+
| md5                                             |
+-------------------------------------------------+
| \xb1\n\x8d\xb1d\xe0uA\x05\xb7\xa9\x9b\xe7.?\xe5 |
+-------------------------------------------------+
```

### SHA1
```
SHA1(input)
```

**Description**

Computes the hash of the input using the
[SHA-1 algorithm][hash-link-to-sha-1-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 20 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA1("Hello World") as sha1;

+-----------------------------------------------------------+
| sha1                                                      |
+-----------------------------------------------------------+
| \nMU\xa8\xd7x\xe5\x02/\xabp\x19w\xc5\xd8@\xbb\xc4\x86\xd0 |
+-----------------------------------------------------------+
```

### SHA256
```
SHA256(input)
```

**Description**

Computes the hash of the input using the
[SHA-256 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 32 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA256("Hello World") as sha256;
```

### SHA512
```
SHA512(input)
```

**Description**

Computes the hash of the input using the
[SHA-512 algorithm][hash-link-to-sha-2-wikipedia]. The input can either be
`STRING` or `BYTES`. The string version treats the input as an array of bytes.

This function returns 64 bytes.

**Return type**

`BYTES`

**Example**

```sql
SELECT SHA512("Hello World") as sha512;
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[hash-link-to-farmhash-github]: https://github.com/google/farmhash

[hash-link-to-md5-wikipedia]: https://en.wikipedia.org/wiki/MD5

[hash-link-to-sha-1-wikipedia]: https://en.wikipedia.org/wiki/SHA-1

[hash-link-to-sha-2-wikipedia]: https://en.wikipedia.org/wiki/SHA-2

<!-- mdlint on -->

## String functions

These string functions work on two different values:
`STRING` and `BYTES` data types. `STRING` values must be well-formed UTF-8.

Functions that return position values, such as [STRPOS][string-link-to-strpos],
encode those positions as `INT64`. The value `1`
refers to the first character (or byte), `2` refers to the second, and so on.
The value `0` indicates an invalid index. When working on `STRING` types, the
returned positions refer to character positions.

All string comparisons are done byte-by-byte, without regard to Unicode
canonical equivalence.

### ASCII

```sql
ASCII(value)
```

**Description**

Returns the ASCII code for the first character or byte in `value`. Returns
`0` if `value` is empty or the ASCII code is `0` for the first character
or byte.

**Return type**

`INT64`

**Examples**

```sql
SELECT ASCII('abcd') as A, ASCII('a') as B, ASCII('') as C, ASCII(NULL) as D;

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| 97    | 97    | 0     | NULL  |
+-------+-------+-------+-------+
```

### BYTE_LENGTH

```sql
BYTE_LENGTH(value)
```

**Description**

Returns the length of the `STRING` or `BYTES` value in `BYTES`,
regardless of whether the type of the value is `STRING` or `BYTES`.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters, b'абвгд' AS bytes)

SELECT
  characters,
  BYTE_LENGTH(characters) AS string_example,
  bytes,
  BYTE_LENGTH(bytes) AS bytes_example
FROM example;

+------------+----------------+-------+---------------+
| characters | string_example | bytes | bytes_example |
+------------+----------------+-------+---------------+
| абвгд      | 10             | абвгд | 10            |
+------------+----------------+-------+---------------+
```

### CHAR_LENGTH

```sql
CHAR_LENGTH(value)
```

**Description**

Returns the length of the `STRING` in characters.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  CHAR_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+
```

### CHARACTER_LENGTH

```sql
CHARACTER_LENGTH(value)
```

**Description**

Synonym for [CHAR_LENGTH][string-link-to-char-length].

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  CHARACTER_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+
```

### CHR

```sql
CHR(value)
```

**Description**

Takes a Unicode [code point][string-link-to-code-points-wikipedia] and returns
the character that matches the code point. Each valid code point should fall
within the range of [0, 0xD7FF] and [0xE000, 0x10FFFF]. Returns an empty string
if the code point is `0`. If an invalid Unicode code point is specified, an
error is returned.

To work with an array of Unicode code points, see
[`CODE_POINTS_TO_STRING`][string-link-to-codepoints-to-string]

**Return type**

`STRING`

**Examples**

```sql
SELECT CHR(65) AS A, CHR(255) AS B, CHR(513) AS C, CHR(1024)  AS D;

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| A     | ÿ     | ȁ     | Ѐ     |
+-------+-------+-------+-------+
```

```sql
SELECT CHR(97) AS A, CHR(0xF9B5) AS B, CHR(0) AS C, CHR(NULL) AS D;

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| a     | 例    |       | NULL  |
+-------+-------+-------+-------+
```

### CODE_POINTS_TO_BYTES

```sql
CODE_POINTS_TO_BYTES(ascii_values)
```

**Description**

Takes an array of extended ASCII
[code points][string-link-to-code-points-wikipedia]
(`ARRAY` of `INT64`) and returns `BYTES`.

To convert from `BYTES` to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`BYTES`

**Examples**

The following is a basic example using `CODE_POINTS_TO_BYTES`.

```sql
SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]) AS bytes;

+----------+
| bytes    |
+----------+
| AbCd     |
+----------+
```

The following example uses a rotate-by-13 places (ROT13) algorithm to encode a
string.

```sql
SELECT CODE_POINTS_TO_BYTES(ARRAY_AGG(
  (SELECT
      CASE
        WHEN chr BETWEEN b'a' and b'z'
          THEN TO_CODE_POINTS(b'a')[offset(0)] +
            MOD(code+13-TO_CODE_POINTS(b'a')[offset(0)],26)
        WHEN chr BETWEEN b'A' and b'Z'
          THEN TO_CODE_POINTS(b'A')[offset(0)] +
            MOD(code+13-TO_CODE_POINTS(b'A')[offset(0)],26)
        ELSE code
      END
   FROM
     (SELECT code, CODE_POINTS_TO_BYTES([code]) chr)
  ) ORDER BY OFFSET)) AS encoded_string
FROM UNNEST(TO_CODE_POINTS(b'Test String!')) code WITH OFFSET;

+------------------+
| encoded_string   |
+------------------+
| Grfg Fgevat!     |
+------------------+
```

### CODE_POINTS_TO_STRING

```sql
CODE_POINTS_TO_STRING(value)
```

**Description**

Takes an array of Unicode [code points][string-link-to-code-points-wikipedia]
(`ARRAY` of `INT64`) and
returns a `STRING`. If a code point is 0, does not return a character for it
in the `STRING`.

To convert from a string to an array of code points, see
[TO_CODE_POINTS][string-link-to-code-points].

**Return type**

`STRING`

**Examples**

The following are basic examples using `CODE_POINTS_TO_STRING`.

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024]) AS string;

+--------+
| string |
+--------+
| AÿȁЀ   |
+--------+
```

```sql
SELECT CODE_POINTS_TO_STRING([97, 0, 0xF9B5]) AS string;

+--------+
| string |
+--------+
| a例    |
+--------+
```

```sql
SELECT CODE_POINTS_TO_STRING([65, 255, NULL, 1024]) AS string;

+--------+
| string |
+--------+
| NULL   |
+--------+
```

The following example computes the frequency of letters in a set of words.

```sql
WITH Words AS (
  SELECT word
  FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama']) AS word
)
SELECT
  CODE_POINTS_TO_STRING([code_point]) AS letter,
  COUNT(*) AS letter_count
FROM Words,
  UNNEST(TO_CODE_POINTS(word)) AS code_point
GROUP BY 1
ORDER BY 2 DESC;

+--------+--------------+
| letter | letter_count |
+--------+--------------+
| a      | 5            |
| f      | 3            |
| r      | 2            |
| b      | 2            |
| l      | 2            |
| o      | 2            |
| g      | 1            |
| z      | 1            |
| e      | 1            |
| m      | 1            |
| i      | 1            |
+--------+--------------+
```

### CONCAT

```sql
CONCAT(value1[, ...])
```

**Description**

Concatenates one or more values into a single result. All values must be
`BYTES` or data types that can be cast to `STRING`.

The function returns `NULL` if any input argument is `NULL`.

Note: You can also use the
[|| concatenation operator][string-link-to-operators] to concatenate
values into a string.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT CONCAT('T.P.', ' ', 'Bar') as author;

+---------------------+
| author              |
+---------------------+
| T.P. Bar            |
+---------------------+
```

```sql
SELECT CONCAT('Summer', ' ', 1923) as release_date;

+---------------------+
| release_date        |
+---------------------+
| Summer 1923         |
+---------------------+
```

```sql

With Employees AS
  (SELECT
    'John' AS first_name,
    'Doe' AS last_name
  UNION ALL
  SELECT
    'Jane' AS first_name,
    'Smith' AS last_name
  UNION ALL
  SELECT
    'Joe' AS first_name,
    'Jackson' AS last_name)

SELECT
  CONCAT(first_name, ' ', last_name)
  AS full_name
FROM Employees;

+---------------------+
| full_name           |
+---------------------+
| John Doe            |
| Jane Smith          |
| Joe Jackson         |
+---------------------+
```

### ENDS_WITH

```sql
ENDS_WITH(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if the second
value is a suffix of the first.

**Return type**

`BOOL`

**Examples**

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  ENDS_WITH(item, 'e') as example
FROM items;

+---------+
| example |
+---------+
|    True |
|   False |
|    True |
+---------+
```

### FORMAT 
<a id="format_string"></a>

```sql
FORMAT(format_string_expression, data_type_expression[, ...])
```

**Description**

`FORMAT` formats a data type expression as a string.

+ `format_string_expression`: Can contain zero or more
  [format specifiers][format-specifiers]. Each format specifier is introduced
  by the `%` symbol, and must map to one or more of the remaining arguments.
  In general, this is a one-to-one mapping, except when the `*` specifier is
  present. For example, `%.*i` maps to two arguments&mdash;a length argument
  and a signed integer argument.  If the number of arguments related to the
  format specifiers is not the same as the number of arguments, an error occurs.
+ `data_type_expression`: The value to format as a string. This can be any
  ZetaSQL data type.

**Return type**

`STRING`

**Examples**

<table>
<tr>
<th>Description</th>
<th>Statement</th>
<th>Result</th>
</tr>
<tr>
<td>Simple integer</td>
<td>FORMAT('%d', 10)</td>
<td>10</td>
</tr>
<tr>
<td>Integer with left blank padding</td>
<td>FORMAT('|%10d|', 11)</td>
<td>|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;11|</td>
</tr>
<tr>
<td>Integer with left zero padding</td>
<td>FORMAT('+%010d+', 12)</td>
<td>+0000000012+</td>
</tr>
<tr>
<td>Integer with commas</td>
<td>FORMAT("%'d", 123456789)</td>
<td>123,456,789</td>
</tr>
<tr>
<td>STRING</td>
<td>FORMAT('-%s-', 'abcd efg')</td>
<td>-abcd efg-</td>
</tr>
<tr>
<td>DOUBLE</td>
<td>FORMAT('%f %E', 1.1, 2.2)</td>
<td>1.100000&nbsp;2.200000E+00</td>
</tr>

<tr>
<td>DATE</td>
<td>FORMAT('%t', date '2015-09-01')</td>
<td>2015-09-01</td>
</tr>

<tr>
<td>TIMESTAMP</td>
<td>FORMAT('%t', timestamp '2015-09-01 12:34:56
America/Los_Angeles')</td>
<td>2015&#8209;09&#8209;01&nbsp;19:34:56+00</td>
</tr>
</table>

The `FORMAT()` function does not provide fully customizable formatting for all
types and values, nor formatting that is sensitive to locale.

If custom formatting is necessary for a type, you must first format it using
type-specific format functions, such as `FORMAT_DATE()` or `FORMAT_TIMESTAMP()`.
For example:

```sql
SELECT FORMAT('date: %s!', FORMAT_DATE('%B %d, %Y', date '2015-01-02'));
```

Returns

```
date: January 02, 2015!
```

#### Supported format specifiers 
<a id="format_specifiers"></a>

```
%[flags][width][.precision]specifier
```

A [format specifier][format-specifier-list] adds formatting when casting a
value to a string. It can optionally contain these sub-specifiers:

+ [Flags][flags]
+ [Width][width]
+ [Precision][precision]

Additional information about format specifiers:

+ [%g and %G behavior][g-and-g-behavior]
+ [%p and %P behavior][p-and-p-behavior]
+ [%t and %T behavior][t-and-t-behavior]
+ [Error conditions][error-format-specifiers]
+ [NULL argument handling][null-format-specifiers]
+ [Additional semantic rules][rules-format-specifiers]

##### Format specifiers 
<a id="format_specifier_list"></a>

<table>
 <tr>
    <td>Specifier</td>
    <td>Description</td>
    <td width="200px">Examples</td>
    <td>Types</td>
 </tr>
 <tr>
    <td><code>d</code> or <code>i</code></td>
    <td>Decimal integer</td>
    <td>392</td>
    <td>
    <span>INT32</span><br><span>INT64</span><br><span>UINT32</span><br><span>UINT64</span><br>
    </td>
 </tr>

 <tr>
    <td><code>u</code></td>
    <td>Unsigned integer</td>
    <td>7235</td>
    <td>
    <span>UINT32</span><br><span>UINT64</span><br>
    </td>
 </tr>

 <tr>
    <td><code>o</code></td>
    <td>Octal</td>
    <td>610</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>x</code></td>
    <td>Hexadecimal integer</td>
    <td>7fa</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>X</code></td>
    <td>Hexadecimal integer (uppercase)</td>
    <td>7FA</td>
    <td>
    <span>INT32</span><br><span>INT64</span><a href="#oxX">*</a><br><span>UINT32</span><br><span>UINT64</span>
    </td>
 </tr>
 <tr>
    <td><code>f</code></td>
    <td>Decimal notation, in [-](integer part).(fractional part) for finite
        values, and in lowercase for non-finite values</td>
    <td>392.650000<br/>
    inf<br/>
    nan</td>
    <td>
    <span> NUMERIC</span><br><span> BIGNUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>F</code></td>
    <td>Decimal notation, in [-](integer part).(fractional part) for finite
        values, and in uppercase for non-finite values</td>
    <td>392.650000<br/>
    INF<br/>
    NAN</td>
    <td>
    <span> NUMERIC</span><br><span> BIGNUMERIC</span><br><span> FLOAT</span><br><span> DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>e</code></td>
    <td>Scientific notation (mantissa/exponent), lowercase</td>
    <td>3.926500e+02<br/>
    inf<br/>
    nan</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>E</code></td>
    <td>Scientific notation (mantissa/exponent), uppercase</td>
    <td>3.926500E+02<br/>
    INF<br/>
    NAN</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>g</code></td>
    <td>Either decimal notation or scientific notation, depending on the input
        value's exponent and the specified precision. Lowercase.
        See <a href="#g_and_g_behavior">%g and %G behavior</a> for details.</td>
    <td>392.65<br/>
      3.9265e+07<br/>
    inf<br/>
    nan</td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>
 <tr>
    <td><code>G</code></td>
    <td>
      Either decimal notation or scientific notation, depending on the input
      value's exponent and the specified precision. Uppercase.
      See <a href="#g_and_g_behavior">%g and %G behavior</a> for details.
    </td>
    <td>
      392.65<br/>
      3.9265E+07<br/>
      INF<br/>
      NAN
    </td>
    <td>
    <span>NUMERIC</span><br><span>BIGNUMERIC</span><br><span>FLOAT</span><br><span>DOUBLE</span>
    </td>
 </tr>

 <tr>
    <td><code>p</code></td>
    <td>
      
      Produces a one-line printable string representing a protocol buffer
      or JSON.
      
      See <a href="#p_and_p_behavior">%p and %P behavior</a>.
    </td>
    <td>
      
<pre>year: 2019 month: 10</pre>
      
      
<pre>{"month":10,"year":2019}</pre>
      
    </td>
    <td>
      PROTO
      <br />
      JSON
    </td>
 </tr>

  <tr>
    <td><code>P</code></td>
    <td>
      
      Produces a multi-line printable string representing a protocol buffer
      or JSON.
      
      See <a href="#p_and_p_behavior">%p and %P behavior</a>.
    </td>
    <td>
      
<pre>
year: 2019
month: 10
</pre>
      
      
<pre>
{
  "month": 10,
  "year": 2019
}
</pre>
      
    </td>
    <td>
      PROTO
      <br />
      JSON
    </td>
  </tr>

 <tr>
    <td><code>s</code></td>
    <td>String of characters</td>
    <td>sample</td>
    <td>STRING</td>
 </tr>
 <tr>
    <td><code>t</code></td>
    <td>
      Returns a printable string representing the value. Often looks
      similar to casting the argument to <code>STRING</code>.
      See <a href="#t_and_t_behavior">%t and %T behavior</a>.
    </td>
    <td>
      sample<br/>
      2014&#8209;01&#8209;01
    </td>
    <td>&lt;any&gt;</td>
 </tr>
 <tr>
    <td><code>T</code></td>
    <td>
      Produces a string that is a valid ZetaSQL constant with a
      similar type to the value's type (maybe wider, or maybe string).
      See <a href="#t_and_t_behavior">%t and %T behavior</a>.
    </td>
    <td>
      'sample'<br/>
      b'bytes&nbsp;sample'<br/>
      1234<br/>
      2.3<br/>
      date&nbsp;'2014&#8209;01&#8209;01'
    </td>
    <td>&lt;any&gt;</td>
 </tr>
 <tr>
    <td><code>%</code></td>
    <td>'%%' produces a single '%'</td>
    <td>%</td>
    <td>n/a</td>
 </tr>
</table>

<a id="oxX"></a><sup>*</sup>The specifiers `%o`, `%x`, and `%X` raise an
error if negative values are used.

The format specifier can optionally contain the sub-specifiers identified above
in the specifier prototype.

These sub-specifiers must comply with the following specifications.

##### Flags 
<a id="flags"></a>

<table>
 <tr>
    <td>Flags</td>
    <td>Description</td>
 </tr>
 <tr>
    <td><code>-</code></td>
    <td>Left-justify within the given field width; Right justification is the
default (see width sub-specifier)</td>
</tr>
 <tr>
    <td><code>+</code></td>
    <td>Forces to precede the result with a plus or minus sign (<code>+</code>
or <code>-</code>) even for positive numbers. By default, only negative numbers
are preceded with a <code>-</code> sign</td>
</tr>
 <tr>
    <td>&lt;space&gt;</td>
    <td>If no sign is going to be written, a blank space is inserted before the
value</td>
</tr>
 <tr>
    <td><code>#</code></td>
    <td><ul>
      <li>For `%o`, `%x`, and `%X`, this flag means to precede the
          value with 0, 0x or 0X respectively for values different than zero.</li>
      <li>For `%f`, `%F`, `%e`, and `%E`, this flag means to add the decimal
          point even when there is no fractional part, unless the value
          is non-finite.</li>
      <li>For `%g` and `%G`, this flag means to add the decimal point even
          when there is no fractional part unless the value is non-finite, and
          never remove the trailing zeros after the decimal point.</li>
      </ul>
   </td>
 </tr>
 <tr>
    <td><code>0</code></td>
    <td>
      Left-pads the number with zeroes (0) instead of spaces when padding is
      specified (see width sub-specifier)</td>
 </tr>
 <tr>
  <td><code>'</code></td>
  <td>
    <p>Formats integers using the appropriating grouping character.
       For example:</p>
    <ul>
      <li><code>FORMAT("%'d", 12345678)</code> returns <code>12,345,678</code></li>
      <li><code>FORMAT("%'x", 12345678)</code> returns <code>bc:614e</code></li>
      <li><code>FORMAT("%'o", 55555)</code> returns <code>15,4403</code></li>
      <p>This flag is only relevant for decimal, hex, and octal values.</p>
    </ul>
  </td>
 </tr>
</table>

Flags may be specified in any order. Duplicate flags are not an error. When
flags are not relevant for some element type, they are ignored.

##### Width 
<a id="width"></a>

<table>
  <tr>
    <td>Width</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>&lt;number&gt;</td>
    <td>
      Minimum number of characters to be printed. If the value to be printed
      is shorter than this number, the result is padded with blank spaces.
      The value is not truncated even if the result is larger
    </td>
  </tr>
  <tr>
    <td><code>*</code></td>
    <td>
      The width is not specified in the format string, but as an additional
      integer value argument preceding the argument that has to be formatted
    </td>
  </tr>
</table>

##### Precision 
<a id="precision"></a>

<table>
 <tr>
    <td>Precision</td>
    <td>Description</td>
 </tr>
 <tr>
    <td><code>.</code>&lt;number&gt;</td>
    <td>
      <ul>
      <li>For integer specifiers `%d`, `%i`, `%o`, `%u`, `%x`, and `%X`:
          precision specifies the
          minimum number of digits to be written. If the value to be written is
          shorter than this number, the result is padded with trailing zeros.
          The value is not truncated even if the result is longer. A precision
          of 0 means that no character is written for the value 0.</li>
      <li>For specifiers `%a`, `%A`, `%e`, `%E`, `%f`, and `%F`: this is the
          number of digits to be printed after the decimal point. The default
          value is 6.</li>
      <li>For specifiers `%g` and `%G`: this is the number of significant digits
          to be printed, before the removal of the trailing zeros after the
          decimal point. The default value is 6.</li>
      </ul>
   </td>
</tr>
 <tr>
    <td><code>.*</code></td>
    <td>
      The precision is not specified in the format string, but as an
      additional integer value argument preceding the argument that has to be
      formatted
   </td>
  </tr>
</table>

##### %g and %G behavior 
<a id="g_and_g_behavior"></a>
The `%g` and `%G` format specifiers choose either the decimal notation (like
the `%f` and `%F` specifiers) or the scientific notation (like the `%e` and `%E`
specifiers), depending on the input value's exponent and the specified
[precision](#precision).

Let p stand for the specified [precision](#precision) (defaults to 6; 1 if the
specified precision is less than 1). The input value is first converted to
scientific notation with precision = (p - 1). If the resulting exponent part x
is less than -4 or no less than p, the scientific notation with precision =
(p - 1) is used; otherwise the decimal notation with precision = (p - 1 - x) is
used.

Unless [`#` flag](#flags) is present, the trailing zeros after the decimal point
are removed, and the decimal point is also removed if there is no digit after
it.

##### %p and %P behavior 
<a id="p_and_p_behavior"></a>

The `%p` format specifier produces a one-line printable string. The `%P`
format specifier produces a multi-line printable string. You can use these
format specifiers with the following data types:

<table>
  <tr>
    <td><strong>Type</strong></td>
    <td><strong>%p</strong></td>
    <td><strong>%P</strong></td>
  </tr>
 
  <tr>
    <td>PROTO</td>
    <td valign="top">
      <p>PROTO input:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
      <p>Produces a one-line printable string representing a protocol buffer:</p>
      <pre>year: 2019 month: 10</pre>
    </td>
    <td valign="top">
      <p>PROTO input:</p>
<pre>
message ReleaseDate {
 required int32 year = 1 [default=2019];
 required int32 month = 2 [default=10];
}</pre>
      <p>Produces a multi-line printable string representing a protocol buffer:</p>
<pre>
year: 2019
month: 10
</pre>
    </td>
  </tr>
 
 
  <tr>
    <td>JSON</td>
    <td valign="top">
      <p>JSON input:</p>
<pre>
JSON '
{
  "month": 10,
  "year": 2019
}
'</pre>
      <p>Produces a one-line printable string representing JSON:</p>
      <pre>{"month":10,"year":2019}</pre>
    </td>
    <td valign="top">
      <p>JSON input:</p>
<pre>
JSON '
{
  "month": 10,
  "year": 2019
}
'</pre>
      <p>Produces a multi-line printable string representing JSON:</p>
<pre>
{
  "month": 10,
  "year": 2019
}
</pre>
    </td>
  </tr>
 
</table>

##### %t and %T behavior 
<a id="t_and_t_behavior"></a>

The `%t` and `%T` format specifiers are defined for all types. The
[width](#width), [precision](#precision), and [flags](#flags) act as they do
for `%s`: the [width](#width) is the minimum width and the `STRING` will be
padded to that size, and [precision](#precision) is the maximum width
of content to show and the `STRING` will be truncated to that size, prior to
padding to width.

The `%t` specifier is always meant to be a readable form of the value.

The `%T` specifier is always a valid SQL literal of a similar type, such as a
wider numeric type.
The literal will not include casts or a type name, except for the special case
of non-finite floating point values.

The `STRING` is formatted as follows:

<table>
  <tr>
    <td><strong>Type</strong></td>
    <td><strong>%t</strong></td>
    <td><strong>%T</strong></td>
  </tr>
  <tr>
    <td><code>NULL</code> of any type</td>
    <td>NULL</td>
    <td>NULL</td>
  </tr>
  <tr>
    <td><span> INT32</span><br><span> INT64</span><br><span> UINT32</span><br><span> UINT64</span><br></td>
    <td>123</td>
    <td>123</td>
  </tr>

  <tr>
    <td>NUMERIC</td>
    <td>123.0  <em>(always with .0)</em>
    <td>NUMERIC "123.0"</td>
  </tr>
  <tr>

    <td>FLOAT, DOUBLE</td>

    <td>
      123.0  <em>(always with .0)</em><br/>
      123e+10<br><code>inf</code><br><code>-inf</code><br><code>NaN</code>
    </td>
    <td>
      123.0  <em>(always with .0)</em><br/>
      123e+10<br/>
      CAST("inf" AS &lt;type&gt;)<br/>
      CAST("-inf" AS &lt;type&gt;)<br/>
      CAST("nan" AS &lt;type&gt;)
    </td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>unquoted string value</td>
    <td>quoted string literal</td>
  </tr>
  <tr>
    <td>BYTES</td>
    <td>
      unquoted escaped bytes<br/>
      e.g. abc\x01\x02
    </td>
    <td>
      quoted bytes literal<br/>
      e.g. b"abc\x01\x02"
    </td>
  </tr>

  <tr>
    <td>ENUM</td>
    <td>EnumName</td>
    <td>"EnumName"</td>
  </tr>
 
 
  <tr>
    <td>DATE</td>
    <td>2011-02-03</td>
    <td>DATE "2011-02-03"</td>
  </tr>
 
 
  <tr>
    <td>TIMESTAMP</td>
    <td>2011-02-03 04:05:06+00</td>
    <td>TIMESTAMP "2011-02-03 04:05:06+00"</td>
  </tr>

 
  <tr>
    <td>INTERVAL</td>
    <td>1-2 3 4:5:6.789</td>
    <td>INTERVAL "1-2 3 4:5:6.789" YEAR TO SECOND</td>
  </tr>
 
 
  <tr>
    <td>PROTO</td>
    <td>
      one-line printable string representing a protocol buffer.
    </td>
    <td>
      quoted string literal with one-line printable string representing a
      protocol buffer.
    </td>
  </tr>
 
  <tr>
    <td>ARRAY</td>
    <td>[value, value, ...]<br/>
    where values are formatted with %t</td>
    <td>[value, value, ...]<br/>
    where values are formatted with %T</td>
  </tr>
 
  <tr>
    <td>STRUCT</td>
    <td>(value, value, ...)<br/>
    where fields are formatted with %t</td>
    <td>(value, value, ...)<br/>
    where fields are formatted with %T<br/>
    <br/>
    Special cases:<br/>
    Zero fields: STRUCT()<br/>
    One field: STRUCT(value)</td>
  </tr>
  
 
  <tr>
    <td>JSON</td>
    <td>
      one-line printable string representing JSON.<br />
      <pre class="lang-json prettyprint">{"name":"apple","stock":3}</pre>
    </td>
    <td>
      one-line printable string representing a JSON literal.<br />
      <pre class="lang-sql prettyprint">JSON '{"name":"apple","stock":3}'</pre>
    </td>
  </tr>
 
</table>

##### Error conditions 
<a id="error_format_specifiers"></a>

If a format specifier is invalid, or is not compatible with the related
argument type, or the wrong number or arguments are provided, then an error is
produced.  For example, the following `<format_string>` expressions are invalid:

```sql
FORMAT('%s', 1)
```

```sql
FORMAT('%')
```

##### NULL argument handling 
<a id="null_format_specifiers"></a>

A `NULL` format string results in a `NULL` output `STRING`. Any other arguments
are ignored in this case.

The function generally produces a `NULL` value if a `NULL` argument is present.
For example, `FORMAT('%i', NULL_expression)` produces a `NULL STRING` as
output.

However, there are some exceptions: if the format specifier is %t or %T
(both of which produce `STRING`s that effectively match CAST and literal value
semantics), a `NULL` value produces 'NULL' (without the quotes) in the result
`STRING`. For example, the function:

```sql
FORMAT('00-%t-00', NULL_expression);
```

Returns

```sql
00-NULL-00
```

##### Additional semantic rules 
<a id="rules_format_specifiers"></a>

`DOUBLE` and
`FLOAT` values can be `+/-inf` or `NaN`.
When an argument has one of those values, the result of the format specifiers
`%f`, `%F`, `%e`, `%E`, `%g`, `%G`, and `%t` are `inf`, `-inf`, or `nan`
(or the same in uppercase) as appropriate.  This is consistent with how
ZetaSQL casts these values to `STRING`.  For `%T`,
ZetaSQL returns quoted strings for
`DOUBLE` values that don't have non-string literal
representations.

### FROM_BASE32

```sql
FROM_BASE32(string_expr)
```

**Description**

Converts the base32-encoded input `string_expr` into `BYTES` format. To convert
`BYTES` to a base32-encoded `STRING`, use [TO_BASE32][string-link-to-base32].

**Return type**

`BYTES`

**Example**

```sql
SELECT FROM_BASE32('MFRGGZDF74======') AS byte_data;

+-----------+
| byte_data |
+-----------+
| abcde\xff |
+-----------+
```

### FROM_BASE64

```sql
FROM_BASE64(string_expr)
```

**Description**

Converts the base64-encoded input `string_expr` into
`BYTES` format. To convert
`BYTES` to a base64-encoded `STRING`,
use [TO_BASE64][string-link-to-base64].

There are several base64 encodings in common use that vary in exactly which
alphabet of 65 ASCII characters are used to encode the 64 digits and padding.
See [RFC 4648](https://tools.ietf.org/html/rfc4648#section-4) for details. This
function expects the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`BYTES`

**Example**

```sql
SELECT FROM_BASE64('/+A=') AS byte_data;

+------------+
| byte_data |
+-----------+
| \377\340  |
+-----------+
```

To work with an encoding using a different base64 alphabet, you might need to
compose `FROM_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To decode a
`base64url`-encoded string, replace `-` and `_` with `+` and `/` respectively.

```sql
SELECT FROM_BASE64(REPLACE(REPLACE('_-A=', '-', '+'), '_', '/')) AS binary;

+-----------+
| binary    |
+-----------+
| \377\340  |
+-----------+
```

### FROM_HEX

```sql
FROM_HEX(string)
```

**Description**

Converts a hexadecimal-encoded `STRING` into `BYTES` format. Returns an error
if the input `STRING` contains characters outside the range
`(0..9, A..F, a..f)`. The lettercase of the characters does not matter. If the
input `STRING` has an odd number of characters, the function acts as if the
input has an additional leading `0`. To convert `BYTES` to a hexadecimal-encoded
`STRING`, use [TO_HEX][string-link-to-to-hex].

**Return type**

`BYTES`

**Example**

```sql
WITH Input AS (
  SELECT '00010203aaeeefff' AS hex_str UNION ALL
  SELECT '0AF' UNION ALL
  SELECT '666f6f626172'
)
SELECT hex_str, FROM_HEX(hex_str) AS bytes_str
FROM Input;

+------------------+----------------------------------+
| hex_str          | bytes_str                        |
+------------------+----------------------------------+
| 0AF              | \x00\xaf                         |
| 00010203aaeeefff | \x00\x01\x02\x03\xaa\xee\xef\xff |
| 666f6f626172     | foobar                           |
+------------------+----------------------------------+
```

### INITCAP

```sql
INITCAP(value[, delimiters])
```

**Description**

Takes a `STRING` and returns it with the first character in each word in
uppercase and all other characters in lowercase. Non-alphabetic characters
remain the same.

`delimiters` is an optional string argument that is used to override the default
set of characters used to separate words. If `delimiters` is not specified, it
defaults to the following characters: \
`<whitespace> [ ] ( ) { } / | \ < > ! ? @ " ^ # $ & ~ _ , . : ; * % + -`

If `value` or `delimiters` is `NULL`, the function returns `NULL`.

**Return type**

`STRING`

**Examples**

```sql
WITH example AS
(
  SELECT 'Hello World-everyone!' AS value UNION ALL
  SELECT 'tHe dog BARKS loudly+friendly' AS value UNION ALL
  SELECT 'apples&oranges;&pears' AS value UNION ALL
  SELECT 'καθίσματα ταινιών' AS value
)
SELECT value, INITCAP(value) AS initcap_value FROM example

+-------------------------------+-------------------------------+
| value                         | initcap_value                 |
+-------------------------------+-------------------------------+
| Hello World-everyone!         | Hello World-Everyone!         |
| tHe dog BARKS loudly+friendly | The Dog Barks Loudly+Friendly |
| apples&oranges;&pears         | Apples&Oranges;&Pears         |
| καθίσματα ταινιών             | Καθίσματα Ταινιών             |
+-------------------------------+-------------------------------+

WITH example AS
(
  SELECT 'hello WORLD!' AS value, '' AS delimiters UNION ALL
  SELECT 'καθίσματα ταιντιώ@ν' AS value, 'τ@' AS delimiters UNION ALL
  SELECT 'Apples1oranges2pears' AS value, '12' AS delimiters UNION ALL
  SELECT 'tHisEisEaESentence' AS value, 'E' AS delimiters
)
SELECT value, delimiters, INITCAP(value, delimiters) AS initcap_value FROM example;

+----------------------+------------+----------------------+
| value                | delimiters | initcap_value        |
+----------------------+------------+----------------------+
| hello WORLD!         |            | Hello world!         |
| καθίσματα ταιντιώ@ν  | τ@         | ΚαθίσματΑ τΑιντΙώ@Ν  |
| Apples1oranges2pears | 12         | Apples1Oranges2Pears |
| tHisEisEaESentence   | E          | ThisEIsEAESentence   |
+----------------------+------------+----------------------+
```

### INSTR

```sql
INSTR(source_value, search_value[, position[, occurrence]])
```

**Description**

Returns the lowest 1-based index of `search_value` in `source_value`. 0 is
returned when no match is found. `source_value` and `search_value` must be the
same type, either `STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`source_value`, otherwise it starts at the beginning of `source_value`. If
`position` is negative, the function searches backwards from the end of
`source_value`, with -1 indicating the last character. `position` cannot be 0.

If `occurrence` is specified, the search returns the position of a specific
instance of `search_value` in `source_value`, otherwise it returns the index of
the first occurrence. If `occurrence` is greater than the number of matches
found, 0 is returned. For `occurrence` > 1, the function searches for
overlapping occurrences, in other words, the function searches for additional
occurrences beginning with the second character in the previous occurrence.
`occurrence` cannot be 0 or negative.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS
(SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 2 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 1 as position, 3 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, 3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, -1 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'an' as search_value, -3 as position, 1 as
occurrence UNION ALL
SELECT 'banana' as source_value, 'ann' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 1 as
occurrence UNION ALL
SELECT 'helloooo' as source_value, 'oo' as search_value, 1 as position, 2 as
occurrence
)
SELECT source_value, search_value, position, occurrence, INSTR(source_value,
search_value, position, occurrence) AS instr
FROM example;

+--------------+--------------+----------+------------+-------+
| source_value | search_value | position | occurrence | instr |
+--------------+--------------+----------+------------+-------+
| banana       | an           | 1        | 1          | 2     |
| banana       | an           | 1        | 2          | 4     |
| banana       | an           | 1        | 3          | 0     |
| banana       | an           | 3        | 1          | 4     |
| banana       | an           | -1       | 1          | 4     |
| banana       | an           | -3       | 1          | 4     |
| banana       | ann          | 1        | 1          | 0     |
| helloooo     | oo           | 1        | 1          | 5     |
| helloooo     | oo           | 1        | 2          | 6     |
+--------------+--------------+----------+------------+-------+
```

### LEFT

```sql
LEFT(value, length)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of the specified
number of leftmost characters or bytes from `value`. The `length` is an
`INT64` that specifies the length of the returned
value. If `value` is of type `BYTES`, `length` is the number of leftmost bytes
to return. If `value` is `STRING`, `length` is the number of leftmost characters
to return.

If `length` is 0, an empty `STRING` or `BYTES` value will be
returned. If `length` is negative, an error will be returned. If `length`
exceeds the number of characters or bytes from `value`, the original `value`
will be returned.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH examples AS
(SELECT 'apple' as example
UNION ALL
SELECT 'banana' as example
UNION ALL
SELECT 'абвгд' as example
)
SELECT example, LEFT(example, 3) AS left_example
FROM examples;

+---------+--------------+
| example | left_example |
+---------+--------------+
| apple   | app          |
| banana  | ban          |
| абвгд   | абв          |
+---------+--------------+
```

```sql
WITH examples AS
(SELECT b'apple' as example
UNION ALL
SELECT b'banana' as example
UNION ALL
SELECT b'\xab\xcd\xef\xaa\xbb' as example
)
SELECT example, LEFT(example, 3) AS left_example
FROM examples;

+----------------------+--------------+
| example              | left_example |
+----------------------+--------------+
| apple                | app          |
| banana               | ban          |
| \xab\xcd\xef\xaa\xbb | \xab\xcd\xef |
+----------------------+--------------+
```

### LENGTH

```sql
LENGTH(value)
```

**Description**

Returns the length of the `STRING` or `BYTES` value. The returned
value is in characters for `STRING` arguments and in bytes for the `BYTES`
argument.

**Return type**

`INT64`

**Examples**

```sql

WITH example AS
  (SELECT 'абвгд' AS characters)

SELECT
  characters,
  LENGTH(characters) AS string_example,
  LENGTH(CAST(characters AS BYTES)) AS bytes_example
FROM example;

+------------+----------------+---------------+
| characters | string_example | bytes_example |
+------------+----------------+---------------+
| абвгд      |              5 |            10 |
+------------+----------------+---------------+
```

### LPAD

```sql
LPAD(original_value, return_length[, pattern])
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value` prepended
with `pattern`. The `return_length` is an `INT64` that
specifies the length of the returned value. If `original_value` is of type
`BYTES`, `return_length` is the number of bytes. If `original_value` is
of type `STRING`, `return_length` is the number of characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `LPAD('hello world', 7);` returns `'hello w'`.

If `original_value`, `return_length`, or `pattern` is `NULL`, this function
returns `NULL`.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, len, FORMAT('%T', LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);

+------+-----+----------+
| t    | len | LPAD     |
|------|-----|----------|
| abc  | 5   | "  abc"  |
| abc  | 2   | "ab"     |
| 例子  | 4   | "  例子" |
+------+-----+----------+
```

```sql
SELECT t, len, pattern, FORMAT('%T', LPAD(t, len, pattern)) AS LPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

+------+-----+---------+--------------+
| t    | len | pattern | LPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | "defdeabc"   |
| abc  | 5   | -       | "--abc"      |
| 例子  | 5   | 中文    | "中文中例子"   |
+------+-----+---------+--------------+
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', LPAD(t, len)) AS LPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

+-----------------+-----+------------------+
| t               | len | LPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | b"  abc"         |
| b"abc"          | 2   | b"ab"            |
| b"\xab\xcd\xef" | 4   | b" \xab\xcd\xef" |
+-----------------+-----+------------------+
```

```sql
SELECT
  FORMAT('%T', t) AS t,
  len,
  FORMAT('%T', pattern) AS pattern,
  FORMAT('%T', LPAD(t, len, pattern)) AS LPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);

+-----------------+-----+---------+-------------------------+
| t               | len | pattern | LPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | b"defdeabc"             |
| b"abc"          | 5   | b"-"    | b"--abc"                |
| b"\xab\xcd\xef" | 5   | b"\x00" | b"\x00\x00\xab\xcd\xef" |
+-----------------+-----+---------+-------------------------+
```

### LOWER

```sql
LOWER(value)
```

**Description**

For `STRING` arguments, returns the original string with all alphabetic
characters in lowercase. Mapping between lowercase and uppercase is done
according to the
[Unicode Character Database][string-link-to-unicode-character-definitions]
without taking into account language-specific mappings.

For `BYTES` arguments, the argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql

WITH items AS
  (SELECT
    'FOO' as item
  UNION ALL
  SELECT
    'BAR' as item
  UNION ALL
  SELECT
    'BAZ' as item)

SELECT
  LOWER(item) AS example
FROM items;

+---------+
| example |
+---------+
| foo     |
| bar     |
| baz     |
+---------+
```

### LTRIM

```sql
LTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes leading characters.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT '   apple   ' as item
  UNION ALL
  SELECT '   banana   ' as item
  UNION ALL
  SELECT '   orange   ' as item)

SELECT
  CONCAT('#', LTRIM(item), '#') as example
FROM items;

+-------------+
| example     |
+-------------+
| #apple   #  |
| #banana   # |
| #orange   # |
+-------------+
```

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  LTRIM(item, '*') as example
FROM items;

+-----------+
| example   |
+-----------+
| apple***  |
| banana*** |
| orange*** |
+-----------+
```

```sql
WITH items AS
  (SELECT 'xxxapplexxx' as item
  UNION ALL
  SELECT 'yyybananayyy' as item
  UNION ALL
  SELECT 'zzzorangezzz' as item
  UNION ALL
  SELECT 'xyzpearxyz' as item)
```

```sql
SELECT
  LTRIM(item, 'xyz') as example
FROM items;

+-----------+
| example   |
+-----------+
| applexxx  |
| bananayyy |
| orangezzz |
| pearxyz   |
+-----------+
```

### NORMALIZE

```sql
NORMALIZE(value[, normalization_mode])
```

**Description**

Takes a string value and returns it as a normalized string. If you do not
provide a normalization mode, `NFC` is used.

[Normalization][string-link-to-normalization-wikipedia] is used to ensure that
two strings are equivalent. Normalization is often used in situations in which
two strings render the same on the screen but have different Unicode code
points.

`NORMALIZE` supports four optional normalization modes:

| Value   | Name                                           | Description|
|---------|------------------------------------------------|------------|
| `NFC`   | Normalization Form Canonical Composition       | Decomposes and recomposes characters by canonical equivalence.|
| `NFKC`  | Normalization Form Compatibility Composition   | Decomposes characters by compatibility, then recomposes them by canonical equivalence.|
| `NFD`   | Normalization Form Canonical Decomposition     | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.|
| `NFKD`  | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

**Return type**

`STRING`

**Examples**

```sql
SELECT a, b, a = b as normalized
FROM (SELECT NORMALIZE('\u00ea') as a, NORMALIZE('\u0065\u0302') as b);

+---+---+------------+
| a | b | normalized |
+---+---+------------+
| ê | ê | true       |
+---+---+------------+
```
The following example normalizes different space characters.

```sql
WITH EquivalentNames AS (
  SELECT name
  FROM UNNEST([
      'Jane\u2004Doe',
      'John\u2004Smith',
      'Jane\u2005Doe',
      'Jane\u2006Doe',
      'John Smith']) AS name
)
SELECT
  NORMALIZE(name, NFKC) AS normalized_name,
  COUNT(*) AS name_count
FROM EquivalentNames
GROUP BY 1;

+-----------------+------------+
| normalized_name | name_count |
+-----------------+------------+
| John Smith      | 2          |
| Jane Doe        | 3          |
+-----------------+------------+
```

### NORMALIZE_AND_CASEFOLD

```sql
NORMALIZE_AND_CASEFOLD(value[, normalization_mode])
```

**Description**

Takes a string value and returns it as a normalized string with
normalization.

[Normalization][string-link-to-normalization-wikipedia] is used to ensure that
two strings are equivalent. Normalization is often used in situations in which
two strings render the same on the screen but have different Unicode code
points.

[Case folding][string-link-to-case-folding-wikipedia] is used for the caseless
comparison of strings. If you need to compare strings and case should not be
considered, use `NORMALIZE_AND_CASEFOLD`, otherwise use
[`NORMALIZE`][string-link-to-normalize].

`NORMALIZE_AND_CASEFOLD` supports four optional normalization modes:

| Value   | Name                                           | Description|
|---------|------------------------------------------------|------------|
| `NFC`   | Normalization Form Canonical Composition       | Decomposes and recomposes characters by canonical equivalence.|
| `NFKC`  | Normalization Form Compatibility Composition   | Decomposes characters by compatibility, then recomposes them by canonical equivalence.|
| `NFD`   | Normalization Form Canonical Decomposition     | Decomposes characters by canonical equivalence, and multiple combining characters are arranged in a specific order.|
| `NFKD`  | Normalization Form Compatibility Decomposition | Decomposes characters by compatibility, and multiple combining characters are arranged in a specific order.|

**Return type**

`STRING`

**Examples**

```sql
SELECT
  a, b,
  NORMALIZE(a) = NORMALIZE(b) as normalized,
  NORMALIZE_AND_CASEFOLD(a) = NORMALIZE_AND_CASEFOLD(b) as normalized_with_case_folding
FROM (SELECT 'The red barn' AS a, 'The Red Barn' AS b);

+--------------+--------------+------------+------------------------------+
| a            | b            | normalized | normalized_with_case_folding |
+--------------+--------------+------------+------------------------------+
| The red barn | The Red Barn | false      | true                         |
+--------------+--------------+------------+------------------------------+
```

```sql
WITH Strings AS (
  SELECT '\u2168' AS a, 'IX' AS b UNION ALL
  SELECT '\u0041\u030A', '\u00C5'
)
SELECT a, b,
  NORMALIZE_AND_CASEFOLD(a, NFD)=NORMALIZE_AND_CASEFOLD(b, NFD) AS nfd,
  NORMALIZE_AND_CASEFOLD(a, NFC)=NORMALIZE_AND_CASEFOLD(b, NFC) AS nfc,
  NORMALIZE_AND_CASEFOLD(a, NFKD)=NORMALIZE_AND_CASEFOLD(b, NFKD) AS nkfd,
  NORMALIZE_AND_CASEFOLD(a, NFKC)=NORMALIZE_AND_CASEFOLD(b, NFKC) AS nkfc
FROM Strings;

+---+----+-------+-------+------+------+
| a | b  | nfd   | nfc   | nkfd | nkfc |
+---+----+-------+-------+------+------+
| Ⅸ | IX | false | false | true | true |
| Å | Å  | true  | true  | true | true |
+---+----+-------+-------+------+------+
```

### OCTET_LENGTH

```sql
OCTET_LENGTH(value)
```

Alias for [`BYTE_LENGTH`](#byte-length).

### REGEXP_CONTAINS

```sql
REGEXP_CONTAINS(value, regexp)
```

**Description**

Returns `TRUE` if `value` is a partial match for the regular expression,
`regexp`.

If the `regexp` argument is invalid, the function returns an error.

You can search for a full match by using `^` (beginning of text) and `$` (end of
text). Due to regular expression operator precedence, it is good practice to use
parentheses around everything between `^` and `$`.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`BOOL`

**Examples**

```sql
SELECT
  email,
  REGEXP_CONTAINS(email, r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+') AS is_valid
FROM
  (SELECT
    ['foo@example.com', 'bar@example.org', 'www.example.net']
    AS addresses),
  UNNEST(addresses) AS email;

+-----------------+----------+
| email           | is_valid |
+-----------------+----------+
| foo@example.com | true     |
| bar@example.org | true     |
| www.example.net | false    |
+-----------------+----------+

-- Performs a full match, using ^ and $. Due to regular expression operator
-- precedence, it is good practice to use parentheses around everything between ^
-- and $.
SELECT
  email,
  REGEXP_CONTAINS(email, r'^([\w.+-]+@foo\.com|[\w.+-]+@bar\.org)$')
    AS valid_email_address,
  REGEXP_CONTAINS(email, r'^[\w.+-]+@foo\.com|[\w.+-]+@bar\.org$')
    AS without_parentheses
FROM
  (SELECT
    ['a@foo.com', 'a@foo.computer', 'b@bar.org', '!b@bar.org', 'c@buz.net']
    AS addresses),
  UNNEST(addresses) AS email;

+----------------+---------------------+---------------------+
| email          | valid_email_address | without_parentheses |
+----------------+---------------------+---------------------+
| a@foo.com      | true                | true                |
| a@foo.computer | false               | true                |
| b@bar.org      | true                | true                |
| !b@bar.org     | false               | true                |
| c@buz.net      | false               | false               |
+----------------+---------------------+---------------------+
```

### REGEXP_EXTRACT

```sql
REGEXP_EXTRACT(value, regexp)
```

**Description**

Returns the first substring in `value` that matches the regular expression,
`regexp`. Returns `NULL` if there is no match.

If the regular expression contains a capturing group, the function returns the
substring that is matched by that capturing group. If the expression does not
contain a capturing group, the function returns the entire matching substring.

Returns an error if:

+ The regular expression is invalid
+ The regular expression has more than one capturing group

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'baz@example.net' as email)

SELECT
  REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+')
  AS user_name
FROM email_addresses;

+-----------+
| user_name |
+-----------+
| foo       |
| bar       |
| baz       |
+-----------+
```

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'baz@example.net' as email)

SELECT
  REGEXP_EXTRACT(email, r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)')
  AS top_level_domain
FROM email_addresses;

+------------------+
| top_level_domain |
+------------------+
| com              |
| org              |
| net              |
+------------------+
```

### REGEXP_EXTRACT_ALL

```sql
REGEXP_EXTRACT_ALL(value, regexp)
```

**Description**

Returns an array of all substrings of `value` that match the regular expression,
`regexp`.

The `REGEXP_EXTRACT_ALL` function only returns non-overlapping matches. For
example, using this function to extract `ana` from `banana` returns only one
substring, not two.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

An `ARRAY` of either `STRING`s or `BYTES`

**Examples**

```sql
WITH code_markdown AS
  (SELECT 'Try `function(x)` or `function(y)`' as code)

SELECT
  REGEXP_EXTRACT_ALL(code, '`(.+?)`') AS example
FROM code_markdown;

+----------------------------+
| example                    |
+----------------------------+
| [function(x), function(y)] |
+----------------------------+
```

### REGEXP_INSTR

```sql
REGEXP_INSTR(source_value, regexp [, position[, occurrence, [occurrence_position]]])
```

**Description**

Returns the lowest 1-based index of a regular expression, `regexp`, in
`source_value`. Returns `0` when no match is found or the regular expression
is empty. Returns an error if the regular expression is invalid or has more than
one capturing group. `source_value` and `regexp` must be the same type, either
`STRING` or `BYTES`.

If `position` is specified, the search starts at this position in
`source_value`, otherwise it starts at the beginning of `source_value`. If
`position` is negative, the function searches backwards from the end of
`source_value`, with -1 indicating the last character. `position` cannot be 0.

If `occurrence` is specified, the search returns the position of a specific
instance of `regexp` in `source_value`, otherwise it returns the index of
the first occurrence. If `occurrence` is greater than the number of matches
found, 0 is returned. For `occurrence` > 1, the function searches for
overlapping occurrences, in other words, the function searches for additional
occurrences beginning with the second character in the previous occurrence.
`occurrence` cannot be 0 or negative.

You can optionally use `occurrence_position` to specify where a position
in relation to an `occurrence` starts. Your choices are:
+  `0`: Returns the beginning position of the occurrence.
+  `1`: Returns the first position following the end of the occurrence. If the
   end of the occurrence is also the end of the input, one off the
   end of the occurrence is returned. For example, length of a string + 1.

**Return type**

`INT64`

**Examples**

```sql
WITH example AS (
  SELECT 'ab@gmail.com' AS source_value, '@[^.]*' AS regexp UNION ALL
  SELECT 'ab@mail.com', '@[^.]*' UNION ALL
  SELECT 'abc@gmail.com', '@[^.]*' UNION ALL
  SELECT 'abc.com', '@[^.]*')
SELECT source_value, regexp, REGEXP_INSTR(source_value, regexp) AS instr
FROM example;

+---------------+--------+-------+
| source_value  | regexp | instr |
+---------------+--------+-------+
| ab@gmail.com  | @[^.]* | 3     |
| ab@mail.com   | @[^.]* | 3     |
| abc@gmail.com | @[^.]* | 4     |
| abc.com       | @[^.]* | 0     |
+---------------+--------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com' AS source_value, '@[^.]*' AS regexp, 1 AS position UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 3 UNION ALL
  SELECT 'a@gmail.com b@gmail.com', '@[^.]*', 4)
SELECT
  source_value, regexp, position,
  REGEXP_INSTR(source_value, regexp, position) AS instr
FROM example;

+-------------------------+--------+----------+-------+
| source_value            | regexp | position | instr |
+-------------------------+--------+----------+-------+
| a@gmail.com b@gmail.com | @[^.]* | 1        | 2     |
| a@gmail.com b@gmail.com | @[^.]* | 2        | 2     |
| a@gmail.com b@gmail.com | @[^.]* | 3        | 14    |
| a@gmail.com b@gmail.com | @[^.]* | 4        | 14    |
+-------------------------+--------+----------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com b@gmail.com c@gmail.com' AS source_value,
         '@[^.]*' AS regexp, 1 AS position, 1 AS occurrence UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 2 UNION ALL
  SELECT 'a@gmail.com b@gmail.com c@gmail.com', '@[^.]*', 1, 3)
SELECT
  source_value, regexp, position, occurrence,
  REGEXP_INSTR(source_value, regexp, position, occurrence) AS instr
FROM example;

+-------------------------------------+--------+----------+------------+-------+
| source_value                        | regexp | position | occurrence | instr |
+-------------------------------------+--------+----------+------------+-------+
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 1          | 2     |
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 2          | 14    |
| a@gmail.com b@gmail.com c@gmail.com | @[^.]* | 1        | 3          | 26    |
+-------------------------------------+--------+----------+------------+-------+
```

```sql
WITH example AS (
  SELECT 'a@gmail.com' AS source_value, '@[^.]*' AS regexp,
         1 AS position, 1 AS occurrence, 0 AS o_position UNION ALL
  SELECT 'a@gmail.com', '@[^.]*', 1, 1, 1)
SELECT
  source_value, regexp, position, occurrence, o_position,
  REGEXP_INSTR(source_value, regexp, position, occurrence, o_position) AS instr
FROM example;

+--------------+--------+----------+------------+------------+-------+
| source_value | regexp | position | occurrence | o_position | instr |
+--------------+--------+----------+------------+------------+-------+
| a@gmail.com  | @[^.]* | 1        | 1          | 0          | 2     |
| a@gmail.com  | @[^.]* | 1        | 1          | 1          | 8     |
+--------------+--------+----------+------------+------------+-------+
```

### REGEXP_MATCH

<p class="caution"><strong>Deprecated.</strong> Use <a href="#regexp_contains">REGEXP_CONTAINS</a>.</p>

```sql
REGEXP_MATCH(value, regexp)
```

**Description**

Returns `TRUE` if `value` is a full match for the regular expression, `regexp`.

If the `regexp` argument is invalid, the function returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`BOOL`

**Examples**

```sql
WITH email_addresses AS
  (SELECT 'foo@example.com' as email
  UNION ALL
  SELECT 'bar@example.org' as email
  UNION ALL
  SELECT 'notavalidemailaddress' as email)

SELECT
  email,
  REGEXP_MATCH(email,
               r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
               AS valid_email_address
FROM email_addresses;

+-----------------------+---------------------+
| email                 | valid_email_address |
+-----------------------+---------------------+
| foo@example.com       | true                |
| bar@example.org       | true                |
| notavalidemailaddress | false               |
+-----------------------+---------------------+
```

### REGEXP_REPLACE

```sql
REGEXP_REPLACE(value, regexp, replacement)
```

**Description**

Returns a `STRING` where all substrings of `value` that
match regular expression `regexp` are replaced with `replacement`.

You can use backslashed-escaped digits (\1 to \9) within the `replacement`
argument to insert text matching the corresponding parenthesized group in the
`regexp` pattern. Use \0 to refer to the entire matching text.

To add a backslash in your regular expression, you must first escape it. For
example, `SELECT REGEXP_REPLACE('abc', 'b(.)', 'X\\1');` returns `aXc`. You can
also use [raw strings][string-link-to-lexical-literals] to remove one layer of
escaping, for example `SELECT REGEXP_REPLACE('abc', 'b(.)', r'X\1');`.

The `REGEXP_REPLACE` function only replaces non-overlapping matches. For
example, replacing `ana` within `banana` results in only one replacement, not
two.

If the `regexp` argument is not a valid regular expression, this function
returns an error.

Note: ZetaSQL provides regular expression support using the
[re2][string-link-to-re2] library; see that documentation for its
regular expression syntax.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH markdown AS
  (SELECT '# Heading' as heading
  UNION ALL
  SELECT '# Another heading' as heading)

SELECT
  REGEXP_REPLACE(heading, r'^# ([a-zA-Z0-9\s]+$)', '<h1>\\1</h1>')
  AS html
FROM markdown;

+--------------------------+
| html                     |
+--------------------------+
| <h1>Heading</h1>         |
| <h1>Another heading</h1> |
+--------------------------+
```

### REPLACE

```sql
REPLACE(original_value, from_value, to_value)
```

**Description**

Replaces all occurrences of `from_value` with `to_value` in `original_value`.
If `from_value` is empty, no replacement is made.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH desserts AS
  (SELECT 'apple pie' as dessert
  UNION ALL
  SELECT 'blackberry pie' as dessert
  UNION ALL
  SELECT 'cherry pie' as dessert)

SELECT
  REPLACE (dessert, 'pie', 'cobbler') as example
FROM desserts;

+--------------------+
| example            |
+--------------------+
| apple cobbler      |
| blackberry cobbler |
| cherry cobbler     |
+--------------------+
```

### REPEAT

```sql
REPEAT(original_value, repetitions)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value`, repeated.
The `repetitions` parameter specifies the number of times to repeat
`original_value`. Returns `NULL` if either `original_value` or `repetitions`
are `NULL`.

This function returns an error if the `repetitions` value is negative.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, n, REPEAT(t, n) AS REPEAT FROM UNNEST([
  STRUCT('abc' AS t, 3 AS n),
  ('例子', 2),
  ('abc', null),
  (null, 3)
]);

+------+------+-----------+
| t    | n    | REPEAT    |
|------|------|-----------|
| abc  | 3    | abcabcabc |
| 例子 | 2    | 例子例子  |
| abc  | NULL | NULL      |
| NULL | 3    | NULL      |
+------+------+-----------+
```

### REVERSE

```sql
REVERSE(value)
```

**Description**

Returns the reverse of the input `STRING` or `BYTES`.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH example AS (
  SELECT 'foo' AS sample_string, b'bar' AS sample_bytes UNION ALL
  SELECT 'абвгд' AS sample_string, b'123' AS sample_bytes
)
SELECT
  sample_string,
  REVERSE(sample_string) AS reverse_string,
  sample_bytes,
  REVERSE(sample_bytes) AS reverse_bytes
FROM example;

+---------------+----------------+--------------+---------------+
| sample_string | reverse_string | sample_bytes | reverse_bytes |
+---------------+----------------+--------------+---------------+
| foo           | oof            | bar          | rab           |
| абвгд         | дгвба          | 123          | 321           |
+---------------+----------------+--------------+---------------+
```

### RIGHT

```sql
RIGHT(value, length)
```

**Description**

Returns a `STRING` or `BYTES` value that consists of the specified
number of rightmost characters or bytes from `value`. The `length` is an
`INT64` that specifies the length of the returned
value. If `value` is `BYTES`, `length` is the number of rightmost bytes to
return. If `value` is `STRING`, `length` is the number of rightmost characters
to return.

If `length` is 0, an empty `STRING` or `BYTES` value will be
returned. If `length` is negative, an error will be returned. If `length`
exceeds the number of characters or bytes from `value`, the original `value`
will be returned.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH examples AS
(SELECT 'apple' as example
UNION ALL
SELECT 'banana' as example
UNION ALL
SELECT 'абвгд' as example
)
SELECT example, RIGHT(example, 3) AS right_example
FROM examples;

+---------+---------------+
| example | right_example |
+---------+---------------+
| apple   | ple           |
| banana  | ana           |
| абвгд   | вгд           |
+---------+---------------+
```

```sql
WITH examples AS
(SELECT b'apple' as example
UNION ALL
SELECT b'banana' as example
UNION ALL
SELECT b'\xab\xcd\xef\xaa\xbb' as example
)
SELECT example, RIGHT(example, 3) AS right_example
FROM examples;

+----------------------+---------------+
| example              | right_example |
+----------------------+---------------+
| apple                | ple           |
| banana               | ana           |
| \xab\xcd\xef\xaa\xbb | \xef\xaa\xbb  |
+----------------------+---------------+
```

### RPAD

```sql
RPAD(original_value, return_length[, pattern])
```

**Description**

Returns a `STRING` or `BYTES` value that consists of `original_value` appended
with `pattern`. The `return_length` parameter is an
`INT64` that specifies the length of the
returned value. If `original_value` is `BYTES`,
`return_length` is the number of bytes. If `original_value` is `STRING`,
`return_length` is the number of characters.

The default value of `pattern` is a blank space.

Both `original_value` and `pattern` must be the same data type.

If `return_length` is less than or equal to the `original_value` length, this
function returns the `original_value` value, truncated to the value of
`return_length`. For example, `RPAD('hello world', 7);` returns `'hello w'`.

If `original_value`, `return_length`, or `pattern` is `NULL`, this function
returns `NULL`.

This function returns an error if:

+ `return_length` is negative
+ `pattern` is empty

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
SELECT t, len, FORMAT('%T', RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 5 AS len),
  ('abc', 2),
  ('例子', 4)
]);

+------+-----+----------+
| t    | len | RPAD     |
|------|-----|----------|
| abc  | 5   | "abc  "  |
| abc  | 2   | "ab"     |
| 例子  | 4   | "例子  " |
+------+-----+----------+
```

```sql
SELECT t, len, pattern, FORMAT('%T', RPAD(t, len, pattern)) AS RPAD FROM UNNEST([
  STRUCT('abc' AS t, 8 AS len, 'def' AS pattern),
  ('abc', 5, '-'),
  ('例子', 5, '中文')
]);

+------+-----+---------+--------------+
| t    | len | pattern | RPAD         |
|------|-----|---------|--------------|
| abc  | 8   | def     | "abcdefde"   |
| abc  | 5   | -       | "abc--"      |
| 例子  | 5   | 中文     | "例子中文中"  |
+------+-----+---------+--------------+
```

```sql
SELECT FORMAT('%T', t) AS t, len, FORMAT('%T', RPAD(t, len)) AS RPAD FROM UNNEST([
  STRUCT(b'abc' AS t, 5 AS len),
  (b'abc', 2),
  (b'\xab\xcd\xef', 4)
]);

+-----------------+-----+------------------+
| t               | len | RPAD             |
|-----------------|-----|------------------|
| b"abc"          | 5   | b"abc  "         |
| b"abc"          | 2   | b"ab"            |
| b"\xab\xcd\xef" | 4   | b"\xab\xcd\xef " |
+-----------------+-----+------------------+
```

```sql
SELECT
  FORMAT('%T', t) AS t,
  len,
  FORMAT('%T', pattern) AS pattern,
  FORMAT('%T', RPAD(t, len, pattern)) AS RPAD
FROM UNNEST([
  STRUCT(b'abc' AS t, 8 AS len, b'def' AS pattern),
  (b'abc', 5, b'-'),
  (b'\xab\xcd\xef', 5, b'\x00')
]);

+-----------------+-----+---------+-------------------------+
| t               | len | pattern | RPAD                    |
|-----------------|-----|---------|-------------------------|
| b"abc"          | 8   | b"def"  | b"abcdefde"             |
| b"abc"          | 5   | b"-"    | b"abc--"                |
| b"\xab\xcd\xef" | 5   | b"\x00" | b"\xab\xcd\xef\x00\x00" |
+-----------------+-----+---------+-------------------------+
```

### RTRIM

```sql
RTRIM(value1[, value2])
```

**Description**

Identical to [TRIM][string-link-to-trim], but only removes trailing characters.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  RTRIM(item, '*') as example
FROM items;

+-----------+
| example   |
+-----------+
| ***apple  |
| ***banana |
| ***orange |
+-----------+
```

```sql
WITH items AS
  (SELECT 'applexxx' as item
  UNION ALL
  SELECT 'bananayyy' as item
  UNION ALL
  SELECT 'orangezzz' as item
  UNION ALL
  SELECT 'pearxyz' as item)

SELECT
  RTRIM(item, 'xyz') as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```

### SAFE_CONVERT_BYTES_TO_STRING

```sql
SAFE_CONVERT_BYTES_TO_STRING(value)
```

**Description**

Converts a sequence of `BYTES` to a `STRING`. Any invalid UTF-8 characters are
replaced with the Unicode replacement character, `U+FFFD`.

**Return type**

`STRING`

**Examples**

The following statement returns the Unicode replacement character, &#65533;.

```sql
SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\xc2') as safe_convert;
```

### SOUNDEX

```sql
SOUNDEX(value)
```

**Description**

Returns a `STRING` that represents the
[Soundex][string-link-to-soundex-wikipedia] code for `value`.

SOUNDEX produces a phonetic representation of a string. It indexes words by
sound, as pronounced in English. It is typically used to help determine whether
two strings, such as the family names _Levine_ and _Lavine_, or the words _to_
and _too_, have similar English-language pronunciation.

The result of the SOUNDEX consists of a letter followed by 3 digits. Non-latin
characters are ignored. If the remaining string is empty after removing
non-Latin characters, an empty `STRING` is returned.

**Return type**

`STRING`

**Examples**

```sql
WITH example AS (
  SELECT 'Ashcraft' AS value UNION ALL
  SELECT 'Raven' AS value UNION ALL
  SELECT 'Ribbon' AS value UNION ALL
  SELECT 'apple' AS value UNION ALL
  SELECT 'Hello world!' AS value UNION ALL
  SELECT '  H3##!@llo w00orld!' AS value UNION ALL
  SELECT '#1' AS value UNION ALL
  SELECT NULL AS value
)
SELECT value, SOUNDEX(value) AS soundex
FROM example;

+----------------------+---------+
| value                | soundex |
+----------------------+---------+
| Ashcraft             | A261    |
| Raven                | R150    |
| Ribbon               | R150    |
| apple                | a140    |
| Hello world!         | H464    |
|   H3##!@llo w00orld! | H464    |
| #1                   |         |
| NULL                 | NULL    |
+----------------------+---------+
```

### SPLIT

```sql
SPLIT(value[, delimiter])
```

**Description**

Splits `value` using the `delimiter` argument.

For `STRING`, the default delimiter is the comma `,`.

For `BYTES`, you must specify a delimiter.

Splitting on an empty delimiter produces an array of UTF-8 characters for
`STRING` values, and an array of `BYTES` for `BYTES` values.

Splitting an empty `STRING` returns an
`ARRAY` with a single empty
`STRING`.

**Return type**

`ARRAY` of type `STRING` or
`ARRAY` of type `BYTES`

**Examples**

```sql
WITH letters AS
  (SELECT '' as letter_group
  UNION ALL
  SELECT 'a' as letter_group
  UNION ALL
  SELECT 'b c d' as letter_group)

SELECT SPLIT(letter_group, ' ') as example
FROM letters;

+----------------------+
| example              |
+----------------------+
| []                   |
| [a]                  |
| [b, c, d]            |
+----------------------+
```

### STARTS_WITH

```sql
STARTS_WITH(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns `TRUE` if the second value is a
prefix of the first.

**Return type**

`BOOL`

**Examples**

```sql
WITH items AS
  (SELECT 'foo' as item
  UNION ALL
  SELECT 'bar' as item
  UNION ALL
  SELECT 'baz' as item)

SELECT
  STARTS_WITH(item, 'b') as example
FROM items;

+---------+
| example |
+---------+
|   False |
|    True |
|    True |
+---------+
```

### STRPOS

```sql
STRPOS(value1, value2)
```

**Description**

Takes two `STRING` or `BYTES` values. Returns the 1-based index of the first
occurrence of `value2` inside `value1`. Returns `0` if `value2` is not found.

**Return type**

`INT64`

**Examples**

```sql
WITH email_addresses AS
  (SELECT
    'foo@example.com' AS email_address
  UNION ALL
  SELECT
    'foobar@example.com' AS email_address
  UNION ALL
  SELECT
    'foobarbaz@example.com' AS email_address
  UNION ALL
  SELECT
    'quxexample.com' AS email_address)

SELECT
  STRPOS(email_address, '@') AS example
FROM email_addresses;

+---------+
| example |
+---------+
|       4 |
|       7 |
|      10 |
|       0 |
+---------+
```

### SUBSTR

```sql
SUBSTR(value, position[, length])
```

**Description**

Returns a substring of the supplied `STRING` or `BYTES` value. The
`position` argument is an integer specifying the starting position of the
substring, with position = 1 indicating the first character or byte. The
`length` argument is the maximum number of characters for `STRING` arguments,
or bytes for `BYTES` arguments.

If `position` is negative, the function counts from the end of `value`,
with -1 indicating the last character.

If `position` is a position off the left end of the
`STRING` (`position` = 0 or `position` &lt; `-LENGTH(value)`), the function
starts from position = 1. If `length` exceeds the length of `value`, the
function returns fewer than `length` characters.

If `length` is less than 0, the function returns an error.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 2) as example
FROM items;

+---------+
| example |
+---------+
| pple    |
| anana   |
| range   |
+---------+
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 2, 2) as example
FROM items;

+---------+
| example |
+---------+
| pp      |
| an      |
| ra      |
+---------+
```

```sql
WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, -2) as example
FROM items;

+---------+
| example |
+---------+
| le      |
| na      |
| ge      |
+---------+
```

### SUBSTRING

```sql
SUBSTRING(value, position[, length])
```

Alias for [`SUBSTR`](#substr).

### TO_BASE32

```sql
TO_BASE32(bytes_expr)
```

**Description**

Converts a sequence of `BYTES` into a base32-encoded `STRING`. To convert a
base32-encoded `STRING` into `BYTES`, use [FROM_BASE32][string-link-to-from-base32].

**Return type**

`STRING`

**Example**

```sql
SELECT TO_BASE32(b'abcde\xFF') AS base32_string;

+------------------+
| base32_string    |
+------------------+
| MFRGGZDF74====== |
+------------------+
```

### TO_BASE64

```sql
TO_BASE64(bytes_expr)
```

**Description**

Converts a sequence of `BYTES` into a base64-encoded `STRING`. To convert a
base64-encoded `STRING` into `BYTES`, use [FROM_BASE64][string-link-to-from-base64].

There are several base64 encodings in common use that vary in exactly which
alphabet of 65 ASCII characters are used to encode the 64 digits and padding.
See [RFC 4648](https://tools.ietf.org/html/rfc4648#section-4) for details. This
function adds padding and uses the alphabet `[A-Za-z0-9+/=]`.

**Return type**

`STRING`

**Example**

```sql
SELECT TO_BASE64(b'\377\340') AS base64_string;

+---------------+
| base64_string |
+---------------+
| /+A=          |
+---------------+
```

To work with an encoding using a different base64 alphabet, you might need to
compose `TO_BASE64` with the `REPLACE` function. For instance, the
`base64url` url-safe and filename-safe encoding commonly used in web programming
uses `-_=` as the last characters rather than `+/=`. To encode a
`base64url`-encoded string, replace `+` and `/` with `-` and `_` respectively.

```sql
SELECT REPLACE(REPLACE(TO_BASE64(b'\377\340'), '+', '-'), '/', '_') as websafe_base64;

+----------------+
| websafe_base64 |
+----------------+
| _-A=           |
+----------------+
```

### TO_CODE_POINTS

```sql
TO_CODE_POINTS(value)
```

**Description**

Takes a [value](#string_values) and returns an array of
`INT64`.

+ If `value` is a `STRING`, each element in the returned array represents a
  [code point][string-link-to-code-points-wikipedia]. Each code point falls
  within the range of [0, 0xD7FF] and [0xE000, 0x10FFFF].
+ If `value` is `BYTES`, each element in the array is an extended ASCII
  character value in the range of [0, 255].

To convert from an array of code points to a `STRING` or `BYTES`, see
[CODE_POINTS_TO_STRING][string-link-to-codepoints-to-string] or
[CODE_POINTS_TO_BYTES][string-link-to-codepoints-to-bytes].

**Return type**

`ARRAY` of `INT64`

**Examples**

The following example gets the code points for each element in an array of
words.

```sql
SELECT word, TO_CODE_POINTS(word) AS code_points
FROM UNNEST(['foo', 'bar', 'baz', 'giraffe', 'llama']) AS word;

+---------+------------------------------------+
| word    | code_points                        |
+---------+------------------------------------+
| foo     | [102, 111, 111]                    |
| bar     | [98, 97, 114]                      |
| baz     | [98, 97, 122]                      |
| giraffe | [103, 105, 114, 97, 102, 102, 101] |
| llama   | [108, 108, 97, 109, 97]            |
+---------+------------------------------------+
```

The following example converts integer representations of `BYTES` to their
corresponding ASCII character values.

```sql
SELECT word, TO_CODE_POINTS(word) AS bytes_value_as_integer
FROM UNNEST([b'\x00\x01\x10\xff', b'\x66\x6f\x6f']) AS word;

+------------------+------------------------+
| word             | bytes_value_as_integer |
+------------------+------------------------+
| \x00\x01\x10\xff | [0, 1, 16, 255]        |
| foo              | [102, 111, 111]        |
+------------------+------------------------+
```

The following example demonstrates the difference between a `BYTES` result and a
`STRING` result.

```sql
SELECT TO_CODE_POINTS(b'Ā') AS b_result, TO_CODE_POINTS('Ā') AS s_result;

+------------+----------+
| b_result   | s_result |
+------------+----------+
| [196, 128] | [256]    |
+------------+----------+
```

Notice that the character, Ā, is represented as a two-byte Unicode sequence. As
a result, the `BYTES` version of `TO_CODE_POINTS` returns an array with two
elements, while the `STRING` version returns an array with a single element.

### TO_HEX

```sql
TO_HEX(bytes)
```

**Description**

Converts a sequence of `BYTES` into a hexadecimal `STRING`. Converts each byte
in the `STRING` as two hexadecimal characters in the range
`(0..9, a..f)`. To convert a hexadecimal-encoded
`STRING` to `BYTES`, use [FROM_HEX][string-link-to-from-hex].

**Return type**

`STRING`

**Example**

```sql
WITH Input AS (
  SELECT b'\x00\x01\x02\x03\xAA\xEE\xEF\xFF' AS byte_str UNION ALL
  SELECT b'foobar'
)
SELECT byte_str, TO_HEX(byte_str) AS hex_str
FROM Input;

+----------------------------------+------------------+
| byte_string                      | hex_string       |
+----------------------------------+------------------+
| \x00\x01\x02\x03\xaa\xee\xef\xff | 00010203aaeeefff |
| foobar                           | 666f6f626172     |
+----------------------------------+------------------+
```

### TRANSLATE

```sql
TRANSLATE(expression, source_characters, target_characters)
```

**Description**

In `expression`, replaces each character in `source_characters` with the
corresponding character in `target_characters`. All inputs must be the same
type, either `STRING` or `BYTES`.

+ Each character in `expression` is translated at most once.
+ A character in `expression` that is not present in `source_characters` is left
  unchanged in `expression`.
+ A character in `source_characters` without a corresponding character in
  `target_characters` is omitted from the result.
+ A duplicate character in `source_characters` results in an error.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH example AS (
  SELECT 'This is a cookie' AS expression, 'sco' AS source_characters, 'zku' AS
  target_characters UNION ALL
  SELECT 'A coaster' AS expression, 'co' AS source_characters, 'k' as
  target_characters
)
SELECT expression, source_characters, target_characters, TRANSLATE(expression,
source_characters, target_characters) AS translate
FROM example;

+------------------+-------------------+-------------------+------------------+
| expression       | source_characters | target_characters | translate        |
+------------------+-------------------+-------------------+------------------+
| This is a cookie | sco               | zku               | Thiz iz a kuukie |
| A coaster        | co                | k                 | A kaster         |
+------------------+-------------------+-------------------+------------------+
```

### TRIM

```sql
TRIM(value1[, value2])
```

**Description**

Removes all leading and trailing characters that match `value2`. If
`value2` is not specified, all leading and trailing whitespace characters (as
defined by the Unicode standard) are removed. If the first argument is of type
`BYTES`, the second argument is required.

If `value2` contains more than one character or byte, the function removes all
leading or trailing characters or bytes contained in `value2`.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT '   apple   ' as item
  UNION ALL
  SELECT '   banana   ' as item
  UNION ALL
  SELECT '   orange   ' as item)

SELECT
  CONCAT('#', TRIM(item), '#') as example
FROM items;

+----------+
| example  |
+----------+
| #apple#  |
| #banana# |
| #orange# |
+----------+
```

```sql
WITH items AS
  (SELECT '***apple***' as item
  UNION ALL
  SELECT '***banana***' as item
  UNION ALL
  SELECT '***orange***' as item)

SELECT
  TRIM(item, '*') as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
+---------+
```

```sql
WITH items AS
  (SELECT 'xxxapplexxx' as item
  UNION ALL
  SELECT 'yyybananayyy' as item
  UNION ALL
  SELECT 'zzzorangezzz' as item
  UNION ALL
  SELECT 'xyzpearxyz' as item)

SELECT
  TRIM(item, 'xyz') as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```

### UNICODE

```sql
UNICODE(value)
```

**Description**

Returns the Unicode [code point][string-code-point] for the first character in
`value`. Returns `0` if `value` is empty, or if the resulting Unicode code
point is `0`.

**Return type**

`INT64`

**Examples**

```sql
SELECT UNICODE('âbcd') as A, UNICODE('â') as B, UNICODE('') as C, UNICODE(NULL) as D;

+-------+-------+-------+-------+
| A     | B     | C     | D     |
+-------+-------+-------+-------+
| 226   | 226   | 0     | NULL  |
+-------+-------+-------+-------+
```

### UPPER

```sql
UPPER(value)
```

**Description**

For `STRING` arguments, returns the original string with all alphabetic
characters in uppercase. Mapping between uppercase and lowercase is done
according to the
[Unicode Character Database][string-link-to-unicode-character-definitions]
without taking into account language-specific mappings.

For `BYTES` arguments, the argument is treated as ASCII text, with all bytes
greater than 127 left intact.

**Return type**

`STRING` or `BYTES`

**Examples**

```sql
WITH items AS
  (SELECT
    'foo' as item
  UNION ALL
  SELECT
    'bar' as item
  UNION ALL
  SELECT
    'baz' as item)

SELECT
  UPPER(item) AS example
FROM items;

+---------+
| example |
+---------+
| FOO     |
| BAR     |
| BAZ     |
+---------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[string-link-to-code-points-wikipedia]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-unicode-character-definitions]: http://unicode.org/ucd/

[string-link-to-normalization-wikipedia]: https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization

[string-link-to-case-folding-wikipedia]: https://en.wikipedia.org/wiki/Letter_case#Case_folding

[string-link-to-soundex-wikipedia]: https://en.wikipedia.org/wiki/Soundex

[string-link-to-re2]: https://github.com/google/re2/wiki/Syntax

[string-code-point]: https://en.wikipedia.org/wiki/Code_point

[string-link-to-strpos]: #strpos

[string-link-to-char-length]: #char_length

[string-link-to-code-points]: #to_code_points

[string-link-to-base64]: #to_base64

[string-link-to-trim]: #trim

[string-link-to-normalize]: #normalize

[string-link-to-normalize-casefold]: #normalize_and_casefold

[string-link-to-from-base64]: #from_base64

[string-link-to-codepoints-to-string]: #code_points_to_string

[string-link-to-codepoints-to-bytes]: #code_points_to_bytes

[string-link-to-base32]: #to_base32

[string-link-to-from-base32]: #from_base32

[string-link-to-from-hex]: #from_hex

[string-link-to-to-hex]: #to_hex

[string-link-to-lexical-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#string_and_bytes_literals

[format-specifiers]: #format_specifiers

[format-specifier-list]: #format_specifier_list

[flags]: #flags

[width]: #width

[precision]: #precision

[g-and-g-behavior]: #g_and_g_behavior

[p-and-p-behavior]: #p_and_p_behavior

[t-and-t-behavior]: #t_and_t_behavior

[error-format-specifiers]: #error_format_specifiers

[null-format-specifiers]: #null_format_specifiers

[rules-format-specifiers]: #rules_format_specifiers

[string-link-to-operators]: #operators

<!-- mdlint on -->

## JSON functions

ZetaSQL supports the following functions, which can retrieve and
transform JSON data.

### Function overview

#### Standard JSON extraction functions (recommended)

The following functions use double quotes to escape invalid
[JSONPath][JSONPath-format] characters: <code>"a.b"</code>.

This behavior is consistent with the ANSI standard.

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#json_query"><code>JSON_QUERY</code></a></td>
      <td>
        Extracts a JSON value, such as an array or object, or a JSON
        scalar value, such as a string, number, or boolean.
      </td>
      <td>
        JSON-formatted <code>STRING</code>
         or
        <code>JSON</code>
        
      </td>
    </tr>
    <tr>
      <td><a href="#json_value"><code>JSON_VALUE</code></a></td>
      <td>
        Extracts a scalar value.
        A scalar value can represent a string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if a non-scalar value is selected.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#json_query_array"><code>JSON_QUERY_ARRAY</code></a></td>
      <td>
        Extracts an array of JSON values, such as arrays or objects, and
        JSON scalar values, such as strings, numbers, and booleans.
      </td>
      <td>
        <code>ARRAY&lt;JSON-formatted STRING&gt;</code>
         or
        <code>ARRAY&lt;JSON&gt;</code>
        
      </td>
    </tr>
    
    
    <tr>
      <td><a href="#json_value_array"><code>JSON_VALUE_ARRAY</code></a></td>
      <td>
        Extracts an array of scalar values. A scalar value can represent a
        string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if the selected value is not an array or
        not an array containing only scalar values.
      </td>
      <td><code>ARRAY&lt;STRING&gt;</code></td>
    </tr>
    
  </tbody>
</table>

#### Legacy JSON extraction functions

The following functions use single quotes and brackets to escape invalid
[JSONPath][JSONPath-format] characters: <code>['a.b']</code></td>.

While these functions are supported by ZetaSQL, we recommend using
the functions in the previous table.

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#json_extract"><code>JSON_EXTRACT</code></a></td>
      <td>
        Extracts a JSON value, such as an array or object, or a JSON
        scalar value, such as a string, number, or boolean.
      </td>
      <td>
        JSON-formatted <code>STRING</code>
         or
        <code>JSON</code>
        
      </td>
    </tr>
    <tr>
      <td><a href="#json_extract_scalar"><code>JSON_EXTRACT_SCALAR</code></a></td>
      <td>
        Extracts a scalar value.
        A scalar value can represent a string, number, or boolean.
        Removes the outermost quotes and unescapes the values.
        Returns a SQL <code>NULL</code> if a non-scalar value is selected.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    
  </tbody>
</table>

#### Other JSON functions

<table>
  <thead>
    <tr>
      <th>JSON function</th>
      <th>Description</th>
      <th>Return type</th>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a href="#parse_json"><code>PARSE_JSON</code></a></td>
      <td>
        Takes a JSON-formatted string and returns a JSON value.
      </td>
      <td><code>JSON</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#to_json"><code>TO_JSON</code></a></td>
      <td>
        Takes a SQL value and returns a JSON value.
      </td>
      <td><code>JSON</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#to_json_string"><code>TO_JSON_STRING</code></a></td>
      <td>
        Takes a SQL value and returns a JSON-formatted string
        representation of the value.
      </td>
      <td>JSON-formatted <code>STRING</code></td>
    </tr>
    
    
    
    <tr>
      <td><a href="#string_for_json"><code>STRING</code></a></td>
      <td>
        Extracts a string from JSON.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#bool_for_json"><code>BOOL</code></a></td>
      <td>
        Extracts a boolean from JSON.
      </td>
      <td><code>BOOL</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#int64_for_json"><code>INT64</code></a></td>
      <td>
        Extracts a 64-bit integer from JSON.
      </td>
      <td><code>INT64</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#double_for_json"><code>DOUBLE</code></a></td>
      <td>
        Extracts a 64-bit floating-point number from JSON.
      </td>
      <td><code>DOUBLE</code></td>
    </tr>
    
    
    <tr>
      <td><a href="#json_type"><code>JSON_TYPE</code></a></td>
      <td>
        Returns the type of the outermost JSON value as a string.
      </td>
      <td><code>STRING</code></td>
    </tr>
    
  </tbody>
</table>

### JSON_EXTRACT

Note: This function is deprecated. Consider using [JSON_QUERY][json-query].

```sql
JSON_EXTRACT(json_string_expr, json_path)
```

```sql
JSON_EXTRACT(json_expr, json_path)
```

**Description**

Extracts a JSON value, such as an array or object, or a JSON scalar
value, such as a string, number, or boolean. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using single quotes and brackets.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string "null" is encountered.
    For example:

    ```sql
    SELECT JSON_EXTRACT("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_EXTRACT(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    ```sql
    SELECT JSON_EXTRACT('{"a":null}', "$.a"); -- Returns a SQL NULL
    SELECT JSON_EXTRACT('{"a":null}', "$.b"); -- Returns a SQL NULL
    ```

    
    ```sql
    SELECT JSON_EXTRACT(JSON '{"a":null}', "$.a"); -- Returns a JSON 'null'
    SELECT JSON_EXTRACT(JSON '{"a":null}', "$.b"); -- Returns a SQL NULL
    ```
    

If you want to include non-scalar values such as arrays in the extraction, then
use `JSON_EXTRACT`. If you only want to extract scalar values such strings,
numbers, and booleans, then use `JSON_EXTRACT_SCALAR`.

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_EXTRACT(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')
  AS json_data;

+-----------------------------------+
| json_data                         |
+-----------------------------------+
| {"students":[{"id":5},{"id":12}]} |
+-----------------------------------+
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT JSON_EXTRACT(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------------------------------------------------+
| json_text_string                                          |
+-----------------------------------------------------------+
| {"class":{"students":[{"name":"Jane"}]}}                  |
| {"class":{"students":[]}}                                 |
| {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
+-----------------------------------------------------------+
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------+
| first_student   |
+-----------------+
| {"name":"Jane"} |
| NULL            |
| {"name":"John"} |
+-----------------+
```

```sql
SELECT JSON_EXTRACT(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-------------------+
| second_student    |
+-------------------+
| NULL              |
| NULL              |
| NULL              |
| "Jamie"           |
+-------------------+
```

```sql
SELECT JSON_EXTRACT(json_text, "$.class['students']") AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+------------------------------------+
| student_names                      |
+------------------------------------+
| [{"name":"Jane"}]                  |
| []                                 |
| [{"name":"John"},{"name":"Jamie"}] |
+------------------------------------+
```

### JSON_QUERY

```sql
JSON_QUERY(json_string_expr, json_path)
```

```sql
JSON_QUERY(json_expr, json_path)
```

**Description**

Extracts a JSON value, such as an array or object, or a JSON scalar
value, such as a string, number, or boolean. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string "null" is encountered.
    For example:

    ```sql
    SELECT JSON_QUERY("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_QUERY(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    ```sql
    SELECT JSON_QUERY('{"a":null}', "$.a"); -- Returns a SQL NULL
    SELECT JSON_QUERY('{"a":null}', "$.b"); -- Returns a SQL NULL
    ```

    
    ```sql
    SELECT JSON_QUERY(JSON '{"a":null}', "$.a"); -- Returns a JSON 'null'
    SELECT JSON_QUERY(JSON '{"a":null}', "$.b"); -- Returns a SQL NULL
    ```
    

If you want to include non-scalar values such as arrays in the extraction, then
use `JSON_QUERY`. If you only want to extract scalar values such strings,
numbers, and booleans, then use `JSON_VALUE`.

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_QUERY(JSON '{"class":{"students":[{"id":5},{"id":12}]}}', '$.class')
  AS json_data;

+-----------------------------------+
| json_data                         |
+-----------------------------------+
| {"students":[{"id":5},{"id":12}]} |
+-----------------------------------+
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT JSON_QUERY(json_text, '$') AS json_text_string
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------------------------------------------------+
| json_text_string                                          |
+-----------------------------------------------------------+
| {"class":{"students":[{"name":"Jane"}]}}                  |
| {"class":{"students":[]}}                                 |
| {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
+-----------------------------------------------------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[0]') AS first_student
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-----------------+
| first_student   |
+-----------------+
| {"name":"Jane"} |
| NULL            |
| {"name":"John"} |
+-----------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class.students[1].name') AS second_student_name
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name" : null}]}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+-------------------+
| second_student    |
+-------------------+
| NULL              |
| NULL              |
| NULL              |
| "Jamie"           |
+-------------------+
```

```sql
SELECT JSON_QUERY(json_text, '$.class."students"') AS student_names
FROM UNNEST([
  '{"class" : {"students" : [{"name" : "Jane"}]}}',
  '{"class" : {"students" : []}}',
  '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'
  ]) AS json_text;

+------------------------------------+
| student_names                      |
+------------------------------------+
| [{"name":"Jane"}]                  |
| []                                 |
| [{"name":"John"},{"name":"Jamie"}] |
+------------------------------------+
```

### JSON_EXTRACT_SCALAR

Note: This function is deprecated. Consider using [JSON_VALUE][json-value].

```sql
JSON_EXTRACT_SCALAR(json_string_expr[, json_path])
```

```sql
JSON_EXTRACT_SCALAR(json_expr[, json_path])
```

**Description**

Extracts a scalar value and then returns it as a string. A scalar value can
represent a string, number, or boolean. Removes the outermost quotes and
unescapes the return values. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using single quotes and brackets.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned. If this optional parameter is not provided, then the JSONPath `$`
    symbol is applied, which means that the entire JSON-formatted string is
    analyzed.

If you only want to extract scalar values such strings, numbers, and booleans,
then use `JSON_EXTRACT_SCALAR`. If you want to include non-scalar values such as
arrays in the extraction, then use `JSON_EXTRACT`.

**Return type**

`STRING`

**Examples**

In the following example, `age` is extracted.

```sql
SELECT JSON_EXTRACT_SCALAR(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

+------------+
| scalar_age |
+------------+
| 6          |
+------------+
```

The following example compares how results are returned for the `JSON_EXTRACT`
and `JSON_EXTRACT_SCALAR` functions.

```sql
SELECT JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.name') AS json_name,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.name') AS scalar_name,
  JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.age') AS json_age,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

+-----------+-------------+----------+------------+
| json_name | scalar_name | json_age | scalar_age |
+-----------+-------------+----------+------------+
| "Jakob"   | Jakob       | "6"      | 6          |
+-----------+-------------+----------+------------+
```

```sql
SELECT JSON_EXTRACT('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract,
  JSON_EXTRACT_SCALAR('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract_scalar;

+--------------------+---------------------+
| json_extract       | json_extract_scalar |
+--------------------+---------------------+
| ["apple","banana"] | NULL                |
+--------------------+---------------------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c") AS hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

### JSON_VALUE

```sql
JSON_VALUE(json_string_expr[, json_path])
```

```sql
JSON_VALUE(json_expr[, json_path])
```

**Description**

Extracts a scalar value and then returns it as a string. A scalar value can
represent a string, number, or boolean. Removes the outermost quotes and
unescapes the return values. If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned. If this optional parameter is not provided, then the JSONPath `$`
    symbol is applied, which means that the entire JSON-formatted string is
    analyzed.

If you only want to extract scalar values such strings, numbers, and booleans,
then use `JSON_VALUE`. If you want to include non-scalar values such as arrays
in the extraction, then use `JSON_QUERY`.

**Return type**

`STRING`

**Examples**

In the following example, JSON data is extracted and returned as a scalar value.

```sql
SELECT JSON_VALUE(JSON '{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

+------------+
| scalar_age |
+------------+
| 6          |
+------------+
```

The following example compares how results are returned for the `JSON_QUERY`
and `JSON_VALUE` functions.

```sql
SELECT JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.name') AS json_name,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.name') AS scalar_name,
  JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.age') AS json_age,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.age') AS scalar_age;

+-----------+-------------+----------+------------+
| json_name | scalar_name | json_age | scalar_age |
+-----------+-------------+----------+------------+
| "Jakob"   | Jakob       | "6"      | 6          |
+-----------+-------------+----------+------------+
```

```sql
SELECT JSON_QUERY('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_query,
  JSON_VALUE('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_value;

+--------------------+------------+
| json_query         | json_value |
+--------------------+------------+
| ["apple","banana"] | NULL       |
+--------------------+------------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes. For example:

```sql
SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c') AS hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

### JSON_QUERY_ARRAY

```sql
JSON_QUERY_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_QUERY_ARRAY(json_expr[, json_path])
```

**Description**

Extracts an array of JSON values, such as arrays or objects, and
JSON scalar values, such as strings, numbers, and booleans.
If a JSON key uses invalid
[JSONPath][JSONPath-format] characters, then you can escape those characters
using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

**Return type**

+ `json_string_expr`: `ARRAY<JSON-formatted STRING>`
+ `json_expr`: `ARRAY<JSON>`

**Examples**

This extracts items in JSON to an array of `JSON` values:

```sql
SELECT JSON_QUERY_ARRAY(
  JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits'
  ) AS json_array;

+---------------------------------+
| json_array                      |
+---------------------------------+
| ["apples", "oranges", "grapes"] |
+---------------------------------+
```

This extracts the items in a JSON-formatted string to a string array:

```sql
SELECT JSON_QUERY_ARRAY('[1,2,3]') AS string_array;

+--------------+
| string_array |
+--------------+
| [1, 2, 3]    |
+--------------+
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_QUERY_ARRAY('[1,2,3]','$')
  ) AS integer_element
) AS integer_array;

+---------------+
| integer_array |
+---------------+
| [1, 2, 3]     |
+---------------+
```

This extracts string values in a JSON-formatted string to an array:

```sql
-- Doesn't strip the double quotes
SELECT JSON_QUERY_ARRAY('["apples","oranges","grapes"]', '$') AS string_array;

+---------------------------------+
| string_array                    |
+---------------------------------+
| ["apples", "oranges", "grapes"] |
+---------------------------------+

-- Strips the double quotes
SELECT ARRAY(
  SELECT JSON_VALUE(string_element, '$')
  FROM UNNEST(JSON_QUERY_ARRAY('["apples","oranges","grapes"]','$')) AS string_element
) AS string_array;

+---------------------------+
| string_array              |
+---------------------------+
| [apples, oranges, grapes] |
+---------------------------+
```

This extracts only the items in the `fruit` property to an array:

```sql
SELECT JSON_QUERY_ARRAY(
  '{"fruit":[{"apples":5,"oranges":10},{"apples":2,"oranges":4}],"vegetables":[{"lettuce":7,"kale": 8}]}',
  '$.fruit'
) AS string_array;

+-------------------------------------------------------+
| string_array                                          |
+-------------------------------------------------------+
| [{"apples":5,"oranges":10}, {"apples":2,"oranges":4}] |
+-------------------------------------------------------+
```

These are equivalent:

```sql
SELECT JSON_QUERY_ARRAY('{"fruits":["apples","oranges","grapes"]}','$.fruits') AS string_array;

SELECT JSON_QUERY_ARRAY('{"fruits":["apples","oranges","grapes"]}','$."fruits"') AS string_array;

-- The queries above produce the following result:
+---------------------------------+
| string_array                    |
+---------------------------------+
| ["apples", "oranges", "grapes"] |
+---------------------------------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_QUERY_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

+-----------+
| hello     |
+-----------+
| ["world"] |
+-----------+
```

The following examples show how invalid requests and empty arrays are handled:

```sql
-- An error is returned if you provide an invalid JSONPath.
SELECT JSON_QUERY_ARRAY('["foo","bar","baz"]','INVALID_JSONPath') AS result;

-- If the JSONPath does not refer to an array, then NULL is returned.
SELECT JSON_QUERY_ARRAY('{"a":"foo"}','$.a') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a key that does not exist is specified, then the result is NULL.
SELECT JSON_QUERY_ARRAY('{"a":"foo"}','$.b') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- Empty arrays in JSON-formatted strings are supported.
SELECT JSON_QUERY_ARRAY('{"a":"foo","b":[]}','$.b') AS result;

+--------+
| result |
+--------+
| []     |
+--------+
```

### JSON_VALUE_ARRAY

```sql
JSON_VALUE_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_VALUE_ARRAY(json_expr[, json_path])
```

**Description**

Extracts an array of scalar values and returns an array of string-formatted
scalar values. A scalar value can represent a string, number, or boolean. If a
JSON key uses invalid [JSONPath][JSONPath-format] characters, you can escape
those characters using double quotes.

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class" : {"students" : [{"name" : "Jane"}]}}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

**Return type**

`ARRAY<STRING>`

**Examples**

This extracts items in JSON to a string array:

```sql
SELECT JSON_VALUE_ARRAY(
  JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits'
  ) AS string_array;

+---------------------------+
| string_array              |
+---------------------------+
| [apples, oranges, grapes] |
+---------------------------+
```

The following example compares how results are returned for the
`JSON_QUERY_ARRAY` and `JSON_VALUE_ARRAY` functions.

```sql
SELECT JSON_QUERY_ARRAY('["apples","oranges"]') AS json_array,
       JSON_VALUE_ARRAY('["apples","oranges"]') AS string_array;

+-----------------------+-------------------+
| json_array            | string_array      |
+-----------------------+-------------------+
| ["apples", "oranges"] | [apples, oranges] |
+-----------------------+-------------------+
```

This extracts the items in a JSON-formatted string to a string array:

```sql
-- Strips the double quotes
SELECT JSON_VALUE_ARRAY('["foo","bar","baz"]','$') AS string_array;

+-----------------+
| string_array    |
+-----------------+
| [foo, bar, baz] |
+-----------------+
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_VALUE_ARRAY('[1,2,3]','$')
  ) AS integer_element
) AS integer_array;

+---------------+
| integer_array |
+---------------+
| [1, 2, 3]     |
+---------------+
```

These are equivalent:

```sql
SELECT JSON_VALUE_ARRAY('{"fruits":["apples","oranges","grapes"]}','$.fruits') AS string_array;
SELECT JSON_VALUE_ARRAY('{"fruits":["apples","oranges","grapes"]}','$."fruits"') AS string_array;

-- The queries above produce the following result:
+---------------------------+
| string_array              |
+---------------------------+
| [apples, oranges, grapes] |
+---------------------------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_VALUE_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

+---------+
| hello   |
+---------+
| [world] |
+---------+
```

The following examples explore how invalid requests and empty arrays are
handled:

```sql
-- An error is thrown if you provide an invalid JSONPath.
SELECT JSON_VALUE_ARRAY('["foo","bar","baz"]','INVALID_JSONPath') AS result;

-- If the JSON-formatted string is invalid, then NULL is returned.
SELECT JSON_VALUE_ARRAY('}}','$') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If the JSON document is NULL, then NULL is returned.
SELECT JSON_VALUE_ARRAY(NULL,'$') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a JSONPath does not match anything, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":["foo","bar","baz"]}','$.b') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a JSONPath matches an object that is not an array, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":"foo"}','$') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a JSONPath matches an array of non-scalar objects, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":[{"b":"foo","c":1},{"b":"bar","c":2}],"d":"baz"}','$.a') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a JSONPath matches an array of mixed scalar and non-scalar objects,
-- then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a":[10, {"b": 20}]','$.a') AS result;

+--------+
| result |
+--------+
| NULL   |
+--------+

-- If a JSONPath matches an empty JSON array, then the output is an empty array instead of NULL.
SELECT JSON_VALUE_ARRAY('{"a":"foo","b":[]}','$.b') AS result;

+--------+
| result |
+--------+
| []     |
+--------+

-- If a JSONPath matches an array that contains scalar objects and a JSON null,
-- then the output is an array of the scalar objects and a SQL NULL.
SELECT JSON_VALUE_ARRAY('["world", null, 1]') AS result;

+------------------+
| result           |
+------------------+
| [world, NULL, 1] |
+------------------+

```

### PARSE_JSON

```sql
PARSE_JSON(json_string_expr[, wide_number_mode=>{ 'exact' | 'round' } ])
```

**Description**

Takes a SQL `STRING` value and returns a SQL `JSON` value.
The `STRING` value represents a string-formatted JSON value.

This function supports an optional mandatory-named argument called
`wide_number_mode` that determines how to handle numbers that cannot be stored
in a `JSON` value without the loss of precision. If used,
`wide_number_mode` must include one of these values:

+ `exact`: Only accept numbers that can be stored without loss of precision. If
  a number that cannot be stored without loss of precision is encountered,
  the function throws an error.
+ `round`: If a number that cannot be stored without loss of precision is
  encountered, attempt to round it to a number that can be stored without
  loss of precision. If the number cannot be rounded, the function throws an
  error.

If `wide_number_mode` is not used, the function implicitly includes
`wide_number_mode=>'exact'`. If a number appears in a JSON object or array,
the `wide_number_mode` argument is applied to the number in the object or array.

Numbers from the following domains can be stored in JSON without loss of
precision:

+ 64-bit signed/unsigned integers, such as `INT64`
+ `DOUBLE`

**Return type**

`JSON`

**Examples**

In the following example, a JSON-formatted string is converted to `JSON`.

```sql
SELECT PARSE_JSON('{"coordinates":[10,20],"id":1}') AS json_data;

+--------------------------------+
| json_data                      |
+--------------------------------+
| {"coordinates":[10,20],"id":1} |
+--------------------------------+
```

The following queries fail because:

+ The number that was passed in cannot be stored without loss of precision.
+ `wide_number_mode=>'exact'` is used implicitly in the first query and
  explicitly in the second query.

```sql
SELECT PARSE_JSON('{"id":922337203685477580701}') AS json_data; -- fails
SELECT PARSE_JSON('{"id":922337203685477580701}', wide_number_mode=>'exact') AS json_data; -- fails
```

The following query rounds the number to a number that can be stored in JSON.

```sql
SELECT PARSE_JSON('{"id":922337203685477580701}', wide_number_mode=>'round') AS json_data;

+--------------------------------+
| json_data                      |
+--------------------------------+
| {"id":9.223372036854776e+20}   |
+--------------------------------+
```

### TO_JSON

```sql
TO_JSON(sql_value[, stringify_wide_numbers=>{ TRUE | FALSE } ])
```

**Description**

Takes a SQL value and returns a JSON value. The value
must be a supported ZetaSQL data type. You can review the
ZetaSQL data types that this function supports and their
JSON encodings [here][json-encodings].

This function supports an optional mandatory-named argument called
`stringify_wide_numbers`.

+ If this argument is `TRUE`, numeric values outside
  of the `DOUBLE` type domain are encoded as strings.
+ If this argument is not used or is `FALSE`, numeric values outside
  of the `DOUBLE` type domain are not encoded
  as strings, but are stored as JSON numbers. If a numerical value cannot be
  stored in JSON without loss of precision, an error is thrown.

The following numerical data types are affected by the
`stringify_wide_numbers` argument:

+ `INT64`
+ `UINT64`
+ `NUMERIC`
+ `BIGNUMERIC`

If one of these numerical data types appears in a container data type
such as an `ARRAY` or `STRUCT`, the `stringify_wide_numbers` argument is
applied to the numerical data types in the container data type.

**Return type**

A JSON value

**Examples**

In the following example, the query converts rows in a table to JSON values.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT TO_JSON(t) AS json_objects
FROM CoordinatesTable AS t;

+--------------------------------+
| json_objects                   |
+--------------------------------+
| {"coordinates":[10,20],"id":1} |
| {"coordinates":[30,40],"id":2} |
| {"coordinates":[50,60],"id":3} |
+--------------------------------+
```

In the following example, the query returns a large numerical value as a
JSON string.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>TRUE) as stringify_on

+--------------------+
| stringify_on       |
+--------------------+
| "9007199254740993" |
+--------------------+
```

In the following example, both queries return a large numerical value as a
JSON number.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>FALSE) as stringify_off
SELECT TO_JSON(9007199254740993) as stringify_off

+------------------+
| stringify_off    |
+------------------+
| 9007199254740993 |
+------------------+
```

In the following example, only large numeric values are converted to
JSON strings.

```sql
With T1 AS (
  (SELECT 9007199254740993 AS id) UNION ALL
  (SELECT 2 AS id))
SELECT TO_JSON(t, stringify_wide_numbers=>TRUE) AS json_objects
FROM T1 AS t;

+---------------------------+
| json_objects              |
+---------------------------+
| {"id":"9007199254740993"} |
| {"id":2}                  |
+---------------------------+
```

In this example, the values `9007199254740993` (`INT64`)
and `2.1` (`DOUBLE`) are converted
to the common supertype `DOUBLE`, which is not
affected by the `stringify_wide_numbers` argument.

```sql
With T1 AS (
  (SELECT 9007199254740993 AS id) UNION ALL
  (SELECT 2.1 AS id))
SELECT TO_JSON(t, stringify_wide_numbers=>TRUE) AS json_objects
FROM T1 AS t;

+------------------------------+
| json_objects                 |
+------------------------------+
| {"id":9.007199254740992e+15} |
| {"id":2.1}                   |
+------------------------------+
```

### TO_JSON_STRING

```sql
TO_JSON_STRING(value[, pretty_print])
```

**Description**

Takes a SQL value and returns a JSON-formatted string
representation of the value. The value must be a supported ZetaSQL
data type. You can review the ZetaSQL data types that this function
supports and their JSON encodings [here][json-encodings].

This function supports an optional boolean parameter called `pretty_print`.
If `pretty_print` is `true`, the returned value is formatted for easy
readability.

**Return type**

A JSON-formatted `STRING`

**Examples**

Convert rows in a table to JSON-formatted strings.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t) AS json_data
FROM CoordinatesTable AS t;

+----+-------------+--------------------------------+
| id | coordinates | json_data                      |
+----+-------------+--------------------------------+
| 1  | [10, 20]    | {"id":1,"coordinates":[10,20]} |
| 2  | [30, 40]    | {"id":2,"coordinates":[30,40]} |
| 3  | [50, 60]    | {"id":3,"coordinates":[50,60]} |
+----+-------------+--------------------------------+
```

Convert rows in a table to JSON-formatted strings that are easy to read.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t, true) AS json_data
FROM CoordinatesTable AS t;

+----+-------------+--------------------+
| id | coordinates | json_data          |
+----+-------------+--------------------+
| 1  | [10, 20]    | {                  |
|    |             |   "id": 1,         |
|    |             |   "coordinates": [ |
|    |             |     10,            |
|    |             |     20             |
|    |             |   ]                |
|    |             | }                  |
+----+-------------+--------------------+
| 2  | [30, 40]    | {                  |
|    |             |   "id": 2,         |
|    |             |   "coordinates": [ |
|    |             |     30,            |
|    |             |     40             |
|    |             |   ]                |
|    |             | }                  |
+----+-------------+--------------------+
```

 
### STRING 
<a id="string_for_json"></a>

```sql
STRING(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON string, and returns that value as a SQL
`STRING`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON value is not a string, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`STRING`

**Examples**

```sql
SELECT STRING(JSON '"purple"') AS color;

+--------+
| color  |
+--------+
| purple |
+--------+
```

```sql
SELECT STRING(JSON_QUERY(JSON '{"name": "sky", "color": "blue"}', "$.color")) AS color;

+-------+
| color |
+-------+
| blue  |
+-------+
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not of type string.
SELECT STRING(JSON '123') AS result; -- Throws an error
SELECT STRING(JSON 'null') AS result; -- Throws an error
SELECT SAFE.STRING(JSON '123') AS result; -- Returns a SQL NULL
```

### BOOL 
<a id="bool_for_json"></a>

```sql
BOOL(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON boolean, and returns that value as a SQL
`BOOL`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON value is not a boolean, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`BOOL`

**Examples**

```sql
SELECT BOOL(JSON 'true') AS vacancy;

+---------+
| vacancy |
+---------+
| true    |
+---------+
```

```sql
SELECT BOOL(JSON_QUERY(JSON '{"hotel class": "5-star", "vacancy": true}', "$.vacancy")) AS vacancy;

+---------+
| vacancy |
+---------+
| true    |
+---------+
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type bool.
SELECT BOOL(JSON '123') AS result; -- Throws an error
SELECT BOOL(JSON 'null') AS result; -- Throw an error
SELECT SAFE.BOOL(JSON '123') AS result; -- Returns a SQL NULL
```

### INT64 
<a id="int64_for_json"></a>

```sql
INT64(json_expr)
```

**Description**

Takes a JSON expression, extracts a JSON number and returns that value as a SQL
`INT64`. If the expression is SQL `NULL`, the function returns SQL
`NULL`. If the extracted JSON number has a fractional part or is outside of the
INT64 domain, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`INT64`

**Examples**

```sql
SELECT INT64(JSON '2005') AS flight_number;

+---------------+
| flight_number |
+---------------+
| 2005          |
+---------------+
```

```sql
SELECT INT64(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

+---------------+
| flight_number |
+---------------+
| 2005          |
+---------------+
```

```sql
SELECT INT64(JSON '10.0') AS score;

+-------+
| score |
+-------+
| 10    |
+-------+
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT INT64(JSON '10.1') AS result;  -- Throws an error
SELECT INT64(JSON '"strawberry"') AS result; -- Throws an error
SELECT INT64(JSON 'null') AS result; -- Throws an error
SELECT SAFE.INT64(JSON '"strawberry"') AS result;  -- Returns a SQL NULL

```

### DOUBLE 
<a id="double_for_json"></a>

```sql
DOUBLE(json_expr[, wide_number_mode=>{ 'exact' | 'round'])
```

**Description**

Takes a JSON expression, extracts a JSON number and returns that value as a SQL
`DOUBLE`. If the expression is SQL `NULL`, the
function returns SQL `NULL`. If the extracted JSON value is not a number, an
error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

This function supports an optional mandatory-named argument called
`wide_number_mode` which defines what happens with a number that cannot be
represented as a DOUBLE without loss of precision.

This argument accepts one of the two case-sensitive values:

+   ‘exact’: The function fails if the result cannot be represented as a
    `DOUBLE` without loss of precision.
+   ‘round’: The numeric value stored in JSON will be rounded to
    `DOUBLE`. If such rounding is not possible, the
    function fails. This is the default value if the argument is not specified.

**Return type**

`DOUBLE`

**Examples**

```sql
SELECT DOUBLE(JSON '9.8') AS velocity;

+----------+
| velocity |
+----------+
| 9.8      |
+----------+
```

```sql
SELECT DOUBLE(JSON_QUERY(JSON '{"vo2_max": 39.1, "age": 18}', "$.vo2_max")) AS vo2_max;

+---------+
| vo2_max |
+---------+
| 39.1    |
+---------+
```

```sql
SELECT DOUBLE(JSON '18446744073709551615', wide_number_mode=>'round') as result;

+------------------------+
| result                 |
+------------------------+
| 1.8446744073709552e+19 |
+------------------------+
```

```sql
SELECT DOUBLE(JSON '18446744073709551615') as result;

+------------------------+
| result                 |
+------------------------+
| 1.8446744073709552e+19 |
+------------------------+
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type DOUBLE.
SELECT DOUBLE(JSON '"strawberry"') AS result;
SELECT DOUBLE(JSON 'null') AS result;

-- An error is thrown because `wide_number_mode` is case-sensitive and not "exact" or "round".
SELECT DOUBLE(JSON '123.4', wide_number_mode=>'EXACT') as result;
SELECT DOUBLE(JSON '123.4', wide_number_mode=>'exac') as result;

-- An error is thrown because the number cannot be converted to DOUBLE without loss of precision
SELECT DOUBLE(JSON '18446744073709551615', wide_number_mode=>'exact') as result;

-- Returns a SQL NULL
SELECT SAFE.DOUBLE(JSON '"strawberry"') AS result;
```

### JSON_TYPE 
<a id="json_type"></a>

```sql
JSON_TYPE(json_expr)
```

**Description**

Takes a JSON expression and returns the type of the outermost JSON value as a
SQL `STRING`. The names of these JSON types can be returned:

+ `object`
+ `array`
+ `string`
+ `number`
+ `boolean`
+ `null`

If the expression is SQL `NULL`, the function returns SQL `NULL`. If the
extracted JSON value is not a valid JSON type, an error is produced.

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color" : "blue"}'
    ```

**Return type**

`STRING`

**Examples**

```sql
SELECT json_val, JSON_TYPE(json_val) AS type
FROM
  UNNEST(
    [
      JSON '"apple"',
      JSON '10',
      JSON '3.14',
      JSON 'null',
      JSON '{"city": "New York", "State": "NY"}',
      JSON '["apple", "banana"]',
      JSON 'false'
    ]
  ) AS json_val;

+----------------------------------+---------+
| json_val                         | type    |
+----------------------------------+---------+
| "apple"                          | string  |
| 10                               | number  |
| 3.14                             | number  |
| null                             | null    |
| {"State":"NY","city":"New York"} | object  |
| ["apple","banana"]               | array   |
| false                            | boolean |
+----------------------------------+---------+
```

### JSON encodings 
<a id="json_encodings"></a>

The following table includes common encodings that are used when a
SQL value is encoded as JSON value with
the `TO_JSON_STRING`
or `TO_JSON` function.

<table>
  <thead>
    <tr>
      <th>From SQL</th>
      <th width='400px'>To JSON</th>
      <th>Examples</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>NULL</td>
      <td>
        <p>null</p>
      </td>
      <td>
        SQL input: <code>NULL</code><br />
        JSON output: <code>null</code>
      </td>
    </tr>
    
    <tr>
      <td>BOOL</td>
      <td>boolean</td>
      <td>
        SQL input: <code>TRUE</code><br />
        JSON output: <code>true</code><br />
        <hr />
        SQL input: <code>FALSE</code><br />
        JSON output: <code>false</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>
        INT32<br/>
        UINT32
      </td>
      <td>integer</td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>12345678901</code><br />
        JSON output: <code>12345678901</code><br />
      </td>
    </tr>
    
    
    
    <tr>
      <td>
        INT64
        <br />UINT64
      </td>
      <td>
        <p>(TO_JSON_STRING only)</p>
        <p>number or string</p>
        <p>
          Encoded as a number when the value is in the range of
          [-2<sup>53</sup>, 2<sup>53</sup>], which is the range of
          integers that can be represented losslessly as IEEE 754
          double-precision floating point numbers. A value outside of this range
          is encoded as a string.
        </p>
      <td>
        SQL input: <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        INT64
        <br />UINT64
      </td>
      <td>
        <p>(TO_JSON only)</p>
        <p>number or string</p>
        <p>
          If the <code>stringify_wide_numbers</code> argument
          is <code>TRUE</code> and the value is outside of the
          DOUBLE type domain, the value is
          encoded as a string. If the value cannot be stored in
          JSON without loss of precision, the function fails.
          Otherwise, the value is encoded as a number.
        </p>
        <p>
          If the <code>stringify_wide_numbers</code> is not used or is
          <code>FALSE</code>, numeric values outside of the
          `DOUBLE` type domain are not
          encoded as strings, but are stored as JSON numbers. If a
          numerical value cannot be stored in JSON without loss of precision,
          an error is thrown.
        </p>
      <td>
        SQL input: <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE:
        <code>9007199254740992</code><br />
        JSON output: <code>9007199254740992</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE:
        <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        NUMERIC
        <br/>BIGNUMERIC
      </td>
      <td>
        <p>(TO_JSON_STRING only)</p>
        <p>number or string</p>
        <p>
          Encoded as a number when the value is in the range of
          [-2<sup>53</sup>, 2<sup>53</sup>] and has no fractional
          part. A value outside of this range is encoded as a string.
        </p>
      </td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
        <hr />
        SQL input: <code>123.56</code><br />
        JSON output: <code>"123.56"</code><br />
      </td>
    </tr>
    
    
    
    
    <tr>
      <td>
        NUMERIC
        <br/>BIGNUMERIC
      </td>
      <td>
        <p>(TO_JSON only)</p>
        <p>number or string</p>
        <p>
          If the <code>stringify_wide_numbers</code> argument
          is <code>TRUE</code> and the value is outside of the
          DOUBLE type domain, it is
          encoded as a string. Otherwise, it's encoded as a number.
        </p>
      </td>
      <td>
        SQL input: <code>-1</code><br />
        JSON output: <code>-1</code><br />
        <hr />
        SQL input: <code>0</code><br />
        JSON output: <code>0</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input: <code>123.56</code><br />
        JSON output: <code>123.56</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE: <code>9007199254740993</code><br />
        JSON output: <code>"9007199254740993"</code><br />
        <hr />
        SQL input with stringify_wide_numbers=>TRUE: <code>123.56</code><br />
        JSON output: <code>123.56</code><br />
      </td>
    </tr>
    
    
    
    <tr>
      <td>
        FLOAT<br />
        DOUBLE
      </td>
      <td>
        <p>number or string</p>
        <p>
          <code>+/-inf</code> and <code>NaN</code> are encoded as
          <code>Infinity</code>, <code>-Infinity</code>, and <code>NaN</code>.
          Otherwise, this value is encoded as a number.
        </p>
      </td>
      <td>
        SQL input: <code>1.0</code><br />
        JSON output: <code>1</code><br />
        <hr />
        SQL input: <code>9007199254740993</code><br />
        JSON output: <code>9007199254740993</code><br />
        <hr />
        SQL input: <code>"+inf"</code><br />
        JSON output: <code>"Infinity"</code><br />
        <hr />
        SQL input: <code>"-inf"</code><br />
        JSON output: <code>"-Infinity"</code><br />
        <hr />
        SQL input: <code>"NaN"</code><br />
        JSON output: <code>"NaN"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>STRING</td>
      <td>
        <p>string</p>
        <p>
          Encoded as a string, escaped according to the JSON standard.
          Specifically, <code>"</code>, <code>\</code>, and the control
          characters from <code>U+0000</code> to <code>U+001F</code> are
          escaped.
        </p>
      </td>
      <td>
        SQL input: <code>"abc"</code><br />
        JSON output: <code>"abc"</code><br />
        <hr />
        SQL input: <code>"\"abc\""</code><br />
        JSON output: <code>"\"abc\""</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>BYTES</td>
      <td>
        <p>string</p>
        <p>Uses RFC 4648 Base64 data encoding.</p>
      </td>
      <td>
        SQL input: <code>b"Google"</code><br />
        JSON output: <code>"R29vZ2xl"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>ENUM</td>
      <td>
        <p>string</p>
        <p>
          Invalid enum values are encoded as their number, such as 0 or 42.
        </p>
      </td>
      <td>
        SQL input: <code>Color.Red</code><br />
        JSON output: <code>"Red"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>DATE</td>
      <td>string</td>
      <td>
        SQL input: <code>DATE '2017-03-06'</code><br />
        JSON output: <code>"2017-03-06"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>TIMESTAMP</td>
      <td>
        <p>string</p>
        <p>
          Encoded as ISO 8601 date and time, where T separates the date and
          time and Z (Zulu/UTC) represents the time zone.
        </p>
      </td>
      <td>
        SQL input: <code>TIMESTAMP '2017-03-06 12:34:56.789012'</code><br />
        JSON output: <code>"2017-03-06T12:34:56.789012Z"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>DATETIME</td>
      <td>
        <p>string</p>
        <p>
          Encoded as ISO 8601 date and time, where T separates the date and
          time.
        </p>
      </td>
      <td>
        SQL input: <code>DATETIME '2017-03-06 12:34:56.789012'</code><br />
        JSON output: <code>"2017-03-06T12:34:56.789012"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>TIME</td>
      <td>
        <p>string</p>
        <p>Encoded as ISO 8601 time.</p>
      </td>
      <td>
        SQL input: <code>TIME '12:34:56.789012'</code><br />
        JSON output: <code>"12:34:56.789012"</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>JSON</td>
      <td>
        <p>data of the input JSON</p>
      </td>
      <td>
        SQL input: <code>JSON '{"item": "pen", "price": 10}'</code><br />
        JSON output: <code>{"item":"pen", "price":10}</code><br />
        <hr />
        SQL input:<code>[1, 2, 3]</code><br />
        JSON output:<code>[1, 2, 3]</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>ARRAY</td>
      <td>
        <p>array</p>
        <p>
          Can contain zero or more elements.
        </p>
      </td>
      <td>
        SQL input: <code>["red", "blue", "green"]</code><br />
        JSON output: <code>["red","blue","green"]</code><br />
        <hr />
        SQL input:<code>[1, 2, 3]</code><br />
        JSON output:<code>[1,2,3]</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>STRUCT</td>
      <td>
        <p>object</p>
        <p>
          The object can contain zero or more key/value pairs.
          Each value is formatted according to its type.
        </p>
        <p>
          For <code>TO_JSON</code>, a field is
          included in the output string and any duplicates of this field are
          omitted.
          For <code>TO_JSON_STRING</code>,
          a field and any duplicates of this field are included in the
          output string.
        </p>
        <p>
          Anonymous fields are represented with <code>""</code>.
        </p>
        <p>
          Invalid UTF-8 field names might result in unparseable JSON. String
          values are escaped according to the JSON standard. Specifically,
          <code>"</code>, <code>\</code>, and the control characters from
          <code>U+0000</code> to <code>U+001F</code> are escaped.
        </p>
      </td>
      <td>
        SQL input: <code>STRUCT(12 AS purchases, TRUE AS inStock)</code><br />
        JSON output: <code>{"inStock": true,"purchases":12}</code><br />
      </td>
    </tr>
    
    
    <tr>
      <td>PROTO</td>
      <td>
        <p>object</p>
        <p>
          The object can contain zero or more key/value pairs.
          Each value is formatted according to its type.
        </p>
        <p>
          Field names with underscores are converted to camel-case in accordance
          with
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. Field values are formatted according to
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. If a <code>field_value</code> is a
          non-empty repeated field or submessage, elements/fields are indented
          to the appropriate level.
        </p>
        <ul>
          <li>
            Field names that are not valid UTF-8 might result in unparseable
            JSON.
          </li>
          <li>Field annotations are ignored.</li>
          <li>Repeated fields are represented as arrays.</li>
          <li>Submessages are formatted as values of PROTO type.</li>
          <li>
            Extension fields are included in the output, where the extension
            field name is enclosed in brackets and prefixed with the full name
            of the extension type.
          </li>
          
        </ul>
      </td>
      <td>
        SQL input: <code>NEW Item(12 AS purchases,TRUE AS in_Stock)</code><br />
        JSON output: <code>{"purchases":12,"inStock": true}</code><br />
      </td>
    </tr>
    
  </tbody>
</table>

### JSONPath 
<a id="JSONPath_format"></a>

Most JSON functions pass in a `json_string_expr` and `json_path`
parameter. The `json_string_expr` parameter passes in a JSON-formatted
string, and the `json_path` parameter identifies the value or
values you want to obtain from the JSON-formatted string.

The `json_string_expr` parameter must be a JSON string that is
formatted like this:

```json
'{"class" : {"students" : [{"name" : "Jane"}]}}'
```

You construct the `json_path` parameter using the
[JSONPath][json-path] format. As part of this format, this parameter must start
with a `$` symbol, which refers to the outermost level of the JSON-formatted
string. You can identify child values using dots. If the JSON object is an
array, you can use brackets to specify the array index. If the keys contain
`$`, dots, or brackets, refer to each JSON function for how to escape
them.

JSONPath | Description            | Example               | Result using the above `json_string_expr`
-------- | ---------------------- | --------------------- | -----------------------------------------
$        | Root object or element | "$"                   | `{"class":{"students":[{"name":"Jane"}]}}`
.        | Child operator         | "$.class.students"    | `[{"name":"Jane"}]`
[]       | Subscript operator     | "$.class.students[0]" | `{"name":"Jane"}`

A JSON functions returns `NULL` if the `json_path` parameter does
not match a value in `json_string_expr`. If the selected value for a scalar
function is not scalar, such as an object or an array, the function
returns `NULL`.

If the JSONPath is invalid, the function raises an error.

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[json-query]: #json_query

[json-value]: #json_value

[json-query-array]: #json_query_array

[json-value-array]: #json_value_array

[json-encodings]: #json_encodings

[JSONPath-format]: #JSONPath_format

[json-path]: https://github.com/json-path/JSONPath#operators

<!-- mdlint on -->

## Array functions

### ARRAY

```sql
ARRAY(subquery)
```

**Description**

The `ARRAY` function returns an `ARRAY` with one element for each row in a
[subquery][subqueries].

If `subquery` produces a
[SQL table][datamodel-sql-tables],
the table must have exactly one column. Each element in the output `ARRAY` is
the value of the single column of a row in the table.

If `subquery` produces a
[value table][datamodel-value-tables],
then each element in the output `ARRAY` is the entire corresponding row of the
value table.

**Constraints**

+ Subqueries are unordered, so the elements of the output `ARRAY` are not
guaranteed to preserve any order in the source table for the subquery. However,
if the subquery includes an `ORDER BY` clause, the `ARRAY` function will return
an `ARRAY` that honors that clause.
+ If the subquery returns more than one column, the `ARRAY` function returns an
error.
+ If the subquery returns an `ARRAY` typed column or `ARRAY` typed rows, the
  `ARRAY` function returns an error: ZetaSQL does not support
  `ARRAY`s with elements of type
  [`ARRAY`][array-data-type].
+ If the subquery returns zero rows, the `ARRAY` function returns an empty
`ARRAY`. It never returns a `NULL` `ARRAY`.

**Return type**

ARRAY

**Examples**

```sql
SELECT ARRAY
  (SELECT 1 UNION ALL
   SELECT 2 UNION ALL
   SELECT 3) AS new_array;

+-----------+
| new_array |
+-----------+
| [1, 2, 3] |
+-----------+
```

To construct an `ARRAY` from a subquery that contains multiple
columns, change the subquery to use `SELECT AS STRUCT`. Now
the `ARRAY` function will return an `ARRAY` of `STRUCT`s. The `ARRAY` will
contain one `STRUCT` for each row in the subquery, and each of these `STRUCT`s
will contain a field for each column in that row.

```sql
SELECT
  ARRAY
    (SELECT AS STRUCT 1, 2, 3
     UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array;

+------------------------+
| new_array              |
+------------------------+
| [{1, 2, 3}, {4, 5, 6}] |
+------------------------+
```

Similarly, to construct an `ARRAY` from a subquery that contains
one or more `ARRAY`s, change the subquery to use `SELECT AS STRUCT`.

```sql
SELECT ARRAY
  (SELECT AS STRUCT [1, 2, 3] UNION ALL
   SELECT AS STRUCT [4, 5, 6]) AS new_array;

+----------------------------+
| new_array                  |
+----------------------------+
| [{[1, 2, 3]}, {[4, 5, 6]}] |
+----------------------------+
```

### ARRAY_CONCAT

```sql
ARRAY_CONCAT(array_expression[, ...])
```

**Description**

Concatenates one or more arrays with the same element type into a single array.

The function returns `NULL` if any input argument is `NULL`.

Note: You can also use the [|| concatenation operator][array-link-to-operators]
to concatenate arrays.

**Return type**

ARRAY

**Examples**

```sql
SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;

+--------------------------------------------------+
| count_to_six                                     |
+--------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                               |
+--------------------------------------------------+
```

### ARRAY_FILTER

```sql
ARRAY_FILTER(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias->boolean_expression
    | (element_alias, index_alias)->boolean_expression
  }
```

**Description**

Takes an array, filters out unwanted elements, and returns the results in a new
array.

+   `array_expression`: The array to filter.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition]. If the expression evaluates to
    `FALSE` or `NULL`, the element is removed from the resulting array.
+   `element_alias`: An alias that represents an array element.
+   `index_alias`: An alias that represents the zero-based offset of the array
    element.
+   `boolean_expression`: The predicate used to filter the array elements.

Returns `NULL` if the `array_expression` is `NULL`.

**Return type**

ARRAY

**Example**

```sql
SELECT
  ARRAY_FILTER([1,2,3], e->e>1) AS a1,
  ARRAY_FILTER([0,2,3], (e, i)->e>i) AS a2;

+-------+-------+
| a1    | a2    |
+-------+-------+
| [2,3] | [2,3] |
+-------+-------+
```

### ARRAY_INCLUDES

+   [Signature 1](#array_includes_signature1): `ARRAY_INCLUDES(array_expression,
    target_element)`
+   [Signature 2](#array_includes_signature2): `ARRAY_INCLUDES(array_expression,
    lambda_expression)`

#### Signature 1 
<a id="array_includes_signature1"></a>

```sql
ARRAY_INCLUDES(array_expression, target_element)
```

**Description**

Takes an array and returns `TRUE` if there is an element in the array that is
equal to the target element.

+   `array_expression`: The array to search.
+   `target_element`: The target element to search for in the array.

Returns `NULL` if `array_expression` or `target_element` is `NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if `0` exists in an
array. Then the query checks to see if `1` exists in an array.

```sql
SELECT
  ARRAY_INCLUDES([1,2,3], 0) AS a1,
  ARRAY_INCLUDES([1,2,3], 1) AS a2;

+-------+------+
| a1    | a2   |
+-------+------+
| false | true |
+-------+------+
```

#### Signature 2 
<a id="array_includes_signature2"></a>

```sql
ARRAY_INCLUDES(array_expression, lambda_expression)

lambda_expression: element_alias->boolean_expression
```

**Description**

Takes an array and returns `TRUE` if the lambda expression evaluates to `TRUE`
for any element in the array.

+   `array_expression`: The array to search.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition].
+   `element_alias`: An alias that represents an array element.
+   `boolean_expression`: The predicate used to evaluate the array elements.

Returns `NULL` if `array_expression` is `NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if any elements that are
greater than 3 exist in an array (`e>3`). Then the query checks to see if any
any elements that are greater than 0 exist in an array (`e>0`).

```sql
SELECT
  ARRAY_INCLUDES([1,2,3], e->e>3) AS a1,
  ARRAY_INCLUDES([1,2,3], e->e>0) AS a2;

+-------+------+
| a1    | a2   |
+-------+------+
| false | true |
+-------+------+
```

### ARRAY_INCLUDES_ANY

```sql
ARRAY_INCLUDES_ANY(source_array_expression, target_array_expression)
```

**Description**

Takes a source and target array. Returns `TRUE` if any elements in the target
array are in the source array, otherwise returns `FALSE`.

+   `source_array_expression`: The array to search.
+   `target_array_expression`: The target array that contains the elements to
    search for in the source array.

Returns `NULL` if `source_array_expression` or `target_array_expression` is
`NULL`.

**Return type**

BOOL

**Example**

In the following example, the query first checks to see if `3`, `4`, or `5`
exists in an array. Then the query checks to see if `4`, `5`, or `6` exists in
an array.

```sql
SELECT
  ARRAY_INCLUDES_ANY([1,2,3], [3,4,5]) AS a1,
  ARRAY_INCLUDES_ANY([1,2,3], [4,5,6]) AS a2;

+------+-------+
| a1   | a2    |
+------+-------+
| true | false |
+------+-------+
```

### ARRAY_LENGTH

```sql
ARRAY_LENGTH(array_expression)
```

**Description**

Returns the size of the array. Returns 0 for an empty array. Returns `NULL` if
the `array_expression` is `NULL`.

**Return type**

INT64

**Examples**

```sql
WITH items AS
  (SELECT ["coffee", NULL, "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list)
SELECT ARRAY_TO_STRING(list, ', ', 'NULL'), ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;

+---------------------------------+------+
| list                            | size |
+---------------------------------+------+
| [coffee, NULL, milk]            | 3    |
| [cake, pie]                     | 2    |
+---------------------------------+------+
```

### ARRAY_TO_STRING

```sql
ARRAY_TO_STRING(array_expression, delimiter[, null_text])
```

**Description**

Returns a concatenation of the elements in `array_expression`
as a STRING. The value for `array_expression`
can either be an array of STRING or
BYTES data types.

If the `null_text` parameter is used, the function replaces any `NULL` values in
the array with the value of `null_text`.

If the `null_text` parameter is not used, the function omits the `NULL` value
and its preceding delimiter.

**Examples**

```sql
WITH items AS
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie                      |
+--------------------------------+
```

```sql
WITH items AS
  (SELECT ["coffee", "tea", "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie", NULL] as list)

SELECT ARRAY_TO_STRING(list, '--', 'MISSING') AS text
FROM items;

+--------------------------------+
| text                           |
+--------------------------------+
| coffee--tea--milk              |
| cake--pie--MISSING             |
+--------------------------------+
```

### ARRAY_TRANSFORM

```sql
ARRAY_TRANSFORM(array_expression, lambda_expression)

lambda_expression:
  {
    element_alias->transform_expression
    | (element_alias, index_alias)->transform_expression
  }
```

**Description**

Takes an array, transforms the elements, and returns the results in a new array.

+   `array_expression`: The array to transform.
+   `lambda_expression`: Each element in `array_expression` is evaluated against
    the [lambda expression][lambda-definition]. The evaluation results are
    returned in a new array.
+   `element_alias`: An alias that represents an array element.
+   `index_alias`: An alias that represents the zero-based offset of the array
    element.
+   `transform_expression`: The expression used to transform the array elements.

Returns `NULL` if the `array_expression` is `NULL`.

**Return type**

ARRAY

**Example**

```sql
SELECT
  ARRAY_TRANSFORM([1,2,3], e->e+1) AS a1,
  ARRAY_TRANSFORM([1,2,3], (e, i)->e+i) AS a2;

+---------+---------+
| a1      | a2      |
+---------+---------+
| [2,3,4] | [1,3,5] |
+---------+---------+
```

### FLATTEN

```sql
FLATTEN(array_elements_field_access_expression)
```

**Description**

Takes a nested array and flattens a specific part of it into a single, flat
array with the
[array elements field access operator][array-el-field-operator].
Returns `NULL` if the input value is `NULL`.
If `NULL` array elements are
encountered, they are added to the resulting array.

There are several ways to flatten nested data into arrays. To learn more, see
[Flattening nested data into an array][flatten-tree-to-array].

**Return type**

`ARRAY`

**Examples**

In the following example, all of the arrays for `v.sales.quantity` are
concatenated in a flattened array.

```sql
WITH t AS (
  SELECT
  [
    STRUCT([STRUCT([1,2,3] AS quantity), STRUCT([4,5,6] AS quantity)] AS sales),
    STRUCT([STRUCT([7,8] AS quantity), STRUCT([] AS quantity)] AS sales)
  ] AS v
)
SELECT FLATTEN(v.sales.quantity) AS all_values
FROM t;

+--------------------------+
| all_values               |
+--------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8] |
+--------------------------+
```

In the following example, `OFFSET` gets the second value in each array and
concatenates them.

```sql
WITH t AS (
  SELECT
  [
    STRUCT([STRUCT([1,2,3] AS quantity), STRUCT([4,5,6] AS quantity)] AS sales),
    STRUCT([STRUCT([7,8,9] AS quantity), STRUCT([10,11,12] AS quantity)] AS sales)
  ] AS v
)
SELECT FLATTEN(v.sales.quantity[OFFSET(1)]) AS second_values
FROM t;

+---------------+
| second_values |
+---------------+
| [2, 5, 8, 11] |
+---------------+
```

In the following example, all values for `v.price` are returned in a
flattened array.

```sql
WITH t AS (
  SELECT
  [
    STRUCT(1 AS price, 2 AS quantity),
    STRUCT(10 AS price, 20 AS quantity)
  ] AS v
)
SELECT FLATTEN(v.price) AS all_prices
FROM t;

+------------+
| all_prices |
+------------+
| [1, 10]    |
+------------+
```

For more examples, including how to use protocol buffers with `FLATTEN`, see the
[array elements field access operator][array-el-field-operator].

### GENERATE_ARRAY

```sql
GENERATE_ARRAY(start_expression, end_expression[, step_expression])
```

**Description**

Returns an array of values. The `start_expression` and `end_expression`
parameters determine the inclusive start and end of the array.

The `GENERATE_ARRAY` function accepts the following data types as inputs:

<ul>
<li>INT64</li><li>UINT64</li><li>NUMERIC</li><li>BIGNUMERIC</li><li>DOUBLE</li>
</ul>

The `step_expression` parameter determines the increment used to
generate array values. The default value for this parameter is `1`.

This function returns an error if `step_expression` is set to 0, or if any
input is `NaN`.

If any argument is `NULL`, the function will return a `NULL` array.

**Return Data Type**

ARRAY

**Examples**

The following returns an array of integers, with a default step of 1.

```sql
SELECT GENERATE_ARRAY(1, 5) AS example_array;

+-----------------+
| example_array   |
+-----------------+
| [1, 2, 3, 4, 5] |
+-----------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_ARRAY(0, 10, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| [0, 3, 6, 9]  |
+---------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_ARRAY(10, 0, -3) AS example_array;

+---------------+
| example_array |
+---------------+
| [10, 7, 4, 1] |
+---------------+
```

The following returns an array using the same value for the `start_expression`
and `end_expression`.

```sql
SELECT GENERATE_ARRAY(4, 4, 10) AS example_array;

+---------------+
| example_array |
+---------------+
| [4]           |
+---------------+
```

The following returns an empty array, because the `start_expression` is greater
than the `end_expression`, and the `step_expression` value is positive.

```sql
SELECT GENERATE_ARRAY(10, 0, 3) AS example_array;

+---------------+
| example_array |
+---------------+
| []            |
+---------------+
```

The following returns a `NULL` array because `end_expression` is `NULL`.

```sql
SELECT GENERATE_ARRAY(5, NULL, 1) AS example_array;

+---------------+
| example_array |
+---------------+
| NULL          |
+---------------+
```

The following returns multiple arrays.

```sql
SELECT GENERATE_ARRAY(start, 5) AS example_array
FROM UNNEST([3, 4, 5]) AS start;

+---------------+
| example_array |
+---------------+
| [3, 4, 5]     |
| [4, 5]        |
| [5]           |
+---------------+
```

### GENERATE_DATE_ARRAY

```sql
GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])
```

**Description**

Returns an array of dates. The `start_date` and `end_date`
parameters determine the inclusive start and end of the array.

The `GENERATE_DATE_ARRAY` function accepts the following data types as inputs:

+ `start_date` must be a DATE
+ `end_date` must be a DATE
+ `INT64_expr` must be an INT64
+ `date_part` must be either DAY, WEEK, MONTH, QUARTER, or YEAR.

The `INT64_expr` parameter determines the increment used to generate dates. The
default value for this parameter is 1 day.

This function returns an error if `INT64_expr` is set to 0.

**Return Data Type**

An ARRAY containing 0 or more DATE values.

**Examples**

The following returns an array of dates, with a default step of 1.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;

+--------------------------------------------------+
| example                                          |
+--------------------------------------------------+
| [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
+--------------------------------------------------+
```

The following returns an array using a user-specified step size.

```sql
SELECT GENERATE_DATE_ARRAY(
 '2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example;

+--------------------------------------+
| example                              |
+--------------------------------------+
| [2016-10-05, 2016-10-07, 2016-10-09] |
+--------------------------------------+
```

The following returns an array using a negative value, `-3` for its step size.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL -3 DAY) AS example;

+--------------------------+
| example                  |
+--------------------------+
| [2016-10-05, 2016-10-02] |
+--------------------------+
```

The following returns an array using the same value for the `start_date`and
`end_date`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-05', INTERVAL 8 DAY) AS example;

+--------------+
| example      |
+--------------+
| [2016-10-05] |
+--------------+
```

The following returns an empty array, because the `start_date` is greater
than the `end_date`, and the `step` value is positive.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05',
  '2016-10-01', INTERVAL 1 DAY) AS example;

+---------+
| example |
+---------+
| []      |
+---------+
```

The following returns a `NULL` array, because one of its inputs is
`NULL`.

```sql
SELECT GENERATE_DATE_ARRAY('2016-10-05', NULL) AS example;

+---------+
| example |
+---------+
| NULL    |
+---------+
```

The following returns an array of dates, using MONTH as the `date_part`
interval:

```sql
SELECT GENERATE_DATE_ARRAY('2016-01-01',
  '2016-12-31', INTERVAL 2 MONTH) AS example;

+--------------------------------------------------------------------------+
| example                                                                  |
+--------------------------------------------------------------------------+
| [2016-01-01, 2016-03-01, 2016-05-01, 2016-07-01, 2016-09-01, 2016-11-01] |
+--------------------------------------------------------------------------+
```

The following uses non-constant dates to generate an array.

```sql
SELECT GENERATE_DATE_ARRAY(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM (
  SELECT DATE '2016-01-01' AS date_start, DATE '2016-01-31' AS date_end
  UNION ALL SELECT DATE "2016-04-01", DATE "2016-04-30"
  UNION ALL SELECT DATE "2016-07-01", DATE "2016-07-31"
  UNION ALL SELECT DATE "2016-10-01", DATE "2016-10-31"
) AS items;

+--------------------------------------------------------------+
| date_range                                                   |
+--------------------------------------------------------------+
| [2016-01-01, 2016-01-08, 2016-01-15, 2016-01-22, 2016-01-29] |
| [2016-04-01, 2016-04-08, 2016-04-15, 2016-04-22, 2016-04-29] |
| [2016-07-01, 2016-07-08, 2016-07-15, 2016-07-22, 2016-07-29] |
| [2016-10-01, 2016-10-08, 2016-10-15, 2016-10-22, 2016-10-29] |
+--------------------------------------------------------------+
```

### GENERATE_TIMESTAMP_ARRAY

```sql
GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp,
                         INTERVAL step_expression date_part)
```

**Description**

Returns an `ARRAY` of `TIMESTAMPS` separated by a given interval. The
`start_timestamp` and `end_timestamp` parameters determine the inclusive
lower and upper bounds of the `ARRAY`.

The `GENERATE_TIMESTAMP_ARRAY` function accepts the following data types as
inputs:

+ `start_timestamp`: `TIMESTAMP`
+ `end_timestamp`: `TIMESTAMP`
+ `step_expression`: `INT64`
+ Allowed `date_part` values are:
  `NANOSECOND`
  (if the SQL engine supports it),
  `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, or `DAY`.

The `step_expression` parameter determines the increment used to generate
timestamps.

**Return Data Type**

An `ARRAY` containing 0 or more
`TIMESTAMP` values.

**Examples**

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1 day.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00',
                                INTERVAL 1 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-06 00:00:00+00, 2016-10-07 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMP`s at intervals of 1
second.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:02',
                                INTERVAL 1 SECOND) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 00:00:01+00, 2016-10-05 00:00:02+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` of `TIMESTAMPS` with a negative
interval.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-01 00:00:00',
                                INTERVAL -2 DAY) AS timestamp_array;

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-06 00:00:00+00, 2016-10-04 00:00:00+00, 2016-10-02 00:00:00+00] |
+--------------------------------------------------------------------------+
```

The following example returns an `ARRAY` with a single element, because
`start_timestamp` and `end_timestamp` have the same value.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+--------------------------+
| timestamp_array          |
+--------------------------+
| [2016-10-05 00:00:00+00] |
+--------------------------+
```

The following example returns an empty `ARRAY`, because `start_timestamp` is
later than `end_timestamp`.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-06 00:00:00', '2016-10-05 00:00:00',
                                INTERVAL 1 HOUR) AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| []              |
+-----------------+
```

The following example returns a null `ARRAY`, because one of the inputs is
`NULL`.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', NULL, INTERVAL 1 HOUR)
  AS timestamp_array;

+-----------------+
| timestamp_array |
+-----------------+
| NULL            |
+-----------------+
```

The following example generates `ARRAY`s of `TIMESTAMP`s from columns containing
values for `start_timestamp` and `end_timestamp`.

```sql
SELECT GENERATE_TIMESTAMP_ARRAY(start_timestamp, end_timestamp, INTERVAL 1 HOUR)
  AS timestamp_array
FROM
  (SELECT
    TIMESTAMP '2016-10-05 00:00:00' AS start_timestamp,
    TIMESTAMP '2016-10-05 02:00:00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 12:00:00' AS start_timestamp,
    TIMESTAMP '2016-10-05 14:00:00' AS end_timestamp
   UNION ALL
   SELECT
    TIMESTAMP '2016-10-05 23:59:00' AS start_timestamp,
    TIMESTAMP '2016-10-06 01:59:00' AS end_timestamp);

+--------------------------------------------------------------------------+
| timestamp_array                                                          |
+--------------------------------------------------------------------------+
| [2016-10-05 00:00:00+00, 2016-10-05 01:00:00+00, 2016-10-05 02:00:00+00] |
| [2016-10-05 12:00:00+00, 2016-10-05 13:00:00+00, 2016-10-05 14:00:00+00] |
| [2016-10-05 23:59:00+00, 2016-10-06 00:59:00+00, 2016-10-06 01:59:00+00] |
+--------------------------------------------------------------------------+
```

### ARRAY_REVERSE

```sql
ARRAY_REVERSE(value)
```

**Description**

Returns the input ARRAY with elements in reverse order.

**Return type**

ARRAY

**Examples**

```sql
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [4, 5] AS arr UNION ALL
  SELECT [] AS arr
)
SELECT
  arr,
  ARRAY_REVERSE(arr) AS reverse_arr
FROM example;

+-----------+-------------+
| arr       | reverse_arr |
+-----------+-------------+
| [1, 2, 3] | [3, 2, 1]   |
| [4, 5]    | [5, 4]      |
| []        | []          |
+-----------+-------------+
```

### ARRAY_IS_DISTINCT

```sql
ARRAY_IS_DISTINCT(value)
```

**Description**

Returns true if the array contains no repeated elements, using the same equality
comparison logic as `SELECT DISTINCT`.

**Return type**

BOOL

**Examples**

```sql
WITH example AS (
  SELECT [1, 2, 3] AS arr UNION ALL
  SELECT [1, 1, 1] AS arr UNION ALL
  SELECT [1, 2, NULL] AS arr UNION ALL
  SELECT [1, 1, NULL] AS arr UNION ALL
  SELECT [1, NULL, NULL] AS arr UNION ALL
  SELECT [] AS arr UNION ALL
  SELECT CAST(NULL AS ARRAY<INT64>) AS arr
)
SELECT
  arr,
  ARRAY_IS_DISTINCT(arr) as is_distinct
FROM example;

+-----------------+-------------+
| arr             | is_distinct |
+-----------------+-------------+
| [1, 2, 3]       | true        |
| [1, 1, 1]       | false       |
| [1, 2, NULL]    | true        |
| [1, 1, NULL]    | false       |
| [1, NULL, NULL] | false       |
| []              | true        |
| NULL            | NULL        |
+-----------------+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[array-subscript-operator]: #array_subscript_operator

[subqueries]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#subqueries

[datamodel-sql-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#standard_sql_tables

[datamodel-value-tables]: https://github.com/google/zetasql/blob/master/docs/data-model.md#value_tables

[array-data-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#array_type

[flatten-tree-to-array]: https://github.com/google/zetasql/blob/master/docs/arrays.md#flattening_nested_data_into_arrays

[array-el-field-operator]: #array_el_field_operator

[array-link-to-operators]: #operators

[lambda-definition]: https://github.com/google/zetasql/blob/master/docs/functions-reference.md#lambdas

<!-- mdlint on -->

## Date functions

ZetaSQL supports the following `DATE` functions.

### CURRENT_DATE

```sql
CURRENT_DATE([time_zone])
```

**Description**

Returns the current date as of the specified or default timezone. Parentheses
are optional when called with no
arguments.

 This function supports an optional
`time_zone` parameter. This parameter is a string representing the timezone to
use. If no timezone is specified, the default timezone, which is implementation defined,
is used. See [Timezone definitions][date-functions-link-to-timezone-definitions]
for information on how to specify a time zone.

If the `time_zone` parameter evaluates to `NULL`, this function returns `NULL`.

**Return Data Type**

DATE

**Example**

```sql
SELECT CURRENT_DATE() AS the_date;

+--------------+
| the_date     |
+--------------+
| 2016-12-25   |
+--------------+
```

When a column named `current_date` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][date-functions-link-to-range-variables]. For example, the
following query will select the function in the `the_date` column and the table
column in the `current_date` column.

```sql
WITH t AS (SELECT 'column value' AS `current_date`)
SELECT current_date() AS the_date, t.current_date FROM t;

+------------+--------------+
| the_date   | current_date |
+------------+--------------+
| 2016-12-25 | column value |
+------------+--------------+
```

### EXTRACT

```sql
EXTRACT(part FROM date_expression)
```

**Description**

Returns the value corresponding to the specified date part. The `part` must
be one of:

+   `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day
    of the week.
+   `DAY`
+   `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53]. Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of the date in the range [0, 53].
  Weeks begin on `WEEKDAY`. Dates prior to
  the first `WEEKDAY` of the year are in week 0. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `date_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+   `MONTH`
+   `QUARTER`: Returns values in the range [1,4].
+   `YEAR`
+   `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
    week-numbering year, which is the Gregorian calendar year containing the
    Thursday of the week to which `date_expression` belongs.

**Return Data Type**

INT64

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
date part.

```sql
SELECT EXTRACT(DAY FROM DATE '2013-12-25') AS the_day;

+---------+
| the_day |
+---------+
| 25      |
+---------+
```

In the following example, `EXTRACT` returns values corresponding to different
date parts from a column of dates near the end of the year.

```sql
SELECT
  date,
  EXTRACT(ISOYEAR FROM date) AS isoyear,
  EXTRACT(ISOWEEK FROM date) AS isoweek,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(WEEK FROM date) AS week
FROM UNNEST(GENERATE_DATE_ARRAY('2015-12-23', '2016-01-09')) AS date
ORDER BY date;
+------------+---------+---------+------+------+
| date       | isoyear | isoweek | year | week |
+------------+---------+---------+------+------+
| 2015-12-23 | 2015    | 52      | 2015 | 51   |
| 2015-12-24 | 2015    | 52      | 2015 | 51   |
| 2015-12-25 | 2015    | 52      | 2015 | 51   |
| 2015-12-26 | 2015    | 52      | 2015 | 51   |
| 2015-12-27 | 2015    | 52      | 2015 | 52   |
| 2015-12-28 | 2015    | 53      | 2015 | 52   |
| 2015-12-29 | 2015    | 53      | 2015 | 52   |
| 2015-12-30 | 2015    | 53      | 2015 | 52   |
| 2015-12-31 | 2015    | 53      | 2015 | 52   |
| 2016-01-01 | 2015    | 53      | 2016 | 0    |
| 2016-01-02 | 2015    | 53      | 2016 | 0    |
| 2016-01-03 | 2015    | 53      | 2016 | 1    |
| 2016-01-04 | 2016    | 1       | 2016 | 1    |
| 2016-01-05 | 2016    | 1       | 2016 | 1    |
| 2016-01-06 | 2016    | 1       | 2016 | 1    |
| 2016-01-07 | 2016    | 1       | 2016 | 1    |
| 2016-01-08 | 2016    | 1       | 2016 | 1    |
| 2016-01-09 | 2016    | 1       | 2016 | 1    |
+------------+---------+---------+------+------+
```

In the following example, `date_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT DATE('2017-11-05') AS date)
SELECT
  date,
  EXTRACT(WEEK(SUNDAY) FROM date) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM date) AS week_monday FROM table;

+------------+-------------+-------------+
| date       | week_sunday | week_monday |
+------------+-------------+-------------+
| 2017-11-05 | 45          | 44          |
+------------+-------------+-------------+
```

### DATE

```sql
1. DATE(year, month, day)
2. DATE(timestamp_expression[, timezone])
3. DATE(datetime_expression)
```

**Description**

1. Constructs a DATE from INT64 values representing
   the year, month, and day.
2. Extracts the DATE from a TIMESTAMP expression. It supports an
   optional parameter to [specify a timezone][date-functions-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Extracts the DATE from a DATETIME expression.

**Return Data Type**

DATE

**Example**

```sql
SELECT
  DATE(2016, 12, 25) AS date_ymd,
  DATE(DATETIME "2016-12-25 23:59:59") AS date_dt,
  DATE(TIMESTAMP "2016-12-25 05:30:00+07", "America/Los_Angeles") AS date_tstz;

+------------+------------+------------+
| date_ymd   | date_dt    | date_tstz  |
+------------+------------+------------+
| 2016-12-25 | 2016-12-25 | 2016-12-24 |
+------------+------------+------------+
```

### DATE_ADD

```sql
DATE_ADD(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds a specified time interval to a DATE.

`DATE_ADD` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_ADD(DATE "2008-12-25", INTERVAL 5 DAY) AS five_days_later;

+--------------------+
| five_days_later    |
+--------------------+
| 2008-12-30         |
+--------------------+
```

### DATE_SUB

```sql
DATE_SUB(date_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts a specified time interval from a DATE.

`DATE_SUB` supports the following `date_part` values:

+  `DAY`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `MONTH`
+  `QUARTER`
+  `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when
the date is at (or near) the last day of the month. If the resulting
month has fewer days than the original date's day, then the resulting
date is the last date of that month.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_SUB(DATE "2008-12-25", INTERVAL 5 DAY) AS five_days_ago;

+---------------+
| five_days_ago |
+---------------+
| 2008-12-20    |
+---------------+
```

### DATE_DIFF

```sql
DATE_DIFF(date_expression_a, date_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
`DATE` objects (`date_expression_a` - `date_expression_b`).
If the first `DATE` is earlier than the second one,
the output is negative.

`DATE_DIFF` supports the following `date_part` values:

+  `DAY`
+  `WEEK` This date part begins on Sunday.
+  `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
   `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
   boundaries. ISO weeks begin on Monday.
+  `MONTH`, except when the first two arguments are `TIMESTAMP` objects.
+  `QUARTER`
+  `YEAR`
+  `ISOYEAR`: Uses the [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

INT64

**Example**

```sql
SELECT DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY) AS days_diff;

+-----------+
| days_diff |
+-----------+
| 559       |
+-----------+
```

```sql
SELECT
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', DAY) AS days_diff,
  DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', WEEK) AS weeks_diff;

+-----------+------------+
| days_diff | weeks_diff |
+-----------+------------+
| 1         | 1          |
+-----------+------------+
```

The example above shows the result of `DATE_DIFF` for two days in succession.
`DATE_DIFF` with the date part `WEEK` returns 1 because `DATE_DIFF` counts the
number of date part boundaries in this range of dates. Each `WEEK` begins on
Sunday, so there is one date part boundary between Saturday, 2017-10-14
and Sunday, 2017-10-15.

The following example shows the result of `DATE_DIFF` for two dates in different
years. `DATE_DIFF` with the date part `YEAR` returns 3 because it counts the
number of Gregorian calendar year boundaries between the two dates. `DATE_DIFF`
with the date part `ISOYEAR` returns 2 because the second date belongs to the
ISO year 2015. The first Thursday of the 2015 calendar year was 2015-01-01, so
the ISO year 2015 begins on the preceding Monday, 2014-12-29.

```sql
SELECT
  DATE_DIFF('2017-12-30', '2014-12-30', YEAR) AS year_diff,
  DATE_DIFF('2017-12-30', '2014-12-30', ISOYEAR) AS isoyear_diff;

+-----------+--------------+
| year_diff | isoyear_diff |
+-----------+--------------+
| 3         | 2            |
+-----------+--------------+
```

The following example shows the result of `DATE_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATE_DIFF` with the date part `WEEK` returns 0 because this date part
uses weeks that begin on Sunday. `DATE_DIFF` with the date part `WEEK(MONDAY)`
returns 1. `DATE_DIFF` with the date part `ISOWEEK` also returns 1 because
ISO weeks begin on Monday.

```sql
SELECT
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATE_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

+-----------+-------------------+--------------+
| week_diff | week_weekday_diff | isoweek_diff |
+-----------+-------------------+--------------+
| 0         | 1                 | 1            |
+-----------+-------------------+--------------+
```

### DATE_TRUNC

```sql
DATE_TRUNC(date_expression, date_part)
```

**Description**

Truncates the date to the specified granularity.

`DATE_TRUNC` supports the following values for `date_part`:

+  `DAY`
+  `WEEK`
+  `WEEK(<WEEKDAY>)`: Truncates `date_expression` to the preceding week
   boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
   `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
   `SATURDAY`.
+  `ISOWEEK`: Truncates `date_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+  `MONTH`
+  `QUARTER`
+  `YEAR`
+  `ISOYEAR`: Truncates `date_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

DATE

**Examples**

```sql
SELECT DATE_TRUNC(DATE '2008-12-25', MONTH) AS month;

+------------+
| month      |
+------------+
| 2008-12-01 |
+------------+
```

In the following example, the original date falls on a Sunday. Because
the `date_part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATE` for the
preceding Monday.

```sql
SELECT date AS original, DATE_TRUNC(date, WEEK(MONDAY)) AS truncated
FROM (SELECT DATE('2017-11-05') AS date);

+------------+------------+
| original   | truncated  |
+------------+------------+
| 2017-11-05 | 2017-10-30 |
+------------+------------+
```

In the following example, the original `date_expression` is in the Gregorian
calendar year 2015. However, `DATE_TRUNC` with the `ISOYEAR` date part
truncates the `date_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `date_expression` 2015-06-15 is
2014-12-29.

```sql
SELECT
  DATE_TRUNC('2015-06-15', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATE '2015-06-15') AS isoyear_number;

+------------------+----------------+
| isoyear_boundary | isoyear_number |
+------------------+----------------+
| 2014-12-29       | 2015           |
+------------------+----------------+
```

### DATE_FROM_UNIX_DATE

```sql
DATE_FROM_UNIX_DATE(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of days since 1970-01-01.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE_FROM_UNIX_DATE(14238) AS date_from_epoch;

+-----------------+
| date_from_epoch |
+-----------------+
| 2008-12-25      |
+-----------------+
```

### FORMAT_DATE

```sql
FORMAT_DATE(format_string, date_expr)
```

**Description**

Formats the `date_expr` according to the specified `format_string`.

See [Supported Format Elements For DATE][date-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

STRING

**Examples**

```sql
SELECT FORMAT_DATE("%x", DATE "2008-12-25") AS US_format;

+------------+
| US_format  |
+------------+
| 12/25/08   |
+------------+
```

```sql
SELECT FORMAT_DATE("%b-%d-%Y", DATE "2008-12-25") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT FORMAT_DATE("%b %Y", DATE "2008-12-25") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

### LAST_DAY

```sql
LAST_DAY(date_expression[, date_part])
```

**Description**

Returns the last day from a date expression. This is commonly used to return
the last day of the month.

You can optionally specify the date part for which the last day is returned.
If this parameter is not used, the default value is `MONTH`.
`LAST_DAY` supports the following values for `date_part`:

+  `YEAR`
+  `QUARTER`
+  `MONTH`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `WEEK(<WEEKDAY>)`. `<WEEKDAY>` represents the starting day of the week.
   Valid values are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`. Uses [ISO 8601][ISO-8601-week] week boundaries. ISO weeks begin
   on Monday.
+  `ISOYEAR`. Uses the [ISO 8601][ISO-8601] week-numbering year boundary.
   The ISO year boundary is the Monday of the first week whose Thursday belongs
   to the corresponding Gregorian calendar year.

**Return Data Type**

`DATE`

**Example**

These both return the last day of the month:

```sql
SELECT LAST_DAY(DATE '2008-11-25', MONTH) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

```sql
SELECT LAST_DAY(DATE '2008-11-25') AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

This returns the last day of the year:

```sql
SELECT LAST_DAY(DATE '2008-11-25', YEAR) AS last_day

+------------+
| last_day   |
+------------+
| 2008-12-31 |
+------------+
```

This returns the last day of the week for a week that starts on a Sunday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(SUNDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-15 |
+------------+
```

This returns the last day of the week for a week that starts on a Monday:

```sql
SELECT LAST_DAY(DATE '2008-11-10', WEEK(MONDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-16 |
+------------+
```

### PARSE_DATE

```sql
PARSE_DATE(format_string, date_string)
```

**Description**

Converts a [string representation of date][date-format] to a
`DATE` object.

`format_string` contains the [format elements][date-format-elements]
that define how `date_string` is formatted. Each element in
`date_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `date_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_DATE("%A %b %e %Y", "Thursday Dec 25 2008")

-- This doesn't work because the year element is in different locations.
SELECT PARSE_DATE("%Y %A %b %e", "Thursday Dec 25 2008")

-- This doesn't work because one of the year elements is missing.
SELECT PARSE_DATE("%A %b %e", "Thursday Dec 25 2008")

-- This works because %F can find all matching elements in date_string.
SELECT PARSE_DATE("%F", "2000-12-30")
```

The format string fully supports most format elements except for
`%a`, `%A`, `%g`, `%G`, `%j`, `%u`, `%U`, `%V`, `%w`, and `%W`.

When using `PARSE_DATE`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01`.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the date string. In
  addition, leading and trailing white spaces in the date string are always
  allowed -- even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones.

**Return Data Type**

DATE

**Examples**

This example converts a `MM/DD/YY` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE("%x", "12/25/08") AS parsed;

+------------+
| parsed     |
+------------+
| 2008-12-25 |
+------------+
```

This example converts a `YYYYMMDD` formatted string to a `DATE` object:

```sql
SELECT PARSE_DATE("%Y%m%d", "20081225") AS parsed;

+------------+
| parsed     |
+------------+
| 2008-12-25 |
+------------+
```

### UNIX_DATE

```sql
UNIX_DATE(date_expression)
```

**Description**

Returns the number of days since 1970-01-01.

**Return Data Type**

INT64

**Example**

```sql
SELECT UNIX_DATE(DATE "2008-12-25") AS days_from_epoch;

+-----------------+
| days_from_epoch |
+-----------------+
| 14238           |
+-----------------+
```

### Supported format elements for DATE

Unless otherwise noted, DATE functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
    <td>Wednesday</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
    <td>Wed</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
    <td>January</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
    <td>Jan</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal
number (00-99).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
    <td>2021-01-20</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
    year without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
    <td>020</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
    <td>01</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
    <td>1</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
   <td>3</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
    <td>3</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y produces as many
    characters as it takes to fully render the year.</td>
    <td>2021</td>
 </tr>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[date-format]: #format_date

[date-format-elements]: #supported_format_elements_for_date

[date-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[date-functions-link-to-timezone-definitions]: #timezone_definitions

<!-- mdlint on -->

## Datetime functions

ZetaSQL supports the following `DATETIME` functions.

### CURRENT_DATETIME

```sql
CURRENT_DATETIME([timezone])
```

**Description**

Returns the current time as a `DATETIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `timezone` parameter.
See [Timezone definitions][datetime-link-to-timezone-definitions] for
information on how to specify a time zone.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT CURRENT_DATETIME() as now;

+----------------------------+
| now                        |
+----------------------------+
| 2016-05-19 10:38:47.046465 |
+----------------------------+
```

When a column named `current_datetime` is present, the column name and the
function call without parentheses are ambiguous. To ensure the function call,
add parentheses; to ensure the column name, qualify it with its
[range variable][datetime-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_datetime` column.

```sql
WITH t AS (SELECT 'column value' AS `current_datetime`)
SELECT current_datetime() as now, t.current_datetime FROM t;

+----------------------------+------------------+
| now                        | current_datetime |
+----------------------------+------------------+
| 2016-05-19 10:38:47.046465 | column value     |
+----------------------------+------------------+
```

### DATETIME

```sql
1. DATETIME(year, month, day, hour, minute, second)
2. DATETIME(date_expression[, time_expression])
3. DATETIME(timestamp_expression [, timezone])
```

**Description**

1. Constructs a `DATETIME` object using INT64 values
   representing the year, month, day, hour, minute, and second.
2. Constructs a `DATETIME` object using a DATE object and an optional TIME object.
3. Constructs a `DATETIME` object using a TIMESTAMP object. It supports an
   optional parameter to [specify a timezone][datetime-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME(2008, 12, 25, 05, 30, 00) as datetime_ymdhms,
  DATETIME(TIMESTAMP "2008-12-25 05:30:00+00", "America/Los_Angeles") as datetime_tstz;

+---------------------+---------------------+
| datetime_ymdhms     | datetime_tstz       |
+---------------------+---------------------+
| 2008-12-25 05:30:00 | 2008-12-24 21:30:00 |
+---------------------+---------------------+
```

### EXTRACT

```sql
EXTRACT(part FROM datetime_expression)
```

**Description**

Returns a value that corresponds to the
specified `part` from a supplied `datetime_expression`.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day of
   of the week.
+ `DAY`
+ `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53].  Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of `datetime_expression` in the
  range [0, 53]. Weeks begin on `WEEKDAY`.
  `datetime`s prior to the first `WEEKDAY` of the year are in week 0. Valid
  values for `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`,
  `THURSDAY`, `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `datetime_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
  week-numbering year, which is the Gregorian calendar year containing the
  Thursday of the week to which `date_expression` belongs.
+ `DATE`
+ `TIME`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`, except in the following cases:

+ If `part` is `DATE`, returns a `DATE` object.
+ If `part` is `TIME`, returns a `TIME` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00)) as hour;

+------------------+
| hour             |
+------------------+
| 15               |
+------------------+
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of datetimes.

```sql
WITH Datetimes AS (
  SELECT DATETIME '2005-01-03 12:34:56' AS datetime UNION ALL
  SELECT DATETIME '2007-12-31' UNION ALL
  SELECT DATETIME '2009-01-01' UNION ALL
  SELECT DATETIME '2009-12-31' UNION ALL
  SELECT DATETIME '2017-01-02' UNION ALL
  SELECT DATETIME '2017-05-26'
)
SELECT
  datetime,
  EXTRACT(ISOYEAR FROM datetime) AS isoyear,
  EXTRACT(ISOWEEK FROM datetime) AS isoweek,
  EXTRACT(YEAR FROM datetime) AS year,
  EXTRACT(WEEK FROM datetime) AS week
FROM Datetimes
ORDER BY datetime;

+---------------------+---------+---------+------+------+
| datetime            | isoyear | isoweek | year | week |
+---------------------+---------+---------+------+------+
| 2005-01-03 12:34:56 | 2005    | 1       | 2005 | 1    |
| 2007-12-31 00:00:00 | 2008    | 1       | 2007 | 52   |
| 2009-01-01 00:00:00 | 2009    | 1       | 2009 | 0    |
| 2009-12-31 00:00:00 | 2009    | 53      | 2009 | 52   |
| 2017-01-02 00:00:00 | 2017    | 1       | 2017 | 1    |
| 2017-05-26 00:00:00 | 2017    | 21      | 2017 | 21   |
+---------------------+---------+---------+------+------+
```

In the following example, `datetime_expression` falls on a Sunday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT DATETIME(TIMESTAMP "2017-11-05 00:00:00+00", "UTC") AS datetime)
SELECT
  datetime,
  EXTRACT(WEEK(SUNDAY) FROM datetime) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM datetime) AS week_monday
FROM table;

+---------------------+-------------+---------------+
| datetime            | week_sunday | week_monday   |
+---------------------+-------------+---------------+
| 2017-11-05 00:00:00 | 45          | 44            |
+---------------------+-------------+---------------+
```

### DATETIME_ADD

```sql
DATETIME_ADD(datetime_expression, INTERVAL int64_expression part)
```

**Description**

Adds `int64_expression` units of `part` to the `DATETIME` object.

`DATETIME_ADD` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`. Equivalent to 7 `DAY`s.
+ `MONTH`
+ `QUARTER`
+ `YEAR`

Special handling is required for MONTH, QUARTER, and YEAR parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original DATETIME's day, then the result day is the last day of
the new month.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as later;

+-----------------------------+------------------------+
| original_date               | later                  |
+-----------------------------+------------------------+
| 2008-12-25 15:30:00         | 2008-12-25 15:40:00    |
+-----------------------------+------------------------+
```

### DATETIME_SUB

```sql
DATETIME_SUB(datetime_expression, INTERVAL int64_expression part)
```

**Description**

Subtracts `int64_expression` units of `part` from the `DATETIME`.

`DATETIME_SUB` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`. Equivalent to 7 `DAY`s.
+ `MONTH`
+ `QUARTER`
+ `YEAR`

Special handling is required for `MONTH`, `QUARTER`, and `YEAR` parts when the
date is at (or near) the last day of the month. If the resulting month has fewer
days than the original `DATETIME`'s day, then the result day is the last day of
the new month.

**Return Data Type**

`DATETIME`

**Example**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original_date,
  DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as earlier;

+-----------------------------+------------------------+
| original_date               | earlier                |
+-----------------------------+------------------------+
| 2008-12-25 15:30:00         | 2008-12-25 15:20:00    |
+-----------------------------+------------------------+
```

### DATETIME_DIFF

```sql
DATETIME_DIFF(datetime_expression_a, datetime_expression_b, part)
```

**Description**

Returns the whole number of specified `part` intervals between two
`DATETIME` objects (`datetime_expression_a` - `datetime_expression_b`).
If the first `DATETIME` is earlier than the second one,
the output is negative. Throws an error if the computation overflows the
result type, such as if the difference in
nanoseconds
between the two `DATETIME` objects would overflow an
`INT64` value.

`DATETIME_DIFF` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`: This date part begins on Sunday.
+ `WEEK(<WEEKDAY>)`: This date part begins on `WEEKDAY`. Valid values for
  `WEEKDAY` are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
  `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Uses [ISO 8601 week][ISO-8601-week]
  boundaries. ISO weeks begin on Monday.
+ `MONTH`, except when the first two arguments are `TIMESTAMP` objects.
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Uses the [ISO 8601][ISO-8601]
  week-numbering year boundary. The ISO year boundary is the Monday of the
  first week whose Thursday belongs to the corresponding Gregorian calendar
  year.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  DATETIME "2010-07-07 10:20:00" as first_datetime,
  DATETIME "2008-12-25 15:30:00" as second_datetime,
  DATETIME_DIFF(DATETIME "2010-07-07 10:20:00",
    DATETIME "2008-12-25 15:30:00", DAY) as difference;

+----------------------------+------------------------+------------------------+
| first_datetime             | second_datetime        | difference             |
+----------------------------+------------------------+------------------------+
| 2010-07-07 10:20:00        | 2008-12-25 15:30:00    | 559                    |
+----------------------------+------------------------+------------------------+
```

```sql
SELECT
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', DAY) as days_diff,
  DATETIME_DIFF(DATETIME '2017-10-15 00:00:00',
    DATETIME '2017-10-14 00:00:00', WEEK) as weeks_diff;

+-----------+------------+
| days_diff | weeks_diff |
+-----------+------------+
| 1         | 1          |
+-----------+------------+
```

The example above shows the result of `DATETIME_DIFF` for two `DATETIME`s that
are 24 hours apart. `DATETIME_DIFF` with the part `WEEK` returns 1 because
`DATETIME_DIFF` counts the number of part boundaries in this range of
`DATETIME`s. Each `WEEK` begins on Sunday, so there is one part boundary between
Saturday, `2017-10-14 00:00:00` and Sunday, `2017-10-15 00:00:00`.

The following example shows the result of `DATETIME_DIFF` for two dates in
different years. `DATETIME_DIFF` with the date part `YEAR` returns 3 because it
counts the number of Gregorian calendar year boundaries between the two
`DATETIME`s. `DATETIME_DIFF` with the date part `ISOYEAR` returns 2 because the
second `DATETIME` belongs to the ISO year 2015. The first Thursday of the 2015
calendar year was 2015-01-01, so the ISO year 2015 begins on the preceding
Monday, 2014-12-29.

```sql
SELECT
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', YEAR) AS year_diff,
  DATETIME_DIFF('2017-12-30 00:00:00',
    '2014-12-30 00:00:00', ISOYEAR) AS isoyear_diff;

+-----------+--------------+
| year_diff | isoyear_diff |
+-----------+--------------+
| 3         | 2            |
+-----------+--------------+
```

The following example shows the result of `DATETIME_DIFF` for two days in
succession. The first date falls on a Monday and the second date falls on a
Sunday. `DATETIME_DIFF` with the date part `WEEK` returns 0 because this time
part uses weeks that begin on Sunday. `DATETIME_DIFF` with the date part
`WEEK(MONDAY)` returns 1. `DATETIME_DIFF` with the date part
`ISOWEEK` also returns 1 because ISO weeks begin on Monday.

```sql
SELECT
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK) AS week_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', WEEK(MONDAY)) AS week_weekday_diff,
  DATETIME_DIFF('2017-12-18', '2017-12-17', ISOWEEK) AS isoweek_diff;

+-----------+-------------------+--------------+
| week_diff | week_weekday_diff | isoweek_diff |
+-----------+-------------------+--------------+
| 0         | 1                 | 1            |
+-----------+-------------------+--------------+
```

### DATETIME_TRUNC

```sql
DATETIME_TRUNC(datetime_expression, part)
```

**Description**

Truncates a `DATETIME` object to the granularity of `part`.

`DATETIME_TRUNC` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`: Truncates `datetime_expression` to the preceding week
  boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Truncates `datetime_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Truncates `datetime_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

**Return Data Type**

`DATETIME`

**Examples**

```sql
SELECT
  DATETIME "2008-12-25 15:30:00" as original,
  DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY) as truncated;

+----------------------------+------------------------+
| original                   | truncated              |
+----------------------------+------------------------+
| 2008-12-25 15:30:00        | 2008-12-25 00:00:00    |
+----------------------------+------------------------+
```

In the following example, the original `DATETIME` falls on a Sunday. Because the
`part` is `WEEK(MONDAY)`, `DATE_TRUNC` returns the `DATETIME` for the
preceding Monday.

```sql
SELECT
 datetime AS original,
 DATETIME_TRUNC(datetime, WEEK(MONDAY)) AS truncated
FROM (SELECT DATETIME(TIMESTAMP "2017-11-05 00:00:00+00", "UTC") AS datetime);

+---------------------+---------------------+
| original            | truncated           |
+---------------------+---------------------+
| 2017-11-05 00:00:00 | 2017-10-30 00:00:00 |
+---------------------+---------------------+
```

In the following example, the original `datetime_expression` is in the Gregorian
calendar year 2015. However, `DATETIME_TRUNC` with the `ISOYEAR` date part
truncates the `datetime_expression` to the beginning of the ISO year, not the
Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `datetime_expression`
2015-06-15 00:00:00 is 2014-12-29.

```sql
SELECT
  DATETIME_TRUNC('2015-06-15 00:00:00', ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM DATETIME '2015-06-15 00:00:00') AS isoyear_number;

+---------------------+----------------+
| isoyear_boundary    | isoyear_number |
+---------------------+----------------+
| 2014-12-29 00:00:00 | 2015           |
+---------------------+----------------+
```

### FORMAT_DATETIME

```sql
FORMAT_DATETIME(format_string, datetime_expression)
```

**Description**

Formats a `DATETIME` object according to the specified `format_string`. See
[Supported Format Elements For DATETIME][datetime-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Examples**

```sql
SELECT
  FORMAT_DATETIME("%c", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+--------------------------+
| formatted                |
+--------------------------+
| Thu Dec 25 15:30:00 2008 |
+--------------------------+
```

```sql
SELECT
  FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT
  FORMAT_DATETIME("%b %Y", DATETIME "2008-12-25 15:30:00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

### LAST_DAY

```sql
LAST_DAY(datetime_expression[, date_part])
```

**Description**

Returns the last day from a datetime expression that contains the date.
This is commonly used to return the last day of the month.

You can optionally specify the date part for which the last day is returned.
If this parameter is not used, the default value is `MONTH`.
`LAST_DAY` supports the following values for `date_part`:

+  `YEAR`
+  `QUARTER`
+  `MONTH`
+  `WEEK`. Equivalent to 7 `DAY`s.
+  `WEEK(<WEEKDAY>)`. `<WEEKDAY>` represents the starting day of the week.
   Valid values are `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`,
   `FRIDAY`, and `SATURDAY`.
+  `ISOWEEK`. Uses [ISO 8601][ISO-8601-week] week boundaries. ISO weeks begin
   on Monday.
+  `ISOYEAR`. Uses the [ISO 8601][ISO-8601] week-numbering year boundary.
   The ISO year boundary is the Monday of the first week whose Thursday belongs
   to the corresponding Gregorian calendar year.

**Return Data Type**

`DATE`

**Example**

These both return the last day of the month:

```sql
SELECT LAST_DAY(DATETIME '2008-11-25', MONTH) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

```sql
SELECT LAST_DAY(DATETIME '2008-11-25') AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-30 |
+------------+
```

This returns the last day of the year:

```sql
SELECT LAST_DAY(DATETIME '2008-11-25 15:30:00', YEAR) AS last_day

+------------+
| last_day   |
+------------+
| 2008-12-31 |
+------------+
```

This returns the last day of the week for a week that starts on a Sunday:

```sql
SELECT LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(SUNDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-15 |
+------------+
```

This returns the last day of the week for a week that starts on a Monday:

```sql
SELECT LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(MONDAY)) AS last_day

+------------+
| last_day   |
+------------+
| 2008-11-16 |
+------------+
```

### PARSE_DATETIME

```sql
PARSE_DATETIME(format_string, datetime_string)
```
**Description**

Converts a [string representation of a datetime][datetime-format] to a
`DATETIME` object.

`format_string` contains the [format elements][datetime-format-elements]
that define how `datetime_string` is formatted. Each element in
`datetime_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `datetime_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because the year element is in different locations.
SELECT PARSE_DATETIME("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because one of the year elements is missing.
SELECT PARSE_DATETIME("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This works because %c can find all matching elements in datetime_string.
SELECT PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008")
```

The format string fully supports most format elements, except for
`%a`, `%A`, `%g`, `%G`, `%j`, `%P`, `%u`, `%U`, `%V`, `%w`, and `%W`.

`PARSE_DATETIME` parses `string` according to the following rules:

+ **Unspecified fields.** Any unspecified field is initialized from
  `1970-01-01 00:00:00.0`. For example, if the year is unspecified then it
  defaults to `1970`.
+ **Case insensitivity.** Names, such as `Monday` and `February`,
  are case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the
  `DATETIME` string. Leading and trailing
  white spaces in the `DATETIME` string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two or more format elements have overlapping
  information, the last one generally overrides any earlier ones, with some
  exceptions. For example, both `%F` and `%Y` affect the year, so the earlier
  element overrides the later. See the descriptions
  of `%s`, `%C`, and `%y` in
  [Supported Format Elements For DATETIME][datetime-format-elements].
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`DATETIME`

**Examples**

The following examples parse a `STRING` literal as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1998-10-18 13:45:55') AS datetime;

+---------------------+
| datetime            |
+---------------------+
| 1998-10-18 13:45:55 |
+---------------------+
```

```sql
SELECT PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', '8/30/2018 2:23:38 pm') AS datetime

+---------------------+
| datetime            |
+---------------------+
| 2018-08-30 14:23:38 |
+---------------------+
```

The following example parses a `STRING` literal
containing a date in a natural language format as a
`DATETIME`.

```sql
SELECT PARSE_DATETIME('%A, %B %e, %Y','Wednesday, December 19, 2018')
  AS datetime;

+---------------------+
| datetime            |
+---------------------+
| 2018-12-19 00:00:00 |
+---------------------+
```

### Supported format elements for DATETIME

Unless otherwise noted, `DATETIME` functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
    <td>Wednesday</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
    <td>Wed</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
    <td>January</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
    <td>Jan</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal number (00-99).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%c</td>
    <td>The date and time representation.</td>
    <td>Wed Jan 20 21:47:00 2021</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
    <td>2021-01-20</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
    <td>09</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
    <td>020</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
    <td>9</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
    <td47></td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
    <td>01</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
    <td>pm</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
    <td>PM</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
    <td>1</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
    <td>21:47</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
    <td>09:47:00 PM</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
    <td>00</td>
 </tr>
 <tr>
    <td>%s</td>
    <td>The number of seconds since 1970-01-01 00:00:00. Always overrides all
    other format elements, independent of where %s appears in the string.
    If multiple %s elements appear, then the last one takes precedence.</td>
    <td>1611179220</td>
</tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
    <td>3</td>
</tr>
 <tr>
    <td>%V</td>
    <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
    <td>3</td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
    <td>%</td>
 </tr>
 <tr>
    <td>%E&lt;number&gt;S</td>
    <td>Seconds with &lt;number&gt; digits of fractional precision.</td>
    <td>00.000 for %E3S</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
    <td>00.123456</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y
    produces as many characters as it takes to fully render the
year.</td>
    <td>2021</td>
 </tr>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[datetime-format]: #format_datetime

[datetime-format-elements]: #supported_format_elements_for_datetime

[datetime-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[datetime-link-to-timezone-definitions]: #timezone_definitions

<!-- mdlint on -->

## Time functions

ZetaSQL supports the following `TIME` functions.

### CURRENT_TIME

```sql
CURRENT_TIME([timezone])
```

**Description**

Returns the current time as a `TIME` object. Parentheses are optional when
called with no arguments.

This function supports an optional `timezone` parameter.
See [Timezone definitions][time-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT CURRENT_TIME() as now;

+----------------------------+
| now                        |
+----------------------------+
| 15:31:38.776361            |
+----------------------------+
```

When a column named `current_time` is present, the column name and the function
call without parentheses are ambiguous. To ensure the function call, add
parentheses; to ensure the column name, qualify it with its
[range variable][time-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_time` column.

```sql
WITH t AS (SELECT 'column value' AS `current_time`)
SELECT current_time() as now, t.current_time FROM t;

+-----------------+--------------+
| now             | current_time |
+-----------------+--------------+
| 15:31:38.776361 | column value |
+-----------------+--------------+
```

### TIME

```sql
1. TIME(hour, minute, second)
2. TIME(timestamp, [timezone])
3. TIME(datetime)
```

**Description**

1. Constructs a `TIME` object using `INT64`
   values representing the hour, minute, and second.
2. Constructs a `TIME` object using a `TIMESTAMP` object. It supports an
   optional
   parameter to [specify a timezone][time-link-to-timezone-definitions]. If no
   timezone is specified, the default timezone, which is implementation defined, is used.
3. Constructs a `TIME` object using a
  `DATETIME` object.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME(15, 30, 00) as time_hms,
  TIME(TIMESTAMP "2008-12-25 15:30:00+08", "America/Los_Angeles") as time_tstz;

+----------+-----------+
| time_hms | time_tstz |
+----------+-----------+
| 15:30:00 | 23:30:00  |
+----------+-----------+
```

```sql
SELECT TIME(DATETIME "2008-12-25 15:30:00.000000") AS time_dt;

+----------+
| time_dt  |
+----------+
| 15:30:00 |
+----------+
```

### EXTRACT

```sql
EXTRACT(part FROM time_expression)
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `time_expression`.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`

**Example**

In the following example, `EXTRACT` returns a value corresponding to the `HOUR`
time part.

```sql
SELECT EXTRACT(HOUR FROM TIME "15:30:00") as hour;

+------------------+
| hour             |
+------------------+
| 15               |
+------------------+
```

### TIME_ADD

```sql
TIME_ADD(time_expression, INTERVAL int64_expression part)
```

**Description**

Adds `int64_expression` units of `part` to the `TIME` object.

`TIME_ADD` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you add an hour to `23:30:00`, the returned
value is `00:30:00`.

**Return Data Types**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original_time,
  TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE) as later;

+-----------------------------+------------------------+
| original_time               | later                  |
+-----------------------------+------------------------+
| 15:30:00                    | 15:40:00               |
+-----------------------------+------------------------+
```

### TIME_SUB

```sql
TIME_SUB(time_expression, INTERVAL int64_expression part)
```

**Description**

Subtracts `int64_expression` units of `part` from the `TIME` object.

`TIME_SUB` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

This function automatically adjusts when values fall outside of the 00:00:00 to
24:00:00 boundary. For example, if you subtract an hour from `00:30:00`, the
returned value is `23:30:00`.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original_date,
  TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE) as earlier;

+-----------------------------+------------------------+
| original_date                | earlier                |
+-----------------------------+------------------------+
| 15:30:00                    | 15:20:00               |
+-----------------------------+------------------------+
```

### TIME_DIFF

```sql
TIME_DIFF(time_expression_a, time_expression_b, part)
```

**Description**

Returns the whole number of specified `part` intervals between two
`TIME` objects (`time_expression_a` - `time_expression_b`). If the first
`TIME` is earlier than the second one, the output is negative. Throws an error
if the computation overflows the result type, such as if the difference in
nanoseconds
between the two `TIME` objects would overflow an
`INT64` value.

`TIME_DIFF` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIME "15:30:00" as first_time,
  TIME "14:35:00" as second_time,
  TIME_DIFF(TIME "15:30:00", TIME "14:35:00", MINUTE) as difference;

+----------------------------+------------------------+------------------------+
| first_time                 | second_time            | difference             |
+----------------------------+------------------------+------------------------+
| 15:30:00                   | 14:35:00               | 55                     |
+----------------------------+------------------------+------------------------+
```

### TIME_TRUNC

```sql
TIME_TRUNC(time_expression, part)
```

**Description**

Truncates a `TIME` object to the granularity of `part`.

`TIME_TRUNC` supports the following values for `part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`

**Return Data Type**

`TIME`

**Example**

```sql
SELECT
  TIME "15:30:00" as original,
  TIME_TRUNC(TIME "15:30:00", HOUR) as truncated;

+----------------------------+------------------------+
| original                   | truncated              |
+----------------------------+------------------------+
| 15:30:00                   | 15:00:00               |
+----------------------------+------------------------+
```

### FORMAT_TIME

```sql
FORMAT_TIME(format_string, time_object)
```

**Description**
Formats a `TIME` object according to the specified `format_string`. See
[Supported Format Elements For TIME][time-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIME("%R", TIME "15:30:00") as formatted_time;

+----------------+
| formatted_time |
+----------------+
| 15:30          |
+----------------+
```

### PARSE_TIME

```sql
PARSE_TIME(format_string, time_string)
```

**Description**

Converts a [string representation of time][time-format] to a
`TIME` object.

`format_string` contains the [format elements][time-format-elements]
that define how `time_string` is formatted. Each element in
`time_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `time_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIME("%I:%M:%S", "07:30:00")

-- This doesn't work because the seconds element is in different locations.
SELECT PARSE_TIME("%S:%I:%M", "07:30:00")

-- This doesn't work because one of the seconds elements is missing.
SELECT PARSE_TIME("%I:%M", "07:30:00")

-- This works because %T can find all matching elements in time_string.
SELECT PARSE_TIME("%T", "07:30:00")
```

The format string fully supports most format elements except for `%P`.

When using `PARSE_TIME`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from
  `00:00:00.0`. For instance, if `seconds` is unspecified then it
  defaults to `00`, and so on.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the `TIME` string. In
  addition, leading and trailing white spaces in the `TIME` string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information, the last one generally overrides any earlier ones.
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIME`

**Example**

```sql
SELECT PARSE_TIME("%H", "15") as parsed_time;

+-------------+
| parsed_time |
+-------------+
| 15:00:00    |
+-------------+
```

```sql
SELECT PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') AS parsed_time

+-------------+
| parsed_time |
+-------------+
| 14:23:38    |
+-------------+
```

### Supported format elements for TIME

Unless otherwise noted, `TIME` functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
    <td>09</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
    <td>9</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
    <td>47</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
    <td>pm</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
    <td>PM</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
    <td>21:47</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
    <td>09:47:00 PM</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
    <td>00</td>
 </tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
    <td>21:47:00</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
    <td>%</td>
 </tr>
 <tr>
    <td>%E&lt;number&gt;S</td>
    <td>Seconds with &lt;number&gt; digits of fractional precision.</td>
    <td>00.000 for %E3S</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
    <td>00.123456</td>
 </tr>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[time-format]: #format_time

[time-format-elements]: #supported_format_elements_for_time

[time-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[time-link-to-timezone-definitions]: #timezone_definitions

[time-to-string]: #cast

<!-- mdlint on -->

## Timestamp functions

ZetaSQL supports the following `TIMESTAMP` functions.

NOTE: These functions return a runtime error if overflow occurs; result
values are bounded by the defined [date][data-types-link-to-date_type]
and [timestamp][data-types-link-to-timestamp_type] min/max values.

### CURRENT_TIMESTAMP

```sql
CURRENT_TIMESTAMP()
```

**Description**

`CURRENT_TIMESTAMP()` produces a TIMESTAMP value that is continuous,
non-ambiguous, has exactly 60 seconds per minute and does not repeat values over
the leap second. Parentheses are optional.

This function handles leap seconds by smearing them across a window of 20 hours
around the inserted leap second.

**Supported Input Types**

Not applicable

**Result Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT CURRENT_TIMESTAMP() as now;

+---------------------------------------------+
| now                                         |
+---------------------------------------------+
| 2020-06-02 17:00:53.110 America/Los_Angeles |
+---------------------------------------------+
```

When a column named `current_timestamp` is present, the column name and the
function call without parentheses are ambiguous. To ensure the function call,
add parentheses; to ensure the column name, qualify it with its
[range variable][timestamp-functions-link-to-range-variables]. For example, the
following query will select the function in the `now` column and the table
column in the `current_timestamp` column.

```sql
WITH t AS (SELECT 'column value' AS `current_timestamp`)
SELECT current_timestamp() AS now, t.current_timestamp FROM t;

+---------------------------------------------+-------------------+
| now                                         | current_timestamp |
+---------------------------------------------+-------------------+
| 2020-06-02 17:00:53.110 America/Los_Angeles | column value      |
+---------------------------------------------+-------------------+
```

### EXTRACT

```sql
EXTRACT(part FROM timestamp_expression [AT TIME ZONE timezone])
```

**Description**

Returns a value that corresponds to the specified `part` from
a supplied `timestamp_expression`. This function supports an optional
`timezone` parameter. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

Allowed `part` values are:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAYOFWEEK`: Returns values in the range [1,7] with Sunday as the first day of
   of the week.
+ `DAY`
+ `DAYOFYEAR`
+ `WEEK`: Returns the week number of the date in the range [0, 53].  Weeks begin
  with Sunday, and dates prior to the first Sunday of the year are in week 0.
+ `WEEK(<WEEKDAY>)`: Returns the week number of `timestamp_expression` in the
  range [0, 53]. Weeks begin on `WEEKDAY`. `datetime`s prior to the first
  `WEEKDAY` of the year are in week 0. Valid values for `WEEKDAY` are `SUNDAY`,
  `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and `SATURDAY`.
+ `ISOWEEK`: Returns the [ISO 8601 week][ISO-8601-week]
  number of the `datetime_expression`. `ISOWEEK`s begin on Monday. Return values
  are in the range [1, 53]. The first `ISOWEEK` of each ISO year begins on the
  Monday before the first Thursday of the Gregorian calendar year.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Returns the [ISO 8601][ISO-8601]
  week-numbering year, which is the Gregorian calendar year containing the
  Thursday of the week to which `date_expression` belongs.
+ `DATE`
<li><code>DATETIME</code></li>
<li><code>TIME</code></li>
</ul>

Returned values truncate lower order time periods. For example, when extracting
seconds, `EXTRACT` truncates the millisecond and microsecond values.

**Return Data Type**

`INT64`, except when:

+ `part` is `DATE`, returns a `DATE` object.
+ `part` is `DATETIME`, returns a `DATETIME` object.
+ `part` is `TIME`, returns a `TIME` object.

**Examples**

In the following example, `EXTRACT` returns a value corresponding to the `DAY`
time part.

```sql
WITH Input AS (SELECT TIMESTAMP("2008-12-25 05:30:00+00") AS timestamp_value)
SELECT
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "UTC") AS the_day_utc,
  EXTRACT(DAY FROM timestamp_value AT TIME ZONE "America/Los_Angeles") AS the_day_california
FROM Input

+-------------+--------------------+
| the_day_utc | the_day_california |
+-------------+--------------------+
| 25          | 24                 |
+-------------+--------------------+
```

In the following example, `EXTRACT` returns values corresponding to different
time parts from a column of timestamps.

```sql
WITH Timestamps AS (
  SELECT TIMESTAMP("2005-01-03 12:34:56+00") AS timestamp_value UNION ALL
  SELECT TIMESTAMP("2007-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-01-01 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2009-12-31 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-01-02 12:00:00+00") UNION ALL
  SELECT TIMESTAMP("2017-05-26 12:00:00+00")
)
SELECT
  timestamp_value,
  EXTRACT(ISOYEAR FROM timestamp_value) AS isoyear,
  EXTRACT(ISOWEEK FROM timestamp_value) AS isoweek,
  EXTRACT(YEAR FROM timestamp_value) AS year,
  EXTRACT(WEEK FROM timestamp_value) AS week
FROM Timestamps
ORDER BY timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------+---------+------+------+
| timestamp_value                             | isoyear | isoweek | year | week |
+---------------------------------------------+---------+---------+------+------+
| 2005-01-03 04:34:56.000 America/Los_Angeles | 2005    | 1       | 2005 | 1    |
| 2007-12-31 04:00:00.000 America/Los_Angeles | 2008    | 1       | 2007 | 52   |
| 2009-01-01 04:00:00.000 America/Los_Angeles | 2009    | 1       | 2009 | 0    |
| 2009-12-31 04:00:00.000 America/Los_Angeles | 2009    | 53      | 2009 | 52   |
| 2017-01-02 04:00:00.000 America/Los_Angeles | 2017    | 1       | 2017 | 1    |
| 2017-05-26 05:00:00.000 America/Los_Angeles | 2017    | 21      | 2017 | 21   |
+---------------------------------------------+---------+---------+------+------+
```

In the following example, `timestamp_expression` falls on a Monday. `EXTRACT`
calculates the first column using weeks that begin on Sunday, and it calculates
the second column using weeks that begin on Monday.

```sql
WITH table AS (SELECT TIMESTAMP("2017-11-06 00:00:00+00") AS timestamp_value)
SELECT
  timestamp_value,
  EXTRACT(WEEK(SUNDAY) FROM timestamp_value) AS week_sunday,
  EXTRACT(WEEK(MONDAY) FROM timestamp_value) AS week_monday
FROM table;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+-------------+---------------+
| timestamp_value                             | week_sunday | week_monday   |
+---------------------------------------------+-------------+---------------+
| 2017-11-05 16:00:00.000 America/Los_Angeles | 45          | 44            |
+---------------------------------------------+-------------+---------------+
```

### STRING

```sql
STRING(timestamp_expression[, timezone])
```

**Description**

Converts a `timestamp_expression` to a STRING data type. Supports an optional
parameter to specify a time zone. See
[Time zone definitions][timestamp-link-to-timezone-definitions] for information
on how to specify a time zone.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS string;

+-------------------------------+
| string                        |
+-------------------------------+
| 2008-12-25 15:30:00+00        |
+-------------------------------+
```

### TIMESTAMP

```sql
TIMESTAMP(string_expression[, timezone])
TIMESTAMP(date_expression[, timezone])
TIMESTAMP(datetime_expression[, timezone])
```

**Description**

+  `string_expression[, timezone]`: Converts a STRING expression to a TIMESTAMP
   data type. `string_expression` must include a
   timestamp literal.
   If `string_expression` includes a timezone in the timestamp literal, do not
   include an explicit `timezone`
   argument.
+  `date_expression[, timezone]`: Converts a DATE object to a TIMESTAMP
   data type.
+  `datetime_expression[, timezone]`: Converts a
   DATETIME object to a TIMESTAMP data type.

This function supports an optional
parameter to [specify a time zone][timestamp-link-to-timezone-definitions]. If
no time zone is specified, the default time zone, which is implementation defined,
is used.

**Return Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00+00") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 15:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP("2008-12-25 15:30:00 UTC") AS timestamp_str;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_str                               |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP(DATETIME "2008-12-25 15:30:00") AS timestamp_datetime;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_datetime                          |
+---------------------------------------------+
| 2008-12-25 15:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

```sql
SELECT TIMESTAMP(DATE "2008-12-25") AS timestamp_date;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| timestamp_date                              |
+---------------------------------------------+
| 2008-12-25 00:00:00.000 America/Los_Angeles |
+---------------------------------------------+
```

### TIMESTAMP_ADD

```sql
TIMESTAMP_ADD(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Adds `int64_expression` units of `date_part` to the timestamp, independent of
any time zone.

`TIMESTAMP_ADD` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Types**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS later;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| original                                    | later                                       |
+---------------------------------------------+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:40:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

### TIMESTAMP_SUB

```sql
TIMESTAMP_SUB(timestamp_expression, INTERVAL int64_expression date_part)
```

**Description**

Subtracts `int64_expression` units of `date_part` from the timestamp,
independent of any time zone.

`TIMESTAMP_SUB` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT
  TIMESTAMP("2008-12-25 15:30:00+00") AS original,
  TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS earlier;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| original                                    | earlier                                     |
+---------------------------------------------+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles | 2008-12-25 07:20:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

### TIMESTAMP_DIFF

```sql
TIMESTAMP_DIFF(timestamp_expression_a, timestamp_expression_b, date_part)
```

**Description**

Returns the whole number of specified `date_part` intervals between two
`TIMESTAMP` objects (`timestamp_expression_a` - `timestamp_expression_b`).
If the first `TIMESTAMP` is earlier than the second one,
the output is negative. Throws an error if the computation overflows the
result type, such as if the difference in
nanoseconds
between the two `TIMESTAMP` objects would overflow an
`INT64` value.

`TIMESTAMP_DIFF` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`. Equivalent to 60 `MINUTE`s.
+ `DAY`. Equivalent to 24 `HOUR`s.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT
  TIMESTAMP("2010-07-07 10:20:00+00") AS later_timestamp,
  TIMESTAMP("2008-12-25 15:30:00+00") AS earlier_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP "2010-07-07 10:20:00+00", TIMESTAMP "2008-12-25 15:30:00+00", HOUR) AS hours;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+-------+
| later_timestamp                             | earlier_timestamp                           | hours |
+---------------------------------------------+---------------------------------------------+-------+
| 2010-07-07 03:20:00.000 America/Los_Angeles | 2008-12-25 07:30:00.000 America/Los_Angeles | 13410 |
+---------------------------------------------+---------------------------------------------+-------+
```

In the following example, the first timestamp occurs before the second
timestamp, resulting in a negative output.

```sql
SELECT TIMESTAMP_DIFF(TIMESTAMP "2018-08-14", TIMESTAMP "2018-10-14", DAY);

+---------------+
| negative_diff |
+---------------+
| -61           |
+---------------+
```

In this example, the result is 0 because only the number of whole specified
`HOUR` intervals are included.

```sql
SELECT TIMESTAMP_DIFF("2001-02-01 01:00:00", "2001-02-01 00:00:01", HOUR)

+---------------+
| negative_diff |
+---------------+
| 0             |
+---------------+
```

### TIMESTAMP_TRUNC

```sql
TIMESTAMP_TRUNC(timestamp_expression, date_part[, timezone])
```

**Description**

Truncates a timestamp to the granularity of `date_part`.

`TIMESTAMP_TRUNC` supports the following values for `date_part`:

+ `NANOSECOND`
  (if the SQL engine supports it)
+ `MICROSECOND`
+ `MILLISECOND`
+ `SECOND`
+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>):` Truncates `timestamp_expression` to the preceding
  week boundary, where weeks begin on `WEEKDAY`. Valid values for `WEEKDAY` are
  `SUNDAY`, `MONDAY`, `TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, and
  `SATURDAY`.
+ `ISOWEEK`: Truncates `timestamp_expression` to the preceding
   [ISO 8601 week][ISO-8601-week] boundary. `ISOWEEK`s
   begin on Monday. The first `ISOWEEK` of each ISO year contains the first
   Thursday of the corresponding Gregorian calendar year. Any `date_expression`
   earlier than this will truncate to the preceding Monday.
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`: Truncates `timestamp_expression` to the preceding [ISO 8601][ISO-8601]
    week-numbering year boundary. The ISO year boundary is the Monday of the
    first week whose Thursday belongs to the corresponding Gregorian calendar
    year.

`TIMESTAMP_TRUNC` function supports an optional `timezone` parameter. This
parameter applies to the following `date_parts`:

+ `MINUTE`
+ `HOUR`
+ `DAY`
+ `WEEK`
+ `WEEK(<WEEKDAY>)`
+ `ISOWEEK`
+ `MONTH`
+ `QUARTER`
+ `YEAR`
+ `ISOYEAR`

Use this parameter if you want to use a time zone other than the
default time zone, which is implementation defined, as part of the
truncate operation.

When truncating a `TIMESTAMP` to `MINUTE`
or`HOUR`, `TIMESTAMP_TRUNC` determines the civil time of the
`TIMESTAMP` in the specified (or default) time zone
and subtracts the minutes and seconds (when truncating to HOUR) or the seconds
(when truncating to MINUTE) from that `TIMESTAMP`.
While this provides intuitive results in most cases, the result is
non-intuitive near daylight savings transitions that are not hour aligned.

**Return Data Type**

`TIMESTAMP`

**Examples**

```sql
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC") AS utc,
  TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "America/Los_Angeles") AS la;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+
| utc                                         | la                                          |
+---------------------------------------------+---------------------------------------------+
| 2008-12-24 16:00:00.000 America/Los_Angeles | 2008-12-25 00:00:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+
```

In the following example, `timestamp_expression` has a time zone offset of +12.
The first column shows the `timestamp_expression` in UTC time. The second
column shows the output of `TIMESTAMP_TRUNC` using weeks that start on Monday.
Because the `timestamp_expression` falls on a Sunday in UTC, `TIMESTAMP_TRUNC`
truncates it to the preceding Monday. The third column shows the same function
with the optional [Time zone definition][timestamp-link-to-timezone-definitions]
argument 'Pacific/Auckland'. Here the function truncates the
`timestamp_expression` using New Zealand Daylight Time, where it falls on a
Monday.

```sql
SELECT
  timestamp_value AS timestamp_value,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "UTC") AS utc_truncated,
  TIMESTAMP_TRUNC(timestamp_value, WEEK(MONDAY), "Pacific/Auckland") AS nzdt_truncated
FROM (SELECT TIMESTAMP("2017-11-06 00:00:00+12") AS timestamp_value);

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
| timestamp_value                             | utc_truncated                               | nzdt_truncated                              |
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
| 2017-11-05 04:00:00.000 America/Los_Angeles | 2017-10-29 17:00:00.000 America/Los_Angeles | 2017-11-05 03:00:00.000 America/Los_Angeles |
+---------------------------------------------+---------------------------------------------+---------------------------------------------+
```

In the following example, the original `timestamp_expression` is in the
Gregorian calendar year 2015. However, `TIMESTAMP_TRUNC` with the `ISOYEAR` date
part truncates the `timestamp_expression` to the beginning of the ISO year, not
the Gregorian calendar year. The first Thursday of the 2015 calendar year was
2015-01-01, so the ISO year 2015 begins on the preceding Monday, 2014-12-29.
Therefore the ISO year boundary preceding the `timestamp_expression`
2015-06-15 00:00:00+00 is 2014-12-29.

```sql
SELECT
  TIMESTAMP_TRUNC("2015-06-15 00:00:00+00", ISOYEAR) AS isoyear_boundary,
  EXTRACT(ISOYEAR FROM TIMESTAMP "2015-06-15 00:00:00+00") AS isoyear_number;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+----------------+
| isoyear_boundary                            | isoyear_number |
+---------------------------------------------+----------------+
| 2014-12-29 00:00:00.000 America/Los_Angeles | 2015           |
+---------------------------------------------+----------------+
```

### FORMAT_TIMESTAMP

```sql
FORMAT_TIMESTAMP(format_string, timestamp[, timezone])
```

**Description**

Formats a timestamp according to the specified `format_string`.

See [Supported Format Elements For TIMESTAMP][timestamp-format-elements]
for a list of format elements that this function supports.

**Return Data Type**

`STRING`

**Example**

```sql
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:30:00+00", "UTC") AS formatted;

+--------------------------+
| formatted                |
+--------------------------+
| Thu Dec 25 15:30:00 2008 |
+--------------------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00+00") AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec-25-2008 |
+-------------+
```

```sql
SELECT FORMAT_TIMESTAMP("%b %Y", TIMESTAMP "2008-12-25 15:30:00+00")
  AS formatted;

+-------------+
| formatted   |
+-------------+
| Dec 2008    |
+-------------+
```

### PARSE_TIMESTAMP

```sql
PARSE_TIMESTAMP(format_string, timestamp_string[, timezone])
```

**Description**

Converts a [string representation of a timestamp][timestamp-format] to a
`TIMESTAMP` object.

`format_string` contains the [format elements][timestamp-format-elements]
that define how `timestamp_string` is formatted. Each element in
`timestamp_string` must have a corresponding element in `format_string`. The
location of each element in `format_string` must match the location of
each element in `timestamp_string`.

```sql
-- This works because elements on both sides match.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because the year element is in different locations.
SELECT PARSE_TIMESTAMP("%a %b %e %Y %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This doesn't work because one of the year elements is missing.
SELECT PARSE_TIMESTAMP("%a %b %e %I:%M:%S", "Thu Dec 25 07:30:00 2008")

-- This works because %c can find all matching elements in timestamp_string.
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008")
```

The format string fully
supports most format elements, except for
`%a`, `%A`, `%g`, `%G`, `%j`, `%P`, `%u`, `%U`, `%V`, `%w`, and `%W`.

When using `PARSE_TIMESTAMP`, keep the following in mind:

+ **Unspecified fields.** Any unspecified field is initialized from `1970-01-01
  00:00:00.0`. This initialization value uses the time zone specified by the
  function's time zone argument, if present. If not, the initialization value
  uses the default time zone, which is implementation defined.  For instance, if the year
  is unspecified then it defaults to `1970`, and so on.
+ **Case insensitivity.** Names, such as `Monday`, `February`, and so on, are
  case insensitive.
+ **Whitespace.** One or more consecutive white spaces in the format string
  matches zero or more consecutive white spaces in the timestamp string. In
  addition, leading and trailing white spaces in the timestamp string are always
  allowed, even if they are not in the format string.
+ **Format precedence.** When two (or more) format elements have overlapping
  information (for example both `%F` and `%Y` affect the year), the last one
  generally overrides any earlier ones, with some exceptions (see the
  descriptions of `%s`, `%C`, and `%y`).
+ **Format divergence.** `%p` can be used with `am`, `AM`, `pm`, and `PM`.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008") AS parsed;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+---------------------------------------------+
| parsed                                      |
+---------------------------------------------+
| 2008-12-25 07:30:00.000 America/Los_Angeles |
+---------------------------------------------+
```

### TIMESTAMP_SECONDS

```sql
TIMESTAMP_SECONDS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since 1970-01-01 00:00:00
UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_MILLIS

```sql
TIMESTAMP_MILLIS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since 1970-01-01
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_MICROS

```sql
TIMESTAMP_MICROS(int64_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since 1970-01-01
00:00:00 UTC and returns a timestamp.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### UNIX_SECONDS

```sql
UNIX_SECONDS(timestamp_expression)
```

**Description**

Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher
levels of precision.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00") AS seconds;

+------------+
| seconds    |
+------------+
| 1230219000 |
+------------+
```

### UNIX_MILLIS

```sql
UNIX_MILLIS(timestamp_expression)
```

**Description**

Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates
higher levels of precision.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00") AS millis;

+---------------+
| millis        |
+---------------+
| 1230219000000 |
+---------------+
```

### UNIX_MICROS

```sql
UNIX_MICROS(timestamp_expression)
```

**Description**

Returns the number of microseconds since 1970-01-01 00:00:00 UTC. Truncates
higher levels of precision.

**Return Data Type**

`INT64`

**Example**

```sql
SELECT UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00") AS micros;

+------------------+
| micros           |
+------------------+
| 1230219000000000 |
+------------------+
```

### TIMESTAMP_FROM_UNIX_SECONDS

```sql
TIMESTAMP_FROM_UNIX_SECONDS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_SECONDS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of seconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_SECONDS(1230219000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_FROM_UNIX_MILLIS

```sql
TIMESTAMP_FROM_UNIX_MILLIS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MILLIS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of milliseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MILLIS(1230219000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### TIMESTAMP_FROM_UNIX_MICROS

```sql
TIMESTAMP_FROM_UNIX_MICROS(int64_expression)
```

```sql
TIMESTAMP_FROM_UNIX_MICROS(timestamp_expression)
```

**Description**

Interprets `int64_expression` as the number of microseconds since
1970-01-01 00:00:00 UTC and returns a timestamp. If a timestamp is passed in,
the same timestamp is returned.

**Return Data Type**

`TIMESTAMP`

**Example**

```sql
SELECT TIMESTAMP_FROM_UNIX_MICROS(1230219000000000) AS timestamp_value;

-- Display of results may differ, depending upon the environment and time zone where this query was executed.
+------------------------+
| timestamp_value        |
+------------------------+
| 2008-12-25 15:30:00+00 |
+------------------------+
```

### Supported format elements for TIMESTAMP

Unless otherwise noted, TIMESTAMP functions that use format strings support the
following elements:

<table>
 <tr>
    <td class="tab0">Format element</td>
    <td class="tab0">Description</td>
    <td class="tab0">Example</td>
 </tr>
 <tr>
    <td>%A</td>
    <td>The full weekday name.</td>
    <td>Wednesday</td>
 </tr>
 <tr>
    <td>%a</td>
    <td>The abbreviated weekday name.</td>
    <td>Wed</td>
 </tr>
 <tr>
    <td>%B</td>
    <td>The full month name.</td>
    <td>January</td>
 </tr>
 <tr>
    <td>%b or %h</td>
    <td>The abbreviated month name.</td>
    <td>Jan</td>
 </tr>
 <tr>
    <td>%C</td>
    <td>The century (a year divided by 100 and truncated to an integer) as a
    decimal
number (00-99).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%c</td>
    <td>The date and time representation in the format %a %b %e %T %Y.</td>
    <td>Wed Jan 20 16:47:00 2021</td>
 </tr>
 <tr>
    <td>%D</td>
    <td>The date in the format %m/%d/%y.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%d</td>
    <td>The day of the month as a decimal number (01-31).</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%e</td>
    <td>The day of month as a decimal number (1-31); single digits are preceded
    by a
space.</td>
    <td>20</td>
 </tr>
 <tr>
    <td>%F</td>
    <td>The date in the format %Y-%m-%d.</td>
    <td>2021-01-20</td>
 </tr>
 <tr>
    <td>%G</td>
    <td>The
    <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    with century as a decimal number. Each ISO year begins
    on the Monday before the first Thursday of the Gregorian calendar year.
    Note that %G and %Y may produce different results near Gregorian year
    boundaries, where the Gregorian year and ISO year can diverge.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%g</td>
    <td>The
    <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> year
    without century as a decimal number (00-99). Each ISO
    year begins on the Monday before the first Thursday of the Gregorian
    calendar year. Note that %g and %y may produce different results near
    Gregorian year boundaries, where the Gregorian year and ISO year can
    diverge.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%H</td>
    <td>The hour (24-hour clock) as a decimal number (00-23).</td>
    <td>16</td>
 </tr>
 <tr>
    <td>%I</td>
    <td>The hour (12-hour clock) as a decimal number (01-12).</td>
    <td>04</td>
 </tr>
 <tr>
    <td>%j</td>
    <td>The day of the year as a decimal number (001-366).</td>
    <td>020</td>
 </tr>
 <tr>
    <td>%k</td>
    <td>The hour (24-hour clock) as a decimal number (0-23); single digits are
    preceded
by a space.</td>
    <td>16</td>
 </tr>
 <tr>
    <td>%l</td>
    <td>The hour (12-hour clock) as a decimal number (1-12); single digits are
    preceded
by a space.</td>
    <td>11</td>
 </tr>
 <tr>
    <td>%M</td>
    <td>The minute as a decimal number (00-59).</td>
    <td>47</td>
 </tr>
 <tr>
    <td>%m</td>
    <td>The month as a decimal number (01-12).</td>
    <td>01</td>
 </tr>
 <tr>
    <td>%n</td>
    <td>A newline character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%P</td>
    <td>Either am or pm.</td>
    <td>am</td>
 </tr>
 <tr>
    <td>%p</td>
    <td>Either AM or PM.</td>
    <td>AM</td>
 </tr>
 <tr>
    <td>%Q</td>
    <td>The quarter as a decimal number (1-4).</td>
    <td>1</td>
 </tr>
 <tr>
    <td>%R</td>
    <td>The time in the format %H:%M.</td>
    <td>16:47</td>
 </tr>
 <tr>
    <td>%r</td>
    <td>The 12-hour clock time using AM/PM notation.</td>
    <td>04:47:00 PM</td>
 </tr>
 <tr>
    <td>%S</td>
    <td>The second as a decimal number (00-60).</td>
    <td>00</td>
 </tr>
 <tr>
    <td>%s</td>
    <td>The number of seconds since 1970-01-01 00:00:00 UTC. Always overrides all
    other format elements, independent of where %s appears in the string.
    If multiple %s elements appear, then the last one takes precedence.</td>
    <td>1611179220</td>
</tr>
 <tr>
    <td>%T</td>
    <td>The time in the format %H:%M:%S.</td>
    <td>16:47:00</td>
 </tr>
 <tr>
    <td>%t</td>
    <td>A tab character.</td>
    <td></td>
 </tr>
 <tr>
    <td>%U</td>
    <td>The week number of the year (Sunday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%u</td>
    <td>The weekday (Monday as the first day of the week) as a decimal number
    (1-7).</td>
    <td>3</td>
</tr>
 <tr>
    <td>%V</td>
   <td>The <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO 8601</a>
    week number of the year (Monday as the first
    day of the week) as a decimal number (01-53).  If the week containing
    January 1 has four or more days in the new year, then it is week 1;
    otherwise it is week 53 of the previous year, and the next week is
    week 1.</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%W</td>
    <td>The week number of the year (Monday as the first day of the week) as a
    decimal number (00-53).</td>
    <td>03</td>
 </tr>
 <tr>
    <td>%w</td>
    <td>The weekday (Sunday as the first day of the week) as a decimal number
    (0-6).</td>
    <td>3</td>
 </tr>
 <tr>
    <td>%X</td>
    <td>The time representation in HH:MM:SS format.</td>
    <td>16:47:00</td>
 </tr>
 <tr>
    <td>%x</td>
    <td>The date representation in MM/DD/YY format.</td>
    <td>01/20/21</td>
 </tr>
 <tr>
    <td>%Y</td>
    <td>The year with century as a decimal number.</td>
    <td>2021</td>
 </tr>
 <tr>
    <td>%y</td>
    <td>The year without century as a decimal number (00-99), with an optional
    leading zero. Can be mixed with %C. If %C is not specified, years 00-68 are
    2000s, while years 69-99 are 1900s.</td>
    <td>21</td>
 </tr>
 <tr>
    <td>%Z</td>
    <td>The time zone name.</td>
    <td>UTC-5</td>
 </tr>
 <tr>
    <td>%z</td>
    <td>The offset from the Prime Meridian in the format +HHMM or -HHMM as
    appropriate,
with positive values representing locations east of Greenwich.</td>
    <td>-0500</td>
 </tr>
 <tr>
    <td>%%</td>
    <td>A single % character.</td>
    <td>%</td>
 </tr>
 <tr>
    <td>%Ez</td>
    <td>RFC 3339-compatible numeric time zone (+HH:MM or -HH:MM).</td>
    <td>-05:00</td>
 </tr>
 <tr>
    <td>%E&lt;number&gt;S</td>
    <td>Seconds with &lt;number&gt; digits of fractional precision.</td>
    <td>00.000 for %E3S</td>
 </tr>
 <tr>
    <td>%E*S</td>
    <td>Seconds with full fractional precision (a literal '*').</td>
    <td>00.123456</td>
 </tr>
 <tr>
    <td>%E4Y</td>
    <td>Four-character years (0001 ... 9999). Note that %Y
    produces as many characters as it takes to fully render the
year.</td>
    <td>2021</td>
 </tr>
</table>

### Time zone definitions 
<a id="timezone_definitions"></a>

Certain date and timestamp functions allow you to override the default time zone
and specify a different one. You can specify a time zone by either supplying
the [time zone name][timezone-by-name] (for example, `America/Los_Angeles`)
or time zone offset from UTC (for example, -08).

If you choose to use a time zone offset, use this format:

```sql
(+|-)H[H][:M[M]]
```

The following timestamps are equivalent because the time zone offset
for `America/Los_Angeles` is `-08` for the specified date and time.

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00 America/Los_Angeles") as millis;
```

```sql
SELECT UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00-08:00") as millis;
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[ISO-8601]: https://en.wikipedia.org/wiki/ISO_8601

[ISO-8601-week]: https://en.wikipedia.org/wiki/ISO_week_date

[timezone-by-name]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

[timestamp-link-to-timezone-definitions]: #timezone_definitions

[timestamp-format]: #format_timestamp

[timestamp-format-elements]: #supported_format_elements_for_timestamp

[timestamp-functions-link-to-range-variables]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#range_variables

[data-types-link-to-date_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#date_type

[data-types-link-to-timestamp_type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#timestamp_type

[timestamp-literals]: https://github.com/google/zetasql/blob/master/docs/lexical.md#timestamp_literals

<!-- mdlint on -->

## Interval functions

ZetaSQL supports the following `INTERVAL` functions.

### MAKE_INTERVAL

```sql
MAKE_INTERVAL(year, month, day, hour, minute, second)
```

**Description**

Constructs an `INTERVAL` object using `INT64` values representing the year,
month, day, hour, minute, and second. All arguments are optional with default
value of 0 and can be used as named arguments.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  MAKE_INTERVAL(1, 6, 15) AS i1,
  MAKE_INTERVAL(hour => 10, second => 20) AS i2,
  MAKE_INTERVAL(1, minute => 5, day => 2) AS i3

+--------------+---------------+-------------+
| i1           | i2            | i3          |
+--------------+---------------+-------------+
| 1-6 15 0:0:0 | 0-0 0 10:0:20 | 1-0 2 0:5:0 |
+--------------+---------------+-------------+
```

### EXTRACT

```sql
EXTRACT(part FROM interval_expression)
```

**Description**

Returns the value corresponding to the specified date part. The `part` must be
one of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND` or
`MICROSECOND`.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  EXTRACT(YEAR FROM i) AS year,
  EXTRACT(MONTH FROM i) AS month,
  EXTRACT(DAY FROM i) AS day,
  EXTRACT(HOUR FROM i) AS hour,
  EXTRACT(MINUTE FROM i) AS minute,
  EXTRACT(SECOND FROM i) AS second,
  EXTRACT(MILLISECOND FROM i) AS milli,
  EXTRACT(MICROSECOND FROM i) AS micro
FROM
  UNNEST([INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND,
          INTERVAL '0-13 370 48:61:61' YEAR TO SECOND]) AS i

+------+-------+-----+------+--------+--------+-------+--------+
| year | month | day | hour | minute | second | milli | micro  |
+------+-------+-----+------+--------+--------+-------+--------+
| 1    | 2     | 3   | 4    | 5      | 6      | 789   | 789999 |
| 1    | 1     | 370 | 49   | 2      | 1      | 0     | 0      |
+------+-------+-----+------+--------+--------+-------+--------+
```

### JUSTIFY_DAYS

```sql
JUSTIFY_DAYS(interval_expression)
```

**Description**

Normalizes the day part of the interval to the range from -29 to 29 by
incrementing/decrementing the month or year part of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  JUSTIFY_DAYS(INTERVAL 29 DAY) AS i1,
  JUSTIFY_DAYS(INTERVAL -30 DAY) AS i2,
  JUSTIFY_DAYS(INTERVAL 31 DAY) AS i3,
  JUSTIFY_DAYS(INTERVAL -65 DAY) AS i4,
  JUSTIFY_DAYS(INTERVAL 370 DAY) AS i5

+--------------+--------------+-------------+---------------+--------------+
| i1           | i2           | i3          | i4            | i5           |
+--------------+--------------+-------------+---------------+--------------+
| 0-0 29 0:0:0 | -0-1 0 0:0:0 | 0-1 1 0:0:0 | -0-2 -5 0:0:0 | 1-0 10 0:0:0 |
+--------------+--------------+-------------+---------------+--------------+
```

### JUSTIFY_HOURS

```sql
JUSTIFY_HOURS(interval_expression)
```

**Description**

Normalizes the time part of the interval to the range from -23:59:59.999999 to
23:59:59.999999 by incrementing/decrementing the day part of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT
  JUSTIFY_HOURS(INTERVAL 23 HOUR) AS i1,
  JUSTIFY_HOURS(INTERVAL -24 HOUR) AS i2,
  JUSTIFY_HOURS(INTERVAL 47 HOUR) AS i3,
  JUSTIFY_HOURS(INTERVAL -12345 MINUTE) AS i4

+--------------+--------------+--------------+-----------------+
| i1           | i2           | i3           | i4              |
+--------------+--------------+--------------+-----------------+
| 0-0 0 23:0:0 | 0-0 -1 0:0:0 | 0-0 1 23:0:0 | 0-0 -8 -13:45:0 |
+--------------+--------------+--------------+-----------------+
```

### JUSTIFY_INTERVAL

```sql
JUSTIFY_INTERVAL(interval_expression)
```

**Description**

Normalizes the days and time parts of the interval.

**Return Data Type**

`INTERVAL`

**Example**

```sql
SELECT JUSTIFY_INTERVAL(INTERVAL '29 49:00:00' DAY TO SECOND) AS i

+-------------+
| i           |
+-------------+
| 0-1 1 1:0:0 |
+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

## Protocol buffer functions

ZetaSQL supports the following protocol buffer functions.

### PROTO_DEFAULT_IF_NULL

```sql
PROTO_DEFAULT_IF_NULL(proto_field_expression)
```

**Description**

Evaluates any expression that results in a proto field access.
If the `proto_field_expression` evaluates to `NULL`, returns the default
value for the field. Otherwise, returns the field value.

Stipulations:

+ The expression cannot resolve to a required field.
+ The expression cannot resolve to a message field.
+ The expression must resolve to a regular proto field access, not
  a virtual field.
+ The expression cannot access a field with
  `zetasql.use_defaults=false`.

**Return Type**

Type of `proto_field_expression`.

**Example**

In the following example, each book in a library has a country of origin. If
the country is not set, the country defaults to unknown.

In this statement, table `library_books` contains a column named `book`,
whose type is `Book`.

```sql
SELECT PROTO_DEFAULT_IF_NULL(book.country) as origin FROM library_books;
```

`Book` is a type that contains a field called `country`.

```proto
message Book {
  optional string country = 4 [default = 'Unknown'];
}
```

This is the result if `book.country` evaluates to `Canada`.

```sql
+-----------------+
| origin          |
+-----------------+
| Canada          |
+-----------------+
```

This is the result if `book` is `NULL`. Since `book` is `NULL`,
`book.country` evaluates to `NULL` and therefore the function result is the
default value for `country`.

```sql
+-----------------+
| origin          |
+-----------------+
| Unknown         |
+-----------------+
```

### FILTER_FIELDS

```sql
FILTER_FIELDS(proto_expression, proto_field_list)

proto_field_list:
  {+|-}proto_field_path[, ...]
```

**Description**

Takes a protocol buffer and a list of its fields to include or exclude.
Returns a version of that protocol buffer with unwanted fields removed.
Returns `NULL` if the protocol buffer is `NULL`.

Input values:

+ `proto_expression`: The protocol buffer to filter.
+ `proto_field_list`: The fields to exclude or include in the resulting
  protocol buffer.
+ `+`: Include a protocol buffer field and its children in the results.
+ `-`: Exclude a protocol buffer field and its children in the results.
+ `proto_field_path`: The protocol buffer field to include or exclude.
  If the field represents an [extension][querying-proto-extensions], you can use
  syntax for that extension in the path.

Protocol buffer field expression behavior:

+ The first field in `proto_field_list` determines the default
  inclusion/exclusion. By default, when you include the first field, all other
  fields are excluded. Or by default, when you exclude the first field, all
  other fields are included.
+ A required field in the protocol buffer cannot be excluded explicitly or
  implicitly.
+ If a field is included, its child fields and descendants are implicitly
  included in the results.
+ If a field is excluded, its child fields and descendants are
  implicitly excluded in the results.
+ A child field must be listed after its parent field in the argument list,
  but does not need to come right after the parent field.

Caveats:

+ If you attempt to exclude/include a field that already has been
  implicitly excluded/included, an error is produced.
+ If you attempt to explicitly include/exclude a field that has already
  implicitly been included/excluded, an error is produced.

**Return type**

Type of `proto_expression`

**Examples**

The examples in this section reference a protocol buffer called `Award` and
a table called `MusicAwards`.

```proto
message Award {
  required int32 year = 1;
  optional int32 month = 2;
  repeated Type type = 3;

  message Type {
    optional string award_name = 1;
    optional string category = 2;
  }
}
```

```sql
WITH
  MusicAwards AS (
    SELECT
      CAST(
        '''
        year: 2001
        month: 9
        type { award_name: 'Best Artist' category: 'Artist' }
        type { award_name: 'Best Album' category: 'Album' }
        '''
        AS zetasql.examples.music.Award) AS award_col
    UNION ALL
    SELECT
      CAST(
        '''
        year: 2001
        month: 12
        type { award_name: 'Best Song' category: 'Song' }
        '''
        AS zetasql.examples.music.Award) AS award_col
  )
SELECT *
FROM MusicAwards

+---------------------------------------------------------+
| award_col                                               |
+---------------------------------------------------------+
| {                                                       |
|   year: 2001                                            |
|   month: 9                                              |
|   type { award_name: "Best Artist" category: "Artist" } |
|   type { award_name: "Best Album" category: "Album" }   |
| }                                                       |
| {                                                       |
|   year: 2001                                            |
|   month: 12                                             |
|   type { award_name: "Best Song" category: "Song" }     |
| }                                                       |
+---------------------------------------------------------+
```

The following example returns protocol buffers that only include the `year`
field.

```sql
SELECT FILTER_FIELDS(award_col, +year) AS filtered_fields
FROM MusicAwards

+-----------------+
| filtered_fields |
+-----------------+
| {year: 2001}    |
| {year: 2001}    |
+-----------------+
```

The following example returns protocol buffers that include all but the `type`
field.

```sql
SELECT FILTER_FIELDS(award_col, -type) AS filtered_fields
FROM MusicAwards

+------------------------+
| filtered_fields        |
+------------------------+
| {year: 2001 month: 9}  |
| {year: 2001 month: 12} |
+------------------------+
```

The following example returns protocol buffers that only include the `year` and
`type.award_name` fields.

```sql
SELECT FILTER_FIELDS(award_col, +year, +type.award_name) AS filtered_fields
FROM MusicAwards

+--------------------------------------+
| filtered_fields                      |
+--------------------------------------+
| {                                    |
|   year: 2001                         |
|   type { award_name: "Best Artist" } |
|   type { award_name: "Best Album" }  |
| }                                    |
| {                                    |
|   year: 2001                         |
|   type { award_name: "Best Song" }   |
| }                                    |
+--------------------------------------+
```

The following example returns the `year` and `type` fields, but excludes the
`award_name` field in the `type` field.

```sql
SELECT FILTER_FIELDS(award_col, +year, +type, -type.award_name) AS filtered_fields
FROM MusicAwards

+---------------------------------+
| filtered_fields                 |
+---------------------------------+
| {                               |
|   year: 2001                    |
|   type { award_name: "Artist" } |
|   type { award_name: "Album" }  |
| }                               |
| {                               |
|   year: 2001                    |
|   type { award_name: "Song" }   |
| }                               |
+---------------------------------+
```

The following example produces an error because `year` is a required field
and cannot be excluded explicitly or implicitly from the results.

```sql
SELECT FILTER_FIELDS(award_col, -year) AS filtered_fields
FROM MusicAwards

-- Error
```

The following example produces an error because when `year` was included,
`month` was implicitly excluded. You cannot explicitly exclude a field that
has already been implicitly excluded.

```sql
SELECT FILTER_FIELDS(award_col, +year, -month) AS filtered_fields
FROM MusicAwards

-- Error
```

### FROM_PROTO

```sql
FROM_PROTO(expression)
```

**Description**

Returns a ZetaSQL value. The valid `expression` types are defined
in the table below, along with the return types that they produce.
Other input `expression` types are invalid. If `expression` cannot be converted
to a valid value, an error is returned.

<table width="100%">
  <thead>
    <tr>
      <th width="50%"><code>expression</code> type</th>
      <th width="50%">Return type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <ul>
        <li>INT32</li>
        <li>google.protobuf.Int32Value</li>
        </ul>
      </td>
      <td>INT32</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT32</li>
        <li>google.protobuf.UInt32Value</li>
        </ul>
      </td>
      <td>UINT32</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>INT64</li>
        <li>google.protobuf.Int64Value</li>
        </ul>
      </td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT64</li>
        <li>google.protobuf.UInt64Value</li>
        </ul>
      </td>
      <td>UINT64</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>FLOAT</li>
        <li>google.protobuf.FloatValue</li>
        </ul>
      </td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DOUBLE</li>
        <li>google.protobuf.DoubleValue</li>
        </ul>
      </td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BOOL</li>
        <li>google.protobuf.BoolValue</li>
        </ul>
      </td>
      <td>BOOL</td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>STRING</li>
          <li>
            google.protobuf.StringValue
            <p>
            Note: The <code>StringValue</code>
            value field must be
            UTF-8 encoded.
            </p>
          </li>
        </ul>
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BYTES</li>
        <li>google.protobuf.BytesValue</li>
        </ul>
      </td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DATE</li>
        <li>google.type.Date</li>
        </ul>
      </td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIME</li>
        <li>
          google.type.TimeOfDay

          

          

        </li>
        </ul>
      </td>
      <td>TIME</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIMESTAMP</li>
        <li>
          google.protobuf.Timestamp

          

          

        </li>
        </ul>
      </td>
      <td>TIMESTAMP</td>
    </tr>
  </tbody>
</table>

**Return Type**

The return type depends upon the `expression` type. See the return types
in the table above.

**Examples**

Convert a `google.type.Date` type into a `DATE` type.

```sql
SELECT FROM_PROTO(
  new google.type.Date(
    2019 as year,
    10 as month,
    30 as day
  )
)

+------------+
| $col1      |
+------------+
| 2019-10-30 |
+------------+
```

Pass in and return a `DATE` type.

```sql
SELECT FROM_PROTO(DATE '2019-10-30')

+------------+
| $col1      |
+------------+
| 2019-10-30 |
+------------+
```

### TO_PROTO

```
TO_PROTO(expression)
```

**Description**

Returns a PROTO value. The valid `expression` types are defined in the
table below, along with the return types that they produce. Other input
`expression` types are invalid.

<table width="100%">
  <thead>
    <tr>
      <th width="50%"><code>expression</code> type</th>
      <th width="50%">Return type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <ul>
        <li>INT32</li>
        <li>google.protobuf.Int32Value</li>
        </ul>
      </td>
      <td>google.protobuf.Int32Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT32</li>
        <li>google.protobuf.UInt32Value</li>
        </ul>
      </td>
      <td>google.protobuf.UInt32Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>INT64</li>
        <li>google.protobuf.Int64Value</li>
        </ul>
      </td>
      <td>google.protobuf.Int64Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>UINT64</li>
        <li>google.protobuf.UInt64Value</li>
        </ul>
      </td>
      <td>google.protobuf.UInt64Value</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>FLOAT</li>
        <li>google.protobuf.FloatValue</li>
        </ul>
      </td>
      <td>google.protobuf.FloatValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DOUBLE</li>
        <li>google.protobuf.DoubleValue</li>
        </ul>
      </td>
      <td>google.protobuf.DoubleValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BOOL</li>
        <li>google.protobuf.BoolValue</li>
        </ul>
      </td>
      <td>google.protobuf.BoolValue</td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>STRING</li>
          <li>google.protobuf.StringValue</li>
        </ul>
      </td>
      <td>google.protobuf.StringValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>BYTES</li>
        <li>google.protobuf.BytesValue</li>
        </ul>
      </td>
      <td>google.protobuf.BytesValue</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>DATE</li>
        <li>google.type.Date</li>
        </ul>
      </td>
      <td>google.type.Date</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIME</li>
        <li>google.type.TimeOfDay</li>
        </ul>
      </td>
      <td>google.type.TimeOfDay</td>
    </tr>
    <tr>
      <td>
        <ul>
        <li>TIMESTAMP</li>
        <li>google.protobuf.Timestamp</li>
        </ul>
      </td>
      <td>google.protobuf.Timestamp</td>
    </tr>
  </tbody>
</table>

**Return Type**

The return type depends upon the `expression` type. See the return types
in the table above.

**Examples**

Convert a `DATE` type into a `google.type.Date` type.

```sql
SELECT TO_PROTO(DATE '2019-10-30')

+--------------------------------+
| $col1                          |
+--------------------------------+
| {year: 2019 month: 10 day: 30} |
+--------------------------------+
```

Pass in and return a `google.type.Date` type.

```sql
SELECT TO_PROTO(
  new google.type.Date(
    2019 as year,
    10 as month,
    30 as day
  )
)

+--------------------------------+
| $col1                          |
+--------------------------------+
| {year: 2019 month: 10 day: 30} |
+--------------------------------+
```

### EXTRACT {#proto_extract}

```sql
EXTRACT( extraction_type (proto_field) FROM proto_expression )

extraction_type:
  { FIELD | RAW | HAS | ONEOF_CASE }
```

**Description**

Extracts a value from a protocol buffer. `proto_expression` represents the
expression that returns a protocol buffer, `proto_field` represents the field
of the protocol buffer to extract from, and `extraction_type` determines the
type of data to return. `EXTRACT` can be used to get values of ambiguous fields.
An alternative to `EXTRACT` is the [dot operator][querying-protocol-buffers].

**Extraction Types**

You can choose the type of information to get with `EXTRACT`. Your choices are:

+  `FIELD`: Extract a value from a protocol buffer field.
+  `RAW`: Extract an uninterpreted value from a
    protocol buffer field. Raw values
    ignore any ZetaSQL type annotations.
+  `HAS`: Returns `TRUE` if a protocol buffer field is set in a proto message;
   otherwise, `FALSE`. Returns an error if this is used with a scalar proto3
   field. Alternatively, use [`has_x`][has-value], to perform this task.
+  `ONEOF_CASE`: Returns the name of the set protocol buffer field in a Oneof.
   If no field is set, returns an empty string.

**Return Type**

The return type depends upon the extraction type in the query.

+  `FIELD`: Protocol buffer field type.
+  `RAW`: Protocol buffer field
    type. Format annotations are
    ignored.
+  `HAS`: `BOOL`
+  `ONEOF_CASE`: `STRING`

**Examples**

The examples in this section reference two protocol buffers called `Album` and
`Chart`, and one table called `AlbumList`.

```proto
message Album {
  optional string album_name = 1;
  repeated string song = 2;
  oneof group_name {
    string solo = 3;
    string duet = 4;
    string band = 5;
  }
}
```

```proto
message Chart {
  optional int64 date = 1 [(zetasql.format) = DATE];
  optional string chart_name = 2;
  optional int64 rank = 3;
}
```

```sql
WITH AlbumList AS (
  SELECT
    NEW Album(
      'Beyonce' AS solo,
      'Lemonade' AS album_name,
      ['Sandcastles','Hold Up'] AS song) AS album_col,
    NEW Chart(
      'Billboard' AS chart_name,
      '2016-04-23' AS date,
      1 AS rank) AS chart_col
    UNION ALL
  SELECT
    NEW Album(
      'The Beetles' AS band,
      'Rubber Soul' AS album_name,
      ['The Word', 'Wait', 'Nowhere Man'] AS song) AS album_col,
    NEW Chart(
      'Billboard' AS chart_name,
      1 as rank) AS chart_col
)
SELECT * FROM AlbumList
```

The following example extracts the album names from a table called `AlbumList`
that contains a proto-typed column called `Album`.

```sql
SELECT EXTRACT(FIELD(album_name) FROM album_col) AS name_of_album
FROM AlbumList

+------------------+
| name_of_album    |
+------------------+
| Lemonade         |
| Rubber Soul      |
+------------------+
```

A table called `AlbumList` contains a proto-typed column called `Album`.
`Album` contains a field called `date`, which can store an integer. The
`date` field has an annotated format called `DATE` assigned to it, which means
that when you extract the value in this field, it returns a `DATE`, not an
`INT64`.

If you would like to return the value for `date` as an `INT64`, not
as a `DATE`, use the `RAW` extraction type in your query. For example:

```sql
SELECT
  EXTRACT(RAW(date) FROM chart_col) AS raw_date,
  EXTRACT(FIELD(date) FROM chart_col) AS formatted_date
FROM AlbumList

+----------+----------------+
| raw_date | formatted_date |
+----------+----------------+
| 16914    | 2016-04-23     |
| 0        | 1970-01-01     |
+----------+----------------+
```

The following example checks to see if release dates exist in a table called
`AlbumList` that contains a protocol buffer called `Chart`.

```sql
SELECT EXTRACT(HAS(date) FROM chart_col) AS has_release_date
FROM AlbumList

+------------------+
| has_release_date |
+------------------+
| TRUE             |
| FALSE            |
+------------------+
```

The following example extracts the group name that is assigned to an artist in
a table called `AlbumList`. The group name is set for exactly one
protocol buffer field inside of the `group_name` Oneof. The `group_name` Oneof
exists inside the `Chart` protocol buffer.

```sql
SELECT EXTRACT(ONEOF_CASE(group_name) FROM album_col) AS artist_type
FROM AlbumList;

+-------------+
| artist_type |
+-------------+
| solo        |
| band        |
+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[querying-protocol-buffers]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#querying_protocol_buffers

[has-value]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#checking_if_a_field_has_a_value

[querying-proto-extensions]: https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md#extensions

[field-access-operator]: #field-access-operator

<!-- mdlint on -->

## Security functions

ZetaSQL supports the following security functions.

### SESSION_USER

```
SESSION_USER()
```

**Description**

Returns the email address of the user that is running the query.

**Return Data Type**

STRING

**Example**

```sql
SELECT SESSION_USER() as user;

+----------------------+
| user                 |
+----------------------+
| jdoe@example.com     |
+----------------------+

```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

## Net functions

### NET.IP_FROM_STRING

```
NET.IP_FROM_STRING(addr_str)
```

**Description**

Converts an IPv4 or IPv6 address from text (STRING) format to binary (BYTES)
format in network byte order.

This function supports the following formats for `addr_str`:

+ IPv4: Dotted-quad format. For example, `10.1.2.3`.
+ IPv6: Colon-separated format. For example,
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. For more examples, see the
  [IP Version 6 Addressing Architecture][net-link-to-ipv6-rfc].

This function does not support [CIDR notation][net-link-to-cidr-notation], such as `10.1.2.3/32`.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  addr_str, FORMAT("%T", NET.IP_FROM_STRING(addr_str)) AS ip_from_string
FROM UNNEST([
  '48.49.50.51',
  '::1',
  '3031:3233:3435:3637:3839:4041:4243:4445',
  '::ffff:192.0.2.128'
]) AS addr_str;
```

| addr_str                                | ip_from_string                                                      |
|-----------------------------------------|---------------------------------------------------------------------|
| 48.49.50.51                             | b"0123"                                                             |
| ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
| 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
| ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |

### NET.SAFE_IP_FROM_STRING

```
NET.SAFE_IP_FROM_STRING(addr_str)
```

**Description**

Similar to [`NET.IP_FROM_STRING`][net-link-to-ip-from-string], but returns `NULL`
instead of throwing an error if the input is invalid.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  addr_str,
  FORMAT("%T", NET.SAFE_IP_FROM_STRING(addr_str)) AS safe_ip_from_string
FROM UNNEST([
  '48.49.50.51',
  '::1',
  '3031:3233:3435:3637:3839:4041:4243:4445',
  '::ffff:192.0.2.128',
  '48.49.50.51/32',
  '48.49.50',
  '::wxyz'
]) AS addr_str;
```

| addr_str                                | safe_ip_from_string                                                 |
|-----------------------------------------|---------------------------------------------------------------------|
| 48.49.50.51                             | b"0123"                                                             |
| ::1                                     | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" |
| 3031:3233:3435:3637:3839:4041:4243:4445 | b"0123456789@ABCDE"                                                 |
| ::ffff:192.0.2.128                      | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" |
| 48.49.50.51/32                          | NULL                                                                |
| 48.49.50                                | NULL                                                                |
| ::wxyz                                  | NULL                                                                |

### NET.IP_TO_STRING

```
NET.IP_TO_STRING(addr_bin)
```

**Description**
Converts an IPv4 or IPv6 address from binary (BYTES) format in network byte
order to text (STRING) format.

If the input is 4 bytes, this function returns an IPv4 address as a STRING. If
the input is 16 bytes, it returns an IPv6 address as a STRING.

If this function receives a `NULL` input, it returns `NULL`. If the input has
a length different from 4 or 16, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

STRING

**Example**

```sql
SELECT FORMAT("%T", x) AS addr_bin, NET.IP_TO_STRING(x) AS ip_to_string
FROM UNNEST([
  b"0123",
  b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
  b"0123456789@ABCDE",
  b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80"
]) AS x;
```

| addr_bin                                                            | ip_to_string                            |
|---------------------------------------------------------------------|-----------------------------------------|
| b"0123"                                                             | 48.49.50.51                             |
| b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" | ::1                                     |
| b"0123456789@ABCDE"                                                 | 3031:3233:3435:3637:3839:4041:4243:4445 |
| b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x80" | ::ffff:192.0.2.128                      |

### NET.IP_NET_MASK

```
NET.IP_NET_MASK(num_output_bytes, prefix_length)
```

**Description**

Returns a network mask: a byte sequence with length equal to `num_output_bytes`,
where the first `prefix_length` bits are set to 1 and the other bits are set to
0. `num_output_bytes` and `prefix_length` are INT64.
This function throws an error if `num_output_bytes` is not 4 (for IPv4) or 16
(for IPv6). It also throws an error if `prefix_length` is negative or greater
than `8 * num_output_bytes`.

**Return Data Type**

BYTES

**Example**

```sql
SELECT x, y, FORMAT("%T", NET.IP_NET_MASK(x, y)) AS ip_net_mask
FROM UNNEST([
  STRUCT(4 as x, 0 as y),
  (4, 20),
  (4, 32),
  (16, 0),
  (16, 1),
  (16, 128)
]);
```

| x  | y   | ip_net_mask                                                         |
|----|-----|---------------------------------------------------------------------|
| 4  | 0   | b"\x00\x00\x00\x00"                                                 |
| 4  | 20  | b"\xff\xff\xf0\x00"                                                 |
| 4  | 32  | b"\xff\xff\xff\xff"                                                 |
| 16 | 0   | b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
| 16 | 1   | b"\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" |
| 16 | 128 | b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" |

### NET.IP_TRUNC

```
NET.IP_TRUNC(addr_bin, prefix_length)
```

**Description**
Takes `addr_bin`, an IPv4 or IPv6 address in binary (BYTES) format in network
byte order, and returns a subnet address in the same format. The result has the
same length as `addr_bin`, where the first `prefix_length` bits are equal to
those in `addr_bin` and the remaining bits are 0.

This function throws an error if `LENGTH(addr_bin)` is not 4 or 16, or if
`prefix_len` is negative or greater than `LENGTH(addr_bin) * 8`.

**Return Data Type**

BYTES

**Example**

```sql
SELECT
  FORMAT("%T", x) as addr_bin, prefix_length,
  FORMAT("%T", NET.IP_TRUNC(x, prefix_length)) AS ip_trunc
FROM UNNEST([
  STRUCT(b"\xAA\xBB\xCC\xDD" as x, 0 as prefix_length),
  (b"\xAA\xBB\xCC\xDD", 11), (b"\xAA\xBB\xCC\xDD", 12),
  (b"\xAA\xBB\xCC\xDD", 24), (b"\xAA\xBB\xCC\xDD", 32),
  (b'0123456789@ABCDE', 80)
]);
```

| addr_bin            | prefix_length | ip_trunc                              |
|---------------------|---------------|---------------------------------------|
| b"\xaa\xbb\xcc\xdd" | 0             | b"\x00\x00\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 11            | b"\xaa\xa0\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 12            | b"\xaa\xb0\x00\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 24            | b"\xaa\xbb\xcc\x00"                   |
| b"\xaa\xbb\xcc\xdd" | 32            | b"\xaa\xbb\xcc\xdd"                   |
| b"0123456789@ABCDE" | 80            | b"0123456789\x00\x00\x00\x00\x00\x00" |

### NET.IPV4_FROM_INT64

```
NET.IPV4_FROM_INT64(integer_value)
```

**Description**

Converts an IPv4 address from integer format to binary (BYTES) format in network
byte order. In the integer input, the least significant bit of the IP address is
stored in the least significant bit of the integer, regardless of host or client
architecture. For example, `1` means `0.0.0.1`, and `0x1FF` means `0.0.1.255`.

This function checks that either all the most significant 32 bits are 0, or all
the most significant 33 bits are 1 (sign-extended from a 32-bit integer).
In other words, the input should be in the range `[-0x80000000, 0xFFFFFFFF]`;
otherwise, this function throws an error.

This function does not support IPv6.

**Return Data Type**

BYTES

**Example**

```sql
SELECT x, x_hex, FORMAT("%T", NET.IPV4_FROM_INT64(x)) AS ipv4_from_int64
FROM (
  SELECT CAST(x_hex AS INT64) x, x_hex
  FROM UNNEST(["0x0", "0xABCDEF", "0xFFFFFFFF", "-0x1", "-0x2"]) AS x_hex
);
```

| x          | x_hex      | ipv4_from_int64     |
|------------|------------|---------------------|
| 0          | 0x0        | b"\x00\x00\x00\x00" |
| 11259375   | 0xABCDEF   | b"\x00\xab\xcd\xef" |
| 4294967295 | 0xFFFFFFFF | b"\xff\xff\xff\xff" |
| -1         | -0x1       | b"\xff\xff\xff\xff" |
| -2         | -0x2       | b"\xff\xff\xff\xfe" |

### NET.IPV4_TO_INT64

```
NET.IPV4_TO_INT64(addr_bin)
```

**Description**

Converts an IPv4 address from binary (BYTES) format in network byte order to
integer format. In the integer output, the least significant bit of the IP
address is stored in the least significant bit of the integer, regardless of
host or client architecture. For example, `1` means `0.0.0.1`, and `0x1FF` means
`0.0.1.255`. The output is in the range `[0, 0xFFFFFFFF]`.

If the input length is not 4, this function throws an error.

This function does not support IPv6.

**Return Data Type**

INT64

**Example**

```sql
SELECT
  FORMAT("%T", x) AS addr_bin,
  FORMAT("0x%X", NET.IPV4_TO_INT64(x)) AS ipv4_to_int64
FROM
UNNEST([b"\x00\x00\x00\x00", b"\x00\xab\xcd\xef", b"\xff\xff\xff\xff"]) AS x;
```

| addr_bin            | ipv4_to_int64 |
|---------------------|---------------|
| b"\x00\x00\x00\x00" | 0x0           |
| b"\x00\xab\xcd\xef" | 0xABCDEF      |
| b"\xff\xff\xff\xff" | 0xFFFFFFFF    |

### NET.FORMAT_IP (DEPRECATED)

```
NET.FORMAT_IP(integer)
```

**Description**

This function is deprecated. It is the same as
[`NET.IP_TO_STRING`][net-link-to-ip-to-string]`(`[`NET.IPV4_FROM_INT64`][net-link-to-ipv4-from-int64]`(integer))`,
except that this function does not allow negative input values.

**Return Data Type**

STRING

### NET.PARSE_IP (DEPRECATED)

```
NET.PARSE_IP(addr_str)
```

**Description**

This function is deprecated. It is the same as
[`NET.IPV4_TO_INT64`][net-link-to-ipv4-to-int64]`(`[`NET.IP_FROM_STRING`][net-link-to-ip-from-string]`(addr_str))`,
except that this function truncates the input at the first `'\x00'` character,
if any, while `NET.IP_FROM_STRING` treats `'\x00'` as invalid.

**Return Data Type**

INT64

### NET.FORMAT_PACKED_IP (DEPRECATED)

```
NET.FORMAT_PACKED_IP(bytes_value)
```

**Description**

This function is deprecated. It is the same as [`NET.IP_TO_STRING`][net-link-to-ip-to-string].

**Return Data Type**

STRING

### NET.PARSE_PACKED_IP (DEPRECATED)

```
NET.PARSE_PACKED_IP(addr_str)
```

**Description**

This function is deprecated. It is the same as
[`NET.IP_FROM_STRING`][net-link-to-ip-from-string], except that this function truncates
the input at the first `'\x00'` character, if any, while `NET.IP_FROM_STRING`
treats `'\x00'` as invalid.

**Return Data Type**

BYTES

### NET.IP_IN_NET

```
NET.IP_IN_NET(address, subnet)
```

**Description**

Takes an IP address and a subnet CIDR as STRING and returns true if the IP
address is contained in the subnet.

This function supports the following formats for `address` and `subnet`:

+ IPv4: Dotted-quad format. For example, `10.1.2.3`.
+ IPv6: Colon-separated format. For example,
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. For more examples, see the
  [IP Version 6 Addressing Architecture][net-link-to-ipv6-rfc].
+ CIDR (IPv4): Dotted-quad format. For example, `10.1.2.0/24`
+ CIDR (IPv6): Colon-separated format. For example, `1:2::/48`.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

BOOL

### NET.MAKE_NET

```
NET.MAKE_NET(address, prefix_length)
```

**Description**

Takes an IPv4 or IPv6 address as STRING and an integer representing the prefix
length (the number of leading 1-bits in the network mask). Returns a
STRING representing the [CIDR subnet][net-link-to-cidr-notation] with the given prefix length.

The value of `prefix_length` must be greater than or equal to 0. A smaller value
means a bigger subnet, covering more IP addresses. The result CIDR subnet must
be no smaller than `address`, meaning that the value of `prefix_length` must be
less than or equal to the prefix length in `address`. See the effective upper
bound below.

This function supports the following formats for `address`:

+ IPv4: Dotted-quad format, such as `10.1.2.3`. The value of `prefix_length`
  must be less than or equal to 32.
+ IPv6: Colon-separated format, such as
  `1234:5678:90ab:cdef:1234:5678:90ab:cdef`. The value of `prefix_length` must
  be less than or equal to 128.
+ CIDR (IPv4): Dotted-quad format, such as `10.1.2.0/24`.
  The value of `prefix_length` must be less than or equal to the number after
  the slash in `address` (24 in the example), which must be less than or equal
  to 32.
+ CIDR (IPv6): Colon-separated format, such as `1:2::/48`.
  The value of `prefix_length` must be less than or equal to the number after
  the slash in `address` (48 in the example), which must be less than or equal
  to 128.

If this function receives a `NULL` input, it returns `NULL`. If the input is
considered invalid, an `OUT_OF_RANGE` error occurs.

**Return Data Type**

STRING

### NET.HOST

```
NET.HOST(url)
```

**Description**

Takes a URL as a STRING and returns the host as a STRING. For best results, URL
values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply with RFC 3986 formatting,
this function makes a best effort to parse the input and return a relevant
result. If the function cannot parse the input, it
returns NULL.

<p class="note"><b>Note:</b> The function does not perform any normalization.</a>
</p>

**Return Data Type**

STRING

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                               | description                                                                   | host               | suffix  | domain         |
|---------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                  | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                    | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                 | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                  | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                              | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"    |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;" | non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK"|
| "mailto:?to=&subject=&body="                                        | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

### NET.PUBLIC_SUFFIX

```
NET.PUBLIC_SUFFIX(url)
```

**Description**

Takes a URL as a STRING and returns the public suffix (such as `com`, `org`,
or `net`) as a STRING. A public suffix is an ICANN domain registered at
[publicsuffix.org][net-link-to-public-suffix]. For best results, URL values
should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply
with RFC 3986 formatting, this function makes a best effort to parse the input
and return a relevant result.

This function returns NULL if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle (not leading or trailing);
+ The parsed host does not contain any public suffix.

Before looking up the public suffix, this function temporarily normalizes the
host by converting upper case English letters to lower case and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode].
The function then returns the public suffix as part of the original host instead
of the normalized host.

<p class="note"><b>Note:</b> The function does not perform
<a href="https://en.wikipedia.org/wiki/Unicode_equivalence">Unicode normalization</a>.
</p>

<p class="note"><b>Note:</b> The public suffix data at
<a href="https://publicsuffix.org/list/">publicsuffix.org</a> also contains
private domains. This function ignores the private domains.</p>

<p class="note"><b>Note:</b> The public suffix data may change over time.
Consequently, input that produces a NULL result now may produce a non-NULL value
in the future.</p>

**Return Data Type**

STRING

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                              | description                                                                   | host               | suffix  | domain         |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                 | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                   | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                 | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                            | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"     |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;"| non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK |
| "mailto:?to=&subject=&body="                                       | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

### NET.REG_DOMAIN

```
NET.REG_DOMAIN(url)
```

**Description**

Takes a URL as a STRING and returns the registered or registerable domain (the
[public suffix](#netpublic_suffix) plus one preceding label), as a
STRING. For best results, URL values should comply with the format as defined by
[RFC 3986][net-link-to-rfc-3986-appendix-a]. If the URL value does not comply with RFC 3986 formatting,
this function makes a best effort to parse the input and return a relevant
result.

This function returns NULL if any of the following is true:

+ It cannot parse the host from the input;
+ The parsed host contains adjacent dots in the middle (not leading or trailing);
+ The parsed host does not contain any public suffix;
+ The parsed host contains only a public suffix without any preceding label.

Before looking up the public suffix, this function temporarily normalizes the
host by converting upper case English letters to lowercase and encoding all
non-ASCII characters with [Punycode][net-link-to-punycode]. The function then returns
the registered or registerable domain as part of the original host instead of
the normalized host.

<p class="note"><b>Note:</b> The function does not perform
<a href="https://en.wikipedia.org/wiki/Unicode_equivalence">Unicode normalization</a>.
</p>

<p class="note"><b>Note:</b> The public suffix data at
<a href="https://publicsuffix.org/list/">publicsuffix.org</a> also contains
private domains. This function does not treat a private domain as a public
suffix. For example, if "us.com" is a private domain in the public suffix data,
NET.REG_DOMAIN("foo.us.com") returns "us.com" (the public suffix "com" plus
the preceding label "us") rather than "foo.us.com" (the private domain "us.com"
plus the preceding label "foo").
</p>

<p class="note"><b>Note:</b> The public suffix data may change over time.
Consequently, input that produces a NULL result now may produce a non-NULL value
in the future.</p>

**Return Data Type**

STRING

**Example**

```sql
SELECT
  FORMAT("%T", input) AS input,
  description,
  FORMAT("%T", NET.HOST(input)) AS host,
  FORMAT("%T", NET.PUBLIC_SUFFIX(input)) AS suffix,
  FORMAT("%T", NET.REG_DOMAIN(input)) AS domain
FROM (
  SELECT "" AS input, "invalid input" AS description
  UNION ALL SELECT "http://abc.xyz", "standard URL"
  UNION ALL SELECT "//user:password@a.b:80/path?query",
                   "standard URL with relative scheme, port, path and query, but no public suffix"
  UNION ALL SELECT "https://[::1]:80", "standard URL with IPv6 host"
  UNION ALL SELECT "http://例子.卷筒纸.中国", "standard URL with internationalized domain name"
  UNION ALL SELECT "    www.Example.Co.UK    ",
                   "non-standard URL with spaces, upper case letters, and without scheme"
  UNION ALL SELECT "mailto:?to=&subject=&body=", "URI rather than URL--unsupported"
);
```

| input                                                              | description                                                                   | host               | suffix  | domain         |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------|--------------------|---------|----------------|
| ""                                                                 | invalid input                                                                 | NULL               | NULL    | NULL           |
| "http://abc.xyz"                                                   | standard URL                                                                  | "abc.xyz"          | "xyz"   | "abc.xyz"      |
| "//user:password@a.b:80/path?query"                                | standard URL with relative scheme, port, path and query, but no public suffix | "a.b"              | NULL    | NULL           |
| "https://[::1]:80"                                                 | standard URL with IPv6 host                                                   | "[::1]"            | NULL    | NULL           |
| "http://例子.卷筒纸.中国"                                            | standard URL with internationalized domain name                               | "例子.卷筒纸.中国"    | "中国"  | "卷筒纸.中国"  |
| "&nbsp;&nbsp;&nbsp;&nbsp;www.Example.Co.UK&nbsp;&nbsp;&nbsp;&nbsp;"| non-standard URL with spaces, upper case letters, and without scheme          | "www.Example.Co.UK"| "Co.UK" | "Example.Co.UK"|
| "mailto:?to=&subject=&body="                                       | URI rather than URL--unsupported                                              | "mailto"           | NULL    | NULL           |

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[net-link-to-ipv6-rfc]: http://www.ietf.org/rfc/rfc2373.txt

[net-link-to-cidr-notation]: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing

[net-link-to-rfc-3986-appendix-a]: https://tools.ietf.org/html/rfc3986#appendix-A

[net-link-to-public-suffix]: https://publicsuffix.org/list/

[net-link-to-punycode]: https://en.wikipedia.org/wiki/Punycode

[net-link-to-ip-from-string]: #netip_from_string

[net-link-to-ip-to-string]: #netip_to_string

[net-link-to-ipv4-from-int64]: #netipv4_from_int64

[net-link-to-ipv4-to-int64]: #netipv4_to_int64

<!-- mdlint on -->

## Operators

Operators are represented by special characters or keywords; they do not use
function call syntax. An operator manipulates any number of data inputs, also
called operands, and returns a result.

Common conventions:

+  Unless otherwise specified, all operators return `NULL` when one of the
   operands is `NULL`.
+  All operators will throw an error if the computation result overflows.
+  For all floating point operations, `+/-inf` and `NaN` may only be returned
   if one of the operands is `+/-inf` or `NaN`. In other cases, an error is
   returned.

### Operator precedence

The following table lists all ZetaSQL operators from highest to
lowest precedence, i.e. the order in which they will be evaluated within a
statement.

<table>
  <thead>
    <tr>
      <th>Order of Precedence</th>
      <th>Operator</th>
      <th>Input Data Types</th>
      <th>Name</th>
      <th>Operator Arity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>Field access operator</td>
      <td><span> JSON</span><br><span> PROTO</span><br><span> STRUCT</span><br></td>
      <td>Field access operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array elements field access operator</td>
      <td>ARRAY</td>
      <td>Field access operator for elements in an array</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>Array subscript operator</td>
      <td>ARRAY</td>
      <td>Array position. Must be used with OFFSET or ORDINAL&mdash;see
      <a href="#array_functions">Array Functions</a>

.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>JSON subscript operator</td>
      <td>JSON</td>
      <td>Field name or array position in JSON.</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>2</td>
      <td>+</td>
      <td>All numeric types</td>
      <td>Unary plus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>-</td>
      <td>All numeric types</td>
      <td>Unary minus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>~</td>
      <td>Integer or BYTES</td>
      <td>Bitwise not</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>3</td>
      <td>*</td>
      <td>All numeric types</td>
      <td>Multiplication</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>/</td>
      <td>All numeric types</td>
      <td>Division</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>&nbsp;</td>
      <td>||</td>
      <td>STRING, BYTES, or ARRAY&#60;T&#62;</td>
      <td>Concatenation operator</td>
      <td>Binary</td>
    </tr>
    
    <tr>
      <td>4</td>
      <td>+</td>
      <td>
        All numeric types, DATE with
        INT64
        , INTERVAL
      </td>
      <td>Addition</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>-</td>
      <td>
        All numeric types, DATE with
        INT64
        , INTERVAL
      </td>
      <td>Subtraction</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>5</td>
      <td>&lt;&lt;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise left-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;&gt;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise right-shift</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>6</td>
      <td>&amp;</td>
      <td>Integer or BYTES</td>
      <td>Bitwise and</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>7</td>
      <td>^</td>
      <td>Integer or BYTES</td>
      <td>Bitwise xor</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>8</td>
      <td>|</td>
      <td>Integer or BYTES</td>
      <td>Bitwise or</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>9 (Comparison Operators)</td>
      <td>=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Less than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;=</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Greater than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>!=, &lt;&gt;</td>
      <td>Any comparable type. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Not equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] LIKE</td>
      <td>STRING and byte</td>
      <td>Value does [not] match the pattern specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] BETWEEN</td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] within the range specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] IN</td>
      <td>Any comparable types. See
      <a href="https://github.com/google/zetasql/blob/master/docs/data-types.md">Data Types</a>

      for a complete list.</td>
      <td>Value is [not] in the set of values specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] <code>NULL</code></td>
      <td>All</td>
      <td>Value is [not] <code>NULL</code></td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] TRUE</td>
      <td>BOOL</td>
      <td>Value is [not] TRUE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] FALSE</td>
      <td>BOOL</td>
      <td>Value is [not] FALSE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>10</td>
      <td>NOT</td>
      <td>BOOL</td>
      <td>Logical NOT</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>11</td>
      <td>AND</td>
      <td>BOOL</td>
      <td>Logical AND</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>12</td>
      <td>OR</td>
      <td>BOOL</td>
      <td>Logical OR</td>
      <td>Binary</td>
    </tr>
  </tbody>
</table>

Operators with the same precedence are left associative. This means that those
operators are grouped together starting from the left and moving right. For
example, the expression:

`x AND y AND z`

is interpreted as

`( ( x AND y ) AND z )`

The expression:

```
x * y / z
```

is interpreted as:

```
( ( x * y ) / z )
```

All comparison operators have the same priority, but comparison operators are
not associative. Therefore, parentheses are required in order to resolve
ambiguity. For example:

`(x < y) IS FALSE`

### Field access operator 
<a id="field_access_operator"></a>

```
expression.fieldname[. ...]
```

**Description**

Gets the value of a field. Alternatively known as the dot operator. Can be
used to access nested fields. For example, `expression.fieldname1.fieldname2`.

**Input types**

+ `STRUCT`
+ `PROTO`
+ `JSON`

**Return type**

+ For `STRUCT`: SQL data type of `fieldname`. If a field is not found in
  the struct, an error is thrown.
+ For `PROTO`: SQL data type of `fieldname`. If a field is not found in
  the protocol buffer, an error is thrown.
+ For `JSON`: `JSON`. If a field is not found in a JSON value, a SQL `NULL` is
  returned.

**Example**

In the following example, the expression is `t.customer` and the
field access operations are `.address` and `.country`. An operation is an
application of an operator (`.`) to specific operands (in this case,
`address` and `country`, or more specifically, `t.customer` and `address`,
for the first operation, and `t.customer.address` and `country` for the
second operation).

```sql
WITH orders AS (
  SELECT STRUCT(STRUCT('Yonge Street' AS street, 'Canada' AS country) AS address) AS customer
)
SELECT t.customer.address.country FROM orders AS t;

+---------+
| country |
+---------+
| Canada  |
+---------+
```

### Array subscript operator 
<a id="array_subscript_operator"></a>

```
array_expression[array_subscript_specifier]

array_subscript_specifier:
  position_keyword(index)

position_keyword:
  { OFFSET | SAFE_OFFSET | ORDINAL | SAFE_ORDINAL }
```

Note: The brackets (`[]`) around `array_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

Gets a value from an array at a specific location.

**Input types**

+ `array_expression`: The input array.
+ `position_keyword`: Where the index for the array should start and how
  out-of-range indexes are handled. Your choices are:
  + `OFFSET`: The index starts at zero.
    Produces an error if the index is out of range.
  + `SAFE_OFFSET`: The index starts at
    zero. Returns `NULL` if the index is out of range.
  + `ORDINAL`: The index starts at one.
    Produces an error if the index is out of range.
  + `SAFE_ORDINAL`: The index starts at
    one. Returns `NULL` if the index is out of range.
+ `index`: An integer that represents a specific position in the array.

**Return type**

`T` where `array_expression` is `ARRAY<T>`.

**Examples**

In this example, the array subscript operator is used to return values at
specific locations in `item_array`. This example also shows what happens when
you reference an index (`6`) in an array that is out of range. If the
`SAFE` prefix is included, `NULL` is returned, otherwise an error is produced.

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array,
  item_array[OFFSET(1)] AS item_offset,
  item_array[ORDINAL(1)] AS item_ordinal,
  item_array[SAFE_OFFSET(6)] AS item_safe_offset,
FROM Items

+----------------------------------+--------------+--------------+------------------+
| item_array                       | item_offset  | item_ordinal | item_safe_offset |
+----------------------------------+--------------+--------------+------------------+
| [coffee, tea, milk]              | tea          | coffee       | NULL             |
+----------------------------------+--------------+--------------+------------------+
```

In the following example, when you reference an index in an array that is out of
range and the `SAFE` prefix is not included, an error is produced.

```sql
WITH Items AS (SELECT ["coffee", "tea", "milk"] AS item_array)
SELECT
  item_array[OFFSET(6)] AS item_offset
FROM Items

-- Error. OFFSET(6) is out of range.
```

### JSON subscript operator

```
json_expression[array_element_id]
```

```
json_expression[field_name]
```

Note: The brackets (`[]`) around `array_element_id` and `field_name` are part
of the syntax; they do not represent an optional part.

**Description**

Gets a value of an array element or field in a JSON expression. Can be
used to access nested data.

**Input types**

+ `JSON expression`: The `JSON` expression that contains an array element or
  field to return.
+ `[array_element_id]`: An `INT64` expression that represents a zero-based index
  in the array. If a negative value is entered, or the value is greater than
  or equal to the size of the array, or the JSON expression doesn't represent
  a JSON array, a SQL `NULL` is returned.
+ `[field_name]`: A `STRING` expression that represents the name of a field in
  JSON. If the field name is not found, or the JSON expression is not a
  JSON object, a SQL `NULL` is returned.

**Return type**

`JSON`

**Example**

In the following example:

+ `json_value` is a JSON expression.
+ `.class` is a JSON field access.
+ `.students` is a JSON field access.
+ `[0]` is a JSON subscript expression with an element offset that
  accesses the zeroth element of an array in the JSON value.
+ `['name']` is a JSON subscript expression with a field name that
  accesses a field.

```sql
SELECT json_value.class.students[0]['name'] AS first_student
FROM
  UNNEST(
    [
      JSON '{"class" : {"students" : [{"name" : "Jane"}]}}',
      JSON '{"class" : {"students" : []}}',
      JSON '{"class" : {"students" : [{"name" : "John"}, {"name": "Jamie"}]}}'])
    AS json_value;

+-----------------+
| first_student   |
+-----------------+
| "Jane"          |
| NULL            |
| "John"          |
+-----------------+
```

### Array elements field access operator 
<a id="array_el_field_operator"></a>

```
array_expression.field_or_element[. ...]

field_or_element:
  { fieldname | array_element }

array_element:
  array_fieldname[array_subscript_specifier]
```

Note: The brackets (`[]`) around `array_subscript_specifier` are part of the
syntax; they do not represent an optional part.

**Description**

The array elements field access operation lets you traverse through the
levels of a nested data type inside an array.

**Input types**

+ `array_expression`: An expression that evaluates to an `ARRAY` value.
+ `field_or_element[. ...]`: The field to access. This can also be a position
  in an `ARRAY`-typed field.
+ `fieldname`: The name of the field to access.

  For example, this query returns all values for the `items` field inside of the
  `my_array` array expression:

  ```sql
  WITH T AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items)
  FROM T
  ```
+ `array_element`: If the field to access is an `ARRAY` field (`array_field`),
  you can additionally access a specific position in the field
  with the [array subscript operator][array-subscript-operator]
  (`[array_subscript_specifier]`). This operation returns only elements at a
  selected position, rather than all elements, in the array field.

  For example, this query only returns values at position 0 in the `items`
  array field:

  ```sql
  WITH T AS ( SELECT [STRUCT(['foo', 'bar'] AS items)] AS my_array )
  SELECT FLATTEN(my_array.items[OFFSET(0)])
  FROM T
  ```

**Details**

The array elements field access operation is not a typical expression
that returns a typed value; it represents a concept outside the type system
and can only be interpreted by the following operations:

+  [`FLATTEN` operation][flatten-operation]: Returns an array. For example:

   ```sql
   FLATTEN(x.y.z)
   ```
+  [`UNNEST` operation][operators-link-to-unnest]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `FLATTEN` operator.
   For example, these do the same thing:

   ```sql
   UNNEST(x.y.z)
   ```

   ```sql
   UNNEST(FLATTEN(x.y.z))
   ```
+  [`FROM` operation][operators-link-to-from-clause]: Returns a table.
   `array_expression` must be a path expression.
   Implicitly implements the `UNNEST` operator and the `FLATTEN` operator.
   For example, these do the same thing:

   ```sql
   SELECT * FROM T, T.x.y.z;
   ```

   ```sql
   SELECT * FROM T, UNNEST(x.y.z);
   ```

   ```sql
   SELECT * FROM T, UNNEST(FLATTEN(x.y.z));
   ```

If `NULL` array elements are encountered, they are added to the resulting array.

**Common shapes of this operation**

This operation can take several shapes. The right-most value in
the operation determines what type of array is returned. Here are some example
shapes and a description of what they return:

The following shapes extract the final non-array field from each element of
an array expression and return an array of those non-array field values.

+ `array_expression.non_array_field_1`
+ `array_expression.non_array_field_1.array_field.non_array_field_2`

The following shapes extract the final array field from each element of the
array expression and concatenate the array fields together.
An empty array or a `NULL` array contributes no elements to the resulting array.

+ `array_expression.non_array_field_1.array_field_1`
+ `array_expression.non_array_field_1.array_field_1.non_array_field_2.array_field_2`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1`

The following shapes extract the final array field from each element of the
array expression at a specific position. Then they return an array of those
extracted elements. An empty array or a `NULL` array contributes no elements
to the resulting array.

+ `array_expression.non_array_field_1.array_field_1[OFFSET(1)]`
+ `array_expression.non_array_field_1.array_field_1[SAFE_OFFSET(1)]`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1[ORDINAL(2)]`
+ `array_expression.non_array_field_1.non_array_field_2.array_field_1[SAFE_ORDINAL(2)]`

**Return Value**

+ `FLATTEN` of an array element access operation returns an `ARRAY`.
+ `UNNEST` of an array element access operation, whether explicit or implicit,
   returns a table.

**Examples**

The next examples in this section reference a table called `T`, that contains
a nested struct in an array called `my_array`:

```sql
WITH
  T AS (
    SELECT
      [
        STRUCT(
          [
            STRUCT([25.0, 75.0] AS prices),
            STRUCT([30.0] AS prices)
          ] AS sales
        )
      ] AS my_array
  )
SELECT * FROM T;

+----------------------------------------------+
| my_array                                     |
+----------------------------------------------+
| [{[{[25, 75] prices}, {[30] prices}] sales}] |
+----------------------------------------------+
```

This is what the array elements field access operator looks like in the
`FLATTEN` operator:

```sql
SELECT FLATTEN(my_array.sales.prices) AS all_prices FROM T;

+--------------+
| all_prices   |
+--------------+
| [25, 75, 30] |
+--------------+
```

This is how you use the array subscript operator to only return values at a
specific index in the `prices` array:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(0)]) AS first_prices FROM T;

+--------------+
| first_prices |
+--------------+
| [25, 30]     |
+--------------+
```

This is an example of an explicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM T, UNNEST(my_array.sales.prices) AS all_prices

+------------+
| all_prices |
+------------+
| 25         |
| 75         |
| 30         |
+------------+
```

This is an example of an implicit `UNNEST` operation that includes the
array elements field access operator:

```sql
SELECT all_prices FROM T, T.my_array.sales.prices AS all_prices

+------------+
| all_prices |
+------------+
| 25         |
| 75         |
| 30         |
+------------+
```

This query produces an error because one of the `prices` arrays does not have
an element at index `1` and `OFFSET` is used:

```sql
SELECT FLATTEN(my_array.sales.prices[OFFSET(1)]) AS second_prices FROM T;

-- Error
```

This query is like the previous query, but `SAFE_OFFSET` is used. This
produces a `NULL` value instead of an error.

```sql
SELECT FLATTEN(my_array.sales.prices[SAFE_OFFSET(1)]) AS second_prices FROM T;

+---------------+
| second_prices |
+---------------+
| [75, NULL]    |
+---------------+
```

In this next example, an empty array and a `NULL` field value have been added to
the query. These contribute no elements to the result.

```sql
WITH
  T AS (
    SELECT
      [
        STRUCT(
          [
            STRUCT([25.0, 75.0] AS prices),
            STRUCT([30.0] AS prices),
            STRUCT(ARRAY<DOUBLE>[] AS prices),
            STRUCT(NULL AS prices)
          ] AS sales
        )
      ] AS my_array
  )
SELECT FLATTEN(my_array.sales.prices) AS first_prices FROM T;

+--------------+
| first_prices |
+--------------+
| [25, 75, 30] |
+--------------+
```

The next examples in this section reference a protocol buffer called
`Album` that looks like this:

```proto
message Album {
  optional string album_name = 1;
  repeated string song = 2;
  oneof group_name {
    string solo = 3;
    string duet = 4;
    string band = 5;
  }
}
```

Nested data is common in protocol buffers that have data within repeated
messages. The following example extracts a flattened array of songs from a
table called `AlbumList` that contains a column called `Album` of type `PROTO`.

```sql
WITH
  AlbumList AS (
    SELECT
      [
        NEW Album(
          'OneWay' AS album_name,
          ['North', 'South'] AS song,
          'Crossroads' AS band),
        NEW Album(
          'After Hours' AS album_name,
          ['Snow', 'Ice', 'Water'] AS song,
          'Sunbirds' AS band)]
        AS albums_array
  )
SELECT FLATTEN(albums_array.song) AS songs FROM AlbumList

+------------------------------+
| songs                        |
+------------------------------+
| [North,South,Snow,Ice,Water] |
+------------------------------+
```

The following example extracts a flattened array of album names from a
table called `AlbumList` that contains a proto-typed column called `Album`.

```sql
WITH
  AlbumList AS (
    SELECT
      [
        (
          SELECT
            NEW Album(
              'OneWay' AS album_name,
              ['North', 'South'] AS song,
              'Crossroads' AS band) AS album_col
        ),
        (
          SELECT
            NEW Album(
              'After Hours' AS album_name,
              ['Snow', 'Ice', 'Water'] AS song,
              'Sunbirds' AS band) AS album_col
        )]
        AS albums_array
  )
SELECT names FROM AlbumList, UNNEST(albums_array.album_name) AS names

+----------------------+
| names                |
+----------------------+
| [OneWay,After Hours] |
+----------------------+
```

### Arithmetic operators

All arithmetic operators accept input of numeric type T, and the result type
has type T unless otherwise indicated in the description below:

<table>
  <thead>
    <tr>
    <th>Name</th>
    <th>Syntax</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Addition</td>
      <td>X + Y</td>
    </tr>
    <tr>
      <td>Subtraction</td>
      <td>X - Y</td>
    </tr>
    <tr>
      <td>Multiplication</td>
      <td>X * Y</td>
    </tr>
    <tr>
      <td>Division</td>
      <td>X / Y</td>
    </tr>
    <tr>
      <td>Unary Plus</td>
      <td>+ X</td>
    </tr>
    <tr>
      <td>Unary Minus</td>
      <td>- X</td>
    </tr>
  </tbody>
</table>

NOTE: Divide by zero operations return an error. To return a different result,
consider the IEEE_DIVIDE or SAFE_DIVIDE functions.

Result types for Addition and Multiplication:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Subtraction:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Division:

<table style="font-size:small">

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>INT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>INT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT32</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>UINT64</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>NUMERIC</th><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>BIGNUMERIC</th><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>FLOAT</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
<tr><th>DOUBLE</th><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Unary Plus:

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">UINT32</td><td style="vertical-align:middle">UINT64</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

Result types for Unary Minus:

<table>

<thead>
<tr>
<th>INPUT</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th><th>NUMERIC</th><th>BIGNUMERIC</th><th>FLOAT</th><th>DOUBLE</th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle">INT32</td><td style="vertical-align:middle">INT64</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">ERROR</td><td style="vertical-align:middle">NUMERIC</td><td style="vertical-align:middle">BIGNUMERIC</td><td style="vertical-align:middle">FLOAT</td><td style="vertical-align:middle">DOUBLE</td></tr>
</tbody>

</table>

### Date arithmetics operators
Operators '+' and '-' can be used for arithmetic operations on dates.

```sql
date_expression + int64_expression
int64_expression + date_expression
date_expression - int64_expression
```

**Description**

Adds or subtracts `int64_expression` days to or from `date_expression`. This is
equivalent to `DATE_ADD` or `DATE_SUB` functions, when interval is expressed in
days.

**Return Data Type**

DATE

**Example**

```sql
SELECT DATE "2020-09-22" + 1 AS day_later, DATE "2020-09-22" - 7 AS week_ago

+------------+------------+
| day_later  | week_ago   |
+------------+------------+
| 2020-09-23 | 2020-09-15 |
+------------+------------+
```

### Datetime subtraction

```sql
date_expression - date_expression
timestamp_expression - timestamp_expression
datetime_expression - datetime_expression
```

**Description**

Computes the difference between two datetime values as an interval.

**Return Data Type**

INTERVAL

**Example**

```sql
SELECT
  DATE "2021-05-20" - DATE "2020-04-19" AS date_diff,
  TIMESTAMP "2021-06-01 12:34:56.789" - TIMESTAMP "2021-05-31 00:00:00" AS time_diff

+-------------------+------------------------+
| date_diff         | time_diff              |
+-------------------+------------------------+
| 0-0 396 0:0:0     | 0-0 0 36:34:56.789     |
+-------------------+------------------------+
```

### Interval arithmetic operators

**Addition and subtraction**

```sql
date_expression + interval_expression = DATETIME
date_expression - interval_expression = DATETIME
timestamp_expression + interval_expression = TIMESTAMP
timestamp_expression - interval_expression = TIMESTAMP
datetime_expression + interval_expression = DATETIME
datetime_expression - interval_expression = DATETIME

```

**Description**

Adds an interval to a datetime value or subtracts an interval from a datetime
value.
**Example**

```sql
SELECT
  DATE "2021-04-20" + INTERVAL 25 HOUR AS date_plus,
  TIMESTAMP "2021-05-02 00:01:02.345" - INTERVAL 10 SECOND AS time_minus;

+-------------------------+--------------------------------+
| date_plus               | time_minus                     |
+-------------------------+--------------------------------+
| 2021-04-21 01:00:00     | 2021-05-02 00:00:52.345+00     |
+-------------------------+--------------------------------+
```

**Multiplication and division**

```sql
interval_expression * integer_expression = INTERVAL
interval_expression / integer_expression = INTERVAL

```

**Description**

Multiplies or divides an interval value by an integer.

**Example**

```sql
SELECT
  INTERVAL '1:2:3' HOUR TO SECOND * 10 AS mul1,
  INTERVAL 35 SECOND * 4 AS mul2,
  INTERVAL 10 YEAR / 3 AS div1,
  INTERVAL 1 MONTH / 12 AS div2

+----------------+--------------+-------------+--------------+
| mul1           | mul2         | div1        | div2         |
+----------------+--------------+-------------+--------------+
| 0-0 0 10:20:30 | 0-0 0 0:2:20 | 3-4 0 0:0:0 | 0-0 2 12:0:0 |
+----------------+--------------+-------------+--------------+
```

### Bitwise operators
All bitwise operators return the same type
 and the same length as
the first operand.

<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th style="white-space:nowrap">Input Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Bitwise not</td>
<td>~ X</td>
<td style="white-space:nowrap">Integer or BYTES</td>
<td>Performs logical negation on each bit, forming the ones' complement of the
given binary value.</td>
</tr>
<tr>
<td>Bitwise or</td>
<td>X | Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical inclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise xor</td>
<td style="white-space:nowrap">X ^ Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical exclusive OR
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Bitwise and</td>
<td style="white-space:nowrap">X &amp; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: Same type as X</td>
<td>Takes two bit patterns of equal length and performs the logical AND
operation on each pair of the corresponding bits.
This operator throws an error if X and Y are BYTES of different lengths.
</td>
</tr>
<tr>
<td>Left shift</td>
<td style="white-space:nowrap">X &lt;&lt; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: INT64</td>
<td>Shifts the first operand X to the left.
This operator returns
0 or a byte sequence of b'\x00'
if the second operand Y is greater than or equal to

the bit length of the first operand X (for example, 64 if X has the type INT64).

This operator throws an error if Y is negative.</td>
</tr>
<tr>
<td>Right shift</td>
<td style="white-space:nowrap">X &gt;&gt; Y</td>
<td style="white-space:nowrap">X: Integer or BYTES
<br>Y: INT64</td>
<td>Shifts the first operand X to the right. This operator does not do sign bit
extension with a signed type (i.e. it fills vacant bits on the left with 0).
This operator returns
0 or a byte sequence of b'\x00'
if the second operand Y is greater than or equal to

the bit length of the first operand X (for example, 64 if X has the type INT64).

This operator throws an error if Y is negative.</td>
</tr>
</tbody>
</table>

### Logical operators

ZetaSQL supports the `AND`, `OR`, and  `NOT` logical operators.
Logical operators allow only BOOL or `NULL` input
and use [three-valued logic][three-valued-logic]
to produce a result. The result can be `TRUE`, `FALSE`, or `NULL`:

| x       | y       | x AND y | x OR y |
| ------- | ------- | ------- | ------ |
| TRUE    | TRUE    | TRUE    | TRUE   |
| TRUE    | FALSE   | FALSE   | TRUE   |
| TRUE    | NULL    | NULL    | TRUE   |
| FALSE   | TRUE    | FALSE   | TRUE   |
| FALSE   | FALSE   | FALSE   | FALSE  |
| FALSE   | NULL    | FALSE   | NULL   |
| NULL    | TRUE    | NULL    | TRUE   |
| NULL    | FALSE   | FALSE   | NULL   |
| NULL    | NULL    | NULL    | NULL   |

| x       | NOT x   |
| ------- | ------- |
| TRUE    | FALSE   |
| FALSE   | TRUE    |
| NULL    | NULL    |

**Examples**

The examples in this section reference a table called `entry_table`:

```sql
+-------+
| entry |
+-------+
| a     |
| b     |
| c     |
| NULL  |
+-------+
```

```sql
SELECT 'a' FROM entry_table WHERE entry = 'a'

-- a => 'a' = 'a' => TRUE
-- b => 'b' = 'a' => FALSE
-- NULL => NULL = 'a' => NULL

+-------+
| entry |
+-------+
| a     |
+-------+
```

```sql
SELECT entry FROM entry_table WHERE NOT (entry = 'a')

-- a => NOT('a' = 'a') => NOT(TRUE) => FALSE
-- b => NOT('b' = 'a') => NOT(FALSE) => TRUE
-- NULL => NOT(NULL = 'a') => NOT(NULL) => NULL

+-------+
| entry |
+-------+
| b     |
| c     |
+-------+
```

```sql
SELECT entry FROM entry_table WHERE entry IS NULL

-- a => 'a' IS NULL => FALSE
-- b => 'b' IS NULL => FALSE
-- NULL => NULL IS NULL => TRUE

+-------+
| entry |
+-------+
| NULL  |
+-------+
```

### Comparison operators

Comparisons always return BOOL. Comparisons generally
require both operands to be of the same type. If operands are of different
types, and if ZetaSQL can convert the values of those types to a
common type without loss of precision, ZetaSQL will generally coerce
them to that common type for the comparison; ZetaSQL will generally
[coerce literals to the type of non-literals][link-to-coercion], where
present. Comparable data types are defined in
[Data Types][operators-link-to-data-types].

NOTE: ZetaSQL allows comparisons
between signed and unsigned integers.

STRUCTs support only 4 comparison operators: equal
(=), not equal (!= and <>), and IN.

The following rules apply when comparing these data types:

+  Floating point:
   All comparisons with NaN return FALSE,
   except for `!=` and `<>`, which return TRUE.
+  BOOL: FALSE is less than TRUE.
+  STRING: Strings are
   compared codepoint-by-codepoint, which means that canonically equivalent
   strings are only guaranteed to compare as equal if
   they have been normalized first.
+  `NULL`: The convention holds here: any operation with a `NULL` input returns
   `NULL`.

<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Less Than</td>
<td>X &lt; Y</td>
<td>
  Returns TRUE if X is less than Y.
  

</td>
</tr>
<tr>
<td>Less Than or Equal To</td>
<td>X &lt;= Y</td>
<td>
  Returns TRUE if X is less than or equal to Y.
  

</td>
</tr>
<tr>
<td>Greater Than</td>
<td>X &gt; Y</td>
<td>
  Returns TRUE if X is greater than Y.
  

</td>
</tr>
<tr>
<td>Greater Than or Equal To</td>
<td>X &gt;= Y</td>
<td>
  Returns TRUE if X is greater than or equal to Y.
  

</td>
</tr>
<tr>
<td>Equal</td>
<td>X = Y</td>
<td>
  Returns TRUE if X is equal to Y.
  

</td>
</tr>
<tr>
<td>Not Equal</td>
<td>X != Y<br>X &lt;&gt; Y</td>
<td>
  Returns TRUE if X is not equal to Y.
  

</td>
</tr>
<tr>
<td>BETWEEN</td>
<td>X [NOT] BETWEEN Y AND Z</td>
<td>
  <p>
    Returns TRUE if X is [not] within the range specified. The result of "X
    BETWEEN Y AND Z" is equivalent to "Y &lt;= X AND X &lt;= Z" but X is
    evaluated only once in the former.
    

  </p>
</td>
</tr>
<tr>
<td>LIKE</td>
<td>X [NOT] LIKE Y</td>
<td>Checks if the STRING in the first operand X
matches a pattern specified by the second operand Y. Expressions can contain
these characters:
<ul>
<li>A percent sign "%" matches any number of characters or bytes</li>
<li>An underscore "_" matches a single character or byte</li>
<li>You can escape "\", "_", or "%" using two backslashes. For example, <code>
"\\%"</code>. If you are using raw strings, only a single backslash is
required. For example, <code>r"\%"</code>.</li>
</ul>
</td>
</tr>
<tr>
<td>IN</td>
<td>Multiple - see below</td>
<td>
  Returns FALSE if the right operand is empty. Returns <code>NULL</code> if
  the left operand is <code>NULL</code>. Returns TRUE or <code>NULL</code>,
  never FALSE, if the right operand contains <code>NULL</code>. Arguments on
  either side of IN are general expressions. Neither operand is required to be
  a literal, although using a literal on the right is most common. X is
  evaluated only once.
  

</td>
</tr>
</tbody>
</table>

When testing values that have a STRUCT data type for
equality, it's possible that one or more fields are `NULL`. In such cases:

+ If all non-NULL field values are equal, the comparison returns NULL.
+ If any non-NULL field values are not equal, the comparison returns false.

The following table demonstrates how STRUCT data
types are compared when they have fields that are `NULL` valued.

<table>
<thead>
<tr>
<th>Struct1</th>
<th>Struct2</th>
<th>Struct1 = Struct2</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(2, NULL)</code></td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td><code>STRUCT(1,2)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
</tbody>
</table>

### IN operator 
<a id="in_operators"></a>

The `IN` operator supports the following syntax:

```sql
search_value [NOT] IN value_set

value_set:
  {
    (expression[, ...])
    | (subquery)
    | UNNEST(array_expression)
  }
```

**Description**

Checks for an equal value in a set of values.
[Semantic rules][semantic-rules-in] apply, but in general, `IN` returns `TRUE`
if an equal value is found, `FALSE` if an equal value is excluded, otherwise
`NULL`. `NOT IN` returns `FALSE` if an equal value is found, `TRUE` if an
equal value is excluded, otherwise `NULL`.

+ `search_value`: The expression that is compared to a set of values.
+ `value_set`: One or more values to compare to a search value.
   + `(expression[, ...])`: A list of expressions.
   + `(subquery)`: A [subquery][operators-subqueries] that returns
     a single column. The values in that column are the set of values.
     If no rows are produced, the set of values is empty.
   + `UNNEST(array_expression)`: An [UNNEST operator][operators-link-to-unnest]
      that returns a column of values from an array expression. This is
      equivalent to:

      ```sql
      IN (SELECT element FROM UNNEST(array_expression) AS element)
      ```

<a id="semantic_rules_in"></a>
**Semantic rules**

When using the `IN` operator, the following semantics apply in this order:

+ Returns `FALSE` if `value_set` is empty.
+ Returns `NULL` if `search_value` is `NULL`.
+ Returns `TRUE` if `value_set` contains a value equal to `search_value`.
+ Returns `NULL` if `value_set` contains a `NULL`.
+ Returns `FALSE`.

When using the `NOT IN` operator, the following semantics apply in this order:

+ Returns `TRUE` if `value_set` is empty.
+ Returns `NULL` if `search_value` is `NULL`.
+ Returns `FALSE` if `value_set` contains a value equal to `search_value`.
+ Returns `NULL` if `value_set` contains a `NULL`.
+ Returns `TRUE`.

This operator generally supports [collation][link-collation-concepts],
however, `x [NOT] IN UNNEST` is not supported.

[link-collation-concepts]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md

The semantics of:

```
x IN (y, z, ...)
```

are defined as equivalent to:

```
(x = y) OR (x = z) OR ...
```

and the subquery and array forms are defined similarly.

```
x NOT IN ...
```

is equivalent to:

```
NOT(x IN ...)
```

The `UNNEST` form treats an array scan like `UNNEST` in the
[FROM][operators-link-to-from-clause] clause:

```
x [NOT] IN UNNEST(<array expression>)
```

This form is often used with `ARRAY` parameters. For example:

```
x IN UNNEST(@array_parameter)
```

See the [Arrays][operators-link-to-filtering-arrays] topic for more information
on how to use this syntax.

`IN` can be used with multi-part keys by using the struct constructor syntax.
For example:

```
(Key1, Key2) IN ( (12,34), (56,78) )
(Key1, Key2) IN ( SELECT (table.a, table.b) FROM table )
```

See the [Struct Type][operators-link-to-struct-type] for more information.

**Return Data Type**

`BOOL`

**Examples**

You can use these `WITH` clauses to emulate temporary tables for
`Words` and `Items` in the following examples:

```sql
WITH Words AS (
  SELECT 'Intend' as value UNION ALL
  SELECT 'Secure' UNION ALL
  SELECT 'Clarity' UNION ALL
  SELECT 'Peace' UNION ALL
  SELECT 'Intend'
 )
SELECT * FROM Words;

+----------+
| value    |
+----------+
| Intend   |
| Secure   |
| Clarity  |
| Peace    |
| Intend   |
+----------+
```

```sql
WITH
  Items AS (
    SELECT STRUCT('blue' AS color, 'round' AS shape) AS info UNION ALL
    SELECT STRUCT('blue', 'square') UNION ALL
    SELECT STRUCT('red', 'round')
  )
SELECT * FROM Items;

+----------------------------+
| info                       |
+----------------------------+
| {blue color, round shape}  |
| {blue color, square shape} |
| {red color, round shape}   |
+----------------------------+
```

Example with `IN` and an expression:

```sql
SELECT * FROM Words WHERE value IN ('Intend', 'Secure');

+----------+
| value    |
+----------+
| Intend   |
| Secure   |
| Intend   |
+----------+
```

Example with `NOT IN` and an expression:

```sql
SELECT * FROM Words WHERE value NOT IN ('Intend');

+----------+
| value    |
+----------+
| Secure   |
| Clarity  |
| Peace    |
+----------+
```

Example with `IN`, a scalar subquery, and an expression:

```sql
SELECT * FROM Words WHERE value IN ((SELECT 'Intend'), 'Clarity');

+----------+
| value    |
+----------+
| Intend   |
| Clarity  |
| Intend   |
+----------+
```

Example with `IN` and an `UNNEST` operation:

```sql
SELECT * FROM Words WHERE value IN UNNEST(['Secure', 'Clarity']);

+----------+
| value    |
+----------+
| Secure   |
| Clarity  |
+----------+
```

Example with `IN` and a `STRUCT`:

```sql
SELECT
  (SELECT AS STRUCT Items.info) as item
FROM
  Items
WHERE (info.shape, info.color) IN (('round', 'blue'));

+------------------------------------+
| item                               |
+------------------------------------+
| { {blue color, round shape} info } |
+------------------------------------+
```

### IS operators

IS operators return TRUE or FALSE for the condition they are testing. They never
return `NULL`, even for `NULL` inputs, unlike the IS\_INF and IS\_NAN functions
defined in [Mathematical Functions][operators-link-to-math-functions]. If NOT is present,
the output BOOL value is inverted.

<table>
<thead>
<tr>
<th>Function Syntax</th>
<th>Input Data Type</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
  <td><pre>X IS [NOT] NULL</pre></td>
<td>Any value type</td>
<td>BOOL</td>
<td>Returns TRUE if the operand X evaluates to <code>NULL</code>, and returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] TRUE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to TRUE. Returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] FALSE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to FALSE. Returns FALSE
otherwise.</td>
</tr>
</tbody>
</table>

### IS DISTINCT FROM operator 
<a id="is_distinct"></a>

```sql
expression_1 IS [NOT] DISTINCT FROM expression_2
```

**Description**

`IS DISTINCT FROM` returns `TRUE` if the input values are considered to be
distinct from each other by the [`DISTINCT`][operators-distinct] and
[`GROUP BY`][operators-group-by] clauses. Otherwise, returns `FALSE`.

`a IS DISTINCT FROM b` being `TRUE` is equivalent to:

+ `SELECT COUNT(DISTINCT x) FROM UNNEST([a,b]) x` returning `2`.
+ `SELECT * FROM UNNEST([a,b]) x GROUP BY x` returning 2 rows.

`a IS DISTINCT FROM b` is equivalent to `NOT (a = b)`, except for the
following cases:

+ This operator never returns `NULL` so `NULL` values are considered to be
  distinct from non-`NULL` values, not other `NULL` values.
+ `NaN` values are considered to be distinct from non-`NaN` values, but not
  other `NaN` values.

**Input types**

+ `expression_1`: The first value to compare. This can be a groupable data type,
  `NULL` or `NaN`.
+ `expression_2`: The second value to compare. This can be a groupable
  data type, `NULL` or `NaN`.
+ `NOT`: If present, the output `BOOL` value is inverted.

**Return type**

`BOOL`

**Examples**

These return `TRUE`:

```sql
SELECT 1 IS DISTINCT FROM 2
```

```sql
SELECT 1 IS DISTINCT FROM NULL
```

```sql
SELECT 1 IS NOT DISTINCT FROM 1
```

```sql
SELECT NULL IS NOT DISTINCT FROM NULL
```

These return `FALSE`:

```sql
SELECT NULL IS DISTINCT FROM NULL
```

```sql
SELECT 1 IS DISTINCT FROM 1
```

```sql
SELECT 1 IS NOT DISTINCT FROM 2
```

```sql
SELECT 1 IS NOT DISTINCT FROM NULL
```

### Concatenation operator

The concatenation operator combines multiple values into one.

<table>
<thead>
<tr>
<th>Function Syntax</th>
<th>Input Data Type</th>
<th>Result Data Type</th>
</tr>
</thead>
<tbody>
<tr>
  <td><pre>STRING || STRING [ || ... ]</pre></td>
<td>STRING</td>
<td>STRING</td>
</tr>
<tr>
  <td><pre>BYTES || BYTES [ || ... ]</pre></td>
<td>BYTES</td>
<td>STRING</td>
</tr>
<tr>
  <td><pre>ARRAY&#60;T&#62; || ARRAY&#60;T&#62; [ || ... ]</pre></td>
<td>ARRAY&#60;T&#62;</td>
<td>ARRAY&#60;T&#62;</td>
</tr>
</tbody>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[three-valued-logic]: https://en.wikipedia.org/wiki/Three-valued_logic

[semantic-rules-in]: #semantic_rules_in

[array-subscript-operator]: #array_subscript_operator

[operators-link-to-filtering-arrays]: https://github.com/google/zetasql/blob/master/docs/arrays.md#filtering_arrays

[operators-link-to-data-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-from-clause]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#from_clause

[operators-link-to-unnest]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#unnest_operator

[operators-distinct]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#select_distinct

[operators-group-by]: https://github.com/google/zetasql/blob/master/docs/query-syntax.md#group_by_clause

[operators-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md#about_subqueries

[operators-link-to-struct-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#struct_type

[operators-link-to-math-functions]: #mathematical_functions

[link-to-coercion]: #coercion

[operators-link-to-array-safeoffset]: #safe-offset-and-safe-ordinal

[flatten-operation]: #flatten

<!-- mdlint on -->

## Conditional expressions

Conditional expressions impose constraints on the evaluation order of their
inputs. In essence, they are evaluated left to right, with short-circuiting, and
only evaluate the output value that was chosen. In contrast, all inputs to
regular functions are evaluated before calling the function. Short-circuiting in
conditional expressions can be exploited for error handling or performance
tuning.

### CASE expr

```sql
CASE expr
  WHEN expr_to_match THEN result
  [ ... ]
  [ ELSE else_result ]
END
```

**Description**

Compares `expr` to `expr_to_match` of each successive `WHEN` clause and returns
the first result where this comparison returns true. The remaining `WHEN`
clauses and `else_result` are not evaluated. If the `expr = expr_to_match`
comparison returns false or NULL for all `WHEN` clauses, returns `else_result`
if present; if not present, returns NULL.

`expr` and `expr_to_match` can be any type. They must be implicitly
coercible to a common [supertype][cond-exp-supertype]; equality comparisons are
done on coerced values. There may be multiple `result` types. `result` and
`else_result` expressions must be coercible to a common supertype.

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```sql
WITH Numbers AS
 (SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 8 UNION ALL
  SELECT 60, 6 UNION ALL
  SELECT 50, 10)
SELECT A, B,
  CASE A
    WHEN 90 THEN 'red'
    WHEN 50 THEN 'blue'
    ELSE 'green'
  END
  AS result
FROM Numbers

+------------------+
| A  | B  | result |
+------------------+
| 90 | 2  | red    |
| 50 | 8  | blue   |
| 60 | 6  | green  |
| 50 | 10 | blue   |
+------------------+
```

### CASE

```sql
CASE
  WHEN condition THEN result
  [ ... ]
  [ ELSE else_result ]
END
```

**Description**

Evaluates the condition of each successive `WHEN` clause and returns the
first result where the condition is true; any remaining `WHEN` clauses
and `else_result` are not evaluated. If all conditions are false or NULL,
returns `else_result` if present; if not present, returns NULL.

`condition` must be a boolean expression. There may be multiple `result` types.
`result` and `else_result` expressions must be implicitly coercible to a
common [supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `result`[, ...] and `else_result`.

**Example**

```sql
WITH Numbers AS
 (SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 6 UNION ALL
  SELECT 20, 10)
SELECT A, B,
  CASE
    WHEN A > 60 THEN 'red'
    WHEN A > 30 THEN 'blue'
    ELSE 'green'
  END
  AS result
FROM Numbers

+------------------+
| A  | B  | result |
+------------------+
| 90 | 2  | red    |
| 50 | 6  | blue   |
| 20 | 10 | green  |
+------------------+
```

### COALESCE

```sql
COALESCE(expr[, ...])
```

**Description**

Returns the value of the first non-null expression. The remaining
expressions are not evaluated. An input expression can be any type.
There may be multiple input expression types.
All input expressions must be implicitly coercible to a common
[supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr`[, ...].

**Examples**

```sql
SELECT COALESCE('A', 'B', 'C') as result

+--------+
| result |
+--------+
| A      |
+--------+
```

```sql
SELECT COALESCE(NULL, 'B', 'C') as result

+--------+
| result |
+--------+
| B      |
+--------+
```

### IF

```sql
IF(expr, true_result, else_result)
```

**Description**

If `expr` is true, returns `true_result`, else returns `else_result`.
`else_result` is not evaluated if `expr` is true. `true_result` is not
evaluated if `expr` is false or NULL.

`expr` must be a boolean expression. `true_result` and `else_result`
must be coercible to a common [supertype][cond-exp-supertype].

**Return Data Type**

[Supertype][cond-exp-supertype] of `true_result` and `else_result`.

**Example**

```sql
WITH Numbers AS
 (SELECT 10 as A, 20 as B UNION ALL
  SELECT 50, 30 UNION ALL
  SELECT 60, 60)
SELECT
  A, B,
  IF( A<B, 'true', 'false') as result
FROM Numbers

+------------------+
| A  | B  | result |
+------------------+
| 10 | 20 | true   |
| 50 | 30 | false  |
| 60 | 60 | false  |
+------------------+
```

### IFNULL

```sql
IFNULL(expr, null_result)
```

**Description**

If `expr` is NULL, return `null_result`. Otherwise, return `expr`. If `expr`
is not NULL, `null_result` is not evaluated.

`expr` and `null_result` can be any type and must be implicitly coercible to
a common [supertype][cond-exp-supertype]. Synonym for
`COALESCE(expr, null_result)`.

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` or `null_result`.

**Examples**

```sql
SELECT IFNULL(NULL, 0) as result

+--------+
| result |
+--------+
| 0      |
+--------+
```

```sql
SELECT IFNULL(10, 0) as result

+--------+
| result |
+--------+
| 10     |
+--------+
```

### NULLIF

```sql
NULLIF(expr, expr_to_match)
```

**Description**

Returns NULL if `expr = expr_to_match` is true, otherwise
returns `expr`.

`expr` and `expr_to_match` must be implicitly coercible to a
common [supertype][cond-exp-supertype], and must be comparable.

**Return Data Type**

[Supertype][cond-exp-supertype] of `expr` and `expr_to_match`.

**Example**

```sql
SELECT NULLIF(0, 0) as result

+--------+
| result |
+--------+
| NULL   |
+--------+
```

```sql
SELECT NULLIF(10, 0) as result

+--------+
| result |
+--------+
| 10     |
+--------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[cond-exp-supertype]: #supertypes

<!-- mdlint on -->

## Expression subqueries

There are four types of expression subqueries, i.e. subqueries that are used as
expressions.  Expression subqueries return `NULL` or a single value, as opposed to
a column or table, and must be surrounded by parentheses. For a fuller
discussion of subqueries, see
[Subqueries][exp-sub-link-to-subqueries].

<table>
<thead>
<tr>
<th>Type of Subquery</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Scalar</td>
<td>Any type T</td>
<td>A subquery in parentheses inside an expression (e.g. in the
<code>SELECT</code> list or <code>WHERE</code> clause) is interpreted as a
scalar subquery. The <code>SELECT</code> list in a scalar subquery must have
exactly one field. If the subquery returns exactly one row, that single value is
the scalar subquery result. If the subquery returns zero rows, the scalar
subquery value is <code>NULL</code>. If the subquery returns more than one row, the query
fails with a runtime error. When the subquery is written with <code>SELECT AS
STRUCT</code>  or <code>SELECT AS ProtoName</code>, it can include multiple
columns, and the returned value is the constructed STRUCT or PROTO. Selecting
multiple columns without using <code>SELECT AS</code> is an error.</td>
</tr>
<tr>
<td>ARRAY</td>
<td>ARRAY</td>
<td>Can use <code>SELECT AS STRUCT</code> or <code>SELECT AS ProtoName</code> to
build arrays of structs or PROTOs, and conversely, selecting multiple columns
without using <code>SELECT AS</code> is an error. Returns an empty ARRAY if the
subquery returns zero rows. Never returns a <code>NULL</code> ARRAY.</td>
</tr>

<tr>
<td>IN</td>
<td>BOOL</td>
<td>Occurs in an expression following the IN operator. The subquery must produce
a single column whose type is equality-compatible with the expression on the
left side of the IN operator. Returns FALSE if the subquery returns zero rows.
<code>x IN ()</code> is equivalent to <code>x IN (value, value, ...)</code>
See the <code>IN</code> operator in
<a href="#comparison_operators">Comparison Operators</a>

for full semantics.</td>
</tr>

<tr>
<td>EXISTS</td>
<td>BOOL</td>
<td>Returns TRUE if the subquery produced one or more rows. Returns FALSE if the
subquery produces zero rows. Never returns <code>NULL</code>. Unlike all other expression
subqueries, there are no rules about the column list. Any number of columns may
be selected and it will not affect the query result.</td>

</tr>
</tbody>
</table>

**Examples**

The following examples of expression subqueries assume that `t.int_array` has
type `ARRAY<INT64>`.

<table>
<thead>
<tr>
<th>Type</th>
<th>Subquery</th>
<th>Result Data Type</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td rowspan="8" style="vertical-align:top">Scalar</td>
<td><code>(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>INT64</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT DISTINCT i FROM t.int_array i)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT i FROM t.int_array i WHERE i=5)</code></td>
<td>INT64, possibly runtime error</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT ARRAY_AGG(i) FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Uses the ARRAY_AGG aggregation function to return an ARRAY.</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a)</code></td>
<td>STRING</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT 'xxx' a, 123 b)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a, 123 b)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>(SELECT AS STRUCT 'xxx' a)</code></td>
<td>STRUCT</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="7" style="vertical-align:top">ARRAY</td>
<td><code>ARRAY(SELECT COUNT(*) FROM t.int_array)</code></td>
<td>ARRAY of size 1</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT x FROM t)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>Error</td>
<td>Returns an error because there is more than one column</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 5 a, COUNT(*) b FROM t.int_array)</code></td>
<td>ARRAY</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT i FROM t.int_array i)</code></td>
<td>ARRAY</td>
<td>Makes an ARRAY of one-field STRUCTs</td>
</tr>
<tr>
<td><code>ARRAY(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>ARRAY</td>
<td>Returns an ARRAY of STRUCTs with anonymous or duplicate fields.</td>
</tr>
<tr>
<td><code>ARRAY(SELECT  AS TypeName SUM(x) a, SUM(y) b, SUM(z) c from t)</code></td>
<td>array&lt;TypeName></td>
<td>Selecting into a named type. Assume TypeName is a STRUCT type with fields
a,b,c.</td>
</tr>
<tr>
<td style="vertical-align:top">STRUCT</td>
<td><code>(SELECT AS STRUCT 1 x, 2, 3 x)</code></td>
<td>STRUCT</td>
<td>Constructs a STRUCT with anonymous or duplicate fields.</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">EXISTS</td>
<td><code>EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>NOT EXISTS(SELECT x,y,z FROM table WHERE y=z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td rowspan="2" style="vertical-align:top">IN</td>
<td><code>x IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
<tr>
<td><code>x NOT IN (SELECT y FROM table WHERE z)</code></td>
<td>BOOL</td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[exp-sub-link-to-subqueries]: https://github.com/google/zetasql/blob/master/docs/subqueries.md

<!-- mdlint on -->

## Debugging functions

ZetaSQL supports the following debugging functions.

### ERROR
```
ERROR(error_message)
```

**Description**

Returns an error. The `error_message` argument is a `STRING`.

ZetaSQL treats `ERROR` in the same way as any expression that may
result in an error: there is no special guarantee of evaluation order.

**Return Data Type**

ZetaSQL infers the return type in context.

**Examples**

In the following example, the query returns an error message if the value of the
row does not match one of two defined values.

```sql
SELECT
  CASE
    WHEN value = 'foo' THEN 'Value is foo.'
    WHEN value = 'bar' THEN 'Value is bar.'
    ELSE ERROR(concat('Found unexpected value: ', value))
  END AS new_value
FROM (
  SELECT 'foo' AS value UNION ALL
  SELECT 'bar' AS value UNION ALL
  SELECT 'baz' AS value);

Found unexpected value: baz
```

In the following example, ZetaSQL may evaluate the `ERROR` function
before or after the <nobr>`x > 0`</nobr> condition, because ZetaSQL
generally provides no ordering guarantees between `WHERE` clause conditions and
there are no special guarantees for the `ERROR` function.

```sql
SELECT *
FROM (SELECT -1 AS x)
WHERE x > 0 AND ERROR('Example error');
```

In the next example, the `WHERE` clause evaluates an `IF` condition, which
ensures that ZetaSQL only evaluates the `ERROR` function if the
condition fails.

```sql
SELECT *
FROM (SELECT -1 AS x)
WHERE IF(x > 0, true, ERROR(FORMAT('Error: x must be positive but is %t', x)));'

Error: x must be positive but is -1
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

<!-- mdlint on -->

