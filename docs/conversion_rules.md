

# Conversion rules

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

[con-rules-link-to-conversion-functions]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md

[con-rules-link-to-cast]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#cast

[con-rules-link-to-conversion-functions-other]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#other_conv_functions

<!-- mdlint on -->

