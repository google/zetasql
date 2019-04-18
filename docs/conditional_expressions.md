

# Conditional Expressions

Conditional expressions impose constraints on the evaluation order of their
inputs. In essence, they are evaluated left to right, with short-circuiting, and
only evaluate the output value that was chosen. In contrast, all inputs to
regular functions are evaluated before calling the function. Short-circuiting in
conditional expressions can be exploited for error handling or performance
tuning.

<table>
<thead>
<tr>
<th>Syntax</th>
<th>Input Data Types</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
  <td><pre>CASE expr
  WHEN value THEN result
  [WHEN ...]
  [ELSE else_result]
  END</pre></td>
<td><code>expr</code> and <code>value</code>: Any type</td>
<td><code>result</code> and <code>else_result</code>: Supertype of input
types.</td>
<td>Compares <code>expr</code> to value of each successive <code>WHEN</code>
clause and returns the first result where this comparison returns true. The
remaining <code>WHEN</code> clauses and <code>else_result</code> are not
evaluated. If the
<code>expr = value</code> comparison returns false or <code>NULL</code> for
all <code>WHEN</code> clauses, returns
<code>else_result</code> if present; if not present, returns <code>NULL</code>.
<code>expr</code> and <code>value</code> expressions
must be implicitly coercible to a common supertype; equality comparisons are
done on coerced values. <code>result</code> and <code>else_result</code>
expressions must be coercible to a common supertype.</td>
</tr>

<tr>
  <td><pre>CASE
  WHEN cond1 THEN result
  [WHEN cond2...]
  [ELSE else_result]
  END</pre></td>
<td><code>cond</code>: BOOL</td>
<td><code>result</code> and <code>else_result</code>: Supertype of input
types.</td>
<td>Evaluates condition <code>cond</code> of each successive <code>WHEN</code>
clause and returns the first result where the condition is true; any remaining
<code>WHEN</code> clauses and <code>else_result</code> are not evaluated. If all
conditions are false or <code>NULL</code>, returns
<code>else_result</code> if present; if not present, returns
<code>NULL</code>. <code>result</code> and <code>else_result</code>
expressions must be implicitly coercible to a common supertype. </td>
</tr>

<tr>
<td><a id="coalesce"></a>COALESCE(expr1, ..., exprN)</td>
<td>Any type</td>
<td>Supertype of input types</td>
<td>Returns the value of the first non-null expression. The remaining
expressions are not evaluated. All input expressions must be implicitly
coercible to a common supertype.</td>
</tr>
<tr>
<td><a id="if"></a>IF(cond, true_result, else_result)</td>
<td><code>cond</code>: BOOL</td>
<td><code>true_result</code> and <code>else_result</code>: Any type.</td>
<td>If <code>cond</code> is true, returns <code>true_result</code>, else returns
<code>else_result</code>. <code>else_result</code> is not evaluated if
<code>cond</code> is true. <code>true_result</code> is not evaluated if
<code>cond</code> is false or <code>NULL</code>. <code>true_result</code> and
<code>else_result</code> must be coercible to a common supertype.</td>
</tr>
<tr>
<td><a id="ifnull"></a>IFNULL(expr, null_result)</td>
<td>Any type</td>
<td>Any type or supertype of input types.</td>
<td>If <code>expr</code> is <code>NULL</code>, return <code>null_result</code>. Otherwise,
return <code>expr</code>. If <code>expr</code> is not <code>NULL</code>,
<code>null_result</code> is not evaluated. <code>expr</code> and
<code>null_result</code> must be implicitly coercible to a common
supertype. Synonym for <code>COALESCE(expr, null_result)</code>.</td>
</tr>
<tr>
<td><a id="nullif"></a>NULLIF(expression, expression_to_match)</td>
<td>Any type T or subtype of T</td>
<td>Any type T or subtype of T</td>
<td>Returns <code>NULL</code> if <code>expression = expression_to_match</code>
is true, otherwise returns <code>expression</code>. <code>expression</code> and
<code>expression_to_match</code> must be implicitly coercible to a common
supertype; equality comparison is done on coerced values.</td>
</tr>
</tbody>
</table>

