

# Mathematical functions

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
largest negative input value, which has no positive representation. Returns
`+inf` for a `+/-inf` argument.

### SIGN

```
SIGN(X)
```

**Description**

Returns -1, 0, or +1 for negative, zero and positive arguments respectively.
For floating point arguments, this function does not distinguish between
positive and negative zero. Returns `NaN` for a `NaN` argument.

### IS_INF

```
IS_INF(X)
```

**Description**

Returns `TRUE` if the value is positive or negative infinity. Returns
`NULL` for `NULL` inputs.

### IS_NAN

```
IS_NAN(X)
```

**Description**

Returns `TRUE` if the value is a `NaN` value. Returns `NULL` for `NULL` inputs.

### IEEE_DIVIDE

```
IEEE_DIVIDE(X, Y)
```

**Description**

Divides X by Y; this function never fails. Returns
`DOUBLE` unless
both X and Y are `FLOAT`, in which case it returns
FLOAT. Unlike the division operator (/),
this function does not generate errors for division by zero or overflow.</p>

Special cases:

+ If the result overflows, returns `+/-inf`.
+ If Y=0 and X=0, returns `NaN`.
+ If Y=0 and X!=0, returns `+/-inf`.
+ If X = `+/-inf` and Y = `+/-inf`, returns `NaN`.

The behavior of `IEEE_DIVIDE` is further illustrated in the table below.

#### Special cases for `IEEE_DIVIDE`

The following table lists special cases for `IEEE_DIVIDE`.

<table>
<thead>
<tr>
<th>Numerator Data Type (X)</th>
<th>Denominator Data Type (Y)</th>
<th>Result Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>Anything except 0</td>
<td>0</td>
<td><code>+/-inf</code></td>
</tr>
<tr>
<td>0</td>
<td>0</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td>0</td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td>0</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

### RAND

```
RAND()
```

**Description**

Generates a pseudo-random value of type DOUBLE in the
range of [0, 1), inclusive of 0 and exclusive of 1.

### SQRT

```
SQRT(X)
```

**Description**

Computes the square root of X. Generates an error if X is less than 0. Returns
`+inf` if X is `+inf`.

### POW

```
POW(X, Y)
```

**Description**

Returns the value of X raised to the power of Y. If the result underflows and is
not representable, then the function returns a  value of zero. Returns an error
if one of the following is true:

+ X is a finite value less than 0 and Y is a noninteger
+ X is 0 and Y is a finite value less than 0

The behavior of `POW()` is further illustrated in the table below.

### POWER

```
POWER(X, Y)
```

**Description**

Synonym of `POW()`.

#### Special cases for `POW(X, Y)` and `POWER(X, Y)`

The following are special cases for `POW(X, Y)` and `POWER(X, Y)`.

<table>
<thead>
<tr>
<th>X</th>
<th>Y</th>
<th>POW(X, Y) or POWER(X, Y)</th>
</tr>
</thead>
<tbody>
<tr>
<td>1.0</td>
<td>Any value including <code>NaN</code></td>
<td>1.0</td>
</tr>
<tr>
<td>any including <code>NaN</code></td>
<td>0</td>
<td>1.0</td>
</tr>
<tr>
<td>-1.0</td>
<td><code>+/-inf</code></td>
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
<td>0</td>
</tr>
<tr>
<td>ABS(X) &lt; 1</td>
<td><code>+inf</code></td>
<td>0</td>
</tr>
<tr>
<td>ABS(X) &gt; 1</td>
<td><code>+inf</code></td>
<td><code>+inf</code></td>
</tr>
<tr>
<td><code>-inf</code></td>
<td>Y &lt; 0</td>
<td>0</td>
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
</tbody>
</table>

### EXP

```
EXP(X)
```

**Description**

Computes *e* to the power of X, also called the natural exponential function. If
the result underflows, this function returns a zero. Generates an error if the
result overflows. If X is `+/-inf`, then this function returns `+inf` or 0.

### LN

```
LN(X)
```

**Description**

Computes the natural logarithm of X. Generates an error if X is less than or
equal to zero. If X is `+inf`, then this function returns `+inf`.

### LOG

```
LOG(X [, Y])
```

**Description**

If only X is present, `LOG` is a synonym of `LN`. If Y is also present, `LOG`
computes the logarithm of X to base Y. Generates an error in these cases:

+ X is less than or equal to zero
+ Y is 1.0
+ Y is less than or equal to zero.

The behavior of `LOG(X, Y)` is further illustrated in the table below.

<a name="special_log"></a>

#### Special cases for `LOG(X, Y)`

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
<td>0.0 Y &lt; 1.0</td>
<td><code>-inf</code></td>
</tr>
<tr>
<td><code>+inf</code></td>
<td>Y &gt; 1.0</td>
<td><code>+inf</code></td>
</tr>
</tbody>
</table>

### LOG10

```
LOG10(X)
```

**Description**

Similar to `LOG`, but computes logarithm to base 10.

### GREATEST

```
GREATEST(X1,...,XN)
```

**Description**

Returns <code>NULL</code> if any of the inputs is <code>NULL</code>. Otherwise, returns <code>NaN</code> if any of
the inputs is <code>NaN</code>. Otherwise, returns the largest value among X1,...,XN
according to the &lt; comparison.

### LEAST

```
LEAST(X1,...,XN)
```

**Description**

Returns `NULL` if any of the inputs is `NULL`. Returns `NaN` if any of the
inputs is `NaN`. Otherwise, returns the smallest value among X1,...,XN
according to the &gt; comparison.

### DIV

```
DIV(X, Y)
```

**Description**

Returns the result of integer division of X by Y. Division by zero returns
an error. Division by -1 may overflow. See the table below for possible result
types.

### SAFE_DIVIDE

```
SAFE_DIVIDE(X, Y)
```

**Description**

Equivalent to the division operator (<code>/</code>), but returns
<code>NULL</code> if an error occurs, such as a division by zero error.

### MOD

```
MOD(X, Y)
```

**Description**

Modulo function: returns the remainder of the division of X by Y. Returned
value has the same sign as X. An error is generated if Y is 0. See the table
below for possible result types.

<a name="result_div_mod"></a>

#### Result types for `DIV(X, Y)` and `MOD(X, Y)`

<table>
<thead>
<tr><th>&nbsp;</th><th>INT32</th><th>INT64</th><th>UINT32</th><th>UINT64</th></tr>
</thead>
<tbody><tr><td>INT32</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td></tr><tr><td>INT64</td><td>INT64</td><td>INT64</td><td>INT64</td><td>ERROR</td></tr><tr><td>UINT32</td><td>INT64</td><td>INT64</td><td>UINT64</td><td>UINT64</td></tr><tr><td>UINT64</td><td>ERROR</td><td>ERROR</td><td>UINT64</td><td>UINT64</td></tr></tbody>
</table>

<a name="rounding_functions"></a>

### ROUND

```
ROUND(X [, N])
```

**Description**

If only X is present, `ROUND` rounds X to the nearest integer. If N is present,
`ROUND` rounds X to N decimal places after the decimal point. If N is negative,
`ROUND` will round off digits to the left of the decimal point. Rounds halfway
cases away from zero. Generates an error if overflow occurs.

### TRUNC

```
TRUNC(X [, N])
```

**Description**

If only X is present, `TRUNC` rounds X to the nearest integer whose absolute
value is not greater than the absolute value of X. If N is also present, `TRUNC`
behaves like `ROUND(X, N)`, but always rounds towards zero and never overflows.

### CEIL

```
CEIL(X)
```

**Description**

Returns the smallest integral value (with DOUBLE
type) that is not less than X.

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

Returns the largest integral value (with DOUBLE
type) that is not greater than X.

#### Example rounding function behavior
Example behavior of ZetaSQL rounding functions:

<table>
<thead>
<tr>
<th>Input "X"</th>
<th>ROUND(X)</th>
<th>TRUNC(X)</th>
<th>CEIL(X)</th>
<th>FLOOR(X)</th>
</tr>
</thead>
<tbody>
<tr>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.3</td>
<td>2.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.8</td>
<td>3.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.5</td>
<td>3.0</td>
<td>2.0</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>-2.3</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.8</td>
<td>-3.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.5</td>
<td>-3.0</td>
<td>-2.0</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>0</td>
<td>0</td>
<td>0</td>
<td>0</td>
<td>0</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
<td><code>+/-inf</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

<a name="trigonometric_and_hyperbolic_functions"></a>

### COS

```
COS(X)
```

**Description**

Computes cosine of X. Never fails.

### COSH

```
COSH(X)
```

**Description**

Computes the hyperbolic cosine of X. Generates an error if an overflow
occurs.

### ACOS

```
ACOS(X)
```

**Description**

Computes the principal value of the arc cosine of X. The return value is in
the range [0,]. Generates an error if X is a finite value outside of range
[-1, 1].

### ACOSH

```
ACOSH(X)
```

**Description**

Computes the inverse hyperbolic cosine of X. Generates an error if X is a
finite value less than 1.

### SIN

```
SIN(X)
```

**Description**

Computes the sine of X. Never fails.

### SINH

```
SINH(X)
```

**Description**

Computes the hyperbolic sine of X. Generates an error if an overflow
occurs.

### ASIN

```
ASIN(X)
```

**Description**

Computes the principal value of the arc sine of X. The return value is in
the range [-&pi;/2,&pi;/2]. Generates an error if X is a finite value outside of range
[-1, 1].

### ASINH

```
ASINH(X)
```

**Description**

Computes the inverse hyperbolic sine of X. Does not fail.

### TAN

```
TAN(X)
```

**Description**

Computes tangent of X. Generates an error if an overflow occurs.

### TANH

```
TANH(X)
```

**Description**

Computes hyperbolic tangent of X. Does not fail.

### ATAN

```
ATAN(X)
```

**Description**

Computes the principal value of the arc tangent of X. The return value is in
the range [-&pi;/2,&pi;/2]. Does not fail.

### ATANH

```
ATANH(X)
```

**Description**

Computes the inverse hyperbolic tangent of X. Generates an error if the
absolute value of X is greater or equal 1.

### ATAN2

```
ATAN2(Y, X)
```

**Description**

Calculates the principal value of the arc tangent of Y/X using the signs of
the two arguments to determine the quadrant. The return value is in the range
[-&pi;,&pi;]. The behavior of this function is further illustrated in <a
href="#special_atan2">the table below</a>.

<a name="special_atan2"></a>
#### Special cases for `ATAN2()`

<table>
<thead>
<tr>
<th>Y</th>
<th>X</th>
<th>ATAN2(Y, X)</th>
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
<td>0</td>
<td>0</td>
<td>0, &pi; or -&pi; depending on the sign of X and Y</td>
</tr>
<tr>
<td>Finite value</td>
<td><code>-inf</code></td>
<td>&pi; or -&pi; depending on the sign of Y</td>
</tr>
<tr>
<td>Finite value</td>
<td><code>+inf</code></td>
<td>0</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td>Finite value</td>
<td>&pi;/2 or &pi;/2 depending on the sign of Y</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>-inf</code></td>
<td>&frac34;&pi; or -&frac34;&pi; depending on the sign of Y</td>
</tr>
<tr>
<td><code>+/-inf</code></td>
<td><code>+inf</code></td>
<td>&pi;/4 or -&pi;/4 depending on the sign of Y</td>
</tr>
</tbody>
</table>

<a name="special_trig_hyperbolic"></a>
#### Special cases for trigonometric and hyperbolic rounding functions

<table>
<thead>
<tr>
<th>X</th>
<th>COS(X)</th>
<th>COSH(X)</th>
<th>ACOS(X)</th>
<th>ACOSH(X)</th>
<th>SIN(X)</th>
<th>SINH(X)</th>
<th>ASIN(X)</th>
<th>ASINH(X)</th>
<th>TAN(X)</th>
<th>TANH(X)</th>
<th>ATAN(X)</th>
<th>ATANH(X)</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>+/-inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td>=+1.0</td>
<td>&pi;/2</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td><code>=+inf</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td><code>-inf</code></td>
<td><code>NaN</code></td>
<td>-1.0</td>
<td>-&pi;/2</td>
<td><code>NaN</code></td>
</tr>
<tr>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
<td><code>NaN</code></td>
</tr>
</tbody>
</table>

