

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# JSON functions

ZetaSQL supports the following functions, which can retrieve and
transform JSON data.

### Categories

The JSON functions are grouped into the following categories based on their
behavior:

<table>
  <thead>
    <tr>
      <td>Category</td>
      <td width='300px'>Functions</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    
    <tr>
      <td><a id="extractors"></a>
        
        Standard extractors
        
      </td>
      <td>
        
        <a href="#json_query"><code>JSON_QUERY</code></a><br>
        <a href="#json_value"><code>JSON_VALUE</code></a><br>
        
        
        <a href="#json_query_array"><code>JSON_QUERY_ARRAY</code></a><br>
        
        
        <a href="#json_value_array"><code>JSON_VALUE_ARRAY</code></a><br>
        
      </td>
      <td>
        Functions that extract JSON data.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="extractors_legacy"></a>
        
        Legacy extractors
        
      </td>
      <td>
        
        <a href="#json_extract"><code>JSON_EXTRACT</code></a><br>
        <a href="#json_extract_scalar"><code>JSON_EXTRACT_SCALAR</code></a><br>
        
        
        <a href="#json_extract_array"><code>JSON_EXTRACT_ARRAY</code></a><br>
        
        
        <a href="#json_extract_string_array"><code>JSON_EXTRACT_STRING_ARRAY</code></a><br>
        
      </td>
      <td>
        Functions that extract JSON data.<br>
        
        While these functions are supported by ZetaSQL, we recommend
        using the <a href="#extractors">standard extractor functions</a>.
        
      </td>
    </tr>
    
    
    <tr>
      <td><a id="lax_converters"></a>Lax converters</td>
      <td>
        
        <a href="#lax_bool"><code>LAX_BOOL</code></a><br>
        
        
        <a href="#lax_bool_array"><code>LAX_BOOL_ARRAY</code></a><br>
        
        
        <a href="#lax_double"><code>LAX_DOUBLE</code></a><br>
        
        
        <a href="#lax_double_array"><code>LAX_DOUBLE_ARRAY</code></a><br>
        
        
        <a href="#lax_float"><code>LAX_FLOAT</code></a><br>
        
        
        <a href="#lax_float_array"><code>LAX_FLOAT_ARRAY</code></a><br>
        
        
        <a href="#lax_int32"><code>LAX_INT32</code></a><br>
        
        
        <a href="#lax_int32_array"><code>LAX_INT32_ARRAY</code></a><br>
        
        
        <a href="#lax_int64"><code>LAX_INT64</code></a><br>
        
        
        <a href="#lax_int64_array"><code>LAX_INT64_ARRAY</code></a><br>
        
        
        <a href="#lax_string"><code>LAX_STRING</code></a><br>
        
        
        <a href="#lax_string_array"><code>LAX_STRING_ARRAY</code></a><br>
        
        
        <a href="#lax_uint32"><code>LAX_UINT32</code></a><br>
        
        
        <a href="#lax_uint32_array"><code>LAX_UINT32_ARRAY</code></a><br>
        
        
        <a href="#lax_uint64"><code>LAX_UINT64</code></a><br>
        
        
        <a href="#lax_uint64_array"><code>LAX_UINT64_ARRAY</code></a><br>
        
      </td>
      <td>
        Functions that flexibly convert a JSON value to an SQL value
        without returning errors.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="converters"></a>Converters</td>
      <td>
        
        <a href="#bool_for_json"><code>BOOL</code></a><br>
        
        
        <a href="#bool_array_for_json"><code>BOOL_ARRAY</code></a><br>
        
        
        <a href="#double_for_json"><code>DOUBLE</code></a><br>
        
        
        <a href="#double_array_for_json"><code>DOUBLE_ARRAY</code></a><br>
        
        
        <a href="#float_for_json"><code>FLOAT</code></a><br>
        
        
        <a href="#float_array_for_json"><code>FLOAT_ARRAY</code></a><br>
        
        
        <a href="#int32_for_json"><code>INT32</code></a><br>
        
        
        <a href="#int32_array_for_json"><code>INT32_ARRAY</code></a><br>
        
        
        <a href="#int64_for_json"><code>INT64</code></a><br>
        
        
        <a href="#int64_array_for_json"><code>INT64_ARRAY</code></a><br>
        
        
        <a href="#string_for_json"><code>STRING</code></a><br>
        
        
        <a href="#string_array_for_json"><code>STRING_ARRAY</code></a><br>
        
        
        <a href="#uint32_for_json"><code>UINT32</code></a><br>
        
        
        <a href="#uint32_array_for_json"><code>UINT32_ARRAY</code></a><br>
        
        
        <a href="#uint64_for_json"><code>UINT64</code></a><br>
        
        
        <a href="#uint64_array_for_json"><code>UINT64_ARRAY</code></a><br>
        
      </td>
      <td>
        Functions that convert a JSON value to a SQL value.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="other_converters"></a>Other converters</td>
      <td>
        
        <a href="#parse_json"><code>PARSE_JSON</code></a><br>
        
        
        <a href="#to_json"><code>TO_JSON</code></a><br>
        
        
        <a href="#to_json_string"><code>TO_JSON_STRING</code></a><br>
        
      </td>
      <td>
        Other conversion functions from or to JSON.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="constructors"></a>Constructors</td>
      <td>
        <a href="#json_array"><code>JSON_ARRAY</code></a><br>
        <a href="#json_object"><code>JSON_OBJECT</code></a><br>
      </td>
      <td>
        Functions that create JSON.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="mutators"></a>Mutators</td>
      <td>
        
        <a href="#json_array_append"><code>JSON_ARRAY_APPEND</code></a><br>
        
        
        <a href="#json_array_insert"><code>JSON_ARRAY_INSERT</code></a><br>
        
        
        <a href="#json_remove"><code>JSON_REMOVE</code></a><br>
        
        
        <a href="#json_set"><code>JSON_SET</code></a><br>
        
        
        <a href="#json_strip_nulls"><code>JSON_STRIP_NULLS</code></a><br>
        
      </td>
      <td>
        Functions that mutate existing JSON.
      </td>
    </tr>
    
    
    <tr>
      <td><a id="accessors"></a>Accessors</td>
      <td>
        
        
        <a href="#json_type"><code>JSON_TYPE</code></a><br>
        
      </td>
      <td>
        Functions that provide access to JSON properties.
      </td>
    </tr>
    
  </tbody>
</table>

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#bool_for_json"><code>BOOL</code></a>

</td>
  <td>
    Converts a JSON boolean to a SQL <code>BOOL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#bool_array_for_json"><code>BOOL</code></a>

</td>
  <td> Converts a JSON array of booleans to a SQL <code>ARRAY&lt;BOOL&gt;</code> value.</td>
</tr>

<tr>
  <td>
  
  <a href="#double_for_json"><code>DOUBLE</code></a>

  
  </td>
  <td>
    Converts a JSON number to a SQL
    <code>DOUBLE</code> value.
  </td>
</tr>

<tr>
  <td>
    
      <a href="#double_array_for_json"><code>DOUBLE_ARRAY</code></a>

    
  </td>
  <td> Converts a JSON array of numbers to a SQL <code>ARRAY&lt;DOUBLE&gt;</code> value.</td>
</tr>

<tr>
  <td>
    
      <a href="#float_for_json"><code>FLOAT</code></a>

    
  </td>
  <td> Converts a JSON number to a SQL <code>FLOAT</code> value.</td>
</tr>

<tr>
  <td>
    
      <a href="#float_array_for_json"><code>FLOAT_ARRAY</code></a>

    
  </td>
  <td>Converts a JSON array of numbers to a SQL <code>ARRAY&lt;FLOAT&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#int32_for_json"><code>INT32</code></a>

</td>
  <td>
    Converts a JSON number to a SQL <code>INT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#int32_array_for_json"><code>INT32_ARRAY</code></a>

</td>
  <td>Converts a JSON number to a SQL <code>ARRAY&lt;INT32&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#int64_for_json"><code>INT64</code></a>

</td>
  <td>
    Converts a JSON number to a SQL <code>INT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#int64_array_for_json"><code>INT64_ARRAY</code></a>

</td>
  <td>Converts a JSON array of numbers to a SQL <code>ARRAY&lt;INT64&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#json_array"><code>JSON_ARRAY</code></a>

</td>
  <td>Creates a JSON array.</td>
</tr>

<tr>
  <td><a href="#json_array_append"><code>JSON_ARRAY_APPEND</code></a>

</td>
  <td>Appends JSON data to the end of a JSON array.</td>
</tr>

<tr>
  <td><a href="#json_array_insert"><code>JSON_ARRAY_INSERT</code></a>

</td>
  <td>Inserts JSON data into a JSON array.</td>
</tr>

<tr>
  <td><a href="#json_extract"><code>JSON_EXTRACT</code></a>

</td>
  <td>
    (Deprecated)
    Extracts a JSON value and converts it to a SQL
    JSON-formatted <code>STRING</code>
     or
    <code>JSON</code>
    
    value.
  </td>
</tr>

<tr>
  <td><a href="#json_extract_array"><code>JSON_EXTRACT_ARRAY</code></a>

</td>
  <td>
    (Deprecated)
    Extracts a JSON array and converts it to
    a SQL <code>ARRAY&lt;JSON-formatted STRING&gt;</code>
     or
    <code>ARRAY&lt;JSON&gt;</code>
    
    value.
  </td>
</tr>

<tr>
  <td><a href="#json_extract_scalar"><code>JSON_EXTRACT_SCALAR</code></a>

</td>
  <td>
    (Deprecated)
    Extracts a JSON scalar value and converts it to a SQL
    <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#json_extract_string_array"><code>JSON_EXTRACT_STRING_ARRAY</code></a>

</td>
  <td>
    (Deprecated)
    Extracts a JSON array of scalar values and converts it to a SQL
    <code>ARRAY&lt;STRING&gt;</code> value.
  </td>
</tr>

<tr>
  <td><a href="#json_object"><code>JSON_OBJECT</code></a>

</td>
  <td>Creates a JSON object.</td>
</tr>

<tr>
  <td><a href="#json_query"><code>JSON_QUERY</code></a>

</td>
  <td>
    Extracts a JSON value and converts it to a SQL
    JSON-formatted <code>STRING</code>
     or
    <code>JSON</code>
    
    value.
  </td>
</tr>

<tr>
  <td><a href="#json_query_array"><code>JSON_QUERY_ARRAY</code></a>

</td>
  <td>
    Extracts a JSON array and converts it to
    a SQL <code>ARRAY&lt;JSON-formatted STRING&gt;</code>
     or
    <code>ARRAY&lt;JSON&gt;</code>
    
    value.
  </td>
</tr>

<tr>
  <td><a href="#json_remove"><code>JSON_REMOVE</code></a>

</td>
  <td>Produces JSON with the specified JSON data removed.</td>
</tr>

<tr>
  <td><a href="#json_set"><code>JSON_SET</code></a>

</td>
  <td>Inserts or replaces JSON data.</td>
</tr>

<tr>
  <td><a href="#json_strip_nulls"><code>JSON_STRIP_NULLS</code></a>

</td>
  <td>Removes JSON nulls from JSON objects and JSON arrays.</td>
</tr>

<tr>
  <td><a href="#json_type"><code>JSON_TYPE</code></a>

</td>
  <td>
    Gets the JSON type of the outermost JSON value and converts the name of
    this type to a SQL <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#json_value"><code>JSON_VALUE</code></a>

</td>
  <td>
    Extracts a JSON scalar value and converts it to a SQL
    <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#json_value_array"><code>JSON_VALUE_ARRAY</code></a>

</td>
  <td>
    Extracts a JSON array of scalar values and converts it to a SQL
    <code>ARRAY&lt;STRING&gt;</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_bool"><code>LAX_BOOL</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>BOOL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_bool_array"><code>LAX_BOOL_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;BOOL&gt;</code> value.</td>
</tr>

<tr>
  <td>
  
  <a href="#lax_double"><code>LAX_DOUBLE</code></a>

  
  </td>
  <td>
    Attempts to convert a JSON value to a
    SQL <code>DOUBLE</code> value.
  </td>
</tr>

<tr>
  <td>
    
      <a href="#lax_double_array"><code>LAX_DOUBLE_ARRAY</code></a>

    
  </td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;DOUBLE&gt;</code> value.</td>
</tr>

<tr>
  <td>
    
      <a href="#lax_float"><code>LAX_FLOAT</code></a>

    
  </td>
  <td>Attempts to convert a JSON value to a SQL <code>FLOAT</code> value.</td>
</tr>

<tr>
  <td>
    
      <a href="#lax_float_array"><code>LAX_FLOAT_ARRAY</code></a>

    
  </td>
  <td> Attempts to convert a JSON value to a SQL <code>ARRAY&gt;FLOAT&lt;</code> value.</td>
</tr>

<tr>
  <td><a href="#lax_int32"><code>LAX_INT32</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>INT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_int32_array"><code>LAX_INT32_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;INT32&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#lax_int64"><code>LAX_INT64</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>INT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_int64_array"><code>LAX_INT64_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;INT64&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#lax_string"><code>LAX_STRING</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_string_array"><code>LAX_STRING_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;STRING&gt;</code>value.</td>
</tr>

<tr>
  <td><a href="#lax_uint32"><code>LAX_UINT32</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>UINT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_uint32_array"><code>LAX_UINT32_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;UINT32&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#lax_uint64"><code>LAX_UINT64</code></a>

</td>
  <td>
    Attempts to convert a JSON value to a SQL <code>UINT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#lax_uint64_array"><code>LAX_UINT64_ARRAY</code></a>

</td>
  <td>Attempts to convert a JSON value to a SQL <code>ARRAY&lt;UINT64&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#parse_json"><code>PARSE_JSON</code></a>

</td>
  <td>
    Converts a JSON-formatted <code>STRING</code> value to a
    <code>JSON</code> value.
  </td>
</tr>

<tr>
  <td><a href="#string_for_json"><code>STRING</code></a>

</td>
  <td>
    Converts a JSON string to a SQL <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td>
    <a href="#string_array_for_json"><code>STRING_ARRAY</code></a>

  </td>
  <td>
    Converts a JSON array of strings to a SQL <code>ARRAY&lt;STRING&gt;</code>
    value.
  </td>
</tr>

<tr>
  <td><a href="#to_json"><code>TO_JSON</code></a>

</td>
  <td>
    Converts a SQL value to a JSON value.
  </td>
</tr>

<tr>
  <td><a href="#to_json_string"><code>TO_JSON_STRING</code></a>

</td>
  <td>
    Converts a SQL value to a JSON-formatted <code>STRING</code> value.
  </td>
</tr>

<tr>
  <td><a href="#uint32_for_json"><code>UINT32</code></a>

</td>
  <td>
    Converts a JSON number to a SQL <code>UINT32</code> value.
  </td>
</tr>

<tr>
  <td><a href="#uint32_array_for_json"><code>UINT32_ARRAY</code></a>

</td>
  <td>Converts a JSON number to a SQL <code>ARRAY&lt;UINT32&gt;</code> value.</td>
</tr>

<tr>
  <td><a href="#uint64_for_json"><code>UINT64</code></a>

</td>
  <td>
    Converts a JSON number to a SQL <code>UINT64</code> value.
  </td>
</tr>

<tr>
  <td><a href="#uint64_array_for_json"><code>UINT64_ARRAY</code></a>

</td>
  <td>Converts a JSON number to a SQL <code>ARRAY&lt;UINT64&gt;</code> value.</td>
</tr>

  </tbody>
</table>

### `BOOL` 
<a id="bool_for_json"></a>

```sql
BOOL(json_expr)
```

**Description**

Converts a JSON boolean to a SQL `BOOL` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON 'true'
    ```

    If the JSON value is not a boolean, an error is produced. If the expression
    is SQL `NULL`, the function returns SQL `NULL`.

**Return type**

`BOOL`

**Examples**

```sql
SELECT BOOL(JSON 'true') AS vacancy;

/*---------*
 | vacancy |
 +---------+
 | true    |
 *---------*/
```

```sql
SELECT BOOL(JSON_QUERY(JSON '{"hotel class": "5-star", "vacancy": true}', "$.vacancy")) AS vacancy;

/*---------*
 | vacancy |
 +---------+
 | true    |
 *---------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type bool.
SELECT BOOL(JSON '123') AS result; -- Throws an error
SELECT BOOL(JSON 'null') AS result; -- Throws an error
SELECT SAFE.BOOL(JSON '123') AS result; -- Returns a SQL NULL
```

### `BOOL_ARRAY` 
<a id="bool_array_for_json"></a>

```sql
BOOL_ARRAY(json_expr)
```

**Description**

Converts a JSON array of booleans to a SQL `ARRAY<BOOL>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[true]'
    ```

    If the JSON value is not an array of booleans, an error is produced. If the
    expression is SQL `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<BOOL>`

**Examples**

```sql
SELECT BOOL_ARRAY(JSON '[true, false]') AS vacancies;

/*---------------*
 | vacancies     |
 +---------------+
 | [true, false] |
 *---------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of booleans.
SELECT BOOL_ARRAY(JSON '[123]') AS result; -- Throws an error
SELECT BOOL_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT BOOL_ARRAY(JSON 'null') AS result; -- Throws an error
```

### `DOUBLE` 
<a id="double_for_json"></a>

```sql
DOUBLE(json_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Converts a JSON number to a SQL `DOUBLE` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '9.8'
    ```

    If the JSON value is not a number, an error is produced. If the expression
    is a SQL `NULL`, the function returns SQL `NULL`.
+   `wide_number_mode`: Optional mandatory-named argument,
    which defines what happens with a number that cannot be
    represented as a `DOUBLE` without loss of
    precision. This argument accepts one of the two case-sensitive values:

    +   `exact`: The function fails if the result cannot be represented as a
        `DOUBLE` without loss of precision.
    +   `round` (default): The numeric value stored in JSON will be rounded to
        `DOUBLE`. If such rounding is not possible,
        the function fails.

**Return type**

`DOUBLE`

**Examples**

```sql
SELECT DOUBLE(JSON '9.8') AS velocity;

/*----------*
 | velocity |
 +----------+
 | 9.8      |
 *----------*/
```

```sql
SELECT DOUBLE(JSON_QUERY(JSON '{"vo2_max": 39.1, "age": 18}', "$.vo2_max")) AS vo2_max;

/*---------*
 | vo2_max |
 +---------+
 | 39.1    |
 *---------*/
```

```sql
SELECT DOUBLE(JSON '18446744073709551615', wide_number_mode=>'round') as result;

/*------------------------*
 | result                 |
 +------------------------+
 | 1.8446744073709552e+19 |
 *------------------------*/
```

```sql
SELECT DOUBLE(JSON '18446744073709551615') as result;

/*------------------------*
 | result                 |
 +------------------------+
 | 1.8446744073709552e+19 |
 *------------------------*/
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

### `DOUBLE_ARRAY` 
<a id="double_array_for_json"></a>

```sql
DOUBLE_ARRAY(json_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Converts a JSON array of numbers to a SQL `ARRAY<DOUBLE>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[9.8]'
    ```

    If the JSON value is not an array of numbers, an error is produced. If the
    expression is a SQL `NULL`, the function returns SQL `NULL`.
+   `wide_number_mode`: Optional mandatory-named argument, which defines what
    happens with a number that cannot be represented as a
    `DOUBLE` without loss of precision. This argument accepts
    one of the two case-sensitive values:

    +   `exact`: The function fails if the result cannot be represented as a
        `DOUBLE` without loss of precision.
    +   `round` (default): The numeric value stored in JSON will be rounded to
        `DOUBLE`. If such rounding is not possible, the
        function fails.

**Return type**

`ARRAY<DOUBLE>`

**Examples**

```sql
SELECT DOUBLE_ARRAY(JSON '[9, 9.8]') AS velocities;

/*-------------*
 | velocities  |
 +-------------+
 | [9.0, 9.8]  |
 *-------------*/
```

```sql
SELECT DOUBLE_ARRAY(JSON '[18446744073709551615]', wide_number_mode=>'round') as result;

/*--------------------------*
 | result                   |
 +--------------------------+
 | [1.8446744073709552e+19] |
 *--------------------------*/
```

```sql
SELECT DOUBLE_ARRAY(JSON '[18446744073709551615]') as result;

/*--------------------------*
 | result                   |
 +--------------------------+
 | [1.8446744073709552e+19] |
 *--------------------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers.
SELECT DOUBLE_ARRAY(JSON '["strawberry"]') AS result;
SELECT DOUBLE_ARRAY(JSON '[null]') AS result;
SELECT DOUBLE_ARRAY(JSON 'null') AS result;

-- An error is thrown because `wide_number_mode` is case-sensitive and not "exact" or "round".
SELECT DOUBLE_ARRAY(JSON '[123.4]', wide_number_mode=>'EXACT') as result;
SELECT DOUBLE_ARRAY(JSON '[123.4]', wide_number_mode=>'exac') as result;

-- An error is thrown because the number cannot be converted to DOUBLE without loss of precision
SELECT DOUBLE_ARRAY(JSON '[18446744073709551615]', wide_number_mode=>'exact') as result;
```

### `FLOAT` 
<a id="float_for_json"></a>

```sql
FLOAT(json_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Converts a JSON number to a SQL `FLOAT` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '9.8'
    ```

    If the JSON value is not a number, an error is produced. If the expression
    is a SQL `NULL`, the function returns SQL `NULL`.
+   `wide_number_mode`: Optional mandatory-named argument, which defines what
    happens with a number that cannot be represented as a
    `FLOAT` without loss of precision. This argument accepts
    one of the two case-sensitive values:

    +   `exact`: The function fails if the result cannot be represented as a
        `FLOAT` without loss of precision.
    +   `round` (default): The numeric value stored in JSON will be rounded to
        `FLOAT`. If such rounding is not possible, the function
        fails.

**Return type**

`FLOAT`

**Examples**

```sql
SELECT FLOAT(JSON '9.8') AS velocity;

/*----------*
 | velocity |
 +----------+
 | 9.8      |
 *----------*/
```

```sql
SELECT FLOAT(JSON_QUERY(JSON '{"vo2_max": 39.1, "age": 18}', "$.vo2_max")) AS vo2_max;

/*---------*
 | vo2_max |
 +---------+
 | 39.1    |
 *---------*/
```

```sql
SELECT FLOAT(JSON '16777217', wide_number_mode=>'round') as result;

/*------------*
 | result     |
 +------------+
 | 16777216.0 |
 *------------*/
```

```sql
SELECT FLOAT(JSON '16777216') as result;

/*------------*
 | result     |
 +------------+
 | 16777216.0 |
 *------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not of type FLOAT.
SELECT FLOAT(JSON '"strawberry"') AS result;
SELECT FLOAT(JSON 'null') AS result;

-- An error is thrown because `wide_number_mode` is case-sensitive and not "exact" or "round".
SELECT FLOAT(JSON '123.4', wide_number_mode=>'EXACT') as result;
SELECT FLOAT(JSON '123.4', wide_number_mode=>'exac') as result;

-- An error is thrown because the number cannot be converted to FLOAT without loss of precision
SELECT FLOAT(JSON '16777217', wide_number_mode=>'exact') as result;

-- Returns a SQL NULL
SELECT SAFE.FLOAT(JSON '"strawberry"') AS result;
```

### `FLOAT_ARRAY` 
<a id="float_array_for_json"></a>

```sql
FLOAT_ARRAY(json_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Converts a JSON array of numbers to a SQL `ARRAY<FLOAT>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[9.8]'
    ```

    If the JSON value is not an array of numbers, an error is produced. If the
    expression is a SQL `NULL`, the function returns SQL `NULL`.
+   `wide_number_mode`: Optional mandatory-named argument, which defines what
    happens with a number that cannot be represented as a
    `FLOAT` without loss of precision. This argument accepts
    one of the two case-sensitive values:

    +   `exact`: The function fails if the result cannot be represented as a
        `FLOAT` without loss of precision.
    +   `round` (default): The numeric value stored in JSON will be rounded to
        `FLOAT`. If such rounding is not possible, the function
        fails.

**Return type**

`ARRAY<FLOAT>`

**Examples**

```sql
SELECT FLOAT_ARRAY(JSON '[9, 9.8]') AS velocities;

/*-------------*
 | velocities  |
 +-------------+
 | [9.0, 9.8]  |
 *-------------*/
```

```sql
SELECT FLOAT_ARRAY(JSON '[16777217]', wide_number_mode=>'round') as result;

/*--------------*
 | result       |
 +--------------+
 | [16777216.0] |
 *--------------*/
```

```sql
SELECT FLOAT_ARRAY(JSON '[16777216]') as result;

/*--------------*
 | result       |
 +--------------+
 | [16777216.0] |
 *--------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers in FLOAT domain.
SELECT FLOAT_ARRAY(JSON '["strawberry"]') AS result;
SELECT FLOAT_ARRAY(JSON '[null]') AS result;
SELECT FLOAT_ARRAY(JSON 'null') AS result;

-- An error is thrown because `wide_number_mode` is case-sensitive and not "exact" or "round".
SELECT FLOAT_ARRAY(JSON '[123.4]', wide_number_mode=>'EXACT') as result;
SELECT FLOAT_ARRAY(JSON '[123.4]', wide_number_mode=>'exac') as result;

-- An error is thrown because the number cannot be converted to FLOAT without loss of precision
SELECT FLOAT_ARRAY(JSON '[16777217]', wide_number_mode=>'exact') as result;
```

### `INT32` 
<a id="int32_for_json"></a>

```sql
INT32(json_expr)
```

**Description**

Converts a JSON number to a SQL `INT32` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

    If the JSON value is not a number, or the JSON number is not in the SQL
    `INT32` domain, an error is produced. If the expression is SQL `NULL`, the
    function returns SQL `NULL`.

**Return type**

`INT32`

**Examples**

```sql
SELECT INT32(JSON '2005') AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT32(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT32(JSON '10.0') AS score;

/*-------*
 | score |
 +-------+
 | 10    |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT INT32(JSON '10.1') AS result;  -- Throws an error
SELECT INT32(JSON '"strawberry"') AS result; -- Throws an error
SELECT INT32(JSON 'null') AS result; -- Throws an error
SELECT SAFE.INT32(JSON '"strawberry"') AS result;  -- Returns a SQL NULL
```

### `INT32_ARRAY` 
<a id="int32_array_for_json"></a>

```sql
INT32_ARRAY(json_expr)
```

**Description**

Converts a JSON number to a SQL `INT32_ARRAY` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999]'
    ```

    If the JSON value is not an array of numbers, or the JSON numbers are not in
    the SQL `INT32` domain, an error is produced. If the expression is SQL
    `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<INT32>`

**Examples**

```sql
SELECT INT32_ARRAY(JSON '[2005, 2003]') AS flight_numbers;

/*----------------*
 | flight_numbers |
 +----------------+
 | [2005, 2003]   |
 *----------------*/
```

```sql
SELECT INT32_ARRAY(JSON '[10.0]') AS scores;

/*--------*
 | scores |
 +--------+
 | [10]   |
 *--------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers in INT32 domain.
SELECT INT32_ARRAY(JSON '[10.1]') AS result;  -- Throws an error
SELECT INT32_ARRAY(JSON '["strawberry"]') AS result; -- Throws an error
SELECT INT32_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT INT32_ARRAY(JSON 'null') AS result; -- Throws an error
```

### `INT64` 
<a id="int64_for_json"></a>

```sql
INT64(json_expr)
```

**Description**

Converts a JSON number to a SQL `INT64` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

    If the JSON value is not a number, or the JSON number is not in the SQL
    `INT64` domain, an error is produced. If the expression is SQL `NULL`, the
    function returns SQL `NULL`.

**Return type**

`INT64`

**Examples**

```sql
SELECT INT64(JSON '2005') AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT64(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT INT64(JSON '10.0') AS score;

/*-------*
 | score |
 +-------+
 | 10    |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT INT64(JSON '10.1') AS result;  -- Throws an error
SELECT INT64(JSON '"strawberry"') AS result; -- Throws an error
SELECT INT64(JSON 'null') AS result; -- Throws an error
SELECT SAFE.INT64(JSON '"strawberry"') AS result;  -- Returns a SQL NULL
```

### `INT64_ARRAY` 
<a id="int64_array_for_json"></a>

```sql
INT64_ARRAY(json_expr)
```

**Description**

Converts a JSON array of numbers to a SQL `INT64_ARRAY` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999]'
    ```

    If the JSON value is not an array of numbers, or the JSON numbers are not in
    the SQL `INT64` domain, an error is produced. If the expression is SQL
    `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<INT64>`

**Examples**

```sql
SELECT INT64_ARRAY(JSON '[2005, 2003]') AS flight_numbers;

/*----------------*
 | flight_numbers |
 +----------------+
 | [2005, 2003]   |
 *----------------*/
```

```sql
SELECT INT64_ARRAY(JSON '[10.0]') AS scores;

/*--------*
 | scores |
 +--------+
 | [10]   |
 *--------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers in INT64 domain.
SELECT INT64_ARRAY(JSON '[10.1]') AS result;  -- Throws an error
SELECT INT64_ARRAY(JSON '["strawberry"]') AS result; -- Throws an error
SELECT INT64_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT INT64_ARRAY(JSON 'null') AS result; -- Throws an error
```

### `JSON_ARRAY`

```sql
JSON_ARRAY([value][, ...])
```

**Description**

Creates a JSON array from zero or more SQL values.

Arguments:

+   `value`: A [JSON encoding-supported][json-encodings] value to add
    to a JSON array.

**Return type**

`JSON`

**Examples**

The following query creates a JSON array with one value in it:

```sql
SELECT JSON_ARRAY(10) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [10]      |
 *-----------*/
```

You can create a JSON array with an empty JSON array in it. For example:

```sql
SELECT JSON_ARRAY([]) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [[]]      |
 *-----------*/
```

```sql
SELECT JSON_ARRAY(10, 'foo', NULL) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | [10,"foo",null] |
 *-----------------*/
```

```sql
SELECT JSON_ARRAY(STRUCT(10 AS a, 'foo' AS b)) AS json_data

/*----------------------*
 | json_data            |
 +----------------------+
 | [{"a":10,"b":"foo"}] |
 *----------------------*/
```

```sql
SELECT JSON_ARRAY(10, ['foo', 'bar'], [20, 30]) AS json_data

/*----------------------------*
 | json_data                  |
 +----------------------------+
 | [10,["foo","bar"],[20,30]] |
 *----------------------------*/
```

```sql
SELECT JSON_ARRAY(10, [JSON '20', JSON '"foo"']) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | [10,[20,"foo"]] |
 *-----------------*/
```

You can create an empty JSON array. For example:

```sql
SELECT JSON_ARRAY() AS json_data

/*-----------*
 | json_data |
 +-----------+
 | []        |
 *-----------*/
```

### `JSON_ARRAY_APPEND`

```sql
JSON_ARRAY_APPEND(
  json_expr,
  json_path_value_pair[, ...]
  [, append_each_element=>{ TRUE | FALSE }]
)

json_path_value_pair:
  json_path, value
```

Appends JSON data to the end of a JSON array.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '["a", "b", "c"]'
    ```
+   `json_path_value_pair`: A value and the [JSONPath][JSONPath-format] for
    that value. This includes:

    +   `json_path`: Append `value` at this [JSONPath][JSONPath-format]
        in `json_expr`.

    +   `value`: A [JSON encoding-supported][json-encodings] value to
        append.
+   `append_each_element`: An optional, mandatory named argument.

    +   If `TRUE` (default), and `value` is a SQL array,
        appends each element individually.

    +   If `FALSE,` and `value` is a SQL array, appends
        the array as one element.

Details:

+   Path value pairs are evaluated left to right. The JSON produced by
    evaluating one pair becomes the JSON against which the next pair
    is evaluated.
+   The operation is ignored if the path points to a JSON non-array value that
    is not a JSON null.
+   If `json_path` points to a JSON null, the JSON null is replaced by a
    JSON array that contains `value`.
+   If the path exists but has an incompatible type at any given path token,
    the path value pair operation is ignored.
+   The function applies all path value pair append operations even if an
    individual path value pair operation is invalid. For invalid operations,
    the operation is ignored and the function continues to process the rest of
    the path value pairs.
+   If any `json_path` is an invalid [JSONPath][JSONPath-format], an error is
    produced.
+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   If `append_each_element` is SQL `NULL`, the function returns `json_expr`.
+   If `json_path` is SQL `NULL`, the `json_path_value_pair` operation is
    ignored.

**Return type**

`JSON`

**Examples**

In the following example, path `$` is matched and appends `1`.

```sql
SELECT JSON_ARRAY_APPEND(JSON '["a", "b", "c"]', '$', 1) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | ["a","b","c",1] |
 *-----------------*/
```

In the following example, `append_each_element` defaults to `TRUE`, so
`[1, 2]` is appended as individual elements.

```sql
SELECT JSON_ARRAY_APPEND(JSON '["a", "b", "c"]', '$', [1, 2]) AS json_data

/*-------------------*
 | json_data         |
 +-------------------+
 | ["a","b","c",1,2] |
 *-------------------*/
```

In the following example, `append_each_element` is `FALSE`, so
`[1, 2]` is appended as one element.

```sql
SELECT JSON_ARRAY_APPEND(
  JSON '["a", "b", "c"]',
  '$', [1, 2],
  append_each_element=>FALSE) AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | ["a","b","c",[1,2]] |
 *---------------------*/
```

In the following example, `append_each_element` is `FALSE`, so
`[1, 2]` and `[3, 4]` are each appended as one element.

```sql
SELECT JSON_ARRAY_APPEND(
  JSON '["a", ["b"], "c"]',
  '$[1]', [1, 2],
  '$[1][1]', [3, 4],
  append_each_element=>FALSE) AS json_data

/*-----------------------------*
 | json_data                   |
 +-----------------------------+
 | ["a",["b",[1,2,[3,4]]],"c"] |
 *-----------------------------*/
```

In the following example, the first path `$[1]` appends `[1, 2]` as single
elements, and then the second path `$[1][1]` is not a valid path to an array,
so the second operation is ignored.

```sql
SELECT JSON_ARRAY_APPEND(
  JSON '["a", ["b"], "c"]',
  '$[1]', [1, 2],
  '$[1][1]', [3, 4]) AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | ["a",["b",1,2],"c"] |
 *---------------------*/
```

In the following example, path `$.a` is matched and appends `2`.

```sql
SELECT JSON_ARRAY_APPEND(JSON '{"a": [1]}', '$.a', 2) AS json_data

/*-------------*
 | json_data   |
 +-------------+
 | {"a":[1,2]} |
 *-------------*/
```

In the following example, a value is appended into a JSON null.

```sql
SELECT JSON_ARRAY_APPEND(JSON '{"a": null}', '$.a', 10)

/*------------*
 | json_data  |
 +------------+
 | {"a":[10]} |
 *------------*/
```

In the following example, path `$.a` is not an array, so the operation is
ignored.

```sql
SELECT JSON_ARRAY_APPEND(JSON '{"a": 1}', '$.a', 2) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":1}   |
 *-----------*/
```

In the following example, path `$.b` does not exist, so the operation is
ignored.

```sql
SELECT JSON_ARRAY_APPEND(JSON '{"a": 1}', '$.b', 2) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":1}   |
 *-----------*/
```

### `JSON_ARRAY_INSERT`

```sql
JSON_ARRAY_INSERT(
  json_expr,
  json_path_value_pair[, ...]
  [, insert_each_element=>{ TRUE | FALSE }]
)

json_path_value_pair:
  json_path, value
```

Produces a new JSON value that is created by inserting JSON data into
a JSON array.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '["a", "b", "c"]'
    ```
+   `json_path_value_pair`: A value and the [JSONPath][JSONPath-format] for
    that value. This includes:

    +   `json_path`: Insert `value` at this [JSONPath][JSONPath-format]
        in `json_expr`.

    +   `value`: A [JSON encoding-supported][json-encodings] value to
        insert.
+   `insert_each_element`: An optional, mandatory named argument.

    +   If `TRUE` (default), and `value` is a SQL array,
        inserts each element individually.

    +   If `FALSE,` and `value` is a SQL array, inserts
        the array as one element.

Details:

+   Path value pairs are evaluated left to right. The JSON produced by
    evaluating one pair becomes the JSON against which the next pair
    is evaluated.
+   The operation is ignored if the path points to a JSON non-array value that
    is not a JSON null.
+   If `json_path` points to a JSON null, the JSON null is replaced by a
    JSON array of the appropriate size and padded on the left with JSON nulls.
+   If the path exists but has an incompatible type at any given path token,
    the path value pair operator is ignored.
+   The function applies all path value pair append operations even if an
    individual path value pair operation is invalid. For invalid operations,
    the operation is ignored and the function continues to process the rest of
    the path value pairs.
+   If the array index in `json_path` is larger than the size of the array, the
    function extends the length of the array to the index, fills in
    the array with JSON nulls, then adds `value` at the index.
+   If any `json_path` is an invalid [JSONPath][JSONPath-format], an error is
    produced.
+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   If `insert_each_element` is SQL `NULL`, the function returns `json_expr`.
+   If `json_path` is SQL `NULL`, the `json_path_value_pair` operation is
    ignored.

**Return type**

`JSON`

**Examples**

In the following example, path `$[1]` is matched and inserts `1`.

```sql
SELECT JSON_ARRAY_INSERT(JSON '["a", ["b", "c"], "d"]', '$[1]', 1) AS json_data

/*-----------------------*
 | json_data             |
 +-----------------------+
 | ["a",1,["b","c"],"d"] |
 *-----------------------*/
```

In the following example, path `$[1][0]` is matched and inserts `1`.

```sql
SELECT JSON_ARRAY_INSERT(JSON '["a", ["b", "c"], "d"]', '$[1][0]', 1) AS json_data

/*-----------------------*
 | json_data             |
 +-----------------------+
 | ["a",[1,"b","c"],"d"] |
 *-----------------------*/
```

In the following example, `insert_each_element` defaults to `TRUE`, so
`[1, 2]` is inserted as individual elements.

```sql
SELECT JSON_ARRAY_INSERT(JSON '["a", "b", "c"]', '$[1]', [1, 2]) AS json_data

/*-------------------*
 | json_data         |
 +-------------------+
 | ["a",1,2,"b","c"] |
 *-------------------*/
```

In the following example, `insert_each_element` is `FALSE`, so `[1, 2]` is
inserted as one element.

```sql
SELECT JSON_ARRAY_INSERT(
  JSON '["a", "b", "c"]',
  '$[1]', [1, 2],
  insert_each_element=>FALSE) AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | ["a",[1,2],"b","c"] |
 *---------------------*/
```

In the following example, path `$[7]` is larger than the length of the
matched array, so the array is extended with JSON nulls and `"e"` is inserted at
the end of the array.

```sql
SELECT JSON_ARRAY_INSERT(JSON '["a", "b", "c", "d"]', '$[7]', "e") AS json_data

/*--------------------------------------*
 | json_data                            |
 +--------------------------------------+
 | ["a","b","c","d",null,null,null,"e"] |
 *--------------------------------------*/
```

In the following example, path `$.a` is an object, so the operation is ignored.

```sql
SELECT JSON_ARRAY_INSERT(JSON '{"a": {}}', '$.a[0]', 2) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":{}}  |
 *-----------*/
```

In the following example, path `$` does not specify a valid array position,
so the operation is ignored.

```sql
SELECT JSON_ARRAY_INSERT(JSON '[1, 2]', '$', 3) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [1,2]     |
 *-----------*/
```

In the following example, a value is inserted into a JSON null.

```sql
SELECT JSON_ARRAY_INSERT(JSON '{"a": null}', '$.a[2]', 10) AS json_data

/*----------------------*
 | json_data            |
 +----------------------+
 | {"a":[null,null,10]} |
 *----------------------*/
```

In the following example, the operation is ignored because you can't insert
data into a JSON number.

```sql
SELECT JSON_ARRAY_INSERT(JSON '1', '$[0]', 'r1') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | 1         |
 *-----------*/
```

### `JSON_EXTRACT`

Note: This function is deprecated. Consider using [JSON_QUERY][json-query].

```sql
JSON_EXTRACT(json_string_expr, json_path)
```

```sql
JSON_EXTRACT(json_expr, json_path)
```

**Description**

Extracts a JSON value and converts it to a
SQL JSON-formatted `STRING` or `JSON` value.
This function uses single quotes and brackets to escape invalid
[JSONPath][JSONPath-format] characters in JSON keys. For example: `['a.b']`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class": {"students": [{"name": "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string `null` is encountered.
    For example:

    ```sql
    SELECT JSON_EXTRACT("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class": {"students": [{"name": "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_EXTRACT(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_EXTRACT(JSON '{"class": {"students": [{"id": 5}, {"id": 12}]}}', '$.class')
  AS json_data;

/*-----------------------------------*
 | json_data                         |
 +-----------------------------------+
 | {"students":[{"id":5},{"id":12}]} |
 *-----------------------------------*/
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "Jane"}]}}',
  '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"Jane"}]}}                  |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": []}}',
  '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[]}}                                 |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
  '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "Jane"}]}}',
  '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"Jane"} |
 *-----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": []}}',
  '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | NULL            |
 *-----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
  '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"John"} |
 *-----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "Jane"}]}}',
  '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": []}}',
  '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "John"}, {"name": null}]}}',
  '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
  '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | "Jamie"        |
 *----------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "Jane"}]}}',
  "$.class['students']") AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"Jane"}]                  |
 *------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": []}}',
  "$.class['students']") AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | []                                 |
 *------------------------------------*/
```

```sql
SELECT JSON_EXTRACT(
  '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
  "$.class['students']") AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"John"},{"name":"Jamie"}] |
 *------------------------------------*/
```

```sql
SELECT JSON_EXTRACT('{"a": null}', "$.a"); -- Returns a SQL NULL
SELECT JSON_EXTRACT('{"a": null}', "$.b"); -- Returns a SQL NULL
```

```sql
SELECT JSON_EXTRACT(JSON '{"a": null}', "$.a"); -- Returns a JSON 'null'
SELECT JSON_EXTRACT(JSON '{"a": null}', "$.b"); -- Returns a SQL NULL
```

[json-query]: #json_query

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_EXTRACT_ARRAY`

Note: This function is deprecated. Consider using
[JSON_QUERY_ARRAY][json-query-array].

```sql
JSON_EXTRACT_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_EXTRACT_ARRAY(json_expr[, json_path])
```

**Description**

Extracts a JSON array and converts it to
a SQL `ARRAY<JSON-formatted STRING>` or
`ARRAY<JSON>` value.
This function uses single quotes and brackets to escape invalid
[JSONPath][JSONPath-format] characters in JSON keys. For example: `['a.b']`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '["a", "b", {"key": "c"}]'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '["a", "b", {"key": "c"}]'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: `ARRAY<JSON-formatted STRING>`
+ `json_expr`: `ARRAY<JSON>`

**Examples**

This extracts items in JSON to an array of `JSON` values:

```sql
SELECT JSON_EXTRACT_ARRAY(
  JSON '{"fruits":["apples","oranges","grapes"]}','$.fruits'
  ) AS json_array;

/*---------------------------------*
 | json_array                      |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
SELECT JSON_EXTRACT_ARRAY('[1,2,3]') AS string_array;

/*--------------*
 | string_array |
 +--------------+
 | [1, 2, 3]    |
 *--------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_EXTRACT_ARRAY('[1,2,3]','$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

This extracts string values in a JSON-formatted string to an array:

```sql
-- Doesn't strip the double quotes
SELECT JSON_EXTRACT_ARRAY('["apples", "oranges", "grapes"]', '$') AS string_array;

/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/

-- Strips the double quotes
SELECT ARRAY(
  SELECT JSON_EXTRACT_SCALAR(string_element, '$')
  FROM UNNEST(JSON_EXTRACT_ARRAY('["apples","oranges","grapes"]','$')) AS string_element
) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

This extracts only the items in the `fruit` property to an array:

```sql
SELECT JSON_EXTRACT_ARRAY(
  '{"fruit": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}',
  '$.fruit'
) AS string_array;

/*-------------------------------------------------------*
 | string_array                                          |
 +-------------------------------------------------------+
 | [{"apples":5,"oranges":10}, {"apples":2,"oranges":4}] |
 *-------------------------------------------------------*/
```

These are equivalent:

```sql
SELECT JSON_EXTRACT_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$[fruits]') AS string_array;

SELECT JSON_EXTRACT_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits') AS string_array;

-- The queries above produce the following result:
/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_ARRAY('{"a.b": {"c": ["world"]}}', "$['a.b'].c") AS hello;

/*-----------*
 | hello     |
 +-----------+
 | ["world"] |
 *-----------*/
```

The following examples explore how invalid requests and empty arrays are
handled:

+  If a JSONPath is invalid, an error is thrown.
+  If a JSON-formatted string is invalid, the output is NULL.
+  It is okay to have empty arrays in the JSON-formatted string.

```sql
-- An error is thrown if you provide an invalid JSONPath.
SELECT JSON_EXTRACT_ARRAY('["foo", "bar", "baz"]', 'INVALID_JSONPath') AS result;

-- If the JSONPath does not refer to an array, then NULL is returned.
SELECT JSON_EXTRACT_ARRAY('{"a": "foo"}', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a key that does not exist is specified, then the result is NULL.
SELECT JSON_EXTRACT_ARRAY('{"a": "foo"}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- Empty arrays in JSON-formatted strings are supported.
SELECT JSON_EXTRACT_ARRAY('{"a": "foo", "b": []}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/
```

[json-query-array]: #json_query_array

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_EXTRACT_SCALAR`

Note: This function is deprecated. Consider using [JSON_VALUE][json-value].

```sql
JSON_EXTRACT_SCALAR(json_string_expr[, json_path])
```

```sql
JSON_EXTRACT_SCALAR(json_expr[, json_path])
```

**Description**

Extracts a JSON scalar value and converts it to a SQL `STRING` value.
In addition, this function:

+   Removes the outermost quotes and unescapes the return values.
+   Returns a SQL `NULL` if a non-scalar value is selected.
+   Uses single quotes and brackets to escape invalid [JSONPath][JSONPath-format]
    characters in JSON keys. For example: `['a.b']`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"name": "Jane", "age": "6"}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "Jane", "age": "6"}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

`STRING`

**Examples**

In the following example, `age` is extracted.

```sql
SELECT JSON_EXTRACT_SCALAR(JSON '{"name": "Jakob", "age": "6" }', '$.age') AS scalar_age;

/*------------*
 | scalar_age |
 +------------+
 | 6          |
 *------------*/
```

The following example compares how results are returned for the `JSON_EXTRACT`
and `JSON_EXTRACT_SCALAR` functions.

```sql
SELECT JSON_EXTRACT('{"name": "Jakob", "age": "6" }', '$.name') AS json_name,
  JSON_EXTRACT_SCALAR('{"name": "Jakob", "age": "6" }', '$.name') AS scalar_name,
  JSON_EXTRACT('{"name": "Jakob", "age": "6" }', '$.age') AS json_age,
  JSON_EXTRACT_SCALAR('{"name": "Jakob", "age": "6" }', '$.age') AS scalar_age;

/*-----------+-------------+----------+------------*
 | json_name | scalar_name | json_age | scalar_age |
 +-----------+-------------+----------+------------+
 | "Jakob"   | Jakob       | "6"      | 6          |
 *-----------+-------------+----------+------------*/
```

```sql
SELECT JSON_EXTRACT('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract,
  JSON_EXTRACT_SCALAR('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_extract_scalar;

/*--------------------+---------------------*
 | json_extract       | json_extract_scalar |
 +--------------------+---------------------+
 | ["apple","banana"] | NULL                |
 *--------------------+---------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c") AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

[json-value]: #json_value

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_EXTRACT_STRING_ARRAY`

Note: This function is deprecated. Consider using
[JSON_VALUE_ARRAY][json-value-array].

```sql
JSON_EXTRACT_STRING_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_EXTRACT_STRING_ARRAY(json_expr[, json_path])
```

**Description**

Extracts a JSON array of scalar values and converts it to a SQL `ARRAY<STRING>`
value. In addition, this function:

+   Removes the outermost quotes and unescapes the values.
+   Returns a SQL `NULL` if the selected value is not an array or
    not an array containing only scalar values.
+   Uses single quotes and brackets to escape invalid [JSONPath][JSONPath-format]
    characters in JSON keys. For example: `['a.b']`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '["apples", "oranges", "grapes"]'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '["apples", "oranges", "grapes"]'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

Caveats:

+ A JSON `null` in the input array produces a SQL `NULL` as the output for that
  JSON `null`.
+ If a JSONPath matches an array that contains scalar objects and a JSON `null`,
  then the output is an array of the scalar objects and a SQL `NULL`.

**Return type**

`ARRAY<STRING>`

**Examples**

This extracts items in JSON to a string array:

```sql
SELECT JSON_EXTRACT_STRING_ARRAY(
  JSON '{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits'
  ) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

The following example compares how results are returned for the
`JSON_EXTRACT_ARRAY` and `JSON_EXTRACT_STRING_ARRAY` functions.

```sql
SELECT JSON_EXTRACT_ARRAY('["apples", "oranges"]') AS json_array,
JSON_EXTRACT_STRING_ARRAY('["apples", "oranges"]') AS string_array;

/*-----------------------+-------------------*
 | json_array            | string_array      |
 +-----------------------+-------------------+
 | ["apples", "oranges"] | [apples, oranges] |
 *-----------------------+-------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
-- Strips the double quotes
SELECT JSON_EXTRACT_STRING_ARRAY('["foo", "bar", "baz"]', '$') AS string_array;

/*-----------------*
 | string_array    |
 +-----------------+
 | [foo, bar, baz] |
 *-----------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_EXTRACT_STRING_ARRAY('[1, 2, 3]', '$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

These are equivalent:

```sql
SELECT JSON_EXTRACT_STRING_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$[fruits]') AS string_array;

SELECT JSON_EXTRACT_STRING_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits') AS string_array;

-- The queries above produce the following result:
/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets: `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_STRING_ARRAY('{"a.b": {"c": ["world"]}}', "$['a.b'].c") AS hello;

/*---------*
 | hello   |
 +---------+
 | [world] |
 *---------*/
```

The following examples explore how invalid requests and empty arrays are
handled:

```sql
-- An error is thrown if you provide an invalid JSONPath.
SELECT JSON_EXTRACT_STRING_ARRAY('["foo", "bar", "baz"]', 'INVALID_JSONPath') AS result;

-- If the JSON formatted string is invalid, then NULL is returned.
SELECT JSON_EXTRACT_STRING_ARRAY('}}', '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If the JSON document is NULL, then NULL is returned.
SELECT JSON_EXTRACT_STRING_ARRAY(NULL, '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath does not match anything, then the output is NULL.
SELECT JSON_EXTRACT_STRING_ARRAY('{"a": ["foo", "bar", "baz"]}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an object that is not an array, then the output is NULL.
SELECT JSON_EXTRACT_STRING_ARRAY('{"a": "foo"}', '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of non-scalar objects, then the output is NULL.
SELECT JSON_EXTRACT_STRING_ARRAY('{"a": [{"b": "foo", "c": 1}, {"b": "bar", "c":2}], "d": "baz"}', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of mixed scalar and non-scalar objects, then the output is NULL.
SELECT JSON_EXTRACT_STRING_ARRAY('{"a": [10, {"b": 20}]', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an empty JSON array, then the output is an empty array instead of NULL.
SELECT JSON_EXTRACT_STRING_ARRAY('{"a": "foo", "b": []}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/

-- In the following query, the JSON null input is returned as a
-- SQL NULL in the output.
SELECT JSON_EXTRACT_STRING_ARRAY('["world", 1, null]') AS result;

/*------------------*
 | result           |
 +------------------+
 | [world, 1, NULL] |
 *------------------*/

```

[json-value-array]: #json_value_array

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_OBJECT`

+   [Signature 1](#json_object_signature1):
    `JSON_OBJECT([json_key, json_value][, ...])`
+   [Signature 2](#json_object_signature2):
    `JSON_OBJECT(json_key_array, json_value_array)`

#### Signature 1 
<a id="json_object_signature1"></a>

```sql
JSON_OBJECT([json_key, json_value][, ...])
```

**Description**

Creates a JSON object, using key-value pairs.

Arguments:

+   `json_key`: A `STRING` value that represents a key.
+   `json_value`: A [JSON encoding-supported][json-encodings] value.

Details:

+   If two keys are passed in with the same name, only the first key-value pair
    is preserved.
+   The order of key-value pairs is not preserved.
+   If `json_key` is `NULL`, an error is produced.

**Return type**

`JSON`

**Examples**

You can create an empty JSON object by passing in no JSON keys and values.
For example:

```sql
SELECT JSON_OBJECT() AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {}        |
 *-----------*/
```

You can create a JSON object by passing in key-value pairs. For example:

```sql
SELECT JSON_OBJECT('foo', 10, 'bar', TRUE) AS json_data

/*-----------------------*
 | json_data             |
 +-----------------------+
 | {"bar":true,"foo":10} |
 *-----------------------*/
```

```sql
SELECT JSON_OBJECT('foo', 10, 'bar', ['a', 'b']) AS json_data

/*----------------------------*
 | json_data                  |
 +----------------------------+
 | {"bar":["a","b"],"foo":10} |
 *----------------------------*/
```

```sql
SELECT JSON_OBJECT('a', NULL, 'b', JSON 'null') AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | {"a":null,"b":null} |
 *---------------------*/
```

```sql
SELECT JSON_OBJECT('a', 10, 'a', 'foo') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":10}  |
 *-----------*/
```

```sql
WITH Items AS (SELECT 'hello' AS key, 'world' AS value)
SELECT JSON_OBJECT(key, value) AS json_data FROM Items

/*-------------------*
 | json_data         |
 +-------------------+
 | {"hello":"world"} |
 *-------------------*/
```

An error is produced if a SQL `NULL` is passed in for a JSON key.

```sql
-- Error: A key cannot be NULL.
SELECT JSON_OBJECT(NULL, 1) AS json_data
```

An error is produced if the number of JSON keys and JSON values don't match:

```sql
-- Error: No matching signature for function JSON_OBJECT for argument types:
-- STRING, INT64, STRING
SELECT JSON_OBJECT('a', 1, 'b') AS json_data
```

#### Signature 2 
<a id="json_object_signature2"></a>

```sql
JSON_OBJECT(json_key_array, json_value_array)
```

Creates a JSON object, using an array of keys and values.

Arguments:

+   `json_key_array`: An array of zero or more `STRING` keys.
+   `json_value_array`: An array of zero or more
    [JSON encoding-supported][json-encodings] values.

Details:

+   If two keys are passed in with the same name, only the first key-value pair
    is preserved.
+   The order of key-value pairs is not preserved.
+   The number of keys must match the number of values, otherwise an error is
    produced.
+   If any argument is `NULL`, an error is produced.
+   If a key in `json_key_array` is `NULL`, an error is produced.

**Return type**

`JSON`

**Examples**

You can create an empty JSON object by passing in an empty array of
keys and values. For example:

```sql
SELECT JSON_OBJECT(CAST([] AS ARRAY<STRING>), []) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {}        |
 *-----------*/
```

You can create a JSON object by passing in an array of keys and an array of
values. For example:

```sql
SELECT JSON_OBJECT(['a', 'b'], [10, NULL]) AS json_data

/*-------------------*
 | json_data         |
 +-------------------+
 | {"a":10,"b":null} |
 *-------------------*/
```

```sql
SELECT JSON_OBJECT(['a', 'b'], [JSON '10', JSON '"foo"']) AS json_data

/*--------------------*
 | json_data          |
 +--------------------+
 | {"a":10,"b":"foo"} |
 *--------------------*/
```

```sql
SELECT
  JSON_OBJECT(
    ['a', 'b'],
    [STRUCT(10 AS id, 'Red' AS color), STRUCT(20 AS id, 'Blue' AS color)])
    AS json_data

/*------------------------------------------------------------*
 | json_data                                                  |
 +------------------------------------------------------------+
 | {"a":{"color":"Red","id":10},"b":{"color":"Blue","id":20}} |
 *------------------------------------------------------------*/
```

```sql
SELECT
  JSON_OBJECT(
    ['a', 'b'],
    [TO_JSON(10), TO_JSON(['foo', 'bar'])])
    AS json_data

/*----------------------------*
 | json_data                  |
 +----------------------------+
 | {"a":10,"b":["foo","bar"]} |
 *----------------------------*/
```

The following query groups by `id` and then creates an array of keys and
values from the rows with the same `id`:

```sql
WITH
  Fruits AS (
    SELECT 0 AS id, 'color' AS json_key, 'red' AS json_value UNION ALL
    SELECT 0, 'fruit', 'apple' UNION ALL
    SELECT 1, 'fruit', 'banana' UNION ALL
    SELECT 1, 'ripe', 'true'
  )
SELECT JSON_OBJECT(ARRAY_AGG(json_key), ARRAY_AGG(json_value)) AS json_data
FROM Fruits
GROUP BY id

/*----------------------------------*
 | json_data                        |
 +----------------------------------+
 | {"color":"red","fruit":"apple"}  |
 | {"fruit":"banana","ripe":"true"} |
 *----------------------------------*/
```

An error is produced if the size of the JSON keys and values arrays don't
match:

```sql
-- Error: The number of keys and values must match.
SELECT JSON_OBJECT(['a', 'b'], [10]) AS json_data
```

An error is produced if the array of JSON keys or JSON values is a SQL `NULL`.

```sql
-- Error: The keys array cannot be NULL.
SELECT JSON_OBJECT(CAST(NULL AS ARRAY<STRING>), [10, 20]) AS json_data
```

```sql
-- Error: The values array cannot be NULL.
SELECT JSON_OBJECT(['a', 'b'], CAST(NULL AS ARRAY<INT64>)) AS json_data
```

[json-encodings]: #json_encodings

### `JSON_QUERY`

```sql
JSON_QUERY(json_string_expr, json_path)
```

```sql
JSON_QUERY(json_expr, json_path)
```

**Description**

Extracts a JSON value and converts it to a SQL
JSON-formatted `STRING` or
`JSON` value.
This function uses double quotes to escape invalid
[JSONPath][JSONPath-format] characters in JSON keys. For example: `"a.b"`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class": {"students": [{"name": "Jane"}]}}'
    ```

    Extracts a SQL `NULL` when a JSON-formatted string `null` is encountered.
    For example:

    ```sql
    SELECT JSON_QUERY("null", "$") -- Returns a SQL NULL
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"class": {"students": [{"name": "Jane"}]}}'
    ```

    Extracts a JSON `null` when a JSON `null` is encountered.

    ```sql
    SELECT JSON_QUERY(JSON 'null', "$") -- Returns a JSON 'null'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: A JSON-formatted `STRING`
+ `json_expr`: `JSON`

**Examples**

In the following example, JSON data is extracted and returned as JSON.

```sql
SELECT
  JSON_QUERY(
    JSON '{"class": {"students": [{"id": 5}, {"id": 12}]}}',
    '$.class') AS json_data;

/*-----------------------------------*
 | json_data                         |
 +-----------------------------------+
 | {"students":[{"id":5},{"id":12}]} |
 *-----------------------------------*/
```

In the following examples, JSON data is extracted and returned as
JSON-formatted strings.

```sql
SELECT
  JSON_QUERY('{"class": {"students": [{"name": "Jane"}]}}', '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"Jane"}]}}                  |
 *-----------------------------------------------------------*/
```

```sql
SELECT JSON_QUERY('{"class": {"students": []}}', '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[]}}                                 |
 *-----------------------------------------------------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "John"},{"name": "Jamie"}]}}',
    '$') AS json_text_string;

/*-----------------------------------------------------------*
 | json_text_string                                          |
 +-----------------------------------------------------------+
 | {"class":{"students":[{"name":"John"},{"name":"Jamie"}]}} |
 *-----------------------------------------------------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "Jane"}]}}',
    '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"Jane"} |
 *-----------------*/
```

```sql
SELECT
  JSON_QUERY('{"class": {"students": []}}', '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | NULL            |
 *-----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
    '$.class.students[0]') AS first_student;

/*-----------------*
 | first_student   |
 +-----------------+
 | {"name":"John"} |
 *-----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "Jane"}]}}',
    '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": []}}',
    '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "John"}, {"name": null}]}}',
    '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | NULL           |
 *----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
    '$.class.students[1].name') AS second_student;

/*----------------*
 | second_student |
 +----------------+
 | "Jamie"        |
 *----------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "Jane"}]}}',
    '$.class."students"') AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"Jane"}]                  |
 *------------------------------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": []}}',
    '$.class."students"') AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | []                                 |
 *------------------------------------*/
```

```sql
SELECT
  JSON_QUERY(
    '{"class": {"students": [{"name": "John"}, {"name": "Jamie"}]}}',
    '$.class."students"') AS student_names;

/*------------------------------------*
 | student_names                      |
 +------------------------------------+
 | [{"name":"John"},{"name":"Jamie"}] |
 *------------------------------------*/
```

```sql
SELECT JSON_QUERY('{"a": null}', "$.a"); -- Returns a SQL NULL
SELECT JSON_QUERY('{"a": null}', "$.b"); -- Returns a SQL NULL
```

```sql
SELECT JSON_QUERY(JSON '{"a": null}', "$.a"); -- Returns a JSON 'null'
SELECT JSON_QUERY(JSON '{"a": null}', "$.b"); -- Returns a SQL NULL
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

[JSONPath-mode]: #JSONPath_mode

### `JSON_QUERY_ARRAY`

```sql
JSON_QUERY_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_QUERY_ARRAY(json_expr[, json_path])
```

**Description**

Extracts a JSON array and converts it to
a SQL `ARRAY<JSON-formatted STRING>` or
`ARRAY<JSON>` value.
In addition, this function uses double quotes to escape invalid
[JSONPath][JSONPath-format] characters in JSON keys. For example: `"a.b"`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '["a", "b", {"key": "c"}]'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '["a", "b", {"key": "c"}]'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

+ `json_string_expr`: `ARRAY<JSON-formatted STRING>`
+ `json_expr`: `ARRAY<JSON>`

**Examples**

This extracts items in JSON to an array of `JSON` values:

```sql
SELECT JSON_QUERY_ARRAY(
  JSON '{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits'
  ) AS json_array;

/*---------------------------------*
 | json_array                      |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
SELECT JSON_QUERY_ARRAY('[1, 2, 3]') AS string_array;

/*--------------*
 | string_array |
 +--------------+
 | [1, 2, 3]    |
 *--------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_QUERY_ARRAY('[1, 2, 3]','$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

This extracts string values in a JSON-formatted string to an array:

```sql
-- Doesn't strip the double quotes
SELECT JSON_QUERY_ARRAY('["apples", "oranges", "grapes"]', '$') AS string_array;

/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

```sql
-- Strips the double quotes
SELECT ARRAY(
  SELECT JSON_VALUE(string_element, '$')
  FROM UNNEST(JSON_QUERY_ARRAY('["apples", "oranges", "grapes"]', '$')) AS string_element
) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

This extracts only the items in the `fruit` property to an array:

```sql
SELECT JSON_QUERY_ARRAY(
  '{"fruit": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}',
  '$.fruit'
) AS string_array;

/*-------------------------------------------------------*
 | string_array                                          |
 +-------------------------------------------------------+
 | [{"apples":5,"oranges":10}, {"apples":2,"oranges":4}] |
 *-------------------------------------------------------*/
```

These are equivalent:

```sql
SELECT JSON_QUERY_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits') AS string_array;

SELECT JSON_QUERY_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$."fruits"') AS string_array;

-- The queries above produce the following result:
/*---------------------------------*
 | string_array                    |
 +---------------------------------+
 | ["apples", "oranges", "grapes"] |
 *---------------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_QUERY_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

/*-----------*
 | hello     |
 +-----------+
 | ["world"] |
 *-----------*/
```

The following examples show how invalid requests and empty arrays are handled:

```sql
-- An error is returned if you provide an invalid JSONPath.
SELECT JSON_QUERY_ARRAY('["foo", "bar", "baz"]', 'INVALID_JSONPath') AS result;

-- If the JSONPath does not refer to an array, then NULL is returned.
SELECT JSON_QUERY_ARRAY('{"a": "foo"}', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a key that does not exist is specified, then the result is NULL.
SELECT JSON_QUERY_ARRAY('{"a": "foo"}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- Empty arrays in JSON-formatted strings are supported.
SELECT JSON_QUERY_ARRAY('{"a": "foo", "b": []}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_REMOVE`

```sql
JSON_REMOVE(json_expr, json_path[, ...])
```

Produces a new SQL `JSON` value with the specified JSON data removed.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '{"class": {"students": [{"name": "Jane"}]}}'
    ```
+   `json_path`: Remove data at this [JSONPath][JSONPath-format] in `json_expr`.

Details:

+   Paths are evaluated left to right. The JSON produced by evaluating the
    first path is the JSON for the next path.
+   The operation ignores non-existent paths and continue processing the rest
    of the paths.
+   For each path, the entire matched JSON subtree is deleted.
+   If the path matches a JSON object key, this function deletes the
    key-value pair.
+   If the path matches an array element, this function deletes the specific
    element from the matched array.
+   If removing the path results in an empty JSON object or empty JSON array,
    the empty structure is preserved.
+   If `json_path` is `$` or an invalid [JSONPath][JSONPath-format], an error is
    produced.
+   If `json_path` is SQL `NULL`, the path operation is ignored.

**Return type**

`JSON`

**Examples**

In the following example, the path `$[1]` is matched and removes
`["b", "c"]`.

```sql
SELECT JSON_REMOVE(JSON '["a", ["b", "c"], "d"]', '$[1]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | ["a","d"] |
 *-----------*/
```

You can use the field access operator to pass JSON data into this function.
For example:

```sql
WITH T AS (SELECT JSON '{"a": {"b": 10, "c": 20}}' AS data)
SELECT JSON_REMOVE(data.a, '$.b') AS json_data FROM T

/*-----------*
 | json_data |
 +-----------+
 | {"c":20}  |
 *-----------*/
```

In the following example, the first path `$[1]` is matched and removes
`["b", "c"]`. Then, the second path `$[1]` is matched and removes `"d"`.

```sql
SELECT JSON_REMOVE(JSON '["a", ["b", "c"], "d"]', '$[1]', '$[1]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | ["a"]     |
 *-----------*/
```

The structure of an empty array is preserved when all elements are deleted
from it. For example:

```sql
SELECT JSON_REMOVE(JSON '["a", ["b", "c"], "d"]', '$[1]', '$[1]', '$[0]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | []        |
 *-----------*/
```

In the following example, the path `$.a.b.c` is matched and removes the
`"c":"d"` key-value pair from the JSON object.

```sql
SELECT JSON_REMOVE(JSON '{"a": {"b": {"c": "d"}}}', '$.a.b.c') AS json_data

/*----------------*
 | json_data      |
 +----------------+
 | {"a":{"b":{}}} |
 *----------------*/
```

In the following example, the path `$.a.b` is matched and removes the
`"b": {"c":"d"}` key-value pair from the JSON object.

```sql
SELECT JSON_REMOVE(JSON '{"a": {"b": {"c": "d"}}}', '$.a.b') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":{}}  |
 *-----------*/
```

In the following example, the path `$.b` is not valid, so the operation makes
no changes.

```sql
SELECT JSON_REMOVE(JSON '{"a": 1}', '$.b') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":1}   |
 *-----------*/
```

In the following example, path `$.a.b` and `$.b` don't exist, so those
operations are ignored, but the others are processed.

```sql
SELECT JSON_REMOVE(JSON '{"a": [1, 2, 3]}', '$.a[0]', '$.a.b', '$.b', '$.a[0]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"a":[3]} |
 *-----------*/
```

If you pass in `$` as the path, an error is produced. For example:

```sql
-- Error: The JSONPath cannot be '$'
SELECT JSON_REMOVE(JSON '{}', '$') AS json_data
```

In the following example, the operation is ignored because you can't remove
data from a JSON null.

```sql
SELECT JSON_REMOVE(JSON 'null', '$.a.b') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | null      |
 *-----------*/
```

### `JSON_SET`

```sql
JSON_SET(
  json_expr,
  json_path_value_pair[, ...]
  [, create_if_missing=> { TRUE | FALSE }]
)

json_path_value_pair:
  json_path, value
```

Produces a new SQL `JSON` value with the specified JSON data inserted
or replaced.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '{"class": {"students": [{"name": "Jane"}]}}'
    ```
+   `json_path_value_pair`: A value and the [JSONPath][JSONPath-format] for
    that value. This includes:

    +   `json_path`: Insert or replace `value` at this [JSONPath][JSONPath-format]
        in `json_expr`.

    +   `value`: A [JSON encoding-supported][json-encodings] value to
        insert.
+   `create_if_missing`: An optional, mandatory named argument.

    +   If TRUE (default), replaces or inserts data if the path does not exist.

    +   If FALSE, only _existing_ JSONPath values are replaced. If the path
        doesn't exist, the set operation is ignored.

Details:

+   Path value pairs are evaluated left to right. The JSON produced by
    evaluating one pair becomes the JSON against which the next pair
    is evaluated.
+   If a matched path has an existing value, it overwrites the existing data
    with `value`.
+   If `create_if_missing` is `TRUE`:

      +  If a path doesn't exist, the remainder of the path is recursively
         created.
      +  If the matched path prefix points to a JSON null, the remainder of the
         path is recursively created, and `value` is inserted.
      +  If a path token points to a JSON array and the specified index is
         _larger_ than the size of the array, pads the JSON array with JSON
         nulls, recursively creates the remainder of the path at the specified
         index, and inserts the path value pair.
+   This function applies all path value pair set operations even if an
    individual path value pair operation is invalid. For invalid operations,
    the operation is ignored and the function continues to process the rest
    of the path value pairs.
+   If the path exists but has an incompatible type at any given path
    token, no update happens for that specific path value pair.
+   If any `json_path` is an invalid [JSONPath][JSONPath-format], an error is
    produced.
+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   If `json_path` is SQL `NULL`, the `json_path_value_pair` operation is
    ignored.
+   If `create_if_missing` is SQL `NULL`, the set operation is ignored.

**Return type**

`JSON`

**Examples**

In the following example, the path `$` matches the entire `JSON` value
and replaces it with `{"b": 2, "c": 3}`.

```sql
SELECT JSON_SET(JSON '{"a": 1}', '$', JSON '{"b": 2, "c": 3}') AS json_data

/*---------------*
 | json_data     |
 +---------------+
 | {"b":2,"c":3} |
 *---------------*/
```

In the following example, `create_if_missing` is `FALSE` and the path `$.b`
doesn't exist, so the set operation is ignored.

```sql
SELECT JSON_SET(
  JSON '{"a": 1}',
  "$.b", 999,
  create_if_missing => false) AS json_data

/*------------*
 | json_data  |
 +------------+
 | '{"a": 1}' |
 *------------*/
```

In the following example, `create_if_missing` is `TRUE` and the path `$.a`
exists, so the value is replaced.

```sql
SELECT JSON_SET(
  JSON '{"a": 1}',
  "$.a", 999,
  create_if_missing => false) AS json_data

/*--------------*
 | json_data    |
 +--------------+
 | '{"a": 999}' |
 *--------------*/
```

In the following example, the path `$.a` is matched, but `$.a.b` does not
exist, so the new path and the value are inserted.

```sql
SELECT JSON_SET(JSON '{"a": {}}', '$.a.b', 100) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | {"a":{"b":100}} |
 *-----------------*/
```

In the following example, the path prefix `$` points to a JSON null, so the
remainder of the path is created for the value `100`.

```sql
SELECT JSON_SET(JSON 'null', '$.a.b', 100) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | {"a":{"b":100}} |
 *-----------------*/
```

In the following example, the path `$.a.c` implies that the value at `$.a` is
a JSON object but it's not. This part of the operation is ignored, but the other
parts of the operation are completed successfully.

```sql
SELECT JSON_SET(
  JSON '{"a": 1}',
  '$.b', 2,
  '$.a.c', 100,
  '$.d', 3) AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | {"a":1,"b":2,"d":3} |
 *---------------------*/
```

In the following example, the path `$.a[2]` implies that the value for `$.a` is
an array, but it's not, so the operation is ignored for that value.

```sql
SELECT JSON_SET(
  JSON '{"a": 1}',
  '$.a[2]', 100,
  '$.b', 2) AS json_data

/*---------------*
 | json_data     |
 +---------------+
 | {"a":1,"b":2} |
 *---------------*/
```

In the following example, the path `$[1]` is matched and replaces the
array element value with `foo`.

```sql
SELECT JSON_SET(JSON '["a", ["b", "c"], "d"]', '$[1]', "foo") AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | ["a","foo","d"] |
 *-----------------*/
```

In the following example, the path `$[1][0]` is matched and replaces the
array element value with `foo`.

```sql
SELECT JSON_SET(JSON '["a", ["b", "c"], "d"]', '$[1][0]', "foo") AS json_data

/*-----------------------*
 | json_data             |
 +-----------------------+
 | ["a",["foo","c"],"d"] |
 *-----------------------*/
```

In the following example, the path prefix `$` points to a JSON null, so the
remainder of the path is created. The resulting array is padded with
JSON nulls and appended with `foo`.

```sql
SELECT JSON_SET(JSON 'null', '$[0][3]', "foo")

/*--------------------------*
 | json_data                |
 +--------------------------+
 | [[null,null,null,"foo"]] |
 *--------------------------*/
```

In the following example, the path `$[1]` is matched, the matched array is
extended since `$[1][4]` is larger than the existing array, and then `foo` is
inserted in the array.

```sql
SELECT JSON_SET(JSON '["a", ["b", "c"], "d"]', '$[1][4]', "foo") AS json_data

/*-------------------------------------*
 | json_data                           |
 +-------------------------------------+
 | ["a",["b","c",null,null,"foo"],"d"] |
 *-------------------------------------*/
```

In the following example, the path `$[1][0][0]` implies that the value of
`$[1][0]` is an array, but it is not, so the operation is ignored.

```sql
SELECT JSON_SET(JSON '["a", ["b", "c"], "d"]', '$[1][0][0]', "foo") AS json_data

/*---------------------*
 | json_data           |
 +---------------------+
 | ["a",["b","c"],"d"] |
 *---------------------*/
```

In the following example, the path `$[1][2]` is larger than the length of
the matched array. The array length is extended and the remainder of the path
is recursively created. The operation continues to the path `$[1][2][1]`
and inserts `foo`.

```sql
SELECT JSON_SET(JSON '["a", ["b", "c"], "d"]', '$[1][2][1]', "foo") AS json_data

/*----------------------------------*
 | json_data                        |
 +----------------------------------+
 | ["a",["b","c",[null,"foo"]],"d"] |
 *----------------------------------*/
```

In the following example, because the `JSON` object is empty, key `b` is
inserted, and the remainder of the path is recursively created.

```sql
SELECT JSON_SET(JSON '{}', '$.b[2].d', 100) AS json_data

/*-----------------------------*
 | json_data                   |
 +-----------------------------+
 | {"b":[null,null,{"d":100}]} |
 *-----------------------------*/
```

In the following example, multiple values are set.

```sql
SELECT JSON_SET(
  JSON '{"a": 1, "b": {"c":3}, "d": [4]}',
  '$.a', 'v1',
  '$.b.e', 'v2',
  '$.d[2]', 'v3') AS json_data

/*---------------------------------------------------*
 | json_data                                         |
 +---------------------------------------------------+
 | {"a":"v1","b":{"c":3,"e":"v2"},"d":[4,null,"v3"]} |
 *---------------------------------------------------*/
```

### `JSON_STRIP_NULLS`

```sql
JSON_STRIP_NULLS(
  json_expr
  [, json_path]
  [, include_arrays => { TRUE | FALSE }]
  [, remove_empty => { TRUE | FALSE }]
)
```

Recursively removes JSON nulls from JSON objects and JSON arrays.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '{"a": null, "b": "c"}'
    ```
+   `json_path`: Remove JSON nulls at this [JSONPath][JSONPath-format] for
    `json_expr`.
+   `include_arrays`: An optional, mandatory named argument that is either
     `TRUE` (default) or `FALSE`. If `TRUE` or omitted, the function removes
     JSON nulls from JSON arrays. If `FALSE`, does not.
+   `remove_empty`: An optional, mandatory named argument that is either
     `TRUE` or `FALSE` (default). If `TRUE`, the function removes empty
     JSON objects after JSON nulls are removed. If `FALSE` or omitted, does not.

    If `remove_empty` is `TRUE` and `include_arrays` is `TRUE` or omitted,
    the function additionally removes empty JSON arrays.

Details:

+   If a value is a JSON null, the associated key-value pair is removed.
+   If `remove_empty` is set to `TRUE`, the function recursively removes empty
    containers after JSON nulls are removed.
+   If the function generates JSON with nothing in it, the function returns a
    JSON null.
+   If `json_path` is an invalid [JSONPath][JSONPath-format], an error is
    produced.
+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   If `json_path`, `include_arrays`, or `remove_empty` is SQL `NULL`, the
    function returns `json_expr`.

**Return type**

`JSON`

**Examples**

In the following example, all JSON nulls are removed.

```sql
SELECT JSON_STRIP_NULLS(JSON '{"a": null, "b": "c"}') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"b":"c"} |
 *-----------*/
```

In the following example, all JSON nulls are removed from a JSON array.

```sql
SELECT JSON_STRIP_NULLS(JSON '[1, null, 2, null]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [1,2]     |
 *-----------*/
```

In the following example, `include_arrays` is set as `FALSE` so that JSON nulls
are not removed from JSON arrays.

```sql
SELECT JSON_STRIP_NULLS(JSON '[1, null, 2, null]', include_arrays=>FALSE) AS json_data

/*-----------------*
 | json_data       |
 +-----------------+
 | [1,null,2,null] |
 *-----------------*/
```

In the following example, `remove_empty` is omitted and defaults to
`FALSE`, and the empty structures are retained.

```sql
SELECT JSON_STRIP_NULLS(JSON '[1, null, 2, null, [null]]') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [1,2,[]]  |
 *-----------*/
```

In the following example, `remove_empty` is set as `TRUE`, and the
empty structures are removed.

```sql
SELECT JSON_STRIP_NULLS(
  JSON '[1, null, 2, null, [null]]',
  remove_empty=>TRUE) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | [1,2]     |
 *-----------*/
```

In the following examples, `remove_empty` is set as `TRUE`, and the
empty structures are removed. Because no JSON data is left the function
returns JSON null.

```sql
SELECT JSON_STRIP_NULLS(JSON '{"a": null}', remove_empty=>TRUE) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | null      |
 *-----------*/
```

```sql
SELECT JSON_STRIP_NULLS(JSON '{"a": [null]}', remove_empty=>TRUE) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | null      |
 *-----------*/
```

In the following example, empty structures are removed for JSON objects,
but not JSON arrays.

```sql
SELECT JSON_STRIP_NULLS(
  JSON '{"a": {"b": {"c": null}}, "d": [null], "e": [], "f": 1}',
  include_arrays=>FALSE,
  remove_empty=>TRUE) AS json_data

/*---------------------------*
 | json_data                 |
 +---------------------------+
 | {"d":[null],"e":[],"f":1} |
 *---------------------------*/
```

In the following example, empty structures are removed for both JSON objects,
and JSON arrays.

```sql
SELECT JSON_STRIP_NULLS(
  JSON '{"a": {"b": {"c": null}}, "d": [null], "e": [], "f": 1}',
  remove_empty=>TRUE) AS json_data

/*-----------*
 | json_data |
 +-----------+
 | {"f":1}   |
 *-----------*/
```

In the following example, because no JSON data is left, the function returns a
JSON null.

```sql
SELECT JSON_STRIP_NULLS(JSON 'null') AS json_data

/*-----------*
 | json_data |
 +-----------+
 | null      |
 *-----------*/
```

### `JSON_TYPE` 
<a id="json_type"></a>

```sql
JSON_TYPE(json_expr)
```

**Description**

Gets the JSON type of the outermost JSON value and converts the name of
this type to a SQL `STRING` value. The names of these JSON types can be
returned: `object`, `array`, `string`, `number`, `boolean`, `null`

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "sky", "color": "blue"}'
    ```

    If this expression is SQL `NULL`, the function returns SQL `NULL`. If the
    extracted JSON value is not a valid JSON type, an error is produced.

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

/*----------------------------------+---------*
 | json_val                         | type    |
 +----------------------------------+---------+
 | "apple"                          | string  |
 | 10                               | number  |
 | 3.14                             | number  |
 | null                             | null    |
 | {"State":"NY","city":"New York"} | object  |
 | ["apple","banana"]               | array   |
 | false                            | boolean |
 *----------------------------------+---------*/
```

### `JSON_VALUE`

```sql
JSON_VALUE(json_string_expr[, json_path])
```

```sql
JSON_VALUE(json_expr[, json_path])
```

**Description**

Extracts a JSON scalar value and converts it to a SQL `STRING` value.
In addition, this function:

+   Removes the outermost quotes and unescapes the values.
+   Returns a SQL `NULL` if a non-scalar value is selected.
+   Uses double quotes to escape invalid [JSONPath][JSONPath-format] characters
    in JSON keys. For example: `"a.b"`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"name": "Jakob", "age": "6"}'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '{"name": "Jane", "age": "6"}'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

    If `json_path` returns a JSON `null` or a non-scalar value (in other words,
    if `json_path` refers to an object or an array), then a SQL `NULL` is
    returned.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

**Return type**

`STRING`

**Examples**

In the following example, JSON data is extracted and returned as a scalar value.

```sql
SELECT JSON_VALUE(JSON '{"name": "Jakob", "age": "6" }', '$.age') AS scalar_age;

/*------------*
 | scalar_age |
 +------------+
 | 6          |
 *------------*/
```

The following example compares how results are returned for the `JSON_QUERY`
and `JSON_VALUE` functions.

```sql
SELECT JSON_QUERY('{"name": "Jakob", "age": "6"}', '$.name') AS json_name,
  JSON_VALUE('{"name": "Jakob", "age": "6"}', '$.name') AS scalar_name,
  JSON_QUERY('{"name": "Jakob", "age": "6"}', '$.age') AS json_age,
  JSON_VALUE('{"name": "Jakob", "age": "6"}', '$.age') AS scalar_age;

/*-----------+-------------+----------+------------*
 | json_name | scalar_name | json_age | scalar_age |
 +-----------+-------------+----------+------------+
 | "Jakob"   | Jakob       | "6"      | 6          |
 *-----------+-------------+----------+------------*/
```

```sql
SELECT JSON_QUERY('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_query,
  JSON_VALUE('{"fruits": ["apple", "banana"]}', '$.fruits') AS json_value;

/*--------------------+------------*
 | json_query         | json_value |
 +--------------------+------------+
 | ["apple","banana"] | NULL       |
 *--------------------+------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes. For example:

```sql
SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c') AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `JSON_VALUE_ARRAY`

```sql
JSON_VALUE_ARRAY(json_string_expr[, json_path])
```

```sql
JSON_VALUE_ARRAY(json_expr[, json_path])
```

**Description**

Extracts a JSON array of scalar values and converts it to a SQL
`ARRAY<STRING>` value.
In addition, this function:

+   Removes the outermost quotes and unescapes the values.
+   Returns a SQL `NULL` if the selected value is not an array or
    not an array containing only scalar values.
+   Uses double quotes to escape invalid [JSONPath][JSONPath-format] characters
    in JSON keys. For example: `"a.b"`.

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '["apples", "oranges", "grapes"]'
    ```
+   `json_expr`: JSON. For example:

    ```
    JSON '["apples", "oranges", "grapes"]'
    ```
+   `json_path`: The [JSONPath][JSONPath-format]. This identifies the data that
    you want to obtain from the input. If this optional parameter is not
    provided, then the JSONPath `$` symbol is applied, which means that all of
    the data is analyzed.

There are differences between the JSON-formatted string and JSON input types.
For details, see [Differences between the JSON and JSON-formatted STRING types][differences-json-and-string].

Caveats:

+ A JSON `null` in the input array produces a SQL `NULL` as the output for that
  JSON `null`.
+ If a JSONPath matches an array that contains scalar objects and a JSON `null`,
  then the output is an array of the scalar objects and a SQL `NULL`.

**Return type**

`ARRAY<STRING>`

**Examples**

This extracts items in JSON to a string array:

```sql
SELECT JSON_VALUE_ARRAY(
  JSON '{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits'
  ) AS string_array;

/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

The following example compares how results are returned for the
`JSON_QUERY_ARRAY` and `JSON_VALUE_ARRAY` functions.

```sql
SELECT JSON_QUERY_ARRAY('["apples", "oranges"]') AS json_array,
       JSON_VALUE_ARRAY('["apples", "oranges"]') AS string_array;

/*-----------------------+-------------------*
 | json_array            | string_array      |
 +-----------------------+-------------------+
 | ["apples", "oranges"] | [apples, oranges] |
 *-----------------------+-------------------*/
```

This extracts the items in a JSON-formatted string to a string array:

```sql
-- Strips the double quotes
SELECT JSON_VALUE_ARRAY('["foo", "bar", "baz"]', '$') AS string_array;

/*-----------------*
 | string_array    |
 +-----------------+
 | [foo, bar, baz] |
 *-----------------*/
```

This extracts a string array and converts it to an integer array:

```sql
SELECT ARRAY(
  SELECT CAST(integer_element AS INT64)
  FROM UNNEST(
    JSON_VALUE_ARRAY('[1, 2, 3]', '$')
  ) AS integer_element
) AS integer_array;

/*---------------*
 | integer_array |
 +---------------+
 | [1, 2, 3]     |
 *---------------*/
```

These are equivalent:

```sql
SELECT JSON_VALUE_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$.fruits') AS string_array;
SELECT JSON_VALUE_ARRAY('{"fruits": ["apples", "oranges", "grapes"]}', '$."fruits"') AS string_array;

-- The queries above produce the following result:
/*---------------------------*
 | string_array              |
 +---------------------------+
 | [apples, oranges, grapes] |
 *---------------------------*/
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes: `" "`. For example:

```sql
SELECT JSON_VALUE_ARRAY('{"a.b": {"c": ["world"]}}', '$."a.b".c') AS hello;

/*---------*
 | hello   |
 +---------+
 | [world] |
 *---------*/
```

The following examples explore how invalid requests and empty arrays are
handled:

```sql
-- An error is thrown if you provide an invalid JSONPath.
SELECT JSON_VALUE_ARRAY('["foo", "bar", "baz"]', 'INVALID_JSONPath') AS result;

-- If the JSON-formatted string is invalid, then NULL is returned.
SELECT JSON_VALUE_ARRAY('}}', '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If the JSON document is NULL, then NULL is returned.
SELECT JSON_VALUE_ARRAY(NULL, '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath does not match anything, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a": ["foo", "bar", "baz"]}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an object that is not an array, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a": "foo"}', '$') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of non-scalar objects, then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a": [{"b": "foo", "c": 1}, {"b": "bar", "c": 2}], "d": "baz"}', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an array of mixed scalar and non-scalar objects,
-- then the output is NULL.
SELECT JSON_VALUE_ARRAY('{"a": [10, {"b": 20}]', '$.a') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

-- If a JSONPath matches an empty JSON array, then the output is an empty array instead of NULL.
SELECT JSON_VALUE_ARRAY('{"a": "foo", "b": []}', '$.b') AS result;

/*--------*
 | result |
 +--------+
 | []     |
 *--------*/

-- In the following query, the JSON null input is returned as a
-- SQL NULL in the output.
SELECT JSON_VALUE_ARRAY('["world", null, 1]') AS result;

/*------------------*
 | result           |
 +------------------+
 | [world, NULL, 1] |
 *------------------*/

```

[JSONPath-format]: #JSONPath_format

[differences-json-and-string]: #differences_json_and_string

### `LAX_BOOL` 
<a id="lax_bool"></a>

```sql
LAX_BOOL(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `BOOL` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON 'true'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>BOOL</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>TRUE</code>.
      Otherwise, returns <code>FALSE</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string is <code>'true'</code>, returns <code>TRUE</code>.
      If the JSON string is <code>'false'</code>, returns <code>FALSE</code>.
      If the JSON string is any other value or has whitespace in it,
      returns <code>NULL</code>.
      This conversion is case-insensitive.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      If the JSON number is a representation of <code>0</code>,
      returns <code>FALSE</code>. Otherwise, returns <code>TRUE</code>.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`BOOL`

**Examples**

Example with input that is a JSON boolean:

```sql
SELECT LAX_BOOL(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | true   |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_BOOL(JSON '"true"') AS result;

/*--------*
 | result |
 +--------+
 | TRUE   |
 *--------*/
```

```sql
SELECT LAX_BOOL(JSON '"true "') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_BOOL(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_BOOL(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | TRUE   |
 *--------*/
```

```sql
SELECT LAX_BOOL(JSON '0') AS result;

/*--------*
 | result |
 +--------+
 | FALSE  |
 *--------*/
```

```sql
SELECT LAX_BOOL(JSON '0.0') AS result;

/*--------*
 | result |
 +--------+
 | FALSE  |
 *--------*/
```

```sql
SELECT LAX_BOOL(JSON '-1.1') AS result;

/*--------*
 | result |
 +--------+
 | TRUE   |
 *--------*/
```

### `LAX_BOOL_ARRAY` 
<a id="lax_bool_array"></a>

```sql
LAX_BOOL_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<BOOL>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[true]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;BOOL&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to <a href="#lax_bool"><code>LAX_BOOL</code></a> conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<BOOL>`

**Examples**

Example with input that is a JSON array of booleans:

```sql
SELECT LAX_BOOL_ARRAY(JSON '[true, false]') AS result;

/*---------------*
 | result        |
 +---------------+
 | [true, false] |
 *---------------*/
```

Examples with inputs that are JSON arrays of strings:

```sql
SELECT LAX_BOOL_ARRAY(JSON '["true", "false", "TRue", "FaLse"]') AS result;

/*----------------------------*
 | result                     |
 +----------------------------+
 | [true, false, true, false] |
 *----------------------------*/
```

```sql
SELECT LAX_BOOL_ARRAY(JSON '["true ", "foo", "null", ""]') AS result;

/*-------------------------*
 | result                  |
 +-------------------------+
 | [NULL, NULL, NULL, NULL |
 *-------------------------*/
```

Examples with input that is JSON array of numbers:

```sql
SELECT LAX_BOOL_ARRAY(JSON '[10, 0, 0.0, -1.1]') AS result;

/*--------------------------*
 | result                   |
 +--------------------------+
 | TRUE, FALSE, FALSE, TRUE |
 *--------------------------*/
```

Example with input that is JSON array of other types:

```sql
SELECT LAX_BOOL_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_BOOL_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_BOOL_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_BOOL_ARRAY(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_DOUBLE` 
<a id="lax_double"></a>

```sql
LAX_DOUBLE(json_expr)
```

**Description**

Attempts to convert a JSON value to a
SQL `DOUBLE` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '9.8'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>DOUBLE</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      <code>NULL</code>
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the result as a
      <code>DOUBLE</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as a
      <code>DOUBLE</code> value.
      Large JSON numbers are rounded.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`DOUBLE`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_DOUBLE(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | 9.8    |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '9') AS result;

/*--------*
 | result |
 +--------+
 | 9.0    |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '9007199254740993') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | 9007199254740992.0 |
 *--------------------*/
```

```sql
SELECT LAX_DOUBLE(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | 1e+100 |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_DOUBLE(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_DOUBLE(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10.0   |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1.1    |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110.0  |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"9007199254740993"') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | 9007199254740992.0 |
 *--------------------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 1.5    |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"NaN"') AS result;

/*--------*
 | result |
 +--------+
 | NaN    |
 *--------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"Inf"') AS result;

/*----------*
 | result   |
 +----------+
 | Infinity |
 *----------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"-InfiNiTY"') AS result;

/*-----------*
 | result    |
 +-----------+
 | -Infinity |
 *-----------*/
```

```sql
SELECT LAX_DOUBLE(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_DOUBLE_ARRAY` 
<a id="lax_double_array"></a>

```sql
LAX_DOUBLE_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<DOUBLE>`
value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[9.8]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;DOUBLE&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to
      <a href="#lax_double"><code>LAX_DOUBLE</code></a>
      conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<DOUBLE>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '[9.8, 9]') AS result;

/*-------------*
 | result      |
 +-------------+
 | [9.8, 9.0,] |
 *-------------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '[9007199254740993, -9007199254740993]') AS result;

/*-------------------------------------------*
 | result                                    |
 +-------------------------------------------+
 | [9007199254740992.0, -9007199254740992.0] |
 *-------------------------------------------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '[-1.79769e+308, 2.22507e-308, 1.79769e+308, 1e100]') AS result;

/*-----------------------------------------------------*
 | result                                              |
 +-----------------------------------------------------+
 | [-1.79769e+308, 2.22507e-308, 1.79769e+308, 1e+100] |
 *-----------------------------------------------------*/
```

Example with inputs that is JSON array of booleans:

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '[true, false]') AS result;

/*----------------*
 | result         |
 +----------------+
 | [NULL, NULL]   |
 *----------------*/
```

Examples with inputs that are JSON arrays of strings:

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-------------------------*
 | result                  |
 +-------------------------+
 | [10.0, 1.1, 110.0, 1.5] |
 *-------------------------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '["9007199254740993"]') AS result;

/*----------------------*
 | result               |
 +----------------------+
 | [9007199254740992.0] |
 *----------------------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '["NaN", "Inf", "-InfiNiTY"]') AS result;

/*----------------------------*
 | result                     |
 +----------------------------+
 | [NaN, Infinity, -Infinity] |
 *----------------------------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is JSON array of other types:

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_DOUBLE_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_DOUBLE_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_FLOAT` 
<a id="lax_float"></a>

```sql
LAX_FLOAT(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `FLOAT` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '9.8'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>FLOAT</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      <code>NULL</code>
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the result as a
      <code>FLOAT</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as a
      <code>FLOAT</code> value.
      Large JSON numbers are rounded.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`FLOAT`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_FLOAT(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | 9.8    |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '9') AS result;

/*--------*
 | result |
 +--------+
 | 9.0    |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '16777217') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | 16777216.0         |
 *--------------------*/
```

```sql
SELECT LAX_FLOAT(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_FLOAT(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_FLOAT(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10.0   |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1.1    |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110.0  |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '"16777217"') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | 16777216.0         |
 *--------------------*/
```

```sql
SELECT LAX_FLOAT(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 1.5    |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '"NaN"') AS result;

/*--------*
 | result |
 +--------+
 | NaN    |
 *--------*/
```

```sql
SELECT LAX_FLOAT(JSON '"Inf"') AS result;

/*----------*
 | result   |
 +----------+
 | Infinity |
 *----------*/
```

```sql
SELECT LAX_FLOAT(JSON '"-InfiNiTY"') AS result;

/*-----------*
 | result    |
 +-----------+
 | -Infinity |
 *-----------*/
```

```sql
SELECT LAX_FLOAT(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_FLOAT_ARRAY` 
<a id="lax_float_array"></a>

```sql
LAX_FLOAT_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<FLOAT>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[9.8, 9]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;FLOAT&gt;</code></th>
  </tr>
<tr>
    <td>array</td>
    <td>
      Converts every element according to
      <a href="#lax_float"><code>LAX_FLOAT_ARRAY</code></a>
      conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<FLOAT>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[9.8, 9]') AS result;

/*------------*
 | result     |
 +------------+
 | [9.8, 9.0] |
 *------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[16777217, -16777217]') AS result;

/*---------------------------*
 | result                    |
 +---------------------------+
 | [16777216.0, -16777216.0] |
 *---------------------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[-3.40282e+38, 1.17549e-38, 3.40282e+38]') AS result;

/*------------------------------------------*
 | result                                   |
 +------------------------------------------+
 | [-3.40282e+38, 1.17549e-38, 3.40282e+38] |
 *------------------------------------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[-1.79769e+308, 2.22507e-308, 1.79769e+308, 1e100]') AS result;

/*-----------------------*
 | result                |
 +-----------------------+
 | [NULL, 0, NULL, NULL] |
 *-----------------------*/
```

Example with inputs that is JSON array of booleans:

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[true, false]') AS result;

/*----------------*
 | result         |
 +----------------+
 | [NULL, NULL]   |
 *----------------*/
```

Examples with inputs that are JSON arrays of strings:

```sql
SELECT LAX_FLOAT_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-------------------------*
 | result                  |
 +-------------------------+
 | [10.0, 1.1, 110.0, 1.5] |
 *------------------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '["16777217"]') AS result;

/*--------------*
 | result       |
 +--------------+
 | [16777216.0] |
 *--------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '["NaN", "Inf", "-InfiNiTY"]') AS result;

/*----------------------------*
 | result                     |
 +----------------------------+
 | [NaN, Infinity, -Infinity] |
 *----------------------------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is JSON array of other types:

```sql
SELECT LAX_FLOAT_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_FLOAT_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_FLOAT_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_INT32` 
<a id="lax_int32"></a>

```sql
LAX_INT32(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `INT32` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>INT32</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>1</code>.
      If <code>false</code>, returns <code>0</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the results as an
      <code>INT32</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as an <code>INT32</code> value.
      If the JSON number can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`INT32`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_INT32(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '10.0') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '1.1') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '3.5') AS result;

/*--------*
 | result |
 +--------+
 | 4      |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '1.1e2') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_INT32(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | 0      |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_INT32(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 2      |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '"1e100"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT32(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_INT32_ARRAY` 
<a id="lax_int32_array"></a>

```sql
LAX_INT32_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<INT32>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;INT32&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to <a href="#lax_int32"><code>LAX_INT32</code></a> conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<INT32>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_INT32_ARRAY(JSON '[10, 10.0, 1.1, 3.5, 1.1e2]') AS result;

/*---------------------*
 | result              |
 +---------------------+
 | [10, 10, 1, 4, 110] |
 *---------------- ----*/
```

```sql
SELECT LAX_INT32_ARRAY(JSON '[1e100]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

Example with inputs that is JSON array of booleans:

```sql
SELECT LAX_INT32_ARRAY(JSON '[true, false]') AS result;

/*--------*
 | result |
 +--------+
 | [1, 0] |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_INT32_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-----------------*
 | result          |
 +-----------------+
 | [10, 1, 110, 2] |
 *-----------------*/
```

```sql
SELECT LAX_INT32_ARRAY(JSON '["1e100"]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

```sql
SELECT LAX_INT32_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is JSON array of other types:

```sql
SELECT LAX_INT32_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_INT32_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT32_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT32_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_INT64` 
<a id="lax_int64"></a>

```sql
LAX_INT64(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `INT64` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>INT64</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>1</code>.
      If <code>false</code>, returns <code>0</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the results as an
      <code>INT64</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as an <code>INT64</code> value.
      If the JSON number can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`INT64`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_INT64(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '10.0') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '1.1') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '3.5') AS result;

/*--------*
 | result |
 +--------+
 | 4      |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '1.1e2') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_INT64(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | 0      |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_INT64(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 2      |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '"1e100"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT64(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_INT64_ARRAY` 
<a id="lax_int64_array"></a>

```sql
LAX_INT64_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<INT64>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;INT64&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to <a href="#lax_int64"><code>LAX_INT64</code></a> conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<INT64>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_INT64_ARRAY(JSON '[10, 10.0, 1.1, 3.5, 1.1e2]') AS result;

/*---------------------*
 | result              |
 +---------------------+
 | [10, 10, 1, 4, 110] |
 *---------------------*/
```

```sql
SELECT LAX_INT64_ARRAY(JSON '[1e100]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

Example with inputs that is JSON array of booleans:

```sql
SELECT LAX_INT64_ARRAY(JSON '[true, false]') AS result;

/*--------*
 | result |
 +--------+
 | [1, 0] |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_INT64_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-----------------*
 | result          |
 +-----------------+
 | [10, 1, 110, 2] |
 *-----------------*/
```

```sql
SELECT LAX_INT64_ARRAY(JSON '["1e100"]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

```sql
SELECT LAX_INT64_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is JSON array of other types:

```sql
SELECT LAX_INT64_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_INT64_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT64_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_INT64_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_STRING` 
<a id="lax_string"></a>

```sql
LAX_STRING(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `STRING` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '"name"'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>STRING</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>'true'</code>.
      If <code>false</code>, returns <code>'false'</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      Returns the JSON string as a <code>STRING</code> value.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Returns the JSON number as a <code>STRING</code> value.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`STRING`

**Examples**

Examples with inputs that are JSON strings:

```sql
SELECT LAX_STRING(JSON '"purple"') AS result;

/*--------*
 | result |
 +--------+
 | purple |
 *--------*/
```

```sql
SELECT LAX_STRING(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_STRING(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | true   |
 *--------*/
```

```sql
SELECT LAX_STRING(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | false  |
 *--------*/
```

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_STRING(JSON '10.0') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_STRING(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_STRING(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | 1e+100 |
 *--------*/
```

### `LAX_STRING_ARRAY` 
<a id="lax_string_array"></a>

```sql
LAX_STRING_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<STRING>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '["a", "b"]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>STRING</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to <a href="#lax_string"><code>LAX_STRING</code></a> conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<STRING>`

**Examples**

Example with input that is a JSON array of strings:

```sql
SELECT LAX_STRING_ARRAY(JSON '["purple", "10"]') AS result;

/*--------------*
 | result       |
 +--------------+
 | [purple, 10] |
 *--------------*/
```

Example with input that is a JSON array of booleans:

```sql
SELECT LAX_STRING_ARRAY(JSON '[true, false]') AS result;

/*---------------*
 | result        |
 +---------------+
 | [true, false] |
 *---------------*/
```

Example with input that is a JSON array of numbers:

```sql
SELECT LAX_STRING_ARRAY(JSON '[10.0, 10, 1e100]') AS result;

/*------------------*
 | result           |
 +------------------+
 | [10, 10, 1e+100] |
 *------------------*/
```

Example with input that is a JSON array of other types:

```sql
SELECT LAX_STRING_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_STRING_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_STRING_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_STRING_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_UINT32` 
<a id="lax_uint32"></a>

```sql
LAX_UINT32(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `UINT32` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>UINT32</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>1</code>.
      If <code>false</code>, returns <code>0</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the results as an
      <code>UINT32</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as an <code>UINT32</code> value.
      If the JSON number can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`UINT32`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_UINT32(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '10.0') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '1.1') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '3.5') AS result;

/*--------*
 | result |
 +--------+
 | 4      |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '1.1e2') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '-1') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_UINT32(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | 0      |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_UINT32(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 2      |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '"1e100"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT32(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_UINT32_ARRAY` 
<a id="lax_uint32_array"></a>

```sql
LAX_UINT32_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<UINT32>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;UINT32&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to
      <a href="#lax_uint32"><code>LAX_UINT32</code></a>
      conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<UINT32>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_UINT32_ARRAY(JSON '[10, 10.0, 1.1, 3.5, 1.1e2]') AS result;

/*---------------------*
 | result              |
 +---------------------+
 | [10, 10, 1, 4, 110] |
 *---------------------*/
```

```sql
SELECT LAX_UINT32_ARRAY(JSON '[1e100]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

Example with inputs that is a JSON array of booleans:

```sql
SELECT LAX_UINT32_ARRAY(JSON '[true, false]') AS result;

/*--------*
 | result |
 +--------+
 | [1, 0] |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_UINT32_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-----------------*
 | result          |
 +-----------------+
 | [10, 1, 110, 2] |
 *-----------------*/
```

```sql
SELECT LAX_UINT32_ARRAY(JSON '["1e100"]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

```sql
SELECT LAX_UINT32_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is a JSON array of other types:

```sql
SELECT LAX_UINT32_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_UINT32_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT32_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT32_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_UINT64` 
<a id="lax_uint64"></a>

```sql
LAX_UINT64(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `UINT64` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>UINT64</code></th>
  </tr>
  <tr>
    <td>boolean</td>
    <td>
      If the JSON boolean is <code>true</code>, returns <code>1</code>.
      If <code>false</code>, returns <code>0</code>.
    </td>
  </tr>
  <tr>
    <td>string</td>
    <td>
      If the JSON string represents a JSON number, parses it as
      a <code>BIGNUMERIC</code> value, and then safe casts the results as an
      <code>UINT64</code> value.
      If the JSON string can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>number</td>
    <td>
      Casts the JSON number as an <code>UINT64</code> value.
      If the JSON number can't be converted, returns <code>NULL</code>.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`UINT64`

**Examples**

Examples with inputs that are JSON numbers:

```sql
SELECT LAX_UINT64(JSON '10') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '10.0') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '1.1') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '3.5') AS result;

/*--------*
 | result |
 +--------+
 | 4      |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '1.1e2') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '-1') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '1e100') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

Examples with inputs that are JSON booleans:

```sql
SELECT LAX_UINT64(JSON 'true') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON 'false') AS result;

/*--------*
 | result |
 +--------+
 | 0      |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_UINT64(JSON '"10"') AS result;

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '"1.1"') AS result;

/*--------*
 | result |
 +--------+
 | 1      |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '"1.1e2"') AS result;

/*--------*
 | result |
 +--------+
 | 110    |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '"+1.5"') AS result;

/*--------*
 | result |
 +--------+
 | 2      |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '"1e100"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT64(JSON '"foo"') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `LAX_UINT64_ARRAY` 
<a id="lax_uint64_array"></a>

```sql
LAX_UINT64_ARRAY(json_expr)
```

**Description**

Attempts to convert a JSON value to a SQL `ARRAY<UINT64>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

Details:

+   If `json_expr` is SQL `NULL`, the function returns SQL `NULL`.
+   See the conversion rules in the next section for additional `NULL` handling.

**Conversion rules**

<table>
  <tr>
    <th width='200px'>From JSON type</th>
    <th>To SQL <code>ARRAY&lt;UINT64&gt;</code></th>
  </tr>
  <tr>
    <td>array</td>
    <td>
      Converts every element according to <a href="#lax_uint64"><code>LAX_UINT64</code></a> conversion rules.
    </td>
  </tr>
  <tr>
    <td>other type or null</td>
    <td><code>NULL</code></td>
  </tr>
</table>

**Return type**

`ARRAY<UINT64>`

**Examples**

Examples with inputs that are JSON arrays of numbers:

```sql
SELECT LAX_UINT64_ARRAY(JSON '[10, 10.0, 1.1, 3.5, 1.1e2]') AS result;

/*---------------------*
 | result              |
 +---------------------+
 | [10, 10, 1, 4, 110] |
 *---------------------*/
```

```sql
SELECT LAX_UINT64_ARRAY(JSON '[1e100]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

Example with inputs that is a JSON array of booleans:

```sql
SELECT LAX_UINT64_ARRAY(JSON '[true, false]') AS result;

/*--------*
 | result |
 +--------+
 | [1, 0] |
 *--------*/
```

Examples with inputs that are JSON strings:

```sql
SELECT LAX_UINT64_ARRAY(JSON '["10", "1.1", "1.1e2", "+1.5"]') AS result;

/*-----------------*
 | result          |
 +-----------------+
 | [10, 1, 110, 2] |
 *-----------------*/
```

```sql
SELECT LAX_UINT64_ARRAY(JSON '["1e100"]') AS result;

/*--------*
 | result |
 +--------+
 | [NULL] |
 *--------*/
```

```sql
SELECT LAX_UINT64_ARRAY(JSON '["foo", "null", ""]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Example with input that is a JSON array of other types:

```sql
SELECT LAX_UINT64_ARRAY(JSON '[null, {"foo": 1}, [1]]') AS result;

/*--------------------*
 | result             |
 +--------------------+
 | [NULL, NULL, NULL] |
 *--------------------*/
```

Examples with inputs that are not JSON arrays:

```sql
SELECT LAX_UINT64_ARRAY(NULL) AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT64_ARRAY(JSON 'null') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

```sql
SELECT LAX_UINT64_ARRAY(JSON '9.8') AS result;

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/
```

### `PARSE_JSON`

```sql
PARSE_JSON(json_string_expr[, wide_number_mode=>{ 'exact' | 'round' }])
```

**Description**

Converts a JSON-formatted `STRING` value to a [`JSON` value](https://www.json.org/json-en.html).

Arguments:

+   `json_string_expr`: A JSON-formatted string. For example:

    ```
    '{"class": {"students": [{"name": "Jane"}]}}'
    ```
+   `wide_number_mode`: Optional mandatory-named argument that determines how to
    handle numbers that cannot be stored in a `JSON` value without the loss of
    precision. If used, `wide_number_mode` must include one of these values:

    +   `exact` (default): Only accept numbers that can be stored without loss
        of precision. If a number that cannot be stored without loss of
        precision is encountered, the function throws an error.
    +   `round`: If a number that cannot be stored without loss of precision is
        encountered, attempt to round it to a number that can be stored without
        loss of precision. If the number cannot be rounded, the function throws
        an error.

    If a number appears in a JSON object or array, the `wide_number_mode`
    argument is applied to the number in the object or array.

Numbers from the following domains can be stored in JSON without loss of
precision:

+ 64-bit signed/unsigned integers, such as `INT64`
+ `DOUBLE`

**Return type**

`JSON`

**Examples**

In the following example, a JSON-formatted string is converted to `JSON`.

```sql
SELECT PARSE_JSON('{"coordinates": [10, 20], "id": 1}') AS json_data;

/*--------------------------------*
 | json_data                      |
 +--------------------------------+
 | {"coordinates":[10,20],"id":1} |
 *--------------------------------*/
```

The following queries fail because:

+ The number that was passed in cannot be stored without loss of precision.
+ `wide_number_mode=>'exact'` is used implicitly in the first query and
  explicitly in the second query.

```sql
SELECT PARSE_JSON('{"id": 922337203685477580701}') AS json_data; -- fails
SELECT PARSE_JSON('{"id": 922337203685477580701}', wide_number_mode=>'exact') AS json_data; -- fails
```

The following query rounds the number to a number that can be stored in JSON.

```sql
SELECT PARSE_JSON('{"id": 922337203685477580701}', wide_number_mode=>'round') AS json_data;

/*------------------------------*
 | json_data                    |
 +------------------------------+
 | {"id":9.223372036854776e+20} |
 *------------------------------*/
```

You can also use valid JSON-formatted strings that don't represent name/value pairs. For example:

```sql
SELECT PARSE_JSON('6') AS json_data;

/*------------------------------*
 | json_data                    |
 +------------------------------+
 | 6                            |
 *------------------------------*/
```

```sql
SELECT PARSE_JSON('"red"') AS json_data;

/*------------------------------*
 | json_data                    |
 +------------------------------+
 | "red"                        |
 *------------------------------*/
```

### `STRING` 
<a id="string_for_json"></a>

```sql
STRING(json_expr)
```

**Description**

Converts a JSON string to a SQL `STRING` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '"purple"'
    ```

    If the JSON value is not a string, an error is produced. If the expression
    is SQL `NULL`, the function returns SQL `NULL`.

**Return type**

`STRING`

**Examples**

```sql
SELECT STRING(JSON '"purple"') AS color;

/*--------*
 | color  |
 +--------+
 | purple |
 *--------*/
```

```sql
SELECT STRING(JSON_QUERY(JSON '{"name": "sky", "color": "blue"}', "$.color")) AS color;

/*-------*
 | color |
 +-------+
 | blue  |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not of type string.
SELECT STRING(JSON '123') AS result; -- Throws an error
SELECT STRING(JSON 'null') AS result; -- Throws an error
SELECT SAFE.STRING(JSON '123') AS result; -- Returns a SQL NULL
```

### `STRING_ARRAY` 
<a id="string_array_for_json"></a>

```sql
STRING_ARRAY(json_expr)
```

**Description**

Converts a JSON array of strings to a SQL `ARRAY<STRING>` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '["purple", "blue"]'
    ```

    If the JSON value is not an array of strings, an error is produced. If the
    expression is SQL `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<STRING>`

**Examples**

```sql
SELECT STRING_ARRAY(JSON '["purple", "blue"]') AS colors;

/*----------------*
 | colors         |
 +----------------+
 | [purple, blue] |
 *----------------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of strings.
SELECT STRING_ARRAY(JSON '[123]') AS result; -- Throws an error
SELECT STRING_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT STRING_ARRAY(JSON 'null') AS result; -- Throws an error
```

### `TO_JSON`

```sql
TO_JSON(sql_value[, stringify_wide_numbers=>{ TRUE | FALSE }])
```

**Description**

Converts a SQL value to a JSON value.

Arguments:

+   `sql_value`: The SQL value to convert to a JSON value. You can review the
    ZetaSQL data types that this function supports and their
    JSON encodings [here][json-encodings].
+   `stringify_wide_numbers`: Optional mandatory-named argument that is either
    `TRUE` or `FALSE` (default).

    +   If `TRUE`, numeric values outside of the
        `DOUBLE` type domain are encoded as strings.
    +   If `FALSE` (default), numeric values outside of the
    `DOUBLE` type domain are not encoded as strings,
    but are stored as JSON numbers. If a numerical value cannot be stored in
    JSON without loss of precision, an error is thrown.

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

`JSON`

**Examples**

In the following example, the query converts rows in a table to JSON values.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10, 20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30, 40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50, 60] AS coordinates))
SELECT TO_JSON(t) AS json_objects
FROM CoordinatesTable AS t;

/*--------------------------------*
 | json_objects                   |
 +--------------------------------+
 | {"coordinates":[10,20],"id":1} |
 | {"coordinates":[30,40],"id":2} |
 | {"coordinates":[50,60],"id":3} |
 *--------------------------------*/
```

In the following example, the query returns a large numerical value as a
JSON string.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>TRUE) as stringify_on;

/*--------------------*
 | stringify_on       |
 +--------------------+
 | "9007199254740993" |
 *--------------------*/
```

In the following example, both queries return a large numerical value as a
JSON number.

```sql
SELECT TO_JSON(9007199254740993, stringify_wide_numbers=>FALSE) as stringify_off;
SELECT TO_JSON(9007199254740993) as stringify_off;

/*------------------*
 | stringify_off    |
 +------------------+
 | 9007199254740993 |
 *------------------*/
```

In the following example, only large numeric values are converted to
JSON strings.

```sql
With T1 AS (
  (SELECT 9007199254740993 AS id) UNION ALL
  (SELECT 2 AS id))
SELECT TO_JSON(t, stringify_wide_numbers=>TRUE) AS json_objects
FROM T1 AS t;

/*---------------------------*
 | json_objects              |
 +---------------------------+
 | {"id":"9007199254740993"} |
 | {"id":2}                  |
 *---------------------------*/
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

/*------------------------------*
 | json_objects                 |
 +------------------------------+
 | {"id":9.007199254740992e+15} |
 | {"id":2.1}                   |
 *------------------------------*/
```

[json-encodings]: #json_encodings

### `TO_JSON_STRING`

```sql
TO_JSON_STRING(value[, pretty_print])
```

**Description**

Converts a SQL value to a JSON-formatted `STRING` value.

Arguments:

+   `value`: A SQL value. You can review the ZetaSQL data types that
    this function supports and their JSON encodings [here][json-encodings].
+   `pretty_print`: Optional boolean parameter. If `pretty_print` is `true`, the
    `returned value is formatted for easy readability.

**Return type**

A JSON-formatted `STRING`

**Examples**

The following query converts a `STRUCT` value to a JSON-formatted string:

```sql
SELECT TO_JSON_STRING(STRUCT(1 AS id, [10,20] AS coordinates)) AS json_data

/*--------------------------------*
 | json_data                      |
 +--------------------------------+
 | {"id":1,"coordinates":[10,20]} |
 *--------------------------------*/
```

The following query converts a `STRUCT` value to a JSON-formatted string that is
easy to read:

```sql
SELECT TO_JSON_STRING(STRUCT(1 AS id, [10,20] AS coordinates), true) AS json_data

/*--------------------*
 | json_data          |
 +--------------------+
 | {                  |
 |   "id": 1,         |
 |   "coordinates": [ |
 |     10,            |
 |     20             |
 |   ]                |
 | }                  |
 *--------------------*/
```

[json-encodings]: #json_encodings

### `UINT32` 
<a id="uint32_for_json"></a>

```sql
UINT32(json_expr)
```

**Description**

Converts a JSON number to a SQL `UINT32` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

    If the JSON value is not a number, or the JSON number is not in the SQL
    `UINT32` domain, an error is produced. If the expression is SQL `NULL`, the
    function returns SQL `NULL`.

**Return type**

`UINT32`

**Examples**

```sql
SELECT UINT32(JSON '2005') AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT UINT32(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT UINT32(JSON '10.0') AS score;

/*-------*
 | score |
 +-------+
 | 10    |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT UINT32(JSON '10.1') AS result;  -- Throws an error
SELECT UINT32(JSON '-1') AS result;  -- Throws an error
SELECT UINT32(JSON '"strawberry"') AS result; -- Throws an error
SELECT UINT32(JSON 'null') AS result; -- Throws an error
SELECT SAFE.UINT32(JSON '"strawberry"') AS result;  -- Returns a SQL NULL
```

### `UINT32_ARRAY` 
<a id="uint32_array_for_json"></a>

```sql
UINT32_ARRAY(json_expr)
```

**Description**

Converts a JSON number to a SQL `UINT32_ARRAY` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

    If the JSON value is not an array of numbers, or the JSON numbers are not in
    the SQL `UINT32` domain, an error is produced. If the expression is SQL
    `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<UINT32>`

**Examples**

```sql
SELECT UINT32_ARRAY(JSON '[2005, 2003]') AS flight_numbers;

/*----------------*
 | flight_numbers |
 +----------------+
 | [2005, 2003]   |
 *----------------*/
```

```sql
SELECT UINT32_ARRAY(JSON '[10.0]') AS scores;

/*--------*
 | scores |
 +--------+
 | [10]   |
 *--------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers in UINT32 domain.
SELECT UINT32_ARRAY(JSON '[10.1]') AS result;  -- Throws an error
SELECT UINT32_ARRAY(JSON '[-1]') AS result;  -- Throws an error
SELECT UINT32_ARRAY(JSON '["strawberry"]') AS result; -- Throws an error
SELECT UINT32_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT UINT32_ARRAY(JSON 'null') AS result; -- Throws an error
```

### `UINT64` 
<a id="uint64_for_json"></a>

```sql
UINT64(json_expr)
```

**Description**

Converts a JSON number to a SQL `UINT64` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '999'
    ```

    If the JSON value is not a number, or the JSON number is not in the SQL
    `UINT64` domain, an error is produced. If the expression is SQL `NULL`, the
    function returns SQL `NULL`.

**Return type**

`UINT64`

**Examples**

```sql
SELECT UINT64(JSON '2005') AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT UINT64(JSON_QUERY(JSON '{"gate": "A4", "flight_number": 2005}', "$.flight_number")) AS flight_number;

/*---------------*
 | flight_number |
 +---------------+
 | 2005          |
 *---------------*/
```

```sql
SELECT UINT64(JSON '10.0') AS score;

/*-------*
 | score |
 +-------+
 | 10    |
 *-------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if JSON is not a number or cannot be converted to a 64-bit integer.
SELECT UINT64(JSON '10.1') AS result;  -- Throws an error
SELECT UINT64(JSON '-1') AS result;  -- Throws an error
SELECT UINT64(JSON '"strawberry"') AS result; -- Throws an error
SELECT UINT64(JSON 'null') AS result; -- Throws an error
SELECT SAFE.UINT64(JSON '"strawberry"') AS result;  -- Returns a SQL NULL
```

### `UINT64_ARRAY` 
<a id="uint64_array_for_json"></a>

```sql
UINT64_ARRAY(json_expr)
```

**Description**

Converts a JSON number to a SQL `UINT64_ARRAY` value.

Arguments:

+   `json_expr`: JSON. For example:

    ```
    JSON '[999, 12]'
    ```

    If the JSON value is not an array of numbers, or the JSON numbers are not in
    the SQL `UINT64` domain, an error is produced. If the expression is SQL
    `NULL`, the function returns SQL `NULL`.

**Return type**

`ARRAY<UINT64>`

**Examples**

```sql
SELECT UINT64_ARRAY(JSON '[2005, 2003]') AS flight_numbers;

/*----------------*
 | flight_numbers |
 +----------------+
 | [2005, 2003]   |
 *----------------*/
```

```sql
SELECT UINT64_ARRAY(JSON '[10.0]') AS scores;

/*--------*
 | scores |
 +--------+
 | [10]   |
 *--------*/
```

The following examples show how invalid requests are handled:

```sql
-- An error is thrown if the JSON is not an array of numbers in UINT64 domain.
SELECT UINT64_ARRAY(JSON '[10.1]') AS result;  -- Throws an error
SELECT UINT64_ARRAY(JSON '[-1]') AS result;  -- Throws an error
SELECT UINT64_ARRAY(JSON '["strawberry"]') AS result; -- Throws an error
SELECT UINT64_ARRAY(JSON '[null]') AS result; -- Throws an error
SELECT UINT64_ARRAY(JSON 'null') AS result; -- Throws an error
```

### JSON encodings 
<a id="json_encodings"></a>

You can encode a SQL value as a JSON value with the following functions:

+ `TO_JSON_STRING`
+ `TO_JSON`
+ `JSON_SET` (uses `TO_JSON` encoding)
+ `JSON_ARRAY` (uses `TO_JSON` encoding)
+ `JSON_ARRAY_APPEND` (uses `TO_JSON` encoding)
+ `JSON_ARRAY_INSERT` (uses `TO_JSON` encoding)
+ `JSON_OBJECT` (uses `TO_JSON` encoding)

The following SQL to JSON encodings are supported:

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
          The object can contain zero or more key-value pairs.
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
          The object can contain zero or more key-value pairs.
          Each value is formatted according to its type.
        </p>
        <p>
          Field names with underscores are converted to camel case in accordance
          with
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. Field values are formatted according to
          <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
          protobuf json conversion</a>. If a <code>field_value</code> is a
          non-empty repeated field or submessage, the elements and fields are
          indented to the appropriate level.
        </p>
        <ul>
          <li>
            Field names that aren't valid UTF-8 might result in unparseable
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
    
    
    
    <tr>
      <td>RANGE</td>
      <td>
        <p>range</p>
        <p>
          Encoded as an object with a <code>start</code> and <code>end</code>
          value. Any unbounded part of the range is represented as
          <code>null</code>.
        </p>
      </td>
      <td>
        SQL input: <code>RANGE&lt;DATE&gt; '[2024-07-24, 2024-07-25)'</code><br />
        JSON output: <code>{"start":"2024-07-24","end":"2024-07-25"}</code><br />
        <hr />
        SQL input: <code>RANGE&lt;DATETIME&gt; '[2024-07-24 10:00:00, UNBOUNDED)'</code><br />
        JSON output: <code>{"start":"2024-07-24T10:00:00","end":null}</code><br />
      </td>
    </tr>
    
  </tbody>
</table>

### JSONPath format 
<a id="JSONPath_format"></a>

With the JSONPath format, you can identify the values you want to
obtain from a JSON-formatted string.

If a key in a JSON functions contains a JSON format operator, refer to each
JSON function for how to escape them.

A JSON function returns `NULL` if the JSONPath format does not match a value in
a JSON-formatted string. If the selected value for a scalar function is not
scalar, such as an object or an array, the function returns `NULL`. If the
JSONPath format is invalid, an error is produced.

#### Operators for JSONPath

The JSONPath format supports these operators:

<table>
  <thead>
    <tr>
      <th>Operator</th>
      <th width='300px'>Description</th>
      <th>Examples</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>$</code></td>
      <td>
        Root object or element. The JSONPath format must start with this
        operator, which refers to the outermost level of the
        JSON-formatted string.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$"</code>
        </p>
        <p>
          JSON result:<br />
          <code>{"class":{"students":[{"name":"Jane"}]}}</code><br />
        </p>
      </td>
    </tr>
    <tr>
      <td><code>.</code></td>
      <td>
        Child operator. You can identify child values using dot-notation.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$.class.students"</code>
        </p>
        <p>
          JSON result:<br />
          <code>[{"name":"Jane"}]</code>
        </p>
      </td>
    </tr>
    <tr>
      <td><code>[]</code></td>
      <td>
        Subscript operator. If the object is a JSON array, you can use
        brackets to specify the array index.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"class" : {"students" : [{"name" : "Jane"}]}}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$.class.students[0]"</code>
        </p>
        <p>
          JSON result:<br />
          <code>{"name":"Jane"}</code>
        </p>
      </td>
    </tr>

    <tr>
      <td>
        <code>[][]</code><br />
        <code>[][][]...</code>
      </td>
      <td>
        Child subscript operator. If the object is a JSON array within
        an array, you can use as many additional brackets as you need
        to specify the child array index.
      </td>
      <td>
        <p>
          JSON-formatted string:<br />
          <code>'{"a": [["b", "c"], "d"], "e":"f"}'</code>
        </p>
        <p>
          JSON path:<br />
          <code>"$.a[0][1]"</code>
        </p>
        <p>
          JSON result:<br />
          <code>"c"</code>
        </p>
      </td>
    </tr>

  </tbody>
</table>

### Differences between the JSON and JSON-formatted STRING types 
<a id="differences_json_and_string"></a>

Many JSON functions accept two input types:

+  [`JSON`][JSON-type] type
+  `STRING` type

The `STRING` version of the extraction functions behaves differently than the
`JSON` version, mainly because `JSON` type values are always validated whereas
JSON-formatted `STRING` type values are not.

#### Non-validation of `STRING` inputs

The following `STRING` is invalid JSON because it is missing a trailing `}`:

```
{"hello": "world"
```

The JSON function reads the input from the beginning and stops as soon as the
field to extract is found, without reading the remainder of the input. A parsing
error is not produced.

With the `JSON` type, however, `JSON '{"hello": "world"'` returns a parsing
error.

For example:

```sql
SELECT JSON_VALUE('{"hello": "world"', "$.hello") AS hello;

/*-------*
 | hello |
 +-------+
 | world |
 *-------*/
```

```sql
SELECT JSON_VALUE(JSON '{"hello": "world"', "$.hello") AS hello;
-- An error is returned: Invalid JSON literal: syntax error while parsing
-- object - unexpected end of input; expected '}'
```

#### No strict validation of extracted values

In the following examples, duplicated keys are not removed when using a
JSON-formatted string. Similarly, keys order is preserved. For the `JSON`
type, `JSON '{"key": 1, "key": 2}'` will result in `JSON '{"key":1}'` during
parsing.

```sql
SELECT JSON_QUERY('{"key": 1, "key": 2}', "$") AS string;

/*-------------------*
 | string            |
 +-------------------+
 | {"key":1,"key":2} |
 *-------------------*/
```

```sql
SELECT JSON_QUERY(JSON '{"key": 1, "key": 2}', "$") AS json;

/*-----------*
 | json      |
 +-----------+
 | {"key":1} |
 *-----------*/
```

#### JSON `null`

When using a JSON-formatted `STRING` type in a JSON function, a JSON `null`
value is extracted as a SQL `NULL` value.

When using a JSON type in a JSON function, a JSON `null` value returns a JSON
`null` value.

```sql
WITH t AS (
  SELECT '{"name": null}' AS json_string, JSON '{"name": null}' AS json)
SELECT JSON_QUERY(json_string, "$.name") AS name_string,
  JSON_QUERY(json_string, "$.name") IS NULL AS name_string_is_null,
  JSON_QUERY(json, "$.name") AS name_json,
  JSON_QUERY(json, "$.name") IS NULL AS name_json_is_null
FROM t;

/*-------------+---------------------+-----------+-------------------*
 | name_string | name_string_is_null | name_json | name_json_is_null |
 +-------------+---------------------+-----------+-------------------+
 | NULL        | true                | null      | false             |
 *-------------+---------------------+-----------+-------------------*/
```

[JSONPath-format]: #JSONPath_format

[JSON-type]: https://github.com/google/zetasql/blob/master/docs/data-types.md#json_type

[JSONPath-mode]: #JSONPath_mode

