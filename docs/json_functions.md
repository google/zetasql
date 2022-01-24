

# JSON functions

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

