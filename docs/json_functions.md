

# JSON functions

ZetaSQL supports functions that help you retrieve data stored in
JSON-formatted strings and functions that help you transform data into
JSON-formatted strings.

### JSON_EXTRACT or JSON_EXTRACT_SCALAR

<a id="json_extract"></a>`JSON_EXTRACT(json_string_expr,
json_path_format)`, which returns JSON values as STRINGs.

<a id="json_extract_scalar"></a>`JSON_EXTRACT_SCALAR(json_string_expr,
json_path_format)`, which returns scalar JSON values as STRINGs.

**Description**

Extracts JSON values or JSON scalar values as strings.

+  `json_string_expr`: A JSON-formatted string. For example:

    ```
    {"class" : {"students" : [{"name" : "Jane"}]}}
    ```
+  `json_path_format`: The [JSONpath][jsonpath-format] format.
   This identifies the value or values you want to obtain from the
   JSON-formatted string. If `json_path_format` returns a JSON `null`,
   this is converted into a SQL `NULL`.

In cases where a JSON key uses invalid JSONPath characters, you can escape
those characters using single quotes and brackets.

**Return type**

`STRING`s

**Examples**

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

```sql
SELECT JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.name') as json_name,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.name') as scalar_name,
  JSON_EXTRACT('{ "name" : "Jakob", "age" : "6" }', '$.age') as json_age,
  JSON_EXTRACT_SCALAR('{ "name" : "Jakob", "age" : "6" }', '$.age') as scalar;

+-----------+-------------+----------+--------+
| json_name | scalar_name | json_age | scalar |
+-----------+-------------+----------+--------+
| "Jakob"   | Jakob       | "6"      | 6      |
+-----------+-------------+----------+--------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using single quotes and brackets, `[' ']`. For example:

```sql
SELECT JSON_EXTRACT_SCALAR('{"a.b": {"c": "world"}}', "$['a.b'].c") as hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

### JSON_QUERY or JSON_VALUE

<a id="json_query"></a>`JSON_QUERY(json_string_expr, json_path_format)`,
which returns JSON values as STRINGs.

<a id="json_value"></a>`JSON_VALUE(json_string_expr, json_path_format)`,
which returns scalar JSON values as STRINGs.

**Description**

Extracts JSON values or JSON scalar values as strings.

+  `json_string_expr`: A JSON-formatted string. For example:

  ```
  {"class" : {"students" : [{"name" : "Jane"}]}}
  ```
+  `json_path_format`: The [JSONpath][jsonpath-format] format.
   This identifies the value or values you want to obtain from the
   JSON-formatted string. If `json_path_format` returns a JSON `null`,
   this is converted into a SQL `NULL`.

In cases where a JSON key uses invalid JSONPath characters,
you can escape those characters using double quotes.

**Examples**

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

```sql
SELECT JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.name') as json_name,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.name') as scalar_name,
  JSON_QUERY('{ "name" : "Jakob", "age" : "6" }', '$.age') as json_age,
  JSON_VALUE('{ "name" : "Jakob", "age" : "6" }', '$.age') as scalar;

+-----------+-------------+----------+--------+
| json_name | scalar_name | json_age | scalar |
+-----------+-------------+----------+--------+
| "Jakob"   | Jakob       | "6"      | 6      |
+-----------+-------------+----------+--------+
```

In cases where a JSON key uses invalid JSONPath characters, you can escape those
characters using double quotes. For example:

```sql
SELECT JSON_VALUE('{"a.b": {"c": "world"}}', '$."a.b".c') as hello;

+-------+
| hello |
+-------+
| world |
+-------+
```

### TO_JSON_STRING

```sql
TO_JSON_STRING(value[, pretty_print])
```

**Description**

Returns a JSON-formatted string representation of `value`. This function
supports an optional boolean parameter called `pretty_print`. If `pretty_print`
is `true`, the returned value is formatted for easy readability.

<table>
<thead>
<tr>
<th>Input data type</th>
<th>Returned value</th>
</tr>
</thead>
<tbody>
 <tr>
    <td>NULL of any type</td>
    <td><code>null</code></td>
 </tr>
  <tr>
    <td>BOOL</td>
    <td><code>true</code> or <code>false</code>.</td>
 </tr>

  <tr>
    <td>INT32, UINT32</td>
    <td><p>Same as <code>CAST(value AS STRING)</code>. For example:</p>
    <code>-1, 0, 12345678901</code>
    </td>
 </tr>

 <tr>
    <td>INT64, UINT64</td>
    <td><p>Same as <code>CAST(value AS STRING)</code> when <code>value</code> is
    in the range of [-2<sup>53</sup>, 2<sup>53</sup>], which is the range of integers that can be
    represented losslessly as IEEE 754 double-precision floating point numbers.
    Values outside of this range are represented as quoted strings. For example:
    </p>
    <code>-1</code><br>
    <code>0</code><br>
    <code>12345678901</code><br>
    <code>9007199254740992</code><br>
    <code>-9007199254740992</code><br>
    <code>"9007199254740993"</code><br>
    <p><code>9007199254740993</code> is greater than 2<sup>53</sup>, so it is represented
    as a quoted string.</p>
    </td>
 </tr>
 <tr>
   <td>NUMERIC</td>
   <td><p>Same as <code>CAST(value AS STRING)</code> when <code>value</code> is
     in the range of [-2<sup>53</sup>, 2<sup>53</sup>] and has no fractional
     part. Values outside of this range are represented as quoted strings. For
     example:</p>
     <code>-1</code><br/>
     <code>0</code><br/>
     <code>&quot;9007199254740993&quot;</code><br/>
     <code>&quot;123.56&quot;</code>
    </td>
 </tr>
 <tr>
    <td>FLOAT, DOUBLE</td>
    <td><code>+/-inf</code> and <code>NaN</code> are represented as
    <code>Infinity</code>, <code>-Infinity</code>, and <code>NaN</code>,
    respectively.
    <p>Otherwise, the same as <code>CAST(value AS STRING)</code>.</p>
    </td>
 </tr>
 <tr>
    <td>STRING</td>
    <td>Quoted string value, escaped according to the JSON standard.
    Specifically, <code>"</code>, <code>\</code>, and the control characters
    from <code>U+0000</code> to <code>U+001F</code> are escaped.</td>
 </tr>
 <tr>
    <td>BYTES</td>
    <td><p>Quoted RFC 4648 base64-escaped value. For example:</p>
    <p><code>"R29vZ2xl"</code> is the base64 representation of bytes
    <code>b"Google"</code></p>
    </td>
 </tr>
 
 <tr>
    <td>ENUM</td>
    <td><p>Quoted enum value name as a string.</p>
    <p>Invalid enum values are represented as their number, such as 0 or 42.</p>
    </td>
 </tr>
 
 <tr>
    <td>DATE</td>
    <td><p>Quoted date. For example:</p>
    <code>"2017-03-06"</code>
    </td>
 </tr>
 <tr>
    <td>TIMESTAMP</td>
    <td><p>Quoted ISO 8601 date-time, where T separates the date and time and
    Zulu/UTC represents the time zone. For example:</p>
    <code>"2017-03-06T12:34:56.789012Z"</code>
    </td>
 </tr>
 <tr>
    <td>DATETIME</td>
    <td><p>Quoted ISO 8601 date-time, where T separates the date and time. For
    example:</p>
    <code>"2017-03-06T12:34:56.789012"</code>
    </td>
 </tr>
 <tr>
    <td>TIME</td>
    <td><p>Quoted ISO 8601 time. For example:</p>
    <code>"12:34:56.789012"</code></td>
 </tr>
 <tr>
    <td>ARRAY</td>
    <td>
      <p>
        Array of zero or more elements. Each element is formatted according to
        its type.
      </p>
      <p>
        Example without formatting:
      </p>
      <pre class="lang-sql prettyprint">["red", "blue", "green"]</pre>
      <p>
        Example with formatting:
      </p>
      <pre class="lang-sql prettyprint">
[
  "red",
  "blue",
  "green"
]</pre>
    </td>
 </tr>
 <tr>
    <td>STRUCT</td>
    <td>
      <p>
        Object that contains zero or more key/value pairs.
        Each value is formatted according to its type.
      </p>
      <p>
        Example without formatting:
      </p>
      <pre class="lang-sql prettyprint">{"colors":["red","blue"],"purchases":12,"inStock": true}</pre>
      <p>
        Example with formatting:
      </p>
      <pre class="lang-sql prettyprint">
{
  "color":[
    "red",
    "blue"
   ]
  "purchases":12,
  "inStock": true
}</pre>
      <p>
        Fields with duplicate names might result in unparseable JSON. Anonymous
        fields are represented with <code>""</code>. If a field is a non-empty
        array or object, elements/fields are indented
        to the appropriate level.
      </p>
      <p>
        Invalid UTF-8 field names might result in unparseable JSON. String
        values are escaped according to the JSON standard. Specifically,
        <code>"</code>, <code>\</code>, and the control characters from
        <code>U+0000</code> to <code>U+001F</code> are escaped.
      </p>
    </td>
 </tr>

 <tr>
    <td>PROTO</td>
    <td>
      <p>
        Object that contains zero or more key/value pairs.
        Each value is formatted according to its type.
      </p>
      <p>
        Example without formatting:
      </p>
      <pre class="lang-sql prettyprint">{"colors":["red","blue"],"purchases":12,"inStock": true}</pre>
      <p>
        Example with formatting:
      </p>
      <pre class="lang-sql prettyprint">
{
  "color":[
    "red",
    "blue"
   ]
  "purchases":12,
  "inStock": true
}</pre>
      <p>
        Field names with underscores are converted to camel-case in accordance
        with
        <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
        protobuf json conversion</a>. Field values are formatted according to
        <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
        protobuf json conversion</a>. If a <code>field_value</code> is a non-empty
        repeated field or submessage, elements/fields are indented to the
        appropriate level.
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
          field name is enclosed in brackets and prefixed with the full name of
          the extension type.
        </li>
        
      </ul>
    </td>
 </tr>

</tbody>
</table>

**Return type**

JSON string representation of the value.

**Examples**

Convert rows in a table to JSON.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates) UNION ALL
    (SELECT 3 AS id, [50,60] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t) AS json_data
FROM CoordinatesTable as t;

+----+-------------+--------------------------------+
| id | coordinates | json_data                      |
+----+-------------+--------------------------------+
| 1  | [10, 20]    | {"id":1,"coordinates":[10,20]} |
| 2  | [30, 40]    | {"id":2,"coordinates":[30,40]} |
| 3  | [50, 60]    | {"id":3,"coordinates":[50,60]} |
+----+-------------+--------------------------------+
```

Convert rows in a table to JSON with formatting.

```sql
With CoordinatesTable AS (
    (SELECT 1 AS id, [10,20] AS coordinates) UNION ALL
    (SELECT 2 AS id, [30,40] AS coordinates))
SELECT id, coordinates, TO_JSON_STRING(t, true) AS json_data
FROM CoordinatesTable as t;

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

### JSONPath format

Most JSON functions pass in a `json_string_expr` and `json_path_format`
parameter. The `json_string_expr` parameter passes in a JSON-formatted
string, and the `json_path_format` parameter identifies the value or
values you want to obtain from the JSON-formatted string.

The `json_string_expr` parameter must be a JSON string that is
formatted like this:

```json
{"class" : {"students" : [{"name" : "Jane"}]}}
```

You construct the `json_path_format` parameter using the
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

A JSON functions returns `NULL` if the `json_path_format` parameter does
not match a value in `json_string_expr`. If the selected value for a scalar
function is not scalar, such as an object or an array, the function
returns `NULL`.

If the JSONPath is invalid, the function raises an error.

[jsonpath-format]: #jsonpath_format
[json-path]: https://github.com/json-path/JsonPath#operators

