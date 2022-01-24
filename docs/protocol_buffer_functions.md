

# Protocol buffer functions

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

[field-access-operator]: https://github.com/google/zetasql/blob/master/docs/operators.md#field-access-operator

<!-- mdlint on -->

