

<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Protocol Buffers

Protocol buffers are a flexible, efficient mechanism for serializing structured
data. They are easy to create, small in size, and efficient to send over RPCs.

As efficient and as popular as protocol buffers are, however, when it comes to
data storage they do have one drawback: they do not map well to SQL syntax. For
example, SQL syntax expects that a given field can support a `NULL` or default
value. Protocol buffers, on the other hand, do not support `NULL`s very well,
and there isn't a standard way to determine whether a missing field should get a
`NULL` or a default value.

If you're going to query protocol buffers, you need to understand how they are
represented, what features they support, and what data they can contain. If
you're unfamiliar with protocol buffers in general, or would like a refresher on
how they work in languages other than SQL, see the
[Protocol Buffers Developer Guide](https://developers.google.com/protocol-buffers).

## Constructing protocol buffers

This section covers how to construct protocol buffers using ZetaSQL.

<a id=using_new></a>
### Using NEW

You can create a protocol buffer using the keyword `NEW`:

```
NEW TypeName(field_1 [AS alias], ...field_n [AS alias])
```

When using the `NEW` keyword to create a new protocol buffer:

+ All field expressions must have an [explicit alias](https://github.com/google/zetasql/blob/master/docs/query-syntax#explicit-alias-syntax) or end with an identifier.
  For example, the expression `a.b.c` has the [implicit alias](https://github.com/google/zetasql/blob/master/docs/query-syntax#implicit-aliases) `c`.
+ `NEW` matches fields by alias to the field names of the protocol buffer.
  Aliases must be unique.
+ The expressions must be implicitly coercible or literal-coercible to the type
  of the corresponding protocol buffer field.

Example:

```
SELECT
  key,
  name,
  NEW zetasql.examples.music.Chart(key AS rank, name AS chart_name)
FROM
  (SELECT 1 as key, "2" as name);
```

To create a protocol buffer with an extension, use this syntax:

```
NEW TypeName(expr1 AS (path.to.extension), ...)
```

+   For `path.to.extension`, provide the path to the extension. Place the
    extension path inside parentheses.
+   `expr1` provides the value to set for the extension. `expr1` must be of the
    same type as the extension or
    [coercible to that type](https://github.com/google/zetasql/blob/master/docs/one-pager#conversion-rules).

    Example:

    ```
    SELECT
     NEW zetasql.examples.music.Album (
       album AS album_name,
       count AS (zetasql.examples.music.downloads)
     )
     FROM (SELECT 'Bach: The Goldberg Variations' AS album, 30 AS count);

    +----------------------------------------------------------------------------------------+
    | $col1                                                                                  |
    +----------------------------------------------------------------------------------------+
    | {album_name: 'Bach: The Goldberg Variations' [zetasql.examples.music.downloads]: 30} |
    +----------------------------------------------------------------------------------------+
    ```
+   If `path.to.extension` points to a nested protocol buffer extension, `expr1`
    provides an instance or a text format string of that protocol buffer.

    Example:

    ```
    SELECT
     NEW zetasql.examples.music.Album(
       'Bach: The Goldberg Variations' AS album_name,
       NEW zetasql.examples.music.AlbumExtension(
        DATE(1956,1,1) AS release_date
       )
     AS (zetasql.examples.music.AlbumExtension.album_extension));
    +-------------------------------------------------------------+
    | $col1                                                       |
    +-------------------------------------------------------------+
    | album_name: "Bach: The Goldberg Variations"                 |
    | [zetasql.examples.music.AlbumExtension.album_extension] { |
    |   release_date: -5114                                       |
    | }                                                           |
    +-------------------------------------------------------------+
    ```

### SELECT AS typename

```
SELECT AS catalog.ProtocolBufferName
  expr1 [[AS] protocol_buffer_field1]
  [, ...]
FROM ...
```

A `SELECT AS typename` statement produces a value table where the row type is a
specific named type. Currently, protocol buffers are the only supported type
that can be used with this syntax.

The `SELECT` list may produce multiple columns.  Each produced column must have
an alias (explicitly or implicitly) that matches a unique protocol buffer field
name; to construct the protocol buffer, the query matches each expression with a
protocol buffer field by name. If no explicit alias is given, the expression
must have an implicit alias according to the rules in
[Implicit Aliases](https://github.com/google/zetasql/blob/master/docs/query-syntax.md#implicit_aliases).

When used with `SELECT DISTINCT`, or `GROUP BY` or `ORDER BY` using column
ordinals, these operators are applied first, on the columns in the `SELECT`
list, and then the value construction happens last.  This means that `DISTINCT`
can be applied on the input columns to the value construction, including in
cases where `DISTINCT` would not be allowed after value construction because
equality is not supported on protocol buffer types.

The following is an example of a `SELECT AS typename` query.

```
SELECT AS tests.TestProtocolBuffer mytable.key int64_val, mytable.name string_val
FROM mytable;

```

The query returns the output as a `tests.TestProtocolBuffer` protocol
buffer. `mytable.key int64_val` means that values from the `key` column are
stored in the `int64_val` field in the protocol buffer. Similarly, values from
the `mytable.name` column are stored in the `string_val` protocol buffer field.

 `SELECT AS` does not support setting protocol
buffer extensions. To do so, use the [NEW](#using_new) keyword instead. For
example,  to create a protocol buffer with an extension, change a query like
this:

```
SELECT AS ProtoType field1, field2, ...
```

to a query like this:

```
SELECT AS VALUE NEW ProtoType(field1, field2, field3 AS (path.to.extension), ...)
```

## Casting Protocol Buffers

You can cast `PROTO` to or from `BYTES` or `STRING`.

```
SELECT CAST('first_name: "Jane", last_name: "Doe", customer_no: 1234'
  as example.CustomerInfo);
```

Casting to or from `BYTES` produces or parses proto2 wire format bytes. If
there is a failure during the serialization or deserialization process, an error
is raised. This can happen, for example, if no value is specified for a
required field.

Casting to or from `STRING` produces or parses the proto2 text format. When
casting from `STRING`, unknown field names aren't parseable. This means you need
to be cautious, because round-tripping from `PROTO` to `STRING` back to `PROTO`
could result in loss of data.

`STRING` literals used where a `PROTO` value is expected will be implicitly cast
to `PROTO`. If the literal value cannot be parsed using the expected `PROTO`
type, an error will be raised. To return `NULL`
instead, use [`SAFE_CAST`][link_to_safe_cast].

<a id=type_mapping></a>
## Type mapping

Protocol buffers are represented using the `PROTO` data type.  A column can
contain `PROTO` values the same way it can contain `INT32` or `STRING` values.

A protocol buffer contains zero or more fields inside it. Each field inside a
protocol buffer has its own type. All data types except `STRUCT` can be
contained inside a `PROTO`. Repeated fields in a protocol buffer are represented
as `ARRAY`s. The following table gives examples of the mapping between various
protocol buffer field types and the resulting ZetaSQL types.

<table>
<thead>
<tr>
<th>Protocol Buffer Field Type</th>
<th nowrap>ZetaSQL Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>optional MessageType msg = 1;</code></td>
<td><code>PROTO&lt;MessageType&gt;</code></td>
</tr>
<tr>
<td><code>required int64 int = 1;</code></td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>
<code>optional int64 int = 1;</code><br>
When reading, if this field isn't set, the default value (0) is returned. By
default, protocol buffer fields do not return <code>NULL</code>.
</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>
<code>optional int64 int = 1 [( zetasql.use_defaults ) = false];</code><br>
When reading, if this field isn't set, a <code>NULL</code> value is returned.
This example uses an annotation, which is described in
<a href="#defaults_and_nulls">Defaults and <code>NULL</code>s</a>.
</td>
<td><code>INT64</code></td>
</tr>

<tr>
<td>
<code>optional int32 date = 1 [( zetasql.format ) = DATE];</code>

</td>
<td><code>DATE</code></td>
</tr>
<tr>
<td>
<code>optional int64 time = 1 [( zetasql.format ) = TIMESTAMP_MICROS];</code>

</td>
<td><code>TIMESTAMP</code></td>
</tr>

<tr>
<td><code>repeated int64 repeat = 1;</code></td>
<td><code>ARRAY&lt;INT64&gt;</code></td>
</tr>
</tbody>
</table>

<a name="defaults_and_nulls"></a>
### Default values and `NULL`s

Protocol buffer messages themselves do not have a default value &mdash; only the
fields contained inside a protocol buffer have defaults. When a full protocol
buffer value is returned in the result of a query, it is returned as a blob and
all fields are preserved as they were stored, including unset fields. This means
that you can run a query that returns a protocol buffer, and then extract fields
or check field presence in your client code with normal protocol buffer default
behavior.

By default, `NULL` values are never returned when accessing non-repeated leaf
fields contained in a `PROTO` from within a SQL statement, unless a containing
value is also `NULL`.  If the field value is not explicitly set, the default
value for the field is returned.  A change to the default value for a protocol
buffer field affects all future reads of that field for records where the value
is unset.

For example, suppose that `proto_msg` of type `PROTO` has a field named
`leaf_field`. A reference to `proto_msg.leaf_field` returns:

* `NULL` if `proto_msg` is `NULL`.
* A default value if `proto_msg` is not `NULL` but `leaf_field` is not set.
* The value of `leaf_field` if `proto_msg` is not `NULL` and `leaf_field`
  is set.

#### zetasql.use_defaults

You can change this default behavior using a special annotation on your protocol
message definition, `zetasql.use_defaults`, which you set on an individual
field to cause `NULL` values to be returned whenever a field value is not
explicitly set.

This annotation takes a boolean value.  The default is `true`, which means to
use the protocol buffer field defaults.  The annotation normally is written with
the value `false`, meaning that defaults should be ignored and `NULL`s should be
returned.

The following example shows how you can use the `use_defaults` annotation for an
optional protocol buffer field.

```
import "zetasql/public/proto/type_annotation.proto";

message SimpleMessage {
  // String field, where ZetaSQL interprets missing values as NULLs.
  optional string str = 2 [( zetasql.use_defaults ) = false];
}
```

In the case where protocol buffers have empty repeated fields, an empty `ARRAY`
is returned rather than a `NULL`-valued `ARRAY`. This behavior cannot be
changed.

After a value has been read out of a protocol buffer field, that value is
treated like any other value of that type. For non-`PROTO` values, such as
`INT64`, this means that after you get the value, you will not be able to tell
if the value for that field was set explicitly, or if it was read as a default
value.

#### zetasql.use_field_defaults

The `zetasql.use_field_defaults` annotation is just like
`zetasql.use_defaults`, but you set it on a message and it applies to all
unset fields within a given protocol buffer message. If both are present, the
field-level annotation takes precedence.

```
import "zetasql/public/proto/type_annotation.proto";

message AnotherSimpleMessage {
  // Interpret missing value as NULLs for all fields in this message.
  option ( zetasql.use_field_defaults ) = false;

  optional int64 nullable_int = 1;
  optional string nullable_string = 2;
}
```

<a id=checking_if_a_field_has_a_value></a>
#### Checking if a non-repeated field has a value

You can detect whether `optional` fields are set using a virtual field, `has_X`,
where `X` is the name of the field being checked. The type of the `has_X` field
is `BOOL`. The `has_` field is available for any non-repeated field of a `PROTO`
value. This field equals true if the value `X` is explicitly set in the message.

This field is useful for determining if a protocol buffer field has an explicit
value, or if reads will return a default value. Consider the protocol buffer
example, which has a field `country`. This field has a default value of `United
States`. You can construct a query to determine if a Customer protocol buffer
message has a value for the country field by using the virtual field
`has_country`:

```
SELECT
  c.Orders.shipping_address.has_country
FROM
  Customer c;
```

If `has_country` returns `TRUE`, it indicates that the value for the country
field has been explicitly set. If it returns `FALSE` or `NULL`, it means the
value is not explicitly set.

<a id=checking_for_a_repeated_value></a>
#### Checking for a repeated value

You can use an `EXISTS` subquery to scan inside a repeated field and check if
any value exists with some desired property. For example, the following query
returns the name of every customer who has placed an order for the product
"Foo".

```
SELECT
  c.name
FROM
  Customers AS c
WHERE
  EXISTS(SELECT
           *
         FROM
           c.Orders.line_item AS item
         WHERE
           item.product_name = "Foo");
```

#### Nullness and nested fields

A `PROTO` value may contain fields which are themselves `PROTO`s. When this
happens it is possible for the nested `PROTO` itself to be `NULL`. In such a
case, the fields contained within that nested field are also `NULL`
regardless of their `use_default_value` settings.

Consider this example proto:

```
syntax = "proto2";

import "zetasql/public/proto/type_annotation.proto";

package some.package;

message NestedMessage {
  optional int64 value = 1 [( zetasql.use_defaults ) = true];
}

message OuterMessage {
  optional NestedMessage nested = 1;
}
```

Running the following query returns a `5` for `value` because it is
explicitly defined.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("nested { value: 5 }" as some.package.OuterMessage) as proto_field);
```

If `value` is not explicitly defined but `nested` is, you get a `0` because
the annotation on the protocol buffer definition says to use default values.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("nested { }" as some.package.OuterMessage) as proto_field);
```

However, if `nested` is not explicitly defined, you get a `NULL` even
though the annotation says to use default values for the `value` field. This is
because the containing message is `NULL`. This behavior applies to both
repeated and non-repeated fields within a nested message.

```
SELECT
  proto_field.nested.value
FROM
  (SELECT
     CAST("" as some.package.OuterMessage) as proto_field);
```

<a id="proto_annotations"></a>
### Annotations to extend the type system

The ZetaSQL type system contains more types than the protocol buffer
type system.
<a href="https://developers.google.com/protocol-buffers/docs/proto?csw=1#options">
Proto annotations</a> are used to store non-protocol-buffer types inside
serialized protos and read them back as the correct type.

While protocol buffers themselves do not support `DATE` or `TIMESTAMP` types,
you can use annotations on your protocol message definition to indicate that
certain fields should be interpreted as `DATE` or `TIMESTAMP` values when read
using SQL. For instance, a protocol message definition could contain the
following line:

```
optional int32 date = 2 [( zetasql.format ) = DATE];
```

The `zetasql.format` annotation indicates that this field, which stores an
`int32` in the protocol buffer, should be interpreted as a `DATE`. Queries over
the `date` field return a `DATE` type instead of an `INT32` because of the
annotation.

This result is the equivalent of having an `INT32` column and querying it as
follows:

```
SELECT
  DATE_FROM_UNIX_DATE(date)...
```

<a id=querying_protocol_buffers></a>
## Querying protocol buffers

You use the dot operator to access the fields contained within a protocol
buffer.

<a id=example_protocol_buffer_message></a>
### Example protocol buffer message

To illustrate how to query protocol buffers, consider a table, `Customers`, that
contains a column `Orders` of type `PROTO`. The proto stored in `Orders`
contains fields such as the items ordered and the shipping address. The `.proto`
file that defines this protocol buffer might look like this:

```
import "zetasql/public/proto/type_annotation.proto";

message Orders {
  optional string order_number = 1;
  optional int64 date = 2 [( zetasql.format ) = DATE];

  message Address {
    optional string street = 1;
    optional string city = 2;
    optional string state = 3;
    optional string country = 4 [( zetasql.use_defaults ) = true,
                                  default = "United States"];
  }

  optional Address shipping_address = 3;

  message Item {
    optional string product_name = 1;
    optional int32 quantity = 2;
  }

  repeated Item line_item = 4;

  map<string, string> labels = 5;
}
```

An instance of this message might be:

```
{
  order_number: 1234567
  date: 16242
  shipping_address: {
      street: "1234 Main St"
      city: "AnyCity"
      state: "AnyState"
      country: "United States"
  }
  line_item: {
      product_name: "Foo"
      quantity: 10
  }
  line_item: {
      product_name: "Bar"
      quantity: 5
  }
}
```

<a id=querying_top-level_fields></a>
### Querying top-level fields

You can write a query to return an entire protocol buffer message, or to return
a top-level or nested field of the message.

Using our example protocol buffer message, the following query returns all
protocol buffer values from the `Orders` column:

```
SELECT
  c.Orders
FROM
  Customers c;
```

This query returns the top-level field `order_number` from all protocol buffer
messages in the `Orders` column:

```
SELECT
  c.Orders.order_number
FROM
  Customers c;
```

<a id=querying_nested_paths></a>
### Querying nested paths

Notice that the `Order` protocol buffer contains another protocol buffer
message, `Address`, in the `shipping_address` field. You can create a query that
returns all orders that have a shipping address in the United States:

```
SELECT
  c.Orders.order_number,
  c.Orders.shipping_address
FROM
  Customers c
WHERE
  c.Orders.shipping_address.country = "United States";
```

### Returning repeated fields

Often, a protocol buffer message contains one or more repeated fields which are
returned as `ARRAY` values when referenced in SQL statements. For example, our
protocol buffer message contains a repeated field, `line_item`.

The following query returns a collection of `ARRAY`s containing the line items,
each holding all the line items for one order:

```
SELECT
  c.Orders.line_item
FROM
  Customers c;
```

For more information, see
[Working with Arrays](https://github.com/google/zetasql/blob/master/docs/arrays.md).

### Returning the number of elements in an array

As with any other `ARRAY` value, you can return the number of repeated fields in
a protocol buffer using the `ARRAY_LENGTH` function.

```
SELECT
  c.Orders.order_number,
  ARRAY_LENGTH(c.Orders.line_item)
FROM
  Customers c;
```

### Querying map fields

Maps are not a supported type in ZetaSQL. However, maps are <a href=
"https://developers.google.com/protocol-buffers/docs/proto3#backwards-compatibility">
implemented in proto3 as repeated fields</a>, so you can query maps by querying
the underlying repeated field. The underlying repeated field has `key` and
`value` fields that can be queried.

```
SELECT
  c.Orders.order_number
FROM
  Customers c
WHERE
  EXISTS(SELECT
           *
         FROM
           c.Orders.Labels label
         WHERE
           label.key = "color" AND label.value = "red");
```

<a id="extensions_and_weak_field"></a>

## Extensions

[extensions](https://developers.google.com/protocol-buffers/docs/proto#extensions)
can be queried from `PROTO` values.

<a id="extensions"></a>
### Top-level extensions

If your `PROTO` value contains extensions, you can query those fields using the
following syntax:

```
<identifier_of_proto_value>.(<package_of_extension>.<path_expression_to_extension_field>)
```

For example, consider this proto definition:

```
package some.package;

message Foo {
  optional int32 x = 1;
  extensions 100 to 130;
}

message Point {
  optional int32 x = 1;
  optional int32 y = 2;
}

extend Foo {
  optional int32 bar = 126;
  optional Point point = 127;
}
```

The following sections use this proto definition in a Table, `Test`, which
contains a field, `foo_field` of type `Foo`.

A query that returns the value of the `bar` extension field would resemble the
following:

```sql
SELECT
  foo_field.(some.package.bar)
FROM
  Test;
```

These types of extensions are often referred to as *top-level extensions*.

If you want your statement to return a specific value from a top-level
extension, you would modify it as follows:

```sql
SELECT
  foo_field.(some.package.point).y
FROM
  Test;
```

You can refine your statement to look for a specific value of a top-level
extension as well.

```sql
SELECT
  foo_field.(some.package.bar)
FROM
  Test
WHERE
  foo_field.(some.package.bar) = 5;
```

Note that you can also put back quotes around the components in the extension
path name in case they need to be escaped to avoid collisions with reserved
keywords. For example:

```sql
SELECT
  foo_field.(`some.package`.`bar`).value = 5
FROM
  Test;
```

### Nested extensions

[Nested extensions](https://developers.google.com/protocol-buffers/docs/proto#nested-extensions)
are also supported. These are protocol buffer extensions that are declared
within the scope of some other protocol message. For example:

```
package some.package;

message Baz {
  extend Foo {
    optional Baz foo_ext = 127;
  }
  optional int32 a = 1;
  optional int32 b = 2;
  ...
}
```

To construct queries for nested extensions, you use the same parenthetical
syntax as described in the previous section. To reference a nested extension,
in addition to specifying the package name, you must also specify the name of
the message where the extension is declared. For example:

```sql
SELECT
  foo_field.(some.package.Baz.foo_ext)
FROM
  Test;
```

You can reference a specific field in a nested extension using the same syntax
described in the previous section. For example:

```sql
SELECT
  foo_field.(some.package.Baz.foo_ext).a
FROM
  Test;
```

### Correlated `CROSS JOIN` and repeated extensions

Correlated `CROSS JOIN` is used to "flatten" repeated fields. That is, the
values of the protocol buffer are duplicated once per entry in the repeated
field. In practice, this means that the repeated field is `UNNEST`ed. For
standard repeated fields, this unnesting happens implicitly. For repeated
extensions the `UNNEST` must be specified explicitly.

Consider the following protocol buffer:

```
syntax = "proto2";

package some.package;

message Example {
  optional int64 record_key = 1;
  repeated int64 repeated_value = 2;
  extensions 3 to 3;
}

message Extension {
  extend Example {
    repeated int64 repeated_extension_value = 3;
  }
}
```

The following query which uses the standard repeated field, `repeated_value` in
a correlated `CROSS JOIN` runs without an explicit `UNNEST`.

```
WITH t AS
  (SELECT
     CAST("""
       record_key: 1
       repeated_value: 1
       repeated_value: 2
       repeated_value: 3
       [some.package.Extension.repeated_extension_value]: 4
       [some.package.Extension.repeated_extension_value]: 5
       [some.package.Extension.repeated_extension_value]: 6"""
     as some.package.Example) as proto_field)
SELECT
  t.proto_field.record_key,
  value
FROM
  t,
  t.proto_field.repeated_value value;
```

This query, which uses the repeated extension field, `repeated_extension_value`
in the correlated `CROSS JOIN` requires an explicit `UNNEST`.

```
WITH t AS
  (SELECT
     CAST("""
       record_key: 1
       repeated_value: 1
       repeated_value: 2
       repeated_value: 3
       [some.package.Extension.repeated_extension_value]: 4
       [some.package.Extension.repeated_extension_value]: 5
       [some.package.Extension.repeated_extension_value]: 6"""
     as some.package.Example) as proto_field)
SELECT
  t.proto_field.record_key,
  value
FROM
  t,
  UNNEST(t.proto_field.(some.package.Extension.repeated_extension_value)) value;
```

[link_to_safe_cast]: https://github.com/google/zetasql/blob/master/docs/conversion_rules#safe_casting

<!-- END CONTENT -->

