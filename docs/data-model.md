

<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# ZetaSQL Data Model

The following sections provide an overview of the ZetaSQL data
model.

## Standard SQL Tables

ZetaSQL data is stored in tables. Each table consists of an ordered
list of columns and a number of rows. Each column has a name used to identify it
through SQL statements, and is assigned a specific data type.

For example, the following table, **Singers**, is a standard SQL table.

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
<th>Default Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>SingerId</td>
<td><code>INT64</code></td>
<td><code>&lt;auto-increment&gt;</code></td>
</tr>
<tr>
<td>FirstName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>LastName</td>
<td><code>STRING</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>BirthDate</td>
<td><code>DATE</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Status</td>
<td><code>STRING</code></td>
<td><code>"active"</code></td>
</tr>
<tr>
<td>SingerInfo</td>
<td><code>PROTO&lt;SingerMetadata&gt;</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>Albums</td>
<td><code>PROTO&lt;Album&gt;</code></td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

The proto, `SingerMetadata`, has the following definition:

```
message SingerMetadata {
  optional string    nationality = 1;
  repeated Residence residence   = 2;

  message Residence {
    required int64  start_year   = 1;
    optional int64  end_year     = 2;
    optional string city         = 3;
    optional string country      = 4 [default = "USA"];
  }
}
```

A `SELECT *` statement on this table would return rows similar to the following:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>Status</th>
<th>SingerInfo</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>Marc</td>
<td>Richards</td>
<td>1970-09-03</td>
<td>active</td>
<td>{nationality: "England"}</td>
</tr>
<tr>
<td>2</td>
<td>Catalina</td>
<td>Smith</td>
<td>1990-08-17</td>
<td>inactive</td>
<td>{nationality: "U.S.A."}</td>
</tr>
<tr>
<td>3</td>
<td>Lea</td>
<td>Martin</td>
<td>1991-11-09</td>
<td>active</td>
<td>{nationality: "Australia"}</td>
</tr>
<tr>
<td>4</td>
<td>Xanathe</td>
<td>Riou</td>
<td>1995-05-23</td>
<td>inactive</td>
<td>{nationality: U.S.A."}</td>
</tr>
</tbody>
</table>

While tables do not have a type, some operations will construct an implicit
`STRUCT` type out of a SQL row, using the column names and types for field
definitions.

For more information on the data types ZetaSQL supports, see
[Data Types](https://github.com/google/zetasql/blob/master/docs/data-types.md).

## Constraints

Constraints require that any writes to one or more columns, such as inserts or
updates, conform to certain rules.
[Data manipulation language (DML)](https://github.com/google/zetasql/blob/master/docs/data-manipulation-language.md)
statements enforce constraints. ZetaSQL  supports the
following constraints:

* **Primary key constraint.** A primary key consists of one or more columns, and
  specifies that the value of each row of these combined columns must be unique
  within that table. A table can contain at most one primary key constraint.

  Some [data manipulation language (DML)](https://github.com/google/zetasql/blob/master/docs/data-manipulation-language.md)
  keywords may require the existence of a primary key.
  ZetaSQL also implicitly builds an index on the primary key. The
  default order of this index is ascending. The primary key can contain `NULL`
  values.
* **Unique constraint.** Specifies that one or more columns must contain only
  unique values. Unlike a primary key, more than one unique constraint can exist
  on a table.

## Indexes

An index allows the database engine to query a column or set of columns more
quickly. You can specify that sort order is ascending or descending. A unique
or primary key index defines an indexed column that is subject to the uniqueness
constraint.

<a name="pseudo-columns"></a>
## Pseudo-columns

ZetaSQL tables support pseudo-columns. Pseudo-columns contain data elements
that you can query like regular columns, but are not considered real columns in
the table. Pseudo-column values may not be physically stored with each row, but
the query engine will materialize a value for the column using some appropriate
mechanism.

For example, an engine might support a pseudo-column called `ROWNUM`, which
returns a number indicating the order in which a row was returned. You can then
construct a query like this:

```
SELECT ROWNUM, SingerId, FirstName, LastName FROM Singers
WHERE Status = "active"
```

Here's an example of rows returned by this query:

<table>
<thead>
<tr>
<th>ROWNUM</th>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>1</td>
<td>Marc</td>
<td>Richards</td>
</tr>
<tr>
<td>2</td>
<td>3</td>
<td>Lea</td>
<td>Martin</td>
</tr>
</tbody>
</table>

In this case,the schema for the **Singers** table does not define a column,
`ROWNUM`. Instead, the engine materializes the data only when requested.

To return a value of a pseudo-column, you must specify it in your query.
Pseudo-columns do not show up in `SELECT *` statements. For example:

```
SELECT * FROM singers
```

This query will return all named columns in the table, but won't include
pseudo-columns such as `ROWNUM`.

## Value tables

In addition to standard SQL tables, ZetaSQL supports _value tables_.
In a value table, rather than having rows made up of a list of columns, each row
is a single value of a specific type. These types of tables are common when
working with protocol buffers that may be stored in files instead of in the
database. 

<a id="value-table-example"></a>
For example, the following protocol buffer definition, `AlbumReview`, contains
data about the reviews for an album.

```
message AlbumReview {
  optional string albumtitle = 1;
  optional string reviewer = 2;
  optional string review = 3;
}
```

A list of `AlbumReview` protocol buffers is stored in a file, `AlbumReviewData`.

```
{albumtitle: "Songs on a Broken Banjo", reviewer: "Dan Starling", review: "Off key"}
{albumtitle: "Six and Seven", reviewer: "Alice Wayfarer", review: "Hurt my ears!"}
{albumtitle: "Go! Go! Go!", reviewer: "Eustace Millson", review: "My kids loved it!"}
```

The following query returns a stream of rows, with each row a value of type
AlbumReview.

```
SELECT a FROM AlbumReviewsData a
```

To get specific data, such as all album titles in
the table, you have two options. You can specify `albumtitle` as a protocol
buffer field:

```
SELECT a.albumtitle FROM AlbumReviewsData a
```

You can also access the top-level fields inside the value (if there
are any) like columns in a regular SQL table:

```
SELECT albumtitle FROM AlbumReviewsData
```

Value tables are not limited for use with compound data types.
A value table can consist of any supported ZetaSQL data type, although value
tables consisting of scalar types occur less frequently than structs or
protocol buffers.

### Returning query results as value tables

You can use ZetaSQL to return query results as a value table. This is useful
when you want to create a compound value, such as a protocol buffer, from a
query result and store it as a table that acts like a value table.
To return a query result as a
value table, use the `SELECT AS` statement. See
[Query Syntax](https://github.com/google/zetasql/blob/master/docs/query-syntax.md#value-tables)
for more information and examples.

#### Example: Copying protocol buffers using value tables

In some cases you might not want to work with the data within a protocol buffer,
but with the protocol buffer itself. 

Using `SELECT AS VALUE` can help you keep your ZetaSQL statements as simple
as possible. To illustrate this, consider the [AlbumReview](#value-table-example)
example specified earlier. To create a new table from this data, you could
write:

```
CREATE TABLE Reviews AS
SELECT albumreviews FROM AlbumReviewData albumreviews;
```

This statement creates a standard SQL table that has a single column,
`albumreviews`, which has a protocol buffer value of type
`AlbumReviewData`. To retrieve all album titles from this table, you'd need to
write a query similar to:

```
SELECT r.albumreviews.albumtitle
FROM Reviews r;
```

Now, consider the same initial `CREATE TABLE` statement, this time modified to
use `SELECT AS VALUE`:

```
CREATE TABLE Reviews AS
SELECT AS VALUE albumreviews FROM AlbumReview albumreviews;
```

This statement creates a value table, instead of a standard SQL table. As a
result, you can query any protocol buffer field as if it was a column. Now, if
you want to retrieve all album titles from this table, you can write a much
simpler query:

```
SELECT albumtitle
FROM Reviews;
```

### Set operations on value tables

Normally, a `SET` operation like `UNION ALL` expects all tables to be either
standard SQL tables or value tables. However, ZetaSQL allows you to combine
standard SQL tables with value tables&mdash;provided that the standard SQL table
consists of a single column with a type that matches the value table's type. The
result of these operations is always a value table.

For example, consider the following definition for a table,
**SingersAndAlbums**.

<table>
<thead>
<tr>
<th>Column Name</th>
<th>Data Type</th>
</tr>
</thead>
<tbody>
<tr>
<td>SingerId</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>AlbumId</td>
<td><code>INT64</code></td>
</tr>
<tr>
<td>AlbumReview</td>
<td><code>PROTO&lt;AlbumReview&gt;</code></td>
</tr>
</tbody>
</table>

Next, we have a file, `AlbumReviewData` that contains a list of `AlbumReview`
protocol buffers.

```
{albumtitle: "Songs on a Broken Banjo", reviewer: "Dan Starling", review: "Off key"}
{albumtitle: "Six and Seven", reviewer: "Alice Wayfarer", review: "Hurt my ears!"}
{albumtitle: "Go! Go! Go!", reviewer: "Eustace Millson", review: "My kids loved it!"}
```

The following query combines the `AlbumReview` data from the
**SingersAndAlbums** with the data stored in the `AlbumReviewData` file and
stores it in a new value table, **AllAlbumReviews**.

```
SELECT AS VALUE sa.AlbumReview FROM SingersAndAlbums sa
UNION ALL
SELECT a FROM AlbumReviewData a
```

### Pseudo-columns and value tables

The [Pseudo-columns](#pseudo-columns) section  describes how pseudo-columns
work with standard SQL tables. In most cases, pseudo-columns work the same with
value tables. For example, consider this query:

```
SELECT a.ROWNUM, a.albumtitle AS title FROM AlbumReviewData a
```

The following table demonstrates the result of this query:

<table>
<thead>
<tr>
<th>ROWNUM</th>
<th>title</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>"Songs on a Broken Banjo"</td>
</tr>
<tr>
<td>2</td>
<td>"Six and Seven"</td>
</tr>
<tr>
<td>3</td>
<td>"Go! Go! Go!"</td>
</tr>
</tbody>
</table>

This example works because `a` is an alias of the table `AlbumReviewData`, and
this table has a `ROWNUM` pseudo-column. As a result, `AlbumReviewData a` represents the scanned rows,
not the value.

However, if you tried to construct the query like this:

```
SELECT a.ROWNUM, a.albumtitle AS title FROM (SELECT a FROM AlbumReviewData a)
```

This query does not work. The reason it fails is because the subquery, `SELECT
a FROM AlbumReviewData a`, returns an `AlbumReviewData` value only, and this
value does not have a field called `ROWNUM`.

<!-- END CONTENT -->

