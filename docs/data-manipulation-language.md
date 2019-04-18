
<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Data Manipulation Reference

ZetaSQL supports the following statements for manipulating data:

+ `INSERT`
+ `UPDATE`
+ `DELETE`

<a name="example-data"></a>
## Example data

The sections in this topic use the following table schemas.

<a name="singers-table"></a>
### Singers table

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
<td><code>INT64 NOT NULL</code></td>
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

<pre>
message SingerMetadata {
  optional string    nationality = 1;
  repeated Residence residence   = 2;<br/>
  message Residence {
    required int64  start_year   = 1;
    optional int64  end_year     = 2;
    optional string city         = 3;
    optional string country      = 4 [default = "USA"];
  }
}
</pre>

The proto, `Album`, has the following definition:

<pre>
message Album {
  optional string title = 1;
  optional int32 tracks = 2;
  repeated string comments = 3;
  repeated Song song = 4;<br/>
  message Song {
    optional string songtitle = 1;
    optional int32 length = 2;
    repeated Chart chart = 3;<br/>
    message Chart {
      optional string chartname = 1;
      optional int32 rank = 2;
    }
  }
}
</pre>

<a name="concerts-table"></a>
### Concerts table

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
<td>VenueId</td>
<td><code>INT64 NOT NULL</code></td>
<td><code>&lt;auto-increment&gt;</code></td>
</tr>
<tr>
<td>SingerId</td>
<td><code>INT64 NOT NULL</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>ConcertDate</td>
<td><code>DATE NOT NULL</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>BeginTime</td>
<td><code>TIMESTAMP</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>EndTime</td>
<td><code>TIMESTAMP</code></td>
<td>&nbsp;</td>
</tr>
<tr>
<td>TicketPrices</td>
<td><code>ARRAY&lt;INT64&gt;</code></td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

## INSERT statement

Use the `INSERT` statement when you want to add new rows to a table.

<pre>
INSERT [[OR] IGNORE | REPLACE | UPDATE] [INTO] target_name
 (column_1 [, ..., column_n ] )
 input <br>[ASSERT_ROWS_MODIFIED m]<br/>
input ::=
 VALUES (expr_1 [, ..., expr_n ] )
        [, ..., (expr_k_1 [, ..., expr_k_n ] ) ]
| SELECT_QUERY<br/>
expr ::= value_expression | DEFAULT
</pre>

<a name="statement-rules"></a>
`INSERT` statements must comply with the following rules:

+ Column names must be specified.
+ Duplicate names are not allowed in the list of target columns.
+ Values must be added in the same order as the specified columns.
+ The number of values added must match the number of specified columns.
+ Values must have a type that is [compatible](#compatible-types) with the
  target column.
+ All non-null columns must appear in the column list, and have a non-null value
  specified.

Statements that don't follow these rules will raise an error.

The following statement inserts a new row to the
[Singers example table](#singers-table):

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Catalina", "Smith", "1990-08-17", "active", "nationality:'U.S.A.'");
```

This next statement inserts multiple rows into the table:

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (6, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (7, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'");
```

You can construct `INSERT` statements to insert the results of a query. For
example, the following statement inserts a new concert for a singer into
the [Concerts table](#concerts-table).

```
INSERT INTO Concerts
  (VenueId, SingerId, ConcertDate, BeginTime, EndTime, TicketPrices)
SELECT c.VenueId, c.SingerId, DATE "2015-06-01", c.BeginTime, c.EndTime, c.TicketPrices
  FROM Concerts c WHERE c.SingerId = 1;
```

<a name="compatible-types"></a>
### Value type compatibility

Values added with an `INSERT` statement must be compatible with the target
column's type. A value's type is considered compatible with the target column's
type if one of the following criteria are met:

+ The value type matches the column type exactly. (For example, inserting a
value of type `INT32` in a column that also has a type of `INT32`.)
+ The value type is one that can be implicitly coerced into another type. See
the [Coercion](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#coercion) of the
[Expressions, Functions, and Operators](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md)
topic for more information.
+ The value can be assigned after applying a narrowing integer or floating point
cast. For example, if you attempt to add a value of `INT64` into a column that
supports `INT32`, ZetaSQL automatically adds a narrowing cast to
`INT32`.

### Default values

If the target table supports a default value for a
column, you can omit the column from the `INSERT` statement. Alternatively, you
can insert the default value explicitly using the `DEFAULT` keyword.

**Note:** If a column does not require a value, the default value is typically
`NULL`.

The following example inserts default values for all unspecified columns.
For example, the [Singers table](#singers-table) has two columns that have
defined default values&mdash;the `SingerId` column, which is auto-incremented;
 and the `Status` column, which has a default value of `active`. For all other
columns, the default value is `NULL`.

```
INSERT INTO Singers
  (FirstName)
VALUES ("Neil");
```

If this was the first row added to the table, a `SELECT` statement querying
this row would return the following:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>Status</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>1</td>
<td>Neil</td>
<td>NULL</td>
<td>NULL</td>
<td>active</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>

If you construct an `INSERT` statement to use a default value for a
column and that column does not support a default value, the statement fails.
For example, the `Singers` table contains a column, `SingerId`, which cannot be
`NULL`. Consequently, the following statement fails, because the `SingerId` does
not support a default value:

```
INSERT INTO Singers
  (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (DEFAULT, "Andrew", "Duneskipper", "2000-01-15", "active", NULL);
```

### Duplicate rows

By default, an `INSERT` statement fails when it tries to add a duplicate row.
A duplicate row is defined as one for which the
primary key already exists in the table.

You can change this behavior by choosing one of the following options for
handling duplicate rows:

+ `IGNORE`
+ `REPLACE`
+ `UPDATE`

Your statement would resemble the following:

<pre>
INSERT [OR] IGNORE | REPLACE | UPDATE <em>&lt;rest of statement&gt;</em>
</pre>

These keywords require that your table includes a primary key. If the table does
not have a primary key, the statement fails.

In cases where an `INSERT` statement attempts to add the same row twice,
ZetaSQL treats the inserts as if they were applied sequentially. This means
that:

+ For `INSERT IGNORE` statements, the first row is inserted, and the rest are
ignored.
+ For `INSERT REPLACE` statements, all but the last insert are replaced.
+ For `INSERT UPDATE` statements, updates are applied sequentially, ending with
the last update.

#### INSERT IGNORE

You can instruct your `INSERT` statement to skip any rows that have duplicate
primary keys by using the `IGNORE` keyword.

For example, the following statement instructs ZetaSQL to skip the
row if it already exists in the table:

```
INSERT OR IGNORE INTO Singers
    (FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES ("Catalina", "Smith", "1990-08-17", DEFAULT, "nationality:'U.S.A.'");
```

`INSERT IGNORE` ignores only duplicate rows that have the same primary key.
ZetaSQL raises an error if any other constraint violation occurs, including
duplicate keys in unique indexes.

#### INSERT REPLACE

You can instruct your `INSERT` statement to replace any rows that have duplicate
primary keys by using the `REPLACE` statement. Replaced rows have the same
primary key as the original row.

In the following example, the `REPLACE` keyword is used instead of `IGNORE`.
This time, the duplicate row replaces the existing row in the table.

```
INSERT OR REPLACE INTO Singers
    (FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES ("Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

Using `REPLACE` is the same as deleting the existing row and inserting a
replacement one. As a result, any columns that you do not mention in the
replacement row are cleared and replaced with their default values. If you want
your statement to preserve the values of unspecified columns, use
`INSERT UPDATE`.

#### INSERT UPDATE

With an `INSERT UPDATE` statement, the statement updates the columns specified
for one or more rows. Columns that are not listed in the `INSERT` statement
remain unchanged.

The following example illustrates how `INSERT UPDATE` changes an existing row.
In this case, the status is changed from `active` to `inactive`.

```
INSERT OR UPDATE INTO Singers
    (SingerId, Status)
VALUES (5, "inactive");
```

Notice that if the row does not exist, the previous statement inserts a new row
with values in the specified fields, and all other values set to their
corresponding defaults.

When you use the `UPDATE` keyword, any column that you do not specify remains
unchanged.

### INSERT and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows are added each time you
use an `INSERT` statement by using the `ASSERT_ROWS_MODIFIED` keyword.

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to insert, and ZetaSQL compares that number
against the number of rows actually modified. This check occurs before
ZetaSQL commits the change to the database. If the row count matches,
ZetaSQL commits the changes.  Otherwise, ZetaSQL returns
an error and rolls back the statement.

When calculating the number of rows modified, ZetaSQL includes rows
that were inserted, updated, or replaced. It does not count rows that were
skipped through the `IGNORE` keyword. To illustrate this, consider the following
example of a **Singers** table:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality: 'U.S.A.'</td>
<td>NULL</td>
</tr>
</tbody>
</table>

```
INSERT OR UPDATE INTO Singers
    (SingerId, FirstName, LastName, Birthdate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (6, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'")
ASSERT_ROWS_MODIFIED 2;
```

One of the value expressions in the `INSERT` statement adds a new row, while the
second updates an existing one. Both result in a total of 2 modified rows, which
matches the `ASSERT_ROWS_MODIFIED 2` clause of the statement.

The following statement uses `INSERT IGNORE` to add rows to the table.

```
INSERT OR IGNORE INTO Singers
    (SingerId, FirstName, LastName, Birthdate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'"),
       (7, "Edie", "Silver", "1998-01-23", "active", "nationality:'U.S.A.'")
ASSERT_ROWS_MODIFIED 1;
```

In this case, there is a collision when inserting a new row with a `SingerId` of
`5`, because a row with that `SingerId` value already exists. This statement
inserts one row, which matches the `ASSERT_ROWS_MODIFIED 1` statement.

### INSERT examples

Add a new row to the `Singers` table.

```
INSERT INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Catalina", "Smith", "1990-08-17", DEFAULT, "nationality:'U.S.A.'");
```

**RESULT:** New singer, Catalina Smith, added to table.

Try to add a singer, but only if the `SingerId` is not already in the table.

```
INSERT OR IGNORE INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

**RESULT:** Table unaffected. Catalina Smith remains in the table.

Try to add another singer and replace an existing row if it has the same
`SingerId`.

```
INSERT OR REPLACE INTO Singers
    (SingerId, FirstName, LastName, BirthDate, Status, SingerInfo)
VALUES (5, "Zak", "Sterling", "1996-03-12", "active", "nationality:'U.S.A.'");
```

**RESULT:** Singer, Catalina Smith, replaced with Zak Sterling.

Add a singer to the table, or update a singer if they already exist.

```
INSERT OR UPDATE INTO Singers
    (SingerId, FirstName, LastName, Status)
VALUES (5, "Zak", "Sterling", "inactive");
```

Existing row for Zak Sterling updated. His status is now `inactive`. All other
values, such as `BirthDate`, remain unchanged.

## DELETE Statement

Use the `DELETE` statement when you want to delete rows from a table.

<pre>
DELETE [FROM] target_name
WHERE condition<br>[ASSERT_ROWS_MODIFIED n]
</pre>

**Note**: `DELETE` statements must comply with all
[statement rules](#statement-rules).

### WHERE keyword

Each time you construct a `DELETE` statement, you must use the `WHERE` keyword,
followed by a condition. For example:

```
DELETE FROM Singers
WHERE LastName = "Sterling";
```

You can use the identifier for the target of the update as a range variable
within the `WHERE` clause. For example:

```
DELETE FROM Singers s
WHERE s.LastName = "Sterling";
```

Using an identifier can help make your `DELETE` statement more explicit with
regards to what data ZetaSQL should update.

The `WHERE` keyword is mandatory for any `DELETE` statement. However, you can
set the condition for the `WHERE` keyword to be true, which results in the
statement deleting all rows in the table.

```
DELETE FROM Singers s
WHERE true;
```

### DELETE and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows were deleted each time
you use a `DELETE` statement. You implement this confirmation through the
`ASSERT_ROWS_MODIFIED` keyword.

```
DELETE FROM Singers
WHERE LastName = "Sterling"
ASSERT_ROWS_MODIFIED 1;
```

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to delete, and compare that number against the number of rows
actually deleted. This check occurs before ZetaSQL commits the change
to the database. If the row count matches, ZetaSQL commits the
changes. Otherwise, ZetaSQL returns an error and rolls back the
statement.

## UPDATE statement

Use the `UPDATE` statement when you want to update existing rows within a table.

<pre>
UPDATE target_name
SET update_item [, update_item]*
WHERE condition<br>[ASSERT_ROWS_MODIFIED n]<br/>
update_item ::= path_expression = expression
              | path_expression = DEFAULT
              | (dml_stmt)<br/>
dml_stmt ::= insert_statement | update_statement | delete_statement
</pre>

**Note**: `UPDATE` statements must comply with all
[statement rules](#statement-rules) and use
[compatible types](#compatible-types).

### WHERE keyword

Each `UPDATE` statement must include the `WHERE` keyword, followed by a
condition. For example, the following statement illustrates an `UPDATE`
statement that modifies the row with the primary key, `5`.

```
UPDATE Singers s
SET s.Status = "inactive"
WHERE s.SingerId = 5;
```

To update all rows in a table, use `WHERE true`. The following statement sets
the SingerInfo value in all rows to `NULL` (the default value for the field).

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE true;
```

### UPDATE and ASSERT_ROWS_MODIFIED

With ZetaSQL, you can confirm how many rows were added each time you
use an `UPDATE` statement. You implement this confirmation through the
`ASSERT_ROWS_MODIFIED` keyword.

**Note**: A row still counts as modified even if the updated values are
identical to the previous values.

With the `ASSERT_ROWS_MODIFIED` keyword, you specify the number of rows you
expect the command to update. If the row count matches, ZetaSQL
commits the changes. Otherwise, ZetaSQL returns an error and rolls
back the statement.

The `ASSERT_ROWS_MODIFIED` keyword is helpful when you want to be sure your
statement changes only a specific number of rows. For example, the following
statement verifies that exactly one row was found and updated.

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

### Setting new values

Each `UPDATE` statement requires a `SET` keyword. This keyword identifies the
columns that you want to update.

You specify values for specific columns using the syntax <em>column_name =
expression</em>. Expressions can see all the columns within a table. For
example:

```
UPDATE Singers s
SET s.SingerId = s.SingerId + 1
WHERE true
```

Any assignments within the expression happen simultaneously. For example, the
following statement swaps the values in the `FirstName` and `LastName` column.

```
UPDATE Singers s
SET s.FirstName = s.LastName,
    s.LastName = s.FirstName
WHERE true;
```

Within the `SET` clause, you can use the identifier for the target of the update
as a table alias. For example:

```
UPDATE Singers s
SET s.LastName = "Solo"
WHERE s.SingerId = 5;
```

Using an identifier can help make your `UPDATE` statement more explicit with
regards to what data ZetaSQL should update.

### Specifying columns

An `UPDATE` statement must explicitly state which columns you want to update.
Any column not included in the `UPDATE` statement remains unmodified.

In the following example, notice that only the `Status` column is
specified &mdash; consequently, the other columns
(such as `SingerId`, `FirstName`, `LastName`, and `Birthdate`) remain unchanged.

```
UPDATE Singers s
SET s.Status = "inactive"
WHERE s.SingerId = 5;
```

You can specify the columns to update in any order; however, you can list each
column only once. For example, the following statement is invalid and would be
rejected by ZetaSQL:

```
UPDATE Singers s
SET s.LastName = "Solo", s.LastName = "Organa"
WHERE s.SingerId = 5;
```

`UPDATE` statements must use values that are compatible with the corresponding
column's type. For example, a value of `1991-10-02` works for a column of type
`Date`, but a value of `October 2, 1991` would not. You can, however, use values
that can be cast to the type of the corresponding column. Casting happens
automatically for numerical values (this is also referred to as
[coercion](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#coercion)).

If you attempt to set a column with an invalid value, the statement fails.
For example, the following statement does not work:

```
UPDATE Singers s
SET s.Birthdate = "October 2, 1991"
WHERE s.SingerId = 5;
```

### Returning columns to default values

You can use an `UPDATE` statement to return any value to its corresponding
default by using the `DEFAULT` keyword. In cases where the column contains a
protocol buffer field, that field is cleared.

For example, consider the following **Singers** table:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality: 'USA'"</td>
<td>NULL</td>
</tr>
<tr>
<td>6</td>
<td>Zak</td>
<td>Sterling</td>
<td>1989-1-13</td>
<td>"nationality: 'Australia'"</td>
<td>NULL</td>
</tr>
</tbody>
</table>

If you run the following statement:

```
UPDATE Singers s
SET s.SingerInfo = DEFAULT
WHERE SingerId = 6;
```

The table is updated as follows:

<table>
<thead>
<tr>
<th>SingerId</th>
<th>FirstName</th>
<th>LastName</th>
<th>BirthDate</th>
<th>SingerInfo</th>
<th>Albums</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>Alice</td>
<td>Trentor</td>
<td>1991-10-2</td>
<td>"nationality:'USA'"</td>
<td>NULL</td>
</tr>
<tr>
<td>6</td>
<td>Zak</td>
<td>Sterling</td>
<td>1989-1-13</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>

Notice that the row for `Zak Sterling` now has `NULL` in the `SingerInfo`
column, because `NULL` is the default value for that column. The following
example has the same effect:

```
UPDATE Singers s
SET s.SingerInfo = NULL
WHERE SingerId = 6;
```

### Updating fields

ZetaSQL allows you to update non-repeating or repeating fields within
protocol buffers. To illustrate how to update a non-repeating field, consider
the [Singers example table](#singers-table).  It contains a column, `Albums`, of
type `PROTO<Albums>`, and `Albums` contains a non-repeating field `tracks`. The
following statement updates the value of `tracks`:

```
UPDATE Singers s
SET s.Albums.tracks = 15
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

An update can replace a repeated field using an array of values, as shown below.

```
UPDATE Singers s
SET s.Albums.comments = ["A good album!", "Hurt my ears!", "Totally inedible."]
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

### Nested updates

Inside a parent update statement you can construct DML statements that modify
a repeated field of a protocol buffer or an array.  Such statements are called
**nested updates**.

For example, the `Albums` column contains a repeated field `comments`. This
nested update statement adds a comment to an album:

```
UPDATE Singers s
SET (INSERT s.Albums.comments
     VALUES ("Groovy!"))
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

`Albums` also contains a repeated protocol buffer, `Song`, which provides
information about a song on an album. This nested update statement updates the
album with a new song:

```
UPDATE Singers s
SET (INSERT s.Albums.Song(Song)
     VALUES ("songtitle: 'Bonus Track', length: 180"))
WHERE s.SingerId = 5 AND s.Albums.title = "Fire is Hot";
```

If the repeated field is another protocol buffer, you can provide the
protocol buffer as a string literal. For example, the following statement adds a
new song to an album and updates the number of tracks. Notice that this
statement uses `ASSERT_ROWS_MODIFIED` to ensure that only one `Singer` is
updated.

```
UPDATE Singers s
SET (INSERT s.Albums.Song
     VALUES ('''songtitle: 'Bonus Track', length:180''')),
     s.Albums.tracks = 16
WHERE s.SingerId = 5 and s.Albums.title = "Fire is Hot"
ASSERT_ROWS_MODIFIED 1;
```

You can also nest a nested update statement in another nested update statement.
For example, the `Song` protocol buffer itself has another repeated
protocol buffer, `Chart`, which provides information on what chart the song
appeared on, and what rank it had.

The following statement adds a new chart to a song.

```
UPDATE Singers s
SET (UPDATE s.Albums.Song so
    SET (INSERT INTO so.Chart
         VALUES ("chartname: 'Galaxy Top 100', rank: 5"))
    WHERE so.songtitle = "Bonus Track")
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

This next statement updates the chart to reflect a new rank for the song. Notice
that each inner `UPDATE` statement uses `ASSERT_ROWS_MODIFIED 1` to ensure that
only one update is made.

```
UPDATE Singers s
SET (UPDATE s.Albums.Song so
     SET (UPDATE so.Chart c
          SET c.rank = 2
          WHERE c.chartname = "Galaxy Top 100"
          ASSERT_ROWS_MODIFIED 1)
     WHERE so.songtitle = "Bonus Track"
     ASSERT_ROWS_MODIFIED 1)
WHERE s.SingerId = 5
ASSERT_ROWS_MODIFIED 1;
```

ZetaSQL treats an array or repeated field inside a row that matches
an `UPDATE WHERE` clause as a table, with individual elements of the array or
field treated like rows. These rows can then have nested DML statements run
against them, allowing you to delete, update, and insert data as needed.

#### Modifying multiple fields

The previous sections demonstrated how to update a single value within a
compound data type. You can also perform multiple updates to a compound data
type within a single statement. For example:

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (UPDATE s.Albums.Song song SET song.songtitle = 'No, This Is Rubbish' WHERE song.songtitle = 'This Is Pretty Good'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'"))
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

Nested queries are processed as follows:

1. Delete all rows that match a `WHERE` clause of a `DELETE` statement.
2. Update any remaining rows that match a `WHERE` clause of an `UPDATE`
statement. Each row must match at most one `UPDATE WHERE` clause, or the
statement fails due to overlapping updates.
3. Insert all rows in `INSERT` statements.

You must construct nested statements that affect the same field in the following
order:

+ `DELETE`
+ `UPDATE`
+ `INSERT`

For example:

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (UPDATE s.SingerInfo.Residence r SET r.end_year = 2015 WHERE r.City = 'Eugene'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'"))
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

The following statement is invalid, because the `UPDATE` statement
happens after the `INSERT` statement.

```
UPDATE UpdatedSingers s
SET
    (DELETE FROM s.SingerInfo.Residence r WHERE r.City = 'Seattle'),
    (INSERT s.Albums.Song VALUES ("songtitle: 'The Second Best Song'")),
    (UPDATE s.SingerInfo.Residence r SET r.end_year = 2015 WHERE r.City = 'Eugene')
WHERE SingerId = 3 AND s.Albums.title = 'Go! Go! Go!';
```

**Note:** In nested queries, you cannot use `INSERT OR` statements. These types
of statements don't work because arrays and other compound data types do not
always have a primary key, so there is no applicable definition of duplicate
rows.

You can also add `ASSERT_ROWS_MODIFIED` to nested statements, as shown here:

```
UPDATE Singers
SET
    (DELETE FROM AlbumTitles a WHERE a.AlbumTitles = "Too Many Vegetables"
        ASSERT_ROWS_MODIFIED 1),
    (UPDATE AlbumTitles a SET a.AlbumTitles = "Album IV"
        WHERE a.AlbumTitles = "Album Four"),
    (INSERT AlbumTitles VALUES ("The Sloth and the Tiger"));
```

<!-- END CONTENT -->

