
<!-- This file is auto-generated. DO NOT EDIT.                               -->

<!-- BEGIN CONTENT -->

# Modules

## What are modules?

Modules are a set of ZetaSQL DDL statements that do not have
permanent side effects. Each module is self-contained:

+ Each module has its own namespace. This namespace is empty until `CREATE`
  statements in the module add objects to the module namespace.
+ Module statements cannot have permanent side effects. They cannot:
    + Create permanent objects.
    + Insert data.
    + Modify data.
+ Modules allow public and private object definitions for proper encapsulation.

The purpose of a module is to keep related business logic in one location.
Inside the module, you can create user-defined functions and constants, and
import other modules.

ZetaSQL limits the duration and side effects of module objects to the
invoking *session*. In the context of ZetaSQL modules, a session is
a set of related statements and objects that form a [unit of
work](https://en.wikipedia.org/w/index.php?title=Unit_of_work).

## Creating a module

To create a module, you create a file that contains a `MODULE` statement and
subsequent `IMPORT` and `CREATE` statements. The file extension `.sqlm` must
appear at the end of the filename of the module file.

Modules support the following statements:

+ `MODULE`
+ `IMPORT MODULE`
+ `CREATE ( PUBLIC | PRIVATE ) [ TABLE ] FUNCTION`
+ `CREATE ( PUBLIC | PRIVATE ) CONSTANT`

Modules do not support statements that return results, run queries, or have side
effects.  For example, the following statements are not supported inside of
modules:

+ `SELECT` queries
+ `UPDATE` statements

### Declaring a module

The first statement in a module must be a valid `MODULE` statement
which defines the module name:

<pre>
MODULE <span class="var">identifier_path</span> [ OPTIONS (...)];
</pre>

Each module file must contain only one `MODULE` statement.

By convention, an `IMPORT` statement for this module will generally include the
`identifier_path` in the `IMPORT` statement. For clarity, the `identifier_path`
should reflect the path to the module file that contains the `MODULE` statement.
 For example, if a module file is stored at
`search_path/x/y/z.sqlm`, then the `MODULE` statement will be:

```sql
MODULE x.y.z;
```

And the `IMPORT` statement will be:

```sql
IMPORT MODULE x.y.z;
```

The `IMPORT` statement should not include the `.sqlm` file extension.

### Creating objects within modules

Modules can contain `CREATE` statements to create objects within the module.

#### Specifying Public vs. Private Objects

All `CREATE` statements must indicate if the created object is available outside
of the module in the importing session (public), or only available internally
within the module (private). To specify these properties, use the `PUBLIC` or
`PRIVATE` modifier in the `CREATE` statement.

**Examples**

The following example creates a public function, which the invoking session
can execute.

```sql
CREATE PUBLIC FUNCTION foo(a INT64) AS (a + 1);
```

The following example creates a private function, which only statements within
the same module can execute.

```sql
CREATE PRIVATE FUNCTION bar(b INT64) AS (b - 1);
```

#### Creating user-defined functions and table functions

Modules support creation of
[user-defined functions](https://github.com/google/zetasql/blob/master/docs/user-defined-functions.md),
including
[table-valued functions](https://github.com/google/zetasql/blob/master/docs/user-defined-functions.md#tvfs)
with scalar and templated arguments.

The `TEMP` keyword is not allowed in `CREATE ( PUBLIC | PRIVATE ) FUNCTION`
statements in modules. `TEMP` objects are not meaningful within a module since
the lifetime of the object is the lifetime of the module.

Note that SQL UDFs/TVFs defined in modules cannot directly access any database
schema tables, and therefore cannot rely on the existence of tables in a
database. To reference a database table in a module TVF, the table must be
passed in as a TVF argument of type `ANY TABLE`.

**Examples**

The following example creates a public user-defined function.

```sql
CREATE PUBLIC FUNCTION udf_sample(a INT64) AS (a+1);
```

The following example creates a public templated user-defined function with a
scalar argument.

```sql
CREATE PUBLIC FUNCTION udf_scalar(a ANY TYPE) AS (a+1);
```

The following example creates a public table-valued function with a scalar
argument.

```sql
CREATE PUBLIC TABLE FUNCTION tvf_scalar(a INT64)
AS (SELECT a, udf.udf_sample(a) b);
```

The following example creates a public table-valued function with a table
argument.

```sql
CREATE PUBLIC TABLE FUNCTION tvf_sample2(x TABLE<a STRING, b INT64>)
AS (SELECT a, SUM(b) AS sum_b FROM x GROUP BY a);
```

The following example creates a public templated table-valued function.

```sql
CREATE PUBLIC TABLE FUNCTION tvf_templated(a ANY TYPE, table1 ANY TABLE)
AS (SELECT a, b.* FROM table1);
```

### Referencing module objects from within the same module

Statements in a module can reference other objects in the same module.
Statements can reference objects whose `CREATE` statements appear before
or after that referencing statement.

**Example**

The following example module declares the name of the module, and creates two
public functions and one private function. The second function references the
other two functions, including the function that appears after it in the module.

```sql
MODULE a.b.c;
CREATE PUBLIC FUNCTION foo(x int64) AS (x);
CREATE PUBLIC FUNCTION bar(y int64) AS (foo(y) + baz(y));
CREATE PRIVATE FUNCTION baz(z int64) AS (z);
```

Object references cannot be circular: if a function directly or indirectly
references a second function, then that second function cannot reference the
original function.

## Using an existing module

You can use an existing module by importing it into a session or into another
module.

### Importing a module into a session

To import a module into a session, use the `IMPORT MODULE`
statement.

**Syntax**

<pre>
IMPORT MODULE <span class="var">module_identifier_path</span> [AS alias];
</pre>

This imports a module and creates a namespace visible to the importing session
containing public objects exported from the module.

The `module_identifier_path` is a unique module name that corresponds to the
path ID in the [module declaration](#declaring-a-module).

The `alias` provides the namespace that the `IMPORT MODULE`
statement creates. If `alias` is absent, then the namespace will be the
last name in the `module_identifier_path`.

**Examples**

The following example statement imports the module with the identifier path
`x.y.z` into namespace `z`:

```sql
IMPORT MODULE x.y.z;
```

The following example statement imports the same module with the alias `foo`
into namespace `foo`.

```sql
IMPORT MODULE x.y.z AS foo;
```

### Referencing module objects from a session

Once you have imported a module into a session, you can reference the public
objects in that module from the session. Use the namespace of the module or its
alias to reference the objects in the module.

**Example**

In the following example, the `IMPORT` statement imports the module with the
identifier path `x.y.z` into namespace `z`, and then executes a public
function `bar` from inside of that module.

```sql
IMPORT MODULE x.y.z;
SELECT z.bar(a, b);
```

If the `IMPORT` statement includes an alias, then the statement creates the
namespace with that alias. Use that alias as the identifier path prefix for
the referenced object.

**Example**

In the following example, the `IMPORT` statement assigns alias `foo` to the
module with the identifier path `x.y.z`, and the `SELECT` statement executes a
public function `bar` from inside of that module.

```sql
IMPORT MODULE x.y.z AS foo;
SELECT foo.bar(a, b);
```

### Importing a module into another module

To import a module into another module, use the same syntax as when [importing a
module into a session](#importing-a-module-into-a-session).

+ Imports cannot be circular. For example, if `module1` imports `module2`,
  then `module2` cannot directly or indirectly import `module1`.
+ A module cannot import itself.

### Referencing module objects from another module

Once you have imported a module into another module, you can reference the
public objects that the imported module creates. Use the same syntax as in an
invoking session.

**Example**

In the following example, the `IMPORT` statement imports the module with the
identifier path `x.y.z`, and then creates a function `foo` which
references the public function `bar` from inside of the imported module.

```sql
MODULE a.b.c;
IMPORT MODULE x.y.z;
CREATE PUBLIC FUNCTION foo(d int) AS (z.bar(d));
```

If the `IMPORT` statement includes an alias, you can reference objects from the
imported module inside the importing module using the alias as the identifier
path.

```sql
MODULE a.b.c;
IMPORT MODULE x.y.z AS baz;
CREATE PUBLIC FUNCTION foo(d int) AS (baz.bar(d));
```

<!-- END CONTENT -->

