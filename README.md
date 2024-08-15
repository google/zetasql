## ZetaSQL - Analyzer Framework for SQL

ZetaSQL defines a SQL language (grammar, types, data model, semantics, and
function library) and
implements parsing and analysis for that language as a reusable component.
ZetaSQL is not itself a database or query engine. Instead,
it's intended to be used by multiple engines, to provide consistent
language and behavior (name resolution, type checking, implicit
casting, etc.). Specific query engines may implement a subset of features,
giving errors for unuspported features.
ZetaSQL's compliance test suite can be used to validate query engine
implementations are correct and consistent.

ZetaSQL implements the ZetaSQL language, which is used across several of
Google's SQL products, both publicly and internally, including BigQuery,
Spanner, F1, BigTable, Dremel, Procella, and others.

ZetaSQL and ZetaSQL have been described in these publications:

* (CDMS 2022) [ZetaSQL: A SQL Language as a Component](https://cdmsworkshop.github.io/2022/Slides/Fri_C2.5_DavidWilhite.pptx) (Slides)
* (SIGMOD 2017) [Spanner: Becoming a SQL System](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/46103.pdf) -- See section 6.
* (VLDB 2024) [SQL Has Problems. We Can Fix Them: Pipe Syntax in SQL](https://research.google/pubs/pub1005959/) -- Describes ZetaSQL's new pipe query syntax.

Some other documentation:

* [ZetaSQL Language Reference](docs/README.md)
* [ZetaSQL Resolved AST](docs/resolved_ast.md), documenting the intermediate representation produced by the ZetaSQL analyzer.
* [ZetaSQL Toolkit](https://github.com/GoogleCloudPlatform/zetasql-toolkit), a project using ZetaSQL to analyze and understand queries against BigQuery, and other ZetaSQL engines.

## Project Overview

The main components and APIs are in these directories under `zetasql/`:

* `zetasql/public`: Most public APIs are here.
* `zetasql/resolved_ast`: Defines the [Resolved AST](docs/resolved_ast.md), which the analyzer produces.
* `zetasql/parser`: The grammar and parser implementation. (Semi-public, since the parse trees are not a stable API.)
* `zetasql/analyzer`: The internal implementation of query analysis.
* `zetasql/reference_impl`: The reference implementation for executing queries.
* `zetasql/compliance`: Compliance test framework and compliance tests.
* `zetasql/public/functions`: Function implementations for engines to use.
* `zetasql/tools/execute_query`: Interactive query execution for debugging.
* `zetasql/java/com/google/zetasql`: Java APIs, implemented by calling a local RPC server.

Multiplatform support is planned for the following platforms:

 - Linux (Ubuntu 20.04 is our reference platform, but others may work).
   - gcc-9+ is required, recent versions of clang may work.
 - MacOS (Experimental)

We do not provide any guarantees of API stability and *cannot accept
contributions*.

## Running Queries with `execute_query`

The `execute_query` tool can parse, analyze and run SQL
queries using the reference implementation.

See [Execute Query](execute_query.md) for more details on using the tool.

You can run it using binaries from
[Releases](https://github.com/google/zetasql/releases), or build it using the
instructions below.

There are some runnable example queries in
[tpch examples](../zetasql/examples/tpch/README.md).

### Getting and Running `execute_query`
#### Pre-built Binaries

ZetaSQL provides pre-built binaries for `execute_query` for Linux and MacOS on
the [Releases](https://github.com/google/zetasql/releases) page. You can run
the downloaded binary like:

```bash
./execute_query_linux --web
```

Note the prebuilt binaries require GCC-9+ and tzdata. If you run into dependency
issues, you can try running `execute_query` with Docker. See the
[Run with Docker](#run-with-docker) section.

#### Running from a bazel build

You can build `execute_query` with Bazel from source and run it by:

```bash
bazel run zetasql/tools/execute_query:execute_query -- --web
```

#### Run with Docker

You can run `execute_query` using Docker. First download the pre-built Docker
image `zetasql` or build your own from Dockerfile. See the instructions in the
[Build With Docker](#build-with-docker) section.

Assuming your Docker image name is MyZetaSQLImage, run:

```bash
sudo docker run --init -it -h=$(hostname) -p 8080:8080 MyZetasqlImage execute_query --web
```

Argument descriptions:

* `--init`: Allows `execute_query` to handle signals properly.
* `-it`: Runs the container in interactive mode.
* `-h=$(hostname)`: Makes the hostname of the container the same as that of the
                    host.
* `-p 8080:8080`: Sets up port forwarding.

`-h=$(hostname)` and `-p 8080:8080` together make the URL address of the
web server accessible from the host machine.

Alternatively, you can run this to start a bash shell, and then run
`execute_query` inside:

```bash
sudo docker run --init -it -h=$(hostname) -p 8080:8080 MyZetasqlImage

# Inside the container bash shell
execute_query --web
```

## How to Build

### Build with Bazel

ZetaSQL uses [Bazel](https://bazel.build) for building and dependency
resolution. Instructions for installing Bazel can be found in
https://bazel.build/install. The Bazel version that ZetaSQL uses is specified in
the `.bazelversion` file.

Besides Bazel, the following dependencies are also needed:

* GCC-9+ or equivalent Clang
* tzdata

`tzdata` provides the support for time zone information. It is generally
available on MacOS. If you run Linux and it is not pre-installed, you can
install it with `apt-get install tzdata`.

Once the dependencies are installed, you can build or run ZetaSQL targets as
needed, for example:

```bash
# Build everything.
bazel build ...

# Build and run the execute_query tool.
bazel run //zetasql/tools/execute_query:execute_query -- --web

# The built binary can be found under bazel-bin and run directly.
bazel-bin/tools/execute_query:execute_query --web

# Build and run a test.
bazel test //zetasql/parser:parser_set_test
```

Some Mac users may experience build issues due to the Python error
`ModuleNotFoundError: no module named 'google.protobuf'`. To resolve it, run
`pip install protobuf==<version>` to install python protobuf. The protobuf
version can be found in the `zetasql_deps_step_2.bzl` file.

### Build with Docker

ZetaSQL also provides a `Dockerfile` which configures all the dependencies so
that users can build ZetaSQL more easily across different platforms.

To build the Docker image locally (called MyZetaSQLImage here), run:

```bash
sudo docker build . -t MyZetaSQLImage -f Dockerfile
```

Alternatively, ZetaSQL provides pre-built Docker images named `zetasql`. See the
[Releases](https://github.com/google/zetasql/releases) page. You can load the
downloaded image by:

```bash
sudo docker load -i /path/to/the/downloaded/zetasql_docker.tar
```

To run builds or other commands inside the Docker environment, run this command
to open a bash shell inside the container:

```bash
# Start a bash shell running inside the Docker container.
sudo docker run -it MyZetaSQLImage
```

Then you can run the commands from the [Build with Bazel](#build-with-bazel)
section above.


## Differential Privacy
For questions, documentation, and examples of ZetaSQL's implementation of
Differential Privacy, please check out
(https://github.com/google/differential-privacy).

## Versions

ZetaSQL makes no guarantees regarding compatibility between releases.
Breaking changes may be made at any time. Our releases are numbered based
on the date of the commit the release is cut from. The number format is
YYYY.MM.n, where YYYY is the year, MM is the two digit month, and n is a
sequence number within the time period.

## License

[Apache License 2.0](LICENSE)

## Support Disclaimer
This is not an officially supported Google product.
