## ZetaSQL - Analyzer Framework for SQL

ZetaSQL defines a language (grammar, types, data model, and semantics) as well
as a parser and analyzer.  It is not itself a database or query engine. Instead
it is intended to be used by multiple engines wanting to provide consistent
behavior for all semantic analysis, name resolution, type checking, implicit
casting, etc. Specific query engines may not implement all features in the
ZetaSQL language and may give errors if specific features are not supported. For
example, engine A may not support any updates and engine B may not support
analytic functions.

## Status of Project and Roadmap

This codebase is being open sourced in multiple phases:

1. Parser and Analyzer **Complete**
   - Initial release includes only a subset of tests
2. Reference Implementation
3. Compliance Tests
   - includes framework for validating compliance of arbitrary engines
4. Misc tooling

Until all this code is released, we cannot provide any guarantees of API
stability and cannot accept contributions. We will also be releasing more
documentation over time, particular related to developing engines with this
framework. Documentation on the [language](docs/) itself is fairly
complete.

## How to Build

ZetaSQL uses [bazel](https://bazel.build) for building and
dependency resolution. After installing bazel (version >=0.22), simple run:

```bazel build zetasql/...```

### With docker
 TODO: Add docker build script.

## License

[Apache License 2.0](LICENSE)

## Support Disclaimer
This is not an officially supported Google product.
