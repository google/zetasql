This directory contains some example scripts demonstrating
[SQL pipe syntax](https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md).

These mostly use tables from the TPC-H schema. For more information, see
[examples/tpch](https://github.com/google/zetasql/tree/master/zetasql/examples/tpch)
which also includes standard TPC-H queries in standard syntax and pipe syntax.

The scripts here can be run in the `execute_query` tool by selecting the `tpch`
catalog in the web UI with `--web` or using `--catalog=tpch` flag on the command
line.
See [execute\_query](https://github.com/google/zetasql/blob/master/execute_query.md) for more details on this tool.

For more information on pipe query syntax, see the
[reference documentation](https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md)
and [research paper](https://research.google/pubs/pub1005959/).
