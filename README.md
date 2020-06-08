## ZetaSQL Formatter

This repository is forked from [google/zetasql](https://github.com/google/zetasql) and provides SQL formatter with preserved comments. This formatter can be applied to mainly BigQuery and SpanSQL.

```bash
# To install by pre-commit
pip intall pre-commit
# Copy template .pre-commit-config.yaml to your project.
cp .pre-commit-config.yaml ./path/to/your/project
cd ./path/to/your/project && pre-commit install
```

```bash
# To install for MacOSX
wget https://github.com/Matts966/zetasql-formatter/releases/download/latest/zetasql-formatter_darwin_amd64.zip && sudo unzip zetasql-formatter_darwin_amd64.zip -d /usr/local
```

```bash
# To install for Linux
wget https://github.com/Matts966/zetasql-formatter/releases/download/latest/zetasql-formatter_linux_x86_64.zip && sudo unzip zetasql-formatter_linux_x86_64.zip -d /usr/local
```

```bash
# To apply formatter
zetasql-formatter [paths]
# To apply formatter to queries in a directory using Docker
docker run -it --rm -v `pwd`:/home:Z matts966/zetasql-formatter:latest [paths]
```

```bash
# To build (with heavy work load in disk, memory and CPU)
make build
```

## License

[Apache License 2.0](LICENSE)

## Support Disclaimer
This is not an officially supported Google product.
