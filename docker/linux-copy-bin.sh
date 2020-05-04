#!/bin/bash
set -ex
id=$(docker create matts966/zetasql-formatter)
docker cp $id:/usr/bin/format ./bin/linux/zetasql-formatter
docker rm -v $id
