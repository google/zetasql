#!/bin/bash
#
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

MODE=$1

CC=/usr/bin/gcc
CXX=/usr/bin/g++

if [ "$MODE" = "build" ]; then
  # Build everything.
  bazel build ${BAZEL_ARGS} -c opt ...
elif [ "$MODE" = "execute_query" ]; then
  # Install the execute_query tool.
  bazel build ${BAZEL_ARGS} -c opt --dynamic_mode=off //zetasql/tools/execute_query:execute_query
  # Move the generated binary to the home directory so that users can run it
  # directly.
  cp /zetasql/bazel-bin/zetasql/tools/execute_query/execute_query $HOME/bin/execute_query
  # Remove the downloaded and generated artifacts to keep the image small.
  bazel clean --expunge
else
  echo "Unknown mode: $MODE"
  echo "Supported modes are: build, execute_query"
  exit 1
fi
