//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Tool for running a query against a Catalog constructed from various input
// sources. Also serves as a demo of the PreparedQuery API.
//
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "zetasql/base/status_macros.h"

static absl::Status InitializeExecuteQueryConfig(
    zetasql::ExecuteQueryConfig& config) {
  ZETASQL_RETURN_IF_ERROR(SetDescriptorPoolFromFlags(config));
  ZETASQL_RETURN_IF_ERROR(SetToolModeFromFlags(config));
  ZETASQL_RETURN_IF_ERROR(AddTablesFromFlags(config));
  config.mutable_analyzer_options()
      .mutable_language()
      ->EnableMaximumLanguageFeaturesForDevelopment();
  config.mutable_catalog().AddZetaSQLFunctions(
        config.analyzer_options().language());
  return absl::OkStatus();
}

int main(int argc, char* argv[]) {
  const char kUsage[] =
      "Usage: execute_query [--table_spec=<table_spec>] <sql>\n";
  std::vector<char*> remaining_args = absl::ParseCommandLine(argc, argv);
  if (argc <= 1) {
    ZETASQL_LOG(QFATAL) << kUsage;
  }
  const std::string sql = absl::StrJoin(remaining_args.begin() + 1,
  remaining_args.end(), " ");
  zetasql::ExecuteQueryConfig config;
  absl::Status status = InitializeExecuteQueryConfig(config);
  if (status.ok()) {
    status = ExecuteQuery(sql, config);
  }
  if (status.ok()) {
    return 0;
  } else {
    std::cout << "ERROR: " << status << std::endl;
    return 1;
  }
}
