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

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/tools/execute_query/execute_query_loop.h"
#include "zetasql/tools/execute_query/execute_query_prompt.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "zetasql/tools/execute_query/execute_query_web.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/functional/bind_front.h"
#include "absl/log/initialize.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "zetasql/base/status_macros.h"

namespace {
constexpr absl::string_view kHistoryFileName{
    ".zetasql_execute_query_history"};
}

ABSL_FLAG(
    bool, interactive, false,
    absl::StrFormat("Use interactive shell for entering multiple queries with "
                    "the query history stored in ~/%s.",
                    kHistoryFileName));

ABSL_FLAG(bool, web, false, "Run a local webserver to execute queries.");
ABSL_FLAG(int32_t, port, 8080, "Port to run the local webserver on.");

namespace zetasql {
namespace {

absl::Status RunTool(const std::vector<std::string>& args) {
  ExecuteQueryConfig config;

  ZETASQL_RETURN_IF_ERROR(InitializeExecuteQueryConfig(config));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ExecuteQueryWriter> writer,
                   MakeWriterFromFlags(config, std::cout));

  if (absl::GetFlag(FLAGS_interactive)) {
    ABSL_LOG(QFATAL) << "Interactive mode is not implemented in this version";
  }

  if (absl::GetFlag(FLAGS_web)) {
    return RunExecuteQueryWebServer(absl::GetFlag(FLAGS_port));
  }

  const std::string sql = absl::StrJoin(args, " ");

  ExecuteQuerySingleInput prompt(sql, config);

  return ExecuteQueryLoop(prompt, config, *writer,
                          &ExecuteQueryLoopPrintErrorHandler);
}
}  // namespace
}  // namespace zetasql

int main(int argc, char* argv[]) {
  const char kUsage[] =
      "Usage: execute_query [--table_spec=<table_spec>] "
      "{ --interactive | <sql> } { --web --port=<port> }\n";

  std::vector<std::string> args;

  {
    std::vector<char*> remaining_args = absl::ParseCommandLine(argc, argv);
    args.assign(remaining_args.cbegin() + 1, remaining_args.cend());
  }
  absl::InitializeLog();

  bool args_needed = !absl::GetFlag(FLAGS_interactive);
  args_needed = args_needed && !absl::GetFlag(FLAGS_web);

  if (args_needed && args.empty()) {
    ABSL_LOG(QFATAL) << "\n" << kUsage << "Pass --help for a full list of flags.\n";
  }

  if (const absl::Status status = zetasql::RunTool(args); status.ok()) {
    return 0;
  } else {
    std::cerr << status.message() << '\n';
    return 1;
  }
}
