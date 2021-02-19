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

#include "zetasql/tools/execute_query/execute_query_loop.h"

#include <string>
#include <utility>

#include "zetasql/tools/execute_query/execute_query_prompt.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status ExecuteQueryLoopNoOpStatusHandler(absl::Status status,
                                               absl::string_view sql) {
  return status;
}

absl::Status ExecuteQueryLoop(
    ExecuteQueryPrompt& prompt, ExecuteQueryConfig& config,
    ExecuteQueryWriter& writer,
    const ExecuteQueryLoopStatusHandler status_handler) {
  for (;;) {
    ZETASQL_ASSIGN_OR_RETURN(const std::optional<std::string> sql, prompt.Read());

    if (!sql.has_value()) {
      // Reached end of input
      return absl::OkStatus();
    }

    ZETASQL_RETURN_IF_ERROR(status_handler(ExecuteQuery(*sql, config, writer), *sql));
  }
}

}  // namespace zetasql
