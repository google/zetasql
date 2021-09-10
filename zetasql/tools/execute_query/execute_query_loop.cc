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

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/tools/execute_query/execute_query.pb.h"
#include "zetasql/tools/execute_query/execute_query_prompt.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using execute_query::ParserErrorContext;

absl::Status ExecuteQueryLoopNoOpStatusHandler(absl::Status status) {
  return status;
}

absl::Status ExecuteQueryLoop(
    ExecuteQueryPrompt& prompt, ExecuteQueryConfig& config,
    ExecuteQueryWriter& writer,
    const ExecuteQueryLoopStatusHandler status_handler) {
  for (;;) {
    const absl::StatusOr<std::optional<std::string>> input = prompt.Read();

    if (!input.ok()) {
      ZETASQL_RETURN_IF_ERROR(status_handler(input.status()));
      continue;
    }

    if (!input->has_value()) {
      // Reached end of input
      return absl::OkStatus();
    }

    absl::Status status = ExecuteQuery(**input, config, writer);

    if (!status.ok()) {
      ParserErrorContext ctx;
      ctx.set_text(std::string{absl::StripAsciiWhitespace(**input)});
      internal::AttachPayload(&status, ctx);
    }

    ZETASQL_RETURN_IF_ERROR(status_handler(status));
  }
}

}  // namespace zetasql
