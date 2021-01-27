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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_

#include <string>

#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace zetasql {

class ExecuteQueryPrompt {
 public:
  virtual ~ExecuteQueryPrompt() = default;

  // Read next input line. Return empty optional when input is finished (e.g. at
  // EOF).
  virtual zetasql_base::StatusOr<absl::optional<std::string>> Read() = 0;
};

class ExecuteQuerySingleInput : public ExecuteQueryPrompt {
 public:
  explicit ExecuteQuerySingleInput(absl::string_view query) : query_{query} {}
  ExecuteQuerySingleInput(const ExecuteQuerySingleInput&) = delete;
  ExecuteQuerySingleInput& operator=(const ExecuteQuerySingleInput&) = delete;

  zetasql_base::StatusOr<absl::optional<std::string>> Read() override;

 private:
  const std::string query_;
  bool done_ = false;
};

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_
