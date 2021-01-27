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

#include "zetasql/tools/execute_query/execute_query_prompt.h"

#include <string>

#include "zetasql/base/statusor.h"
#include "absl/types/optional.h"

namespace zetasql {

zetasql_base::StatusOr<absl::optional<std::string>> ExecuteQuerySingleInput::Read() {
  if (!done_) {
    done_ = true;
    return query_;
  }

  return absl::nullopt;
}

}  // namespace zetasql
