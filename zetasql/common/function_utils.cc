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

#include "zetasql/common/function_utils.h"

#include <string>

#include "zetasql/public/function.h"
#include "absl/strings/match.h"

namespace zetasql {

bool FunctionIsOperator(const Function& function) {
  return function.IsZetaSQLBuiltin() &&
         absl::StartsWith(function.Name(), "$") &&
         function.Name() != "$count_star" &&
         !absl::StartsWith(function.Name(), "$extract");
}

}  // namespace zetasql
