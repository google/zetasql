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

#include "zetasql/common/aggregate_null_handling.h"

#include "absl/container/flat_hash_set.h"
#include "zetasql/base/case.h"
#include "zetasql/base/map_util.h"

namespace zetasql {
using StringViewCaseHash = ::zetasql_base::StringViewCaseHash;
using StringViewCaseEqual = ::zetasql_base::StringViewCaseEqual;
bool IgnoresNullArguments(
    const ResolvedNonScalarFunctionCallBase* aggregate_function) {
  static const absl::flat_hash_set<absl::string_view, StringViewCaseHash,
                                   StringViewCaseEqual>* const
      kFunctionsNotIgnoreNullSet =
          new absl::flat_hash_set<absl::string_view, StringViewCaseHash,
                                  StringViewCaseEqual>(
              {"array_agg", "any_value", "approx_top_count", "approx_top_sum",
               "st_nearest_neighbors"});

  switch (aggregate_function->null_handling_modifier()) {
    case ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING:
      return !aggregate_function->function()->IsZetaSQLBuiltin() ||
             !zetasql_base::ContainsKey(*kFunctionsNotIgnoreNullSet,
                               aggregate_function->function()->Name());
    case ResolvedNonScalarFunctionCallBase::RESPECT_NULLS:
      return false;
    case ResolvedNonScalarFunctionCallBase::IGNORE_NULLS:
      return true;
  }
}
}  // namespace zetasql
