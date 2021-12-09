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

#ifndef ZETASQL_REFERENCE_IMPL_COMMON_H_
#define ZETASQL_REFERENCE_IMPL_COMMON_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/collator.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_collation.h"
#include "absl/status/status.h"
#include "zetasql/base/status.h"

namespace zetasql {

// Returns OK if 'type' supports equality comparison, error status otherwise.
absl::Status ValidateTypeSupportsEqualityComparison(const Type* type);

// Returns OK if 'type' supports less/greater comparison, error status
// otherwise.
absl::Status ValidateTypeSupportsOrderComparison(const Type* type);

// Releases the ownership of 'ptrs' in the input vector and returns a regular
// vector of pointers. This method takes a non-const reference by design as it
// is intended to be used like std::move(), clearing the input vector.
template <typename T>
static std::vector<T*> ReleaseAll(
    std::vector<std::unique_ptr<T>>& ptrs) {  // NOLINT(runtime/references)
  std::vector<T*> result;
  result.reserve(ptrs.size());
  for (auto& p : ptrs) {
    result.push_back(p.release());
  }
  ptrs.clear();
  return result;
}

// Returns a collation name from input <resolved_collation>.
absl::StatusOr<std::string>
GetCollationNameFromResolvedCollation(
    const ResolvedCollation& resolved_collation);

// Returns a ZetaSqlCollator from input <resolved_collation>.
absl::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
GetCollatorFromResolvedCollation(const ResolvedCollation& resolved_collation);

// Returns a collator from a value representing a ResolvedCollation object.
// An error will be returned if the input <collation_value> cannot be converted
// to a ResolvedCollation object.
absl::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
GetCollatorFromResolvedCollationValue(const Value& collation_value);

// TODO: Remove other local alias for
// std::vector<std::unique_ptr<const ZetaSqlCollator>> in
// tuple_comparators.h/.cc files.
using CollatorList = std::vector<std::unique_ptr<const ZetaSqlCollator>>;

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_COMMON_H_
