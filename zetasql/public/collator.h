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

#ifndef ZETASQL_PUBLIC_COLLATOR_H_
#define ZETASQL_PUBLIC_COLLATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include <cstdint>
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {

// This class is specific to the COLLATE clause in zetasql.
// For more information on collate semantics in zetasql, see
//
// https://github.com/google/zetasql/blob/master/docs/query-syntax.md#collate
//
// Usage:
// zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>> collator =
//     zetasql::MakeSqlCollator("en_US:ci");
// ZETASQL_RETURN_IF_ERROR(collator);
// absl::Status error;
// int64_t compare_result = (*collator)->CompareUtf8(s1, s2, &error);
class ZetaSqlCollator {
 public:
  ZetaSqlCollator(const ZetaSqlCollator&) = delete;
  ZetaSqlCollator& operator=(const ZetaSqlCollator&) = delete;
  virtual ~ZetaSqlCollator() = 0;

  // A three valued string compare method based on the collate specific rules.
  //
  // Returns -1 if s1 is less than s2.
  // Returns 1 if s1 is greater than s2.
  // Returns 0 is s1 is equal to s2.
  //
  // If an error occurs, <*error> will be updated.
  // Errors will never occur if <s1> and <s2> are valid UTF-8.
  virtual int64_t CompareUtf8(const absl::string_view s1,
                              const absl::string_view s2,
                              absl::Status* error) const = 0;

  // Returns true if this collator uses simple binary comparisons.
  // If true, engines can get equivalent behavior using binary comparison on
  // strings rather than using CompareUtf8, which may allow for more efficient
  // implementation.
  virtual bool IsBinaryComparison() const = 0;

  friend zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
  MakeSqlCollator(absl::string_view collation_name);

  friend zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>>
  MakeSqlCollatorLite(absl::string_view collation_name);

 protected:
  ZetaSqlCollator() = default;
};

// Returns a instance of ZetaSqlCollator corresponding to the given
// <collation_name>. Returns error if <collation_name> is not valid.
//
// Your build target must depend on :collator to use this function;
// :collator_lite is insufficient.
//
// A <collation_name> is composed as "<language_tag> ( ':' <attribute> )*".
// - <language_tag> is considered valid only if it is "unicode" (which means
//   use default unicode collation) or
//   LanguagCodeConverter::GetStatusFromOther(<language_tag>) returns status
//   i18n_identifiers::CANONICAL.
// - <attribute> is an option to customize the collation order.
//   The only supported attributes are:
//     ":ci" Use case-insensitive comparison
//     ":cs" Use case-sensitive comparison
//
zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>> MakeSqlCollator(
    absl::string_view collation_name);

// This lightweight version of MakeFromCollationName() supports only the
// "unicode:cs" collation, unless the full collator implementation has been
// linked in and statically registered, in which case it behaves the same way
// as CreateFromCollationName() above.
//
// ZetaSQL end users should not have any reason to call this function over
// CreateFromCollationName(); it is mostly an implementation detail of the
// :evaluator_lite target. It switches between the "lite" implementation and
// the full ICU-based implementation based on whether the latter has been
// registered with zetasql::internal::RegisterIcuCollatorImpl().
//
zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>> MakeSqlCollatorLite(
    absl::string_view collation_name);

namespace internal {
// Globally registers the collator implementation to be used by
// ZetaSqlCollator::CreateFromCollationNameLite().
// For internal ZetaSQL use only.
void RegisterIcuCollatorImpl(
    std::function<zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>>(
        absl::string_view)>
        create_fn);

// Resets the globally registered collator implementation to the lightweight
// default. For testing only.
void RegisterDefaultCollatorImpl();
}  // namespace internal

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_COLLATOR_H_
