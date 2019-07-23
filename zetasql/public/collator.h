//
// Copyright 2019 ZetaSQL Authors
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

#include <memory>
#include <string>

#include <cstdint>
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"
#include "unicode/coll.h"

namespace zetasql {

// This class is specific to the COLLATE clause in zetasql.
// For more information on collate semantics in zetasql, see
// (broken link).
//
// Usage:
// ZetaSqlCollator* collator =
//   ZetaSqlCollator::CreateFromCollationName("en_US:ci");
// zetasql_base::Status error;
// int64_t compare_result = collator->CompareUtf8(s1, s2, &error);
class ZetaSqlCollator {
 public:
  ZetaSqlCollator() = delete;
  ZetaSqlCollator(const ZetaSqlCollator&) = delete;
  ZetaSqlCollator& operator=(const ZetaSqlCollator&) = delete;
  ~ZetaSqlCollator();

  // Returns a instance of ZetaSqlCollator corresponding to the given
  // <collation_name>. Returns nullptr if <collation_name> is not valid.
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
  // The returned ZetaSqlCollator* is owned by the caller.
  static ZetaSqlCollator* CreateFromCollationName(
      const std::string& collation_name);

  // A three valued std::string compare method based on the collate specific rules.
  //
  // Returns -1 if s1 is less than s2.
  // Returns 1 if s1 is greater than s2.
  // Returns 0 is s1 is equal to s2.
  //
  // If an error occurs, <*error> will be updated.
  // Errors will never occur if <s1> and <s2> are valid UTF-8.
  int64_t CompareUtf8(const absl::string_view s1, const absl::string_view s2,
                    zetasql_base::Status* error) const;

  // Returns true if this collator uses simple binary comparisons.
  // If true, engines can get equivalent behavior using binary comparison on
  // strings rather than using CompareUtf8, which may allow for more efficient
  // implementation.
  bool IsBinaryComparison() const {
    return icu_collator_ == nullptr && !is_case_insensitive_;
  }

 private:
  ZetaSqlCollator(std::unique_ptr<icu::Collator> icu_collator,
                    bool is_unicode, bool is_case_insensitive);

  // icu::Collator used for locale-specific ordering. Not initialized for case
  // sensitive Unicode locale (i.e. is_unicode && !is_case_insensitive_).
  const std::unique_ptr<const icu::Collator> icu_collator_;

  // Set to true if instantiated with "unicode", i.e. default Unicode collation.
  const bool is_unicode_;

  // Collation attribute to specify whether the comparisons should be
  // case-insensitive.
  const bool is_case_insensitive_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_COLLATOR_H_
