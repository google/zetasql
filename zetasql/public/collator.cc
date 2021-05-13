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

#include "zetasql/public/collator.h"

#include <cstdint>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_split.h"
#include "unicode/coll.h"
#include "unicode/errorcode.h"
#include "unicode/utypes.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {
namespace {

static absl::Status MakeCollationError(absl::string_view collation_name,
                                       absl::string_view error = {}) {
  if (error.empty()) {
    return MakeEvalError() << "COLLATE has invalid collation name '"
                           << collation_name << "'";
  } else {
    return MakeEvalError() << "COLLATE has invalid collation name '"
                           << collation_name << "':" << error;
  }
}

// Returns true if <collation_name> is valid and we were able to extract the
// collation parts from it successfully. Otherwise false.
static absl::Status ExtractCollationParts(absl::string_view collation_name,
                                          std::string* language_tag,
                                          std::string* collation_attribute) {
  language_tag->clear();
  collation_attribute->clear();

  const std::vector<std::string> parts = absl::StrSplit(collation_name, ':');
  ZETASQL_RET_CHECK_GT(parts.size(), 0);
  if (parts[0].empty()) {
    return MakeCollationError(collation_name, "cannot contain empty parts");
  }

  if (parts.size() > 2) {
    // We only support case-sensitivity as a collation attribute now. So,
    // specifying multiple attributes is not allowed.
    return MakeCollationError(collation_name,
                              "only case sensitivity attribute is supported");
  }

  // Only ":ci" or ":cs" as a suffix are allowed now for modifying
  // case-sensitivity.
  if (parts.size() == 2 && parts[1] != "ci" && parts[1] != "cs") {
    return MakeCollationError(collation_name,
                              "case sensitivity must be 'ci' or 'cs'");
  }

  *language_tag = parts[0];
  if (parts.size() == 2) {
    *collation_attribute = parts[1];
  }
  return absl::OkStatus();
}

class ZetaSqlCollatorIcu : public ZetaSqlCollator {
 public:
  ZetaSqlCollatorIcu(std::unique_ptr<icu::Collator> icu_collator,
                       bool is_unicode, bool is_case_insensitive);
  ~ZetaSqlCollatorIcu() override {}

  int64_t CompareUtf8(const absl::string_view s1, const absl::string_view s2,
                      absl::Status* error) const override;

  bool IsBinaryComparison() const override {
    return icu_collator_ == nullptr && !is_case_insensitive_;
  }

 private:
  // icu::Collator used for locale-specific ordering. Not initialized for case
  // sensitive Unicode locale (i.e. is_unicode && !is_case_insensitive_).
  const std::unique_ptr<const icu::Collator> icu_collator_;

  // Set to true if instantiated with "unicode", i.e. default Unicode collation.
  const bool is_unicode_;

  // Collation attribute to specify whether the comparisons should be
  // case-insensitive.
  const bool is_case_insensitive_;
};

ZetaSqlCollatorIcu::ZetaSqlCollatorIcu(
    std::unique_ptr<icu::Collator> icu_collator, bool is_unicode,
    bool is_case_insensitive)
    : icu_collator_(std::move(icu_collator)),
      is_unicode_(is_unicode),
      is_case_insensitive_(is_case_insensitive) {}

int64_t ZetaSqlCollatorIcu::CompareUtf8(const absl::string_view s1,
                                          const absl::string_view s2,
                                          absl::Status* error) const {
  if (is_unicode_) {
    if (is_case_insensitive_) {
      ; // Just fall back to icu.
    } else {
      const int result = s1.compare(s2);
      return result < 0 ? -1 : (result > 0 ? 1 : 0);
    }
  }

  icu::ErrorCode icu_error;

  UCollationResult result = icu_collator_->compareUTF8(
      icu::StringPiece(s1.data(), static_cast<int32_t>(s1.size())),
      icu::StringPiece(s2.data(), static_cast<int32_t>(s2.size())), icu_error);
  if (icu_error.isFailure()) {
    *error = absl::Status(absl::StatusCode::kInvalidArgument,
                          "Strings cannot be compared with the collator");
    icu_error.reset();
  }
  // UCollationResult is a three valued enum UCOL_EQUAL, UCOL_LESS AND
  // UCOL_GREATER.
  static_assert(UCOL_LESS == -1, "compareUTF8 result conversion");
  static_assert(UCOL_EQUAL == 0, "compareUTF8 result conversion");
  static_assert(UCOL_GREATER == 1, "compareUTF8 result conversion");
  return result;
}

}  // namespace

zetasql_base::StatusOr<std::unique_ptr<const ZetaSqlCollator>> MakeSqlCollator(
    absl::string_view collation_name) {
  std::string language_tag;
  std::string collation_attribute;
  ZETASQL_RETURN_IF_ERROR(ExtractCollationParts(collation_name, &language_tag,
                                        &collation_attribute));

  const bool is_case_insensitive = (collation_attribute == "ci");
  const bool is_unicode = (language_tag == "unicode");

  std::unique_ptr<icu::Collator> icu_collator;
  ZETASQL_RET_CHECK(!language_tag.empty());

  if (is_unicode && !is_case_insensitive) {
    // No need to instantiate icu::Collator for case-sensitive Unicode
    // collation. In that case we can just compare strings as binary BLOBs.
    return absl::make_unique<const ZetaSqlCollatorIcu>(nullptr, is_unicode,
                                                         is_case_insensitive);
  }
  // icu::Collator library returns a nullptr if it is unable to create an
  // instance from the LanguageCode identified by <collation_name>.
  icu::Locale locale = icu::Locale::createCanonical(language_tag.c_str());
  if (locale.isBogus()) {
    return MakeCollationError(collation_name);
  }
  icu::ErrorCode icu_error;
  icu_collator.reset(icu::Collator::createInstance(locale, icu_error));
  if (icu_error.isFailure() || icu_collator == nullptr) {
    icu_error.reset();
    return MakeCollationError(collation_name, absl::StrCat(" is invalid - ",
        icu_error.errorName()));
  }

  if (is_case_insensitive) {
    // Setting the icu::Collator strength to SECONDARY will ignore case
    // level comparisons.
    icu_collator->setStrength(icu::Collator::SECONDARY);
  } else {
    // We do nothing here as comparisons are case-sensitive by default in
    // icu::Collator.
  }
  return absl::make_unique<const ZetaSqlCollatorIcu>(
      std::move(icu_collator), is_unicode, is_case_insensitive);
}

}  // namespace zetasql
