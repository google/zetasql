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

#ifndef ZETASQL_COMMON_OPTIONS_UTILS_H_
#define ZETASQL_COMMON_OPTIONS_UTILS_H_

#include <algorithm>
#include <cstdint>
#include <string>

#include "google/protobuf/descriptor.h"
// We include the proto to guarantee GetEnumDescriptor exists for _some_ type
// otherwise some compilers (gcc) will think the templated code is invalid
// prematurely.
#include "google/protobuf/descriptor.pb.h"  
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql::internal {

//
// ParseEnumOptionsSet()
//
// This allows parsing strings of the form:
// <base>[,[+-]<enum_value>...]
// to construct a set of effectively enabled proto enums from a base set with
// items added or removed.
//
// Example:
//   auto default_values = {OPT_A, OPT_B};
//   auto all_values = {OPT_A, OPT_B, OPT_C, OPT_D}
//   ZETASQL_ASSIGN_OR_RETURN(auto entry = ParseEnumOptionsSet<OptEnum>(
//        {{"NONE", {}}, {"ALL", all_values}, {"DEFAULT", default_values},
//        /*strip_prefix=*/"OPT_",
//        /*error_context_name=*/"Opts",
//        /*options_str*/="NONE,+A,+D"));
//
// Would return the following: {OPT_A, OPT_D}
//
//   "ALL,-A,-D" -> {OPT_B, OPT_C}
//   "DEFAULT" -> {OPT_A, OPT_B}
//   "DEFAULT,-A,+C" -> {OPT_B, OPT_C}
//
// Explicit duplicate entries are an error
//   "DEFAULT,+A,+A" -> ERROR
//   "DEFAULT,-A,-A" -> ERROR
//   "DEFAULT,+A,-A" -> ERROR
//
// However, a redundant add/remove from a base (other than ALL or NONE) is okay:
//   "DEFAULT,+A" -> {OPT_A, OPT_B}
//
// A removal from a base called 'NONE' is an error:
//   "NONE,-A" -> ERROR
//
// An addition to a base called 'ALL' is an error:
//   "ALL,+A" -> ERROR
//
// ALL and NONE are otherwise not special, if they are not in the base_factory.
//
// Matching is case insensitive, and whitespace between tokens is ignored,
// However the +/- modifier must be a direct prefix to the enum value.
//
// Empty String: Error, a <base> must be part of the string
//   "" -> ERROR
//
//
// Args:
//   base_factory: a mapping from a given base to the initial set of
//     values associated with that base.  'NONE' and 'ALL', if mapped, have
//     special error handling, see above.
//   strip_prefix: This is stripped from the front of enum name before matching
//      with options_str.
//   error_context_name: usually the conceptual 'name' of the enum, used in
//        error messages.
//   options_str: the user strings.
//
template <typename EnumT>
struct EnumOptionsEntry {
  // Canonicalized description of the input string.
  //   This has all whitespace removed, forced to upper case
  std::string description;
  absl::btree_set<EnumT> options;
};

template <typename EnumT>
absl::StatusOr<EnumOptionsEntry<EnumT>> ParseEnumOptionsSet(
    const absl::flat_hash_map<absl::string_view, absl::btree_set<EnumT>>&
        base_factory,
    absl::string_view strip_prefix,
    absl::string_view error_context_name,  // e.g. 'Rewrite'
    absl::string_view options_str) {
  const google::protobuf::EnumDescriptor* enum_descriptor =
      google::protobuf::GetEnumDescriptor<EnumT>();

  std::string upper_options = absl::AsciiStrToUpper(options_str);
  absl::string_view stripped_options =
      absl::StripAsciiWhitespace(upper_options);
  std::vector<absl::string_view> tokens =
      absl::StrSplit(stripped_options, ',', absl::SkipWhitespace());

  // Cheat on error handling by forcing zero tokens to an empty string
  // which we don't expect to matching anything in the base_factory.
  absl::string_view base_token =
      tokens.empty() ? absl::string_view{} : tokens[0];
  absl::btree_set<EnumT> output_options;

  if (auto it = base_factory.find(base_token); it == base_factory.end()) {
    std::vector<absl::string_view> bases;
    for (const auto& [base, _] : base_factory) {
      bases.push_back(base);
    }
    std::sort(bases.begin(), bases.end());

    ZETASQL_RET_CHECK_FAIL() << error_context_name
                     << " list should always start with one of "
                     << absl::StrJoin(bases, ", ");
  } else {
    output_options = it->second;
  }

  // Handle special tokens. These help to 'lint' the input, preventing foolish
  // stuff like 'NONE,-FEATURE_X' which probably indicates an error.
  bool is_all_mode = (base_token == "ALL");
  bool is_none_mode = (base_token == "NONE");

  absl::btree_set<std::string> canonicalized_edits;
  absl::flat_hash_set<EnumT> seen_overrides;

  for (absl::string_view entry : absl::MakeSpan(tokens).subspan(1)) {
    entry = absl::StripAsciiWhitespace(entry);
    bool enable = true;
    if (absl::ConsumePrefix(&entry, "-")) {
      enable = false;
    } else if (!absl::ConsumePrefix(&entry, "+")) {
      ZETASQL_RET_CHECK_FAIL() << error_context_name
                       << " entries should be prefixed with '+' or '-'";
    }
    ZETASQL_RET_CHECK(!absl::ConsumePrefix(&entry, strip_prefix))
        << "For consistency, do not include the " << strip_prefix << " prefix.";

    const google::protobuf::EnumValueDescriptor* enum_value_descriptor =
        enum_descriptor->FindValueByName(absl::StrCat(strip_prefix, entry));
    ZETASQL_RET_CHECK(enum_value_descriptor != nullptr) << entry;
    EnumT enum_value = static_cast<EnumT>(enum_value_descriptor->number());
    ZETASQL_RET_CHECK(seen_overrides.insert(enum_value).second)
        << "Duplicate entry for " << error_context_name << ": "
        << enum_value_descriptor->name();
    if (enable) {
      ZETASQL_RET_CHECK(!is_all_mode) << "Attempting to add " << error_context_name
                              << ", but already started from ALL.";
      output_options.insert(enum_value);
      canonicalized_edits.insert(absl::StrCat("+", entry));
    } else {
      ZETASQL_RET_CHECK(!is_none_mode) << "Attempting to remove " << error_context_name
                               << ", but already started from NONE.";
      output_options.erase(enum_value);
      canonicalized_edits.insert(absl::StrCat("-", entry));
    }
  }
  std::vector<std::string> description_parts;
  description_parts.emplace_back(base_token);
  if (!canonicalized_edits.empty()) {
    description_parts.push_back(absl::StrJoin(canonicalized_edits, ","));
  }
  std::string description = absl::StrJoin(description_parts, ",");
  return EnumOptionsEntry<EnumT>{description, output_options};
}

}  // namespace zetasql::internal

#endif  // ZETASQL_COMMON_OPTIONS_UTILS_H_
