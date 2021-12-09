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

#include "zetasql/reference_impl/rewrite_flags.h"

#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/options.pb.h"
#include "absl/flags/flag.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

namespace zetasql {

namespace {
// Rewrites for constructs which the reference implementation is able to
// successfully execute using a native implementation, even without the rewrite.
// These rewrites are turned off when using the reference impl as a baseline
// for compliance tests, so that the result of executing the query through the
// rewriter can be compared to it.
constexpr ResolvedASTRewrite kReferenceImplOptionalRewrites[] = {
    REWRITE_FLATTEN,        REWRITE_PROTO_MAP_FNS,
    REWRITE_PIVOT,          REWRITE_ARRAY_FILTER_TRANSFORM,
    REWRITE_ARRAY_INCLUDES, REWRITE_UNPIVOT,
    REWRITE_LET_EXPR,
};

RewriteSet DefaultRewrites() {
  return RewriteSet(AnalyzerOptions().enabled_rewrites());
}

RewriteSet MinimalRewrites() {
  RewriteSet minimal_rewrites(DefaultRewrites());
  for (ResolvedASTRewrite rewrite : zetasql::kReferenceImplOptionalRewrites) {
    minimal_rewrites.erase(rewrite);
  }
  return minimal_rewrites;
}

RewriteSet AllRewrites() {
  RewriteSet all_rewrites;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor = descriptor->value(i);
    const ResolvedASTRewrite rewrite =
        static_cast<ResolvedASTRewrite>(value_descriptor->number());
    all_rewrites.insert(rewrite);
  }
  return all_rewrites;
}
}  // namespace

// Returns a textual flag value corresponding to a rewrite set.
std::string AbslUnparseFlag(RewriteSet set) {
  if (set.empty()) {
    return "none";
  } else if (set == DefaultRewrites()) {
    return "default";
  } else if (set == MinimalRewrites()) {
    return "minimal";
  } else if (set == AllRewrites()) {
    return "all";
  }
  return absl::StrJoin(set, ",",
                       [](std::string* out, ResolvedASTRewrite rewrite) {
                         absl::StrAppend(out, ResolvedASTRewrite_Name(rewrite));
                       });
}

// Parses a rewrite set from the command line flag value `text`.
// Returns true and sets `*flag` on success; returns false and sets `*error`
// on failure.
bool AbslParseFlag(absl::string_view text, RewriteSet* set,
                   std::string* error) {
  if (text == "default") {
    *set = DefaultRewrites();
    return true;
  }
  if (text == "none") {
    *set = {};
    return true;
  }
  if (text == "minimal") {
    *set = MinimalRewrites();
    return true;
  }
  if (text == "all") {
    *set = AllRewrites();
    return true;
  }

  // Anything not one of the predefined values above is parsed as a
  // comma-delimited list of rewrite names.
  RewriteSet explicit_rewrites;
  const google::protobuf::EnumDescriptor* descriptor =
      google::protobuf::GetEnumDescriptor<ResolvedASTRewrite>();
  std::vector<absl::string_view> rewrite_names = absl::StrSplit(text, ',');
  for (absl::string_view rewrite_name : rewrite_names) {
    const google::protobuf::EnumValueDescriptor* value_descriptor =
        descriptor->FindValueByName(std::string(rewrite_name));
    if (value_descriptor == nullptr) {
      *error = absl::StrCat("Invalid rewrite: ", rewrite_name);
      return false;
    }
    explicit_rewrites.insert(static_cast<ResolvedASTRewrite>(
        descriptor
            ->FindValueByName(
                std::string(absl::StripAsciiWhitespace(rewrite_name)))
            ->number()));
  }
  *set = explicit_rewrites;
  return true;
}
}  // namespace zetasql

ABSL_FLAG(
    zetasql::RewriteSet, rewrites, zetasql::MinimalRewrites(),
    "Which rewrites to enable. Possible values:\n"
    "  minimal: Enable only rewrites which are on by default and no native\n"
    "    implementation exists in the reference impl.\n"
    "  default: Enable only rewrites which are on by default\n"
    "  none: Disable all rewrites\n"
    "  all: Enable all rewrites\n"
    "  REWRITE_FOO,REWRITE_BAR,...: Enable only rewrites in the explcit list");
