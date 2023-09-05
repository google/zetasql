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

#include <string>
#include <vector>

#include "zetasql/common/options_utils.h"
#include "zetasql/public/options.pb.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace zetasql {

static RewriteSet DefaultRewrites() {
  return RewriteSet(AnalyzerOptions().enabled_rewrites());
}

// Rewrites for constructs which the reference implementation is not able to
// successfully execute using a direct implementation, and thus requires the
// rewriter. These rewrites are enabled for the reference even when using the
// reference impl as a baseline for compliance tests.
static const RewriteSet& MinimalRewrites() {
  static const auto* minimal_rewrites = new RewriteSet({
      // Probably don't add to this list without a very good reason to do so.
      // Features that are rewrite *only* without direct implementation support
      // in the reference implementation are not as well tested as features with
      // both implementations.
      // clang-format off
      // (broken link) start
      REWRITE_INLINE_SQL_TVFS,
      REWRITE_INLINE_SQL_UDAS,
      REWRITE_INLINE_SQL_VIEWS,
      // (broken link) end
      // clang-format on
  });
  return *minimal_rewrites;
}

// Returns a textual flag value corresponding to a rewrite set.
std::string AbslUnparseFlag(RewriteSet set) {
  if (set.empty()) {
    return "none";
  } else if (set == DefaultRewrites()) {
    return "default";
  } else if (set == MinimalRewrites()) {
    return "minimal";
  } else if (set == internal::GetAllRewrites()) {
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
    *set = RewriteSet(internal::GetAllRewrites());
    return true;
  }
  auto statusor_parsed_rewrites = internal::ParseEnabledAstRewrites(text);
  if (statusor_parsed_rewrites.ok()) {
    *set = RewriteSet(statusor_parsed_rewrites->options);
    return true;
  }
  *error = statusor_parsed_rewrites.status().message();
  return false;
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
    "  REWRITE_FOO,REWRITE_BAR,...: Enable only rewrites in the explicit list");
