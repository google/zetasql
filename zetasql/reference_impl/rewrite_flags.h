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

#ifndef ZETASQL_REFERENCE_IMPL_REWRITE_FLAGS_H_
#define ZETASQL_REFERENCE_IMPL_REWRITE_FLAGS_H_

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/options.pb.h"
#include "absl/container/btree_set.h"
#include "absl/flags/declare.h"

namespace zetasql {

// Wrapper around a set of ResolvedASTRewrite to represent the value
// of the 'rewrites' flag. It is necessary to allow AbslParseFlag() and
// AbslUnparseFlag() to go in the zetasql namespace instead of the absl
// namespace.
class RewriteSet : public absl::btree_set<ResolvedASTRewrite> {
 public:
  RewriteSet() {}
  explicit RewriteSet(const absl::btree_set<ResolvedASTRewrite>& set)
      : absl::btree_set<ResolvedASTRewrite>(set) {}
};
std::string AbslUnparseFlag(RewriteSet set);
bool AbslParseFlag(absl::string_view text, RewriteSet* set, std::string* error);

}  // namespace zetasql

// Command-line flag to indicate which rewriters should be enabled.
// Possible values:
//   none: Disable all rewrites
//   default: Use default rewrite settings
//   minimal: Enable only rewrites which are on by default and are necessary for
//     execution in the reference implementation.
//   all: Enable all rewrites
//   REWRITE_FOO,REWRITE_BAR,...: Comma-delimited list of specific rewrites to
//     enable.
ABSL_DECLARE_FLAG(zetasql::RewriteSet, rewrites);

#endif  // ZETASQL_REFERENCE_IMPL_REWRITE_FLAGS_H_
