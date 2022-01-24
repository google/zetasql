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

#ifndef ZETASQL_ANALYZER_REWRITERS_REWRITER_RELEVANCE_CHECKER_H_
#define ZETASQL_ANALYZER_REWRITERS_REWRITER_RELEVANCE_CHECKER_H_

#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Visit the ResolvedAST tree rooted at 'node' and return the set of rewriters
// that can be applied somewhere in the tree.
absl::StatusOr<absl::btree_set<ResolvedASTRewrite>> FindRelevantRewriters(
    const ResolvedNode* node);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_REWRITER_RELEVANCE_CHECKER_H_
