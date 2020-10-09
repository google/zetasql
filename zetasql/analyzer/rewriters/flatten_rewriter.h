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

#ifndef ZETASQL_ANALYZER_REWRITERS_FLATTEN_REWRITER_H_
#define ZETASQL_ANALYZER_REWRITERS_FLATTEN_REWRITER_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/rewrite_utils.h"

namespace zetasql {

// Rewrites `resolved_statement` to a new ResolvedStatement where
// ResolvedFlatten nodes have been replaced by equivalent scan nodes.
zetasql_base::StatusOr<std::unique_ptr<const ResolvedStatement>> RewriteResolvedFlatten(
    const ResolvedStatement& resolved_statement, ColumnFactory& column_factory);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_FLATTEN_REWRITER_H_
