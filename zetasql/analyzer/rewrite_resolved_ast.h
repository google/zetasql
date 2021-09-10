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

#ifndef ZETASQL_ANALYZER_REWRITE_RESOLVED_AST_H_
#define ZETASQL_ANALYZER_REWRITE_RESOLVED_AST_H_

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace zetasql {
// Similar to RewriteResolvedAst() overload in analyzer.h, except that it allows
// the caller to supply an explicit list of rewriters, rather than being
// constrained to a predefined set.
//
// TODO: Move this to analyzer.h once it's stable enough to no longer
// require BUILD visibility restrictions.
absl::Status RewriteResolvedAst(const AnalyzerOptions& analyzer_options,
                                absl::Span<const Rewriter* const> rewriters,
                                absl::string_view sql, Catalog* catalog,
                                TypeFactory* type_factory,
                                AnalyzerOutput& analyzer_output);
}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITE_RESOLVED_AST_H_
