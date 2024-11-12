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

#ifndef ZETASQL_ANALYZER_REWRITERS_PRIVACY_PRIVACY_UTILITY_H_
#define ZETASQL_ANALYZER_REWRITERS_PRIVACY_PRIVACY_UTILITY_H_

#include <memory>
#include <vector>

#include "zetasql/analyzer/named_argument_info.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
// Consistently format a SQL error message that include the parse location in
// the node.
zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ResolvedNode& node);

// Use the resolver to create a new function call using resolved arguments. The
// calling code must ensure that the arguments can always be coerced and
// resolved to a valid function. Any returned status is an internal error.
// TODO: jmorcos - Conditional functions should respect the side effects scope
//                nesting depth.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> ResolveFunctionCall(
    absl::string_view function_name,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<NamedArgumentInfo> named_arguments, Resolver* resolver);
}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_PRIVACY_PRIVACY_UTILITY_H_
