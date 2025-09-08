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

#include "zetasql/analyzer/constant_resolver_helper.h"

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/sql_constant.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::StatusOr<Value> GetResolvedConstantValue(
    const ResolvedConstant& node, const AnalyzerOptions& analyzer_options) {
  if (node.constant()->HasValue()) {
    return node.constant()->GetValue();
  }
  ZETASQL_RET_CHECK_NE(analyzer_options.constant_evaluator(), nullptr)
      << "Constant evaluator is not set in AnalyzerOptions";

  if (node.constant()->Is<SQLConstant>()) {
    ZETASQL_ASSIGN_OR_RETURN(
        Value constant_value,
        analyzer_options.constant_evaluator()->EvaluateAnalysisConstant(node));
    ZETASQL_RET_CHECK(constant_value.is_valid())
        << "Constant evaluator returns invalid value";
    return constant_value;
  }
  return absl::UnimplementedError(
      "Unimplemented constant type. Consider using a literal or SQL constant "
      "instead");
}

}  // namespace zetasql
