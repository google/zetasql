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

#include "zetasql/scripting/script_executor.h"

#include "zetasql/parser/parser.h"
#include "zetasql/scripting/error_helpers.h"
#include "zetasql/scripting/script_executor_impl.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/status.h"

namespace zetasql {

zetasql_base::StatusOr<std::unique_ptr<ScriptExecutor>> ScriptExecutor::Create(
    absl::string_view script, const ScriptExecutorOptions& options,
    StatementEvaluator* statement_evaluator) {
  return ScriptExecutorImpl::Create(script, /*ast_script=*/nullptr, options,
                                    statement_evaluator);
}

zetasql_base::StatusOr<std::unique_ptr<ScriptExecutor>> ScriptExecutor::CreateFromAST(
    absl::string_view script, const ASTScript* ast_script,
    const ScriptExecutorOptions& options,
    StatementEvaluator* statement_evaluator) {
  return ScriptExecutorImpl::Create(script, ast_script, options,
                                    statement_evaluator);
}

absl::Status StatementEvaluator::AssignSystemVariable(
    ScriptExecutor* executor, const ASTSystemVariableAssignment* ast_assignment,
    const Value& value) {
  ZETASQL_ASSIGN_OR_RETURN(bool handled, executor->DefaultAssignSystemVariable(
                                     ast_assignment, value));
  if (handled) {
    return absl::OkStatus();
  } else {
    // By default, assume all engine-owned system variables are read-only.
    return AssignmentToReadOnlySystemVariable(ast_assignment);
  }
}

}  // namespace zetasql
