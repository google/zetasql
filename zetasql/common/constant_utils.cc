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

#include "zetasql/common/constant_utils.h"

#include <algorithm>
#include <ostream>

#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// The constness level of an expression.
enum class ConstnessLevel {
  kForeverConst,
  kAnalysisConst,
  kPlanningConst,
  kStatementConst,
  kQueryConst,
  kNotConst,
};

// Prints the name of a ConstnessLevel enum value.
std::ostream& operator<<(std::ostream& os,
                         const ConstnessLevel& constness_level) {
  switch (constness_level) {
    case ConstnessLevel::kForeverConst:
      return os << "FOREVER_CONST";
    case ConstnessLevel::kAnalysisConst:
      return os << "ANALYSIS_CONST";
    case ConstnessLevel::kPlanningConst:
      return os << "PLANNING_CONST";
    case ConstnessLevel::kStatementConst:
      return os << "STATEMENT_CONST";
    case ConstnessLevel::kQueryConst:
      return os << "QUERY_CONST";
    case ConstnessLevel::kNotConst:
      return os << "NOT_CONST";
    default:
      return os << "INVALID";
  }
}

// Returns the constness level of `node` if it is an expression. Otherwise,
// returns the maximum constness level of all the expressions referenced in
// `node`.
//
// A functional constant is a recursive definition of constness, in which the
// constness level of an expression is the maximum constness level of all the
// expressions it contains.
//
// TODO: b/277365877 - Handle GROUPING_CONST and WINDOW_CONST separately, as
// they can only be understood by the resolver and ZetaSQL rewriter.
absl::StatusOr<ConstnessLevel> GetConstnessLevel(const ResolvedNode* node) {

  // Statements are not allowed to exist when deciding the constness level of
  // an expression.
  ZETASQL_RET_CHECK(!node->IsStatement());

  switch (node->node_kind()) {
    case RESOLVED_LITERAL: {
      // CAVEAT: ARRAY and STRUCT literal syntax might resolve to a literal (as
      // a result of constant folding) or a constructor (`$make_array` function
      // call or `ResolvedMakeStruct` node). A composite type constructor is an
      // ANALYSIS_CONST. We don't have a way to distinguish the original syntax
      // here.
      return ConstnessLevel::kForeverConst;
    }
    case RESOLVED_CONSTANT: {
      return ConstnessLevel::kAnalysisConst;
    }

    case RESOLVED_FLATTENED_ARG: {
      // TODO: b/277365877 - Implement PLANNING_CONST.
      return ConstnessLevel::kNotConst;
    }

    case RESOLVED_FUNCTION_CALL: {
      const ResolvedFunctionCall* function_call =
          node->GetAs<ResolvedFunctionCall>();
      if (function_call->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return ConstnessLevel::kNotConst;
      }

      if (function_call->function()->function_options().volatility ==
          FunctionEnums::STABLE) {
        // TODO: b/277365877 - Implement STATEMENT_CONST.
        return ConstnessLevel::kNotConst;
      }

      // A collapsed array literal is an ANALYSIS_CONST. It's a `$make_array`
      // function call with all arguments satisfying ANALYSIS_CONST constraint.
      // So an ARRAY constructor is at least an ANALYSIS_CONST.
      ConstnessLevel max_constness_level = ConstnessLevel::kAnalysisConst;
      if (function_call->function()->IsZetaSQLBuiltin() &&
          function_call->function()->Name() == "$make_array") {
        for (auto& arg : function_call->argument_list()) {
          ZETASQL_ASSIGN_OR_RETURN(ConstnessLevel arg_constness_level,
                           GetConstnessLevel(arg.get()));
          max_constness_level =
              std::max(max_constness_level, arg_constness_level);
        }
        return max_constness_level;
      }

      // TODO: b/277365877 - Implement PLANNING_CONST.
      return ConstnessLevel::kNotConst;
    }
    case RESOLVED_MAKE_STRUCT: {
      auto* make_struct = node->GetAs<ResolvedMakeStruct>();
      // A collapsed struct literal is an ANALYSIS_CONST. It's a
      // ResolvedMakeStruct with all fields being analysis const.
      ConstnessLevel max_constness_level = ConstnessLevel::kAnalysisConst;
      for (auto& field : make_struct->field_list()) {
        ZETASQL_ASSIGN_OR_RETURN(ConstnessLevel arg_constness_level,
                         GetConstnessLevel(field.get()));
        max_constness_level =
            std::max(max_constness_level, arg_constness_level);
      }
      return max_constness_level;
    }
    case RESOLVED_MAKE_PROTO: {
      // A collapsed proto literal is an ANALYSIS_CONST. It's a
      // ResolvedMakeProto with all fields being analysis const.
      ConstnessLevel max_constness_level = ConstnessLevel::kAnalysisConst;
      for (auto& field : node->GetAs<ResolvedMakeProto>()->field_list()) {
        ZETASQL_ASSIGN_OR_RETURN(ConstnessLevel arg_constness_level,
                         GetConstnessLevel(field->expr()));
        max_constness_level =
            std::max(max_constness_level, arg_constness_level);
      }
      return max_constness_level;
    }

    case RESOLVED_SUBQUERY_EXPR: {
      // TODO: b/277365877 - Implement this case.
      return ConstnessLevel::kNotConst;
    }
    case RESOLVED_WITH_EXPR: {
      // TODO: b/277365877 - Implement this case.
      return ConstnessLevel::kNotConst;
    }

    // TODO: b/277365877 - The following cases are not explicitly discussed in
    // the design doc.
    case RESOLVED_EXPRESSION_COLUMN:
    case RESOLVED_CATALOG_COLUMN_REF:
    case RESOLVED_DMLDEFAULT:
    case RESOLVED_GRAPH_GET_ELEMENT_PROPERTY:
    case RESOLVED_GRAPH_MAKE_ELEMENT: {
      return ConstnessLevel::kNotConst;
    }

    case RESOLVED_PARAMETER:
    case RESOLVED_SYSTEM_VARIABLE: {
      // TODO: b/277365877 - Implement STATEMENT_CONST.
      return ConstnessLevel::kNotConst;
    }
    case RESOLVED_ARGUMENT_REF: {
      // TODO: b/277365877 - Implement QUERY_CONST.
      return ConstnessLevel::kNotConst;
    }
    case RESOLVED_COLUMN_REF: {
      // TODO: b/277365877 - Support correlated column reference.
      // The constness level of a column reference is determined by the source
      // table it comes from. It's impossible to tell from the ResolvedExpr
      // without additional context. We have to return NOT_CONST here.
      // However, since we don't allow table scans (ResolvedTableScan,
      // GraphNodeScan and GraphEdgeScan) that require catalog lookup to be
      // constant, it's safe to assume that non-correlated column references can
      // not be constant.
      return ConstnessLevel::kNotConst;
    }

    // Table scans that require catalog lookup are not constant.
    case RESOLVED_TABLE_SCAN:
    case RESOLVED_GRAPH_NODE_SCAN:
    case RESOLVED_GRAPH_EDGE_SCAN: {
      return ConstnessLevel::kNotConst;
    }

    // TODO: b/277365877 - The following cases are not explicitly discussed in
    // the design doc.
    // Aggregate and analytic scans are not constant because they might be
    // non-deterministic.
    case RESOLVED_AGGREGATE_FUNCTION_CALL:
    case RESOLVED_ANALYTIC_FUNCTION_CALL:
    case RESOLVED_ARRAY_AGGREGATE:
    case RESOLVED_AGGREGATE_SCAN:
    case RESOLVED_ANALYTIC_SCAN: {
      return ConstnessLevel::kNotConst;
    }

    // TODO: b/277365877 - Relax this.
    // For all the other case, defaults to being non-constant.
    default: {
      return ConstnessLevel::kNotConst;
    }
  }
}

bool IsAnalysisConstant(const ResolvedNode* node) {
  absl::StatusOr<ConstnessLevel> constness_level = GetConstnessLevel(node);
  if (!constness_level.ok()) {
    return false;
  }
  return constness_level.value() <= ConstnessLevel::kAnalysisConst;
}

}  // namespace zetasql
