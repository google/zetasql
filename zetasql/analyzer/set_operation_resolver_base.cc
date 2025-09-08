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

#include "zetasql/analyzer/set_operation_resolver_base.h"

#include <functional>
#include <string>
#include <vector>

#include "zetasql/analyzer/input_argument_type_resolver_helper.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

SetOperationResolverBase::SetOperationResolverBase(
    const AnalyzerOptions& analyzer_options, Coercer& coercer,
    ColumnFactory& column_factory)
    : analyzer_options_(analyzer_options),
      coercer_(coercer),
      column_factory_(column_factory) {}

// static
absl::StatusOr<ResolvedSetOperationScan::SetOperationType>
SetOperationResolverBase::GetSetOperationType(
    const ASTSetOperationMetadata* metadata) {
  switch (metadata->op_type()->value()) {
    case ASTSetOperation::UNION:
      return metadata->all_or_distinct()->value() == ASTSetOperation::DISTINCT
                 ? ResolvedSetOperationScan::UNION_DISTINCT
                 : ResolvedSetOperationScan::UNION_ALL;

    case ASTSetOperation::EXCEPT:
      return metadata->all_or_distinct()->value() == ASTSetOperation::DISTINCT
                 ? ResolvedSetOperationScan::EXCEPT_DISTINCT
                 : ResolvedSetOperationScan::EXCEPT_ALL;

    case ASTSetOperation::INTERSECT:
      return metadata->all_or_distinct()->value() == ASTSetOperation::DISTINCT
                 ? ResolvedSetOperationScan::INTERSECT_DISTINCT
                 : ResolvedSetOperationScan::INTERSECT_ALL;

    case ASTSetOperation::NOT_SET:
      return MakeSqlErrorAtLocalNode(metadata) << "Invalid set operation type";
  }
}

// static
std::string SetOperationResolverBase::GetSQLForOperation(
    ResolvedSetOperationScan::SetOperationType op_type) {
  switch (op_type) {
    case ResolvedSetOperationScan::UNION_ALL:
      return "UNION ALL";
    case ResolvedSetOperationScan::UNION_DISTINCT:
      return "UNION DISTINCT";
    case ResolvedSetOperationScan::EXCEPT_ALL:
      return "EXCEPT ALL";
    case ResolvedSetOperationScan::EXCEPT_DISTINCT:
      return "EXCEPT DISTINCT";
    case ResolvedSetOperationScan::INTERSECT_ALL:
      return "INTERSECT ALL";
    case ResolvedSetOperationScan::INTERSECT_DISTINCT:
      return "INTERSECT DISTINCT";
    default:
      return "UNKNOWN";
  }
}

absl::StatusOr<InputArgumentType>
SetOperationResolverBase::GetColumnInputArgumentType(
    const ResolvedColumn& column, const ResolvedScan* resolved_scan) const {
  // If this column was computed, find the expr that computed it.
  const ResolvedExpr* expr = nullptr;
  if (resolved_scan->node_kind() == RESOLVED_PROJECT_SCAN) {
    expr = FindProjectExpr(resolved_scan->GetAs<ResolvedProjectScan>(), column);
  }
  if (expr != nullptr) {
    return GetInputArgumentTypeForExpr(
        expr, /*pick_default_type_for_untyped_expr=*/false, analyzer_options_);
  } else {
    return InputArgumentType(column.type());
  }
}

absl::StatusOr<std::vector<const Type*>>
SetOperationResolverBase::GetSuperTypesOfSetOperation(
    absl::Span<const std::vector<InputArgumentType>> column_type_lists,
    const ASTNode* error_location,
    ResolvedSetOperationScan::SetOperationType op_type,
    std::function<std::string(int)> column_identifier_in_error_string) const {
  std::vector<const Type*> supertypes;
  supertypes.reserve(column_type_lists.size());

  for (int i = 0; i < column_type_lists.size(); ++i) {
    InputArgumentTypeSet type_set;
    for (const InputArgumentType& type : column_type_lists[i]) {
      type_set.Insert(type);
    }
    const Type* supertype = nullptr;
    ZETASQL_RETURN_IF_ERROR(coercer_.GetCommonSuperType(type_set, &supertype));
    if (supertype == nullptr) {
      return MakeSqlErrorAt(error_location)
             << "Column " << column_identifier_in_error_string(i) << " in "
             << GetSQLForOperation(op_type) << " has incompatible types: "
             << InputArgumentType::ArgumentsToString(column_type_lists[i]);
    }

    std::string no_grouping_type;
    bool column_types_must_support_grouping =
        op_type != ResolvedSetOperationScan::UNION_ALL;
    if (column_types_must_support_grouping &&
        !supertype->SupportsGrouping(analyzer_options_.language(),
                                     &no_grouping_type)) {
      return MakeSqlErrorAt(error_location)
             << "Column " << column_identifier_in_error_string(i) << " in "
             << GetSQLForOperation(op_type)
             << " has type that does not support set operation comparisons: "
             << no_grouping_type;
    }
    supertypes.push_back(supertype);
  }
  return supertypes;
}

absl::StatusOr<ResolvedColumnList>
SetOperationResolverBase::BuildFinalColumnList(
    absl::Span<const IdString> final_column_names,
    const std::vector<const Type*>& final_column_types, IdString table_name,
    std::function<void(const ResolvedColumn&)> record_column_access) {
  ZETASQL_RET_CHECK_EQ(final_column_names.size(), final_column_types.size());

  ResolvedColumnList column_list;
  column_list.reserve(final_column_types.size());
  for (int i = 0; i < final_column_names.size(); ++i) {
    column_list.emplace_back(column_factory_.AllocateColumnId(), table_name,
                             final_column_names[i], final_column_types[i]);
    record_column_access(column_list.back());
  }
  return column_list;
}

}  // namespace zetasql
