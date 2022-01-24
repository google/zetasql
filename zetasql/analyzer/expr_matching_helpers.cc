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

#include "zetasql/analyzer/expr_matching_helpers.h"

#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/hash/hash.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::StatusOr<bool> IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                                const ResolvedExpr* expr2) {
  if (expr1->node_kind() != expr2->node_kind() ||
      !expr1->type()->Equals(expr2->type())) {
    return false;
  }

  // Need to make sure that all non-default fields of expressions were accessed
  // to make sure that if new fields are added, this function checks them or
  // expressions are not considered the same.
  expr1->ClearFieldsAccessed();
  expr2->ClearFieldsAccessed();

  switch (expr1->node_kind()) {
    case RESOLVED_LITERAL: {
      const ResolvedLiteral* lit1 = expr1->GetAs<ResolvedLiteral>();
      const ResolvedLiteral* lit2 = expr2->GetAs<ResolvedLiteral>();
      // Value::Equals does the right thing for GROUP BY - NULLs, NaNs, infs
      // etc are considered equal.
      if (!lit1->value().Equals(lit2->value())) {
        return false;
      }
      break;
    }
    case RESOLVED_PARAMETER: {
      const ResolvedParameter* param1 = expr1->GetAs<ResolvedParameter>();
      const ResolvedParameter* param2 = expr2->GetAs<ResolvedParameter>();
      // Parameter names are normalized, so case sensitive comparison is ok.
      if (param1->name() != param2->name() ||
          param1->position() != param2->position()) {
        return false;
      }
      break;
    }
    case RESOLVED_EXPRESSION_COLUMN: {
      const ResolvedExpressionColumn* col1 =
          expr1->GetAs<ResolvedExpressionColumn>();
      const ResolvedExpressionColumn* col2 =
          expr2->GetAs<ResolvedExpressionColumn>();
      // Engine could be case sensitive for column names, so stay conservative
      // and do case sensitive comparison.
      if (col1->name() != col2->name()) {
        return false;
      }
      break;
    }
    case RESOLVED_COLUMN_REF: {
      const ResolvedColumnRef* ref1 = expr1->GetAs<ResolvedColumnRef>();
      const ResolvedColumnRef* ref2 = expr2->GetAs<ResolvedColumnRef>();
      if (ref1->column().column_id() != ref2->column().column_id()) {
        return false;
      }
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* str1 =
          expr1->GetAs<ResolvedGetStructField>();
      const ResolvedGetStructField* str2 =
          expr2->GetAs<ResolvedGetStructField>();
      ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr,
                       IsSameExpressionForGroupBy(str1->expr(), str2->expr()));
      if (str1->field_idx() != str2->field_idx() || !is_same_expr) {
        return false;
      }
      break;
    }
    case RESOLVED_GET_PROTO_FIELD: {
      const ResolvedGetProtoField* proto_field1 =
          expr1->GetAs<ResolvedGetProtoField>();
      const ResolvedGetProtoField* proto_field2 =
          expr2->GetAs<ResolvedGetProtoField>();
      ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr,
                       IsSameExpressionForGroupBy(proto_field1->expr(),
                                                  proto_field2->expr()));
      return proto_field1->expr()->type()->kind() ==
                 proto_field2->expr()->type()->kind() &&
             proto_field1->field_descriptor()->number() ==
                 proto_field2->field_descriptor()->number() &&
             proto_field1->default_value() == proto_field2->default_value() &&
             proto_field1->get_has_bit() == proto_field2->get_has_bit() &&
             proto_field1->format() == proto_field2->format() && is_same_expr;
      break;
    }
    case RESOLVED_CAST: {
      const ResolvedCast* cast1 = expr1->GetAs<ResolvedCast>();
      const ResolvedCast* cast2 = expr2->GetAs<ResolvedCast>();
      ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr, IsSameExpressionForGroupBy(
                                              cast1->expr(), cast2->expr()));
      if (!is_same_expr) {
        return false;
      }
      if (cast1->return_null_on_error() != cast2->return_null_on_error()) {
        return false;
      }
      break;
    }
    case RESOLVED_FUNCTION_CALL: {
      const ResolvedFunctionCall* func1 = expr1->GetAs<ResolvedFunctionCall>();
      const ResolvedFunctionCall* func2 = expr2->GetAs<ResolvedFunctionCall>();
      if (func1->function() != func2->function()) {
        return false;
      }
      if (func1->error_mode() != func2->error_mode()) {
        return false;
      }
      if (func1->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return false;
      }
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg1_list =
          func1->argument_list();
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg2_list =
          func2->argument_list();
      if (arg1_list.size() != arg2_list.size()) {
        return false;
      }
      for (int idx = 0; idx < arg1_list.size(); ++idx) {
        ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr,
                         IsSameExpressionForGroupBy(arg1_list[idx].get(),
                                                    arg2_list[idx].get()));
        if (!is_same_expr) {
          return false;
        }
      }
      if (func1->collation_list().size() != func2->collation_list().size()) {
        return false;
      }
      // If the function arguments are considered the same for group by, we
      // assume the <collation_list> in expressions should be the same. Returns
      // an internal error if that's not the case.
      for (int idx = 0; idx < func1->collation_list().size(); ++idx) {
        ZETASQL_RET_CHECK(func1->collation_list(idx).Equals(func2->collation_list(idx)))
            << "Different collation_list in expressions: "
            << ResolvedCollation::ToString(func1->collation_list()) << " vs "
            << ResolvedCollation::ToString(func2->collation_list());
        if (!func1->collation_list(idx).Equals(func2->collation_list(idx))) {
          return false;
        }
      }
      break;
    }
    case RESOLVED_GET_JSON_FIELD: {
      const auto* json_field1 = expr1->GetAs<ResolvedGetJsonField>();
      const auto* json_field2 = expr2->GetAs<ResolvedGetJsonField>();
      ZETASQL_ASSIGN_OR_RETURN(
          bool is_same_expr,
          IsSameExpressionForGroupBy(json_field1->expr(), json_field2->expr()));
      return is_same_expr &&
             json_field1->field_name() == json_field2->field_name();
    }
    default:
      // Without explicit support for this type of expression, pessimistically
      // say that they are not equal.
      // TODO: Add the following:
      // - RESOLVED_MAKE_STRUCT
      return false;
  }

  ZETASQL_RETURN_IF_ERROR(expr1->CheckFieldsAccessed());
  ZETASQL_RETURN_IF_ERROR(expr2->CheckFieldsAccessed());
  return true;
}

size_t FieldPathHash(const ResolvedExpr* expr) {
  ZETASQL_DCHECK(expr != nullptr);
  switch (expr->node_kind()) {
    case RESOLVED_GET_PROTO_FIELD: {
      // Note that this only hashes the top-level type (e.g. ARRAY).
      const ResolvedGetProtoField* proto_field =
          expr->GetAs<ResolvedGetProtoField>();
      return absl::Hash<std::tuple<int, int, int, size_t>>()(
          std::make_tuple(expr->node_kind(), expr->type()->kind(),
                          proto_field->field_descriptor()->number(),
                          FieldPathHash(proto_field->expr())));
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* struct_field =
          expr->GetAs<ResolvedGetStructField>();
      // Note that this only hashes the top-level type (e.g. ARRAY).
      return absl::Hash<std::tuple<int, int, int, size_t>>()(std::make_tuple(
          expr->node_kind(), expr->type()->kind(), struct_field->field_idx(),
          FieldPathHash(struct_field->expr())));
    }
    case RESOLVED_COLUMN_REF:
      return absl::Hash<std::pair<int, int>>()(std::make_pair(
          expr->node_kind(),
          expr->GetAs<ResolvedColumnRef>()->column().column_id()));
    default:
      return absl::Hash<int>()(expr->node_kind());
  }
}

bool IsSameFieldPath(const ResolvedExpr* field_path1,
                     const ResolvedExpr* field_path2,
                     FieldPathMatchingOption match_option) {
  // Checking types is a useful optimization to allow returning early. However,
  // we can't rely on Type::Equals() because it considers two otherwise
  // identical proto descriptors from different descriptor pools to be
  // different. So we compare Type::Kinds instead.
  if (field_path1->node_kind() != field_path2->node_kind() ||
      field_path1->type()->kind() != field_path2->type()->kind()) {
    return false;
  }

  switch (field_path1->node_kind()) {
    case RESOLVED_GET_PROTO_FIELD: {
      const ResolvedGetProtoField* proto_field1 =
          field_path1->GetAs<ResolvedGetProtoField>();
      const ResolvedGetProtoField* proto_field2 =
          field_path2->GetAs<ResolvedGetProtoField>();

      const bool field_paths_match =
          (proto_field1->expr()->type()->kind() ==
           proto_field2->expr()->type()->kind()) &&
          proto_field1->field_descriptor()->number() ==
              proto_field2->field_descriptor()->number() &&
          proto_field1->default_value() == proto_field2->default_value() &&
          proto_field1->get_has_bit() == proto_field2->get_has_bit() &&
          proto_field1->format() == proto_field2->format() &&
          IsSameFieldPath(proto_field1->expr(), proto_field2->expr(),
                          match_option);
      if (match_option == FieldPathMatchingOption::kFieldPath) {
        return field_paths_match;
      }
      return field_paths_match &&
             proto_field1->type()->Equals(proto_field2->type()) &&
             proto_field1->expr()->type()->Equals(
                 proto_field2->expr()->type()) &&
             proto_field1->return_default_value_when_unset() ==
                 proto_field2->return_default_value_when_unset();
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* struct_field1 =
          field_path1->GetAs<ResolvedGetStructField>();
      const ResolvedGetStructField* struct_field2 =
          field_path2->GetAs<ResolvedGetStructField>();

      const bool field_paths_match =
          (struct_field1->expr()->type()->kind() ==
           struct_field2->expr()->type()->kind()) &&
          (struct_field1->field_idx() == struct_field2->field_idx()) &&
          IsSameFieldPath(struct_field1->expr(), struct_field2->expr(),
                          match_option);
      if (match_option == FieldPathMatchingOption::kFieldPath) {
        return field_paths_match;
      }
      return field_paths_match &&
             struct_field1->type()->Equals(struct_field2->type());
    }
    case RESOLVED_COLUMN_REF: {
      // Ignore ResolvedColumnRef::is_correlated because it is IGNORABLE and
      // therefore semantically meaningless. If the ResolvedColumnRefs indicate
      // the same ResolvedColumn, then the expressions must be equivalent.
      return field_path1->GetAs<ResolvedColumnRef>()->column() ==
             field_path2->GetAs<ResolvedColumnRef>()->column();
    }
    default:
      return false;
  }
}

}  // namespace zetasql
