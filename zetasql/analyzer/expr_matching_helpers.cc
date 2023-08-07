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
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Returns TestIsSameExpressionForGroupByResult::kNotEqual immediately if two
// primitive fields are not equal, otherwise continue. An example use case:
//
//   RETURN_IF_PRIMITIVE_NOT_EQUAL(expr1->field1(), expr1->field1());
//   RETURN_IF_PRIMITIVE_NOT_EQUAL(expr1->field2(), expr1->field2());
//   ...
//
#define RETURN_IF_PRIMITIVE_NOT_EQUAL(lhs, rhs)             \
  if (lhs != rhs) {                                         \
    return TestIsSameExpressionForGroupByResult::kNotEqual; \
  }

// Returns TestIsSameExpressionForGroupByResult::kNotEqual immediately if two
// object fields are not equal, otherwise continue. An example use case:
//
//   RETURN_IF_OBJECT_NOT_EQUAL(expr1->field1(), expr1->field1());
//   RETURN_IF_OBJECT_NOT_EQUAL(expr1->field2(), expr1->field2());
//   ...
//
#define RETURN_IF_OBJECT_NOT_EQUAL(lhs, rhs)                \
  if (!lhs.Equals(rhs)) {                                   \
    return TestIsSameExpressionForGroupByResult::kNotEqual; \
  }

// Returns the result immediately if the result of the
// TestIsSameExpressionForGroupBy function for two expressions is kNotEquala or
// kUnknown, otherwise continue. An example use case:
//
//   RETURN_IF_EXPR_NOT_EQUAL(expr1->sub_expr(), expr2->sub_expr());
//   RETURN_IF_NOT_EQUAL(expr1->field1(), expr1->field1());
//  ...
//
#define RETURN_IF_EXPR_NOT_EQUAL(expr1, expr2)                    \
  ZETASQL_ASSIGN_OR_RETURN(TestIsSameExpressionForGroupByResult result,   \
                   TestIsSameExpressionForGroupBy(expr1, expr2)); \
  if (result != TestIsSameExpressionForGroupByResult::kEqual) {   \
    return result;                                                \
  }

}  // namespace

absl::StatusOr<bool> IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                                const ResolvedExpr* expr2) {
  ZETASQL_ASSIGN_OR_RETURN(TestIsSameExpressionForGroupByResult result,
                   TestIsSameExpressionForGroupBy(expr1, expr2));
  return result == TestIsSameExpressionForGroupByResult::kEqual;
}

absl::StatusOr<TestIsSameExpressionForGroupByResult>
TestIsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                               const ResolvedExpr* expr2) {
  RETURN_IF_PRIMITIVE_NOT_EQUAL(expr1->node_kind(), expr2->node_kind());
  // The type() method returns an abstract type pointer, no way to dereference
  // it and calls RETURN_IF_OBJECT_NOT_EQUAL.
  if (!expr1->type()->Equals(expr2->type())) {
    return TestIsSameExpressionForGroupByResult::kNotEqual;
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
      RETURN_IF_OBJECT_NOT_EQUAL(lit1->value(), lit2->value());
      break;
    }
    case RESOLVED_PARAMETER: {
      const ResolvedParameter* param1 = expr1->GetAs<ResolvedParameter>();
      const ResolvedParameter* param2 = expr2->GetAs<ResolvedParameter>();
      // Parameter names are normalized, so case sensitive comparison is ok.
      RETURN_IF_PRIMITIVE_NOT_EQUAL(param1->name(), param2->name());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(param1->position(), param2->position());
      break;
    }
    case RESOLVED_EXPRESSION_COLUMN: {
      const ResolvedExpressionColumn* col1 =
          expr1->GetAs<ResolvedExpressionColumn>();
      const ResolvedExpressionColumn* col2 =
          expr2->GetAs<ResolvedExpressionColumn>();
      // Engine could be case sensitive for column names, so stay conservative
      // and do case sensitive comparison.
      RETURN_IF_PRIMITIVE_NOT_EQUAL(col1->name(), col2->name());
      break;
    }
    case RESOLVED_COLUMN_REF: {
      const ResolvedColumnRef* ref1 = expr1->GetAs<ResolvedColumnRef>();
      const ResolvedColumnRef* ref2 = expr2->GetAs<ResolvedColumnRef>();
      RETURN_IF_PRIMITIVE_NOT_EQUAL(ref1->column().column_id(),
                                    ref2->column().column_id());
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* str1 =
          expr1->GetAs<ResolvedGetStructField>();
      const ResolvedGetStructField* str2 =
          expr2->GetAs<ResolvedGetStructField>();
      RETURN_IF_EXPR_NOT_EQUAL(str1->expr(), str2->expr());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(str1->field_idx(), str2->field_idx());
      break;
    }
    case RESOLVED_GET_PROTO_FIELD: {
      const ResolvedGetProtoField* proto_field1 =
          expr1->GetAs<ResolvedGetProtoField>();
      const ResolvedGetProtoField* proto_field2 =
          expr2->GetAs<ResolvedGetProtoField>();
      RETURN_IF_EXPR_NOT_EQUAL(proto_field1->expr(), proto_field2->expr());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(proto_field1->expr()->type()->kind(),
                                    proto_field2->expr()->type()->kind());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(proto_field1->field_descriptor()->number(),
                                    proto_field2->field_descriptor()->number());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(proto_field1->default_value(),
                                    proto_field2->default_value());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(proto_field1->get_has_bit(),
                                    proto_field2->get_has_bit());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(proto_field1->format(),
                                    proto_field2->format());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(
          proto_field1->return_default_value_when_unset(),
          proto_field2->return_default_value_when_unset());
      break;
    }
    case RESOLVED_CAST: {
      const ResolvedCast* cast1 = expr1->GetAs<ResolvedCast>();
      const ResolvedCast* cast2 = expr2->GetAs<ResolvedCast>();
      RETURN_IF_EXPR_NOT_EQUAL(cast1->expr(), cast2->expr());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(cast1->return_null_on_error(),
                                    cast2->return_null_on_error());
      break;
    }
    case RESOLVED_FUNCTION_CALL: {
      // TODO: Fix holes in ResolvedFunctionCall node comparison.
      // We need to compare generic_argument_list, hints and function signatures
      // as well.
      const ResolvedFunctionCall* func1 = expr1->GetAs<ResolvedFunctionCall>();
      const ResolvedFunctionCall* func2 = expr2->GetAs<ResolvedFunctionCall>();
      RETURN_IF_PRIMITIVE_NOT_EQUAL(func1->function(), func2->function());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(func1->error_mode(), func2->error_mode());
      if (func1->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return TestIsSameExpressionForGroupByResult::kNotEqual;
      }
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg1_list =
          func1->argument_list();
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg2_list =
          func2->argument_list();
      RETURN_IF_PRIMITIVE_NOT_EQUAL(arg1_list.size(), arg2_list.size());
      for (int idx = 0; idx < arg1_list.size(); ++idx) {
        RETURN_IF_EXPR_NOT_EQUAL(arg1_list[idx].get(), arg2_list[idx].get());
      }
      RETURN_IF_PRIMITIVE_NOT_EQUAL(func1->collation_list().size(),
                                    func2->collation_list().size());
      // If the function arguments are considered the same for group by, we
      // assume the <collation_list> in expressions should be the same. Returns
      // an internal error if that's not the case.
      for (int idx = 0; idx < func1->collation_list().size(); ++idx) {
        ZETASQL_RET_CHECK(func1->collation_list(idx).Equals(func2->collation_list(idx)))
            << "Different collation_list in expressions: "
            << ResolvedCollation::ToString(func1->collation_list()) << " vs "
            << ResolvedCollation::ToString(func2->collation_list());
        RETURN_IF_OBJECT_NOT_EQUAL(func1->collation_list(idx),
                                   func2->collation_list(idx));
      }
      break;
    }
    case RESOLVED_GET_JSON_FIELD: {
      const auto* json_field1 = expr1->GetAs<ResolvedGetJsonField>();
      const auto* json_field2 = expr2->GetAs<ResolvedGetJsonField>();
      RETURN_IF_EXPR_NOT_EQUAL(json_field1->expr(), json_field2->expr());
      RETURN_IF_PRIMITIVE_NOT_EQUAL(json_field1->field_name(),
                                    json_field2->field_name());
      break;
    }
    default:
      // Without explicit support for this type of expression, returns an
      // unknown comparison result.
      return TestIsSameExpressionForGroupByResult::kUnknown;
  }

  ZETASQL_RETURN_IF_ERROR(expr1->CheckFieldsAccessed());
  ZETASQL_RETURN_IF_ERROR(expr2->CheckFieldsAccessed());
  return TestIsSameExpressionForGroupByResult::kEqual;
}

size_t FieldPathHash(const ResolvedExpr* expr) {
  ABSL_DCHECK(expr != nullptr);
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

absl::StatusOr<bool> ExprReferencesNonCorrelatedColumn(
    const ResolvedExpr& expr) {
  ZETASQL_ASSIGN_OR_RETURN(absl::flat_hash_set<const ResolvedColumnRef*> column_refs,
                   CollectFreeColumnRefs(expr));

  for (const ResolvedColumnRef* column_ref : column_refs) {
    if (!column_ref->is_correlated()) {
      return true;
    }
  }
  return false;
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
          proto_field1->return_default_value_when_unset() ==
              proto_field2->return_default_value_when_unset() &&
          IsSameFieldPath(proto_field1->expr(), proto_field2->expr(),
                          match_option);
      if (match_option == FieldPathMatchingOption::kFieldPath) {
        return field_paths_match;
      }
      return field_paths_match &&
             proto_field1->type()->Equals(proto_field2->type()) &&
             proto_field1->expr()->type()->Equals(proto_field2->expr()->type());
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      // Do not need to check field_expr_is_positional as it is just for
      // printing nicely.
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
