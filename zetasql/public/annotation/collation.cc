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

#include "zetasql/public/annotation/collation.h"

#include <string>
#include <utility>
#include <variant>

#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/annotation/default_annotation_spec.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/ret_check.h"


namespace zetasql {

namespace {

// Returns true if <type> supports collation.
bool SupportsCollation(const Type* type) {
  if (type->IsString()) {
    return true;
  } else if (type->IsArray()) {
    return SupportsCollation(type->AsArray()->element_type());
  } else if (type->IsStruct()) {
    for (int i = 0; i < type->AsStruct()->num_fields(); i++) {
      if (SupportsCollation(type->AsStruct()->field(i).type)) {
        return true;
      }
    }
    return false;
  }
  return false;
}

std::string GetArgumentNameOrIndex(const FunctionSignature& signature, int i) {
  if (signature.ConcreteArgument(i).options().has_argument_name()) {
    return signature.ConcreteArgument(i).options().argument_name();
  } else {
    return std::to_string(i + 1);
  }
}

}  // namespace

absl::Status CollationAnnotation::RejectsCollationOnFunctionArguments(
    const ResolvedFunctionCallBase& function_call) {
  const FunctionSignature& signature = function_call.signature();
  // Index or name of argument is kept for error message.
  for (int i = 0; i < signature.NumConcreteArguments(); i++) {
    const zetasql::ResolvedExpr* arg_i = nullptr;
    if (function_call.argument_list_size() > 0) {
      arg_i = function_call.argument_list(i);
      // If the input path expression of FLATTEN function is effectively a
      // single array and does not dot into fields of its elements, the first
      // argument of FLATTEN function is the input array. Otherwise, the first
      // argument of FLATTEN function is set to be a ResolvedFlatten node whose
      // <expr> contains the input array.
      if (function_call.function()->IsZetaSQLBuiltin() &&
          signature.context_id() == FN_FLATTEN &&
          arg_i->node_kind() == RESOLVED_FLATTEN) {
        arg_i = arg_i->GetAs<ResolvedFlatten>()->expr();
      }
    } else if (function_call.generic_argument_list(i)->expr() != nullptr) {
      arg_i = function_call.generic_argument_list(i)->expr();
    } else {
      continue;
    }
    const AnnotationMap* argi_annotation_map = arg_i->type_annotation_map();
    if (CollationAnnotation::ExistsIn(argi_annotation_map)) {
      const std::string argument_name =
          (function_call.function()->IsZetaSQLBuiltin() &&
           signature.context_id() == FN_FLATTEN)
              ? "input array to FLATTEN"
              : absl::StrCat("argument ", GetArgumentNameOrIndex(signature, i));
      return MakeSqlError() << absl::Substitute(
                 "Collation is not allowed on $0 ($1)$2",
                 argument_name,
                 argi_annotation_map->DebugString(GetId()),
                 arg_i->type()->IsString()
                     ? ". Use COLLATE(arg, '') to remove collation"
                     : "");
    }
  }
  return absl::OkStatus();
}

absl::Status CollationAnnotation::CheckAndPropagateForFunctionCallBase(
    const ResolvedFunctionCallBase& function_call,
    AnnotationMap* result_annotation_map) {
  // TODO: add non-default propapation logic for functions.
  const FunctionSignature& signature = function_call.signature();
  if (signature.options().rejects_collation()) {
    ZETASQL_RETURN_IF_ERROR(RejectsCollationOnFunctionArguments(function_call));
    return absl::OkStatus();
  }
  // Default propagation rules.
  if (signature.options().propagates_collation() && signature.IsConcrete() &&
      SupportsCollation(signature.result_type().type())) {
    ZETASQL_ASSIGN_OR_RETURN(const AnnotationMap* collation_to_propagate,
                     GetCollationFromFunctionArguments(
                         /*error_location=*/nullptr, function_call,
                         FunctionEnums::AFFECTS_PROPAGATION));
    // If the result_type has option uses_array_element_for_collation enabled,
    // propagates the collation annotation to array element.
    if (signature.result_type().options().uses_array_element_for_collation()) {
      ZETASQL_RET_CHECK(result_annotation_map->IsArrayMap());
      result_annotation_map =
          result_annotation_map->AsArrayMap()->mutable_element();
    }
    ZETASQL_RETURN_IF_ERROR(
        MergeAnnotations(collation_to_propagate, *result_annotation_map));
  }
  return absl::OkStatus();
}

absl::StatusOr<const AnnotationMap*>
CollationAnnotation::GetCollationFromFunctionArguments(
    const ASTNode* error_location,
    const ResolvedFunctionCallBase& function_call,
    FunctionEnums::ArgumentCollationMode collation_mode_mask) {
  const FunctionSignature& signature = function_call.signature();
  const AnnotationMap* candidate_collation = nullptr;
  // Index or name of argument is kept for error message.
  std::string argument_index_or_name;
  for (int i = 0; i < signature.NumConcreteArguments(); i++) {
    const zetasql::ResolvedExpr* arg_i = nullptr;
    // The <function_call> has exactly one of 'argument_list' or
    // 'generic_argument_list' populated.  Usually 'argument_list' is used,
    // but the 'generic_argument_list' is used when there is a non-expression
    // argument (such as a lambda).  Only expressions can have collation, so
    // we only look for arguments that are expressions.
    if (function_call.argument_list_size() > 0) {
      arg_i = function_call.argument_list(i);
    } else if (function_call.generic_argument_list(i)->expr() != nullptr) {
      arg_i = function_call.generic_argument_list(i)->expr();
    } else {
      continue;
    }
    const AnnotationMap* argi_annotation_map = arg_i->type_annotation_map();
    const bool mode_matches =
        (signature.ConcreteArgument(i).options().argument_collation_mode() &
         collation_mode_mask) != 0;
    if (mode_matches && CollationAnnotation::ExistsIn(argi_annotation_map)) {
      // If an argument has option uses_array_element_for_collation enabled,
      // uses the collation annotation on array element.
      if (signature.ConcreteArgument(i)
              .options()
              .uses_array_element_for_collation()) {
        ZETASQL_RET_CHECK(argi_annotation_map->IsArrayMap());
        argi_annotation_map = argi_annotation_map->AsArrayMap()->element();
      }
      // If there is collation from the argument, it must be the same as the
      // previous arguments.
      if (candidate_collation == nullptr) {
        candidate_collation = argi_annotation_map;
        argument_index_or_name = GetArgumentNameOrIndex(signature, i);
      } else {
        if (!candidate_collation->HasEqualAnnotations(*argi_annotation_map,
                                                      GetId())) {
          // TODO: Add function to zetasql::Type class to output
          // collation within type like ARRAY<STRING COLLATE 'und:ci'>.
          ::zetasql_base::StatusBuilder error =
              MakeSqlError() << absl::Substitute(
                  "Collation for $0 is different on argument $1 ($2) and "
                  "argument $3 ($4)",
                  function_call.function()->SQLName(), argument_index_or_name,
                  candidate_collation->DebugString(GetId()),
                  GetArgumentNameOrIndex(signature, i),
                  argi_annotation_map->DebugString(GetId()));
          if (error_location != nullptr) {
            error.Attach(GetErrorLocationPoint(error_location,
                                               /*include_leftmost_child=*/true)
                             .ToInternalErrorLocation());
          }
          return error;
        }
      }
    }
  }
  return candidate_collation;
}

absl::Status CollationAnnotation::ResolveCollationForResolvedOrderByItem(
    ResolvedOrderByItem* resolved_order_by_item) {
  const ResolvedExpr* collation_name = resolved_order_by_item->collation_name();
  ResolvedCollation resolved_collation;
  if (collation_name != nullptr) {
    ZETASQL_RET_CHECK(collation_name->type()->IsString());
    if (collation_name->Is<ResolvedLiteral>()) {
      resolved_collation = ResolvedCollation::MakeScalar(
          collation_name->GetAs<ResolvedLiteral>()->value().string_value());
    }
  } else if (resolved_order_by_item->column_ref()->type_annotation_map() !=
             nullptr) {
    // There is collation to be propagated from the column_ref.
    ZETASQL_ASSIGN_OR_RETURN(
        resolved_collation,
        ResolvedCollation::MakeResolvedCollation(
            *resolved_order_by_item->column_ref()->type_annotation_map()));
  }
  resolved_order_by_item->set_collation(resolved_collation);
  return absl::OkStatus();
}

absl::Status CollationAnnotation::ScalarMergeIfCompatible(
    const AnnotationMap* in, AnnotationMap& out) const {
  if (!CollationAnnotation::ExistsIn(in)) {
    return absl::OkStatus();
  }
  return DefaultAnnotationSpec::ScalarMergeIfCompatible(in, out);
}

}  // namespace zetasql
