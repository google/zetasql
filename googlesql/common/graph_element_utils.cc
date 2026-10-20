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

#include "googlesql/common/graph_element_utils.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/functions/json.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/types/array_type.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

bool TypeIsOrContainsGraphElement(const Type* type) {
  if (type->IsGraphElement()) {
    return true;
  }

  for (const Type* component_type : type->ComponentTypes()) {
    if (TypeIsOrContainsGraphElement(component_type)) {
      return true;
    }
  }
  return false;
}

absl::StatusOr<JSONValue> MakePropertiesJsonValue(
    absl::Span<Value::Property> properties,
    const LanguageOptions& language_options) {
  functions::JsonObjectBuilder json_builder(language_options,
                                            /*canonicalize_zero=*/false);
  std::vector<absl::string_view> json_keys;
  std::vector<const Value*> json_values;
  for (const auto& p : properties) {
    json_keys.emplace_back(p.first);
    json_values.emplace_back(&p.second);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(JSONValue json_value,
                   functions::JsonObject(json_keys, absl::MakeSpan(json_values),
                                         json_builder));
  return json_value;
}

absl::StatusOr<std::string> GetPropertyName(
    const ResolvedGraphGetElementProperty* node) {
  GOOGLESQL_RET_CHECK(node != nullptr);
  if (node->property_name() != nullptr) {
    GOOGLESQL_RET_CHECK(node->property_name()->Is<ResolvedLiteral>() &&
              node->property_name()->type()->IsString())
        << "Expecting a string literal for property name, but got: "
        << node->property_name()->DebugString();
    return node->property_name()
        ->GetAs<ResolvedLiteral>()
        ->value()
        .string_value();
  }
  GOOGLESQL_RET_CHECK(node->property() != nullptr) << "Expecting property field is set "
                                            "in: "
                                         << node->DebugString();
  return node->property()->Name();
}

namespace {

bool IsDynamicPropertyEquals(const ResolvedNode* node) {
  return node->Is<ResolvedFunctionCall>() &&
         node->GetAs<ResolvedFunctionCall>()->function()->Name() ==
             "$dynamic_property_equals";
}

bool IsStaticPropertyEquals(const ResolvedNode* node) {
  return node->Is<ResolvedFunctionCall>() &&
         node->GetAs<ResolvedFunctionCall>()->function()->Name() == "$equal" &&
         node->GetAs<ResolvedFunctionCall>()->argument_list_size() == 2 &&
         node->GetAs<ResolvedFunctionCall>()
             ->argument_list(0)
             ->Is<ResolvedGraphGetElementProperty>();
}

}  // namespace

absl::StatusOr<bool> ContainsDynamicPropertySpecification(
    const ResolvedExpr* filter_expr,
    std::vector<const ResolvedExpr*>& property_specifications,
    std::vector<const ResolvedExpr*>& remaining_conjuncts) {
  if (filter_expr != nullptr && filter_expr->Is<ResolvedFunctionCall>()) {
    const auto* function_call = filter_expr->GetAs<ResolvedFunctionCall>();
    if (function_call->function()->Name() == "$and") {
      bool found_dynamic_property_specifications = false;
      for (const auto& argument : function_call->argument_list()) {
        GOOGLESQL_ASSIGN_OR_RETURN(
            bool found_dynamic_property_specification,
            ContainsDynamicPropertySpecification(
                argument.get(), property_specifications, remaining_conjuncts));
        found_dynamic_property_specifications |=
            found_dynamic_property_specification;
      }
      return found_dynamic_property_specifications;
    }
    if (IsDynamicPropertyEquals(function_call)) {
      property_specifications.push_back(function_call);
      return true;
    }
    if (IsStaticPropertyEquals(function_call)) {
      property_specifications.push_back(function_call);
      return false;
    }
  }
  // Otherwise, add the function call to remaining conjuncts.
  if (filter_expr != nullptr) {
    remaining_conjuncts.push_back(filter_expr);
  }
  return false;
}

absl::StatusOr<std::vector<std::pair<std::string, const ResolvedExpr*>>>
ToPropertySpecifications(std::vector<const ResolvedExpr*> exprs) {
  std::vector<std::pair<std::string, const ResolvedExpr*>>
      property_specifications;
  property_specifications.reserve(exprs.size());
  for (const ResolvedExpr* expr : exprs) {
    GOOGLESQL_RET_CHECK(expr->Is<ResolvedFunctionCall>());
    const auto* function_call = expr->GetAs<ResolvedFunctionCall>();
    if (IsDynamicPropertyEquals(function_call)) {
      GOOGLESQL_RET_CHECK_EQ(function_call->argument_list_size(), 3);
      GOOGLESQL_RET_CHECK(function_call->argument_list(1)->Is<ResolvedLiteral>());
      GOOGLESQL_RET_CHECK(function_call->argument_list(1)->type()->IsString());
      std::string property_name = function_call->argument_list(1)
                                      ->GetAs<ResolvedLiteral>()
                                      ->value()
                                      .string_value();
      property_specifications.emplace_back(property_name,
                                           function_call->argument_list(2));
    } else {
      GOOGLESQL_RET_CHECK(IsStaticPropertyEquals(function_call));
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::string property_name,
          GetPropertyName(function_call->argument_list(0)
                              ->GetAs<ResolvedGraphGetElementProperty>()));
      property_specifications.emplace_back(property_name,
                                           function_call->argument_list(1));
    }
  }
  GOOGLESQL_RET_CHECK_EQ(property_specifications.size(), exprs.size());
  return property_specifications;
}

bool IsRestrictivePathMode(ResolvedGraphPathModeEnums::PathMode path_mode) {
  return path_mode == ResolvedGraphPathModeEnums::SIMPLE ||
         path_mode == ResolvedGraphPathModeEnums::ACYCLIC ||
         path_mode == ResolvedGraphPathModeEnums::TRAIL;
}

bool HasSearchPrefix(
    ResolvedGraphPathSearchPrefixEnums::PathSearchPrefixType prefix_type) {
  return prefix_type != ResolvedGraphPathSearchPrefixEnums::
                            PATH_SEARCH_PREFIX_TYPE_UNSPECIFIED;
}

absl::StatusOr<std::optional<Value>> IntersectPeriods(const Value& period1,
                                                      const Value& period2) {
  GOOGLESQL_RET_CHECK(!period1.is_null() && !period2.is_null());
  GOOGLESQL_RET_CHECK(period1.type()->IsRange() && period2.type()->IsRange());
  GOOGLESQL_RET_CHECK(period1.type()->AsRange()->element_type()->IsTimestamp());
  GOOGLESQL_RET_CHECK(period2.type()->AsRange()->element_type()->IsTimestamp());

  // A NULL start sorts before any timestamp, so this picks the later start and
  // treats NULL as unbounded.
  Value intersect_start = period1.start().LessThan(period2.start())
                              ? period2.start()
                              : period1.start();
  Value intersect_end;
  if (period1.end().is_null()) {
    intersect_end = period2.end();
  } else if (period2.end().is_null()) {
    intersect_end = period1.end();
  } else {
    intersect_end =
        period1.end().LessThan(period2.end()) ? period1.end() : period2.end();
  }

  if (!intersect_start.is_null() && !intersect_end.is_null() &&
      !intersect_start.LessThan(intersect_end)) {
    return std::nullopt;
  }
  GOOGLESQL_ASSIGN_OR_RETURN(Value intersection,
                   Value::MakeRange(intersect_start, intersect_end));
  return intersection;
}

absl::StatusOr<bool> PeriodContainsPoint(const Value& period,
                                         const Value& point) {
  GOOGLESQL_RET_CHECK(!period.is_null() && !point.is_null());
  GOOGLESQL_RET_CHECK(period.type()->IsRange());
  GOOGLESQL_RET_CHECK(period.type()->AsRange()->element_type()->IsTimestamp());
  GOOGLESQL_RET_CHECK(point.type()->IsTimestamp());

  bool ge_start = period.start().is_null() || !point.LessThan(period.start());
  bool lt_end = period.end().is_null() || point.LessThan(period.end());

  return ge_start && lt_end;
}

}  // namespace googlesql
