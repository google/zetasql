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

#include <vector>

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/types/type.h"

namespace zetasql {

absl::Status RangeFunctionPreResolutionArgumentConstraint(
    const std::vector<InputArgumentType>& args, const LanguageOptions& opts) {
  if (args.size() != 2) {
    return MakeSqlError() << "RANGE() must take exactly two arguments";
  }
  if (args[0].is_null() && args[1].is_null()) {
    return MakeSqlError() << "at least one of RANGE() arguments must be typed";
  }
  if (args[0].type() != args[1].type() && !args[0].is_null() &&
      !args[1].is_null()) {
    return MakeSqlError() << "RANGE() arguments must be of the same type";
  }

  int element_type_idx = 0;
  if (args[0].is_null()) {
    element_type_idx = 1;
  }
  if (args[element_type_idx].type() == nullptr ||
      !RangeType::IsValidElementType(args[element_type_idx].type())) {
    return MakeSqlError() << args[element_type_idx].UserFacingName(
                                 opts.product_mode())
                          << " is not supported by RANGE()";
  }
  return absl::OkStatus();
}

absl::Status PostResolutionArgConstraintForFunctionsWithSingleRangeInput(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& args, const LanguageOptions& opts) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  // TODO: check the extra "is_negative_signature" flag here
  // instead of enumerating signatures.
  if (matched_signature.context_id() != FN_RANGE_IS_START_UNBOUNDED_INT64 &&
      matched_signature.context_id() != FN_RANGE_IS_END_UNBOUNDED_INT64 &&
      matched_signature.context_id() != FN_RANGE_START_INT64 &&
      matched_signature.context_id() != FN_RANGE_END_INT64) {
    return absl::OkStatus();
  }
  // If the argument is untyped NULL for RANGE, we decided to return an error
  // message for now until RANGE supports other types like INT64
  if (args[0].is_untyped_null()) {
    return MakeSqlError()
           << "A literal NULL argument cannot be accepted in place of RANGE. "
           << "Add a CAST to specify the type like CAST(NULL AS RANGE<DATE>). "
           << "Range supports DATE, DATETIME, and TIMESTAMP";
  }
  // TODO: Find a way to access the error message generation
  // helpers on the Function object to generate the best error message here.
  return MakeSqlError() << "RANGE does not support INT64. "
                        << "Range supports DATE, DATETIME, and TIMESTAMP";
}

absl::Status PostResolutionArgConstraintForFunctionsWithTwoRangeInputs(
    const FunctionSignature& matched_signature,
    const std::vector<InputArgumentType>& args, const LanguageOptions& opts) {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);
  // TODO: check the extra "is_negative_signature" flag here
  // instead of enumerating signatures.
  if (matched_signature.context_id() != FN_RANGE_OVERLAPS_LEFT_INT64 &&
      matched_signature.context_id() != FN_RANGE_OVERLAPS_RIGHT_INT64 &&
      matched_signature.context_id() != FN_RANGE_OVERLAPS_BOTH_INT64 &&
      matched_signature.context_id() != FN_RANGE_INTERSECT_LEFT_INT64 &&
      matched_signature.context_id() != FN_RANGE_INTERSECT_RIGHT_INT64 &&
      matched_signature.context_id() != FN_RANGE_INTERSECT_BOTH_INT64) {
    return absl::OkStatus();
  }
  // If any of the inputs is untyped NULL for RANGE, we return an error
  // message for now until RANGE supports other types like INT64
  if (args[0].is_untyped_null() || args[1].is_untyped_null()) {
    return MakeSqlError()
           << "A literal NULL argument cannot be accepted in place of RANGE. "
           << "Add a CAST to specify the type like CAST(NULL AS RANGE<DATE>). "
           << "Range supports DATE, DATETIME, and TIMESTAMP";
  }
  // TODO: Find a way to access the error message generation
  // helpers on the Function object to generate the best error message here.
  return MakeSqlError() << "RANGE does not support INT64. "
                        << "Range supports DATE, DATETIME, and TIMESTAMP";
}

void GetRangeFunctions(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int64_type = type_factory->get_int64();

  InsertFunction(functions, options, "range", Function::SCALAR,
                 {{
                     ARG_RANGE_TYPE_ANY,
                     {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
                     FN_RANGE,
                 }},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &RangeFunctionPreResolutionArgumentConstraint));
  InsertFunction(
      functions, options, "range_is_start_unbounded", Function::SCALAR,
      {{
           bool_type,
           {ARG_RANGE_TYPE_ANY},
           FN_RANGE_IS_START_UNBOUNDED,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           bool_type,
           {int64_type},
           FN_RANGE_IS_START_UNBOUNDED_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithSingleRangeInput));
  InsertFunction(
      functions, options, "range_is_end_unbounded", Function::SCALAR,
      {{
           bool_type,
           {ARG_RANGE_TYPE_ANY},
           FN_RANGE_IS_END_UNBOUNDED,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           bool_type,
           {int64_type},
           FN_RANGE_IS_END_UNBOUNDED_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithSingleRangeInput));
  InsertFunction(
      functions, options, "range_start", Function::SCALAR,
      {{
           ARG_TYPE_ANY_1,
           {ARG_RANGE_TYPE_ANY},
           FN_RANGE_START,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           int64_type,
           {int64_type},
           FN_RANGE_START_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithSingleRangeInput));
  InsertFunction(
      functions, options, "range_end", Function::SCALAR,
      {{
           ARG_TYPE_ANY_1,
           {ARG_RANGE_TYPE_ANY},
           FN_RANGE_END,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           int64_type,
           {int64_type},
           FN_RANGE_END_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithSingleRangeInput));
  InsertFunction(
      functions, options, "range_overlaps", Function::SCALAR,
      {{
           bool_type,
           {ARG_RANGE_TYPE_ANY, ARG_RANGE_TYPE_ANY},
           FN_RANGE_OVERLAPS,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           bool_type,
           {int64_type, ARG_RANGE_TYPE_ANY},
           FN_RANGE_OVERLAPS_LEFT_INT64,
       },
       {
           bool_type,
           {ARG_RANGE_TYPE_ANY, int64_type},
           FN_RANGE_OVERLAPS_RIGHT_INT64,
       },
       {
           bool_type,
           {int64_type, int64_type},
           FN_RANGE_OVERLAPS_BOTH_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithTwoRangeInputs));
  InsertFunction(
      functions, options, "range_intersect", Function::SCALAR,
      {{
           ARG_RANGE_TYPE_ANY,
           {ARG_RANGE_TYPE_ANY, ARG_RANGE_TYPE_ANY},
           FN_RANGE_INTERSECT,
       },
       // Take in INT64 to handle untyped NULL
       // TODO: Propose new FunctionSignatureOptions field to hide
       // this from suggestions in error messages and to provide a signal to
       // IDE tools and the documentation enforcement tools that this function
       // signature should be ignored.
       {
           int64_type,
           {int64_type, ARG_RANGE_TYPE_ANY},
           FN_RANGE_INTERSECT_LEFT_INT64,
       },
       {
           int64_type,
           {ARG_RANGE_TYPE_ANY, int64_type},
           FN_RANGE_INTERSECT_RIGHT_INT64,
       },
       {
           int64_type,
           {int64_type, int64_type},
           FN_RANGE_INTERSECT_BOTH_INT64,
       }},
      FunctionOptions().set_post_resolution_argument_constraint(
          &PostResolutionArgConstraintForFunctionsWithTwoRangeInputs));
}

}  // namespace zetasql
