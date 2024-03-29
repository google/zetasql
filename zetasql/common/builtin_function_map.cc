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
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

namespace {
constexpr absl::string_view kMapFromArray = "MAP_FROM_ARRAY";
}

static absl::Status CheckMapFromArrayPreResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 1) {
    return MakeSqlError() << "No matching signature for function "
                          << kMapFromArray
                          << ". Supported signature: " << kMapFromArray
                          << "(ARRAY<STRUCT<T1, T2>>)";
  }

  if (arguments[0].is_untyped()) {
    return MakeSqlError()
           << kMapFromArray
           << " result type cannot be determined from "
              "argument "
           << arguments[0].UserFacingName(language_options.product_mode())
           << ". Consider casting the argument to ARRAY<STRUCT<T1, T2>> so "
              "that key type T1 and value type T2 can be determined from the "
              "argument";
  }
  return absl::OkStatus();
}

static absl::StatusOr<const Type*> ComputeMapFromArrayResultType(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& signature,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 1);
  auto& input_argument = arguments[0];

  auto make_error_struct_arr_expected =
      [&]() {
        return MakeSqlError()
               << kMapFromArray
               << " input argument must be an array of structs, but got type "
               << input_argument.type()->TypeName(
                      analyzer_options.language().product_mode());
      };
  if (!input_argument.type()->IsArray()) {
    return make_error_struct_arr_expected();
  }

  auto* array_element_type = input_argument.type()->AsArray()->element_type();

  if (!array_element_type->IsStruct()) {
    return make_error_struct_arr_expected();
  }

  auto* struct_type = array_element_type->AsStruct();
  if (struct_type->num_fields() != 2) {
    return MakeSqlError()
           << kMapFromArray << " input array must be of type "
           << "ARRAY<STRUCT<T1, T2>>, but found a struct member with "
           << struct_type->num_fields() << " fields";
  }

  if (!struct_type->field(0).type->SupportsGrouping(
          analyzer_options.language())) {
    return MakeSqlError() << kMapFromArray
                          << " expected a groupable key, but got a key of type "
                          << struct_type->field(0).type->TypeName(
                                 analyzer_options.language().product_mode())
                          << ", which does not support grouping";
  }
  return type_factory->MakeMapType(struct_type->field(0).type,
                                   struct_type->field(1).type);
}

void GetMapCoreFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  // MAP_FROM_ARRAY(ARRAY<STRUCT<K,V>> entries) -> MAP<K,V>
  InsertFunction(
      functions, options, "map_from_array", Function::SCALAR,
      {{ARG_TYPE_ARBITRARY,
        {ARG_ARRAY_TYPE_ANY_1},
        FN_MAP_FROM_ARRAY,
        // TODO: Collation support for MAP<> type.
        FunctionSignatureOptions().set_rejects_collation()}},
      FunctionOptions()
          .set_compute_result_type_callback(&ComputeMapFromArrayResultType)
          .set_pre_resolution_argument_constraint(
              &CheckMapFromArrayPreResolutionArguments)
          .add_required_language_feature(FEATURE_V_1_4_MAP_TYPE));
}

}  // namespace zetasql
