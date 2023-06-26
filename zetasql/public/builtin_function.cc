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

#include "zetasql/public/builtin_function.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class AnalyzerOptions;

using NameToFunctionMap =
    absl::flat_hash_map<std::string, std::unique_ptr<Function>>;
using NameToFunctionPtrMap = absl::flat_hash_map<std::string, const Function*>;
using NameToTypeMap = absl::flat_hash_map<std::string, const Type*>;

std::pair<const NameToFunctionPtrMap&, const NameToTypeMap&>
GetBuiltinFunctionsAndTypesForDefaultOptions() {
  static const auto kFunctionsAndTypes =
      []() -> std::pair<const NameToFunctionPtrMap&, const NameToTypeMap&> {
    // Process lifetime.
    static auto& type_factory = *(new TypeFactory);
    static auto& types = *(new NameToTypeMap);
    static auto& unowned_functions = *(new NameToFunctionPtrMap);

    NameToFunctionMap owned_functions;
    absl::Status status = GetBuiltinFunctionsAndTypes(
        BuiltinFunctionOptions::AllReleasedFunctions(), type_factory,
        owned_functions, types);
    // Non-OK status can be returned if the builtins_options is configured
    // incorrectly, or if an internal invariant is broken do to a bug in the
    // ZetaSQL library.
    ZETASQL_DCHECK_OK(status);
    for (auto& [name, function] : owned_functions) {
      unowned_functions.emplace(name, function.release());
    }
    return {unowned_functions, types};
  }();
  return kFunctionsAndTypes;
}

static const FunctionIdToNameMap& GetFunctionIdToNameMap() {
  static FunctionIdToNameMap* id_map = [] () {
    // Initialize map from ZetaSQL function to function names.
    FunctionIdToNameMap* id_map = new FunctionIdToNameMap();
    TypeFactory type_factory;
    NameToTypeMap types_ignored;
    NameToFunctionMap functions;

    // Enable the maximum language features.  This enables retrieving a maximum
    // set of functions and signatures.
    //
    // TODO: Change this to use only stable features and a select list
    //   of "in_development" features that control function signatures as of the
    //   time the change is made. When that list becomes empty, convert this to
    //   use `GetAllBuiltinFunctionsAndTypes`.
    LanguageOptions options;
    options.EnableMaximumLanguageFeaturesForDevelopment();
    options.set_product_mode(PRODUCT_INTERNAL);

    absl::Status status = GetBuiltinFunctionsAndTypes(options, type_factory,
                                                      functions, types_ignored);
    ZETASQL_DCHECK_OK(status);

    for (const auto& function_entry : functions) {
      for (const FunctionSignature& signature :
           function_entry.second->signatures()) {
        if (signature.options().is_aliased_signature()) {
          continue;
        }
        zetasql_base::InsertOrDie(
            id_map,
            static_cast<FunctionSignatureId>(signature.context_id()),
            function_entry.first);
      }
    }
    return id_map;
  } ();
  return *id_map;
}

const std::string FunctionSignatureIdToName(FunctionSignatureId id) {
  const std::string* name = zetasql_base::FindOrNull(GetFunctionIdToNameMap(), id);
  if (name != nullptr) {
    return *name;
  }
  return absl::StrCat("<INVALID FUNCTION ID: ", id, ">");
}

void GetZetaSQLFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    std::map<std::string, std::unique_ptr<Function>>* functions) {
  NameToTypeMap types_ignored;
  NameToFunctionMap adequately_efficient_function_map;
  absl::Status status = GetBuiltinFunctionsAndTypes(
      options, *type_factory, adequately_efficient_function_map, types_ignored);
  for (auto& [name, function] : adequately_efficient_function_map) {
    functions->emplace(name, std::move(function));
  }
  ZETASQL_DCHECK_OK(status);
}

absl::Status GetZetaSQLFunctionsAndTypes(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    std::map<std::string, std::unique_ptr<Function>>* functions,
    NameToTypeMap* types) {
  NameToFunctionMap adequately_efficient_function_map;
  absl::Status status = GetBuiltinFunctionsAndTypes(
      options, *type_factory, adequately_efficient_function_map, *types);
  if (status.ok()) {
    for (auto& [name, function] : adequately_efficient_function_map) {
      functions->emplace(name, std::move(function));
    }
  }
  return status;
}

absl::Status GetBuiltinFunctionsAndTypes(
    const ZetaSQLBuiltinFunctionOptions& options, TypeFactory& type_factory,
    NameToFunctionMap& functions, NameToTypeMap& types) {
  // TODO: Enable these preconditions with global presubmit.
  // ZETASQL_RET_CHECK(types.empty());
  // ZETASQL_RET_CHECK(functions.empty());
  GetDatetimeFunctions(&type_factory, options, &functions);
  GetIntervalFunctions(&type_factory, options, &functions);
  GetArithmeticFunctions(&type_factory, options, &functions);
  GetBitwiseFunctions(&type_factory, options, &functions);
  GetAggregateFunctions(&type_factory, options, &functions);
  GetApproxFunctions(&type_factory, options, &functions);
  GetStatisticalFunctions(&type_factory, options, &functions);
  GetBooleanFunctions(&type_factory, options, &functions);
  GetLogicFunctions(&type_factory, options, &functions);
  GetStringFunctions(&type_factory, options, &functions);
  GetRegexFunctions(&type_factory, options, &functions);
  GetErrorHandlingFunctions(&type_factory, options, &functions);
  GetConditionalFunctions(&type_factory, options, &functions);
  GetMiscellaneousFunctions(&type_factory, options, &functions);
  GetArrayMiscFunctions(&type_factory, options, &functions);
  GetArrayAggregationFunctions(&type_factory, options, &functions);
  GetSubscriptFunctions(&type_factory, options, &functions);
  GetJSONFunctions(&type_factory, options, &functions);
  ZETASQL_RETURN_IF_ERROR(GetMathFunctions(&type_factory, options, &functions, &types));
  GetHllCountFunctions(&type_factory, options, &functions);
  GetD3ACountFunctions(&type_factory, options, &functions);
  GetKllQuantilesFunctions(&type_factory, options, &functions);
  GetProto3ConversionFunctions(&type_factory, options, &functions);
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_ANALYTIC_FUNCTIONS)) {
    GetAnalyticFunctions(&type_factory, options, &functions);
  }
  GetNetFunctions(&type_factory, options, &functions);
  GetHashingFunctions(&type_factory, options, &functions);
  if (options.language_options.LanguageFeatureEnabled(FEATURE_ENCRYPTION)) {
    GetEncryptionFunctions(&type_factory, options, &functions);
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_GEOGRAPHY)) {
    GetGeographyFunctions(&type_factory, options, &functions);
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_ANONYMIZATION)) {
    GetAnonFunctions(&type_factory, options, &functions);
  }
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_DIFFERENTIAL_PRIVACY)) {
    GetDifferentialPrivacyFunctions(&type_factory, options, &functions, &types);
  }
  GetTypeOfFunction(&type_factory, options, &functions);
  GetFilterFieldsFunction(&type_factory, options, &functions);
  if (options.language_options.LanguageFeatureEnabled(FEATURE_RANGE_TYPE)) {
    GetRangeFunctions(&type_factory, options, &functions);
  }
  GetArraySlicingFunctions(&type_factory, options, &functions);
  GetArrayFilteringFunctions(&type_factory, options, &functions);
  GetArrayTransformFunctions(&type_factory, options, &functions);
  GetArrayIncludesFunctions(&type_factory, options, &functions);
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_4_ARRAY_FIND_FUNCTIONS)) {
    ZETASQL_RETURN_IF_ERROR(
        GetArrayFindFunctions(&type_factory, options, &functions, &types));
  }
  return absl::OkStatus();
}

bool FunctionMayHaveUnintendedArgumentCoercion(const Function* function) {
  if (function->NumSignatures() == 0 ||
      !function->ArgumentsAreCoercible()) {
    return false;
  }
  // This only tests between signature arguments at the same argument
  // index.  It would not correctly analyze multiple signatures whose
  // corresponding arguments are not related to each other, but that
  // is not an issue at the time of the initial implementation.
  int max_num_arguments = 0;
  for (int signature_idx = 0; signature_idx < function->NumSignatures();
       ++signature_idx) {
    const FunctionSignature* signature = function->GetSignature(signature_idx);
    if (signature->arguments().size() > max_num_arguments) {
      max_num_arguments = signature->arguments().size();
    }
  }
  for (int argument_idx = 0; argument_idx < max_num_arguments; ++argument_idx) {
    bool has_signed_arguments = false;
    bool has_unsigned_arguments = false;
    bool has_floating_point_arguments = false;
    for (int signature_idx = 0; signature_idx < function->NumSignatures();
         ++signature_idx) {
      const FunctionSignature* signature =
          function->GetSignature(signature_idx);
      if (argument_idx < signature->arguments().size()) {
        const FunctionArgumentType& argument_type =
            signature->argument(argument_idx);
        if (argument_type.type() != nullptr) {
          if (argument_type.type()->IsSignedInteger()) {
            has_signed_arguments = true;
          } else if (argument_type.type()->IsUnsignedInteger()) {
            has_unsigned_arguments = true;
          } else if (argument_type.type()->IsFloatingPoint()) {
            has_floating_point_arguments = true;
          }
        }
      }
    }
    if (has_signed_arguments &&
        has_floating_point_arguments &&
        !has_unsigned_arguments) {
      return true;
    }
  }
  return false;
}

}  // namespace zetasql
