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

#include <ctype.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/base/case.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class AnalyzerOptions;

static const FunctionIdToNameMap& GetFunctionIdToNameMap() {
  static FunctionIdToNameMap* id_map = [] () {
    // Initialize map from ZetaSQL function to function names.
    FunctionIdToNameMap* id_map = new FunctionIdToNameMap();
    TypeFactory type_factory;
    NameToFunctionMap functions;

    // Enable the maximum language features.  This enables retrieving a maximum
    // set of functions and signatures.
    //
    // TODO: Maybe it is better to add a new function
    // GetAllZetaSQLFunctionsAndSignatures() that does not take any options,
    // to ensure that we get them all.  This could be a ZetaSQL-internal
    // only function.
    LanguageOptions options;
    options.EnableMaximumLanguageFeaturesForDevelopment();
    options.set_product_mode(PRODUCT_INTERNAL);

    GetZetaSQLFunctions(&type_factory, options, &functions);

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

using NameToFunctionMap = std::map<std::string, std::unique_ptr<Function>>;

void GetZetaSQLFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  GetDatetimeFunctions(type_factory, options, functions);
  GetIntervalFunctions(type_factory, options, functions);
  GetArithmeticFunctions(type_factory, options, functions);
  GetBitwiseFunctions(type_factory, options, functions);
  GetAggregateFunctions(type_factory, options, functions);
  GetApproxFunctions(type_factory, options, functions);
  GetStatisticalFunctions(type_factory, options, functions);
  GetBooleanFunctions(type_factory, options, functions);
  GetLogicFunctions(type_factory, options, functions);
  GetStringFunctions(type_factory, options, functions);
  GetRegexFunctions(type_factory, options, functions);
  GetMiscellaneousFunctions(type_factory, options, functions);
  GetSubscriptFunctions(type_factory, options, functions);
  GetJSONFunctions(type_factory, options, functions);
  GetMathFunctions(type_factory, options, functions);
  GetHllCountFunctions(type_factory, options, functions);
  GetKllQuantilesFunctions(type_factory, options, functions);
  GetProto3ConversionFunctions(type_factory, options, functions);
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_ANALYTIC_FUNCTIONS)) {
    GetAnalyticFunctions(type_factory, options, functions);
  }
  GetNetFunctions(type_factory, options, functions);
  GetHashingFunctions(type_factory, options, functions);
  if (options.language_options.LanguageFeatureEnabled(FEATURE_ENCRYPTION)) {
    GetEncryptionFunctions(type_factory, options, functions);
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_GEOGRAPHY)) {
    GetGeographyFunctions(type_factory, options, functions);
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_ANONYMIZATION)) {
    GetAnonFunctions(type_factory, options, functions);
  }
  GetTypeOfFunction(type_factory, options, functions);
  GetFilterFieldsFunction(type_factory, options, functions);
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
