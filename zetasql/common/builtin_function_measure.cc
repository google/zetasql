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
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"

namespace zetasql {

void GetMeasureFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  InsertFunction(
      functions, options, "AGG", Function::AGGREGATE,
      {{ARG_TYPE_ANY_1,
        {ARG_MEASURE_TYPE_ANY_1},
        FN_AGG,
        FunctionSignatureOptions().set_rewrite_options(
            FunctionSignatureRewriteOptions().set_enabled(true).set_rewriter(
                REWRITE_MEASURE_TYPE))}},
      DefaultAggregateFunctionOptions().AddRequiredLanguageFeature(
          FEATURE_ENABLE_MEASURES));
}

}  // namespace zetasql
