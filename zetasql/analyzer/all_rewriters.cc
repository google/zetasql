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

#include "zetasql/analyzer/all_rewriters.h"

#include <vector>

#include "zetasql/analyzer/anonymization_rewriter.h"
#include "zetasql/analyzer/rewriters/array_functions_rewriter.h"
#include "zetasql/analyzer/rewriters/flatten_rewriter.h"
#include "zetasql/analyzer/rewriters/map_function_rewriter.h"
#include "zetasql/analyzer/rewriters/pivot_rewriter.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/rewriters/typeof_function_rewriter.h"
#include "zetasql/analyzer/rewriters/unpivot_rewriter.h"

namespace zetasql {

const std::vector<const Rewriter*>& AllRewriters() {
  static const auto* const kRewriters = new std::vector<const Rewriter*>{
      GetAnonymizationRewriter() /* One per line reduces change conflicts */,
      GetArrayFunctionsRewriter(),
      GetFlattenRewriter(),
      GetMapFunctionRewriter(),
      GetPivotRewriter(),
      GetTypeofFunctionRewriter(),
      GetUnpivotRewriter(),
  };
  return *kRewriters;
}

}  // namespace zetasql
