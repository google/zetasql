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

#ifndef ZETASQL_ANALYZER_CONSTANT_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_CONSTANT_RESOLVER_HELPER_H_

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Returns the value of a ResolvedConstant node. Returns an error if the
// constant type is not supported or the value can not be lazily evaluated.
// If the constant is a SQLConstant without an initialized value,
// `analyzer_options` is used to evaluate it.
// REQUIRES: `analyzer_options` has a constant evaluator set.
absl::StatusOr<Value> GetResolvedConstantValue(
    const ResolvedConstant& node, const AnalyzerOptions& analyzer_options);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_CONSTANT_RESOLVER_HELPER_H_
