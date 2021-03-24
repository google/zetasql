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

#ifndef ZETASQL_COMMON_AGGREGATE_NULL_HANDLING_H_
#define ZETASQL_COMMON_AGGREGATE_NULL_HANDLING_H_

#include "zetasql/resolved_ast/resolved_ast.h"

namespace zetasql {

// Returns true if the given aggregate function is known to ignore all rows
// where at least one input argument is NULL.
//
// The current implementation works as follows:
// - If a null-handling modifier is present, follow it.
// - Otherwise, consider it to respect nulls if it is a ZetaSQL builtin and
//     its name falls within a hard-coded list of null-respecting
//     functions. All ZetaSQL builtins not in this list are assumed to ignore
//     nulls.
bool IgnoresNullArguments(
    const ResolvedNonScalarFunctionCallBase* aggregate_function);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_AGGREGATE_NULL_HANDLING_H_
