//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_SCRIPTING_ERROR_HELPERS_H_
#define ZETASQL_SCRIPTING_ERROR_HELPERS_H_

#include "zetasql/common/errors.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/scripting/script_exception.pb.h"
#include "zetasql/scripting/script_segment.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_payload.h"

namespace zetasql {

// Returns a StatusBuilder adapter, which converts an ErrorLocation relative
// to a particular script segment, to one relative to the entire script.
//
// For example, in the script "SELECT 3; SELECT garbage;", this function
// would convert an ErrorLocation local to that particular statement (line 1,
// column 8) to an equivelant ErrorLocation relative to the entire script (line
// 1, column 18).
//
// Example usage:
//  ZETASQL_RETURN_IF_ERROR(DoSomethingWithScriptSegment).With(
//    AdjustErrorLocation(segment));
//
// If the status's ErrorLocation payload refers to a line/column number outside
// the bounds of the script, the returned status is a generic::internal
// with no ErrorLocation payload.
std::function<::zetasql_base::StatusBuilder(::zetasql_base::StatusBuilder)>
ConvertLocalErrorToScriptError(const ScriptSegment& segment);

// Similar to the above function, but consumes and returns an ErrorLocation,
// rather than a status.  Returns a generic::internal status if
// <error_location_in> is outside the bounds of the script.
zetasql_base::StatusOr<ErrorLocation> ConvertLocalErrorToScriptError(
    const ScriptSegment& segment, const ErrorLocation& error_location_in);

// Helper functions for evaluator and native procedure implementations to mark
// errors as handleable.
//
// TODO: Add optional parameters to these functions, allowing the
// engine to specify the value of engine-defined system variables, once system
// variable support is implemented.

// Creates a handleable script error tied to the location of <node>.
inline zetasql_base::StatusBuilder MakeScriptExceptionAt(const ASTNode* node) {
  return MakeSqlErrorAt(node).Attach(ScriptException());
}

// Creates a handleable script error not tied to any particular location.
inline zetasql_base::StatusBuilder MakeScriptException() {
  return MakeEvalError().Attach(ScriptException());
}

inline zetasql_base::StatusBuilder MakeScriptException(const ScriptException& ex) {
  return MakeEvalError().Attach(ex);
}

}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_ERROR_HELPERS_H_
