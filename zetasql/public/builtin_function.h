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

#ifndef ZETASQL_PUBLIC_BUILTIN_FUNCTION_H_
#define ZETASQL_PUBLIC_BUILTIN_FUNCTION_H_

#include <stddef.h>

#include <map>
#include <string>

#include "zetasql/proto/options.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"

namespace zetasql {

const std::string FunctionSignatureIdToName(FunctionSignatureId id);

// Populates <functions> with a list of built-in ZetaSQL functions. New
// Types are added to <type_factory> as needed to support those functions.
// <type_factory> must outlive <functions>.
// The <options> identify specific ZetaSQL FunctionSignatures that are
// explicitly included or excluded from the returned map.
// A LanguageOptions or AnalyzerOptions object can be passed to <options>
// using the implicit constructor.
//
// It's guaranteed that none of the built-in function names/aliases starts with
// "[a-zA-Z]_", which is reserved for user-defined objects.
void GetZetaSQLFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    std::map<std::string, std::unique_ptr<Function>>* functions);

// If the function allows argument coercion, then checks the function
// signatures to see if they are defined for floating point and
// only one of signed/unsigned integer arguments (but not both integer
// type arguments), and returns true if so.  Otherwise, returns false.
//
// This check is only used in unit tests as a sanity check to ensure that
// the set of signatures for a function collectively make sense; it does
// not get used at analysis time during function resolution.  It is
// intended to verify that function signatures do not allow implicit
// coercion of unsigned integer to a floating point type while signed
// integers have their own signatures (this combination has been incorrectly
// used in the past where such coercions were inadvertently allowed when
// they should not have been).  In the usual case, if signed integers
// have their own signatures then unsigned integers should have their own
// signatures as well (or coercion of unsigned integer arguments should be
// explicitly disabled).
//
// Note that this function does not take into consideration any argument
// constraints that might be present.
bool FunctionMayHaveUnintendedArgumentCoercion(const Function* function);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_BUILTIN_FUNCTION_H_
