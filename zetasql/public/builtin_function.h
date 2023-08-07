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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/proto/options.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/type.h"
#include "absl/base/macros.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"

namespace zetasql {

// Returns collections of Functions and named Types that are part of the
// ZetaSQL core library. `Type*` that are returned in `types` or are used in
// the FunctionSignatures are either statically allocated or allocated and owned
// by `type_factory`.
//
// The primary intended use of this API is to get `FunctionSignature` and `Type`
// objects to populate a `zetasql::Catalog` instance for Analysis. Other tools
// and utilities can also use this to get the list of built-in Functions and
// Types.
//
// `options` is used to control which function signatures and types are
//    returned. See `BuiltinFunctionOptions` for details on how to configure
//    this parameter.
// `type_factory` is used to allocate types used in function signatures and
//    returned in `types`. The lifetime of `type_factory` must exceed the
//    lifetime of `functions` and `types`.
// `functions` is used to return built-in function signatures. It must be empty.
// `types` is used to return built-in named Types. It must be empty.
absl::Status GetBuiltinFunctionsAndTypes(
    const BuiltinFunctionOptions& options, TypeFactory& type_factory,
    absl::flat_hash_map<std::string, std::unique_ptr<Function>>& functions,
    absl::flat_hash_map<std::string, const Type*>& types);
// DEPRECATED - As above but taking LanguageOptions rather than
// BuiltinFunctionOptions
ABSL_DEPRECATED("Inline me!")
inline absl::Status GetBuiltinFunctionsAndTypes(
    const LanguageOptions& options, TypeFactory& type_factory,
    absl::flat_hash_map<std::string, std::unique_ptr<Function>>& functions,
    absl::flat_hash_map<std::string, const Type*>& types) {
  return GetBuiltinFunctionsAndTypes(BuiltinFunctionOptions(options),
                                     type_factory, functions, types);
}

// Returns statically allocated collections of all released FunctionSignatures
// and Types that are part of the ZetaSQL core library, using a reasonable
// default `LanguageOptions`. This includes Functions and Types that are part of
// fully-implemented features for ProductMode `PRODUCT_INTERNAL`. In-development
// features are excluded.
//
// Returned `Function*` and `Type*` are statically allocated and have process
// lifetime.
//
// This API is convenient for tools and utilities that want the full set of
// possible builtin FunctionSignatures and want to reference a statically
// allocated collection for efficiency. The likely calling pattern is:
// ```
//   auto [kAllBuiltinFunctions, kAllBuiltinTypes] =
//        zetasql::GetAllBuiltinFunctionsAndTypesStatic();
// ```
//
// Generally, this is not an appropriate API for populating a Catalog instance
// for query analysis. For that, use `GetBuiltinFunctionsAndTypes` with an
// `options` object initialized to complement the `AnalyzerOptions` used
// for query analysis.
std::pair<const absl::flat_hash_map<std::string, const Function*>&,
          const absl::flat_hash_map<std::string, const Type*>&>
GetBuiltinFunctionsAndTypesForDefaultOptions();

const std::string FunctionSignatureIdToName(FunctionSignatureId id);

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

// DEPRECATED: Use GetBuiltinFunctionsAndTypes
//
ABSL_DEPRECATED("Inline me!")
void GetZetaSQLFunctions(  // NOLINT
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    std::map<std::string, std::unique_ptr<Function>>* functions);
// DEPRECATED - As above but accepting a LanguageOptions rather than a
// BuiltinFunctionOptions.
ABSL_DEPRECATED("Inline me!")
inline void GetZetaSQLFunctions(
    TypeFactory* type_factory, const LanguageOptions& options,
    std::map<std::string, std::unique_ptr<Function>>* functions) {
  GetZetaSQLFunctions(type_factory,
                        zetasql::BuiltinFunctionOptions(options), functions);
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_BUILTIN_FUNCTION_H_
