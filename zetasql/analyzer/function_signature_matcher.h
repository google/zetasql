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

#ifndef ZETASQL_ANALYZER_FUNCTION_SIGNATURE_MATCHER_H_
#define ZETASQL_ANALYZER_FUNCTION_SIGNATURE_MATCHER_H_

#include <memory>
#include <vector>

#include "zetasql/public/coercer.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/types/type_factory.h"

namespace zetasql {

// Determines if the function signature matches the argument list, returning
// a non-templated signature if true.  If <allow_argument_coercion> is TRUE
// then function arguments can be coerced to the required signature
// type(s), otherwise they must be an exact match.
bool FunctionSignatureMatches(
    const LanguageOptions& language_options, const Coercer& coercer,
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, bool allow_argument_coercion,
    TypeFactory* type_factory,
    std::unique_ptr<FunctionSignature>* result_signature,
    SignatureMatchResult* signature_match_result);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_FUNCTION_SIGNATURE_MATCHER_H_
