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

#ifndef ZETASQL_PUBLIC_BUILTIN_FUNCTION_OPTIONS_H_
#define ZETASQL_PUBLIC_BUILTIN_FUNCTION_OPTIONS_H_

#include <stddef.h>

#include <map>
#include <string>

#include "zetasql/proto/options.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/bind_front.h"

namespace zetasql {

struct FunctionSignatureIdHasher {
  size_t operator()(FunctionSignatureId id) const {
    return static_cast<size_t>(id);
  }
};

// Controls whether builtin FunctionSignatureIds and their matching
// FunctionSignatures are included or excluded for an implementation.
//
// LanguageOptions are applied to determine set of candidate functions
// before inclusion/exclusion lists are applied.
struct ZetaSQLBuiltinFunctionOptions {
  // Default constructor.  If LanguageOptions are not passed in, default
  // to the maximum set of language features, along with the default
  // ProductMode (PRODUCT_INTERNAL) and TimestampMode (TIMESTAMP_MODE_NEW).
  ZetaSQLBuiltinFunctionOptions() {
    language_options.EnableMaximumLanguageFeatures();
  }

  ZetaSQLBuiltinFunctionOptions(const LanguageOptions& options)  // NOLINT
      : language_options(options) {}

  explicit ZetaSQLBuiltinFunctionOptions(
      const ZetaSQLBuiltinFunctionOptionsProto& proto)
      : language_options(proto.language_options()) {
    for (int i = 0; i < proto.include_function_ids_size(); ++i) {
      include_function_ids.insert(proto.include_function_ids(i));
    }
    for (int i = 0; i < proto.exclude_function_ids_size(); ++i) {
      exclude_function_ids.insert(proto.exclude_function_ids(i));
    }
  }

  // Specifies the set of language features that are enabled, which affects
  // the functions that should be loaded.  Also specifies the ProductMode
  // and TimestampMode.  This restriction is applied first, before the
  // include/exclude lists below.  For example, if FEATURE_ANALYTIC_FUNCTIONS
  // is not enabled, all analytic functions are skipped.
  LanguageOptions language_options;

  // If not empty, and a FunctionSignatureId is not in the set, then
  // the corresponding FunctionSignature is ignored.
  absl::flat_hash_set<FunctionSignatureId, FunctionSignatureIdHasher>
      include_function_ids;

  // Ignore FunctionSignatures for FunctionSignatureIds in this set.
  absl::flat_hash_set<FunctionSignatureId, FunctionSignatureIdHasher>
      exclude_function_ids;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_BUILTIN_FUNCTION_OPTIONS_H_
