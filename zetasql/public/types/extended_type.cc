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

#include "zetasql/public/types/extended_type.h"

#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_parameters.h"
#include "absl/status/statusor.h"

namespace zetasql {

bool ExtendedType::IsSupportedType(
    const LanguageOptions& language_options) const {
  return language_options.LanguageFeatureEnabled(FEATURE_EXTENDED_TYPES);
}

absl::StatusOr<std::string> ExtendedType::TypeNameWithParameters(
    const TypeParameters& type_params, ProductMode mode) const {
  ZETASQL_DCHECK(type_params.IsEmpty());
  return TypeName(mode);
}

}  // namespace zetasql
