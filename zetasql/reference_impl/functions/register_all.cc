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

#include "zetasql/reference_impl/functions/register_all.h"

#include "zetasql/reference_impl/functions/hash.h"
#include "zetasql/reference_impl/functions/json.h"
#include "zetasql/reference_impl/functions/string_with_collation.h"

namespace zetasql {

void RegisterAllOptionalBuiltinFunctions() {
  RegisterBuiltinJsonFunctions();
  RegisterBuiltinHashFunctions();
  RegisterBuiltinStringWithCollationFunctions();
}

}  // namespace zetasql
