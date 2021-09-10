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

#include "zetasql/common/evaluator_registration_utils.h"

#include "zetasql/public/collator.h"
#include "zetasql/reference_impl/functions/register_all.h"
#include "absl/base/call_once.h"
#include "zetasql/base/status_builder.h"

namespace zetasql::internal {

void EnableFullEvaluatorFeatures() {
  // Because registration is global, it doesn't need to run more than once
  // for a process. This saves a bit of time and avoids lock contention when
  // this function is called repeatedly.
  static absl::once_flag once;
  absl::call_once(once, []() {
    RegisterAllOptionalBuiltinFunctions();
    RegisterIcuCollatorImpl(
        [](absl::string_view collation_name, CollatorLegacyUnicodeMode mode) {
          return zetasql::MakeSqlCollator(collation_name, mode);
        });
  });
}

}  // namespace zetasql::internal
