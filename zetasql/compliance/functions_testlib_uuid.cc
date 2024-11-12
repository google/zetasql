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

#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/uuid_value.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT (build/include)
#include "absl/status/statusor.h"

namespace zetasql {

std::vector<FunctionTestCall> GetFunctionTestsUuidComparisons() {
  // We only add tests for "=" and "<", because the test driver automatically
  // generates all comparison functions for every test case.
  const Value kUuid1 = Uuid(
      UuidValue::FromString("00000000-0000-4000-8000-000000000000").value());
  const Value kUuid2 = Uuid(
      UuidValue::FromString("ffffffff-ffff-4fff-8fff-ffffffffffff").value());
  std::vector<FunctionTestCall> uuid_test_cases = {
      {"=", {NullUuid(), kUuid1}, NullBool()},
      {"=", {kUuid1, NullUuid()}, NullBool()},
      {"=", {NullUuid(), NullUuid()}, NullBool()},

      {"<", {NullUuid(), kUuid1}, NullBool()},
      {"<", {kUuid1, NullUuid()}, NullBool()},
      {"<", {NullUuid(), NullUuid()}, NullBool()},

      {"=", {kUuid1, kUuid1}, Bool(true)},
      {"=", {kUuid2, kUuid2}, Bool(true)},

      {"<", {kUuid1, kUuid2}, Bool(true)},
      {"<", {kUuid2, kUuid1}, Bool(false)},
  };

  for (auto& call : uuid_test_cases) {
    call.params.AddRequiredFeature(FEATURE_V_1_4_UUID_TYPE);
  }
  return uuid_test_cases;
}

}  // namespace zetasql
