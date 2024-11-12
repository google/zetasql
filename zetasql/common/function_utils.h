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

#ifndef ZETASQL_COMMON_FUNCTION_UTILS_H_
#define ZETASQL_COMMON_FUNCTION_UTILS_H_

#include <string>
#include <vector>

namespace zetasql {

class Function;
class FunctionSignature;

// Helper function that used to implement Function::is_operator.
bool FunctionIsOperator(const Function& function_name);

// Update arguments to be used in GetSQL.
// This is used to ensure that named arguments are printed prefixed with their
// names.
void UpdateArgsForGetSQL(const FunctionSignature* signature,
                         std::vector<std::string>* args);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_FUNCTION_UTILS_H_
