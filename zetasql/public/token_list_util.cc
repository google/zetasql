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

#include "zetasql/public/token_list_util.h"

#include <string>
#include <vector>

#include "zetasql/public/simple_token_list.h"
#include "zetasql/public/value.h"

namespace zetasql {

Value TokenListFromStringArray(std::vector<std::string> tokens) {
  return Value::TokenList(tokens::TokenList(std::move(tokens)));
}

}  // namespace zetasql
