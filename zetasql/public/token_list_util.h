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

#ifndef ZETASQL_PUBLIC_TOKEN_LIST_UTIL_H_
#define ZETASQL_PUBLIC_TOKEN_LIST_UTIL_H_

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Creates a TokenList value from a vector of strings.
Value TokenListFromStringArray(std::vector<std::string> tokens);

// Returns a vector of strings from a TokenList value. Each string is the text
// representation of a token in the TokenList.
absl::StatusOr<std::vector<std::string>> StringArrayFromTokenList(
    const Value& token_list);

// Creates a TokenList value from bytes that are serialized from a TokenList.
Value TokenListFromBytes(std::string bytes);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TOKEN_LIST_UTIL_H_
