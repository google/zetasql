//
// Copyright 2018 ZetaSQL Authors
// Copyright 2017 Abseil Authors
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
#ifndef THIRD_PARTY_ZETASQL_ZETASQL_BASE_STRING_NUMBERS_H_
#define THIRD_PARTY_ZETASQL_ZETASQL_BASE_STRING_NUMBERS_H_

// This file contains std::string processing functions related to
// numeric values.

#include <cstdint>
#include "absl/strings/string_view.h"

namespace zetasql_base {

bool safe_strto32_base(absl::string_view text, int32_t* value, int base);

bool safe_strto64_base(absl::string_view text, int64_t* value, int base);

bool safe_strtou32_base(absl::string_view text, uint32_t* value, int base);

bool safe_strtou64_base(absl::string_view text, uint64_t* value, int base);

}  // namespace zetasql_base

#endif  // THIRD_PARTY_ZETASQL_ZETASQL_BASE_STRING_NUMBERS_H_
