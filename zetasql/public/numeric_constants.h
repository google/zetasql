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

#ifndef ZETASQL_PUBLIC_NUMERIC_CONSTANTS_H_
#define ZETASQL_PUBLIC_NUMERIC_CONSTANTS_H_


#include <cstdint>

#include <cstdint>  

// Common constants used by numeric_parser and numeric_value libraries.
namespace zetasql {
namespace internal {
constexpr uint32_t k1e9 = 1000 * 1000 * 1000;
constexpr uint64_t k1e19 = static_cast<uint64_t>(k1e9) * k1e9 * 10;
constexpr __int128 k1e38 = static_cast<__int128>(k1e19) * k1e19;
}  // namespace internal
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_NUMERIC_CONSTANTS_H_
