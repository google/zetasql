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

#ifndef ZETASQL_PUBLIC_CATALOG_HELPER_H_
#define ZETASQL_PUBLIC_CATALOG_HELPER_H_

#include <string>
#include <vector>

namespace zetasql {

// Returns an entry from <possible_names> with the least edit-distance from
// <mistyped_name> (we allow a maximum edit-distance of ~20%), if one exists.
// Edit distance is computed case-sensitively.
// Internal names (with prefix '$') are excluded as possible suggestions.
//
// Returns an empty string if none of the entries in <possible_names> are
// within the allowed edit-distance.
std::string ClosestName(const std::string& mistyped_name,
                        const std::vector<std::string>& possible_names);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_CATALOG_HELPER_H_
