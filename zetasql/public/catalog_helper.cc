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

#include "zetasql/public/catalog_helper.h"

#include <cstdint>
#include <functional>
#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/public/strings.h"
#include "absl/flags/flag.h"
#include "zetasql/base/edit_distance.h"

ABSL_FLAG(int64_t, zetasql_min_length_required_for_edit_distance, 3,
          "Minimum length required of the input string to find its closest "
          "match based on edit distance.");

namespace zetasql {

std::string ClosestName(const std::string& mistyped_name,
                        const std::vector<std::string>& possible_names) {
  if (mistyped_name.size() <
      absl::GetFlag(FLAGS_zetasql_min_length_required_for_edit_distance)) {
    return "";
  }

  // Allow ~20% edit distance for suggestions.
  const int distance_threshold = (mistyped_name.size() < 5)
                                 ? mistyped_name.size() / 2
                                 : mistyped_name.size() / 5 + 2;
  int min_edit_distance = distance_threshold + 1;

  int closest_name_index = -1;
  for (int i = 0; i < possible_names.size(); ++i) {
    // Exclude internal names (with '$' prefix).
    if (IsInternalAlias(possible_names[i])) continue;

    const int edit_distance = zetasql_base::CappedLevenshteinDistance(
        mistyped_name.begin(), mistyped_name.end(), possible_names[i].begin(),
        possible_names[i].end(), std::equal_to<char>(), distance_threshold + 1);
    if (edit_distance < min_edit_distance) {
      min_edit_distance = edit_distance;
      closest_name_index = i;
    } else if (edit_distance == min_edit_distance && closest_name_index != -1 &&
               possible_names[i].compare(possible_names[closest_name_index]) <
                   0) {
      // As a tie-breaker we chose the string which occurs lexicographically
      // first.
      closest_name_index = i;
    }
  }

  if (closest_name_index == -1) {
    // No match found within the allowed ~20% edit distance.
    return "";
  }

  ZETASQL_DCHECK_GE(closest_name_index, 0);
  ZETASQL_DCHECK_LT(closest_name_index, possible_names.size());
  return possible_names[closest_name_index];
}

}  // namespace zetasql
