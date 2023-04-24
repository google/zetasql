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

#include "zetasql/public/functions/range.h"

#include <optional>
#include <string>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/case.h"  
#include "zetasql/base/status.h"                         

namespace zetasql {

namespace {

std::optional<absl::string_view> UnboundedOrValue(
    absl::string_view boundary_value) {
  if (zetasql_base::CaseEqual(boundary_value, "UNBOUNDED") ||
      zetasql_base::CaseEqual(boundary_value, "NULL")) {
    return std::nullopt;
  }
  return {boundary_value};
}

}  // namespace

absl::StatusOr<StringRangeBoundaries> ParseRangeBoundaries(
    absl::string_view range_value, bool strict_formatting) {
  // Check that range_value starts with "[" and ends with ")"
  if (range_value.empty() || range_value.front() != '[' ||
      range_value.back() != ')') {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Failed to parse range: range must be formatted exactly "
        "as [START, END) (found %s)",
        range_value));
  }

  // Remove the "[" from the start and ")" from end
  range_value.remove_prefix(1);
  range_value.remove_suffix(1);
  // TODO: Update to handle FORMAT clause,
  // including dates with formats with "," such as "May 3rd, 2023"
  std::vector<absl::string_view> range_parts = absl::StrSplit(range_value, ',');
  if (range_parts.size() != 2 ||
      (strict_formatting && !absl::StartsWith(range_parts[1], " "))) {
    std::string separator = ", ";
    if (!strict_formatting) {
      separator = ",";
    }
    return absl::InvalidArgumentError(
        "Failed to parse range: range must be formatted exactly as [START, "
        "END) with two parts, START and END, divided with \"" +
        separator + "\"");
  }

  absl::string_view start = absl::StripAsciiWhitespace(range_parts[0]);
  absl::string_view end = absl::StripAsciiWhitespace(range_parts[1]);
  if (strict_formatting) {
    if (start.length() != range_parts[0].length()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Failed to parse range: range must be formatted "
                          "exactly as [START, END) with START having "
                          "no leading or trailing spaces, but found: \"%s\"",
                          range_parts[0]));
    }
    // range_parts[1] will have a leading space from delimiter ", "
    // So +1 to trimmed string length to account for it
    if (end.length()+1 != range_parts[1].length()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Failed to parse range: range must be formatted "
                          "exactly as [START, END) with END having "
                          "no leading or trailing spaces, but found: \"%s\"",
                          range_parts[1]));
    }
  }

  return StringRangeBoundaries{.start = UnboundedOrValue(start),
                               .end = UnboundedOrValue(end)};
}

}  // namespace zetasql
