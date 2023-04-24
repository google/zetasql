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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_
#define ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_

#include <optional>

#include "absl/status/statusor.h"

// Helper functions used for parsing range values from strings.
namespace zetasql {

struct StringRangeBoundaries {
  std::optional<absl::string_view> start;
  std::optional<absl::string_view> end;
};

// Extracts the "start" and "end" parts from a given range literal string
// with format "[<start>, <end>]", and returns it as a std::pair of optional
// "start" and "end" parts respectively. If the "start" and "end" parts are
// unbounded, then std::nullopt is returned in the corresponding part of the
// pair.
// If "strict_formatting" is set to true, then method will require
// <start> and <end> to be delimited by ", " (comma then space) with no extra
// trailing spaces, and will return error if it's not the case.
absl::StatusOr<StringRangeBoundaries> ParseRangeBoundaries(
    absl::string_view range_value, bool strict_formatting = true);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_
