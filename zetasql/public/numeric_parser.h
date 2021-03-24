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

#ifndef ZETASQL_PUBLIC_NUMERIC_PARSER_H_
#define ZETASQL_PUBLIC_NUMERIC_PARSER_H_

#include <cstdint>

#include "zetasql/common/multiprecision_int.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

// Helper functions used for parsing number values from strings.
namespace zetasql {

// FixedPointRepresentation holds a parsed number value. The struct uses an
// array of 64 bit words to hold a scaled version of the parsed number value
// (see multiprecision_int.h for number representation details in the array).
// The scale chosen for representing the parsed value depends on the
// method used to populate the struct.
template <uint32_t word_count>
struct FixedPointRepresentation {
  bool is_negative;
  FixedUint<64, word_count> output;
};

// The following methods parse an input string representing a number value. The
// input string is expected to be in either an ASCII decimal format or an ASCII
// scientific notation format. The scale associated with each method is
// indicated in the comments describing the method.
//
// If 'strict_parsing' is true, an error is returned if more than 'scale'
// significant fractional digits exist. Else, the number is rounded to have
// 'scale' significant fractional digits at most.

// Parses a NUMERIC value. Scale is 9.
template <bool strict_parsing>
absl::Status ParseNumeric(absl::string_view str,
                          FixedPointRepresentation<2>& parsed);

// Parses a BIGNUMERIC value. Scale is 38.
template <bool strict_parsing>
absl::Status ParseBigNumeric(absl::string_view str,
                             FixedPointRepresentation<4>& parsed);

// Parses a number value in a JSON document. Scale is 1074. Uses strict_parsing.
absl::Status ParseJSONNumber(absl::string_view str,
                             FixedPointRepresentation<79>& parsed);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_NUMERIC_PARSER_H_
