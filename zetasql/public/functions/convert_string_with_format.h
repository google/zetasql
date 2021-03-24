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

// This file declares cast functions between strings and numeric types, with
// format strings.

#ifndef ZETASQL_PUBLIC_FUNCTIONS_CONVERT_STRING_WITH_FORMAT_H_
#define ZETASQL_PUBLIC_FUNCTIONS_CONVERT_STRING_WITH_FORMAT_H_

#include <string>

#include "zetasql/public/value.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

namespace internal {  // For internal use only

enum class FormatElement {
  CURRENCY_DOLLAR,  // $
  CURRENCY_C,       // C
  CURRENCY_L,       // L

  DIGIT_0,  // 0
  DIGIT_9,  // 9
  DIGIT_X,  // X

  DECIMAL_POINT_DOT,  // .
  DECIMAL_POINT_D,    // D

  GROUP_SEPARATOR_COMMA,  // ,
  GROUP_SEPARATOR_G,      // G

  SIGN_S,   // S
  SIGN_MI,  // MI
  SIGN_PR,  // PR

  ROMAN_NUMERAL,  // RN

  EXPONENT_EEEE,  // EEEE

  ELEMENT_B,  // B

  ELEMENT_V,  // V

  COMPACT_MODE,  // FM

  TM,   // TM
  TM9,  // TM9
  TME,  // TME
};

struct FormatElementWithCase {
  FormatElement element;

  // True if the element is in uppercase.
  bool upper_case;

  bool operator==(const FormatElementWithCase& rhs) const {
    return element == rhs.element &&
        upper_case == rhs.upper_case;
  }
};

// Represents the result of parsing a format string. It contains information
// that is used to validate the format string, and to generate a
// ValidatedFormatString.
struct ParsedFormatElementInfo {
  std::vector<FormatElementWithCase> elements;
};

// Parses the <format> string and returns the ParsedFormatElementInfo.
zetasql_base::StatusOr<ParsedFormatElementInfo> Parse(absl::string_view format);

// Returns the string representation of the format element. This is intended to
// be used to generate error messages.
std::string FormatElementToString(FormatElement element);

}  // namespace internal

// Validates the format string used in CAST() from a numerical type to string.
absl::Status ValidateNumericalToStringFormat(absl::string_view format);

// Generates the result of CAST() from a numerical value <v> to string using the
// format string <format>.
// Precondition:
// - v cannot be a NullValue,
// - the type of v has to be numerical.
zetasql_base::StatusOr<std::string> NumericalToStringWithFormat(
    const Value& v, absl::string_view format);

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_CONVERT_STRING_WITH_FORMAT_H_
