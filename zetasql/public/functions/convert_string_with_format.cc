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

#include "zetasql/public/functions/convert_string_with_format.h"

#include <vector>

#include "zetasql/public/functions/util.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/status_macros.h"

// Format a numeric type value into a string with a format sting.
// For the spec, see (broken link).

namespace zetasql {
namespace functions {
namespace internal {
namespace {

// Represents a validated format string. It contains the information needed to
// generate the output.
// TODO: it is just a stub for now. Implement it in following CLs.
class ValidatedFormatString {};

// Returns the set of format elements that are repeatable.
const absl::flat_hash_set<FormatElement>& GetRepeatableFormatElements() {
  static absl::flat_hash_set<FormatElement>* m = nullptr;
  if (m == nullptr) {
    absl::flat_hash_set<FormatElement>* t =
        new absl::flat_hash_set<FormatElement>();
    t->insert({FormatElement::DIGIT_0, FormatElement::DIGIT_9,
               FormatElement::DIGIT_X, FormatElement::GROUP_SEPARATOR_COMMA,
               FormatElement::GROUP_SEPARATOR_G});
    m = t;
  }

  return *m;
}

// Validates that a non-repeatable format element is not repeated.
absl::Status ValidateRepetition(
    const ParsedFormatElementInfo& parsed_format_element_info) {
  absl::flat_hash_set<FormatElement> appearedElements;
  for (const auto& element_with_case : parsed_format_element_info.elements) {
    bool repeatable =
        GetRepeatableFormatElements().contains(element_with_case.element);
    if (repeatable) {
      continue;
    }

    if (appearedElements.contains(element_with_case.element)) {
      absl::Status status;
      internal::UpdateError(
          &status,
          absl::Substitute("Format element '$0' cannot be repeated",
                           FormatElementToString(element_with_case.element)));
      return status;
    }
    appearedElements.insert(element_with_case.element);
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<ValidatedFormatString> Validate(
    const ParsedFormatElementInfo& parsed_format_element_info) {
  ZETASQL_RETURN_IF_ERROR(ValidateRepetition(parsed_format_element_info));

  // TODO: Add other validations

  return ValidatedFormatString();
}

}  // namespace

std::string FormatElementToString(FormatElement element) {
  static absl::flat_hash_map<FormatElement, std::string>* m = nullptr;
  if (m == nullptr) {
    absl::flat_hash_map<FormatElement, std::string>* map =
        new absl::flat_hash_map<FormatElement, std::string>();
    map->insert({
        {FormatElement::CURRENCY_DOLLAR, "$"},
        {FormatElement::DIGIT_0, "0"},
        {FormatElement::DIGIT_9, "9"},
        {FormatElement::DIGIT_X, "X"},
        {FormatElement::DECIMAL_POINT_DOT, "."},
        {FormatElement::GROUP_SEPARATOR_COMMA, ","},
        {FormatElement::SIGN_S, "S"},
        {FormatElement::SIGN_MI, "MI"},
        {FormatElement::SIGN_PR, "PR"},
        {FormatElement::ROMAN_NUMERAL, "RN"},
        {FormatElement::EXPONENT_EEEE, "EEEE"},
        {FormatElement::ELEMENT_B, "B"},
        {FormatElement::ELEMENT_V, "V"},
        {FormatElement::COMPACT_MODE, "FM"},
        {FormatElement::TM9, "TM9"},
        {FormatElement::TME, "TME"},
        {FormatElement::TM, "TM"},
        {FormatElement::CURRENCY_C, "C"},
        {FormatElement::CURRENCY_L, "L"},
        {FormatElement::DECIMAL_POINT_D, "D"},
        {FormatElement::GROUP_SEPARATOR_G, "G"},
    });
    m = map;
  }

  auto iter = m->find(element);
  if (iter == m->end()) {
    return "UNKNOWN";
  } else {
    return iter->second;
  }
}

// Gets the format element at the start of the input string <str>.
// Upon return, <length> contains the length of the format element in <str>, and
// <upper> indicates whether the format element is in uppercase or lowercase.
// E.g. if str is "9.9", returns FormatElement::DIGIT_9, and length is 1.
//
// If there is no valid format element, returns the empty optional.
absl::optional<FormatElement> GetFormatElement(absl::string_view str,
                                               int& length,
                                               bool& upper) {
  auto empty = absl::optional<FormatElement>();
  if (str.empty()) {
    return absl::optional<FormatElement>();
  }

  length = 1;
  upper = false;
  switch (str[0]) {
    case '$':
      return FormatElement::CURRENCY_DOLLAR;
    case '0':
      return FormatElement::DIGIT_0;
    case '9':
      return FormatElement::DIGIT_9;
    case 'X':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'x':
      return FormatElement::DIGIT_X;
    case '.':
      return FormatElement::DECIMAL_POINT_DOT;
    case 'D':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'd':
      return FormatElement::DECIMAL_POINT_D;
    case ',':
      return FormatElement::GROUP_SEPARATOR_COMMA;
    case 'G':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'g':
      return FormatElement::GROUP_SEPARATOR_G;
    case 'S':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 's':
      return FormatElement::SIGN_S;
    case 'M':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'm':
      if (absl::StartsWithIgnoreCase(str, "MI")) {
        length = 2;
        return FormatElement::SIGN_MI;
      } else {
        return empty;
      }
    case 'P':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'p':
      if (absl::StartsWithIgnoreCase(str, "PR")) {
        length = 2;
        return FormatElement::SIGN_PR;
      } else {
        return empty;
      }
    case 'R':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'r':
      if (absl::StartsWithIgnoreCase(str, "RN")) {
        length = 2;
        return FormatElement::ROMAN_NUMERAL;
      } else {
        return empty;
      }
    case 'E':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'e':
      if (absl::StartsWithIgnoreCase(str, "EEEE")) {
        length = 4;
        return FormatElement::EXPONENT_EEEE;
      } else {
        return empty;
      }
    case 'B':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'b':
      return FormatElement::ELEMENT_B;
    case 'V':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'v':
      return FormatElement::ELEMENT_V;
    case 'F':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'f':
      if (absl::StartsWithIgnoreCase(str, "FM")) {
        length = 2;
        return FormatElement::COMPACT_MODE;
      } else {
        return empty;
      }
    case 'T':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 't':
      if (absl::StartsWithIgnoreCase(str, "TM9")) {
        length = 3;
        return FormatElement::TM9;
      } else if (absl::StartsWithIgnoreCase(str, "TME")) {
        length = 3;
        return FormatElement::TME;
      } else if (absl::StartsWithIgnoreCase(str, "TM")) {
        length = 2;
        return FormatElement::TM;
      } else {
        return empty;
      }
    case 'C':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'c':
      return FormatElement::CURRENCY_C;
    case 'L':
      upper = true;
      ABSL_FALLTHROUGH_INTENDED;
    case 'l':
      return FormatElement::CURRENCY_L;
    default:
      return empty;
  }
}

zetasql_base::StatusOr<ParsedFormatElementInfo> Parse(absl::string_view format) {
  ParsedFormatElementInfo parsed_format_element_info;

  int index = 0;
  while (index < format.size()) {
    int length;
    bool upper;
    absl::optional<FormatElement> element =
        GetFormatElement(format.substr(index), length, upper);
    if (element.has_value()) {
      parsed_format_element_info.elements.push_back(
          {.element = element.value(),
           // The case is determined by the first character only, the cases of
           // the trailing characters (if there is any) are ignored.
           .upper_case = upper});
      index += length;
    } else {
      absl::Status status;
      internal::UpdateError(&status,
                            absl::Substitute("Invalid format element '$0'",
                                             format.substr(index, 1)));
      return status;
    }
  }

  return parsed_format_element_info;
}

}  // namespace internal

absl::Status ValidateNumericalToStringFormat(absl::string_view format) {
  internal::ParsedFormatElementInfo parsed_format_element_info;
  ZETASQL_ASSIGN_OR_RETURN(parsed_format_element_info, internal::Parse(format));
  return internal::Validate(parsed_format_element_info).status();
}

zetasql_base::StatusOr<std::string> NumericalToStringWithFormat(
    const Value& v, absl::string_view format) {
  ZETASQL_RET_CHECK(!v.is_null());

  internal::ParsedFormatElementInfo parsed_format_element_info;
  ZETASQL_ASSIGN_OR_RETURN(parsed_format_element_info, internal::Parse(format));
  auto retValue = internal::Validate(parsed_format_element_info);
  if (!retValue.ok()) {
    return retValue.status();
  }

  return MakeSqlError() << "NumericalToStringWithFormat is not implemented";
}

}  // namespace functions
}  // namespace zetasql
