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

#include <math.h>

#include <limits>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/format_max_output_width.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {
namespace internal {

using ::testing::_;
using zetasql_base::testing::StatusIs;

TEST(Convert, ParseFormatString) {
  ParsedFormatElementInfo parsed_format_element_info;

  // parse a format string of the text minimal type.
  parsed_format_element_info = ParseForTest("tM9").value();
  std::vector<FormatElement> expected = {};
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));
  EXPECT_THAT(parsed_format_element_info.output_type, OutputType::kTextMinimal);
  EXPECT_THAT(parsed_format_element_info.tm.value(), FormatElement::kTm9Lower);

  // parse a format string of the hexadecimal type.
  parsed_format_element_info = ParseForTest("0Xx").value();
  expected = {
      FormatElement::kDigit0,
      FormatElement::kDigitXUpper,
      FormatElement::kDigitXLower,
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));
  EXPECT_THAT(parsed_format_element_info.output_type, OutputType::kHexadecimal);

  // parse a format string of the roman numeral type.
  parsed_format_element_info = ParseForTest("RNFM").value();
  expected = {};
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));
  EXPECT_THAT(parsed_format_element_info.output_type,
              OutputType::kRomanNumeral);
  EXPECT_THAT(parsed_format_element_info.roman_numeral.value(),
              FormatElement::kRomanNumeralUpper);
  EXPECT_THAT(parsed_format_element_info.has_fm, true);

  // parse a format string of decimal type
  parsed_format_element_info = ParseForTest("9.00EEEE").value();
  expected = {
    FormatElement::kDigit9,
    FormatElement::kDecimalPointDot,
    FormatElement::kDigit0,
    FormatElement::kDigit0,
    FormatElement::kExponentEeeeUpper,
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));
  EXPECT_THAT(parsed_format_element_info.output_type,
              OutputType::kDecimal);

  // check num_integer_digit, scale and has_exponent
  parsed_format_element_info = ParseForTest("9.00EEEE").value();
  EXPECT_THAT(parsed_format_element_info.num_integer_digit, 1);
  EXPECT_THAT(parsed_format_element_info.scale, 2);
  EXPECT_THAT(parsed_format_element_info.has_exponent, true);

  parsed_format_element_info = ParseForTest("09.9").value();
  EXPECT_THAT(parsed_format_element_info.num_integer_digit, 2);
  EXPECT_THAT(parsed_format_element_info.scale, 1);
  EXPECT_THAT(parsed_format_element_info.has_exponent, false);

  // check has_fm, has_b
  parsed_format_element_info = ParseForTest("9.00EEEE").value();
  EXPECT_THAT(parsed_format_element_info.has_fm, false);
  EXPECT_THAT(parsed_format_element_info.has_b, false);
  parsed_format_element_info = ParseForTest("BFM9.00EEEE").value();
  EXPECT_THAT(parsed_format_element_info.has_fm, true);
  EXPECT_THAT(parsed_format_element_info.has_b, true);

  // check index_of_first_zero
  parsed_format_element_info = ParseForTest("9999").value();
  EXPECT_THAT(parsed_format_element_info.index_of_first_zero.has_value(),
              false);
  parsed_format_element_info = ParseForTest("9099").value();
  EXPECT_THAT(parsed_format_element_info.index_of_first_zero.value(), 1);
  parsed_format_element_info = ParseForTest("99099").value();
  EXPECT_THAT(parsed_format_element_info.index_of_first_zero.value(), 2);

  // check decimal_point and decimal_point_index
  parsed_format_element_info = ParseForTest(".99").value();
  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 0);
  EXPECT_THAT(parsed_format_element_info.decimal_point.value(),
              FormatElement::kDecimalPointDot);
  parsed_format_element_info = ParseForTest("9d9").value();
  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 1);
  EXPECT_THAT(parsed_format_element_info.decimal_point.value(),
              FormatElement::kDecimalPointD);
  parsed_format_element_info = ParseForTest("9EEEE").value();
  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 1);
  EXPECT_THAT(parsed_format_element_info.decimal_point.has_value(), false);
  parsed_format_element_info = ParseForTest("999").value();
  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 3);
  EXPECT_THAT(parsed_format_element_info.decimal_point.has_value(), false);

  // check that at most one integer digit is kept when 'eeee' exists.
  parsed_format_element_info = ParseForTest("99EEEE").value();
  expected = {
    FormatElement::kDigit9,
    FormatElement::kExponentEeeeUpper,
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));

  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 1);

  parsed_format_element_info = ParseForTest("999.99EEEE").value();
  expected = {
      FormatElement::kDigit9,
      FormatElement::kDecimalPointDot,
      FormatElement::kDigit9,
      FormatElement::kDigit9,
      FormatElement::kExponentEeeeUpper,
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));
  EXPECT_THAT(parsed_format_element_info.decimal_point_index, 1);

  // check currency
  parsed_format_element_info = ParseForTest("$9.99").value();
  EXPECT_THAT(parsed_format_element_info.currency.value(),
              FormatElement::kCurrencyDollar);
  parsed_format_element_info = ParseForTest("9.99").value();
  EXPECT_THAT(parsed_format_element_info.currency.has_value(), false);

  // check sign and sign_at_front.
  parsed_format_element_info = ParseForTest("9.99").value();
  EXPECT_THAT(parsed_format_element_info.sign.has_value(), false);
  parsed_format_element_info = ParseForTest("S9.99").value();
  EXPECT_THAT(parsed_format_element_info.sign.value(), FormatElement::kSignS);
  EXPECT_THAT(parsed_format_element_info.sign_at_front, true);
  parsed_format_element_info = ParseForTest("9.99S").value();
  EXPECT_THAT(parsed_format_element_info.sign.value(), FormatElement::kSignS);
  EXPECT_THAT(parsed_format_element_info.sign_at_front, false);
  parsed_format_element_info = ParseForTest("9.99MI").value();
  EXPECT_THAT(parsed_format_element_info.sign.value(), FormatElement::kSignMi);
  parsed_format_element_info = ParseForTest("9.99PR").value();
  EXPECT_THAT(parsed_format_element_info.sign.value(), FormatElement::kSignPr);

  // parse a format string that is invalid
  EXPECT_FALSE(ParseForTest("999a").ok());
}

TEST(Convert, ParsedNumberString) {
  internal::ParsedNumberString n =
      internal::ParseFormattedRealNumber("-1.230").value();
  EXPECT_THAT(n.integer_part, "1");
  EXPECT_THAT(n.fractional_part, "230");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, true);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("2.34e+10").value();
  EXPECT_THAT(n.integer_part, "2");
  EXPECT_THAT(n.fractional_part, "34");
  EXPECT_THAT(n.exponent, "+10");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("2.e-23").value();
  EXPECT_THAT(n.integer_part, "2");
  EXPECT_THAT(n.fractional_part, "");
  EXPECT_THAT(n.exponent, "-23");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, false);

  // check that integer_part is empty if the integer part is 0
  n = internal::ParseFormattedRealNumber("0.1").value();
  EXPECT_THAT(n.integer_part, "");
  EXPECT_THAT(n.fractional_part, "1");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("1.").value();
  EXPECT_THAT(n.integer_part, "1");
  EXPECT_THAT(n.fractional_part, "");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("inf").value();
  EXPECT_THAT(n.integer_part, "");
  EXPECT_THAT(n.fractional_part, "");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, true);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("-inf").value();
  EXPECT_THAT(n.integer_part, "");
  EXPECT_THAT(n.fractional_part, "");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, true);
  EXPECT_THAT(n.is_infinity, true);
  EXPECT_THAT(n.is_nan, false);

  n = internal::ParseFormattedRealNumber("nan").value();
  EXPECT_THAT(n.integer_part, "");
  EXPECT_THAT(n.fractional_part, "");
  EXPECT_THAT(n.exponent, "");
  EXPECT_THAT(n.negative, false);
  EXPECT_THAT(n.is_infinity, false);
  EXPECT_THAT(n.is_nan, true);

  // Invalid inputs
  EXPECT_THAT(internal::ParseFormattedRealNumber("-nan").ok(), false);
  EXPECT_THAT(internal::ParseFormattedRealNumber("123").ok(), false);
  EXPECT_THAT(internal::ParseFormattedRealNumber("1.2e").ok(), false);
  EXPECT_THAT(internal::ParseFormattedRealNumber(".2").ok(), false);
}

TEST(Convert, FormatElementToString) {
  EXPECT_THAT(FormatElementToString(FormatElement::kCurrencyDollar), "$");
  EXPECT_THAT(FormatElementToString(FormatElement::kCurrencyCLower), "C");
  EXPECT_THAT(FormatElementToString(FormatElement::kCurrencyCUpper), "C");
  EXPECT_THAT(FormatElementToString(FormatElement::kCurrencyL), "L");
  EXPECT_THAT(FormatElementToString(FormatElement::kDigit0), "0");
  EXPECT_THAT(FormatElementToString(FormatElement::kDigit9), "9");
  EXPECT_THAT(FormatElementToString(FormatElement::kDigitXLower), "X");
  EXPECT_THAT(FormatElementToString(FormatElement::kDigitXUpper), "X");
  EXPECT_THAT(FormatElementToString(FormatElement::kDecimalPointDot), ".");
  EXPECT_THAT(FormatElementToString(FormatElement::kDecimalPointD), "D");
  EXPECT_THAT(FormatElementToString(FormatElement::kGroupSeparatorComma), ",");
  EXPECT_THAT(FormatElementToString(FormatElement::kGroupSeparatorG), "G");
  EXPECT_THAT(FormatElementToString(FormatElement::kSignS), "S");
  EXPECT_THAT(FormatElementToString(FormatElement::kSignMi), "MI");
  EXPECT_THAT(FormatElementToString(FormatElement::kSignPr), "PR");
  EXPECT_THAT(FormatElementToString(FormatElement::kRomanNumeralLower), "RN");
  EXPECT_THAT(FormatElementToString(FormatElement::kRomanNumeralUpper), "RN");
  EXPECT_THAT(FormatElementToString(FormatElement::kExponentEeeeLower),
              "EEEE");
  EXPECT_THAT(FormatElementToString(FormatElement::kExponentEeeeUpper),
              "EEEE");
  EXPECT_THAT(FormatElementToString(FormatElement::kElementB), "B");
  EXPECT_THAT(FormatElementToString(FormatElement::kElementV), "V");
  EXPECT_THAT(FormatElementToString(FormatElement::kCompactMode), "FM");
  EXPECT_THAT(FormatElementToString(FormatElement::kTmLower), "TM");
  EXPECT_THAT(FormatElementToString(FormatElement::kTmUpper), "TM");
  EXPECT_THAT(FormatElementToString(FormatElement::kTm9Lower), "TM9");
  EXPECT_THAT(FormatElementToString(FormatElement::kTm9Upper), "TM9");
  EXPECT_THAT(FormatElementToString(FormatElement::kTmeLower), "TME");
  EXPECT_THAT(FormatElementToString(FormatElement::kTmeUpper), "TME");
}

TEST(Convert, ParseFormatStringErrorCase_GeneralError) {
  // Invalid format element
  EXPECT_THAT(ValidateNumericalToStringFormat("99a"),
              StatusIs(_, ::testing::HasSubstr(
                              "Invalid format element 'a'")));

  // Format string too long
  auto original_value = absl::GetFlag(FLAGS_zetasql_format_max_output_width);
  absl::SetFlag(&FLAGS_zetasql_format_max_output_width, 4);
  EXPECT_THAT(
      ValidateNumericalToStringFormat("99999"),
      StatusIs(_, ::testing::HasSubstr("Format string too long;")));
  absl::SetFlag(&FLAGS_zetasql_format_max_output_width, original_value);
}

// A helper function that validates the format string in both the lower and
// upper case.
template <typename StatusMatcher>
void ValidateFormatString(absl::string_view format_string,
                          StatusMatcher status_matcher) {
  std::string lower_format_string = absl::AsciiStrToLower(format_string);
  EXPECT_THAT(ValidateNumericalToStringFormat(lower_format_string),
              status_matcher)
      << "Format String: " << lower_format_string;

  std::string upper_format_string = absl::AsciiStrToUpper(format_string);
  EXPECT_THAT(ValidateNumericalToStringFormat(upper_format_string),
              status_matcher)
      << "Format String: " << upper_format_string;
}

TEST(Convert, FormatStringValidation_Success) {
  ValidateFormatString("B$99,9,9.99MI", absl::OkStatus());
}

TEST(Convert, FormatStringValidation_TM) {
  // TM* cannot be used with other elements
  ValidateFormatString(
      "9tme",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "fmtme",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(".tme",
                       StatusIs(_, ::testing::HasSubstr("Unexpected 'TME'")));

  // There cannot be multiple TM* elements
  ValidateFormatString(
      "tmtm9",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "tm9tme",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "tmtm",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "tm9tm9",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "tmetme",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));

  // Success cases
  ValidateFormatString("tm", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("tm9", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("tme", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_RN) {
  // When RN exists, then only FM can appear
  ValidateFormatString(
      "RN9", StatusIs(_, ::testing::HasSubstr(
                             "'RN' cannot appear together with '9'")));

  // Success cases
  ValidateFormatString("RNFM", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("FMRN", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_AtLeastOneDigit) {
  // At least one digit must exists
  ValidateFormatString(
      "seeee",
      StatusIs(
          _,
          ::testing::HasSubstr(
              "Format string must contain at least one of 'X', '0' or '9'")));
  ValidateFormatString(
      ".",
      StatusIs(
          _,
          ::testing::HasSubstr(
              "Format string must contain at least one of 'X', '0' or '9'")));
  ValidateFormatString(
      ".EEEE",
      StatusIs(
          _,
          ::testing::HasSubstr(
              "Format string must contain at least one of 'X', '0' or '9'")));

  // Success cases
  ValidateFormatString("9", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("0", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("X", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_X) {
  // When X appears, then the other format elements allowed are 0,  FM, and
  // sign format elements.
  ValidateFormatString(
      "X,",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with ','")));
  ValidateFormatString(
      "XEEEE", StatusIs(_, ::testing::HasSubstr(
                               "'X' cannot appear together with 'EEEE'")));
  ValidateFormatString(
      "99X",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with '9'")));
  ValidateFormatString(
      "X99",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with '9'")));
  ValidateFormatString(
      "X.XX",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with '.'")));
  ValidateFormatString(
      ".XX",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with '.'")));
  ValidateFormatString(
      "0,0X", StatusIs(_, ::testing::HasSubstr(
                              "'X' cannot appear together with ',' or 'G'")));
  ValidateFormatString(
      "0G0X", StatusIs(_, ::testing::HasSubstr(
                              "'X' cannot appear together with ',' or 'G'")));

  // max number of hex digits is 16
  ValidateFormatString(
      "XXXXXXXXXXXX0XXXX",
      StatusIs(_, ::testing::HasSubstr("Max number of 'X' is 16")));
  ValidateFormatString(
      "0000000000000000X",
      StatusIs(_, ::testing::HasSubstr("Max number of 'X' is 16")));

  // Success cases
  ValidateFormatString("FM0X0", StatusIs(absl::StatusCode::kOk));

  ValidateFormatString("000000000000000X", StatusIs(absl::StatusCode::kOk));

  ValidateFormatString("FM0XS",
              StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("FM0XXXXXXXXXXXXXXXS",
              StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_DecimalPoint) {
  // There cannot be multiple decimal point elements
  ValidateFormatString(
      "9.9.9",
      StatusIs(_, ::testing::HasSubstr(
                      "There can be at most one of '.', 'D', or 'V'")));
  ValidateFormatString(
      "9.9V9",
      StatusIs(_, ::testing::HasSubstr(
                      "There can be at most one of '.', 'D', or 'V'")));
  ValidateFormatString(
      "9.9D9",
      StatusIs(_, ::testing::HasSubstr(
                      "There can be at most one of '.', 'D', or 'V'")));

  // Success cases
  ValidateFormatString(".99", absl::OkStatus());
  ValidateFormatString("D99", absl::OkStatus());
  ValidateFormatString("V99", absl::OkStatus());
  ValidateFormatString("9.99", absl::OkStatus());
  ValidateFormatString("9D99", absl::OkStatus());
  ValidateFormatString("FM9V99", absl::OkStatus());
}

TEST(Convert, FormatStringValidation_GroupSeparator) {
  ValidateFormatString(",999",
                         StatusIs(_, ::testing::HasSubstr("Unexpected ','")));

  ValidateFormatString("G999",
                         StatusIs(_, ::testing::HasSubstr("Unexpected 'G'")));
  ValidateFormatString(
      "9,99EEEE",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear together with 'EEEE'")));
  ValidateFormatString(
      "9G99EEEE",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear together with 'EEEE'")));
  ValidateFormatString(
      "9,9.9EEEE",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear together with 'EEEE'")));
  ValidateFormatString(
      "999EEEE,",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear together with 'EEEE'")));
  ValidateFormatString(
      "9,9.9,9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "9,9.9G9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "9,9d9,9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "9,9d9G9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "99V9,9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "99V9g9",
      StatusIs(_, ::testing::HasSubstr(
                      "',' or 'G' cannot appear after '.', 'D' or 'V'")));
  ValidateFormatString(
      "99S,",
      StatusIs(_, ::testing::HasSubstr("Unexpected format element ','")));

  // Success cases
  ValidateFormatString("9,,9,9", absl::OkStatus());
}

TEST(Convert, FormatStringValidation_EEEE) {
  ValidateFormatString(
      "EEEE",
      StatusIs(_, ::testing::HasSubstr("Unexpected 'EEEE'")));
  ValidateFormatString(
      "9EEEEEEEE",
      StatusIs(_, ::testing::HasSubstr("'EEEE' cannot appear after 'EEEE'")));
  ValidateFormatString("9.9EEEE99",
              StatusIs(_, ::testing::HasSubstr(
                              "'9' cannot appear after 'EEEE'")));
  ValidateFormatString("9.9EEEE0",
              StatusIs(_, ::testing::HasSubstr(
                              "'0' cannot appear after 'EEEE'")));

  // Success cases
  ValidateFormatString("9.9EEEE",
              StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_Sign) {
  // There cannot be multiple sign elements
  ValidateFormatString("s99mi",
              StatusIs(_, ::testing::HasSubstr(
                  "There can be at most one of 'S', 'MI', or 'PR'")));
  ValidateFormatString("s99pr",
              StatusIs(_, ::testing::HasSubstr(
                  "There can be at most one of 'S', 'MI', or 'PR'")));
  ValidateFormatString("s9.9s",
              StatusIs(_, ::testing::HasSubstr(
                  "There can be at most one of 'S', 'MI', or 'PR'")));
  ValidateFormatString("s9.9eeeemi",
              StatusIs(_, ::testing::HasSubstr(
                  "There can be at most one of 'S', 'MI', or 'PR'")));
  ValidateFormatString("S00XMI",
              StatusIs(_, ::testing::HasSubstr(
                  "There can be at most one of 'S', 'MI', or 'PR'")));

  // S must appear before or after all digits and exponent.
  ValidateFormatString(
      "9S9",
      StatusIs(
          _, ::testing::HasSubstr(
                 "'S' can only appear before or after all digits and 'EEEE'")));
  ValidateFormatString(
      "XSX",
      StatusIs(
          _, ::testing::HasSubstr(
                 "'S' can only appear before or after all digits and 'EEEE'")));
  ValidateFormatString(
      "99SEEEE",
      StatusIs(
          _, ::testing::HasSubstr(
                 "'S' can only appear before or after all digits and 'EEEE'")));

  // MI must appear after all digits and exponent.
  ValidateFormatString(
      "MI99",
      StatusIs(_, ::testing::HasSubstr(
                      "'MI' can only appear after all digits and 'EEEE'")));
  ValidateFormatString(
      "9MI9",
      StatusIs(_, ::testing::HasSubstr(
                      "'MI' can only appear after all digits and 'EEEE'")));
  ValidateFormatString(
      "99MIEEEE",
      StatusIs(_, ::testing::HasSubstr(
                      "'MI' can only appear after all digits and 'EEEE'")));

  // PR must appear after all digits and exponent.
  ValidateFormatString(
      "PR99",
      StatusIs(_, ::testing::HasSubstr(
                      "'PR' can only appear after all digits and 'EEEE'")));
  ValidateFormatString(
      "9PR9",
      StatusIs(_, ::testing::HasSubstr(
                      "'PR' can only appear after all digits and 'EEEE'")));
  ValidateFormatString(
      "99PREEEE",
      StatusIs(_, ::testing::HasSubstr(
                      "'PR' can only appear after all digits and 'EEEE'")));

  // Success cases
  ValidateFormatString("S9,9", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99S", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99EEEES", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99.99EEEES", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("S99.99EEEE", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("XXXXS", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("SXXXX", StatusIs(absl::StatusCode::kOk));

  ValidateFormatString("99MI", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99EEEEMI", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99.99EEEEMI", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("XXXXMI", StatusIs(absl::StatusCode::kOk));

  ValidateFormatString("99PR", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99EEEEPR", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99.99EEEEPR", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("XXXXPR", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_Currency) {
  // There cannot be multiple currency elements
  ValidateFormatString(
      "C$9.99",
      StatusIs(_, ::testing::HasSubstr(
                      "There can be at most one of '$', 'C' or 'L'")));
  ValidateFormatString(
      "L$9.99",
      StatusIs(_, ::testing::HasSubstr(
                      "There can be at most one of '$', 'C' or 'L'")));

  ValidateFormatString(
      "$XXX",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with '$'")));
  ValidateFormatString(
      "$TM",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "$RN", StatusIs(_, ::testing::HasSubstr(
                             "'RN' cannot appear together with '$'")));

  // Success cases
  ValidateFormatString("$99.99EEEE", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("99$.99EEEE", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("9.9SL", StatusIs(absl::StatusCode::kOk));
  ValidateFormatString("0C000EEEE", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, FormatStringValidation_B) {
  // There cannot be multiple B
  ValidateFormatString(
      "B9.99B",
      StatusIs(_, ::testing::HasSubstr("There can be at most one 'B'")));
  ValidateFormatString(
      "XBXX",
      StatusIs(_, ::testing::HasSubstr("'X' cannot appear together with 'B'")));
  ValidateFormatString(
      "BTM",
      StatusIs(_, ::testing::HasSubstr("'TM', 'TM9' or 'TME' cannot be "
                                       "combined with other format elements")));
  ValidateFormatString(
      "RNB", StatusIs(_, ::testing::HasSubstr(
                             "'RN' cannot appear together with 'B'")));

  // Success cases
  ValidateFormatString("B$99.99EEEE", StatusIs(absl::StatusCode::kOk));
}

TEST(Convert, TextMininmal) {
  EXPECT_THAT(NumericalToStringWithFormat(Value::Double(1.23), "tm",
                                          ProductMode::PRODUCT_INTERNAL),
              StatusIs(_, ::testing::HasSubstr(
                              "Text minimal output is not supported yet")));
}

void ValidateOutput(const Value& value, absl::string_view format_string,
                    const std::string& expected) {
  EXPECT_THAT(NumericalToStringWithFormat(value, format_string,
                                          ProductMode::PRODUCT_INTERNAL)
                  .value(),
              expected)
      << "Input: " << value.DebugString(/*verbose=*/true)
      << ", Format String: [" << format_string << "]"
      << ", Expected: [" << expected << "]";
}

void ValidateOutputInAllLetterCases(const Value& value,
                                    absl::string_view format_string,
                                    const std::string& expected) {
  ValidateOutput(value, format_string, expected);
  ValidateOutput(value, absl::AsciiStrToLower(format_string),
                 absl::AsciiStrToLower(expected));
  ValidateOutput(value, absl::AsciiStrToUpper(format_string),
                 absl::AsciiStrToUpper(expected));
}

void ValidateOutput(double v, absl::string_view format_string,
                    const std::string& expected) {
  ValidateOutputInAllLetterCases(Value::Float(v), format_string, expected);
  ValidateOutputInAllLetterCases(Value::Double(v), format_string, expected);
  ValidateOutputInAllLetterCases(
      Value::Numeric(NumericValue::FromDouble(v).value()), format_string,
      expected);
  ValidateOutputInAllLetterCases(
      Value::BigNumeric(BigNumericValue::FromDouble(v).value()), format_string,
      expected);
}

void ValidateOutput(int v, absl::string_view format_string,
                    const std::string& expected) {
  ValidateOutputInAllLetterCases(Value::Int32(v), format_string, expected);
  ValidateOutputInAllLetterCases(Value::Int64(v), format_string, expected);
  if (v >= 0) {
    ValidateOutputInAllLetterCases(Value::Uint32(v), format_string, expected);
    ValidateOutputInAllLetterCases(Value::Uint64(v), format_string, expected);
  }
  ValidateOutputInAllLetterCases(Value::Float(v), format_string, expected);
  ValidateOutputInAllLetterCases(Value::Double(v), format_string, expected);
  ValidateOutputInAllLetterCases(
      Value::Numeric(NumericValue::FromDouble(v).value()), format_string,
      expected);
  ValidateOutputInAllLetterCases(
      Value::BigNumeric(BigNumericValue::FromDouble(v).value()), format_string,
      expected);
}

TEST(Convert, FormatAsDecimal_GroupSeparators) {
  ValidateOutput(12.3, "99.99", " 12.30");
  ValidateOutput(1.2, "999,999.999", "       1.200");
  ValidateOutput(12.3, "999,999.999", "      12.300");
  ValidateOutput(123.456, "999,999.999", "     123.456");
  ValidateOutput(1234.56, "999,999.999", "   1,234.560");
  ValidateOutput(-12345.678, "999,999.999", " -12,345.678");
  ValidateOutput(1234567.89, "999,999.999", " ###,###.###");
  ValidateOutput(-1234567.89, "999,999.999", "-###,###.###");
  ValidateOutput(1234.56, "9,9g9,9g.99", " 1,2,3,4,.56");
  ValidateOutput(12345.6, "9,9g9,9g.99", " #,#,#,#,.##");
  ValidateOutput(123.456, "999,999.999", "     123.456");
  ValidateOutput(1234.56, "999,999.999", "   1,234.560");
  ValidateOutput(1234.56, "9,9,9,9,9,9.999", "     1,2,3,4.560");
}

TEST(Convert, FormatAsDecimal_Rounding) {
  ValidateOutput(1.29, "9.9", " 1.3");
  ValidateOutput(1.99, "9.9", " 2.0");
  ValidateOutput(9.99, "9.9", " #.#");
  ValidateOutput(0.399, ".9", " .4");
  ValidateOutput(0.399, ".99", " .40");
  ValidateOutput(0.999, ".9", " .#");
  ValidateOutput(0.999, ".99", " .##");

  ValidateOutput(8.99, "9", " 9");
  ValidateOutput(9.99, "9", " #");

  ValidateOutput(9.99, "9eeee", " 1e+01");
  ValidateOutput(9.09, "9eeee", " 9e+00");
}

TEST(Convert, FormatAsDecimal_Exponent) {
  ValidateOutput(123, "9.99EEEE", " 1.23E+02");
  ValidateOutput(123, "9d99EEEE", " 1.23E+02");
  ValidateOutput(0.0123, "9d99EEEE", " 1.23E-02");

  // There is at most one integer digit that is kept if 'EEEE' is specified.
  // Thus, the following cases string generates the same output as the previous
  // test case even though they have more integer digit format elements.
  ValidateOutput(123, "99.99EEEE", " 1.23E+02");
  ValidateOutput(123, "999.99eeee", " 1.23e+02");
  ValidateOutput(123, "0000.99EEEE", " 1.23E+02");
  ValidateOutput(123, "090999.99EEEE", " 1.23E+02");
  ValidateOutput(123.456, "9.9EEEE", " 1.2E+02");

  ValidateOutputInAllLetterCases(Value::Double(1e123), "9.9EEEE", " 1.0E+123");
  ValidateOutputInAllLetterCases(Value::Double(1e-123), "9.9EEEE", " 1.0E-123");

  // zero is displayed before decimal point when eeee is present
  ValidateOutput(0, "0.00EEEE", " 0.00E+00");

  // Test cases when there are no integer digits.
  ValidateOutput(123, ".99EEEE", " .######");
  ValidateOutput(0.12, ".99EEEE", " .######");
  ValidateOutput(0, ".99EEEE", " .00E+00");
  ValidateOutput(0.0, ".99EEEE", " .00E+00");

  // Test cases when there are no decimal point in the format string.
  ValidateOutput(123, "9EEEE", " 1E+02");
  ValidateOutput(199, "9EEEE", " 2E+02");
  ValidateOutput(123, "9999EEEE", " 1E+02");
  ValidateOutput(199, "9999EEEE", " 2E+02");
}

TEST(Convert, FormatAsDecimal_Zero) {
  // Trailing and leading zeros
  ValidateOutput(1.2, "999.999", "   1.200");
  ValidateOutput(1.2, "999.000", "   1.200");

  ValidateOutput(1.2, "000.000", " 001.200");
  ValidateOutput(1.2, "090.000", " 001.200");
  ValidateOutput(1.2, "099.000", " 001.200");
  ValidateOutput(1.2, "990.000", "   1.200");
  ValidateOutput(0.12, "999.999", "    .120");
  ValidateOutput(0.12, "999.000", "    .120");
  ValidateOutput(0.12, "000.000", " 000.120");

  ValidateOutput(1, "909", "  01");
  ValidateOutput(1, "999", "   1");
  ValidateOutput(1, "90909", "  0001");

  // Whether 0 is displayed in the integer part.
  ValidateOutput(-0.1, "99.99", "  -.10");
  ValidateOutput(-0.1, "90.99", " -0.10");
  ValidateOutput(-0.1, "00.99", "-00.10");
  ValidateOutput(0, "999.999", "    .000");

  ValidateOutput(0, "999", "   0");
  ValidateOutput(0, "000.999", " 000.000");
  ValidateOutput(0, "99.", "  0.");
}

TEST(Convert, FormatAsDecimal_V) {
  // Test format element 'V'.
  ValidateOutput(12, "99V99", " 1200");
  ValidateOutput(12, "9V99", " ###");
  ValidateOutput(12.3, "99V99", " 1230");
  ValidateOutput(0.1, "V99", " 10");
  ValidateOutput(0.1, "9V99", "  10");
  ValidateOutput(0.1, "0V99", " 010");

  /* TODO: enable when 'B' is implemented.
  // B & V
  ValidateOutput(0, "B99V99", "     ");
  ValidateOutput(0.1, "B9V99", "    "); */
}

TEST(Convert, FormatAsDecimal_EdgeCases) {
  // Int32
  ValidateOutput(Value::Int32(2'147'483'647), "9,999,999,999",
                 " 2,147,483,647");
  ValidateOutput(Value::Int32(2'147'483'647), "9.999999999eeee",
                 " 2.147483647e+09");
  ValidateOutput(Value::Int32(-2'147'483'648), "9,999,999,999",
                 "-2,147,483,648");
  ValidateOutput(Value::Int32(-2'147'483'648), "9.999999999eeee",
                 "-2.147483648e+09");

  // Int64
  ValidateOutput(Value::Int64(9'223'372'036'854'775'807),
                 "9,999,999,999,999,999,999", " 9,223,372,036,854,775,807");
  ValidateOutput(Value::Int64(9'223'372'036'854'775'807),
                 "9.999999999999999999eeee", " 9.223372036854775807e+18");
  ValidateOutput(Value::Int64(-9'223'372'036'854'775'807 - 1),
                 "9,999,999,999,999,999,999", "-9,223,372,036,854,775,808");
  ValidateOutput(Value::Int64(-9'223'372'036'854'775'807 - 1),
                 "9.999999999999999999eeee", "-9.223372036854775808e+18");

  // UInt32
  ValidateOutput(Value::Uint32(4'294'967'295u), "9,999,999,999",
                 " 4,294,967,295");
  ValidateOutput(Value::Uint32(4'294'967'295u), "9.999999999eeee",
                 " 4.294967295e+09");

  // UInt64
  ValidateOutput(Value::Uint64(18'446'744'073'709'551'615ul),
                 "99,999,999,999,999,999,999", " 18,446,744,073,709,551,615");
  ValidateOutput(Value::Uint64(18'446'744'073'709'551'615ul),
                 "9.9999999999999999999eeee", " 1.8446744073709551615e+19");

  // Numeric
  ValidateOutput(
      Value::Numeric(NumericValue::FromStringStrict(
                         "9.9999999999999999999999999999999999999E+28")
                         .value()),
      "00,000,000,000,000,000,000,000,000,000.0000000000",
      " 99,999,999,999,999,999,999,999,999,999.9999999990");
  ValidateOutput(
      Value::Numeric(NumericValue::FromStringStrict(
                         "9.9999999999999999999999999999999999999E+28")
                         .value()),
      "0.0000000000000000000000000000000000000eeee",
      " 9.9999999999999999999999999999999999999e+28");
  ValidateOutput(
      Value::Numeric(NumericValue::FromStringStrict(
                         "-9.9999999999999999999999999999999999999E+28")
                         .value()),
      "00,000,000,000,000,000,000,000,000,000.0000000000",
      "-99,999,999,999,999,999,999,999,999,999.9999999990");
  ValidateOutput(
      Value::Numeric(NumericValue::FromStringStrict(
                         "-9.9999999999999999999999999999999999999E+28")
                         .value()),
      "0.0000000000000000000000000000000000000eeee",
      "-9.9999999999999999999999999999999999999e+28");

  // Bignumeric
  ValidateOutput(
      Value::BigNumeric(BigNumericValue::FromStringStrict(
                            "5."
                            "78960446186580977117854925043439539266349923328202"
                            "82019728792003956564819967E+38")
                            .value()),
      "000000000000000000000000000000000000000."
      "00000000000000000000000000000000000000",
      " 578960446186580977117854925043439539266."
      "34992332820282019728792003956564819967");
  ValidateOutput(
      Value::BigNumeric(BigNumericValue::FromStringStrict(
                            "5."
                            "78960446186580977117854925043439539266349923328202"
                            "82019728792003956564819967E+38")
                            .value()),
      "0.00000000000000000000000000000000000000"
      "00000000000000000000000000000000000000eeee",
      " 5.78960446186580977117854925043439539266"
      "34992332820282019728792003956564819967e+38");
  ValidateOutput(
      Value::BigNumeric(BigNumericValue::FromStringStrict(
                            "-5."
                            "78960446186580977117854925043439539266349923328202"
                            "82019728792003956564819968E+38")
                            .value()),
      "000000000000000000000000000000000000000."
      "00000000000000000000000000000000000000",
      "-578960446186580977117854925043439539266."
      "34992332820282019728792003956564819968");
  ValidateOutput(
      Value::BigNumeric(BigNumericValue::FromStringStrict(
                            "-5."
                            "78960446186580977117854925043439539266349923328202"
                            "82019728792003956564819968E+38")
                            .value()),
      "0.00000000000000000000000000000000000000"
      "00000000000000000000000000000000000000eeee",
      "-5.78960446186580977117854925043439539266"
      "34992332820282019728792003956564819968e+38");


  // Double
  ValidateOutput(
      Value::Double(1.23456789012345e308), std::string(309, '9'),
      " "
      "123456789012344995627053944997982803791928763973258471284643267727010607"
      "703707669343934312473046000772743672977879463116136186816332348863577383"
      "105271250249631984698798713111342690598734085352313476901534216047495231"
      "312247504708188087672624266222279802787256147681110764246941270927161950"
      "653242549267320537088");
  ValidateOutput(Value::Double(1.23456789012345e308),
                 "9.999999999999999999eeee", " 1.234567890123449956e+308");
  ValidateOutput(
      Value::Double(1.23456789012345e-308),
      absl::StrCat("9.", std::string(337, '9')),
      absl::StrCat("  .", std::string(307, '0'),
                   "123456789012344975373385356245"));
  ValidateOutput(Value::Double(1.23456789012345e-308),
                 "9.999999999999999999eeee", " 1.234567890123449754e-308");
}

TEST(Convert, FormatAsDecimal_SignAndCurrency) {
  // Extra position is reserved for sign if there is no sign format element in
  // the format string.
  ValidateOutput(123, "999", " 123");
  ValidateOutput(-123, "999", "-123");
  ValidateOutput(123.4, "999.99", " 123.40");
  ValidateOutput(-123.4, "999.99", "-123.40");
  ValidateOutput(123.4, "9.9999eeee", " 1.2340e+02");
  ValidateOutput(-123.4, "9.9999eeee", "-1.2340e+02");

  // Explicit sign
  ValidateOutput(12, "S999", " +12");
  ValidateOutput(12, "999S", " 12+");
  ValidateOutput(12, "999MI", " 12 ");
  ValidateOutput(12, "999PR", "  12 ");

  ValidateOutput(-7, "S999", "  -7");
  ValidateOutput(-7, "999S", "  7-");
  ValidateOutput(-7, "999MI", "  7-");
  ValidateOutput(-7, "999PR", "  <7>");

  ValidateOutput(123.4, "S999.99", "+123.40");
  ValidateOutput(123.4, "999.99S", "123.40+");
  ValidateOutput(123.4, "999.99MI", "123.40 ");
  ValidateOutput(123.4, "999.99PR", " 123.40 ");

  ValidateOutput(-123.4, "S999.99", "-123.40");
  ValidateOutput(-123.4, "999.99S", "123.40-");
  ValidateOutput(-123.4, "999.99MI", "123.40-");
  ValidateOutput(-123.4, "999.99PR", "<123.40>");

  ValidateOutput(123.4, "S9.9999eeee", "+1.2340e+02");
  ValidateOutput(123.4, "9.9999eeeeS", "1.2340e+02+");
  ValidateOutput(123.4, "9.9999eeeeMI", "1.2340e+02 ");
  ValidateOutput(123.4, "9.9999eeeePR", " 1.2340e+02 ");

  ValidateOutput(-123.4, "S9.9999eeee", "-1.2340e+02");
  ValidateOutput(-123.4, "9.9999eeeeS", "1.2340e+02-");
  ValidateOutput(-123.4, "9.9999eeeeMI", "1.2340e+02-");
  ValidateOutput(-123.4, "9.9999eeeePR", "<1.2340e+02>");

  // Sign is located before currency.
  ValidateOutput(-12, "$9999", "  -$12");
  ValidateOutput(12, "S$9999", "  +$12");
  ValidateOutput(-12, "L9999", "  -$12");
  ValidateOutput(-12, "C9999", "  -USD12");
  ValidateOutput(-12, "C9999PR", "  <USD12>");

  ValidateOutput(-123.4, "$9.9999eeee", "-$1.2340e+02");
  ValidateOutput(-123.4, "$9.9999eeeeMI", "$1.2340e+02-");
  ValidateOutput(-123.4, "C9.9999eeeePR", "<USD1.2340e+02>");

  // lower case c
  ValidateOutput(-12, "c9999", "  -usd12");
}

}  // namespace internal

}  // namespace functions
}  // namespace zetasql
