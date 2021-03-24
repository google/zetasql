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
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
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

  // parse a format string and verify elements and cases.
  parsed_format_element_info = Parse("tM9x9XEeee.eEEE").value();
  std::vector<FormatElementWithCase> expected = {
      {FormatElement::TM9, false},
      {FormatElement::DIGIT_X, false},
      {FormatElement::DIGIT_9, false},
      {FormatElement::DIGIT_X, true},
      {FormatElement::EXPONENT_EEEE, true},
      {FormatElement::DECIMAL_POINT_DOT, false},
      {FormatElement::EXPONENT_EEEE, false},
  };

  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));

  // parse a format string that contains all supported format elements in upper
  // case.
  parsed_format_element_info =
      Parse("$CL09X.D,GSMIPRRNEEEEBVFMTMTM9TME").value();
  expected = {
      {FormatElement::CURRENCY_DOLLAR, false},
      {FormatElement::CURRENCY_C, true},
      {FormatElement::CURRENCY_L, true},
      {FormatElement::DIGIT_0, false},
      {FormatElement::DIGIT_9, false},
      {FormatElement::DIGIT_X, true},
      {FormatElement::DECIMAL_POINT_DOT, false},
      {FormatElement::DECIMAL_POINT_D, true},
      {FormatElement::GROUP_SEPARATOR_COMMA, false},
      {FormatElement::GROUP_SEPARATOR_G, true},
      {FormatElement::SIGN_S, true},
      {FormatElement::SIGN_MI, true},
      {FormatElement::SIGN_PR, true},
      {FormatElement::ROMAN_NUMERAL, true},
      {FormatElement::EXPONENT_EEEE, true},
      {FormatElement::ELEMENT_B, true},
      {FormatElement::ELEMENT_V, true},
      {FormatElement::COMPACT_MODE, true},
      {FormatElement::TM, true},
      {FormatElement::TM9, true},
      {FormatElement::TME, true},
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));

  // parse a format string that contains all supported format elements in lower
  // case.
  parsed_format_element_info =
      Parse("$cl09x.d,gsmiprrneeeebvfmtmtm9tme").value();
  expected = {
      {FormatElement::CURRENCY_DOLLAR, false},
      {FormatElement::CURRENCY_C, false},
      {FormatElement::CURRENCY_L, false},
      {FormatElement::DIGIT_0, false},
      {FormatElement::DIGIT_9, false},
      {FormatElement::DIGIT_X, false},
      {FormatElement::DECIMAL_POINT_DOT, false},
      {FormatElement::DECIMAL_POINT_D, false},
      {FormatElement::GROUP_SEPARATOR_COMMA, false},
      {FormatElement::GROUP_SEPARATOR_G, false},
      {FormatElement::SIGN_S, false},
      {FormatElement::SIGN_MI, false},
      {FormatElement::SIGN_PR, false},
      {FormatElement::ROMAN_NUMERAL, false},
      {FormatElement::EXPONENT_EEEE, false},
      {FormatElement::ELEMENT_B, false},
      {FormatElement::ELEMENT_V, false},
      {FormatElement::COMPACT_MODE, false},
      {FormatElement::TM, false},
      {FormatElement::TM9, false},
      {FormatElement::TME, false},
  };
  EXPECT_THAT(parsed_format_element_info.elements,
              testing::ContainerEq(expected));

  // parse a format string that is invalid
  EXPECT_FALSE(Parse("999a").ok());
}

TEST(Convert, FormatElementToString) {
  EXPECT_THAT(FormatElementToString(FormatElement::CURRENCY_DOLLAR), "$");
  EXPECT_THAT(FormatElementToString(FormatElement::CURRENCY_C), "C");
  EXPECT_THAT(FormatElementToString(FormatElement::CURRENCY_L), "L");
  EXPECT_THAT(FormatElementToString(FormatElement::DIGIT_0), "0");
  EXPECT_THAT(FormatElementToString(FormatElement::DIGIT_9), "9");
  EXPECT_THAT(FormatElementToString(FormatElement::DIGIT_X), "X");
  EXPECT_THAT(FormatElementToString(FormatElement::DECIMAL_POINT_DOT), ".");
  EXPECT_THAT(FormatElementToString(FormatElement::DECIMAL_POINT_D), "D");
  EXPECT_THAT(FormatElementToString(FormatElement::GROUP_SEPARATOR_COMMA), ",");
  EXPECT_THAT(FormatElementToString(FormatElement::GROUP_SEPARATOR_G), "G");
  EXPECT_THAT(FormatElementToString(FormatElement::SIGN_S), "S");
  EXPECT_THAT(FormatElementToString(FormatElement::SIGN_MI), "MI");
  EXPECT_THAT(FormatElementToString(FormatElement::SIGN_PR), "PR");
  EXPECT_THAT(FormatElementToString(FormatElement::ROMAN_NUMERAL), "RN");
  EXPECT_THAT(FormatElementToString(FormatElement::EXPONENT_EEEE), "EEEE");
  EXPECT_THAT(FormatElementToString(FormatElement::ELEMENT_B), "B");
  EXPECT_THAT(FormatElementToString(FormatElement::ELEMENT_V), "V");
  EXPECT_THAT(FormatElementToString(FormatElement::COMPACT_MODE), "FM");
  EXPECT_THAT(FormatElementToString(FormatElement::TM), "TM");
  EXPECT_THAT(FormatElementToString(FormatElement::TM9), "TM9");
  EXPECT_THAT(FormatElementToString(FormatElement::TME), "TME");
}

TEST(Convert, ParseFormatStringErrorCase_InvalidFormatElement) {
  // Invalid format element
  EXPECT_THAT(ValidateNumericalToStringFormat("99a"),
              StatusIs(_, ::testing::HasSubstr(
                              "Invalid format element 'a'")));
}

TEST(Convert, FormatStringValidation_Success) {
  ZETASQL_ASSERT_OK(ValidateNumericalToStringFormat("$99,9,9.99EEEEMI"));
}

TEST(Convert, FormatStringValidation_Nonrepeatable) {
  // The following format elements cannot be repeated:
  // dollar, dot, sign (S, MI, PR), exponent, TM, TM9, TME, B, RN, V, and FM
  EXPECT_THAT(ValidateNumericalToStringFormat("$$"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element '$' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("CC"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'C' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("LL"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'L' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat(".."),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element '.' cannot be repeated")));

  EXPECT_THAT(ValidateNumericalToStringFormat("dd"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'D' cannot be repeated")));

  EXPECT_THAT(ValidateNumericalToStringFormat("SS"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'S' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("MIMI"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'MI' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("PRPR"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'PR' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("EEEEEEEE"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'EEEE' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("tmtm"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'TM' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("tm9tm9"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'TM9' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("tmetme"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'TME' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("bb"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'B' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("rnrn"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'RN' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("vv"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'V' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("fmfm"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'FM' cannot be repeated")));
  EXPECT_THAT(ValidateNumericalToStringFormat("L$99L"),
              StatusIs(_, ::testing::HasSubstr(
                              "Format element 'L' cannot be repeated")));
}

}  // namespace internal

}  // namespace functions
}  // namespace zetasql
