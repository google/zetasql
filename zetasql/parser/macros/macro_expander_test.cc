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

#include "zetasql/parser/macros/macro_expander.h"

#include <cctype>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/flex_token_provider.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/quoting.h"
#include "zetasql/parser/macros/standalone_macro_expansion.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

using ::testing::_;
using ::testing::AllOf;
using ::testing::Bool;
using ::testing::Combine;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::ExplainMatchResult;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Pointwise;
using ::testing::SizeIs;
using ::testing::Values;
using ::testing::ValuesIn;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// Template specializations to print tokens & warnings in failed test messages.
static void PrintTo(const TokenWithLocation& token, std::ostream* os) {
  *os << absl::StrFormat(
      "(kind: %i, location: %s, text: '%s', prev_spaces: '%s')", token.kind,
      token.location.GetString(), token.text, token.preceding_whitespaces);
}

template <typename T>
static void PrintTo(const std::vector<T>& tokens, std::ostream* os) {
  *os << "Tokens: [\n";
  for (const auto& token : tokens) {
    *os << "  ";
    PrintTo(token, os);
    *os << "\n";
  }
  *os << "]\n";
}

template <typename T>
static std::string ToString(const std::vector<T>& tokens) {
  std::stringstream os;
  PrintTo(tokens, &os);
  return os.str();
}

static void PrintTo(const std::vector<absl::Status>& warnings,
                    std::ostream* os) {
  *os << "Warnings: [\n";
  for (const auto& warning : warnings) {
    *os << "  " << warning << "\n";
  }
  *os << "]\n";
}

static void PrintTo(const ExpansionOutput& expansion_output, std::ostream* os) {
  *os << "ExpansionOutput: {\n";
  PrintTo(expansion_output.expanded_tokens, os);
  PrintTo(expansion_output.warnings, os);
  *os << "}\n";
}

MATCHER_P(TokenIs, expected, "") {
  return ExplainMatchResult(
      FieldsAre(Eq(expected.kind), Eq(expected.location), Eq(expected.text),
                Eq(expected.preceding_whitespaces)),
      arg, result_listener);
}

MATCHER(TokenEq, "") {
  return ExplainMatchResult(TokenIs(::testing::get<0>(arg)),
                            ::testing::get<1>(arg), result_listener);
}

MATCHER_P(TokensEq, expected, ToString(expected)) {
  return ExplainMatchResult(Pointwise(TokenEq(), expected), arg.expanded_tokens,
                            result_listener);
}

template <typename M>
std::string StatusMatcherToString(const M& m) {
  std::stringstream os;
  ((::testing::Matcher<std::vector<absl::Status>>)m).DescribeTo(&os);
  return os.str();
}

MATCHER_P(HasTokens, m,
          absl::StrCat("Has expanded tokens that ", StatusMatcherToString(m))) {
  return ExplainMatchResult(m, arg.expanded_tokens, result_listener);
}

MATCHER_P(HasWarnings, m,
          absl::StrCat("Has warnings that ", StatusMatcherToString(m))) {
  return ExplainMatchResult(m, arg.warnings, result_listener);
}

static LanguageOptions GetLanguageOptions(bool is_strict) {
  LanguageOptions language_options = LanguageOptions();
  language_options.EnableLanguageFeature(FEATURE_V_1_4_SQL_MACROS);
  if (is_strict) {
    language_options.EnableLanguageFeature(FEATURE_V_1_4_ENFORCE_STRICT_MACROS);
  }
  return language_options;
}

static Location MakeLocation(absl::string_view filename, int start_offset,
                             int end_offset) {
  return Location(ParseLocationPoint::FromByteOffset(filename, start_offset),
                  ParseLocationPoint::FromByteOffset(filename, end_offset));
}

static absl::string_view kTopFileName = "top_file.sql";

static Location MakeLocation(int start_offset, int end_offset) {
  return MakeLocation(kTopFileName, start_offset, end_offset);
}

static absl::StatusOr<ExpansionOutput> ExpandMacros(
    const absl::string_view text, const MacroCatalog& macro_catalog,
    const LanguageOptions& language_options) {
  return MacroExpander::ExpandMacros(
      kTopFileName, text, macro_catalog, language_options,
      {.mode = ErrorMessageMode::ERROR_MESSAGE_ONE_LINE});
}

TEST(MacroExpanderTest, ExpandsEmptyMacros) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", ""});

  EXPECT_THAT(ExpandMacros("\t$empty\r\n$empty$empty", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {YYEOF, MakeLocation(21, 21), "", "\t\r\n"}})));
}

TEST(MacroExpanderTest, TrailingWhitespaceIsMovedToEofToken) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(ExpandMacros(";\t", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {';', MakeLocation(0, 1), ";", ""},
                  {YYEOF, MakeLocation(2, 2), "", "\t"}})));
}

TEST(MacroExpanderTest, ErrorsCanPrintLocation) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "$unknown"});

  EXPECT_THAT(
      ExpandMacros("\t$empty\r\n$empty$empty", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Macro 'empty' not found. [at top_file.sql:1:9]")));
}

TEST(MacroExpanderTest, TracksCountOfUnexpandedTokensConsumedIncludingEOF) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", ""});

  LanguageOptions options = GetLanguageOptions(/*is_strict=*/false);

  auto token_provider = std::make_unique<FlexTokenProvider>(
      BisonParserMode::kTokenizer, kTopFileName, "\t$empty\r\n$empty$empty",
      /*start_offset=*/0, options);
  auto arena = std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/1024);
  MacroExpander expander(std::move(token_provider), macro_catalog, arena.get(),
                         ErrorMessageOptions{}, /*parent_location=*/nullptr);

  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  YYEOF, MakeLocation(21, 21), "", "\t\r\n"})));
  EXPECT_EQ(expander.num_unexpanded_tokens_consumed(), 4);
}

TEST(MacroExpanderTest,
     TracksCountOfUnexpandedTokensConsumedButNotFromDefinitions) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "1 2 3"});

  LanguageOptions options = GetLanguageOptions(/*is_strict=*/false);

  auto token_provider = std::make_unique<FlexTokenProvider>(
      BisonParserMode::kTokenizer, kTopFileName, "$m", /*start_offset=*/0,
      options);
  auto arena = std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/1024);
  MacroExpander expander(std::move(token_provider), macro_catalog, arena.get(),
                         ErrorMessageOptions{}, /*parent_location=*/nullptr);

  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  INTEGER_LITERAL, MakeLocation("macro:m", 0, 1), "1", ""})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  INTEGER_LITERAL, MakeLocation("macro:m", 2, 3), "2", " "})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  INTEGER_LITERAL, MakeLocation("macro:m", 4, 5), "3", " "})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(
                  TokenWithLocation{YYEOF, MakeLocation(2, 2), "", ""})));

  // We count 2 unexpanded tokens: $m and YYEOF. Tokens in $m's definition
  // do not count.
  EXPECT_EQ(expander.num_unexpanded_tokens_consumed(), 2);
}

TEST(MacroExpanderTest, ExpandsEmptyMacrosSplicedWithIntLiterals) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", ""});
  macro_catalog.insert({"int", "123"});

  EXPECT_THAT(ExpandMacros("\n$empty()1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {INTEGER_LITERAL, MakeLocation(9, 10), "1", "\n"},
                  {YYEOF, MakeLocation(10, 10), "", ""}})));

  EXPECT_THAT(
      ExpandMacros("\n$empty()$int", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          // Location is from the macro it was pulled from.
          // We can stack it later for nested invocations.
          {INTEGER_LITERAL, MakeLocation("macro:int", 0, 3), "123", "\n"},
          {YYEOF, MakeLocation(13, 13), "", ""}})));

  EXPECT_THAT(ExpandMacros("\n1$empty()", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {INTEGER_LITERAL, MakeLocation(1, 2), "1", "\n"},
                  {YYEOF, MakeLocation(10, 10), "", ""}})));

  EXPECT_THAT(
      ExpandMacros("\n$int()$empty", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {INTEGER_LITERAL, MakeLocation("macro:int", 0, 3), "123", "\n"},
          {YYEOF, MakeLocation(13, 13), "", ""}})));
}

TEST(MacroExpanderTest,
     ExpandsIdentifiersSplicedWithEmptyMacrosAndIntLiterals) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"identifier", "abc"});
  macro_catalog.insert({"empty", ""});
  macro_catalog.insert({"int", "123"});

  EXPECT_THAT(ExpandMacros("\na$empty()1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(1, 2), "a1", "\n"},
                  {YYEOF, MakeLocation(11, 11), "", ""}})));

  EXPECT_THAT(ExpandMacros("\na$empty()$int", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(1, 2), "a123", "\n"},
                  {YYEOF, MakeLocation(14, 14), "", ""}})));

  EXPECT_THAT(
      ExpandMacros("\n$identifier$empty()1", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {IDENTIFIER, MakeLocation("macro:identifier", 0, 3), "abc1", "\n"},
          {YYEOF, MakeLocation(21, 21), "", ""}})));

  EXPECT_THAT(
      ExpandMacros("\n$identifier$empty()$int", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {IDENTIFIER, MakeLocation("macro:identifier", 0, 3), "abc123", "\n"},
          {YYEOF, MakeLocation(24, 24), "", ""}})));
}

TEST(MacroExpanderTest, CanExpandWithoutArgsAndNoSplicing) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"prefix", "xyz"});
  macro_catalog.insert({"suffix1", "123"});
  macro_catalog.insert({"suffix2", "456 abc"});
  macro_catalog.insert({"empty", ""});

  EXPECT_THAT(
      ExpandMacros("select abc tbl_\t$empty+ $suffix1 $suffix2 $prefix\t",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 10), "abc", " "},
          {IDENTIFIER, MakeLocation(11, 15), "tbl_", " "},
          {'+', MakeLocation(22, 23), "+", "\t"},
          {INTEGER_LITERAL, MakeLocation("macro:suffix1", 0, 3), "123", " "},
          {INTEGER_LITERAL, MakeLocation("macro:suffix2", 0, 3), "456", " "},
          {IDENTIFIER, MakeLocation("macro:suffix2", 4, 7), "abc", " "},
          {IDENTIFIER, MakeLocation("macro:prefix", 0, 3), "xyz", " "},
          {YYEOF, MakeLocation(50, 50), "", "\t"}})));
}

TEST(MacroExpanderTest, CanExpandWithoutArgsWithSplicing) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"identifier", "xyz"});
  macro_catalog.insert({"numbers", "123"});
  macro_catalog.insert({"multiple_tokens", "456 pq abc"});
  macro_catalog.insert({"empty", "  "});

  EXPECT_THAT(
      ExpandMacros("select tbl_$numbers$multiple_tokens$empty$identifier "
                   "$empty$identifier$numbers$empty a+b",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 11), "tbl_123456", " "},
          {IDENTIFIER, MakeLocation("macro:multiple_tokens", 4, 6), "pq", " "},
          {IDENTIFIER, MakeLocation("macro:multiple_tokens", 7, 10), "abcxyz",
           " "},
          {IDENTIFIER, MakeLocation("macro:identifier", 0, 3), "xyz123", " "},
          {IDENTIFIER, MakeLocation(85, 86), "a", " "},
          {'+', MakeLocation(86, 87), "+", ""},
          {IDENTIFIER, MakeLocation(87, 88), "b", ""},
          {YYEOF, MakeLocation(88, 88), "", ""},
      })));
}

TEST(MacroExpanderTest, KeywordsCanSpliceToFormIdentifiers) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"suffix", "123"});
  macro_catalog.insert({"empty", "    "});

  EXPECT_THAT(
      ExpandMacros("FROM$suffix  $empty()FROM$suffix  FROM$empty$suffix  "
                   "$empty()FROM$empty()2",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          // Note: spliced tokens take the first unexpanded location
          {IDENTIFIER, MakeLocation(0, 4), "FROM123", ""},
          {IDENTIFIER, MakeLocation(21, 25), "FROM123", "  "},
          {IDENTIFIER, MakeLocation(34, 38), "FROM123", "  "},
          {IDENTIFIER, MakeLocation(61, 65), "FROM2", "  "},
          {YYEOF, MakeLocation(74, 74), "", ""}})));
}

TEST(MacroExpanderTest, SpliceMacroInvocationWithIdentifier_Lenient) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "  a  "});

  EXPECT_THAT(
      ExpandMacros("$m()b", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {IDENTIFIER, MakeLocation("macro:m", 2, 3), "ab", ""},
              {YYEOF, MakeLocation(5, 5), "", ""}}),
          HasWarnings(ElementsAre(StatusIs(
              _, Eq("Splicing tokens (a) and (b) [at top_file.sql:1:5]")))))));
}

TEST(MacroExpanderTest, SpliceMacroInvocationWithIdentifier_Strict) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "  a  "});

  std::vector<absl::Status> warnings;
  EXPECT_THAT(
      ExpandMacros("$m()b", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Splicing tokens (a) and (b) [at top_file.sql:1:5]")));
}

TEST(MacroExpanderTest, StrictProducesErrorOnIncompatibleQuoting) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"single_quoted", "'sq'"});

  ASSERT_THAT(ExpandMacros("select `ab$single_quoted`", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/true)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {KW_SELECT, MakeLocation(0, 6), "select", ""},
                  {IDENTIFIER, MakeLocation(7, 25), "`ab$single_quoted`", " "},
                  {YYEOF, MakeLocation(25, 25), "", ""},
              })));
}

TEST(MacroExpanderTest, LenientCanHandleMixedQuoting) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"single_quoted", "'sq'"});

  ASSERT_THAT(
      ExpandMacros("select `ab$single_quoted`", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      StatusIs(_,
               Eq("Cannot expand a string literal(single "
                  "quote) into a quoted identifier. [at top_file.sql:1:11]")));
}

TEST(MacroExpanderTest, CanHandleUnicode) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"unicode", "'😀'"});

  EXPECT_THAT(ExpandMacros("'$😀$unicode'", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {STRING_LITERAL, MakeLocation(0, 15), "'$😀😀'", ""},
                  {YYEOF, MakeLocation(15, 15), "", ""},
              })));
}

TEST(MacroExpanderTest, ExpandsWithinQuotedIdentifiers_SingleToken) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"identifier", "xyz"});
  macro_catalog.insert({"numbers", "456"});
  macro_catalog.insert({"inner_quoted_id", "`bq`"});
  macro_catalog.insert({"empty", "     "});

  EXPECT_THAT(
      ExpandMacros("select `ab$identifier$numbers$empty$inner_quoted_id`",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 52), "`abxyz456bq`", " "},
          {YYEOF, MakeLocation(52, 52), "", ""},
      })));
}

TEST(MacroExpanderTest, ExpandsWithinQuotedIdentifiers_SingleToken_WithSpaces) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"identifier", "xyz"});
  macro_catalog.insert({"numbers", "456"});
  macro_catalog.insert({"inner_quoted_id", "`bq`"});
  macro_catalog.insert({"empty", "     "});

  EXPECT_THAT(
      ExpandMacros("select `  ab$identifier$numbers$empty$inner_quoted_id  `\n",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 56), "`  abxyz456bq  `", " "},
          {YYEOF, MakeLocation(57, 57), "", "\n"},
      })));
}

TEST(MacroExpanderTest, ExpandsWithinQuotedIdentifiers_MultipleTokens) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"identifier", "xyz"});
  macro_catalog.insert({"numbers", "123 456"});
  macro_catalog.insert({"inner_quoted_id", "`bq`"});
  macro_catalog.insert({"empty", "     "});

  EXPECT_THAT(
      ExpandMacros(
          "select\n`  ab$identifier$numbers$empty$inner_quoted_id  cd\t\t`\n",
          macro_catalog, GetLanguageOptions(/*is_strict=*/false)),

      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 60), "`  abxyz123 456bq  cd\t\t`", "\n"},
          {YYEOF, MakeLocation(61, 61), "", "\n"},
      })));
}

TEST(MacroExpanderTest, UnknownMacrosAreLeftUntouched) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"ints", "\t\t\t1 2\t\t\t"});

  EXPECT_THAT(
      ExpandMacros("  $x$y(a\n,\t$ints\n\t,\t$z,\n$w(\t\tb\n\n,\t\r)\t\n)   ",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {MACRO_INVOCATION, MakeLocation(2, 4), "$x", "  "},
          {MACRO_INVOCATION, MakeLocation(4, 6), "$y", ""},
          {'(', MakeLocation(6, 7), "(", ""},
          {IDENTIFIER, MakeLocation(7, 8), "a", ""},
          {',', MakeLocation(9, 10), ",", "\n"},
          {INTEGER_LITERAL, MakeLocation("macro:ints", 3, 4), "1", "\t"},
          {INTEGER_LITERAL, MakeLocation("macro:ints", 5, 6), "2", " "},
          {',', MakeLocation(18, 19), ",", "\n\t"},
          {MACRO_INVOCATION, MakeLocation(20, 22), "$z", "\t"},
          {',', MakeLocation(22, 23), ",", ""},
          {MACRO_INVOCATION, MakeLocation(24, 26), "$w", "\n"},
          {'(', MakeLocation(26, 27), "(", ""},
          {IDENTIFIER, MakeLocation(29, 30), "b", "\t\t"},
          {',', MakeLocation(32, 33), ",", "\n\n"},
          {')', MakeLocation(35, 36), ")", "\t\r"},
          {')', MakeLocation(38, 39), ")", "\t\n"},
          {YYEOF, MakeLocation(42, 42), "", "   "}})));
}

TEST(MacroExpanderTest, UnknownMacrosAreLeftUntouched_EmptyArgList) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(ExpandMacros("\t$w(\n$z(\n)\t)   ", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {MACRO_INVOCATION, MakeLocation(1, 3), "$w", "\t"},
                  {'(', MakeLocation(3, 4), "(", ""},
                  {MACRO_INVOCATION, MakeLocation(5, 7), "$z", "\n"},
                  {'(', MakeLocation(7, 8), "(", ""},
                  {')', MakeLocation(9, 10), ")", "\n"},
                  {')', MakeLocation(11, 12), ")", "\t"},
                  {YYEOF, MakeLocation(15, 15), "", "   "}})));
}

TEST(MacroExpanderTest, LeavesPlxParamsUndisturbed) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(ExpandMacros("  a${x}   ", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(2, 3), "a", "  "},
                  {DOLLAR_SIGN, MakeLocation(3, 4), "$", ""},
                  {'{', MakeLocation(4, 5), "{", ""},
                  {IDENTIFIER, MakeLocation(5, 6), "x", ""},
                  {'}', MakeLocation(6, 7), "}", ""},
                  {YYEOF, MakeLocation(10, 10), "", "   "}})));
}

TEST(MacroExpanderTest, DoesNotStrictlyTokenizeLiteralContents) {
  MacroCatalog macro_catalog;
  EXPECT_THAT(ExpandMacros(R"("30d\a's")", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {STRING_LITERAL, MakeLocation(0, 9), R"("30d\a's")", ""},
                  {YYEOF, MakeLocation(9, 9), "", ""}})));
}

TEST(MacroExpanderTest, SkipsQuotesInLiterals) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(
      ExpandMacros(R"(SELECT "Doesn't apply")", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "SELECT", ""},
          {STRING_LITERAL, MakeLocation(7, 22), R"("Doesn't apply")", " "},
          {YYEOF, MakeLocation(22, 22), "", ""}})));
}

TEST(MacroExpanderTest, SeparateParenthesesAreNotArgLists) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "   bc   "});

  EXPECT_THAT(ExpandMacros("a$m (x, y)", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(0, 1), "abc", ""},
                  {'(', MakeLocation(4, 5), "(", " "},
                  {IDENTIFIER, MakeLocation(5, 6), "x", ""},
                  {',', MakeLocation(6, 7), ",", ""},
                  {IDENTIFIER, MakeLocation(8, 9), "y", " "},
                  {')', MakeLocation(9, 10), ")", ""},
                  {YYEOF, MakeLocation(10, 10), "", ""}})));
}

TEST(MacroExpanderTest,
     ProducesCorrectErrorOnUnbalancedParenthesesInMacroArgumentLists) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(
      ExpandMacros("a$m((x, y)", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          Eq("Unbalanced parentheses in macro argument list. Make sure that "
             "parentheses are balanced even inside macro arguments. [at "
             "top_file.sql:1:11]")));

  EXPECT_THAT(
      ExpandMacros("$m(x;)", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          Eq("Unbalanced parentheses in macro argument list. Make sure that "
             "parentheses are balanced even inside macro arguments. [at "
             "top_file.sql:1:5]")));
}

TEST(MacroExpanderTest, ArgsNotAllowedInsideLiterals) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(
      ExpandMacros("'$a()'", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(HasWarnings(ElementsAre(
          StatusIs(_, Eq("Argument lists are not allowed inside "
                         "literals [at top_file.sql:1:4]")),
          StatusIs(_,
                   HasSubstr("Macro 'a' not found. [at top_file.sql:1:1]"))))));

  EXPECT_THAT(ExpandMacros("'$a('", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              StatusIs(_, Eq("Nested macro argument lists inside literals are "
                             "not allowed [at top_file.sql:1:4]")));
}

TEST(MacroExpanderTest, SpliceArgWithIdentifier) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "$1a"});

  EXPECT_THAT(ExpandMacros("$m(x)", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(3, 4), "xa", ""},
                  {YYEOF, MakeLocation(5, 5), "", ""},
              })));
}

TEST(MacroExpanderTest, ExpandsArgs) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"select_list", "a , $1, $2  "});
  macro_catalog.insert({"from_clause", "FROM tbl_$1$empty"});
  macro_catalog.insert({"splice", "   $1$2   "});
  macro_catalog.insert({"numbers", "123 456"});
  macro_catalog.insert({"inner_quoted_id", "`bq`"});
  macro_catalog.insert({"empty", "     "});
  macro_catalog.insert({"MakeLiteral", "'$1'"});

  EXPECT_THAT(
      ExpandMacros("select $MakeLiteral(  x  ) $splice(  b  ,   89  ), "
                   "$select_list(  b, c )123 $from_clause(  123  ) ",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {STRING_LITERAL, MakeLocation("macro:MakeLiteral", 0, 4), "'x'", " "},
          {IDENTIFIER, MakeLocation(37, 38), "b89", " "},
          {',', MakeLocation(49, 50), ",", ""},
          {IDENTIFIER, MakeLocation("macro:select_list", 0, 1), "a", " "},
          {',', MakeLocation("macro:select_list", 2, 3), ",", " "},
          {IDENTIFIER, MakeLocation(66, 67), "b", " "},
          {',', MakeLocation("macro:select_list", 6, 7), ",", ""},
          {IDENTIFIER, MakeLocation(69, 70), "c123", " "},
          {KW_FROM, MakeLocation("macro:from_clause", 0, 4), "FROM", " "},
          {IDENTIFIER, MakeLocation("macro:from_clause", 5, 9), "tbl_123", " "},
          {YYEOF, MakeLocation(98, 98), "", " "},
      })));
}

TEST(MacroExpanderTest, ExpandsArgsThatHaveParensAndCommas) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"repeat", "$1, $1, $2, $2"});

  EXPECT_THAT(ExpandMacros("select $repeat(1, (2,3))", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/true)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {KW_SELECT, MakeLocation(0, 6), "select", ""},
                  {INTEGER_LITERAL, MakeLocation(15, 16), "1", " "},
                  {',', MakeLocation("macro:repeat", 2, 3), ",", ""},
                  {INTEGER_LITERAL, MakeLocation(15, 16), "1", " "},
                  {',', MakeLocation("macro:repeat", 6, 7), ",", ""},
                  {'(', MakeLocation(18, 19), "(", " "},
                  {INTEGER_LITERAL, MakeLocation(19, 20), "2", ""},
                  {',', MakeLocation(20, 21), ",", ""},
                  {INTEGER_LITERAL, MakeLocation(21, 22), "3", ""},
                  {')', MakeLocation(22, 23), ")", ""},
                  {',', MakeLocation("macro:repeat", 10, 11), ",", ""},
                  {'(', MakeLocation(18, 19), "(", " "},
                  {INTEGER_LITERAL, MakeLocation(19, 20), "2", ""},
                  {',', MakeLocation(20, 21), ",", ""},
                  {INTEGER_LITERAL, MakeLocation(21, 22), "3", ""},
                  {')', MakeLocation(22, 23), ")", ""},
                  {YYEOF, MakeLocation(24, 24), "", ""}})));
}

TEST(MacroExpanderTest, ExtraArgsProduceWarningOrError) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", ""});

  EXPECT_THAT(ExpandMacros("$empty(x)", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(AllOf(
                  TokensEq(std::vector<TokenWithLocation>{
                      {YYEOF, MakeLocation(9, 9), "", ""}}),
                  HasWarnings(ElementsAre(StatusIs(
                      _, Eq("Macro invocation has too many arguments (1) "
                            "while the definition only references up to 0 "
                            "arguments [at top_file.sql:1:1]")))))));

  EXPECT_THAT(
      ExpandMacros("$empty(x)", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Macro invocation has too many arguments (1) while "
                     "the definition only references up to 0 arguments "
                     "[at top_file.sql:1:1]")));
}

TEST(MacroExpanderTest, UnknownArgsAreLeftUntouched) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", ""});
  macro_catalog.insert({"unknown_not_in_a_literal", "\nx$3\n"});
  macro_catalog.insert({"unknown_inside_literal", "'\ty$3\t'"});

  EXPECT_THAT(
      ExpandMacros(
          "  a$empty()$1$unknown_not_in_a_literal  '$unknown_not_in_a_literal' "
          "$unknown_inside_literal\t$1\n'$2'",
          macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {IDENTIFIER, MakeLocation(2, 3), "a", "  "},
              {MACRO_ARGUMENT_REFERENCE, MakeLocation(11, 13), "$1", ""},
              {IDENTIFIER, MakeLocation("macro:unknown_not_in_a_literal", 1, 2),
               "x", ""},
              {STRING_LITERAL, MakeLocation(40, 67), "'x'", "  "},
              {STRING_LITERAL,
               MakeLocation("macro:unknown_inside_literal", 0, 7), "'\ty\t'",
               " "},
              {MACRO_ARGUMENT_REFERENCE, MakeLocation(92, 94), "$1", "\t"},
              {STRING_LITERAL, MakeLocation(95, 99), "'$2'", "\n"},
              {YYEOF, MakeLocation(99, 99), "", ""}}),
          HasWarnings(ElementsAre(
              StatusIs(_, Eq("Macro invocation missing argument list. "
                             "[at top_file.sql:1:39]")),
              StatusIs(_, Eq("Argument index $3 out of range. Invocation was "
                             "provided only 1 arguments. [at "
                             "macro:unknown_not_in_a_literal:2:2]; Expanded "
                             "from top_file.sql [at top_file.sql:1:14]")),
              StatusIs(_, Eq("Macro invocation missing argument list. [at "
                             "top_file.sql:1:26]; Expanded from top_file.sql "
                             "[at top_file.sql:1:42]")),
              StatusIs(_,
                       Eq("Argument index $3 out of range. Invocation was "
                          "provided only 1 arguments. [at "
                          "macro:unknown_not_in_a_literal:2:2]; Expanded from "
                          "top_file.sql [at top_file.sql:1:42]; Expanded from "
                          "top_file.sql [at top_file.sql:1:1]")),
              StatusIs(_, Eq("Macro invocation missing argument list. "
                             "[at top_file.sql:1:92]")),
              StatusIs(
                  _,
                  Eq("Argument index $3 out of range. Invocation was provided "
                     "only 1 arguments. [at macro:unknown_inside_literal:1:1]; "
                     "Expanded from top_file.sql [at top_file.sql:1:69]; "
                     "Expanded from macro:unknown_inside_literal [at "
                     "macro:unknown_inside_literal:1:10]")))))));

  EXPECT_THAT(
      ExpandMacros("unknown_inside_literal() $unknown_not_in_a_literal()",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_,
               Eq("Argument index $3 out of range. Invocation was provided "
                  "only 1 arguments. [at macro:unknown_not_in_a_literal:2:2]; "
                  "Expanded from top_file.sql [at top_file.sql:1:26]")));
}

TEST(MacroExpanderTest, ExpandsStandaloneDollarSignsAtTopLevel) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"empty", "\n\n"});

  EXPECT_THAT(ExpandMacros("\t\t$empty${a}   \n$$$empty${b}$", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {DOLLAR_SIGN, MakeLocation(8, 9), "$", "\t\t"},
                  {'{', MakeLocation(9, 10), "{", ""},
                  {IDENTIFIER, MakeLocation(10, 11), "a", ""},
                  {'}', MakeLocation(11, 12), "}", ""},
                  {DOLLAR_SIGN, MakeLocation(16, 17), "$", "   \n"},
                  {DOLLAR_SIGN, MakeLocation(17, 18), "$", ""},
                  {DOLLAR_SIGN, MakeLocation(24, 25), "$", ""},
                  {'{', MakeLocation(25, 26), "{", ""},
                  {IDENTIFIER, MakeLocation(26, 27), "b", ""},
                  {'}', MakeLocation(27, 28), "}", ""},
                  {DOLLAR_SIGN, MakeLocation(28, 29), "$", ""},
                  {YYEOF, MakeLocation(29, 29), "", ""}})));
}

TEST(MacroExpanderTest, ProducesWarningOrErrorOnMissingArgumentLists) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "1"});

  EXPECT_THAT(ExpandMacros("$m", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(AllOf(
                  TokensEq(std::vector<TokenWithLocation>{
                      {INTEGER_LITERAL, MakeLocation("macro:m", 0, 1), "1", ""},
                      {YYEOF, MakeLocation(2, 2), "", ""}}),
                  HasWarnings(ElementsAre(
                      StatusIs(_, Eq("Macro invocation missing argument "
                                     "list. [at top_file.sql:1:3]")))))));

  EXPECT_THAT(
      ExpandMacros("$m", macro_catalog, GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Macro invocation missing argument list. [at "
                     "top_file.sql:1:3]")));
}

TEST(MacroExpanderTest, ExpandsAllFormsOfLiterals) {
  MacroCatalog macro_catalog;
  std::vector<std::string> string_literals{
      "'a'",  "'''a'''",  R"("a")",  R"("""a""")",
      "r'a'", "r'''a'''", R"(r"a")", R"(r"""a""")",
  };

  for (const std::string& string_literal : string_literals) {
    int literal_length = static_cast<int>(string_literal.length());
    EXPECT_THAT(
        ExpandMacros(string_literal, macro_catalog,
                     GetLanguageOptions(/*is_strict=*/false)),
        IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
            {STRING_LITERAL, MakeLocation(0, literal_length), string_literal,
             ""},
            {YYEOF, MakeLocation(literal_length, literal_length), "", ""}})));
  }

  std::vector<std::string> byte_literals{
      "b'''a'''",  R"(b"a")",  R"(b"""a""")",  "rb'a'",
      "rb'''a'''", R"(rb"a")", R"(rb"""a""")",
  };

  for (const std::string& byte_literal : byte_literals) {
    int literal_length = static_cast<int>(byte_literal.length());
    EXPECT_THAT(
        ExpandMacros(byte_literal, macro_catalog,
                     GetLanguageOptions(/*is_strict=*/false)),
        IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
            {BYTES_LITERAL, MakeLocation(0, literal_length), byte_literal, ""},
            {YYEOF, MakeLocation(literal_length, literal_length), "", ""}})));
  }
}

static int FinalTokenKind(const QuotingSpec quoting_spec) {
  switch (quoting_spec.literal_kind()) {
    case LiteralTokenKind::kNonLiteral:
      // Tests using this helper only use int literals
      return INTEGER_LITERAL;
    case LiteralTokenKind::kBacktickedIdentifier:
      return IDENTIFIER;
    case LiteralTokenKind::kStringLiteral:
      return STRING_LITERAL;
    case LiteralTokenKind::kBytesLiteral:
      return BYTES_LITERAL;
  }
}

class PositiveNestedLiteralTest
    : public ::testing::TestWithParam<std::tuple<
          /*outer*/ QuotingSpec, /*inner*/ QuotingSpec>> {};

TEST_P(PositiveNestedLiteralTest, PositiveNestedLiteralTestParameterized) {
  const QuotingSpec outer_quoting = std::get<0>(GetParam());
  const QuotingSpec inner_quoting = std::get<1>(GetParam());

  MacroCatalog catalog;
  std::string macro_def = QuoteText("1", inner_quoting);
  catalog.insert({"a", macro_def});
  std::string unexpanded_sql = QuoteText("$a", outer_quoting);
  int unexpanded_length = static_cast<int>(unexpanded_sql.length());

  std::string expanded = QuoteText("1", outer_quoting);

  int final_token_kind = FinalTokenKind(outer_quoting);

  if (inner_quoting.literal_kind() == LiteralTokenKind::kNonLiteral ||
      (outer_quoting.quote_kind() == inner_quoting.quote_kind() &&
       outer_quoting.prefix().length() == inner_quoting.prefix().length())) {
    EXPECT_THAT(
        ExpandMacros(unexpanded_sql, catalog,
                     GetLanguageOptions(/*is_strict=*/false)),
        IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
            {final_token_kind, MakeLocation(0, unexpanded_length),
             QuoteText("1", outer_quoting), ""},
            {YYEOF, MakeLocation(unexpanded_length, unexpanded_length), "", ""},
        })));
  } else {
    ASSERT_TRUE(outer_quoting.literal_kind() == inner_quoting.literal_kind());
    EXPECT_THAT(
        ExpandMacros(unexpanded_sql, catalog,
                     GetLanguageOptions(/*is_strict=*/false)),
        IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
            {final_token_kind, MakeLocation(0, unexpanded_length),
             QuoteText("", outer_quoting), ""},
            {final_token_kind,
             MakeLocation("macro:a", 0, static_cast<int>(macro_def.length())),
             macro_def, " "},
            {final_token_kind, MakeLocation(0, unexpanded_length),
             QuoteText("", outer_quoting), " "},
            {YYEOF, MakeLocation(unexpanded_length, unexpanded_length), "", ""},
        })));
  }
}

class NegativeNestedLiteralTest
    : public ::testing::TestWithParam<std::tuple<
          /*outer*/ QuotingSpec, /*inner*/ QuotingSpec>> {};

TEST_P(NegativeNestedLiteralTest, NegativeNestedLiteralTestParameterized) {
  const QuotingSpec outer_quoting = std::get<0>(GetParam());
  const QuotingSpec inner_quoting = std::get<1>(GetParam());

  MacroCatalog catalog;
  catalog.insert({"a", QuoteText("1", inner_quoting)});
  std::string unexpanded_sql = QuoteText("$a", outer_quoting);

  EXPECT_THAT(ExpandMacros(unexpanded_sql, catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              StatusIs(_, HasSubstr("Cannot expand a ")));
}

const QuoteKind kAllLiteralQuoteKinds[] = {
    QuoteKind::kOneSingleQuote, QuoteKind::kThreeSingleQuotes,
    QuoteKind::kOneDoubleQuote, QuoteKind::kThreeDoubleQuotes};

static std::vector<QuotingSpec> AllStringLiteralQuotingSpecs() {
  const std::vector<absl::string_view> string_literal_prefixes{"", "r", "R"};
  std::vector<QuotingSpec> string_literals;

  for (QuoteKind quote_kind : kAllLiteralQuoteKinds) {
    for (absl::string_view prefix : string_literal_prefixes) {
      string_literals.push_back(QuotingSpec::StringLiteral(prefix, quote_kind));
    }
  }

  return string_literals;
}

static std::vector<QuotingSpec> AllBytesLiteralQuotingSpecs() {
  const std::vector<absl::string_view> bytes_literal_prefixes{
      "b", "B", "br", "bR", "Br", "BR", "rb", "rB", "Rb", "RB",
  };
  std::vector<QuotingSpec> bytes_literals;

  for (QuoteKind quote_kind : kAllLiteralQuoteKinds) {
    for (absl::string_view prefix : bytes_literal_prefixes) {
      bytes_literals.push_back(QuotingSpec::BytesLiteral(prefix, quote_kind));
    }
  }

  return bytes_literals;
}

static std::vector<QuotingSpec> AllStringsAndBytesLiteralsQuotingSpecs() {
  std::vector<QuotingSpec> quoting_specs;
  for (const QuotingSpec& string_spec : AllStringLiteralQuotingSpecs()) {
    quoting_specs.push_back(string_spec);
  }
  for (const QuotingSpec& bytes_spec : AllBytesLiteralQuotingSpecs()) {
    quoting_specs.push_back(bytes_spec);
  }
  return quoting_specs;
}

// Excludes NON_LITERAL
static std::vector<QuotingSpec> AllQuotingSpecs() {
  std::vector<QuotingSpec> quoting_specs;
  quoting_specs.push_back(QuotingSpec::BacktickedIdentifier());
  for (const QuotingSpec& bytes_spec :
       AllStringsAndBytesLiteralsQuotingSpecs()) {
    quoting_specs.push_back(bytes_spec);
  }

  return quoting_specs;
}

// NON_LITERAL expanded within literal.
INSTANTIATE_TEST_SUITE_P(NonLiteralCanBeNestedEverywhere,
                         PositiveNestedLiteralTest,
                         Combine(ValuesIn(AllQuotingSpecs()),
                                 Values(QuotingSpec::NonLiteral())));

// Backticked identifier only works with backticked identifier on both sides
// (Interaction with non-literals is already covered above).
INSTANTIATE_TEST_SUITE_P(BacktickedIdentifierAcceptsItself,
                         PositiveNestedLiteralTest,
                         Combine(Values(QuotingSpec::BacktickedIdentifier()),
                                 Values(QuotingSpec::BacktickedIdentifier())));
INSTANTIATE_TEST_SUITE_P(
    NestingInsideBacktickedIdentifierIsDisallowed, NegativeNestedLiteralTest,
    Combine(Values(QuotingSpec::BacktickedIdentifier()),
            ValuesIn(AllStringsAndBytesLiteralsQuotingSpecs())));

static std::vector<std::tuple<QuotingSpec, QuotingSpec>>
IdenticalLiteralTestCases() {
  std::vector<std::tuple<QuotingSpec, QuotingSpec>> test_cases;
  for (const QuotingSpec& quoting : AllStringsAndBytesLiteralsQuotingSpecs()) {
    test_cases.push_back(std::make_tuple(quoting, quoting));
  }
  return test_cases;
}

INSTANTIATE_TEST_SUITE_P(LiteralsCanNestInIdenticalQuoting,
                         PositiveNestedLiteralTest,
                         ValuesIn(IdenticalLiteralTestCases()));

static std::vector<std::tuple<QuotingSpec, QuotingSpec>>
DifferentLiteralKindTestCases() {
  std::vector<QuotingSpec> literal_quoting_specs =
      AllStringsAndBytesLiteralsQuotingSpecs();

  std::vector<std::tuple<QuotingSpec, QuotingSpec>> test_cases;
  for (const QuotingSpec& outer : literal_quoting_specs) {
    for (const QuotingSpec& inner : literal_quoting_specs) {
      if (outer.literal_kind() != inner.literal_kind()) {
        test_cases.push_back(std::make_tuple(outer, inner));
      }
    }
  }

  return test_cases;
}

INSTANTIATE_TEST_SUITE_P(LiteralsCannotNestInDifferentTokenKinds,
                         NegativeNestedLiteralTest,
                         ValuesIn(DifferentLiteralKindTestCases()));

INSTANTIATE_TEST_SUITE_P(StringLiteralsCanExpandInAnyStringLiteral,
                         PositiveNestedLiteralTest,
                         Combine(ValuesIn(AllStringLiteralQuotingSpecs()),
                                 ValuesIn(AllStringLiteralQuotingSpecs())));
INSTANTIATE_TEST_SUITE_P(BytesLiteralsCanExpandInAnyBytesLiteral,
                         PositiveNestedLiteralTest,
                         Combine(ValuesIn(AllBytesLiteralQuotingSpecs()),
                                 ValuesIn(AllBytesLiteralQuotingSpecs())));

TEST(MacroExpanderTest, DoesNotReexpand) {
  absl::flat_hash_map<std::string, std::string> macro_catalog;
  macro_catalog.insert({"select_list", "a , $1, $2  "});
  macro_catalog.insert({"splice_invoke", "   $$1$2   "});
  macro_catalog.insert({"inner_quoted_id", "`bq`"});
  macro_catalog.insert({"empty", "     "});

  EXPECT_THAT(
      ExpandMacros("$splice_invoke(  inner_\t,\t\tquoted_id   ), "
                   "$$select_list(  b, c  )123\t",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          // Note: the dollar sign is not spliced with the identifier into a new
          // macro invocation token, because we do not reexpand.
          {DOLLAR_SIGN, MakeLocation("macro:splice_invoke", 3, 4), "$", ""},
          {IDENTIFIER, MakeLocation(17, 23), "inner_quoted_id", ""},
          {',', MakeLocation(40, 41), ",", ""},
          {DOLLAR_SIGN, MakeLocation(42, 43), "$", " "},
          {IDENTIFIER, MakeLocation("macro:select_list", 0, 1), "a", ""},
          {',', MakeLocation("macro:select_list", 2, 3), ",", " "},
          {IDENTIFIER, MakeLocation(58, 59), "b", " "},
          {',', MakeLocation("macro:select_list", 6, 7), ",",
           ""},  // Comes from the macro def
          {IDENTIFIER, MakeLocation(61, 62), "c123", " "},
          {YYEOF, MakeLocation(69, 69), "", "\t"},
      })));
}

TEST(MacroExpanderTest,
     CapsWarningCountAndAddsSentinelWarningWhenTooManyAreProduced) {
  MacroCatalog macro_catalog;

  int num_slashes = 30;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput result,
      ExpandMacros(std::string(num_slashes, '\\'), macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)));

  // Add 1 for the YYEOF
  EXPECT_EQ(result.expanded_tokens.size(), num_slashes + 1);
  for (int i = 0; i < num_slashes - 1; ++i) {
    EXPECT_EQ(result.expanded_tokens[i].kind, TokenKinds::BACKSLASH);
  }
  EXPECT_EQ(result.expanded_tokens.back().kind, YYEOF);

  // The number of warnings is the cap (20) + 1 for the sentinel indicating that
  // more warnings were truncated.
  EXPECT_EQ(result.warnings.size(), 21);
  absl::Status sentinel_status = std::move(result.warnings.back());
  result.warnings.pop_back();
  EXPECT_THAT(
      result.warnings,
      Each(StatusIs(
          _,
          HasSubstr(
              R"(Invalid token (\). Did you mean to use it in a literal?)"))));
  EXPECT_THAT(
      sentinel_status,
      StatusIs(_,
               Eq("Warning count limit reached. Truncating further warnings")));
}

// Ignores location
static TokenWithLocation MakeToken(
    int kind, absl::string_view text,
    absl::string_view preceding_whitespaces = "") {
  return {.kind = kind,
          .location = MakeLocation(-1, -1),
          .text = text,
          .preceding_whitespaces = preceding_whitespaces};
}

TEST(TokensToStringTest, CanGenerateStringFromTokens) {
  std::vector<TokenWithLocation> tokens = {MakeToken(IDENTIFIER, "a", "\t"),
                                           MakeToken(KW_FROM, "FROM"),
                                           MakeToken(YYEOF, "", "\n")};

  // Note the forced space between `a` and `FROM`.
  EXPECT_EQ(TokensToString(tokens, /*force_single_whitespace=*/false),
            "\ta FROM\n");
  EXPECT_EQ(TokensToString(tokens, /*force_single_whitespace=*/true), "a FROM");
}

TEST(TokensToStringTest, DoesNotSpliceTokensEvenWhenNoOriginalSpacesExist) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken(IDENTIFIER, "a", "\t"),
      MakeToken(KW_FROM, "FROM"),
      MakeToken(INTEGER_LITERAL, "0x1A"),
      MakeToken(IDENTIFIER, "b", "\t"),
      MakeToken(KW_FROM, "FROM"),
      MakeToken(FLOATING_POINT_LITERAL, "1."),
      MakeToken(IDENTIFIER, "x", "\t"),
      MakeToken(MACRO_INVOCATION, "$a"),
      MakeToken(INTEGER_LITERAL, "123"),
      MakeToken(MACRO_INVOCATION, "$1"),
      MakeToken(INTEGER_LITERAL, "23"),
      MakeToken('*', "*"),
      MakeToken(YYEOF, "", "\n")};

  // Note the forced spaces
  EXPECT_EQ(TokensToString(tokens, /*force_single_whitespace=*/false),
            "\ta FROM 0x1A\tb FROM 1.\tx$a 123$1 23*\n");
}

TEST(TokensToStringTest, DoesNotCauseCommentOuts) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken('-', "-"), MakeToken('-', "-"), MakeToken('/', "/"),
      MakeToken('/', "/"), MakeToken('/', "/"), MakeToken('*', "*"),
      MakeToken(YYEOF, "")};

  // Note the forced spaces, except for -/
  EXPECT_EQ(TokensToString(tokens, /*standardize_to_single_whitespace=*/false),
            "- -/ / / *");
}

TEST(TokensToStringTest, AlwaysSeparatesNumericLiterals) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken(INTEGER_LITERAL, "0x1"),
      MakeToken(INTEGER_LITERAL, "2"),
      MakeToken(INTEGER_LITERAL, "3"),
      MakeToken(FLOATING_POINT_LITERAL, "4."),
      MakeToken(INTEGER_LITERAL, "5"),
      MakeToken(FLOATING_POINT_LITERAL, ".6"),
      MakeToken(INTEGER_LITERAL, "7"),
      MakeToken(FLOATING_POINT_LITERAL, ".8e9"),
      MakeToken(INTEGER_LITERAL, "10"),
      MakeToken(STRING_LITERAL, "'11'"),
      MakeToken(YYEOF, "")};

  // Note the forced spaces
  EXPECT_EQ(TokensToString(tokens, /*standardize_to_single_whitespace=*/false),
            "0x1 2 3 4. 5 .6 7 .8e9 10'11'");
}

class MacroExpanderLenientTokensInStrictModeTest
    : public ::testing::TestWithParam<absl::string_view> {};

TEST_P(MacroExpanderLenientTokensInStrictModeTest,
       LenientTokensDisallowedInStrictMode) {
  absl::string_view text = GetParam();
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", std::string(text)});

  EXPECT_THAT(
      ExpandMacros(text, macro_catalog, GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, HasSubstr("Syntax error")));

  EXPECT_THAT(ExpandMacros("$m()", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/true)),
              StatusIs(_, HasSubstr("Syntax error")));
}

INSTANTIATE_TEST_SUITE_P(LenientTokensDisallowedInStrictMode,
                         MacroExpanderLenientTokensInStrictModeTest,
                         Values("\\", "3d", "12abc", "0x123xyz", "3.4dab",
                                ".4dab", "1.2e3ab", "1.2e-3ab"));

class MacroExpanderLenientTokensTest
    : public ::testing::TestWithParam<absl::string_view> {};

TEST_P(MacroExpanderLenientTokensTest, LenientTokensAllowedOnlyWhenLenient) {
  absl::string_view text = GetParam();
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", std::string(text)});

  int expected_token_count = 2;  // One token + YYEOF
  if (absl::StrContains(text, '.')) {
    // The dot itself, and maybe the other part
    expected_token_count += absl::StartsWith(text, ".") ? 1 : 2;
    if (absl::StrContains(text, "e-")) {
      expected_token_count +=
          2;  // The negative exponent adds '-' & the exp part
    }
  }

  EXPECT_THAT(ExpandMacros(text, macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(HasTokens(SizeIs(expected_token_count))));

  EXPECT_THAT(ExpandMacros("$m", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(HasTokens(SizeIs(expected_token_count))));

  // When preceding with 'a', we need to update the expected token count as the
  // 'a' will splice with the expanded text unless it's a symbol.
  if (!std::isalpha(text.front()) && !std::isdigit(text.front())) {
    expected_token_count++;
  }

  EXPECT_THAT(ExpandMacros("a$m", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(HasTokens(SizeIs(expected_token_count))));
}

INSTANTIATE_TEST_SUITE_P(LenientTokens, MacroExpanderLenientTokensTest,
                         Values("\\", "3d", "12abc", "0x123xyz", "3.4dab",
                                ".4dab", "1.2e3ab", "1.2e-3ab"));

TEST(MacroExpanderTest, ProducesWarningOnLenientTokens) {
  MacroCatalog macro_catalog;

  EXPECT_THAT(ExpandMacros("\\", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(HasWarnings(ElementsAre(
                  StatusIs(_, Eq("Invalid token (\\). Did you mean to use it "
                                 "in a literal? [at top_file.sql:1:1]"))))));
}

class MacroExpanderParameterizedTest : public ::testing::TestWithParam<bool> {};

TEST_P(MacroExpanderParameterizedTest, DoesNotExpandDefineMacroStatements) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"x", "a"});
  macro_catalog.insert({"y", "b"});

  EXPECT_THAT(
      ExpandMacros("DEFINE MACRO $x $y; DEFINE MACRO $y $x", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())),
      IsOkAndHolds(
          AllOf(TokensEq(std::vector<TokenWithLocation>{
                    {KW_DEFINE_FOR_MACROS, MakeLocation(0, 6), "DEFINE", ""},
                    {KW_MACRO, MakeLocation(7, 12), "MACRO", " "},
                    {MACRO_INVOCATION, MakeLocation(13, 15), "$x", " "},
                    {MACRO_INVOCATION, MakeLocation(16, 18), "$y", " "},
                    {';', MakeLocation(18, 19), ";", ""},
                    {KW_DEFINE_FOR_MACROS, MakeLocation(20, 26), "DEFINE", " "},
                    {KW_MACRO, MakeLocation(27, 32), "MACRO", " "},
                    {MACRO_INVOCATION, MakeLocation(33, 35), "$y", " "},
                    {MACRO_INVOCATION, MakeLocation(36, 38), "$x", " "},
                    {YYEOF, MakeLocation(38, 38), "", ""},
                }),
                HasWarnings(IsEmpty()))));
}

TEST_P(MacroExpanderParameterizedTest,
       DefineStatementsAreNotSpecialExceptAtTheStart) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"x", "a"});

  // This will expand the definition, but it will be a syntax error when sent
  // to the parser, because the KW_DEFINE has not been marked as the special
  // KW_DEFINE_FOR_MACROS which has to be at the start, and original (not
  // expanded from a macro).
  EXPECT_THAT(ExpandMacros("SELECT DEFINE MACRO $x() 1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/GetParam())),
              IsOkAndHolds(AllOf(
                  TokensEq(std::vector<TokenWithLocation>{
                      {KW_SELECT, MakeLocation(0, 6), "SELECT", ""},
                      {KW_DEFINE, MakeLocation(7, 13), "DEFINE", " "},
                      {KW_MACRO, MakeLocation(14, 19), "MACRO", " "},
                      {IDENTIFIER, MakeLocation("macro:x", 0, 1), "a", " "},
                      {INTEGER_LITERAL, MakeLocation(25, 26), "1", " "},
                      {YYEOF, MakeLocation(26, 26), "", ""},
                  }),
                  HasWarnings(IsEmpty()))));
}

TEST_P(MacroExpanderParameterizedTest,
       DoesNotRecognizeGeneratedDefineMacroStatements) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"def", "DEFINE MACRO"});

  EXPECT_THAT(
      ExpandMacros("$def() a 1", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())),
      IsOkAndHolds(
          AllOf(TokensEq(std::vector<TokenWithLocation>{
                    // Note that the first token is KW_DEFINE, not
                    // KW_DEFINE_FOR_MACROS
                    {KW_DEFINE, MakeLocation("macro:def", 0, 6), "DEFINE", ""},
                    {KW_MACRO, MakeLocation("macro:def", 7, 12), "MACRO", " "},
                    {IDENTIFIER, MakeLocation(7, 8), "a", " "},
                    {INTEGER_LITERAL, MakeLocation(9, 10), "1", " "},
                    {YYEOF, MakeLocation(10, 10), "", ""},
                }),
                HasWarnings(IsEmpty()))));
}

TEST_P(MacroExpanderParameterizedTest, HandlesInfiniteRecursion) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"a", "$b()"});
  macro_catalog.insert({"b", "$a()"});

  EXPECT_THAT(
      ExpandMacros("$a()", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())),
      StatusIs(absl::StatusCode::kResourceExhausted,
               Eq("Out of stack space due to deeply nested macro calls.")));
}

INSTANTIATE_TEST_SUITE_P(MacroExpanderParameterizedTest,
                         MacroExpanderParameterizedTest, Bool());

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
