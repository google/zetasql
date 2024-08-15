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
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/flex_token_provider.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/quoting.h"
#include "zetasql/parser/macros/standalone_macro_expansion.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/parse_resume_location.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

using Location = ParseLocationRange;

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
using ::testing::Pair;
using ::testing::Pointwise;
using ::testing::SizeIs;
using ::testing::Values;
using ::testing::ValuesIn;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// Template specializations to print tokens & warnings in failed test messages.
static void PrintTo(const TokenWithLocation& token, std::ostream* os) {
  *os << absl::StrFormat(
      "(kind: %i, location: %s, topmost_invocation_location: %s, text: '%s', "
      "prev_spaces: '%s')",
      token.kind, token.location.GetString(),
      token.topmost_invocation_location.GetString(), token.text,
      token.preceding_whitespaces);
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
                Eq(expected.preceding_whitespaces),
                Eq(expected.topmost_invocation_location)),
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
static absl::string_view kDefsFileName = "defs.sql";

static Location MakeLocation(int start_offset, int end_offset) {
  return MakeLocation(kTopFileName, start_offset, end_offset);
}

static absl::StatusOr<ExpansionOutput> ExpandMacros(
    const absl::string_view text, const MacroCatalog& macro_catalog,
    const LanguageOptions& language_options,
    DiagnosticOptions diagnostic_options = {
        .error_message_options = {
            .mode = ErrorMessageMode::ERROR_MESSAGE_ONE_LINE}}) {
  return MacroExpander::ExpandMacros(std::make_unique<FlexTokenProvider>(
                                         kTopFileName, text, /*start_offset=*/0,
                                         /*end_offset=*/std::nullopt),
                                     language_options, macro_catalog,
                                     diagnostic_options);
}

// This function needs to return void due to the assertions.
static void RegisterMacros(absl::string_view source,
                           MacroCatalog& macro_catalog) {
  ParseResumeLocation location =
      ParseResumeLocation::FromStringView(kDefsFileName, source);
  bool at_end_of_input = false;
  while (!at_end_of_input) {
    std::unique_ptr<ParserOutput> output;
    ZETASQL_ASSERT_OK(ParseNextStatement(
        &location, ParserOptions(GetLanguageOptions(/*is_strict=*/false)),
        &output, &at_end_of_input));
    ASSERT_TRUE(output->statement() != nullptr);
    auto def_macro_stmt =
        output->statement()->GetAsOrNull<ASTDefineMacroStatement>();
    ASSERT_TRUE(def_macro_stmt != nullptr);
    ZETASQL_ASSERT_OK(macro_catalog.RegisterMacro(
        {.source_text = source,
         .location = def_macro_stmt->GetParseLocationRange(),
         .name_location = def_macro_stmt->name()->GetParseLocationRange(),
         .body_location = def_macro_stmt->body()->GetParseLocationRange()}));
  }
}

TEST(MacroExpanderTest, ExpandsEmptyMacros) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO empty", macro_catalog);

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
  RegisterMacros("DEFINE MACRO m $unknown", macro_catalog);

  EXPECT_THAT(
      ExpandMacros("\t$empty\r\n$empty$empty", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Macro 'empty' not found. [at top_file.sql:1:9]")));
}

TEST(MacroExpanderTest, ErrorsCanPrintLocation_EmptyFileNames) {
  MacroCatalog macro_catalog;
  ZETASQL_ASSERT_OK(macro_catalog.RegisterMacro({
      .source_text = "DEFINE MACRO m $unknown",
      .location = MakeLocation("", 0, 23),
      .name_location = MakeLocation("", 13, 14),
      .body_location = MakeLocation("", 14, 23),
  }));
  ZETASQL_ASSERT_OK(macro_catalog.RegisterMacro({
      .source_text = "DEFINE MACRO m2 $m()",
      .location = MakeLocation("", 0, 20),
      .name_location = MakeLocation("", 13, 15),
      .body_location = MakeLocation("", 15, 20),
  }));

  auto options = GetLanguageOptions(/*is_strict=*/true);
  EXPECT_THAT(MacroExpander::ExpandMacros(
                  std::make_unique<FlexTokenProvider>(
                      "", "$m2()",
                      /*start_offset=*/0, /*end_offset=*/std::nullopt),
                  options, macro_catalog,
                  {{.mode = ErrorMessageMode::ERROR_MESSAGE_ONE_LINE}}),
              StatusIs(_, Eq("Macro 'unknown' not found. [at 1:16]; "
                             "Expanded from macro:m [at 1:17]; "
                             "Expanded from macro:m2 [at 1:1]")));
}

TEST(MacroExpanderTest, ErrorsOrWarningsDoNotTruncateCaretContext) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO one 1;\n"
      "DEFINE MACRO two 2;\n"
      "DEFINE MACRO add $1 + $2;\n"
      "DEFINE MACRO outer $add($one, $two);  # Extra comment",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros(
          "$outer()", macro_catalog, GetLanguageOptions(/*is_strict=*/false),
          {{.mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET}}),
      IsOkAndHolds(HasWarnings(ElementsAre(
          StatusIs(_,
                   Eq("Invocation of macro 'one' missing argument list. [at "
                      "defs.sql:4:29]\n"
                      "DEFINE MACRO outer $add($one, $two);  # Extra comment\n"
                      "                            ^\n"
                      "Expanded from macro:outer [at top_file.sql:1:1]\n"
                      "$outer()\n"
                      "^")),
          StatusIs(_,
                   Eq("Invocation of macro 'two' missing argument list. [at "
                      "defs.sql:4:35]\n"
                      "DEFINE MACRO outer $add($one, $two);  # Extra comment\n"
                      "                                  ^\n"
                      "Expanded from macro:outer [at top_file.sql:1:1]\n"
                      "$outer()\n"
                      "^"))))));

  EXPECT_THAT(
      ExpandMacros(
          "$outer()", macro_catalog, GetLanguageOptions(/*is_strict=*/true),
          {{.mode = ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET}}),
      StatusIs(_, Eq("Invocation of macro 'one' missing argument list. [at "
                     "defs.sql:4:29]\n"
                     "DEFINE MACRO outer $add($one, $two);  # Extra comment\n"
                     "                            ^\n"
                     "Expanded from macro:outer [at top_file.sql:1:1]\n"
                     "$outer()\n"
                     "^")));
}

TEST(MacroExpanderTest, TracksCountOfUnexpandedTokensConsumedIncludingEOF) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO empty", macro_catalog);

  LanguageOptions options = GetLanguageOptions(/*is_strict=*/false);

  auto token_provider = std::make_unique<FlexTokenProvider>(
      kTopFileName, "\t$empty\r\n$empty$empty",
      /*start_offset=*/0, /*end_offset=*/std::nullopt);
  auto arena = std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/1024);
  MacroExpander expander(std::move(token_provider), options, macro_catalog,
                         arena.get(), DiagnosticOptions{},
                         /*parent_location=*/nullptr);

  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  YYEOF, MakeLocation(21, 21), "", "\t\r\n"})));
  EXPECT_EQ(expander.num_unexpanded_tokens_consumed(), 4);
}

TEST(MacroExpanderTest,
     TracksCountOfUnexpandedTokensConsumedButNotFromDefinitions) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m 1 2 3", macro_catalog);

  LanguageOptions options = GetLanguageOptions(/*is_strict=*/false);

  auto token_provider = std::make_unique<FlexTokenProvider>(
      kTopFileName, "$m", /*start_offset=*/0,
      /*end_offset=*/std::nullopt);
  auto arena = std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/1024);
  MacroExpander expander(std::move(token_provider), options, macro_catalog,
                         arena.get(), DiagnosticOptions{},
                         /*parent_location=*/nullptr);

  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 15, 16),
                  "1", "", MakeLocation(0, 2)})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 17, 18),
                  "2", " ", MakeLocation(0, 2)})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(TokenWithLocation{
                  DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 19, 20),
                  "3", " ", MakeLocation(0, 2)})));
  ASSERT_THAT(expander.GetNextToken(),
              IsOkAndHolds(TokenIs(
                  TokenWithLocation{YYEOF, MakeLocation(2, 2), "", ""})));

  // We count 2 unexpanded tokens: $m and YYEOF. Tokens in $m's definition
  // do not count.
  EXPECT_EQ(expander.num_unexpanded_tokens_consumed(), 2);
}

TEST(MacroExpanderTest, ExpandsEmptyMacrosSplicedWithIntLiterals) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO empty ;\n"
      "DEFINE MACRO int 123;\n",
      macro_catalog);

  EXPECT_THAT(ExpandMacros("\n$empty()1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(9, 10), "1", "\n"},
                  {YYEOF, MakeLocation(10, 10), "", ""}})));

  EXPECT_THAT(ExpandMacros("\n$empty()$int", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  // Location is from the macro it was pulled from.
                  // We can stack it later for nested invocations.
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 38, 41),
                   "123", "\n", MakeLocation(9, 13)},
                  {YYEOF, MakeLocation(13, 13), "", ""}})));

  EXPECT_THAT(ExpandMacros("\n1$empty()", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(1, 2), "1", "\n"},
                  {YYEOF, MakeLocation(10, 10), "", ""}})));

  EXPECT_THAT(ExpandMacros("\n$int()$empty", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 38, 41),
                   "123", "\n", MakeLocation(1, 7)},
                  {YYEOF, MakeLocation(13, 13), "", ""}})));
}

TEST(MacroExpanderTest,
     ExpandsIdentifiersSplicedWithEmptyMacrosAndIntLiterals) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO identifier abc;\n"
      "DEFINE MACRO empty;\n"
      "DEFINE MACRO int 123;\n",
      macro_catalog);

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

  EXPECT_THAT(ExpandMacros("\n$identifier$empty()1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(kDefsFileName, 24, 27), "abc1",
                   "\n", MakeLocation(1, 12)},
                  {YYEOF, MakeLocation(21, 21), "", ""}})));

  EXPECT_THAT(ExpandMacros("\n$identifier$empty()$int", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {IDENTIFIER, MakeLocation(kDefsFileName, 24, 27), "abc123",
                   "\n", MakeLocation(1, 12)},
                  {YYEOF, MakeLocation(24, 24), "", ""}})));
}

TEST(MacroExpanderTest, CanExpandWithoutArgsAndNoSplicing) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO prefix xyz;\n"
      "DEFINE MACRO suffix1 123;\n"
      "DEFINE MACRO suffix2 456 abc;\n"
      "DEFINE MACRO empty ;\n",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros("select abc tbl_\t$empty+ $suffix1 $suffix2 $prefix\t",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 10), "abc", " "},
          {IDENTIFIER, MakeLocation(11, 15), "tbl_", " "},
          {'+', MakeLocation(22, 23), "+", "\t"},
          {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 46, 49), "123",
           " ", MakeLocation(24, 32)},
          {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 72, 75), "456",
           " ", MakeLocation(33, 41)},
          {IDENTIFIER, MakeLocation(kDefsFileName, 76, 79), "abc", " ",
           MakeLocation(33, 41)},
          {IDENTIFIER, MakeLocation(kDefsFileName, 20, 23), "xyz", " ",
           MakeLocation(42, 49)},
          {YYEOF, MakeLocation(50, 50), "", "\t"}})));
}

TEST(MacroExpanderTest, CanExpandWithoutArgsWithSplicing) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO identifier xyz;\n"
      "DEFINE MACRO numbers 123;\n"
      "DEFINE MACRO multiple_tokens 456 pq abc;\n"
      "DEFINE MACRO empty  ;\n",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros("select tbl_$numbers$multiple_tokens$empty$identifier "
                   "$empty$identifier$numbers$empty a+b",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {IDENTIFIER, MakeLocation(7, 11), "tbl_123456", " "},
          {IDENTIFIER, MakeLocation(kDefsFileName, 88, 90), "pq", " ",
           MakeLocation(19, 35)},
          {IDENTIFIER, MakeLocation(kDefsFileName, 91, 94), "abcxyz", " ",
           MakeLocation(19, 35)},
          {IDENTIFIER, MakeLocation(kDefsFileName, 24, 27), "xyz123", " ",
           MakeLocation(59, 70)},
          {IDENTIFIER, MakeLocation(85, 86), "a", " "},
          {'+', MakeLocation(86, 87), "+", ""},
          {IDENTIFIER, MakeLocation(87, 88), "b", ""},
          {YYEOF, MakeLocation(88, 88), "", ""},
      })));
}

TEST(MacroExpanderTest, KeywordsCanSpliceToFormIdentifiers) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO suffix 123;\n"
      "DEFINE MACRO empty    ;\n",
      macro_catalog);

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
  RegisterMacros("DEFINE MACRO m  a  ", macro_catalog);

  EXPECT_THAT(
      ExpandMacros("$m()b", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {IDENTIFIER, MakeLocation(kDefsFileName, 16, 17), "ab", "",
               MakeLocation(0, 4)},
              {YYEOF, MakeLocation(5, 5), "", ""}}),
          HasWarnings(ElementsAre(StatusIs(
              _, Eq("Splicing tokens (a) and (b) [at top_file.sql:1:5]")))))));
}

TEST(MacroExpanderTest, CanSuppressWarningOnIdentifierSplicing) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m  a  ", macro_catalog);

  EXPECT_THAT(ExpandMacros("$m()b", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false),
                           {.warn_on_identifier_splicing = false}),
              IsOkAndHolds(HasWarnings(IsEmpty())));
}

TEST(MacroExpanderTest, SpliceMacroInvocationWithIdentifier_Strict) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m  a  ", macro_catalog);

  std::vector<absl::Status> warnings;
  EXPECT_THAT(
      ExpandMacros("$m()b", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Splicing tokens (a) and (b) [at top_file.sql:1:5]")));
}

TEST(MacroExpanderTest, StrictProducesErrorOnIncompatibleQuoting) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO single_quoted 'sq'", macro_catalog);

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
  RegisterMacros("DEFINE MACRO single_quoted 'sq'", macro_catalog);

  ASSERT_THAT(
      ExpandMacros("select `ab$single_quoted`", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      StatusIs(_,
               Eq("Cannot expand a string literal(single "
                  "quote) into a quoted identifier. [at top_file.sql:1:11]")));
}

TEST(MacroExpanderTest, CanHandleUnicode) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO unicode '😀'", macro_catalog);

  EXPECT_THAT(ExpandMacros("'$😀$unicode'", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {STRING_LITERAL, MakeLocation(0, 15), "'$😀😀'", ""},
                  {YYEOF, MakeLocation(15, 15), "", ""},
              })));
}

TEST(MacroExpanderTest, ExpandsWithinQuotedIdentifiers_SingleToken) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO identifier xyz;\n"
      "DEFINE MACRO numbers 456;\n"
      "DEFINE MACRO inner_quoted_id `bq`;\n"
      "DEFINE MACRO empty     ",
      macro_catalog);

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
  RegisterMacros(
      "DEFINE MACRO identifier xyz;\n"
      "DEFINE MACRO numbers 456;\n"
      "DEFINE MACRO inner_quoted_id `bq`;\n"
      "DEFINE MACRO empty     ",
      macro_catalog);

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
  RegisterMacros(
      "DEFINE MACRO identifier xyz;\n"
      "DEFINE MACRO numbers 123 456;\n"
      "DEFINE MACRO inner_quoted_id `bq`;\n"
      "DEFINE MACRO empty     ",
      macro_catalog);

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
  RegisterMacros("DEFINE MACRO ints\t\t\t1 2\t\t\t", macro_catalog);

  EXPECT_THAT(
      ExpandMacros("  $x$y(a\n,\t$ints\n\t,\t$z,\n$w(\t\tb\n\n,\t\r)\t\n)   ",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {MACRO_INVOCATION, MakeLocation(2, 4), "$x", "  "},
          {MACRO_INVOCATION, MakeLocation(4, 6), "$y", ""},
          {'(', MakeLocation(6, 7), "(", ""},
          {IDENTIFIER, MakeLocation(7, 8), "a", ""},
          {',', MakeLocation(9, 10), ",", "\n"},
          {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 20, 21), "1",
           "\t", MakeLocation(11, 16)},
          {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 22, 23), "2",
           " ", MakeLocation(11, 16)},
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
  RegisterMacros("DEFINE MACRO m   bc   ;", macro_catalog);

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
                   Eq("Macro expansion in literals is deprecated. Strict mode "
                      "does not expand literals [at top_file.sql:1:2]")),
          StatusIs(_,
                   HasSubstr("Macro 'a' not found. [at top_file.sql:1:2]"))))));

  EXPECT_THAT(ExpandMacros("'$a('", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/false)),
              StatusIs(_, Eq("Nested macro argument lists inside literals are "
                             "not allowed [at top_file.sql:1:4]")));
}

TEST(MacroExpanderTest, SpliceArgWithIdentifier) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m $1a;", macro_catalog);

  EXPECT_THAT(
      ExpandMacros("$m(x)", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {IDENTIFIER, MakeLocation(3, 4), "xa", "", MakeLocation(0, 5)},
          {YYEOF, MakeLocation(5, 5), "", ""},
      })));
}

TEST(MacroExpanderTest, ExpandsArgs) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO select_list a , $1, $2  ;\n"
      "DEFINE MACRO from_clause FROM tbl_$1$empty;\n"
      "DEFINE MACRO splice    $1$2   ;\n"
      "DEFINE MACRO numbers 123 456;\n"
      "DEFINE MACRO inner_quoted_id `bq`;\n"
      "DEFINE MACRO empty      ;\n"
      "DEFINE MACRO MakeLiteral '$1';",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros("select $MakeLiteral(  x  ) $splice(  b  ,   89  ), "
                   "$select_list(  b, c )123 $from_clause(  123  ) ",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          {KW_SELECT, MakeLocation(0, 6), "select", ""},
          {STRING_LITERAL, MakeLocation(kDefsFileName, 231, 235), "'x'", " ",
           MakeLocation(7, 26)},
          {IDENTIFIER, MakeLocation(37, 38), "b89", " ", MakeLocation(27, 49)},
          {',', MakeLocation(49, 50), ",", ""},
          {IDENTIFIER, MakeLocation(kDefsFileName, 25, 26), "a", " ",
           MakeLocation(51, 72)},
          {',', MakeLocation(kDefsFileName, 27, 28), ",", " ",
           MakeLocation(51, 72)},
          {IDENTIFIER, MakeLocation(66, 67), "b", " ", MakeLocation(51, 72)},
          {',', MakeLocation(kDefsFileName, 31, 32), ",", "",
           MakeLocation(51, 72)},
          {IDENTIFIER, MakeLocation(69, 70), "c123", " ", MakeLocation(51, 72)},
          {KW_FROM, MakeLocation(kDefsFileName, 64, 68), "FROM", " ",
           MakeLocation(76, 97)},
          {IDENTIFIER, MakeLocation(kDefsFileName, 69, 73), "tbl_123", " ",
           MakeLocation(76, 97)},
          {YYEOF, MakeLocation(98, 98), "", " "},
      })));
}

TEST(MacroExpanderTest, ExpandsArgsThatHaveParensAndCommas) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO repeat $1, $1, $2, $2;", macro_catalog);

  EXPECT_THAT(ExpandMacros("select $repeat(1, (2,3))", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/true)),
              IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
                  {KW_SELECT, MakeLocation(0, 6), "select", ""},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(15, 16), "1", " ",
                   MakeLocation(7, 24)},
                  {',', MakeLocation(kDefsFileName, 22, 23), ",", "",
                   MakeLocation(7, 24)},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(15, 16), "1", " ",
                   MakeLocation(7, 24)},
                  {',', MakeLocation(kDefsFileName, 26, 27), ",", "",
                   MakeLocation(7, 24)},
                  {'(', MakeLocation(18, 19), "(", " ", MakeLocation(7, 24)},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(19, 20), "2", "",
                   MakeLocation(7, 24)},
                  {',', MakeLocation(20, 21), ",", "", MakeLocation(7, 24)},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(21, 22), "3", "",
                   MakeLocation(7, 24)},
                  {')', MakeLocation(22, 23), ")", "", MakeLocation(7, 24)},
                  {',', MakeLocation(kDefsFileName, 30, 31), ",", "",
                   MakeLocation(7, 24)},
                  {'(', MakeLocation(18, 19), "(", " ", MakeLocation(7, 24)},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(19, 20), "2", "",
                   MakeLocation(7, 24)},
                  {',', MakeLocation(20, 21), ",", "", MakeLocation(7, 24)},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(21, 22), "3", "",
                   MakeLocation(7, 24)},
                  {')', MakeLocation(22, 23), ")", "", MakeLocation(7, 24)},
                  {YYEOF, MakeLocation(24, 24), "", ""}})));
}

TEST(MacroExpanderTest, ExtraArgsProduceWarningOrError) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO empty;", macro_catalog);

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
  RegisterMacros(
      "DEFINE MACRO empty;\n"
      "DEFINE MACRO unknown_not_in_a_literal\n"
      "x$3\n"
      ";\n"
      "DEFINE MACRO unknown_inside_literal'\ty$3\t';\n",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros(
          "  a$empty()$1$unknown_not_in_a_literal  '$unknown_not_in_a_literal' "
          "$unknown_inside_literal\t$1\n'$2'",
          macro_catalog, GetLanguageOptions(/*is_strict=*/false),
          {.error_message_options =
               {.mode = ErrorMessageMode::ERROR_MESSAGE_ONE_LINE},
           .max_warning_count = 10}),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {IDENTIFIER, MakeLocation(2, 3), "a", "  "},
              {MACRO_ARGUMENT_REFERENCE, MakeLocation(11, 13), "$1", ""},
              {IDENTIFIER, MakeLocation(kDefsFileName, 58, 59), "x", "",
               MakeLocation(13, 38)},
              {STRING_LITERAL, MakeLocation(40, 67), "'x'", "  "},
              {STRING_LITERAL, MakeLocation(kDefsFileName, 99, 106), "'\ty\t'",
               " ", MakeLocation(68, 91)},
              {MACRO_ARGUMENT_REFERENCE, MakeLocation(92, 94), "$1", "\t"},
              {STRING_LITERAL, MakeLocation(95, 99), "'$2'", "\n"},
              {YYEOF, MakeLocation(99, 99), "", ""}}),
          HasWarnings(ElementsAre(
              StatusIs(_, Eq("Invocation of macro 'unknown_not_in_a_literal' "
                             "missing argument list. [at top_file.sql:1:39]")),
              StatusIs(_, Eq("Argument index $3 out of range. Invocation was "
                             "provided only 0 arguments. [at defs.sql:3:2]; "
                             "Expanded from macro:unknown_not_in_a_literal [at "
                             "top_file.sql:1:14]")),
              StatusIs(
                  _,
                  Eq("Macro expansion in literals is deprecated. Strict mode "
                     "does not expand literals [at top_file.sql:1:42]")),
              StatusIs(_, Eq("Invocation of macro 'unknown_not_in_a_literal' "
                             "missing argument list. [at top_file.sql:1:67]")),
              StatusIs(_, Eq("Argument index $3 out of range. Invocation was "
                             "provided only 0 arguments. [at defs.sql:3:2]; "
                             "Expanded from macro:unknown_not_in_a_literal [at "
                             "top_file.sql:1:42]")),
              StatusIs(_, Eq("Invocation of macro 'unknown_inside_literal' "
                             "missing argument list. [at top_file.sql:1:92]")),
              StatusIs(_,
                       Eq("Macro expansion in literals is deprecated. Strict "
                          "mode does not expand literals [at defs.sql:5:42]; "
                          "Expanded from macro:unknown_inside_literal [at "
                          "top_file.sql:1:69]")),
              StatusIs(_, Eq("Argument index $3 out of range. Invocation was "
                             "provided only 0 arguments. [at defs.sql:5:42]; "
                             "Expanded from macro:unknown_inside_literal [at "
                             "top_file.sql:1:69]")),
              StatusIs(
                  _,
                  Eq("Macro expansion in literals is deprecated. Strict mode "
                     "does not expand literals [at top_file.sql:2:2]")))))));

  EXPECT_THAT(
      ExpandMacros("unknown_inside_literal() $unknown_not_in_a_literal()",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Argument index $3 out of range. Invocation was provided "
                     "only 1 arguments. [at defs.sql:3:2]; "
                     "Expanded from macro:unknown_not_in_a_literal [at "
                     "top_file.sql:1:26]")));
}

TEST(MacroExpanderTest, ExpandsStandaloneDollarSignsAtTopLevel) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO empty\n\n;", macro_catalog);

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
  RegisterMacros("DEFINE MACRO m 1;", macro_catalog);

  EXPECT_THAT(
      ExpandMacros("$m", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 15, 16),
               "1", "", MakeLocation(0, 2)},
              {YYEOF, MakeLocation(2, 2), "", ""}}),
          HasWarnings(ElementsAre(
              StatusIs(_, Eq("Invocation of macro 'm' missing argument list. "
                             "[at top_file.sql:1:3]")))))));

  EXPECT_THAT(
      ExpandMacros("$m", macro_catalog, GetLanguageOptions(/*is_strict=*/true)),
      StatusIs(_, Eq("Invocation of macro 'm' missing argument list. [at "
                     "top_file.sql:1:3]")));
}

TEST(MacroExpanderTest, WarningsAndErrorsAccountForExternalOffsets) {
  MacroCatalog macro_catalog;
  RegisterMacros(R"(   #line 1
   -- line 2
DEFINE MACRO one 1;
DEFINE MACRO repeat $1, $1;
# extra line
/*indent a bit*/ DEFINE MACRO m $repeat( /*stuff*/ $one  --more stuff
  );
)",
                 macro_catalog);

  EXPECT_THAT(
      ExpandMacros("$m()", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(AllOf(
          TokensEq(std::vector<TokenWithLocation>{
              {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 41, 42),
               "1", "", MakeLocation(0, 4)},
              {',', MakeLocation(kDefsFileName, 66, 67), ",", "",
               MakeLocation(0, 4)},
              {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 41, 42),
               "1", " ", MakeLocation(0, 4)},
              {YYEOF, MakeLocation(4, 4), "", ""}}),
          HasWarnings(ElementsAre(StatusIs(
              _, Eq("Invocation of macro 'one' missing argument list. [at "
                    "defs.sql:6:56]; "
                    "Expanded from macro:m [at top_file.sql:1:1]")))))));

  EXPECT_THAT(ExpandMacros("$m()", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/true)),
              StatusIs(_, Eq("Invocation of macro 'one' missing argument list. "
                             "[at defs.sql:6:56]; "
                             "Expanded from macro:m [at top_file.sql:1:1]")));
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
      return DECIMAL_INTEGER_LITERAL;
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

  std::string macro_def = QuoteText("1", inner_quoting);
  MacroCatalog catalog;
  absl::string_view macro_prefix = "DEFINE MACRO a ";
  std::string macro_def_source = absl::StrCat(macro_prefix, macro_def);
  RegisterMacros(macro_def_source, catalog);

  absl::string_view invocation = "$a";
  std::string unexpanded_sql = QuoteText(invocation, outer_quoting);
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
    size_t outer_quote_length = (unexpanded_sql.length() - invocation.length() -
                                 outer_quoting.prefix().length()) /
                                2;
    int unexpanded_start_offset =
        static_cast<int>(outer_quoting.prefix().length() + outer_quote_length);
    EXPECT_THAT(
        ExpandMacros(unexpanded_sql, catalog,
                     GetLanguageOptions(/*is_strict=*/false)),
        IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
            {final_token_kind, MakeLocation(0, unexpanded_length),
             QuoteText("", outer_quoting), ""},
            {final_token_kind,
             MakeLocation(kDefsFileName,
                          static_cast<int>(macro_prefix.length()),
                          static_cast<int>(macro_def_source.length())),
             macro_def, " ",
             MakeLocation(unexpanded_start_offset,
                          unexpanded_start_offset +
                              static_cast<int>(invocation.length()))},
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

  std::string macro_def =
      absl::StrCat("DEFINE MACRO a ", QuoteText("1", inner_quoting));
  MacroCatalog catalog;
  RegisterMacros(macro_def, catalog);

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
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO select_list a , $1, $2  ;\n"
      "DEFINE MACRO splice_invoke    $$1$2   ;\n"
      "DEFINE MACRO inner_quoted_id `bq`;\n"
      "DEFINE MACRO empty      ;\n",
      macro_catalog);

  EXPECT_THAT(
      ExpandMacros("$splice_invoke(  inner_\t,\t\tquoted_id   ), "
                   "$$select_list(  b, c  )123\t",
                   macro_catalog, GetLanguageOptions(/*is_strict=*/false)),
      IsOkAndHolds(TokensEq(std::vector<TokenWithLocation>{
          // Note: the dollar sign is not spliced with the identifier into a new
          // macro invocation token, because we do not reexpand.
          {DOLLAR_SIGN, MakeLocation(kDefsFileName, 69, 70), "$", "",
           MakeLocation(0, 40)},
          {IDENTIFIER, MakeLocation(17, 23), "inner_quoted_id", "",
           MakeLocation(0, 40)},
          {',', MakeLocation(40, 41), ",", ""},
          {DOLLAR_SIGN, MakeLocation(42, 43), "$", " "},
          {IDENTIFIER, MakeLocation(kDefsFileName, 25, 26), "a", "",
           MakeLocation(43, 65)},
          {',', MakeLocation(kDefsFileName, 27, 28), ",", " ",
           MakeLocation(43, 65)},
          {IDENTIFIER, MakeLocation(58, 59), "b", " ", MakeLocation(43, 65)},
          // The next comma originates from the macro def
          {',', MakeLocation(kDefsFileName, 31, 32), ",", "",
           MakeLocation(43, 65)},
          {IDENTIFIER, MakeLocation(61, 62), "c123", " ", MakeLocation(43, 65)},
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
  ASSERT_EQ(result.expanded_tokens.size(), num_slashes + 1);
  for (int i = 0; i < num_slashes - 1; ++i) {
    EXPECT_EQ(result.expanded_tokens[i].kind, TokenKinds::BACKSLASH);
  }
  EXPECT_EQ(result.expanded_tokens.back().kind, YYEOF);

  // The number of warnings is the cap (5) + 1 for the sentinel indicating that
  // more warnings were truncated.
  EXPECT_EQ(result.warnings.size(), 6);
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
  EXPECT_EQ(TokensToString(tokens), "\ta FROM\n");
}

TEST(TokensToStringTest, DoesNotSpliceTokensEvenWhenNoOriginalSpacesExist) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken(IDENTIFIER, "a", "\t"),
      MakeToken(KW_FROM, "FROM"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "0x1A"),
      MakeToken(IDENTIFIER, "b", "\t"),
      MakeToken(KW_FROM, "FROM"),
      MakeToken(FLOATING_POINT_LITERAL, "1."),
      MakeToken(IDENTIFIER, "x", "\t"),
      MakeToken(MACRO_INVOCATION, "$a"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "123"),
      MakeToken(MACRO_INVOCATION, "$1"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "23"),
      MakeToken('*', "*"),
      MakeToken(YYEOF, "", "\n")};

  // Note the forced spaces
  EXPECT_EQ(TokensToString(tokens),
            "\ta FROM 0x1A\tb FROM 1.\tx$a 123$1 23*\n");
}

TEST(TokensToStringTest, DoesNotCauseCommentOuts) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken('-', "-"), MakeToken('-', "-"), MakeToken('/', "/"),
      MakeToken('/', "/"), MakeToken('/', "/"), MakeToken('*', "*"),
      MakeToken(YYEOF, "")};

  // Note the forced spaces, except for -/
  EXPECT_EQ(TokensToString(tokens), "- -/ / / *");
}

TEST(TokensToStringTest, AlwaysSeparatesNumericLiterals) {
  std::vector<TokenWithLocation> tokens = {
      MakeToken(DECIMAL_INTEGER_LITERAL, "0x1"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "2"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "3"),
      MakeToken(FLOATING_POINT_LITERAL, "4."),
      MakeToken(DECIMAL_INTEGER_LITERAL, "5"),
      MakeToken(FLOATING_POINT_LITERAL, ".6"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "7"),
      MakeToken(FLOATING_POINT_LITERAL, ".8e9"),
      MakeToken(DECIMAL_INTEGER_LITERAL, "10"),
      MakeToken(STRING_LITERAL, "'11'"),
      MakeToken(YYEOF, "")};

  // Note the forced spaces
  EXPECT_EQ(TokensToString(tokens), "0x1 2 3 4. 5 .6 7 .8e9 10'11'");
}

class MacroExpanderLenientTokensTest
    : public ::testing::TestWithParam<absl::string_view> {};

TEST_P(MacroExpanderLenientTokensTest, LenientTokensAllowedOnlyWhenLenient) {
  absl::string_view text = GetParam();
  std::string macro_def = absl::StrCat("DEFINE MACRO m ", text);
  MacroCatalog macro_catalog;
  RegisterMacros(macro_def, macro_catalog);

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
                         Values("\\"));

struct LiteralsFollowedByAdjacentIdentifiersTestCase {
  // The macro body text, for example ".123a".
  absl::string_view text;
  // Whether to enable the STRICT macro mode.
  bool is_strict;
  // The expected number of tokens corresponding to `text`.
  int token_count;
  // The expected number of tokens corresponding to `absl::StrCat("a", text)`.
  // This is to test the identifier splicing during macro expansions.
  int token_count_after_splicing;
};

class LiteralsFollowedByAdjacentIdentifiersTest
    : public ::testing::TestWithParam<
          LiteralsFollowedByAdjacentIdentifiersTestCase> {};

TEST_P(LiteralsFollowedByAdjacentIdentifiersTest,
       LiteralsFollowedByAdjacentIdentifiers) {
  absl::string_view text = GetParam().text;
  bool is_strict = GetParam().is_strict;
  int expected_token_count = GetParam().token_count;
  int expected_token_count_after_splicing =
      GetParam().token_count_after_splicing;

  MacroCatalog macro_catalog;
  std::string macro_def = absl::StrCat("DEFINE MACRO m ", text);
  RegisterMacros(macro_def, macro_catalog);
  LanguageOptions language_options = GetLanguageOptions(is_strict);

  EXPECT_THAT(ExpandMacros(text, macro_catalog, language_options),
              IsOkAndHolds(HasTokens(SizeIs(expected_token_count))));

  EXPECT_THAT(ExpandMacros("$m()", macro_catalog, language_options),
              IsOkAndHolds(HasTokens(SizeIs(expected_token_count))));

  // Test identifier splicing.
  EXPECT_THAT(
      ExpandMacros("a$m()", macro_catalog, language_options),
      IsOkAndHolds(HasTokens(SizeIs(expected_token_count_after_splicing))));
}

std::vector<LiteralsFollowedByAdjacentIdentifiersTestCase>
GetLiteralsFollowedByAdjacentIdentifiersTestCases() {
  std::vector<LiteralsFollowedByAdjacentIdentifiersTestCase> cases;
  for (bool is_strict : {false, true}) {
    cases.push_back({
        .text = ".4dab",
        .is_strict = is_strict,
        // Tokens are: . 4 dab YYEOF
        .token_count = 4,
        // After prepending "a", tokens are: a . 4 dab YYEOF
        .token_count_after_splicing = 5,
    });
  }
  // Identifier splicing is only allowed under lenient mode.
  cases.push_back({
      .text = "3d",
      .is_strict = false,
      // Tokens are: 3 d YYEOF
      .token_count = 3,
      // After prepending "a", tokens are: a3 d YYEOF
      .token_count_after_splicing = 3,
  });
  cases.push_back({
      .text = "12abc",
      .is_strict = false,
      // Tokens are: 12 abc YYEOF
      .token_count = 3,
      // After prepending "a", tokens are: a12 abc YYEOF
      .token_count_after_splicing = 3,
  });
  cases.push_back({
      .text = "0x123xyz",
      .is_strict = false,
      // Tokens are: 0x123 xyz YYEOF
      .token_count = 3,
      // After prepending "a", tokens are: a0x123 xyz YYEOF
      .token_count_after_splicing = 3,
  });
  cases.push_back({
      .text = "3.4dab",
      .is_strict = false,
      // Tokens are: 3 . 4 dab YYEOF
      .token_count = 5,
      // After prepending "a", tokens are: a3 . 4 dab YYEOF
      .token_count_after_splicing = 5,
  });
  cases.push_back({
      .text = "1.2e3ab",
      .is_strict = false,
      // Tokens are: 1 . 2 e3ab YYEOF
      .token_count = 5,
      // After prepending "a", tokens are: a1 . 2e3 ab YYEOF
      .token_count_after_splicing = 5,
  });
  cases.push_back({
      .text = "1.2e-3ab",
      .is_strict = false,
      // Tokens are: 1 . 2 e - 3 ab YYEOF
      .token_count = 8,
      // After prepending "a", tokens are: a1 . 2 e - 3 ab YYEOF
      .token_count_after_splicing = 8,
  });
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    LiteralsFollowedByAdjacentIdentifiers,
    LiteralsFollowedByAdjacentIdentifiersTest,
    ValuesIn(GetLiteralsFollowedByAdjacentIdentifiersTestCases()));

struct SplicingNotAllowedTestCase {
  absl::string_view macro_def;
  absl::string_view text;
  absl::string_view error_message;
};

std::vector<SplicingNotAllowedTestCase> GetSplicingNotAllowedTestCases() {
  return {
      {
          .macro_def = "DEFINE MACRO m 3d",
          .text = "a$m()",
          .error_message = "Splicing tokens (a) and (3)",
      },
      {
          .macro_def = "DEFINE MACRO m 12abc",
          .text = "a$m()",
          .error_message = "Splicing tokens (a) and (12)",
      },
      {
          .macro_def = "DEFINE MACRO m 0x123xyz",
          .text = "a$m()",
          .error_message = "Splicing tokens (a) and (0x123)",
      },
  };
}

class SplicingNotAllowedUnderStrictModeTest
    : public ::testing::TestWithParam<SplicingNotAllowedTestCase> {};

TEST_P(SplicingNotAllowedUnderStrictModeTest,
       SplicingNotAllowedUnderStrictMode) {
  LanguageOptions language_options = GetLanguageOptions(/*is_strict=*/true);
  MacroCatalog macro_catalog;
  RegisterMacros(GetParam().macro_def, macro_catalog);
  EXPECT_THAT(ExpandMacros(GetParam().text, macro_catalog, language_options),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(GetParam().error_message)));
}

INSTANTIATE_TEST_SUITE_P(SplicingNotAllowedUnderStrictMode,
                         SplicingNotAllowedUnderStrictModeTest,
                         ValuesIn(GetSplicingNotAllowedTestCases()));

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
  RegisterMacros(
      "DEFINE MACRO x a;\n"
      "DEFINE MACRO y b;\n",
      macro_catalog);

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
  RegisterMacros("DEFINE MACRO x a;\n", macro_catalog);

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
                      {IDENTIFIER, MakeLocation(kDefsFileName, 15, 16), "a",
                       " ", MakeLocation(20, 24)},
                      {DECIMAL_INTEGER_LITERAL, MakeLocation(25, 26), "1", " "},
                      {YYEOF, MakeLocation(26, 26), "", ""},
                  }),
                  HasWarnings(IsEmpty()))));
}

TEST_P(MacroExpanderParameterizedTest,
       DoesNotRecognizeGeneratedDefineMacroStatements) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO def DEFINE MACRO;\n", macro_catalog);

  EXPECT_THAT(ExpandMacros("$def() a 1", macro_catalog,
                           GetLanguageOptions(/*is_strict=*/GetParam())),
              IsOkAndHolds(AllOf(
                  TokensEq(std::vector<TokenWithLocation>{
                      // Note that the first token is KW_DEFINE, not
                      // KW_DEFINE_FOR_MACROS
                      {KW_DEFINE, MakeLocation(kDefsFileName, 17, 23), "DEFINE",
                       "", MakeLocation(0, 6)},
                      {KW_MACRO, MakeLocation(kDefsFileName, 24, 29), "MACRO",
                       " ", MakeLocation(0, 6)},
                      {IDENTIFIER, MakeLocation(7, 8), "a", " "},
                      {DECIMAL_INTEGER_LITERAL, MakeLocation(9, 10), "1", " "},
                      {YYEOF, MakeLocation(10, 10), "", ""},
                  }),
                  HasWarnings(IsEmpty()))));
}

static void PrintTo(const Expansion& expansion, std::ostream* os) {
  *os << absl::StrFormat("{macro_name: %s, invocation: %s, expansion: %s}",
                         expansion.macro_name, expansion.full_match,
                         expansion.expansion);
}

TEST_P(MacroExpanderParameterizedTest, GeneratesCorrectLocationMap) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO a\n"
      "$b(  /*nothing*/  )\n"
      ";\n"
      "DEFINE MACRO b\n"
      "1;\n",
      macro_catalog);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("\t\t$a( /*just a comment*/  )\t\t", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())));
  EXPECT_THAT(output,
              TokensEq(std::vector<TokenWithLocation>{
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 52, 53),
                   "1", "\t\t", MakeLocation(2, 27)},
                  {YYEOF, MakeLocation(29, 29), "", "\t\t"}}));
  EXPECT_EQ(TokensToString(output.expanded_tokens), "\t\t1\t\t");

  // Only the top-level expansions are stored in the map.
  EXPECT_THAT(
      output.location_map,
      ElementsAre(Pair(2, FieldsAre("a", "$a( /*just a comment*/  )", "1"))));
}

TEST_P(MacroExpanderParameterizedTest, TokensToStringTwoGreaterThanSymbols) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO gt >;\n"
      "DEFINE MACRO right_shift >>;\n"
      "DEFINE MACRO splice $1$2;\n"
      "DEFINE MACRO repeat $1 $1\n",
      macro_catalog);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("1 $gt()$gt() 2 $right_shift() 3 $splice(>,>) 4 $repeat(>>)",
                   macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())));
  EXPECT_EQ(TokensToString(output.expanded_tokens), "1 > > 2 >> 3 > > 4 >> >>");
}

TEST_P(MacroExpanderParameterizedTest, TokensToStringAdjacentTokens) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m a+b;", macro_catalog);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("$m()", macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())));
  EXPECT_EQ(TokensToString(output.expanded_tokens), "a+b");
}

TEST_P(MacroExpanderParameterizedTest, GeneratesCorrectLocationMap_Multiple) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO macro xyz;\n"
      "DEFINE MACRO macrowithargs $3, $2, $1, $2;\n"
      "DEFINE MACRO macrowithmacro owner.$macro();\n"
      "DEFINE MACRO limit 10;\n",
      macro_catalog);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("select $macro(), $macrowithargs(a, b, c) from "
                   "$macrowithmacro() limit $limit()",
                   macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())));
  EXPECT_EQ(TokensToString(output.expanded_tokens),
            "select xyz, c, b, a, b from owner.xyz limit 10");

  // Only the top-level expansions are stored in the map.
  EXPECT_THAT(
      output.location_map,
      ElementsAre(Pair(7, FieldsAre("macro", "$macro()", "xyz")),
                  Pair(17, FieldsAre("macrowithargs", "$macrowithargs(a, b, c)",
                                     "c, b, a, b")),
                  Pair(46, FieldsAre("macrowithmacro", "$macrowithmacro()",
                                     "owner.xyz")),
                  Pair(70, FieldsAre("limit", "$limit()", "10"))));
}

TEST_P(MacroExpanderParameterizedTest,
       EmptyArgumentAcceptedWhenMacroAcceptsZeroOrSingleArgument) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO no_args 'nullary';\n"
      "DEFINE MACRO single_arg $1;\n",
      macro_catalog);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("$no_args( /*comment1*/   /*comment2*/)\n"
                   "$single_arg(  /*comment3*/  /*comment4*/)",
                   macro_catalog,
                   GetLanguageOptions(/*is_strict=*/GetParam())));
  EXPECT_THAT(output, TokensEq(std::vector<TokenWithLocation>{
                          {STRING_LITERAL, MakeLocation(kDefsFileName, 21, 30),
                           "'nullary'", "", MakeLocation(0, 38)},
                          {YYEOF, MakeLocation(80, 80), "", "\n"},
                      }));
  EXPECT_EQ(TokensToString(output.expanded_tokens), "'nullary'\n");
}

TEST_P(
    MacroExpanderParameterizedTest,
    CorrectErrorWhenSingleArgumentPassedToNullaryMacroEvenWhenExpandsToEmpty) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO empty;\n"
      "DEFINE MACRO no_args 'nullary';\n",
      macro_catalog);

  bool is_strict = GetParam();
  absl::StatusOr<ExpansionOutput> output =
      ExpandMacros("$no_args( /*comment1*/ $empty()   /*comment2*/)\n",
                   macro_catalog, GetLanguageOptions(is_strict));

  absl::string_view expected_diagnostic =
      "Macro invocation has too many arguments (1) while the definition only "
      "references up to 0 arguments [at top_file.sql:1:1]";

  if (is_strict) {
    EXPECT_THAT(output, StatusIs(_, HasSubstr(expected_diagnostic)));
  } else {
    EXPECT_THAT(output,
                IsOkAndHolds(AllOf(
                    TokensEq(std::vector<TokenWithLocation>{
                        {STRING_LITERAL, MakeLocation(kDefsFileName, 41, 50),
                         "'nullary'", "", MakeLocation(0, 47)},
                        {YYEOF, MakeLocation(48, 48), "", "\n"},
                    }),
                    HasWarnings(ElementsAre(
                        StatusIs(_, HasSubstr(expected_diagnostic)))))));
  }
}

TEST_P(MacroExpanderParameterizedTest,
       TokensToStringForcesSpaceToAvoidCommentOuts) {
  MacroCatalog macro_catalog;
  RegisterMacros(
      "DEFINE MACRO forward_slash /;\n"
      "DEFINE MACRO star *;\n",
      macro_catalog);

  ZETASQL_ASSERT_OK_AND_ASSIGN(ExpansionOutput output,
                       ExpandMacros("$forward_slash()$star()", macro_catalog,
                                    GetLanguageOptions(GetParam())));
  EXPECT_EQ(TokensToString(output.expanded_tokens), "/ *");
}

TEST_P(MacroExpanderParameterizedTest, RightShift) {
  MacroCatalog macro_catalog;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros(">>", macro_catalog, GetLanguageOptions(GetParam())));
  EXPECT_THAT(output, TokensEq(std::vector<TokenWithLocation>{
                          {'>', MakeLocation(0, 1), ">", ""},
                          {'>', MakeLocation(1, 2), ">", ""},
                          {YYEOF, MakeLocation(2, 2), "", ""},
                      }));
  EXPECT_EQ(TokensToString(output.expanded_tokens), ">>");
}

TEST_P(MacroExpanderParameterizedTest, TopLevelCommentsArePreserved) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m /* dropped_comment */ 1;", macro_catalog);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros("/* preserved_comment */ $m()", macro_catalog,
                   GetLanguageOptions(GetParam())));
  EXPECT_THAT(output,
              TokensEq(std::vector<TokenWithLocation>{
                  {COMMENT, MakeLocation(0, 23), "/* preserved_comment */", ""},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(kDefsFileName, 37, 38),
                   "1", " ", MakeLocation(24, 28)},
                  {YYEOF, MakeLocation(28, 28), "", ""},
              }));
}

TEST_P(MacroExpanderParameterizedTest,
       TopLevelCommentsArePreservedExpandingDefineMacroStatements) {
  MacroCatalog macro_catalog;
  RegisterMacros("DEFINE MACRO m1 /*internal comment*/ 123;", macro_catalog);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ExpansionOutput output,
      ExpandMacros(
          "select 1; /*comment 1*/ /*another comment*/ DEFINE MACRO m2 "
          "/*comment 2*/ $m1",
          macro_catalog, GetLanguageOptions(GetParam())));

  EXPECT_THAT(output,
              TokensEq(std::vector<TokenWithLocation>{
                  {KW_SELECT, MakeLocation(0, 6), "select", ""},
                  {DECIMAL_INTEGER_LITERAL, MakeLocation(7, 8), "1", " "},
                  {';', MakeLocation(8, 9), ";", ""},
                  {COMMENT, MakeLocation(10, 23), "/*comment 1*/", " "},
                  {COMMENT, MakeLocation(24, 43), "/*another comment*/", " "},
                  {KW_DEFINE_FOR_MACROS, MakeLocation(44, 50), "DEFINE", " "},
                  {KW_MACRO, MakeLocation(51, 56), "MACRO", " "},
                  {IDENTIFIER, MakeLocation(57, 59), "m2", " "},
                  {COMMENT, MakeLocation(60, 73), "/*comment 2*/", " "},
                  {MACRO_INVOCATION, MakeLocation(74, 77), "$m1", " "},
                  {YYEOF, MakeLocation(77, 77), "", ""},
              }));
}

INSTANTIATE_TEST_SUITE_P(MacroExpanderParameterizedTest,
                         MacroExpanderParameterizedTest, Bool());

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
