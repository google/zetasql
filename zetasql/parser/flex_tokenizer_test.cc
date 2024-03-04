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

#include "zetasql/parser/flex_tokenizer.h"

#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/token_disambiguator.h"
#include "zetasql/public/language_options.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql::parser {

using ::testing::ElementsAre;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

using Token = TokenKinds;
using TokenKind = int;
using Location = ZetaSqlFlexTokenizer::Location;

// This class is a friend of the tokenizer so that it can help us test the
// private API.
class TokenTestThief {
 public:
  static TokenKind Lookahead1(DisambiguatorLexer& lexer,
                              const Location& location) {
    return lexer.Lookahead1(location);
  }
  static TokenKind Lookahead2(DisambiguatorLexer& lexer,
                              const Location& location) {
    return lexer.Lookahead2(location);
  }
};

class FlexTokenizerTest : public ::testing::Test {
 public:
  std::vector<TokenKind> GetAllTokens(BisonParserMode mode,
                                      absl::string_view sql) {
    auto tokenizer = DisambiguatorLexer::Create(
        mode, "fake_file", sql, 0, options_, /*macro_catalog=*/nullptr,
        /*arena=*/nullptr);
    ZETASQL_DCHECK_OK(tokenizer);
    Location location;
    std::vector<TokenKind> tokens;
    do {
      absl::string_view text;
      tokens.emplace_back(tokenizer.value()->GetNextToken(&text, &location));
    } while (tokens.back() != Token::YYEOF);
    return tokens;
  }

 protected:
  LanguageOptions options_;
};

TEST_F(FlexTokenizerTest, ParameterKeywordStatementMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kStatement, "a @select c"),
              ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, '@',
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(FlexTokenizerTest, ParameterKeywordTokenizerMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kTokenizer, "a @select c"),
              ElementsAre(Token::IDENTIFIER, '@', Token::IDENTIFIER,
                          Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(FlexTokenizerTest, SysvarKeywordStatementMode) {
  EXPECT_THAT(
      GetAllTokens(BisonParserMode::kStatement, "a @@where c"),
      ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, Token::KW_DOUBLE_AT,
                  Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(FlexTokenizerTest, SysvarKeywordTokenizerMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kTokenizer, "a @@where c"),
              ElementsAre(Token::IDENTIFIER, Token::KW_DOUBLE_AT,
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(FlexTokenizerTest, QueryParamCurrentDate) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kStatement, "a @current_date c"),
              ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, '@',
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(FlexTokenizerTest, SysvarWithDotId) {
  EXPECT_THAT(
      GetAllTokens(BisonParserMode::kStatement, "SELECT @@ORDER.WITH.c"),
      ElementsAre(Token::MODE_STATEMENT, Token::KW_SELECT, Token::KW_DOUBLE_AT,
                  Token::IDENTIFIER, '.',
                  // The dot identifier mini-tokenizer needs to kick in after
                  // the disambiguation of ORDER to identifier.
                  Token::IDENTIFIER, '.', Token::IDENTIFIER, Token::YYEOF));
}

absl::StatusOr<TokenKind> GetNextToken(DisambiguatorLexer& tokenizer,
                                       Location& location) {
  TokenKind token_kind;
  ZETASQL_RETURN_IF_ERROR(tokenizer.GetNextToken(&location, &token_kind));
  return token_kind;
}

TEST_F(FlexTokenizerTest, Lookahead1) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));
  Location location;
  DisambiguatorLexer& tokenizer = *lexer;
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location),
            Token::INTEGER_LITERAL);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::INTEGER_LITERAL));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::KW_SELECT);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  // TODO: b/324273431 - This should not produce an error.
  EXPECT_THAT(GetNextToken(tokenizer, location),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Internal error: Encountered real EOF"));
}

TEST_F(FlexTokenizerTest, Lookahead1WithForceTerminate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr,
                      /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location),
            Token::INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
}

TEST_F(FlexTokenizerTest, Lookahead2) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location),
            Token::INTEGER_LITERAL);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location),
            Token::INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::KW_SELECT);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::INTEGER_LITERAL));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::KW_SELECT);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);

  // TODO: b/324273431 - This should not produce an error.
  EXPECT_THAT(GetNextToken(tokenizer, location),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Internal error: Encountered real EOF"));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);
}

TEST_F(FlexTokenizerTest, Lookahead2BeforeLookahead1) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  // Calling Lookahead2 before Lookahead1 returns the correct token.
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location),
            Token::INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);

  // Repeated calling Lookahead2 returns the same token.
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location),
            Token::INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location),
            Token::INTEGER_LITERAL);
}

TEST_F(FlexTokenizerTest, Lookahead2NoEnoughTokens) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "", 0, options_,
                      /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);

  // TODO: b/324273431 - This should not produce an error.
  EXPECT_THAT(GetNextToken(tokenizer, location),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Internal error: Encountered real EOF"));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);
}

TEST_F(FlexTokenizerTest, Lookahead2ForceTerminate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, DisambiguatorLexer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location),
            Token::INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // After the force termination both lookaheads return YYEOF.
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);

  // Fetching more tokens returns YYEOF.
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer, location), Token::YYEOF);
}

TEST_F(FlexTokenizerTest, DisambiguatorReturnsYyeofWhenErrors) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer,
      DisambiguatorLexer::Create(BisonParserMode::kStatement, "fake_file",
                                 "SELECT * EXCEPT 1", 0, options_,
                                 /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  DisambiguatorLexer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds('*'));

  int token_kind;
  absl::Status status = tokenizer.GetNextToken(&location, &token_kind);
  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          R"err_msg(EXCEPT must be followed by ALL, DISTINCT, or "(")err_msg"));
  // The returned token should be YYEOF rather than KW_EXCEPT because an error
  // is produced.
  EXPECT_EQ(token_kind, Token::YYEOF);
}

}  // namespace zetasql::parser
