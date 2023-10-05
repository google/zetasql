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

#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql::parser {

using ::testing::ElementsAre;

using Token = TokenKinds;
using TokenKind = int;

// This class is a friend of the tokenizer so that it can help us test the
// private API.
class TokenTestThief {
 public:
  static TokenKind Lookahead1(
      ZetaSqlFlexTokenizer& tokenizer,
      const ZetaSqlFlexTokenizer::Location& location) {
    return tokenizer.Lookahead1(location);
  }
};

class FlexTokenizerTest : public ::testing::Test {
 public:
  std::vector<TokenKind> GetAllTokens(BisonParserMode mode,
                                      absl::string_view sql) {
    ZetaSqlFlexTokenizer tokenizer(mode, "fake_file", sql, 0, options_);
    ZetaSqlFlexTokenizer::Location location;
    std::vector<TokenKind> tokens;
    do {
      tokens.emplace_back(tokenizer.GetNextTokenFlex(&location));
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
              ElementsAre(Token::IDENTIFIER, '@', Token::KW_SELECT,
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
                          Token::KW_WHERE, Token::IDENTIFIER, Token::YYEOF));
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

TEST_F(FlexTokenizerTest, Lookahead1) {
  ZetaSqlFlexTokenizer tokenizer(BisonParserMode::kStatement, "fake_file",
                                   "a 1 SELECT", 0, options_);
  ZetaSqlFlexTokenizer::Location location;
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::MODE_STATEMENT);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);

  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location),
            Token::INTEGER_LITERAL);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::KW_SELECT);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::KW_SELECT);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::YYEOF);

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::YYEOF);
}

TEST_F(FlexTokenizerTest, Lookahead1WithForceTerminate) {
  ZetaSqlFlexTokenizer tokenizer(BisonParserMode::kStatement, "fake_file",
                                   "a 1 SELECT", 0, options_);
  ZetaSqlFlexTokenizer::Location location;
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::MODE_STATEMENT);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::IDENTIFIER);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location),
            Token::INTEGER_LITERAL);
  tokenizer.SetForceTerminate();
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::YYEOF);

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer, location), Token::YYEOF);
  EXPECT_EQ(tokenizer.GetNextTokenFlex(&location), Token::YYEOF);
}

}  // namespace zetasql::parser
