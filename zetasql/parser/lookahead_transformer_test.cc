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

#include "zetasql/parser/lookahead_transformer.h"

#include <memory>
#include <optional>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/parse_resume_location.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql::parser {

using ::zetasql::parser::macros::TokenWithLocation;
using ::testing::ContainerEq;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Field;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::testing::Optional;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

using Token = TokenKinds;
using TokenKind = int;
using Location = ParseLocationRange;

// This class is a friend of the tokenizer so that it can help us test the
// private API.
class TokenTestThief {
 public:
  static TokenKind Lookahead1(LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.Lookahead1();
  }
  static TokenKind Lookahead2(LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.Lookahead2();
  }
  static TokenKind Lookahead3(LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.Lookahead3();
  }
  static TokenKind Lookback1(LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.Lookback1();
  }
  static TokenKind Lookback2(LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.Lookback2();
  }

  static std::optional<TokenWithOverrideError> GetCurrentToken(
      const LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.current_token_;
  }

  static std::optional<TokenWithOverrideError> GetPreviousToken(
      const LookaheadTransformer& lookahead_transformer) {
    return lookahead_transformer.lookback_1_;
  }
};

class LookaheadTransformerTest : public ::testing::Test {
 public:
  std::vector<TokenKind> GetAllTokens(BisonParserMode mode,
                                      absl::string_view sql) {
    auto tokenizer = LookaheadTransformer::Create(
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

TEST_F(LookaheadTransformerTest, ParameterKeywordStatementMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kStatement, "a @select c"),
              ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, '@',
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, ParameterKeywordTokenizerMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kTokenizer, "a @select c"),
              ElementsAre(Token::IDENTIFIER, '@', Token::IDENTIFIER,
                          Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, SysvarKeywordStatementMode) {
  EXPECT_THAT(
      GetAllTokens(BisonParserMode::kStatement, "a @@where c"),
      ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, Token::KW_DOUBLE_AT,
                  Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, SysvarKeywordTokenizerMode) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kTokenizer, "a @@where c"),
              ElementsAre(Token::IDENTIFIER, Token::KW_DOUBLE_AT,
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, QueryParamCurrentDate) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kStatement, "a @current_date c"),
              ElementsAre(Token::MODE_STATEMENT, Token::IDENTIFIER, '@',
                          Token::IDENTIFIER, Token::IDENTIFIER, Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, SysvarWithDotId) {
  EXPECT_THAT(
      GetAllTokens(BisonParserMode::kStatement, "SELECT @@ORDER.WITH.c"),
      ElementsAre(Token::MODE_STATEMENT, Token::KW_SELECT, Token::KW_DOUBLE_AT,
                  Token::IDENTIFIER, '.',
                  Token::IDENTIFIER, '.', Token::IDENTIFIER, Token::YYEOF));
}

absl::StatusOr<TokenKind> GetNextToken(LookaheadTransformer& tokenizer,
                                       Location& location) {
  TokenKind token_kind;
  ZETASQL_RETURN_IF_ERROR(tokenizer.GetNextToken(&location, &token_kind));
  return token_kind;
}

TEST_F(LookaheadTransformerTest, Lookahead1) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));
  Location location;
  LookaheadTransformer& tokenizer = *lexer;
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::INTEGER_LITERAL));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::KW_SELECT);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, Lookahead1WithForceTerminate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr,
                      /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));

  // Then even after YYEOF
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, Lookahead2) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::IDENTIFIER));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::KW_SELECT);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::INTEGER_LITERAL));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::KW_SELECT);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);
}

TEST_F(LookaheadTransformerTest, Lookahead2BeforeLookahead1) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  // Calling Lookahead2 before Lookahead1 returns the correct token.
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);

  // Repeated calling Lookahead2 returns the same token.
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);
}

TEST_F(LookaheadTransformerTest, Lookahead2NoEnoughTokens) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "", 0, options_,
                      /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);

  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);

  // Fetching more tokens returns YYEOF.
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
}

TEST_F(LookaheadTransformerTest, Lookahead2ForceTerminate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // After the force termination both lookaheads return YYEOF.
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);

  // Fetching more tokens returns YYEOF.
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);
}

TEST_F(LookaheadTransformerTest, Lookahead2ForceTerminateLookahead2First) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::IDENTIFIER);
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // After the force termination both lookaheads return YYEOF.
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);

  // Fetching more tokens returns YYEOF.
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_EQ(TokenTestThief::Lookahead2(tokenizer), Token::YYEOF);
  EXPECT_EQ(TokenTestThief::Lookahead1(tokenizer), Token::YYEOF);
}

TEST_F(LookaheadTransformerTest, LookaheadTransformerReturnsYyeofWhenErrors) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT * EXCEPT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

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

static void AdvanceLexer(LookaheadTransformer& tokenizer, Location& location) {
  absl::string_view unused;
  tokenizer.GetNextToken(&unused, &location);
}

MATCHER_P2(TokenIs, expected_kind, status_matcher, "") {
  return ExplainMatchResult(
      Optional(FieldsAre(Field(&TokenWithLocation::kind, Eq(expected_kind)),
                         Eq(kNoToken), status_matcher)),
      arg, result_listener);
}

MATCHER_P(TokenIs, expected_kind, "") {
  return ExplainMatchResult(TokenIs(expected_kind, Eq(absl::OkStatus())), arg,
                            result_listener);
}

TEST_F(LookaheadTransformerTest, LookaheadTransformerHasCorrectPrevToken) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT 1 FULL UNION ALL", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::MODE_STATEMENT));

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::KW_SELECT));

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::INTEGER_LITERAL));

  // The stored token should be KW_FULL_IN_SET_OP rather than KW_FULL.
  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::KW_FULL_IN_SET_OP));
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerHasCorrectPrevTokenAndError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT * EXCEPT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::MODE_STATEMENT));

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::KW_SELECT));

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer), TokenIs('*'));

  constexpr absl::string_view error_message =
      R"(EXCEPT must be followed by ALL, DISTINCT, or "(")";

  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                             HasSubstr(error_message))));

  // Further advancing the lexer returns the same token and error.
  AdvanceLexer(tokenizer, location);
  EXPECT_THAT(TokenTestThief::GetCurrentToken(tokenizer),
              TokenIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                             HasSubstr(error_message))));
}

constexpr int kFurtherLookaheadBeyondEof = 5;

// Keep calling GetNextToken after YYEOF is returned with no errors.
TEST_F(LookaheadTransformerTest,
       LookaheadTransformerGetNextTokenAfterEofNoError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer,
      LookaheadTransformer::Create(
          BisonParserMode::kStatement, "fake_file", "SELECT *", 0, options_,
          /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds('*'));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));

  // Keep calling GetNextToken should always return YYEOF.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  }
}

// Keep calling GetNextToken after YYEOF is returned with errors.
TEST_F(LookaheadTransformerTest,
       LookaheadTransformerGetNextTokenAfterEofWithError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT * EXCEPT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds('*'));

  constexpr absl::string_view error_message =
      R"(EXCEPT must be followed by ALL, DISTINCT, or "(")";
  EXPECT_THAT(
      GetNextToken(tokenizer, location),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr(error_message)));

  // Keep calling GetNextToken should always return the same error.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(
        GetNextToken(tokenizer, location),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr(error_message)));
  }
}

// SetForceTerminate is called without fetching any tokens.
TEST_F(LookaheadTransformerTest,
       LookaheadTransformerGetNextTokenAfterSetForceTerminateNoTokens) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer,
      LookaheadTransformer::Create(
          BisonParserMode::kStatement, "fake_file", "SELECT *", 0, options_,
          /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);
  // Keep calling GetNextToken should always return YYEOF with no errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  }
}

// Last returned token has no errors and then SetForceTerminate is called.
TEST_F(LookaheadTransformerTest,
       LookaheadTransformerGetNextTokenAfterSetForceTerminateNoError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer,
      LookaheadTransformer::Create(
          BisonParserMode::kStatement, "fake_file", "SELECT *", 0, options_,
          /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // Keep calling GetNextToken should always return YYEOF with no errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  }
}

// Last returned token reports an error and then SetForceTerminate is called.
TEST_F(LookaheadTransformerTest,
       LookaheadTransformerGetNextTokenAfterSetForceTerminateLastTokenErrors) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT * EXCEPT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::KW_SELECT));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds('*'));

  constexpr absl::string_view error_message =
      R"(EXCEPT must be followed by ALL, DISTINCT, or "(")";
  EXPECT_THAT(
      GetNextToken(tokenizer, location),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr(error_message)));

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // Keep calling GetNextToken should always return the same error.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(
        GetNextToken(tokenizer, location),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr(error_message)));
  }
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerOverrideLookbackInvalidAnduselessCalls) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kScript, "fake_file",
                           "BEGIN BEGIN END END", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));
  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(tokenizer.OverrideNextTokenLookback(
                  /*parser_lookahead_is_empty=*/true, Token::KW_BEGIN,
                  Token::LB_BEGIN_AT_STATEMENT_START),
              StatusIs(absl::StatusCode::kInternal));

  EXPECT_THAT(tokenizer.OverrideNextTokenLookback(
                  /*parser_lookahead_is_empty=*/false, Token::KW_BEGIN,
                  Token::LB_BEGIN_AT_STATEMENT_START),
              StatusIs(absl::StatusCode::kInternal));

  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(kNoToken));
  // Signal a lookback override of an irrelevant token. This should be ignored
  ZETASQL_ASSERT_OK(tokenizer.OverrideNextTokenLookback(
      /*parser_lookahead_is_empty=*/true, Token::KW_LOOP,
      Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  // Signal a lookback override of an irrelevant token. This should be ignored
  ZETASQL_ASSERT_OK(tokenizer.OverrideNextTokenLookback(
      /*parser_lookahead_is_empty=*/false, Token::KW_LOOP,
      Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::KW_END));
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerOverrideLookbackForEmptyParserLA) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kScript, "fake_file",
                           "BEGIN BEGIN END END", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(kNoToken));
  // Now signal that the next token (the second BEGIN) is the first token in a
  // statement.
  ZETASQL_ASSERT_OK(tokenizer.OverrideNextTokenLookback(
      /*parser_lookahead_is_empty=*/true, Token::KW_BEGIN,
      Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  // This is where we first see the effect of the statement start.
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer),
              Eq(Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookahead1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
  // And this is where we first see the effect in lookback2
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer),
              Eq(Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback2(tokenizer), Eq(Token::KW_END));
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerOverrideLookbackForNonEmptyParserLA) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kScript, "fake_file",
                           "BEGIN BEGIN END END", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location),
              IsOkAndHolds(Token::MODE_SCRIPT));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(kNoToken));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::MODE_SCRIPT));
  // Now signal that the previously consumed token (the first BEGIN) is the
  // first token in statement.
  ZETASQL_ASSERT_OK(tokenizer.OverrideNextTokenLookback(
      /*parser_lookahead_is_empty=*/false, Token::KW_BEGIN,
      Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_BEGIN));
  // This is where we first see the effect of the statement start.
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer),
              Eq(Token::LB_BEGIN_AT_STATEMENT_START));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_BEGIN));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::KW_END));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
  EXPECT_THAT(GetNextToken(tokenizer, location), IsOkAndHolds(Token::YYEOF));
  EXPECT_THAT(TokenTestThief::Lookback1(tokenizer), Eq(Token::KW_END));
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerIdentifyingStartOfExplainExplian) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "EXPLAIN EXPLAIN SELECT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  EXPECT_THAT(GetNextToken(*lexer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));

  std::vector<TokenKind> seen_tokens, seen_lookbacks;
  do {
    seen_tokens.push_back(*GetNextToken(*lexer, location));
    seen_lookbacks.push_back(TokenTestThief::Lookback1(*lexer));
  } while (seen_lookbacks.back() != Token::YYEOF);
  EXPECT_THAT(seen_tokens, ContainerEq(std::vector<TokenKind>{
                               Token::KW_EXPLAIN,
                               Token::KW_EXPLAIN,
                               Token::KW_SELECT,
                               Token::INTEGER_LITERAL,
                               Token::YYEOF,
                               Token::YYEOF,
                           }));
  EXPECT_THAT(seen_lookbacks, ContainerEq(std::vector<TokenKind>{
                                  Token::MODE_STATEMENT,
                                  Token::LB_EXPLAIN_SQL_STATEMENT,
                                  Token::LB_EXPLAIN_SQL_STATEMENT,
                                  Token::KW_SELECT,
                                  Token::INTEGER_LITERAL,
                                  Token::YYEOF,
                              }));
}

TEST_F(LookaheadTransformerTest,
       LookaheadTransformerIdentifyingStartOfHintExplainHint) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "@5 EXPLAIN @{a = 1} EXPLAIN SELECT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  EXPECT_THAT(GetNextToken(*lexer, location),
              IsOkAndHolds(Token::MODE_STATEMENT));

  std::vector<TokenKind> seen_tokens, seen_lookbacks, seen_lookback2s;
  int i = 0;
  do {
    seen_tokens.push_back(*GetNextToken(*lexer, location));
    if (int current = i++; current == 1 || current == 8) {
      // Simulate having parser context to identify end of statement level
      // hints.
      ZETASQL_EXPECT_OK(lexer->OverrideCurrentTokenLookback(
          Token::LB_END_OF_STATEMENT_LEVEL_HINT));
    }
    seen_lookbacks.push_back(TokenTestThief::Lookback1(*lexer));
    seen_lookback2s.push_back(TokenTestThief::Lookback2(*lexer));
  } while (seen_lookbacks.back() != Token::YYEOF);
  EXPECT_THAT(seen_tokens, ContainerEq(std::vector<TokenKind>{
                               Token::KW_OPEN_INTEGER_HINT,
                               Token::INTEGER_LITERAL,
                               Token::KW_EXPLAIN,
                               Token::KW_OPEN_HINT,
                               '{',
                               Token::IDENTIFIER,
                               '=',
                               Token::INTEGER_LITERAL,
                               '}',
                               Token::KW_EXPLAIN,
                               Token::KW_SELECT,
                               Token::INTEGER_LITERAL,
                               Token::YYEOF,
                               Token::YYEOF,
                           }));
  EXPECT_THAT(seen_lookbacks, ContainerEq(std::vector<TokenKind>{
                                  Token::MODE_STATEMENT,
                                  Token::KW_OPEN_INTEGER_HINT,
                                  Token::LB_END_OF_STATEMENT_LEVEL_HINT,
                                  Token::LB_EXPLAIN_SQL_STATEMENT,
                                  Token::KW_OPEN_HINT,
                                  '{',
                                  Token::IDENTIFIER,
                                  '=',
                                  Token::INTEGER_LITERAL,
                                  Token::LB_END_OF_STATEMENT_LEVEL_HINT,
                                  Token::LB_EXPLAIN_SQL_STATEMENT,
                                  Token::KW_SELECT,
                                  Token::INTEGER_LITERAL,
                                  Token::YYEOF,
                              }));
  EXPECT_THAT(seen_lookback2s, ContainerEq(std::vector<TokenKind>{
                                   kNoToken,
                                   Token::MODE_STATEMENT,
                                   Token::KW_OPEN_INTEGER_HINT,
                                   Token::LB_END_OF_STATEMENT_LEVEL_HINT,
                                   Token::LB_EXPLAIN_SQL_STATEMENT,
                                   Token::KW_OPEN_HINT,
                                   '{',
                                   Token::IDENTIFIER,
                                   '=',
                                   Token::INTEGER_LITERAL,
                                   Token::LB_END_OF_STATEMENT_LEVEL_HINT,
                                   Token::LB_EXPLAIN_SQL_STATEMENT,
                                   Token::KW_SELECT,
                                   Token::INTEGER_LITERAL,
                               }));
}

static TokenWithOverrideError GetTokenKindAndError(
    LookaheadTransformer& tokenizer) {
  absl::string_view unused_text;
  Location unused_location;
  TokenKind token_kind = tokenizer.GetNextToken(&unused_text, &unused_location);
  return {.token = {.kind = token_kind}, .error = tokenizer.GetOverrideError()};
}

MATCHER_P2(TokenKindIs, expected_kind, status_matcher, "") {
  return ExplainMatchResult(
      FieldsAre(Field(&TokenWithLocation::kind, Eq(expected_kind)),
                Eq(kNoToken), status_matcher),
      arg, result_listener);
}

MATCHER_P(TokenKindIs, expected_kind, "") {
  return ExplainMatchResult(TokenKindIs(expected_kind, Eq(absl::OkStatus())),
                            arg, result_listener);
}

TEST_F(LookaheadTransformerTest, SetForceTerminateBeforeFetchingTokens) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto lexer,
                       LookaheadTransformer::Create(
                           BisonParserMode::kStatement, "fake_file",
                           "SELECT * EXCEPT 1", 0, options_,
                           /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;
  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // No tokens have been returned, the end of the statement should be 0.
  EXPECT_EQ(end_byte_offset, 0);

  // Fetching more tokens always return YYEOF without errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

// The difference between this test case and
// SetForceTerminateBeforeFetchingTokens is that the lookahead1 of this test
// case is YYEOF.
TEST_F(LookaheadTransformerTest, SetForceTerminateNoTokens) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "", 0, options_,
                      /*macro_catalog=*/nullptr, /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;
  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // If no tokens have been returned, the end of the statement should be 0.
  EXPECT_EQ(end_byte_offset, 0);

  // Fetching more tokens always return YYEOF without errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, SetForceTerminateLastTokenErrors) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", "`", 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  constexpr absl::string_view error_message =
      "Syntax error: Unclosed identifier literal";
  EXPECT_THAT(
      GetTokenKindAndError(tokenizer),
      TokenKindIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                         HasSubstr(error_message))));

  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // The last returned token errors, so the end of the statement should be
  // updated to -1.
  EXPECT_EQ(end_byte_offset, -1);

  // Fetching more tokens should always return YYEOF with the same error.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(
        GetTokenKindAndError(tokenizer),
        TokenKindIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                           HasSubstr(error_message))));
  }
}

TEST_F(LookaheadTransformerTest, SetForceTerminateLastTokenIsYyeof) {
  constexpr absl::string_view kInput = "    ";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));

  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // The last returned token is YYEOF, so the end of the statement should be
  // updated to its end location.
  EXPECT_EQ(end_byte_offset, kInput.size());

  // Fetching more tokens should always return YYEOF with no errors because
  // the last token does not have errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, SetForceTerminateLookahead1IsYyeof) {
  constexpr absl::string_view kInput = "SELECT      ";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::KW_SELECT));

  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // The lookahead is YYEOF, so the end of the statement should be extended to
  // the end location of YYEOF.
  EXPECT_EQ(end_byte_offset, kInput.size());

  // Fetching more tokens should always return YYEOF with no errors because the
  // last token does not have errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, SetForceTerminateLookahead1Errors) {
  constexpr absl::string_view kInput = "SELECT $";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::KW_SELECT));

  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // The lookahead1 errors, so the end of the statement stay at the end location
  // of the last token KW_SELECT.
  EXPECT_EQ(end_byte_offset, 6);

  // Fetching more tokens should always return YYEOF with no errors because the
  // last token does not have errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, SetForceTerminateLookahead1IsNonYyeof) {
  constexpr absl::string_view kInput = "SELECT 1";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::KW_SELECT));

  int end_byte_offset;
  tokenizer.SetForceTerminate(&end_byte_offset);

  // The lookahead1 is not YYEOF, so the end of the statement stay at the end
  // location of the last token KW_SELECT.
  EXPECT_EQ(end_byte_offset, 6);

  // Fetching more tokens should always return YYEOF with no errors because the
  // last token does not have errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, GetNextTokenContinuesToReturnYyeof) {
  constexpr absl::string_view kInput = "SELECT";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));

  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::KW_SELECT));

  // Fetching more tokens should always return YYEOF with no errors because the
  // last token is YYEOF without errors.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, GetNextTokenContinuesToReturnTheSameError) {
  constexpr absl::string_view kInput = "SELECT `";
  constexpr absl::string_view kError =
      "Syntax error: Unclosed identifier literal";

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::KW_SELECT));
  EXPECT_THAT(
      GetTokenKindAndError(tokenizer),
      TokenKindIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                         HasSubstr(kError))));

  // Fetching more tokens should always return YYEOF with the same error.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    EXPECT_THAT(
        GetTokenKindAndError(tokenizer),
        TokenKindIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                           HasSubstr(kError))));
  }
}

MATCHER_P(IsSameOptionalToken, token, "") {
  if (arg.has_value() != token.has_value()) {
    *result_listener << "expected.has_value() = " << token.has_value()
                     << " but actual.has_value() = " << arg.has_value();
    return false;
  }
  if (!arg.has_value()) {
    return true;
  }
  if (!ExplainMatchResult(Eq(token->error), arg->error, result_listener)) {
    return false;
  }
  const TokenWithLocation& expected = token->token;
  return ExplainMatchResult(
      AllOf(Field(&TokenWithLocation::kind, Eq(expected.kind)),
            Field(&TokenWithLocation::location, Eq(expected.location)),
            Field(&TokenWithLocation::text, Eq(expected.text)),
            Field(&TokenWithLocation::preceding_whitespaces,
                  Eq(expected.preceding_whitespaces))),
      arg->token, result_listener);
}

TEST_F(LookaheadTransformerTest, PreviousTokensAreCorrectNoErrors) {
  constexpr absl::string_view kInput = "SELECT 1 *";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  std::optional<TokenWithOverrideError> previous_current_token;
  do {
    EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
                IsSameOptionalToken(previous_current_token));
    previous_current_token = TokenTestThief::GetCurrentToken(tokenizer);

    Location unused_location;
    AdvanceLexer(tokenizer, unused_location);
  } while (TokenTestThief::GetCurrentToken(tokenizer)->token.kind !=
           Token::YYEOF);

  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              IsSameOptionalToken(previous_current_token));

  // The previous tokens should remain YYEOF.
  Location unused_location;
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    AdvanceLexer(tokenizer, unused_location);
    EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
                TokenIs(Token::YYEOF));
  }
}

TEST_F(LookaheadTransformerTest, PreviousTokensAreCorrectWithErrors) {
  constexpr absl::string_view kInput = "SELECT `";
  constexpr absl::string_view kError =
      "Syntax error: Unclosed identifier literal";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  std::optional<TokenWithOverrideError> previous_current_token;
  do {
    EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
                IsSameOptionalToken(previous_current_token));
    previous_current_token = TokenTestThief::GetCurrentToken(tokenizer);

    Location unused_location;
    AdvanceLexer(tokenizer, unused_location);
  } while (TokenTestThief::GetCurrentToken(tokenizer)->token.kind !=
           Token::YYEOF);

  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              IsSameOptionalToken(previous_current_token));

  // The previous token of the lookahead_transformer should remain YYEOF with
  // the same error.
  Location unused_location;
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    AdvanceLexer(tokenizer, unused_location);
    EXPECT_THAT(
        TokenTestThief::GetPreviousToken(tokenizer),
        TokenIs(Token::YYEOF, StatusIs(absl::StatusCode::kInvalidArgument,
                                       HasSubstr(kError))));
  }
}

TEST_F(LookaheadTransformerTest,
       PreviousTokensAreCorrectWithSetForceTerminate) {
  constexpr absl::string_view kInput = "SELECT 1 *";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  Location unused_location;
  AdvanceLexer(tokenizer, unused_location);
  // Current token is MODE_STATEMENT.
  EXPECT_EQ(TokenTestThief::GetPreviousToken(tokenizer), std::nullopt);

  AdvanceLexer(tokenizer, unused_location);
  // Current token is KW_SELECT
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::MODE_STATEMENT));

  // SetForceTerminate does not affect the previous token.
  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::MODE_STATEMENT));

  // Further advancing the lexer should return the current token when
  // SetForceTerminate is called, i.e. KW_SELECT.
  AdvanceLexer(tokenizer, unused_location);
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::KW_SELECT));

  // Starts to return YYEOF consistently.
  AdvanceLexer(tokenizer, unused_location);
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::YYEOF));
  AdvanceLexer(tokenizer, unused_location);
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::YYEOF));
  AdvanceLexer(tokenizer, unused_location);
  EXPECT_THAT(TokenTestThief::GetPreviousToken(tokenizer),
              TokenIs(Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, TokenFusion) {
  constexpr absl::string_view kInput = ">>";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::KW_SHIFT_RIGHT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, TokensWithWhitespacesInBetweenCannotFuse) {
  constexpr absl::string_view kInput = "> >";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               /*macro_catalog=*/nullptr,
                                               /*arena=*/nullptr));
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs('>'));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs('>'));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
}

static constexpr absl::string_view kDefsFileName = "defs.sql";

static absl::Status RegisterMacros(absl::string_view source,
                                   const LanguageOptions& language_options,
                                   macros::MacroCatalog& macro_catalog) {
  ParseResumeLocation location =
      ParseResumeLocation::FromStringView(kDefsFileName, source);
  bool at_end_of_input = false;
  while (!at_end_of_input) {
    std::unique_ptr<ParserOutput> output;
    ZETASQL_RETURN_IF_ERROR(ParseNextStatement(
        &location, ParserOptions(language_options), &output, &at_end_of_input));
    ZETASQL_RET_CHECK(output->statement() != nullptr);
    auto def_macro_stmt =
        output->statement()->GetAsOrNull<ASTDefineMacroStatement>();
    ZETASQL_RET_CHECK(def_macro_stmt != nullptr);
    ZETASQL_RETURN_IF_ERROR(macro_catalog.RegisterMacro(
        {.source_text = source,
         .location = def_macro_stmt->GetParseLocationRange(),
         .name_location = def_macro_stmt->name()->GetParseLocationRange(),
         .body_location = def_macro_stmt->body()->GetParseLocationRange()}));
  }
  return absl::OkStatus();
}

TEST_F(LookaheadTransformerTest, TokensFromDifferentFilesCannotFuse) {
  options_.EnableLanguageFeature(FEATURE_V_1_4_SQL_MACROS);
  macros::MacroCatalog macro_catalog;
  ZETASQL_ASSERT_OK(
      RegisterMacros("DEFINE MACRO greater_than >", options_, macro_catalog));
  auto arena = std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/4096);

  constexpr absl::string_view kInput = ">$greater_than";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(BisonParserMode::kStatement,
                                               "fake_file", kInput, 0, options_,
                                               &macro_catalog, arena.get()));
  LookaheadTransformer& tokenizer = *lexer;

  EXPECT_THAT(GetTokenKindAndError(tokenizer),
              TokenKindIs(Token::MODE_STATEMENT));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs('>'));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs('>'));
  EXPECT_THAT(GetTokenKindAndError(tokenizer), TokenKindIs(Token::YYEOF));
}

TEST_F(LookaheadTransformerTest, RightShiftIsAllowedAfterParentheses) {
  EXPECT_THAT(
      GetAllTokens(BisonParserMode::kStatement, "ARRAY<TYPEOF(1 >> 2)>"),
      ElementsAreArray(std::vector<TokenKind>{
          Token::MODE_STATEMENT,
          Token::KW_ARRAY,
          '<',
          Token::IDENTIFIER,
          '(',
          Token::INTEGER_LITERAL,
          Token::KW_SHIFT_RIGHT,
          Token::INTEGER_LITERAL,
          ')',
          '>',
          Token::YYEOF,
      }));
}

TEST_F(LookaheadTransformerTest, RightShiftIsAllowedAfterUnpairedParentheses) {
  EXPECT_THAT(GetAllTokens(BisonParserMode::kStatement, "ARRAY<)>>>>>"),
              ElementsAreArray(std::vector<TokenKind>{
                  Token::MODE_STATEMENT,
                  Token::KW_ARRAY,
                  '<',
                  ')',
                  Token::KW_SHIFT_RIGHT,
                  Token::KW_SHIFT_RIGHT,
                  '>',
                  Token::YYEOF,
              }));
}

TEST_F(LookaheadTransformerTest, Lookahead3) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));
  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  // Ok to examine lookaheads before fetching any tokens.
  EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  AdvanceLexer(tokenizer, location);
  EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer), Token::KW_SELECT);

  AdvanceLexer(tokenizer, location);
  EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer), Token::YYEOF);

  // Continue fetching tokens returns the same YYEOF.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    AdvanceLexer(tokenizer, location);
    EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer), Token::YYEOF);
  }
}

TEST_F(LookaheadTransformerTest, Lookahead3SetForceTerminate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto lexer, LookaheadTransformer::Create(
                      BisonParserMode::kStatement, "fake_file", "a 1 SELECT", 0,
                      options_, /*macro_catalog=*/nullptr, /*arena=*/nullptr));
  Location location;
  LookaheadTransformer& tokenizer = *lexer;

  // Ok to examine lookaheads before fetching any tokens.
  EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer),
            Token::DECIMAL_INTEGER_LITERAL);

  tokenizer.SetForceTerminate(/*end_byte_offset=*/nullptr);

  // Lookahead3() becomes YYEOF after force termination.
  EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer), Token::YYEOF);

  // Continue fetching tokens returns the same YYEOF.
  for (int i = 0; i < kFurtherLookaheadBeyondEof; ++i) {
    AdvanceLexer(tokenizer, location);
    EXPECT_EQ(TokenTestThief::Lookahead3(tokenizer), Token::YYEOF);
  }
}

}  // namespace zetasql::parser
