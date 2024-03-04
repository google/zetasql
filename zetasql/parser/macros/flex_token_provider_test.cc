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

#include "zetasql/parser/macros/flex_token_provider.h"

#include <ostream>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

using ::testing::_;
using ::testing::Eq;
using ::testing::ExplainMatchResult;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::StatusIs;

// Template specialization to print tokens in failed test messages.
static void PrintTo(const TokenWithLocation& token, std::ostream* os) {
  *os << absl::StrFormat(
      "(kind: %i, location: %s, text: '%s', prev_spaces: '%s')", token.kind,
      token.location.GetString(), token.text, token.preceding_whitespaces);
}

MATCHER_P(IsOkAndHoldsToken, expected, "") {
  return ExplainMatchResult(IsOk(), arg, result_listener) &&
         ExplainMatchResult(
             FieldsAre(Eq(expected.kind), Eq(expected.location),
                       Eq(expected.text), Eq(expected.preceding_whitespaces)),
             arg.value(), result_listener);
}

static absl::string_view kFileName = "<filename>";

static Location MakeLocation(int start_offset, int end_offset) {
  Location location;
  location.set_start(
      ParseLocationPoint::FromByteOffset(kFileName, start_offset));
  location.set_end(ParseLocationPoint::FromByteOffset(kFileName, end_offset));
  return location;
}

static FlexTokenProvider MakeTokenProvider(BisonParserMode mode,
                                           absl::string_view input) {
  return FlexTokenProvider(mode, kFileName, input,
                           /*start_offset=*/0, LanguageOptions());
}

static FlexTokenProvider MakeTokenProvider(absl::string_view input) {
  return MakeTokenProvider(BisonParserMode::kTokenizer, input);
}

TEST(FlexTokenProviderTest, RawTokenizerMode) {
  absl::string_view input = "/*comment*/ 123";

  EXPECT_THAT(
      MakeTokenProvider(BisonParserMode::kTokenizer, input).ConsumeNextToken(),
      IsOkAndHoldsToken(TokenWithLocation{
          .kind = INTEGER_LITERAL,
          .location = MakeLocation(12, 15),
          .text = "123",
          .preceding_whitespaces = "/*comment*/ ",
      }));
}

TEST(FlexTokenProviderTest, RawTokenizerPreserveCommentsMode) {
  absl::string_view input = "/*comment*/ 123";
  FlexTokenProvider provider =
      MakeTokenProvider(BisonParserMode::kTokenizerPreserveComments, input);
  EXPECT_THAT(provider.ConsumeNextToken(), IsOkAndHoldsToken(TokenWithLocation{
                                               .kind = COMMENT,
                                               .location = MakeLocation(0, 11),
                                               .text = "/*comment*/",
                                               .preceding_whitespaces = "",
                                           }));
  EXPECT_THAT(provider.ConsumeNextToken(), IsOkAndHoldsToken(TokenWithLocation{
                                               .kind = INTEGER_LITERAL,
                                               .location = MakeLocation(12, 15),
                                               .text = "123",
                                               .preceding_whitespaces = " ",
                                           }));
}

TEST(FlexTokenProviderTest, RawTokenizerNextStatementMode) {
  absl::string_view input = "/*comment*/ 123";
  FlexTokenProvider provider =
      MakeTokenProvider(BisonParserMode::kNextStatement, input);
  EXPECT_THAT(provider.ConsumeNextToken(), IsOkAndHoldsToken(TokenWithLocation{
                                               .kind = CUSTOM_MODE_START,
                                               .location = MakeLocation(0, 0),
                                               .text = "",
                                               .preceding_whitespaces = "",
                                           }));
  EXPECT_THAT(provider.ConsumeNextToken(),
              IsOkAndHoldsToken(TokenWithLocation{
                  .kind = INTEGER_LITERAL,
                  .location = MakeLocation(12, 15),
                  .text = "123",
                  .preceding_whitespaces = "/*comment*/ ",
              }));
}

TEST(FlexTokenProviderTest, AlwaysEndsWithEOF) {
  absl::string_view input = "\t\t";
  FlexTokenProvider flex_token_provider = MakeTokenProvider(input);
  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              IsOkAndHoldsToken(TokenWithLocation{
                  .kind = TokenKinds::YYEOF,
                  .location = MakeLocation(2, 2),
                  .text = "",
                  .preceding_whitespaces = "\t\t",
              }));
  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              StatusIs(_, HasSubstr("Internal error: Encountered real EOF")));
}

TEST(FlexTokenProviderTest, CanPeekToken) {
  absl::string_view input = "\t123 identifier";
  FlexTokenProvider flex_token_provider = MakeTokenProvider(input);
  const TokenWithLocation int_token{
      .kind = TokenKinds::INTEGER_LITERAL,
      .location = MakeLocation(1, 4),
      .text = "123",
      .preceding_whitespaces = "\t",
  };
  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(int_token));
  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(int_token));
  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              IsOkAndHoldsToken(int_token));

  const TokenWithLocation identifier_token{
      .kind = TokenKinds::IDENTIFIER,
      .location = MakeLocation(5, 15),
      .text = "identifier",
      .preceding_whitespaces = " ",
  };
  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(identifier_token));
  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(identifier_token));
  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              IsOkAndHoldsToken(identifier_token));
}

TEST(FlexTokenProviderTest, TracksCountOfConsumedTokensIncludingEOF) {
  absl::string_view input = "SELECT";
  FlexTokenProvider flex_token_provider = MakeTokenProvider(input);

  TokenWithLocation first_token{
      .kind = TokenKinds::KW_SELECT,
      .location = MakeLocation(0, 6),
      .text = "SELECT",
      .preceding_whitespaces = "",
  };

  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(first_token));
  EXPECT_EQ(flex_token_provider.num_consumed_tokens(), 0);

  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              IsOkAndHoldsToken(first_token));
  EXPECT_EQ(flex_token_provider.num_consumed_tokens(), 1);

  TokenWithLocation second_token{
      .kind = TokenKinds::YYEOF,
      .location = MakeLocation(6, 6),
      .text = "",
      .preceding_whitespaces = "",
  };

  EXPECT_THAT(flex_token_provider.PeekNextToken(),
              IsOkAndHoldsToken(second_token));
  EXPECT_EQ(flex_token_provider.num_consumed_tokens(), 1);

  EXPECT_THAT(flex_token_provider.ConsumeNextToken(),
              IsOkAndHoldsToken(second_token));
  EXPECT_EQ(flex_token_provider.num_consumed_tokens(), 2);
}

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
