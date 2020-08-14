//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/public/parse_tokens.h"

#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"

namespace zetasql {

using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Not;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// This takes a member function of ParseToken (e.g. ParseToken::GetKeyword),
// calls that function on each element of parse_tokens, and returns a string
// with the comma-separated strings of the results.
template <class FUNC>
static std::string Call(const std::vector<ParseToken>& parse_tokens,
                        FUNC function) {
  bool first = true;
  std::string result;
  for (const ParseToken& parse_token : parse_tokens) {
    if (!first) result.append(",");
    first = false;
    absl::StrAppend(&result, (parse_token.*function)());
  }
  return result;
}

TEST(GetNextTokensTest, Locations) {
  ParseTokenOptions options;
  std::vector<ParseToken> parse_tokens;
  const std::string filename = "filename_Locations";
  const std::string input = "seLect `aBc`\n123 gHi;";
  ParseResumeLocation location =
      ParseResumeLocation::FromStringView(filename, input);
  ParseLocationTranslator location_translator(input);

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));
  ASSERT_EQ(6, parse_tokens.size());

  EXPECT_EQ("KEYWORD:SELECT", parse_tokens[0].DebugString());
  EXPECT_EQ("IDENTIFIER:aBc", parse_tokens[1].DebugString());
  EXPECT_EQ("VALUE:123", parse_tokens[2].DebugString());
  EXPECT_EQ("IDENTIFIER_OR_KEYWORD:gHi", parse_tokens[3].DebugString());
  EXPECT_EQ("KEYWORD:;", parse_tokens[4].DebugString());
  EXPECT_EQ("EOF", parse_tokens[5].DebugString());

  EXPECT_EQ("SELECT", parse_tokens[0].GetSQL());
  EXPECT_EQ("aBc", parse_tokens[1].GetSQL());
  EXPECT_EQ("123", parse_tokens[2].GetSQL());
  EXPECT_EQ("gHi", parse_tokens[3].GetSQL());
  EXPECT_EQ(";", parse_tokens[4].GetSQL());
  EXPECT_EQ("", parse_tokens[5].GetSQL());

  EXPECT_EQ("seLect", parse_tokens[0].GetImage());
  EXPECT_EQ("`aBc`", parse_tokens[1].GetImage());
  EXPECT_EQ("123", parse_tokens[2].GetImage());
  EXPECT_EQ("gHi", parse_tokens[3].GetImage());
  EXPECT_EQ(";", parse_tokens[4].GetImage());
  EXPECT_EQ("", parse_tokens[5].GetImage());

  EXPECT_EQ("1,0,0,1,1,0", Call(parse_tokens, &ParseToken::IsKeyword));
  EXPECT_EQ("0,1,0,1,0,0", Call(parse_tokens, &ParseToken::IsIdentifier));
  EXPECT_EQ("0,0,1,0,0,0", Call(parse_tokens, &ParseToken::IsValue));
  EXPECT_EQ("0,0,0,0,0,1", Call(parse_tokens, &ParseToken::IsEndOfInput));

  // Note that keywords are uppercased, but identifiers are not.
  EXPECT_EQ("SELECT,,,GHI,;,", Call(parse_tokens, &ParseToken::GetKeyword));
  EXPECT_EQ(",aBc,,gHi,,", Call(parse_tokens, &ParseToken::GetIdentifier));

  EXPECT_EQ("Uninitialized value", parse_tokens[0].GetValue().DebugString());
  EXPECT_EQ("Uninitialized value", parse_tokens[1].GetValue().DebugString());
  EXPECT_EQ("123", parse_tokens[2].GetValue().DebugString());
  EXPECT_EQ("Uninitialized value", parse_tokens[3].GetValue().DebugString());
  EXPECT_EQ("Uninitialized value", parse_tokens[4].GetValue().DebugString());
  EXPECT_EQ("Uninitialized value", parse_tokens[5].GetValue().DebugString());

  const ParseLocationRange range = parse_tokens[3].GetLocationRange();
  EXPECT_THAT(
      location_translator.GetLineAndColumnAfterTabExpansion(range.start()),
      IsOkAndHolds(std::make_pair(2, 5)));
  EXPECT_THAT(
      location_translator.GetLineAndColumnAfterTabExpansion(range.end()),
      IsOkAndHolds(std::make_pair(2, 8)));

  ParseToken empty;
  EXPECT_EQ("", empty.GetSQL());
  EXPECT_EQ("EOF", empty.DebugString());
}

TEST(GetNextTokensTest, MaxTokensNotResumable) {
  ParseTokenOptions options;
  options.max_tokens = 2;
  std::vector<ParseToken> parse_tokens;
  const std::string filename = "filename_MaxTokensNotResumable";
  ParseResumeLocation location =
      ParseResumeLocation::FromString(filename, "seLect `aBc`\n123 gHi;");

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));
  ASSERT_EQ(2, parse_tokens.size());

  EXPECT_THAT(
      GetParseTokens(options, &location, &parse_tokens),
      StatusIs(
          _,
          HasSubstr("GetParseTokens() called on invalid ParseResumeLocation")));
}

TEST(GetNextTokensTest, LargeMaxTokensNotResumable) {
  ParseTokenOptions options;
  options.max_tokens = 10;
  std::vector<ParseToken> parse_tokens;
  const std::string filename = "filename_LargeMaxTokensNotResumable";
  ParseResumeLocation location =
      ParseResumeLocation::FromString(filename, "seLect `aBc`\n123 gHi;");

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));
  ASSERT_EQ(6, parse_tokens.size());

  // Still not resumable even though all tokens were returned.
  EXPECT_THAT(
      GetParseTokens(options, &location, &parse_tokens),
      StatusIs(
          _,
          HasSubstr("GetParseTokens() called on invalid ParseResumeLocation")));
}

TEST(GetNextTokensTest, PreserveCommentsWithoutEndingNewline) {
  ParseTokenOptions options;
  options.include_comments = true;

  std::vector<ParseToken> parse_tokens;

  const std::string input = "SELECT 1 --Comment";
  ParseResumeLocation location = ParseResumeLocation::FromStringView(input);

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));
  ASSERT_EQ(4, parse_tokens.size());

  EXPECT_EQ("KEYWORD:SELECT", parse_tokens[0].DebugString());
  EXPECT_EQ("VALUE:1", parse_tokens[1].DebugString());
  EXPECT_EQ("COMMENT:--Comment\n", parse_tokens[2].DebugString());
  EXPECT_EQ("EOF", parse_tokens[3].DebugString());

  EXPECT_TRUE(parse_tokens[2].IsComment());
}

TEST(GetNextTokensTest, LocationsWithComments) {
  ParseTokenOptions options;
  options.include_comments = true;

  std::vector<ParseToken> parse_tokens;

  const std::string input =
      "SELECT 1 --Comment\nFROM /* inline */ t;\n/* Multi \n line */ SELECT 1;";
  const std::string filename = "filename_LocationsWithComments";
  ParseResumeLocation location =
      ParseResumeLocation::FromStringView(filename, input);
  ParseLocationTranslator location_translator(input);

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));

  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[2].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(1, 10)));

  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[4].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(2, 6)));

  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[7].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(3, 1)));

  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[7].GetLocationRange().end()),
              IsOkAndHolds(std::make_pair(4, 9)));
}

TEST(GetNextTokensTest, LocationsWithCommentsForNonCommentTokens) {
  ParseTokenOptions options;
  options.include_comments = true;

  std::vector<ParseToken> parse_tokens;

  const std::string filename =
      "filename_LocationsWithCommentsForNonCommentTokens";
  const std::string input =
      "SELECT\na * (\n  /* multi \n line \n comment */\na + b + c\n);\n";
  ParseResumeLocation location =
      ParseResumeLocation::FromStringView(filename, input);
  ParseLocationTranslator location_translator(input);

  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));

  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[3].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(2, 5)));
  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[4].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(3, 3)));
  EXPECT_THAT(location_translator.GetLineAndColumnAfterTabExpansion(
                  parse_tokens[5].GetLocationRange().start()),
              IsOkAndHolds(std::make_pair(6, 1)));
}

TEST(GetNextTokensTest, ResumeLocationIsAdjustedOnError) {
  ParseTokenOptions options;
  std::vector<ParseToken> parse_tokens;

  // Error in bison tokenizer.
  ParseResumeLocation location = ParseResumeLocation::FromString("SELECT 'abc");
  EXPECT_THAT(GetParseTokens(options, &location, &parse_tokens), Not(IsOk()));
  EXPECT_EQ(location.byte_position(), 6);

  // Error converting into ParseToken.
  location = ParseResumeLocation::FromString("SELECT a, ``");
  EXPECT_THAT(GetParseTokens(options, &location, &parse_tokens), Not(IsOk()));
  EXPECT_EQ(location.byte_position(), 9);

  // Error at the very first token.
  location = ParseResumeLocation::FromString("  'abc");
  EXPECT_THAT(GetParseTokens(options, &location, &parse_tokens), Not(IsOk()));
  EXPECT_EQ(location.byte_position(), 0);
}

static void checkEqualBetweenTokenAndProto(const ParseToken& token, ParseTokenProto_Kind proto_kind) {
  auto status_or_proto = token.ToProto();

  // Confirm the ToProto of the token runs successfully and assign the proto as a local variable.
  EXPECT_TRUE(status_or_proto.ok());
  auto proto = status_or_proto.value();

  EXPECT_EQ(proto_kind, proto.kind());
  EXPECT_EQ(token.GetImage(), proto.image());

  // Check whether the Location Range objects in both token and its proto are identical.
  EXPECT_EQ(token.GetLocationRange().start().GetByteOffset(), proto.parse_location_range().start());
  EXPECT_EQ(token.GetLocationRange().end().GetByteOffset(), proto.parse_location_range().end());
  EXPECT_EQ(token.GetLocationRange().start().filename(), proto.parse_location_range().filename());

  // If the token has no valid value, then return directly.
  if (!token.GetValue().is_valid()) {
    return;
  }
  // Check whether the values at token and its proto are identical.
  switch (proto.type().type_kind()) {
    case TypeKind::TYPE_BOOL:
      EXPECT_EQ(token.GetValue().bool_value(), proto.value().bool_value());
      break;
    case TypeKind::TYPE_STRING:
      EXPECT_EQ(token.GetValue().string_value(), proto.value().string_value());
      break;
    case TypeKind::TYPE_NUMERIC:
      EXPECT_EQ(token.GetValue().numeric_value().ToString(), proto.value().numeric_value());
      break;
  }
}

TEST(GetNextTokensTest, ParseTokenToProto) {
  ParseTokenOptions options;
  std::vector<ParseToken> parse_tokens;

  ParseResumeLocation location = ParseResumeLocation::FromString("SELECT 'abc' 1 \ntrue `HASH`\n foo");
  // Check error in bison tokenizer.
  ZETASQL_ASSERT_OK(GetParseTokens(options, &location, &parse_tokens));
  EXPECT_EQ(7, parse_tokens.size());

  // Check individual tokens.
  checkEqualBetweenTokenAndProto(parse_tokens[0], ParseTokenProto_Kind_KEYWORD);
  checkEqualBetweenTokenAndProto(parse_tokens[1], ParseTokenProto_Kind_VALUE);
  checkEqualBetweenTokenAndProto(parse_tokens[2], ParseTokenProto_Kind_VALUE);
  checkEqualBetweenTokenAndProto(parse_tokens[3], ParseTokenProto_Kind_KEYWORD);
  checkEqualBetweenTokenAndProto(parse_tokens[4], ParseTokenProto_Kind_IDENTIFIER);
  checkEqualBetweenTokenAndProto(parse_tokens[5], ParseTokenProto_Kind_IDENTIFIER_OR_KEYWORD);
  checkEqualBetweenTokenAndProto(parse_tokens[6], ParseTokenProto_Kind_END_OF_INPUT);

}

TEST(GetNextTokensTest, ConvertBetweenParseTokenOptionsAndProto) {
  ParseTokenOptions options;
  options.max_tokens = 100;
  options.include_comments = true;
  options.stop_at_end_of_statement = true;

  auto proto = options.ToProto();
  EXPECT_EQ(100, proto.max_tokens());
  EXPECT_EQ(true, proto.include_comments());
  EXPECT_EQ(true, proto.stop_at_end_of_statement());

  options = ParseTokenOptions::FromProto(proto);
  EXPECT_EQ(100, options.max_tokens);
  EXPECT_EQ(true, options.include_comments);
  EXPECT_EQ(true, options.stop_at_end_of_statement);
}

}  // namespace zetasql
