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

#include "zetasql/parser/textmapper_lexer_adapter.h"

#include <vector>

#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/language_options.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql::parser {
namespace {

TEST(ZetaSqlTextmapperLexerTest, TestInstantiate) {
  Lexer lexer = Lexer(BisonParserMode::kTokenizer, "filename", "SELECT 1",
                      /*start_offset=*/0, LanguageOptions(),
                      /*macro_catalog=*/nullptr, /*arena=*/nullptr);
}

TEST(ZetaSqlTextmapperLexerTest, TestCopy) {
  Lexer lexer = Lexer(BisonParserMode::kNextStatement, "filename", "SELECT 1",
                      /*start_offset=*/0, LanguageOptions(),
                      /*macro_catalog=*/nullptr, /*arena=*/nullptr);
  // skip first token setting mode
  (void)lexer.Next();

  Lexer lookahead = lexer;
  EXPECT_EQ(lookahead.Next(), Token::KW_SELECT);
  EXPECT_EQ(lexer.Text(), "");
  EXPECT_EQ(lookahead.Next(), Token::INTEGER_LITERAL);
  EXPECT_EQ(lexer.Text(), "");

  EXPECT_EQ(lexer.Next(), Token::KW_SELECT);
  EXPECT_EQ(lexer.Text(), "SELECT");
  EXPECT_EQ(lexer.Next(), Token::INTEGER_LITERAL);
  EXPECT_EQ(lexer.Text(), "1");
}

TEST(ZetaSqlTextmapperLexerTest, TestDotIdentifier) {
  TextMapperLexerAdapter lexer = TextMapperLexerAdapter(
      BisonParserMode::kTokenizer, "filename", "SELECT a.1b",
      /*start_offset=*/0, LanguageOptions(), /*macro_catalog=*/nullptr,
      /*arena=*/nullptr);
  std::vector<Token> tokens;
  Token next_token;
  do {
    next_token = lexer.Next();
    tokens.push_back(next_token);
  } while (next_token != Token::EOI);

  EXPECT_THAT(tokens,
              testing::ElementsAre(Token::KW_SELECT, Token::IDENTIFIER,
                                   Token::DOT, Token::IDENTIFIER, Token::EOI));
}

}  // namespace
}  // namespace zetasql::parser
