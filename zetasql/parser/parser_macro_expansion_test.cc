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

#include <memory>

#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/parser.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/proto/logging.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {

using MacroCatalog = parser::macros::MacroCatalog;

using ::testing::_;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

static LanguageOptions GetLanguageOptions() {
  LanguageOptions language_options;
  language_options.EnableMaximumLanguageFeatures();
  language_options.EnableLanguageFeature(FEATURE_V_1_4_SQL_MACROS);
  return language_options;
}

TEST(ParserMacroExpansionTest, ExpandsMacros) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"m", "$1 b"});
  ParserOptions parser_options(GetLanguageOptions(), &macro_catalog);

  ParseResumeLocation resume_location =
      ParseResumeLocation::FromStringView("SELECT a$m(x)2");
  bool at_end_of_input;
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseNextStatement(&resume_location, parser_options, &parser_output,
                               &at_end_of_input));
  EXPECT_TRUE(at_end_of_input);
  EXPECT_EQ(parser_output->runtime_info().num_lexical_tokens(), 9);

  EXPECT_EQ(parser_output->statement()->DebugString(),
            R"(QueryStatement [0-macro:m:4]
  Query [0-macro:m:4]
    Select [0-macro:m:4]
      SelectList [7-macro:m:4]
        SelectColumn [7-macro:m:4]
          PathExpression [7-8]
            Identifier(ax) [7-8]
          Alias [macro:m:3-4]
            Identifier(b2) [macro:m:3-4]
)");
}

TEST(ParserMacroExpansionTest, RecognizesOnlyOriginalDefineMacroStatements) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"def", "DEFINE MACRO x 1"});
  ParserOptions parser_options(GetLanguageOptions(), &macro_catalog);

  ParseResumeLocation resume_location =
      ParseResumeLocation::FromStringView("DEFINE MACRO m 1; $def");
  bool at_end_of_input;
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseNextStatement(&resume_location, parser_options, &parser_output,
                               &at_end_of_input));
  EXPECT_FALSE(at_end_of_input);
  EXPECT_EQ(parser_output->runtime_info().num_lexical_tokens(), 7);

  EXPECT_EQ(parser_output->statement()->DebugString(),
            R"(DefineMacroStatement [0-16]
  Identifier(m) [13-14]
  MacroBody(1) [15-16]
)");

  EXPECT_THAT(
      ParseNextStatement(&resume_location, parser_options, &parser_output,
                         &at_end_of_input),
      StatusIs(_, HasSubstr("Syntax error: DEFINE MACRO statements cannot be "
                            "composed from other expansions")));
}

TEST(ParserMacroExpansionTest,
     CorrectErrorOnPartiallyGeneratedDefineMacroStatement) {
  MacroCatalog macro_catalog;
  macro_catalog.insert({"def", "define"});
  ParserOptions parser_options(GetLanguageOptions(), &macro_catalog);

  ParseResumeLocation resume_location =
      ParseResumeLocation::FromStringView("$def MACRO m 1");
  bool at_end_of_input;
  std::unique_ptr<ParserOutput> parser_output;
  EXPECT_THAT(
      ParseNextStatement(&resume_location, parser_options, &parser_output,
                         &at_end_of_input),
      StatusIs(_, HasSubstr("Syntax error: DEFINE MACRO statements cannot be "
                            "composed from other expansions")));
}

}  // namespace zetasql
