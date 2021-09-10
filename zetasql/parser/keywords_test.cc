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

#include "zetasql/parser/keywords.h"

#include <fstream>
#include <iostream>
#include <set>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "re2/re2.h"

namespace zetasql {
namespace parser {
namespace {
std::vector<std::string> FileLines(absl::string_view file_path) {
  std::ifstream file(file_path.data());
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(file, line)) {
    lines.push_back(line);
  }
  return lines;
}

TEST(GetKeywordInfo, Hit) {
  const KeywordInfo* info = GetKeywordInfo("select");
  ASSERT_TRUE(info != nullptr);
  EXPECT_TRUE(info->CanBeReserved());
  EXPECT_TRUE(info->IsAlwaysReserved());

  info = GetKeywordInfo("row");
  ASSERT_TRUE(info != nullptr);
  EXPECT_FALSE(info->CanBeReserved());
  EXPECT_FALSE(info->IsAlwaysReserved());

  info = GetKeywordInfo("qualify");
  ASSERT_TRUE(info != nullptr);
  EXPECT_TRUE(info->CanBeReserved());
  EXPECT_FALSE(info->IsAlwaysReserved());
}

TEST(GetKeywordInfo, NonHit) {
  const KeywordInfo* info = GetKeywordInfo("selected");
  EXPECT_FALSE(info != nullptr);
}

// Returns a section of lines from file 'file_path' delimited by
// BEGIN_<section_delimiter> and END_<section_delimiter>. The section
// delimiters do not need to be on a line by themselves. The lines that contain
// the section delimiters are not included in the result. EXPECTs that the
// section only occurs once, and that it is explicitly closed.
std::vector<std::string> GetSectionFromFile(
    absl::string_view file_path, absl::string_view section_delimiter) {
  bool in_section = false;
  bool seen_section = false;
  std::vector<std::string> result;
  int line_number = 0;
  for (const std::string& line : FileLines(file_path)) {
    ++line_number;
    if (line.find(absl::StrCat("END_", section_delimiter)) !=
        std::string::npos) {
      EXPECT_TRUE(in_section) << line_number;
      in_section = false;
    }
    if (in_section) {
      result.push_back(line);
    }
    if (line.find(absl::StrCat("BEGIN_", section_delimiter)) !=
        std::string::npos) {
      EXPECT_FALSE(in_section) << line_number;
      EXPECT_FALSE(seen_section) << line_number;
      in_section = true;
      seen_section = true;
    }
  }
  EXPECT_TRUE(seen_section) << line_number;
  EXPECT_FALSE(in_section) << line_number;
  return result;
}

// Extracts keyword references from the lines in 'input', lowercases them and
// returns them as a set. Recognizes both the quoted form (e.g. "foo") and the
// direct form (e.g. KW_FOO).
std::set<std::string> ExtractKeywordsFromLines(
    const std::vector<std::string>& input) {
  std::set<std::string> result;
  RE2 extract_quoted_keyword(".*\"([A-Za-z_]+)\".*");
  RE2 extract_unquoted_keyword(".*KW_([A-Za-z_]+).*");
  for (const std::string& line : input) {
    std::string keyword;
    if (RE2::Extract(line, extract_quoted_keyword, "\\1", &keyword)) {
      result.insert(absl::AsciiStrToLower(keyword));
      continue;
    }
    // Conditionally reserved keywords are referred to in the grammar by KW_
    // values, without a quoted keyword name. Look for the KW_ value.
    if (RE2::Extract(line, extract_unquoted_keyword, "\\1", &keyword)) {
      result.insert("kw_" + absl::AsciiStrToLower(keyword));
    }
  }
  return result;
}

// Extracts keywords from tokenizer rule lines in 'input', lowercases them and
// returns them as a set. The keywords must be at the start of each line,
// followed by a space. Rules with trailing context (e.g. "foo/bar") are
// ignored.
std::set<std::string> ExtractTokenizerKeywordsFromLines(
    const std::vector<std::string>& input) {
  std::set<std::string> result;
  RE2 extract_tokenizer_keyword("^([A-Za-z_]+) ");
  for (const std::string& line : input) {
    std::string keyword;
    if (RE2::Extract(line, extract_tokenizer_keyword, "\\1", &keyword)) {
      result.insert(absl::AsciiStrToLower(keyword));
    }
  }
  return result;
}

// Gets bison token names for reserved or non-reserved keywords depending on
// 'reserved', in lowercase.
//
// Keywords are returned here, as they appear in the Bison grammar
// (e.g. "kw_qualify_reserved" or "kw_qualify_nonreserved", not "qualify").
std::set<std::string> GetKeywordsSetForBisonGrammar(bool reserved) {
  std::set<std::string> result;
  for (const KeywordInfo& keyword_info : GetAllKeywords()) {
    if (keyword_info.IsConditionallyReserved()) {
      std::string keyword = absl::AsciiStrToLower(keyword_info.keyword());
      result.insert(absl::StrCat("kw_", keyword,
                                 reserved ? "_reserved" : "_nonreserved"));
    } else if (keyword_info.IsAlwaysReserved() == reserved) {
      result.insert(absl::AsciiStrToLower(keyword_info.keyword()));
    }
  }
  return result;
}

// Gets a set of all keywords, in lowercase.
std::set<std::string> GetAllKeywordsSet() {
  std::set<std::string> result;
  for (const KeywordInfo& keyword_info : GetAllKeywords()) {
    result.insert(absl::AsciiStrToLower(keyword_info.keyword()));
  }
  return result;
}

std::string GetBisonParserPath() {
  return zetasql_base::JoinPath(
      getenv("TEST_SRCDIR"),
      "com_google_zetasql/zetasql/parser/bison_parser.y");
}

std::string GetFlexTokenizerPath() {
  return zetasql_base::JoinPath(
      getenv("TEST_SRCDIR"),
      "com_google_zetasql/zetasql/parser/flex_tokenizer.l");
}

TEST(GetAllKeywords, ReservedMatchesGrammarReservedKeywords) {
  std::set<std::string> rule_reserved_keywords = ExtractKeywordsFromLines(
      GetSectionFromFile(GetBisonParserPath(), "RESERVED_KEYWORD_RULE"));

  std::set<std::string> reserved_keywords =
      GetKeywordsSetForBisonGrammar(/*reserved=*/true);

  EXPECT_THAT(reserved_keywords,
              ::testing::ContainerEq(rule_reserved_keywords));
}

TEST(GetAllKeywords, NonReservedMatchesGrammarKeywordAsIdentifier) {
  std::set<std::string> keyword_as_identifier = ExtractKeywordsFromLines(
      GetSectionFromFile(GetBisonParserPath(), "KEYWORD_AS_IDENTIFIER"));

  std::set<std::string> non_reserved_keywords =
      GetKeywordsSetForBisonGrammar(false /* reserved */);

  EXPECT_THAT(keyword_as_identifier,
              ::testing::ContainerEq(non_reserved_keywords));
}

TEST(GetAllKeywords, NonReservedMatchesGrammarNonReserved) {
  std::set<std::string> grammar_non_reserved_keywords =
      ExtractKeywordsFromLines(
          GetSectionFromFile(GetBisonParserPath(), "NON_RESERVED_KEYWORDS"));

  std::set<std::string> non_reserved_keywords =
      GetKeywordsSetForBisonGrammar(false /* reserved */);

  EXPECT_THAT(grammar_non_reserved_keywords,
              ::testing::ContainerEq(non_reserved_keywords));
}

TEST(GetAllKeywords, ReservedMatchesGrammarReserved) {
  std::set<std::string> grammar_reserved_keywords = ExtractKeywordsFromLines(
      GetSectionFromFile(GetBisonParserPath(), "RESERVED_KEYWORDS"));

  std::set<std::string> expected_reserved_keywords =
      GetKeywordsSetForBisonGrammar(true /* reserved */);

  // A few special keyword tokens in this section are expected, not accounted
  // for in GetKeywordInfo().
  expected_reserved_keywords.insert("kw_and_for_between");
  expected_reserved_keywords.insert("kw_except_in_set_op");

  EXPECT_THAT(grammar_reserved_keywords,
              ::testing::ContainerEq(expected_reserved_keywords));
}

TEST(GetAllKeywords, AllKeywordsHaveTokenizerRules) {
  std::set<std::string> tokenizer_keywords = ExtractTokenizerKeywordsFromLines(
      GetSectionFromFile(GetFlexTokenizerPath(), "KEYWORDS"));

  std::set<std::string> all_keywords = GetAllKeywordsSet();

  // This tests that all the tokenizer rule keywords have an associated defined
  // keyword, but not necessarily that all keywords are used in tokenizer rules.
  EXPECT_THAT(tokenizer_keywords, ::testing::IsSubsetOf(all_keywords));
}

TEST(ParserTest, DontAddNewReservedKeywords) {
  int num_reserved = 0;
  for (const KeywordInfo& keyword_info : GetAllKeywords()) {
    if (keyword_info.IsAlwaysReserved()) {
      ++num_reserved;
    }
  }
  // *** BE VERY CAREFUL CHANGING THIS. ***
  // Adding reserved keywords is a breaking change.  Removing reserved keywords
  // allows new queries to work that will not work on older code.
  // Before changing this, co-ordinate with all engines to make sure the change
  // is done safely.
  //
  // New reserved keywords should generally be marked as conditionally reserved
  // instead, so that engines desiring backward compatibility can opt out.
  EXPECT_EQ(95 /* CAUTION */, num_reserved);
}

}  // namespace
}  // namespace parser
}  // namespace zetasql
