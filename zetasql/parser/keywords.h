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

// An API that provides information on ZetaSQL keywords.
#ifndef ZETASQL_PARSER_KEYWORDS_H_
#define ZETASQL_PARSER_KEYWORDS_H_

#include <string>
#include <vector>

#include "zetasql/base/case.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// Metadata for a keyword.
class KeywordInfo {
 public:
  // The type of keyword.
  enum KeywordClass {
    kReserved,
    kNotReserved,
  };
  KeywordInfo(absl::string_view keyword, int bison_token,
              KeywordClass keyword_class = kNotReserved)
      : keyword_(absl::AsciiStrToUpper(keyword)),
        bison_token_(bison_token),
        keyword_class_(keyword_class) {}

  // The keyword, in upper case.
  const std::string& keyword() const { return keyword_; }

  // The Bison parser token for this keyword.
  int bison_token() const { return bison_token_; }

  bool IsReserved() const { return keyword_class_ != kNotReserved; }

 private:
  std::string keyword_;

  // The bison parser token code for this keyword.
  int bison_token_;

  // The type of keyword.
  KeywordClass keyword_class_ = kNotReserved;
};

// Returns the KeywordInfo for reserved word 'keyword' (case insensitively), or
// NULL if 'keyword' is not a reserved word.
const KeywordInfo* GetReservedKeywordInfo(absl::string_view keyword);

// Returns the KeywordInfo for keyword 'keyword' (case insensitively), or NULL
// if 'keyword' is not a keyword.
const KeywordInfo* GetKeywordInfo(absl::string_view keyword);

// Returns the KeywordInfo for token 'bison_token', or NULL if the 'bison_token'
// is not a keyword token.
const KeywordInfo* GetKeywordInfoForBisonToken(int bison_token);

// Returns a vector of all keywords with their metadata.
const std::vector<KeywordInfo>& GetAllKeywords();

// Returns true if 'identifier' should be treated as a keyword for
// GetParseTokens(). This applies to words that are keywords in JavaCC but not
// in Bison. We want to treat them as keywords in the tokenizer API even though
// they are not keywords in the Bison parser.
bool IsKeywordInTokenizer(absl::string_view identifier);

// Returns true if 'identifier' is not a keyword but it still has a special
// meaning when used as an unescaped identifier. For instance, if identifier is
// 'current_date', then emitting it as CURRENT_DATE would cause it to be
// interpreted with a special meaning (a call to the CURRENT_DATE() function),
// even though CURRENT_DATE is not a reserved keyword. As a result, the
// identifier should be escaped when it is intended to be used as the identifier
// and not the special keyword meaning.
bool NonReservedIdentifierMustBeBackquoted(absl::string_view identifier);

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_KEYWORDS_H_
