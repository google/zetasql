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

// An API that provides information on ZetaSQL keywords.
#ifndef ZETASQL_PARSER_KEYWORDS_H_
#define ZETASQL_PARSER_KEYWORDS_H_

#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/case.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace zetasql {
namespace parser {

// Metadata for a keyword.
class KeywordInfo {
 public:
  KeywordInfo(absl::string_view keyword,
              absl::optional<int> reserved_bison_token,
              absl::optional<int> nonreserved_bison_token)
      : keyword_(absl::AsciiStrToUpper(keyword)),
        reserved_bison_token_(reserved_bison_token),
        nonreserved_bison_token_(nonreserved_bison_token) {
    ZETASQL_DCHECK(reserved_bison_token.has_value() ||
           nonreserved_bison_token.has_value())
        << "Either reserved or nonreserved must have a Bison token";
  }

  // The keyword, in upper case.
  const std::string& keyword() const { return keyword_; }

  // The Bison parser token for this keyword, when it is reserved.
  // Valid only when CanBeReserved() is true.
  int reserved_bison_token() const { return reserved_bison_token_.value(); }

  // The Bison parser token for this keyword, when it is nonreserved.
  // Valid only when IsAlwaysReserved() is false.
  int nonreserved_bison_token() const {
    return nonreserved_bison_token_.value();
  }

  // True if this keyword can be reserved under any LanguageOptions.
  bool CanBeReserved() const { return reserved_bison_token_.has_value(); }

  // True if this keyword is reserved under all LanguageOptions.
  bool IsAlwaysReserved() const {
    return !nonreserved_bison_token_.has_value();
  }

  // True if this keyword can be either reserved or nonreserved, depending on
  // the LanguageOptions.
  bool IsConditionallyReserved() const {
    return reserved_bison_token_.has_value() &&
           nonreserved_bison_token_.has_value();
  }

 private:
  std::string keyword_;

  // The Bison parser token code when this keyword is reserved, or
  // absl::nullopt if this keyword is never reserved.
  absl::optional<int> reserved_bison_token_;

  // The Bison parser token code when this keyword is nonreserved, or
  // absl::nullopt if this keyword is always reserved.
  absl::optional<int> nonreserved_bison_token_;
};

// Returns the KeywordInfo for keyword 'keyword' (case insensitively), or
// nullptr if 'keyword' is not a keyword.
const KeywordInfo* GetKeywordInfo(absl::string_view keyword);

// Returns the KeywordInfo for token 'bison_token', or nullptr if the
// 'bison_token' is not a keyword token.
//
// For conditionally reserved keywords, both the reserved and nonreserved Bison
// tokens are accepted.
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
