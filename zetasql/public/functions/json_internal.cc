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

#include "zetasql/public/functions/json_internal.h"

#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace json_internal {

namespace {
static LazyRE2 kKeyLexer = {"\\.([\\p{L}\\p{N}\\d_\\-\\:\\s]+)"};

static LazyRE2 kKeyRegex = {"\\.[\\p{L}\\p{N}\\d_\\-\\:\\s]+"};

static LazyRE2 kOffsetLexer = {"\\[\\s*([\\p{L}\\p{N}\\d_\\-]+)\\s*\\]"};
static LazyRE2 kOffsetRegex = {"\\[\\s*[\\p{L}\\p{N}\\d_\\-]+\\s*\\]"};

static LazyRE2 kOffsetLexerStandard = {"\\[\\s*([\\p{N}\\d_\\-]+)\\s*\\]"};
static LazyRE2 kOffsetRegexStandard = {"\\[\\s*[\\p{N}\\d_\\-]+\\s*\\]"};

// (?: alterantive-grouping) indicates a non-capuring group. Currently RE2
// does not support *+ and ++ which avoid backtracking and efficient.
// Matches strings with escaped single quotes.
static LazyRE2 kEscKeyLexer = {
    "\\[\\s*'((?:[^\\\\']|\\\\'|\\\\[^'])*)'\\s*\\]"};
static LazyRE2 kEscKeyRegex = {"\\[\\s*'(?:[^\\\\']|\\\\'|\\\\[^'])*'\\s*\\]"};

static LazyRE2 kEscKeyLexerStandard = {
    "\\.\\\"((?:[^\\\\\\\"]|\\\\\\\"|\\\\[^\\\"])*)\\\""};
static LazyRE2 kEscKeyRegexStandard = {
    "\\.\\\"(?:[^\\\\\\\"]|\\\\\\\"|\\\\[^\\\"])*\\\""};

static LazyRE2 kUnSupportedLexer = {"(\\*|\\.\\.|@)"};
static LazyRE2 kBeginRegex = {"\\$"};

}  // namespace

// Checks if the given JSON path is supported and valid.
absl::Status IsValidJSONPath(absl::string_view text, bool sql_standard_mode) {
  if (!RE2::Consume(&text, *kBeginRegex)) {
    return absl::OutOfRangeError("JSONPath must start with '$'");
  }

  const RE2* esc_key_regex = kEscKeyRegex.get();
  const RE2* offset_regex = kOffsetRegex.get();
  if (sql_standard_mode) {
    esc_key_regex = kEscKeyRegexStandard.get();
    offset_regex = kOffsetRegexStandard.get();
  }

  while (RE2::Consume(&text, *kKeyRegex) ||
         RE2::Consume(&text, *offset_regex) ||
         RE2::Consume(&text, *esc_key_regex)) {
  }

  // In non-standard mode, it's allowed to have a trailing dot.
  if (!text.empty() && (text != "." || sql_standard_mode)) {
    std::string token;
    bool is_unsupported = RE2::PartialMatch(text, *kUnSupportedLexer, &token);
    if (is_unsupported) {
      return absl::OutOfRangeError(
          absl::StrCat("Unsupported operator in JSONPath: ", token));
    }
    return absl::OutOfRangeError(
        absl::StrCat("Invalid token in JSONPath at: ", text));
  }
  return absl::OkStatus();
}

void RemoveBackSlashFollowedByChar(std::string* token, char esc_chr) {
  if (token && !token->empty()) {
    std::string::const_iterator ritr = token->cbegin();
    std::string::iterator witr = token->begin();
    for (++ritr; ritr != token->end(); ++ritr) {
      if ((*witr != '\\' || *ritr != esc_chr)) {
        ++witr;
      }
      *witr = *ritr;
    }

    if (witr != token->end()) {
      token->erase(++witr, token->end());
    }
  }
}

absl::StatusOr<std::unique_ptr<ValidJSONPathIterator>>
ValidJSONPathIterator::Create(absl::string_view js_path,
                              bool sql_standard_mode) {
  ZETASQL_RETURN_IF_ERROR(IsValidJSONPath(js_path, sql_standard_mode));
  return absl::WrapUnique(
      new ValidJSONPathIterator(js_path, sql_standard_mode));
}

bool ValidJSONPathIterator::operator++() {
  if ((depth_ == tokens_.size()) && !text_.empty()) {
    Token token;
    Token esc_token;
    is_valid_ = RE2::Consume(&text_, *offset_lexer_, &token) ||
                RE2::Consume(&text_, *kKeyLexer, &token);
    if (is_valid_) {
      ++depth_;
      tokens_.push_back(token);
    } else if ((is_valid_ =
                    RE2::Consume(&text_, *esc_key_lexer_, &esc_token))) {
      ++depth_;
      RemoveBackSlashFollowedByChar(&esc_token, esc_chr_);
      tokens_.push_back(esc_token);
    }
  } else if (depth_ <= tokens_.size()) {
    ++depth_;
    is_valid_ = depth_ <= tokens_.size();
  }
  return is_valid_;
}

ValidJSONPathIterator::ValidJSONPathIterator(absl::string_view input,
                                             bool sql_standard_mode)
    : sql_standard_mode_(sql_standard_mode), text_(input) {
  Init();
  offset_lexer_ = kOffsetLexer.get();
  esc_key_lexer_ = kEscKeyLexer.get();
  esc_chr_ = '\'';
  if (sql_standard_mode) {
    offset_lexer_ = kOffsetLexerStandard.get();
    esc_key_lexer_ = kEscKeyLexerStandard.get();
    esc_chr_ = '"';
  }
}

void ValidJSONPathIterator::Init() {
  depth_ = 0;
  is_valid_ = RE2::Consume(&text_, *kBeginRegex);
  ZETASQL_DCHECK(is_valid_);
  // also consume "." in the case of the path begin just "$."
  if (text_ == ".") {
    absl::ConsumePrefix(&text_, ".");
  }
  tokens_.push_back("");
  depth_ = 1;
}

}  // namespace json_internal
}  // namespace functions
}  // namespace zetasql
