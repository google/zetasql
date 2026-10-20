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

#include "googlesql/public/functions/json_internal.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "googlesql/base/case.h"  
#include "re2/re2.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {
namespace functions {
namespace json_internal {

namespace {
static LazyRE2 kKeyLexer = {"\\.([\\p{L}\\p{N}\\d_\\-\\:\\s]+)"};
static LazyRE2 kKeyLexerStrict = {"\\.([\\p{L}\\p{N}\\d_]+)"};

static LazyRE2 kOffsetLexer = {"\\[\\s*([\\p{L}\\p{N}\\d_\\-]+)\\s*\\]"};
static LazyRE2 kOffsetLexerStandard = {"\\[\\s*([\\p{N}\\d_\\-]+)\\s*\\]"};
static LazyRE2 kOffsetLexerStrict = {"\\[\\s*([\\p{N}\\d]+)\\s*\\]"};

static LazyRE2 kWildcardBracketLexer = {"\\[\\s*\\*\\s*\\]"};

// (?: alterantive-grouping) indicates a non-capuring group. Currently RE2
// does not support *+ and ++ which avoid backtracking and efficient.
// Matches strings with escaped single quotes.
static LazyRE2 kEscKeyLexer = {
    "\\[\\s*'((?:[^\\\\']|\\\\'|\\\\[^'])*)'\\s*\\]"};

static LazyRE2 kEscKeyLexerStandard = {
    "\\.\\\"((?:[^\\\\\\\"]|\\\\\\\"|\\\\[^\\\"])*)\\\""};

static LazyRE2 kUnSupportedLexer = {"(\\*|\\.\\.|@)"};
static LazyRE2 kBeginRegex = {"\\$"};

// This is a regex that supports lax notation. It matches keywords
// {lax, recursive} case agnostically. The keywords are specified before the
// `kBeginRegex`.
static LazyRE2 kLaxOptionsKeywordRegex = {
    R"regexp((?i)(lax|recursive)\s+)regexp"};
constexpr absl::string_view kLaxKeyword = "lax";
constexpr absl::string_view kRecursiveKeyword = "recursive";

constexpr char kStandardEscapeChar = '"';
constexpr char kLegacyEscapeChar = '\'';
constexpr absl::string_view kBeginToken = "";

// We have separate error messages because engines currently validate against
// this message. Engines should not be verifying GoogleSQL internals but that is
// the current state.
constexpr absl::string_view kPrefixErrorMsg = "JSONPath must start with '$'";
constexpr absl::string_view kLaxPrefixErrorMsg =
    "JSONPath must start with zero or more unique modifiers followed by '$'";

}  // namespace

// Checks if the given JSON path is supported and valid.
absl::Status IsValidJSONPath(absl::string_view text, bool sql_standard_mode,
                             bool enable_wildcard_and_descendant) {
  return ParseJsonPath(text, sql_standard_mode, enable_wildcard_and_descendant)
      .status();
}

static absl::string_view GetPrefixErrorMessage(bool enable_lax_mode) {
  return enable_lax_mode ? kLaxPrefixErrorMsg : kPrefixErrorMsg;
}

static absl::Status ValidateAndConsumeStrictPathAfterKeywords(
    absl::string_view& text, bool enable_lax_mode) {
  if (!RE2::Consume(&text, *kBeginRegex)) {
    return absl::OutOfRangeError(GetPrefixErrorMessage(enable_lax_mode));
  }
  while (!text.empty()) {
    if (!RE2::Consume(&text, *kKeyLexer) &&
        !RE2::Consume(&text, *kEscKeyLexerStandard)) {
      std::string parsed_string;
      if (!RE2::Consume(&text, *kOffsetLexerStrict, &parsed_string)) {
        return absl::OutOfRangeError(
            absl::StrCat("Invalid token in JSONPath at: ", text));
      }
      int64_t index;
      if (!absl::SimpleAtoi(parsed_string, &index)) {
        return absl::OutOfRangeError(
            absl::StrCat("Invalid array index: ", parsed_string));
      }
    }
  }
  return absl::OkStatus();
}

// Validates and consumes the lax keywords(specified before the `kBeginRegex`)
// and returns the specified options.
// Returns an error if there are invalid keyword combinations.
static absl::StatusOr<StrictJSONPathIterator::JsonPathOptions>
GetOptionsAndConsumeKeywords(absl::string_view& text) {
  StrictJSONPathIterator::JsonPathOptions lax_options;

  // The valid combinations are {'lax recursive', 'recursive lax', 'lax'} case
  // agnostic.
  absl::string_view matched_keyword;
  while (RE2::Consume(&text, *kLaxOptionsKeywordRegex, &matched_keyword)) {
    // Verify keywords are not repeated.
    if (!lax_options.lax &&
        googlesql_base::CaseEqual(matched_keyword, kLaxKeyword)) {
      lax_options.lax = true;
    } else if (!lax_options.recursive &&
               googlesql_base::CaseEqual(matched_keyword, kRecursiveKeyword)) {
      lax_options.recursive = true;
    } else {
      return absl::OutOfRangeError(kLaxPrefixErrorMsg);
    }
  }

  // 'recursive' without keyword 'lax' is invalid.
  if (lax_options.recursive && !lax_options.lax) {
    return absl::OutOfRangeError(
        "JSONPath has an invalid combination of modifiers. The 'lax' modifier "
        "must be included if 'recursive' is specified.");
  }
  return lax_options;
}

absl::Status IsValidJSONPathStrict(absl::string_view text,
                                   bool enable_lax_mode) {
  if (enable_lax_mode) {
    // For validity checks we can ignore the returned JsonPathOptions.
    GOOGLESQL_RETURN_IF_ERROR(GetOptionsAndConsumeKeywords(text).status());
  }
  return ValidateAndConsumeStrictPathAfterKeywords(text, enable_lax_mode);
}

absl::StatusOr<bool> IsValidAndLaxJSONPath(
    absl::string_view text, bool enable_wildcard_and_descendant) {
  GOOGLESQL_ASSIGN_OR_RETURN(StrictJSONPathIterator::JsonPathOptions options,
                   GetOptionsAndConsumeKeywords(text));
  if (options.lax) {
    GOOGLESQL_RETURN_IF_ERROR(ValidateAndConsumeStrictPathAfterKeywords(
        text, /*enable_lax_mode=*/true));
    return true;
  }
  // This is a non-lax path. Therefore, doesn't need to have strict path
  // semantics.
  GOOGLESQL_RETURN_IF_ERROR(IsValidJSONPath(text, /*sql_standard_mode=*/true,
                                  enable_wildcard_and_descendant));
  // This is a valid JSONPath but does not follow lax mode.
  return false;
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

// Converts structured `JsonPathStep`s back into the legacy string-token list
// expected by `ValidJSONPathIterator`.
absl::StatusOr<std::vector<std::string>> ParsedJsonPath::ToLegacyTokens()
    const {
  GOOGLESQL_RET_CHECK(!steps_.empty() && steps_[0].is_root());
  GOOGLESQL_RET_CHECK(!has_wildcard_or_descendant_);
  std::vector<std::string> tokens;
  tokens.reserve(steps_.size());
  for (const JsonPathStep& step : steps_) {
    switch (step.kind()) {
      case JsonPathStepKind::kRoot:
        // The root '$' step is historically represented as an empty string "".
        tokens.push_back(std::string(kBeginToken));
        break;
      case JsonPathStepKind::kMember:
        tokens.push_back(std::string(step.member_name()));
        break;
      case JsonPathStepKind::kIndex:
        // Preserve the exact raw token string (e.g. "007", "-0", "1_2") for
        // legacy `ValidJSONPathIterator` compatibility.
        tokens.push_back(std::string(step.raw_token()));
        break;
      case JsonPathStepKind::kObjectWildcard:
      case JsonPathStepKind::kSubscriptWildcard:
        GOOGLESQL_RET_CHECK_FAIL() << "ToLegacyTokens() does not support wildcard steps.";
    }
  }
  return tokens;
}

// Returns an `OutOfRangeError` indicating either an unsupported JSONPath
// operator (`@`, or `*` / `..` when `allow_wildcard_and_descendant` is false)
// or a general syntax error at `path`.
static absl::Status MakeInvalidOrUnsupportedPathError(
    absl::string_view path, bool allow_wildcard_and_descendant) {
  std::string unsupported_token;
  if (!allow_wildcard_and_descendant &&
      RE2::PartialMatch(path, *kUnSupportedLexer, &unsupported_token)) {
    return absl::OutOfRangeError(
        absl::StrCat("Unsupported operator in JSONPath: ", unsupported_token));
  }
  // '@' is not supported in any mode, keeping the error message consistent.
  if (absl::StrContains(path, '@')) {
    return absl::OutOfRangeError("Unsupported operator in JSONPath: @");
  }
  return absl::OutOfRangeError(
      absl::StrCat("Invalid token in JSONPath at: ", path));
}

// Consumes a single bracketed subscript step (starting with `[`) from `*path`
// and appends the corresponding `JsonPathStep` to `*parsed`:
//   - Subscript wildcard: `[*]` (when `allow_wildcard_and_descendant` is true)
//   - Array index (SQL standard mode): `[0]`, `[1]`, etc.
//   - Bracketed member or index (legacy mode, `sql_standard_mode = false`):
//     `['key']` or `[key]` / `[0]`
// Precondition: `path->starts_with('[')`.
static absl::Status ParseBracketStep(absl::string_view* path,
                                     bool sql_standard_mode,
                                     bool allow_wildcard_and_descendant,
                                     bool is_descendant,
                                     ParsedJsonPath* parsed) {
  GOOGLESQL_RET_CHECK(path->starts_with('['));
  std::string token;
  if (allow_wildcard_and_descendant &&
      RE2::Consume(path, *kWildcardBracketLexer)) {
    parsed->AddStep(JsonPathStep::SubscriptWildcard(is_descendant));
  } else if (sql_standard_mode &&
             RE2::Consume(path, *kOffsetLexerStandard, &token)) {
    int64_t index = -1;
    // Negative array indexing is not supported in GoogleSQL JSONPath (array
    // indices are 0-based and non-negative). For legacy linear paths,
    // `kOffsetLexerStandard` syntactically accepts tokens containing `-` or `_`
    // (such as `[-1]`, `[-0]`, `[1_2]`). When the path contains wildcard or
    // descendant operators, `ParseJsonPath` validates all `kIndex` steps
    // strictly (matching `StrictJSONPathIterator`) after parsing.
    if (!absl::SimpleAtoi(token, &index)) {
      index = -1;
    }
    parsed->AddStep(
        JsonPathStep::Index(index, std::move(token), is_descendant));
  } else if (!sql_standard_mode && RE2::Consume(path, *kEscKeyLexer, &token)) {
    // Legacy mode allows escaped single quotes, remove the backslash.
    RemoveBackSlashFollowedByChar(&token, kLegacyEscapeChar);
    parsed->AddStep(JsonPathStep::Member(std::move(token), is_descendant));
  } else if (!sql_standard_mode && RE2::Consume(path, *kOffsetLexer, &token)) {
    // In legacy mode (`JSON_EXTRACT`), unquoted brackets like `$[a]` or `$[0]`
    // can match either an object member or an array index via `ToLegacyTokens`.
    parsed->AddStep(JsonPathStep::Member(std::move(token), is_descendant));
  } else {
    // We expect a valid index or wildcard after '[' except in legacy mode.
    // Otherwise, it's an invalid path.
    return MakeInvalidOrUnsupportedPathError(*path,
                                             allow_wildcard_and_descendant);
  }
  return absl::OkStatus();
}

absl::StatusOr<ParsedJsonPath> ParseJsonPath(
    absl::string_view path, bool sql_standard_mode,
    bool enable_wildcard_and_descendant) {
  if (!RE2::Consume(&path, *kBeginRegex)) {
    // This parse function only supports the non-lax default JsonPath format.
    return absl::OutOfRangeError(kPrefixErrorMsg);
  }

  const bool allow_wildcard_and_descendant =
      sql_standard_mode && enable_wildcard_and_descendant;
  ParsedJsonPath parsed;
  parsed.AddStep(JsonPathStep::Root());
  // Tracks the first non-strict member key tail (an unquoted `.key` containing
  // `-`, `:`, or whitespace, or a quoted `."key"` containing `\`) encountered
  // before any wildcard or descendant operator, so it can be rejected if a
  // wildcard or descendant operator appears later in the path.
  absl::string_view first_non_strict_key_tail;

  while (!path.empty()) {
    const absl::string_view step_start = path;
    const bool is_descendant =
        allow_wildcard_and_descendant && path.starts_with("..");
    if (is_descendant) {
      // Strip both dots for `..[...]` so `path` starts with '[', or strip just
      // the first dot so `..key`, `.."key"`, and `..*` start with '.' and
      // reuse the single-dot lexers below.
      path.remove_prefix(path.starts_with("..[") ? 2 : 1);
    }

    // 1. Bracket subscript cases ([...] or ..[...])
    if (path.starts_with('[')) {
      GOOGLESQL_RETURN_IF_ERROR(ParseBracketStep(&path, sql_standard_mode,
                                       allow_wildcard_and_descendant,
                                       is_descendant, &parsed));
      continue;
    }

    // 2. Dot child / descendant cases (., ..)
    std::string token;
    const LazyRE2& key_lexer =
        (is_descendant || parsed.has_wildcard_or_descendant()) ? kKeyLexerStrict
                                                               : kKeyLexer;
    if (allow_wildcard_and_descendant && absl::ConsumePrefix(&path, ".*")) {
      if (!path.empty() && !path.starts_with('.') && !path.starts_with('[')) {
        // Reject substring paths like `$.*key`.
        return absl::OutOfRangeError(
            absl::StrCat("Invalid token in JSONPath at: *", path));
      }
      parsed.AddStep(JsonPathStep::ObjectWildcard(is_descendant));
    } else if (sql_standard_mode &&
               RE2::Consume(&path, *kEscKeyLexerStandard, &token)) {
      // SQL standard double-quoted member names, e.g. `."key"`, `.."key"`.
      // When wildcards or descendant operators are used, reject backslash
      // escapes inside quoted keys (`."..."` / `.."..."`).
      if (allow_wildcard_and_descendant && absl::StrContains(token, '\\')) {
        if (is_descendant || parsed.has_wildcard_or_descendant()) {
          return MakeInvalidOrUnsupportedPathError(
              step_start, allow_wildcard_and_descendant);
        }
        if (first_non_strict_key_tail.empty()) {
          first_non_strict_key_tail = step_start;
        }
      }
      RemoveBackSlashFollowedByChar(&token, kStandardEscapeChar);
      parsed.AddStep(JsonPathStep::Member(std::move(token), is_descendant));
    } else if (RE2::Consume(&path, *key_lexer, &token)) {
      // Match unquoted member names. When wildcards or descendant operators
      // are used, unquoted member names must be strict identifiers
      // (`[\p{L}\p{N}\d_]+`); keys with spaces, colons, or hyphens must be
      // double-quoted (`."a b:c-d"`).
      if (allow_wildcard_and_descendant && !is_descendant &&
          !parsed.has_wildcard_or_descendant() &&
          first_non_strict_key_tail.empty()) {
        absl::string_view strict_path = step_start;
        RE2::Consume(&strict_path, *kKeyLexerStrict);
        if (strict_path.size() != path.size()) {
          first_non_strict_key_tail = strict_path;
        }
      }
      parsed.AddStep(JsonPathStep::Member(std::move(token), is_descendant));
    } else if (!is_descendant && path == "." && !sql_standard_mode) {
      // Legacy mode allows a trailing single dot at the end of the path.
      break;
    } else {
      // Invalid or unsupported path like: $.., $...a, $.@, $. or non-supported
      // operators in legacy mode.
      return MakeInvalidOrUnsupportedPathError(step_start,
                                               allow_wildcard_and_descendant);
    }
  }

  // When wildcard or descendant operators are present, enforce strict
  // member names (no unquoted `-`, `:`, or whitespace, and no `\` in quoted
  // keys) and strict array index validation (`kOffsetLexerStrict` +
  // `absl::SimpleAtoi`, rejecting permissive `kOffsetLexerStandard` tokens
  // containing `-` or `_` such as `$.*[-1]`, `$.*[-0]`, `$.*[1_2]`,
  // `$[-1].*`, as well as overflowing integer indices).
  if (parsed.has_wildcard_or_descendant()) {
    if (!first_non_strict_key_tail.empty()) {
      return absl::OutOfRangeError(absl::StrCat(
          "Invalid token in JSONPath at: ", first_non_strict_key_tail));
    }
    for (const JsonPathStep& step : parsed.steps()) {
      if (!step.is_index()) continue;
      if (absl::StrContains(step.raw_token(), '-') ||
          absl::StrContains(step.raw_token(), '_')) {
        return absl::OutOfRangeError(absl::StrCat(
            "Invalid token in JSONPath at: [", step.raw_token(), "]"));
      }
      if (step.index() < 0) {
        return absl::OutOfRangeError(absl::StrCat(
            "JSONPath contains invalid array index: ", step.raw_token()));
      }
    }
  }

  return parsed;
}

absl::StatusOr<std::unique_ptr<ValidJSONPathIterator>>
ValidJSONPathIterator::Create(absl::string_view js_path,
                              bool sql_standard_mode) {
  GOOGLESQL_ASSIGN_OR_RETURN(ParsedJsonPath parsed_path,
                   ParseJsonPath(js_path, sql_standard_mode,
                                 /*enable_wildcard_and_descendant=*/false));
  return Create(parsed_path);
}

absl::StatusOr<std::unique_ptr<ValidJSONPathIterator>>
ValidJSONPathIterator::Create(const ParsedJsonPath& parsed_path) {
  GOOGLESQL_RET_CHECK(!parsed_path.has_wildcard_or_descendant());
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<Token> tokens, parsed_path.ToLegacyTokens());
  return absl::WrapUnique(new ValidJSONPathIterator(std::move(tokens)));
}

// Validates and initializes a json path. During parsing, initializes all tokens
// that can be re-used by `StrictJSONPathIterator`. This avoids duplicate
// intensive regex matching.
absl::StatusOr<std::unique_ptr<StrictJSONPathIterator>>
StrictJSONPathIterator::Create(absl::string_view path, bool enable_lax_mode) {
  JsonPathOptions path_options;
  if (enable_lax_mode) {
    GOOGLESQL_ASSIGN_OR_RETURN(path_options, GetOptionsAndConsumeKeywords(path));
  }
  if (!RE2::Consume(&path, *kBeginRegex)) {
    return absl::OutOfRangeError(GetPrefixErrorMessage(enable_lax_mode));
  }
  std::vector<StrictJSONPathToken> tokens;
  tokens.push_back(StrictJSONPathToken(std::monostate()));
  while (!path.empty()) {
    std::string parsed_string;
    if (RE2::Consume(&path, *kKeyLexer, &parsed_string)) {
      StrictJSONPathToken strict_token(std::move(parsed_string));
      tokens.push_back(std::move(strict_token));
    } else if ((RE2::Consume(&path, *kOffsetLexerStrict, &parsed_string))) {
      int64_t index;
      if (!absl::SimpleAtoi(parsed_string, &index)) {
        return absl::OutOfRangeError(absl::StrCat(
            "JSONPath contains invalid array index: ", parsed_string));
      }
      StrictJSONPathToken strict_token(index);
      tokens.push_back(std::move(strict_token));
    } else if (RE2::Consume(&path, *kEscKeyLexerStandard, &parsed_string)) {
      tokens.push_back(StrictJSONPathToken(std::move(parsed_string)));
    } else {
      return absl::OutOfRangeError(
          absl::StrCat("Invalid token in JSONPath at: ", path));
    }
  }
  return absl::WrapUnique(
      new StrictJSONPathIterator(std::move(tokens), path_options));
}

absl::StatusOr<std::unique_ptr<StrictJSONPathIterator>>
StrictJSONPathIterator::Create(
    std::vector<std::variant<int64_t, std::string>> tokens,
    bool enable_lax_mode) {
  std::vector<StrictJSONPathToken> strict_tokens;
  strict_tokens.push_back(StrictJSONPathToken(std::monostate()));
  for (auto& token : tokens) {
    if (std::holds_alternative<int64_t>(token)) {
      int64_t index = std::get<int64_t>(token);
      StrictJSONPathToken strict_token(index);
      strict_tokens.push_back(StrictJSONPathToken(std::move(strict_token)));
    } else {
      std::string parsed_string = std::get<std::string>(token);
      strict_tokens.push_back(StrictJSONPathToken(std::move(parsed_string)));
    }
  }
  return absl::WrapUnique(
      new StrictJSONPathIterator(std::move(strict_tokens), JsonPathOptions()));
}

}  // namespace json_internal
}  // namespace functions
}  // namespace googlesql
