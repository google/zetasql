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

#include "zetasql/public/functions/regexp.h"

#include <ctype.h>

#include <algorithm>

#include "zetasql/base/logging.h"
#include "zetasql/public/functions/util.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "unicode/utf8.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

namespace {
using RE2StringView = re2::StringPiece;
}  // namespace
bool RegExp::InitializePatternUtf8(absl::string_view pattern,
                                   zetasql_base::Status* error) {
  RE2::Options options;
  options.set_log_errors(false);
  options.set_utf8(true);
  re_ = absl::make_unique<RE2>(pattern, options);
  if (!re_->ok()) {
    return internal::UpdateError(
        error, absl::StrCat("Cannot parse regular expression: ", re_->error()));
  }
  return true;
}

bool RegExp::InitializePatternBytes(absl::string_view pattern,
                                    zetasql_base::Status* error) {
  RE2::Options options;
  options.set_log_errors(false);
  options.set_utf8(false);
  re_ = absl::make_unique<RE2>(pattern, options);
  if (!re_->ok()) {
    return internal::UpdateError(
        error, absl::StrCat("Cannot parse regular expression: ", re_->error()));
  }
  return true;
}

bool RegExp::Contains(absl::string_view str, bool* out, zetasql_base::Status* error) {
  DCHECK(re_);
  *out = re_->PartialMatch(str, *re_);
  return true;
}

bool RegExp::Match(absl::string_view str, bool* out, zetasql_base::Status* error) {
  DCHECK(re_);
  *out = re_->FullMatch(str, *re_);
  return true;
}

bool RegExp::Extract(absl::string_view str, absl::string_view* out,
                     bool* is_null, zetasql_base::Status* error) {
  DCHECK(re_);
  if (str.data() == nullptr) {
    // Ensure that the output std::string never has a null data pointer if a match is
    // found.
    str = absl::string_view("", 0);
  }
  *is_null = true;
  if (re_->NumberOfCapturingGroups() == 0) {
    RE2StringView out_sv;
    // If there's no capturing subgroups return the entire matching substring.
    if (re_->Match(str,
                   0,           // startpos
                   str.size(),  // endpos
                   RE2::UNANCHORED, &out_sv,
                   1) &&
        out_sv.data() != nullptr) {  // number of matches at out.
      *is_null = false;
      *out = {out_sv.data(), out_sv.length()};
    }
    return true;
  } else if (re_->NumberOfCapturingGroups() == 1) {
    // If there's a single capturing group return substring matching that
    // group.
    RE2StringView out_sv;
    if (RE2::PartialMatch(str, *re_, &out_sv) && out_sv.data() != nullptr) {
      *is_null = false;
      *out = {out_sv.data(), out_sv.length()};
    }
    return true;
  } else {
    return internal::UpdateError(
        error,
        "Regular expression passed to REGEXP_EXTRACT must not have more than 1 "
        "capturing group");
  }
}

void RegExp::ExtractAllReset(const absl::string_view str) {
  extract_all_input_ = str;
  extract_all_position_ = 0;
  last_match_ = false;
}

bool RegExp::ExtractAllNext(absl::string_view* out, zetasql_base::Status* error) {
  DCHECK(re_);
  if (re_->NumberOfCapturingGroups() > 1) {
    return internal::UpdateError(
        error, "Regular expression passed to REGEXP_EXTRACT_ALL must "
               "not have more than 1 capturing group");
  }
  if (last_match_) {
    *out = absl::string_view(nullptr, 0);
    return false;
  }
  RE2StringView groups[2];
  if (!re_->Match(extract_all_input_,
                  extract_all_position_,      // startpos
                  extract_all_input_.size(),  // endpos
                  RE2::UNANCHORED, groups,
                  2)) {  // number of matches at out.
    *out = absl::string_view(nullptr, 0);
    // No matches found in the remaining of the input std::string.
    return false;
  }

  extract_all_position_ = groups[0].end() - extract_all_input_.begin();
  if (re_->NumberOfCapturingGroups() == 0) {
    // If there's no capturing subgroups return the entire matching substring.
    *out = {groups[0].data(), groups[0].length()};
  } else {
    // If there's a single capturing group return substring matching that
    // group.
    *out = {groups[1].data(), groups[1].length()};
  }
  // RE2 library produces empty groups[0] when regular expression matches empty
  // std::string, so in this case we need to advance input by one character.
  if (groups[0].empty() &&
      extract_all_position_ < static_cast<int64_t>(extract_all_input_.size())) {
    if (re_->options().utf8()) {
      constexpr int64_t kMaxUtf8Length = 4;
      int64_t length_after_position =
          extract_all_input_.size() - extract_all_position_;
      int32_t length_after_position32 =
          static_cast<int32_t>(std::min(kMaxUtf8Length, length_after_position));

      int32_t character_length = 0;
      UChar32 character;
      U8_NEXT(&extract_all_input_[extract_all_position_], character_length,
              length_after_position32, character);
      extract_all_position_ += character_length;

      if (character < 0) {
        error->Update(
            zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                         "Input argument to REGEXP_EXTRACT_ALL function "
                         "is not valid UTF8 string"));
        return false;
      }
    } else {
      extract_all_position_++;
    }
  }
  // No more input - next call to ExtractAllNext will return false.
  if (extract_all_position_ >= static_cast<int64_t>(extract_all_input_.size())) {
    last_match_ = true;
  }
  return true;
}

bool RegExp::Replace(absl::string_view str, absl::string_view newsub,
                     std::string* out, zetasql_base::Status* error) {
  // The following implementation is similar to RE2::GlobalReplace, with a few
  // important differences: (1) it works correctly with UTF-8 strings,
  // (2) it returns proper error message instead of logging it, and
  // (3) limits the size of output std::string.

  DCHECK(re_);

  std::string error_string;
  if (!re_->CheckRewriteString(newsub, &error_string)) {
    error->Update(zetasql_base::Status(
        zetasql_base::StatusCode::kOutOfRange,
        absl::StrCat("Invalid REGEXP_REPLACE pattern: ", error_string)));
    return false;
  }

  // "newsub" can reference at most 9 capturing groups indexed 1 to 9. Index 0
  // is reserved for the entire matching substring.
  std::vector<RE2StringView> match(10);

  out->clear();
  // Points to the end of the previous match. This is necessary if the regular
  // expression can match both empty std::string and some non-empty std::string, so that
  // we don't replace an empty match immediately following non-empty match.
  const char* lastend = nullptr;
  for (const char* p = str.data(); p <= str.end(); ) {
    // Find the first matching substring starting at p and store the
    // match and captured groups in vector 'match'.
    if (!re_->Match(str, p - str.data(), str.size(),
                    RE2::UNANCHORED, /* match any substring */
                    match.data(), match.size())) {
      out->append(p, str.end());
      break;
    }
    // Emit text until the start of the match verbatim, and then emit
    // the rewritten match.
    out->append(p, match[0].begin());
    p = match[0].begin();
    if (!match[0].empty()) {
      if (!Rewrite(newsub, match, out, error)) return false;
      p = match[0].end();
    } else {
      // The regexp matches empty substring. Ignore the match if it starts at
      // the end of the previous one.
      if (match[0].begin() != lastend &&
          !Rewrite(newsub, match, out, error)) {
        return false;
      }
      if (p < str.end()) {
        // Move p one character forward.
        int32_t len;
        if (re_->options().utf8()) {
          int32_t char_len = 0;
          constexpr std::ptrdiff_t kMaxUtf8Length = 4;
          U8_FWD_1(p, char_len, std::min(kMaxUtf8Length, str.end() - p));
          len = char_len;
        } else {
          len = 1;
        }
        out->append(p, len);
        p += len;
      } else {
        break;
      }
    }
    lastend = match[0].end();
  }

  return true;
}

void RegExp::SetMaxOutSize(int32_t size) {
  max_out_size_ = size;
}

template <typename StringViewType>
bool RegExp::Rewrite(const absl::string_view rewrite,
                     const std::vector<StringViewType>& groups,
                     std::string* out, zetasql_base::Status* error) {
  for (const char *s = rewrite.data(); s < rewrite.end(); ++s) {
    const char* start = s;
    while (s < rewrite.end() && *s != '\\') s++;
    out->append(start, s);

    if (s < rewrite.end()) {
      s++;
      int c = (s < rewrite.end()) ? *s : -1;
      if (isdigit(c)) {
        int n = (c - '0');
        out->append(groups[n].data(), groups[n].size());
      } else if (c == '\\') {
        out->push_back('\\');
      } else {
        error->Update(zetasql_base::Status(zetasql_base::StatusCode::kInternal,
                                   "Invalid REGEXP_REPLACE pattern"));
        return false;
      }
    }

    if (out->length() > max_out_size_) {
      error->Update(zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                                 "REGEXP_REPLACE: exceeded maximum output "
                                 "length"));
      return false;
    }
  }
  return true;
}

}  // namespace functions
}  // namespace zetasql
