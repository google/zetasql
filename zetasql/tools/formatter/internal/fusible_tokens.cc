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

#include "zetasql/tools/formatter/internal/fusible_tokens.h"

#include <array>
#include <iterator>
#include <ostream>
#include <string>
#include <vector>

#include "zetasql/tools/formatter/internal/token.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"

namespace zetasql::formatter::internal {
namespace {

// Recursive function that searches for a longest fusible sequence in `tokens`
// and returns corresponding match vector.
// `current_match` contains the required context: the last matched token index
// and the last matched FusibleGroup.
FusibleMatchVector FindLongestFusibleMatch(
    const std::vector<Token*>& tokens,
    const FusibleMatchVector& current_match) {
  const FusibleGroup& fusible_group = current_match.CurrentFusibleGroup();
  if (fusible_group.next_tokens.empty()) {
    // We reached the end of a fusible group - nothing else can be fused.
    return current_match;
  }
  int next_token = current_match.EndPosition() + 1;
  // Consume inline comments and query hints in the middle of a match.
  if (!current_match.Empty() && next_token < tokens.size()) {
    if (tokens[next_token]->GetKeyword() == "@") {
      if (next_token + 1 < tokens.size() && tokens[next_token + 1]->IsValue()) {
        // A query hint like "@number" - for number of shards.
        next_token += 2;
      } else if (next_token + 3 < tokens.size() &&
                 tokens[next_token + 1]->GetKeyword() == "{") {
        // A query hint like "@{hint=value}"
        int closing_curly_brace = next_token + 2;
        while (closing_curly_brace < tokens.size() &&
               tokens[closing_curly_brace]->GetKeyword() != "}") {
          closing_curly_brace++;
        }
        if (closing_curly_brace + 1 < tokens.size()) {
          next_token = closing_curly_brace + 1;
        }
      }
    }
    while (next_token < tokens.size() &&
           tokens[next_token]->IsInlineComment()) {
      next_token++;
    }
  }
  if (next_token == tokens.size() || tokens[next_token]->IsComment()) {
    // We reached the last token, or a stand-alone comment which cannot be a
    // part of a fusible group.
    return current_match;
  }

  int max_token = current_match.EndPosition();
  auto best_match = FusibleMatchVector::EmptyMatch(0);
  int match_pos;
  for (const auto& [fused_keyword, next_group] : fusible_group.next_tokens) {
    match_pos = next_token;
    if (fused_keyword == FusibleTokens::kIdentifier) {
      // Current token should be a valid identifier start.
      if (!tokens[match_pos]->MayBeStartOfIdentifier()) {
        continue;
      }
      // Catch further parts of the identifier.
      while (match_pos + 1 < tokens.size() &&
             tokens[match_pos + 1]->MayBeIdentifierContinuation(
                 *tokens[match_pos])) {
        ++match_pos;
      }
    } else if (fused_keyword == FusibleTokens::kAnyWord) {
      if (!tokens[match_pos]->Is(Token::Type::INVALID_TOKEN) &&
          !tokens[match_pos]->IsNonPunctuationKeyword() &&
          !tokens[match_pos]->IsIdentifier()) {
        // kAny should match any identifier or alphabetic keyword (punctuation
        // like "(" are also keywords) - no match.
        continue;
      }
    } else if (tokens[match_pos]->GetKeyword() != fused_keyword ||
               (next_group.type != Token::Type::UNKNOWN &&
                !tokens[match_pos]->Is(next_group.type)) ||
               tokens[match_pos]->IsOneOf(
                   {Token::Type::KEYWORD_AS_IDENTIFIER_FRAGMENT,
                    Token::Type::COMPLEX_TOKEN_CONTINUATION,
                    Token::Type::CASE_KEYWORD})) {
      // No match.
      continue;
    }

    FusibleMatchVector match = FindLongestFusibleMatch(
        tokens, current_match.Append(next_group, match_pos));
    if (match.EndPosition() > max_token ||
        // If match length is equal, prefer the match that ends on a specific
        // keyword or identifier.
        (match.EndPosition() == max_token && !match.Empty() &&
         match.CurrentFusibleGroup().token != FusibleTokens::kAnyWord)) {
      best_match = match;
      max_token = match.EndPosition();
    }
  }
  return best_match.Empty() ? current_match : best_match;
}

// Marks keyword tokens that matched given fusible sequence as keywords.
// This allows judging whether a non-reserved keyword is a keyword or
// identifier.
void AnnotateMatchedTokensAsKeywords(const std::vector<Token*>& tokens,
                                     const FusibleMatchVector& match_vector) {
  if (match_vector.MatchSize() < 2) {
    // Annotate as keywords only if the match contains at least 2 tokens.
    // Otherwise, this will annotate single-standing keywords, that appear to be
    // starting a fusing group. E.g., a match could be a single "SHOW", which
    // might act as an identifier.
    return;
  }
  FusibleMatchVector::const_iterator match_it = match_vector.Begin();
  FusibleMatchVector::const_iterator last_keyword_to_mark =
      --match_vector.End();
  // Remember if the last match says the whole group start has to be marked as
  // certain type.
  const Token::Type mark_as = last_keyword_to_mark->group->mark_as;
  // The trailing match for kAny may be not a keyword; kIdentifier match is
  // also not a keyword - skip those.
  while (last_keyword_to_mark != match_it &&
         (last_keyword_to_mark->group->token == FusibleTokens::kAnyWord ||
          last_keyword_to_mark->group->token == FusibleTokens::kIdentifier)) {
    --last_keyword_to_mark;
  }
  for (int i = match_vector.StartIndex();
       i <= last_keyword_to_mark->end_position; ++i) {
    // Consume inline comments.
    if (tokens[i]->IsComment()) {
      continue;
    }
    if (match_it->group->token == FusibleTokens::kIdentifier) {
      // Skip all tokens which appear to be a part of identifier.
      i = match_it->end_position;
    } else {
      // Mark keywords as keywords.
      if (tokens[i]->IsKeyword()) {
        tokens[i]->MarkUsedAsKeyword();
        if (mark_as != Token::Type::UNKNOWN &&
            match_it == match_vector.Begin()) {
          // Mark the first keyword in the match.
          tokens[i]->SetType(mark_as);
        }
      }
    }
    if (match_it == last_keyword_to_mark) {
      break;
    }
    ++match_it;
  }
}

}  // namespace

const std::vector<FusibleTokens>* GetFusibleTokens() {
  // Note that partial matches are OK; for instance "CROSS JOIN" is a fusible
  // group of length 2, since it matches the first 2 tokens of {"CROSS", kAny,
  // "JOIN"}. kAny matches any keyword, so "CROSS" plus any subsequent token
  // will return a fusible group of length at least 2 (3 if the next token is
  // JOIN).
  const auto& kAny = FusibleTokens::kAnyWord;
  const auto& kId = FusibleTokens::kIdentifier;
  const auto kTopLevel = Token::Type::TOP_LEVEL_KEYWORD;
  const auto kDdlKeyword = Token::Type::DDL_KEYWORD;
  static const auto* fusible_tokens = new std::vector<FusibleTokens>({
      // Note: cannot supports this open source due to initialization pattern.
  });
  return fusible_tokens;
}

const FusibleGroup* GetFusibleGroups() {
  static const FusibleGroup* fusible_groups =
      FusibleGroupsFromTokens(*GetFusibleTokens());
  return fusible_groups;
}

const FusibleGroup* FusibleGroupsFromTokens(
    const std::vector<FusibleTokens>& fusible_tokens) {
  auto* root = new FusibleGroup();
  root->token = FusibleGroup::kRoot;
  for (const auto& tokens : fusible_tokens) {
    auto* current_group = root;
    for (const auto& token : tokens.t) {
      auto& fused_group = current_group->next_tokens[token];
      fused_group.token = token;
      fused_group.full_match_only = tokens.full_match_only;
      if (current_group == root) {
        fused_group.type = tokens.start_with;
      }
      current_group = &fused_group;
    }
    current_group->mark_as = tokens.mark_as;
  }

  return root;
}

bool operator==(const FusibleTokens& a, const FusibleTokens& b) {
  return a.t == b.t && a.mark_as == b.mark_as && a.start_with == b.start_with;
}

bool operator<(const FusibleTokens& a, const FusibleTokens& b) {
  if (a.t == b.t) {
    if (a.start_with == b.start_with) {
      return a.mark_as < b.mark_as;
    }
    return a.start_with < b.start_with;
  }
  return a.t < b.t;
}

std::ostream& operator<<(std::ostream& os, const FusibleTokens& tokens) {
  os << "{{" << absl::StrJoin(tokens.t, ", ") << "}";
  if (tokens.mark_as != Token::Type::UNKNOWN) {
    os << absl::StrCat(" mark_as=", tokens.mark_as);
  }
  if (tokens.start_with != Token::Type::UNKNOWN) {
    os << absl::StrCat(" start_with=", tokens.start_with);
  }
  os << "}";
  return os;
}

FusibleMatchVector FusibleMatchVector::EmptyMatch(int start_index) {
  FusibleMatchVector match;
  match.push_back(FusibleMatch{.group = GetFusibleGroups(),
                               .end_position = start_index - 1});
  return match;
}

FusibleMatchVector FusibleMatchVector::Append(const FusibleGroup& matched_group,
                                              int end_position) const {
  FusibleMatchVector result = *this;
  result.push_back(
      FusibleMatch{.group = &matched_group, .end_position = end_position});
  return result;
}

bool FusibleMatchVector::Empty() const { return size() <= 1; }

int FusibleMatchVector::StartIndex() const { return front().end_position + 1; }

int FusibleMatchVector::EndPosition() const { return back().end_position; }

int FusibleMatchVector::MatchSize() const {
  return static_cast<int>(size()) - 1;
}

void FusibleMatchVector::PopBack() { pop_back(); }

const FusibleGroup& FusibleMatchVector::CurrentFusibleGroup() const {
  return *back().group;
}

FusibleMatchVector::const_iterator FusibleMatchVector::Begin() const {
  return ++cbegin();
}

FusibleMatchVector::const_iterator FusibleMatchVector::End() const {
  return cend();
}

int FindMaxFusibleTokenIndex(const std::vector<Token*>& tokens,
                             int start_index) {
  auto match = FusibleMatchVector::EmptyMatch(start_index);
  match = FindLongestFusibleMatch(tokens, match);
  if (match.Empty()) {
    return start_index;
  }

  // If match ends on a wildcard match (`kAnyWord`), which is in the middle of a
  // fusible sequence - discard the last match. This is done to avoid grouping
  // "LEFT Foo", which is a partial match for "LEFT <kAny> JOIN".
  if (match.CurrentFusibleGroup().token == FusibleTokens::kAnyWord &&
      !match.CurrentFusibleGroup().next_tokens.empty()) {
    match.PopBack();
  }
  // If only a full match should be accepted, and the match ends in the middle
  // of a fusible sequence, the match is discarded.
  if (match.CurrentFusibleGroup().full_match_only) {
    if (!match.CurrentFusibleGroup().next_tokens.empty()) {
      return start_index;
    }
  }
  if (match.Empty()) {
    return start_index;
  }

  AnnotateMatchedTokensAsKeywords(tokens, match);

  return match.EndPosition();
}

}  // namespace zetasql::formatter::internal
