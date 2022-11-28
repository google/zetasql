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
      // (broken link) start
      // ADD ZETASQL_CHECK would be normally followed by an identifier, but
      // it is enough to fuse a single token only and if it was a part of
      // multi-token identifier, IsPartOfSameChunk will handle it.
      {.t{"ADD", "CHECK", kAny}, .mark_as = kTopLevel},
      {.t{"ADD", "COLUMN", kAny}, .mark_as = kTopLevel},
      {.t{"ADD", "CONSTRAINT", kAny}, .mark_as = kTopLevel},
      {.t{"ADD", "FILTER"}, .mark_as = kTopLevel},
      {.t{"ADD", "INDEX"}, .mark_as = kTopLevel},
      {.t{"ADD", kAny, "IF", "EXISTS", kAny}, .mark_as = kTopLevel},
      {.t{"ADD", kAny, "KEY"}, .mark_as = kTopLevel},
      {.t{"AFTER", kId}, .start_with = kDdlKeyword},
      {.t{"ALL", "PRIVILEGES"}},
      {.t{"ALTER", "ALL", "ROW", "ACCESS", "POLICIES"}},
      {.t{"ALTER", "ALL", "ROW", "POLICIES"}},
      {.t{"ALTER", "COLUMN", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "CONSTRAINT", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "DATABASE", kAny}},
      {.t{"ALTER", "FUNCTION", kAny}},
      {.t{"ALTER", "INDEX", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "MATERIALIZED", "VIEW", "IF", "EXISTS", kAny}},
      {.t{"ALTER", "MATERIALIZED", "VIEW", kAny}},
      {.t{"ALTER", "MODEL", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "PROCEDURE", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "ROUTINE", kAny}, .mark_as = kTopLevel},
      {.t{"ALTER", "ROW", "ACCESS", "POLICY", "IF", "EXISTS"}},
      {.t{"ALTER", "ROW", "POLICY", "IF", "EXISTS"}},
      {.t{"ALTER", "SCHEMA", kAny}},
      {.t{"ALTER", "TABLE", kAny}},
      {.t{"ALTER", "VIEW", kAny}},
      {.t{"ALTER", kAny, "FUNCTION", "IF", "EXISTS", kAny}},
      {.t{"ALTER", kAny, "FUNCTION", kAny}},
      {.t{"ALTER", kAny, "IF", "EXISTS", kAny}, .mark_as = kTopLevel},
      {.t{"ANY", "TABLE"}},
      {.t{"ANY", "TYPE"}},
      {.t{"ASSERT", "WARNING"}},
      {.t{"CHANGE", "COLUMN"}},
      {.t{"CLONE", "DATA", "INTO", kAny}},
      {.t{"CLUSTER", "BY"}},
      {.t{"CONSTRAINT", kAny}, .start_with = kDdlKeyword},
      {.t{"COUNT", "(", "*", ")"}},
      {.t{"CREATE", "CONSTANT", "IF", "NOT", "EXISTS", kId, "="}},
      {.t{"CREATE", "CONSTANT", kId, "="}},
      {.t{"CREATE", "DATABASE", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", "DATABASE", kAny}},
      {.t{"CREATE", "FUNCTION", kAny}},
      {.t{"CREATE", "OR", "REPLACE", "CONSTANT", kId, "="}},
      {.t{"CREATE", "OR", "REPLACE", "FUNCTION", kAny}},
      {.t{"CREATE", "OR", "REPLACE", "ROW", "ACCESS", "POLICY", "IF", "NOT",
          "EXISTS"}},
      {.t{"CREATE", "OR", "REPLACE", "ROW", "POLICY", "IF", "NOT", "EXISTS"}},
      {.t{"CREATE", "OR", "REPLACE", "TABLE", kAny}},
      {.t{"CREATE", "OR", "REPLACE", "TYPE", kAny}},
      {.t{"CREATE", "OR", "REPLACE", "VIEW", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "AGGREGATE", "FUNCTION", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "CONSTANT", kId, "="}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "EXTERNAL", "TABLE", "IF", "NOT",
          "EXISTS", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "EXTERNAL", "TABLE", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "FUNCTION", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "TABLE", "FUNCTION", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "TABLE", "IF", "NOT", "EXISTS",
          kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "TABLE", kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "VIEW", "IF", "NOT", "EXISTS",
          kAny}},
      {.t{"CREATE", "OR", "REPLACE", kAny, "VIEW", kAny}},
      {.t{"CREATE", "ROLE"}},
      {.t{"CREATE", "ROW", "ACCESS", "POLICY", "IF", "NOT", "EXISTS"}},
      {.t{"CREATE", "ROW", "POLICY", "IF", "NOT", "EXISTS"}},
      {.t{"CREATE", "TABLE", kAny}},
      {.t{"CREATE", "TYPE", kAny}},
      {.t{"CREATE", "VIEW", kAny}},
      // 2nd token can be TEMP, TEMPORARY, PRIVATE, PUBLIC.  Even if it is
      // misspelled (and not valid SQL), treat as a fused group for
      // simplicity.
      {.t{"CREATE", kAny, "AGGREGATE", "FUNCTION", "IF", "NOT", "EXISTS",
          kAny}},
      {.t{"CREATE", kAny, "AGGREGATE", "FUNCTION", kAny}},
      {.t{"CREATE", kAny, "CONSTANT", "IF", "NOT", "EXISTS", kId, "="}},
      {.t{"CREATE", kAny, "CONSTANT", kId, "="}},
      {.t{"CREATE", kAny, "EXTERNAL", "TABLE", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "EXTERNAL", "TABLE", kAny}},
      {.t{"CREATE", kAny, "FUNCTION", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "FUNCTION", kAny}},
      {.t{"CREATE", kAny, "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "TABLE", "FUNCTION", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "TABLE", "FUNCTION", kAny}},
      {.t{"CREATE", kAny, "TABLE", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "TABLE", kAny}},
      {.t{"CREATE", kAny, "VIEW", "IF", "NOT", "EXISTS", kAny}},
      {.t{"CREATE", kAny, "VIEW", kAny}},
      {.t{"CROSS", "JOIN"}, .mark_as = kTopLevel},
      {.t{"CROSS", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"CURRENT", "ROW"}},
      {.t{"DEFAULT", "NULL"}, .start_with = kDdlKeyword},
      {.t{"DEFAULT", kAny}, .start_with = kDdlKeyword},
      {.t{"DEFINE", "INLINE", "TABLE", kId}},
      {.t{"DEFINE", "MACRO", kAny}},
      {.t{"DEFINE", "PERMANENT", "TABLE", kId}},
      {.t{"DEFINE", "TABLE", kId}},
      {.t{"DELETE", "FROM", kAny}},
      {.t{"DESC", "TABLE", kId}},
      {.t{"DESCRIBE", "TABLE", kId}},
      {.t{"DROP", "ALL", "FILTERS"}, .mark_as = kTopLevel},
      {.t{"DROP", "ALL", "ROW", "ACCESS", "POLICIES"}},
      {.t{"DROP", "ALL", "ROW", "POLICIES"}},
      {.t{"DROP", "COLUMN", kAny}, .mark_as = kTopLevel},
      {.t{"DROP", "CONSTRAINT", kAny}, .mark_as = kTopLevel},
      {.t{"DROP", "FILTERS", "FOR"}, .mark_as = kTopLevel},
      {.t{"DROP", "FILTERS", "FROM"}, .mark_as = kTopLevel},
      {.t{"DROP", "FILTERS"}, .mark_as = kTopLevel},
      {.t{"DROP", "INDEX"}, .mark_as = kTopLevel},
      {.t{"DROP", "ROW", "ACCESS", "POLICY", "IF", "EXISTS"}},
      {.t{"DROP", "ROW", "POLICY", "IF", "EXISTS"}},
      {.t{"DROP", "TABLE", kAny}},
      {.t{"DROP", "VIEW", kAny}},
      {.t{"DROP", kAny, "IF", "EXISTS", kAny}, .mark_as = kTopLevel},
      {.t{"DROP", kAny, "KEY", kAny}, .mark_as = kTopLevel},
      {.t{"EXCEPT", "ALL"}},
      {.t{"EXCEPT", "DISTINCT"}},
      {.t{"EXPORT", "DATA"}},
      {.t{"FILL", "USING", kAny}, .start_with = kDdlKeyword},
      {.t{"FILTER", "USING"}, .mark_as = kTopLevel},
      {.t{"FOLLOWING", kId}, .start_with = kDdlKeyword},
      {.t{"FOREIGN", "KEY"}},
      {.t{"FULL", "JOIN"}, .mark_as = kTopLevel},
      {.t{"FULL", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"FULL", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"GENERATED", "AS"}, .start_with = kDdlKeyword},
      {.t{"GRANT", "TO"}},
      {.t{"GROUP", "BY"}},
      {.t{"HASH", "JOIN"}, .mark_as = kTopLevel},
      {.t{"HAVING", "MAX"}},
      {.t{"HAVING", "MIN"}},
      {.t{"IGNORE", "NULLS"}},
      {.t{"IMPORT", "MODULE"}, .start_with = kTopLevel},
      {.t{"IMPORT", "PROTO"}, .start_with = kTopLevel},
      {.t{"IN", "UNNEST"}},
      {.t{"INNER", "JOIN"}, .mark_as = kTopLevel},
      {.t{"INNER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"INSERT", "IGNORE", "INTO", kId}},
      {.t{"INSERT", "IGNORE", kId}},
      {.t{"INSERT", "INTO", kId}},
      {.t{"INSERT", "OR", "IGNORE", "INTO", kId}},
      {.t{"INSERT", "OR", "IGNORE", kId}},
      {.t{"INSERT", "OR", "REPLACE", "INTO", kId}},
      {.t{"INSERT", "OR", "REPLACE", kId}},
      {.t{"INSERT", "OR", "UPDATE", "INTO", kId}},
      {.t{"INSERT", "OR", "UPDATE", kId}},
      {.t{"INSERT", "REPLACE", "INTO", kId}},
      {.t{"INSERT", "REPLACE", kId}},
      {.t{"INSERT", "UPDATE", "INTO", kId}},
      {.t{"INSERT", "UPDATE", kId}},
      {.t{"INSERT", "VALUES"}},
      {.t{"INTERSECT", "ALL"}},
      {.t{"INTERSECT", "DISTINCT"}},
      {.t{"IS", "FALSE"}},
      {.t{"IS", "NOT", "FALSE"}},
      {.t{"IS", "NOT", "NULL"}},
      {.t{"IS", "NOT", "TRUE"}},
      {.t{"IS", "NULL"}},
      {.t{"IS", "TRUE"}},
      {.t{"LEFT", "JOIN"}, .mark_as = kTopLevel},
      {.t{"LEFT", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"LEFT", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"LOCK", "TABLES"}},
      {.t{"LOOKUP", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "CROSS", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "CROSS", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "FULL", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "FULL", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "FULL", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "HASH", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "INNER", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "INNER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "LEFT", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "LEFT", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "LEFT", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "LOOKUP", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "RIGHT", "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "RIGHT", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NATURAL", "RIGHT", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"NEW", kAny}},
      {.t{"NOT", "BETWEEN"}},
      {.t{"NOT", "DETERMINISTIC"}, .mark_as = kTopLevel},
      {.t{"NOT", "ENFORCED"}, .mark_as = kDdlKeyword},
      {.t{"NOT", "IN", "UNNEST"}},
      {.t{"NOT", "LIKE"}},
      {.t{"ON", "TABLE", kAny}},
      {.t{"ON", "VIEW", kAny}},
      {.t{"ORDER", "BY"}},
      {.t{"PARTITION", "BY"}},
      {.t{"PRECEDING", kId}, .start_with = kDdlKeyword},
      {.t{"PRIMARY", "KEY"}},
      {.t{"REFERENCES", kId}, .start_with = kDdlKeyword},
      {.t{"RENAME", "TO", kAny}, .mark_as = kTopLevel},
      {.t{"REPLACE", "FILTER"}, .mark_as = kTopLevel},
      {.t{"REPLICATION", "CLIENT"}},
      {.t{"REPLICATION", "SLAVE"}},
      {.t{"RESPECT", "NULLS"}},
      {.t{"REVOKE", "FROM"}, .mark_as = kTopLevel},
      {.t{"RIGHT", "JOIN"}, .mark_as = kTopLevel},
      {.t{"RIGHT", "OUTER", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"RIGHT", kAny, "JOIN"}, .mark_as = kTopLevel},
      {.t{"SELECT", "*", "FROM", "UNNEST", "("}, .full_match_only = true},
      {.t{"SELECT", "ALL", "AS", "VALUE"}},
      {.t{"SELECT", "ALL", "AS", kAny}},
      {.t{"SELECT", "AS", "VALUE"}},
      {.t{"SELECT", "AS", kAny}},
      {.t{"SELECT", "DISTINCT", "AS", "VALUE"}},
      {.t{"SELECT", "DISTINCT", "AS", kAny}},
      {.t{"SELECT", "WITH", "ANONYMIZATION"}},
      {.t{"SET", "AS", kAny}},
      {.t{"SET", "DATA", "TYPE", kAny}},
      {.t{"SET", "OPTIONS"}},
      {.t{"SET", kId, "="}},
      {.t{"SHOW", "DATABASES"}},
      {.t{"SHOW", "MATERIALIZED", "VIEWS"}},
      {.t{"SQL", "SECURITY", "DEFINER"}},
      {.t{"SQL", "SECURITY", "INVOKER"}},
      {.t{"TABLE", kId}},
      {.t{"UNBOUNDED", "FOLLOWING"}},
      {.t{"UNBOUNDED", "PRECEDING"}},
      {.t{"UNION", "ALL"}},
      {.t{"UNION", "DISTINCT"}},
      {.t{"USE", "DATABASE", kAny}},
      {.t{"WHEN", "MATCHED"}},
      {.t{"WHEN", "NOT", "MATCHED", "BY", "SOURCE"}},
      {.t{"WITH", "RECURSIVE"}},
      {.t{"[", "DEFAULT", kId, "="},
       .mark_as = kTopLevel,
       .full_match_only = true},
      {.t{"[", kId, "="}, .mark_as = kTopLevel, .full_match_only = true},
      // (broken link) end
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
