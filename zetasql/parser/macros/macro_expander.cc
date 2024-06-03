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

#include "zetasql/parser/macros/macro_expander.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/base/arena_allocator.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/thread_stack.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/quoting.h"
#include "zetasql/parser/macros/standalone_macro_expansion.h"
#include "zetasql/parser/macros/token_provider_base.h"
#include "zetasql/parser/macros/token_splicing_utils.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/base/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace parser {
namespace macros {

// This is a workaround until ZetaSQL's Bison is updated to a newer version
// and has this symbol.
#define YYUNDEF 257

// TODO: we should also employ a cycle detector to find infinitie
//           macro call recursion and give the users a good error message.
#define RETURN_ERROR_IF_OUT_OF_STACK_SPACE() \
  ZETASQL_RETURN_IF_NOT_ENOUGH_STACK(      \
      "Out of stack space due to deeply nested macro calls.")

static absl::string_view GetMacroName(
    const TokenWithLocation& macro_invocation_token) {
  return macro_invocation_token.text.substr(1);
}

// Note: end_offset is exclusive
static absl::string_view GetTextBetween(absl::string_view input, size_t start,
                                        size_t end) {
  ABSL_DCHECK_LE(start, end);
  ABSL_DCHECK_LE(start, input.length());
  size_t len = end - start;
  ABSL_DCHECK_LE(len, input.length());
  return absl::ClippedSubstr(input, start, len);
}

absl::StatusOr<int> ParseMacroArgIndex(absl::string_view text) {
  ZETASQL_RET_CHECK_GE(text.length(), 1);
  ZETASQL_RET_CHECK_EQ(text.front(), '$');
  int arg_index;
  ZETASQL_RET_CHECK(absl::SimpleAtoi(text.substr(1), &arg_index));
  return arg_index;
}

// Similar to IsKeywordOrUnquotedIdentifier, but also returns true for
// EXP_IN_FLOAT_NO_SIGN and STANDALONE_EXPONENT_SIGN because they can also be
// identifiers.
//
// Because a float can end with the token kind EXP_IN_FLOAT_NO_SIGN, such a
// float can be spliced with a following identifier or integer, for example
// "1.E10" splicing with a following "a" and becomes "1.E10a". We allow this
// extra splicing to happen in lenient mode.
static bool TokenCanBeKeywordOrUnquotedIdentifier(
    const TokenWithLocation& token) {
  switch (token.kind) {
    case EXP_IN_FLOAT_NO_SIGN:
    case STANDALONE_EXPONENT_SIGN:
      return true;
    default:
      return IsKeywordOrUnquotedIdentifier(token);
  }
}

// Returns true if the two tokens can be spliced into one
static bool CanSplice(const TokenWithLocation& previous_token,
                      const TokenWithLocation& current_token) {
  if (!TokenCanBeKeywordOrUnquotedIdentifier(previous_token)) {
    return false;
  }
  switch (current_token.kind) {
    case DECIMAL_INTEGER_LITERAL:
    case HEX_INTEGER_LITERAL:
      return true;
    default:
      return TokenCanBeKeywordOrUnquotedIdentifier(current_token);
  }
}

static bool AreSame(const QuotingSpec& q1, const QuotingSpec& q2) {
  ABSL_DCHECK(q1.literal_kind() == q2.literal_kind());

  if (q1.quote_kind() != q2.quote_kind()) {
    return false;
  }

  if (q1.prefix().length() != q2.prefix().length()) {
    return false;
  }

  return q1.prefix().length() == 2 ||
         zetasql_base::CaseEqual(q1.prefix(), q2.prefix());
}

bool MacroExpander::IsStrict() const {
  return language_options_.LanguageFeatureEnabled(
      FEATURE_V_1_4_ENFORCE_STRICT_MACROS);
}

static std::unique_ptr<zetasql_base::UnsafeArena> CreateUnsafeArena() {
  return std::make_unique<zetasql_base::UnsafeArena>(/*block_size=*/4096);
}

MacroExpander::MacroExpander(std::unique_ptr<TokenProviderBase> token_provider,
                             const LanguageOptions& language_options,
                             const MacroCatalog& macro_catalog,
                             zetasql_base::UnsafeArena* arena,
                             ErrorMessageOptions error_message_options,
                             StackFrame* parent_location)
    : MacroExpander(std::move(token_provider), language_options, macro_catalog,
                    arena, /*call_arguments=*/{}, error_message_options,
                    parent_location) {}

absl::StatusOr<ExpansionOutput> MacroExpander::ExpandMacros(
    std::unique_ptr<TokenProviderBase> token_provider,
    const LanguageOptions& language_options, const MacroCatalog& macro_catalog,
    ErrorMessageOptions error_message_options) {
  ExpansionOutput expansion_output;
  expansion_output.arena = CreateUnsafeArena();
  ZETASQL_RETURN_IF_ERROR(ExpandMacrosInternal(
      std::move(token_provider), language_options, macro_catalog,
      expansion_output.arena.get(), /*call_arguments=*/{},
      error_message_options, /*parent_location=*/nullptr,
      &expansion_output.location_map, expansion_output.expanded_tokens,
      expansion_output.warnings,
      /*out_max_arg_ref_index=*/nullptr));
  return expansion_output;
}

absl::Status MacroExpander::MakeSqlErrorAt(const ParseLocationPoint& location,
                                           absl::string_view message) {
  zetasql_base::StatusBuilder status_builder = MakeSqlError() << message;
  InternalErrorLocation internal_location = location.ToInternalErrorLocation();

  StackFrame* next_ancestor = parent_location_;
  std::vector<ErrorSource> error_sources;
  while (next_ancestor != nullptr) {
    error_sources.push_back(next_ancestor->error_source);
    next_ancestor = next_ancestor->parent;
  }
  // ErrorSources are supposed to be supplied in the reverse order of display.
  // See the ErrorSource proto definition.
  std::reverse(error_sources.begin(), error_sources.end());
  for (auto& error_source : error_sources) {
    *internal_location.add_error_source() = std::move(error_source);
  }

  status_builder.AttachPayload(std::move(internal_location));
  absl::Status status = ConvertInternalErrorLocationToExternal(
      std::move(status_builder), token_provider_->input(),
      error_message_options_.input_original_start_line - 1,
      error_message_options_.input_original_start_column - 1);
  return MaybeUpdateErrorFromPayload(error_message_options_,
                                     token_provider_->input(), status);
}

absl::StatusOr<TokenWithLocation> MacroExpander::GetNextToken() {
  // The loop is needed to skip chunks that all expand into empty. For example:
  // $empty   $empty  $empty $empty  (...etc)
  // Eventually we will hit a nonempty expansion, or YYEOF in the output buffer.
  while (output_token_buffer_.empty()) {
    ZETASQL_RETURN_IF_ERROR(LoadPotentiallySplicingTokens());
    ZETASQL_RETURN_IF_ERROR(ExpandPotentiallySplicingTokens());
  }

  TokenWithLocation token = output_token_buffer_.ConsumeToken();
  if (token.kind == ';' || token.kind == YYEOF) {
    at_statement_start_ = true;
    inside_macro_definition_ = false;
    ZETASQL_RET_CHECK(output_token_buffer_.empty());
  }

  if (parent_location_ != nullptr) {
    token.topmost_invocation_location = parent_location_->location;
  }
  return token;
}

int MacroExpander::num_unexpanded_tokens_consumed() const {
  return token_provider_->num_consumed_tokens();
}

// Returns true if `token` may splice from the right of `last_token`.
// `last_was_macro_invocation` is passed separately because a macro invocation
// has multiple tokens when it has arguments, so last_token would only reflect
// the closing parenthesis in that case.
static bool CanUnexpandedTokensSplice(bool last_was_macro_invocation,
                                      const TokenWithLocation& last_token,
                                      const TokenWithLocation& token) {
  if (token.start_offset() > last_token.end_offset()) {
    // Forced space: we will never splice!
    return false;
  }

  if (token.kind == MACRO_INVOCATION ||
      token.kind == MACRO_ARGUMENT_REFERENCE) {
    // Invocations and macro args can splice with pretty much anything.
    // Safer to always load them.
    return true;
  }

  if (token.kind == DECIMAL_INTEGER_LITERAL ||
      token.kind == HEX_INTEGER_LITERAL) {
    // Those will never splice with a previous non-macro token.
    // Otherwise, it'd have already lexed with it.
    return last_was_macro_invocation;
  }

  // Everything else: we have 2 categories:
  // 1. Unquoted identifiers and keywords: those can splice with macro
  //    invocations or argument references.
  // 2. Everything else, e.g. symbols, quoted IDs, string literals, etc, does
  //    not splice.
  if (!TokenCanBeKeywordOrUnquotedIdentifier(token)) {
    return false;
  }
  return last_was_macro_invocation ||
         last_token.kind == MACRO_ARGUMENT_REFERENCE;
}

static absl::string_view AllocateString(absl::string_view str,
                                        zetasql_base::UnsafeArena* arena) {
  return *::zetasql_base::NewInArena<std::basic_string<
      char, std::char_traits<char>, zetasql_base::ArenaAllocator<char, zetasql_base::UnsafeArena>>>(
      arena, str, arena);
}

absl::string_view MacroExpander::MaybeAllocateConcatenation(
    absl::string_view a, absl::string_view b) {
  if (a.empty()) {
    return b;
  }
  if (b.empty()) {
    return a;
  }

  return AllocateString(absl::StrCat(a, b), arena_);
}

absl::StatusOr<TokenWithLocation> MacroExpander::ConsumeInputToken() {
  ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token,
                   token_provider_->ConsumeNextToken());
  if (IsStrict()) {
    return token;
  }

  // TODO: jmorcos - Handle '$' here as well since it is also a lenient token.
  // Add a warning if this is a lenient token (Backslash or a generalized
  // identifier that starts with a number, e.g. 30d or 1ab23cd).
  if (token.kind == BACKSLASH ||
      (token.kind == IDENTIFIER &&
       std::isdigit(token.text.front() && !std::isdigit(token.text.back())))) {
    ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
        token.location.start(),
        absl::StrFormat(
            "Invalid token (%s). Did you mean to use it in a literal?",
            token.text))));
  }
  return token;
}

absl::Status MacroExpander::LoadPotentiallySplicingTokens() {
  ZETASQL_RET_CHECK(splicing_buffer_.empty());
  ZETASQL_RET_CHECK(output_token_buffer_.empty());

  // Special logic to find top-level DEFINE MACRO statement, which do not get
  // expanded.
  if (at_statement_start_ && call_arguments_.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token, token_provider_->PeekNextToken());
    if (token.kind == KW_DEFINE) {
      ZETASQL_ASSIGN_OR_RETURN(token, ConsumeInputToken());
      splicing_buffer_.push(token);

      ZETASQL_ASSIGN_OR_RETURN(token, token_provider_->PeekNextToken());
      if (token.kind == KW_MACRO) {
        // Mark the leading DEFINE keyword as the special one marking a DEFINE
        // MACRO statement.
        splicing_buffer_.back().kind = KW_DEFINE_FOR_MACROS;
        inside_macro_definition_ = true;
        at_statement_start_ = false;
        return absl::OkStatus();
      }
    }
  }

  at_statement_start_ = false;
  bool last_was_macro_invocation = false;

  // Unquoted identifiers and INT_LITERALs may only splice to the left if what
  // came before them was a macro invocation. Unquoted identifiers may further
  // splice with an argument reference to their left.
  while (true) {
    ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token, token_provider_->PeekNextToken());
    if (!splicing_buffer_.empty() &&
        !CanUnexpandedTokensSplice(last_was_macro_invocation,
                                   splicing_buffer_.back(), token)) {
      return absl::OkStatus();
    }
    ZETASQL_ASSIGN_OR_RETURN(token, ConsumeInputToken());
    splicing_buffer_.push(token);
    if (token.kind == YYEOF) {
      return absl::OkStatus();
    }

    if (token.kind == MACRO_INVOCATION) {
      ZETASQL_RETURN_IF_ERROR(LoadArgsIfAny());
      last_was_macro_invocation = true;
    } else {
      last_was_macro_invocation = false;
    }
  }

  ZETASQL_RET_CHECK_FAIL() << "We should never hit this path. We always return from "
                      "inside the loop";
}

absl::Status MacroExpander::LoadArgsIfAny() {
  ZETASQL_RET_CHECK(!splicing_buffer_.empty())
      << "Splicing buffer cannot be empty. This method should not be "
         "called except after a macro invocation has been loaded";
  ZETASQL_RET_CHECK(splicing_buffer_.back().kind == MACRO_INVOCATION)
      << "This method should not be called except after a macro invocation "
         "has been loaded";
  ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token, token_provider_->PeekNextToken());
  if (token.kind != '(' ||
      token.start_offset() > splicing_buffer_.back().end_offset()) {
    // The next token is not an opening parenthesis, or is an open parenthesis
    // that is separated by some whitespace. This means that the current
    // invocation does not have an argument list.
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(token, ConsumeInputToken());
  splicing_buffer_.push(token);
  return LoadUntilParenthesesBalance();
}

absl::Status MacroExpander::LoadUntilParenthesesBalance() {
  ZETASQL_RET_CHECK(!splicing_buffer_.empty());
  ZETASQL_RET_CHECK_EQ(splicing_buffer_.back().kind, '(');
  int num_open_parens = 1;
  while (num_open_parens > 0) {
    ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token, ConsumeInputToken());
    switch (token.kind) {
      case '(':
        num_open_parens++;
        break;
      case ')':
        num_open_parens--;
        break;
      case ';':
      case YYEOF:
        // Always an error, even when not in strict mode.
        return MakeSqlErrorAt(
            token.location.start(),
            "Unbalanced parentheses in macro argument list. Make sure that "
            "parentheses are balanced even inside macro arguments.");
      default:
        break;
    }
    splicing_buffer_.push(token);
  }

  return absl::OkStatus();
}

// Returns the given status as error if expanding in strict mode, or adds it
// as a warning otherwise.
// Note that not all problematic conditions can be relegated to warnings.
// For example, a macro invocation with unbalanced parens is always an error.
absl::Status MacroExpander::RaiseErrorOrAddWarning(absl::Status status) {
  ZETASQL_RET_CHECK(!status.ok());
  if (IsStrict()) {
    return status;
  }
  if (warnings_.size() < max_warnings_) {
    warnings_.push_back(std::move(status));
  } else if (warnings_.size() == max_warnings_) {
    // Add a "sentinel" warning indicating there were more.
    warnings_.push_back(absl::InvalidArgumentError(
        "Warning count limit reached. Truncating further warnings"));
  }
  return absl::OkStatus();
}

absl::StatusOr<TokenWithLocation> MacroExpander::Splice(
    TokenWithLocation pending_token, absl::string_view incoming_token_text,
    const ParseLocationPoint& location) {
  ZETASQL_RET_CHECK(!incoming_token_text.empty());
  ZETASQL_RET_CHECK(!pending_token.text.empty());
  ZETASQL_RET_CHECK_NE(pending_token.kind, YYUNDEF);

  ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
      location, absl::StrFormat("Splicing tokens (%s) and (%s)",
                                pending_token.text, incoming_token_text))));

  pending_token.text =
      MaybeAllocateConcatenation(pending_token.text, incoming_token_text);

  // The splicing token could be a keyword, e.g. FR+OM becoming FROM. But for
  // expansion purposes, these are all identifiers, and likely will splice with
  // more characters and not be a keyword. Re-lexing happens at the very end.
  // TODO: strict mode should produce an error when forming keywords through
  // splicing.
  pending_token.kind = IDENTIFIER;
  return pending_token;
}

absl::StatusOr<TokenWithLocation> MacroExpander::ExpandAndMaybeSpliceMacroItem(
    TokenWithLocation unexpanded_macro_token, TokenWithLocation pending_token) {
  std::vector<TokenWithLocation> expanded_tokens;
  if (unexpanded_macro_token.kind == MACRO_ARGUMENT_REFERENCE) {
    ZETASQL_RETURN_IF_ERROR(
        ExpandMacroArgumentReference(unexpanded_macro_token, expanded_tokens));
  } else {
    ZETASQL_RET_CHECK_EQ(unexpanded_macro_token.kind, MACRO_INVOCATION);

    absl::string_view macro_name = GetMacroName(unexpanded_macro_token);
    std::optional<MacroInfo> macro_info = macro_catalog_.Find(macro_name);
    if (!macro_info.has_value()) {
      ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
          unexpanded_macro_token.location.start(),
          absl::StrFormat("Macro '%s' not found.", macro_name))));
      // In lenient mode, just return this token as is, and let the argument
      // list expand as if it were just normal text.
      return AdvancePendingToken(std::move(pending_token),
                                 std::move(unexpanded_macro_token));
    }
    ZETASQL_RETURN_IF_ERROR(ExpandMacroInvocation(unexpanded_macro_token, *macro_info,
                                          expanded_tokens));
  }

  ZETASQL_RET_CHECK(!expanded_tokens.empty())
      << "A proper expansion should have at least the YYEOF token at "
         "the end. Failure was when expanding "
      << unexpanded_macro_token.text;
  ZETASQL_RET_CHECK(expanded_tokens.back().kind == YYEOF);
  // Pop the trailing space at the end of the expansion, which is tacked
  // on to the YYEOF.
  expanded_tokens.pop_back();

  // Empty expansions are tricky. There are 2 cases, the 3rd is impossible:
  // 1. pending_token == YYUNDEF: only has whitespace, potentially carried from
  //    previous chunks as in: $empty $empty \t $empty
  // 2. pending_token may splice, e.g. part of an identifier (which means there
  //    can never be whitespace separation, or we wouldn't have chunked them
  //    together).
  // 3. (impossible) pending_token never splices, e.g. '+' (in which case we
  //     wouldn't be in the same chunk anyway)
  if (expanded_tokens.empty()) {
    if (!unexpanded_macro_token.preceding_whitespaces.empty()) {
      ZETASQL_RET_CHECK(pending_token.kind == YYUNDEF);
      pending_token.preceding_whitespaces = MaybeAllocateConcatenation(
          pending_token.preceding_whitespaces,
          unexpanded_macro_token.preceding_whitespaces);
    }
    return pending_token;
  }

  if (CanSplice(pending_token, expanded_tokens.front())) {
    ZETASQL_RET_CHECK(unexpanded_macro_token.preceding_whitespaces.empty());
    // TODO: warning on implicit splicing here and everywhere else
    // The first token will splice with what we currently have.
    // TODO: if we already have another token, do we
    // allow splicing identifiers into a keywod? e.g. FR + OM = FROM.
    //   1. Each on its own is an identifier. Together they're a
    //      keyword. This is token splicing. We probably should make
    //      this an error.
    // However:
    //   2. For all we know, more identifiers may come up. So the
    //      final token is FROMother.
    //   3. This is an open question, probably to be done at the final
    //      result like a validation pass. But even that likely won't
    //      get it, because we can't tell the difference between a
    //      quoted and an unquoted identifier.
    //   4. Long term, we'd like to stop splicing, and introduce an
    //      explicit splice operator like C's ##.
    //   5. Furthermore, a keyword like FROM may still be understood
    //      by the parser to be an identifier anyway, such as when it
    //      is preceded by a dot, e.g. 'a.FROM'.
    ZETASQL_ASSIGN_OR_RETURN(
        pending_token,
        Splice(std::move(pending_token), expanded_tokens.front().text,
               unexpanded_macro_token.location.start()));
  } else {
    // The first expanded token becomes the pending token, and takes on the
    // previous whitespace (for example from a series of $empty), as well as
    // the whitespace before the original macro token (but drops the leading
    // whitespaces from the definition.)
    expanded_tokens.front().preceding_whitespaces =
        unexpanded_macro_token.preceding_whitespaces;
    ZETASQL_ASSIGN_OR_RETURN(pending_token,
                     AdvancePendingToken(std::move(pending_token),
                                         std::move(expanded_tokens.front())));
  }

  if (expanded_tokens.size() > 1) {
    // Leading and trailing spaces around the expansion results are
    // discarded, but not the whitespaces within. Consequently, the
    // first and last tokens in an expansion may splice with the
    // calling context. Everything in the middle is kept as is, with
    // no splicing.
    output_token_buffer_.Push(std::move(pending_token));

    // Everything in the middle (all except the first and last) will
    // not splice.
    for (int i = 1; i < expanded_tokens.size() - 1; i++) {
      // Use the unexpanded token's location
      output_token_buffer_.Push(std::move(expanded_tokens[i]));
    }
    pending_token = expanded_tokens.back();
  }
  return pending_token;
}

absl::StatusOr<TokenWithLocation> MacroExpander::AdvancePendingToken(
    TokenWithLocation pending_token, TokenWithLocation incoming_token) {
  if (pending_token.kind != YYUNDEF) {
    output_token_buffer_.Push(std::move(pending_token));
  } else {
    ZETASQL_RET_CHECK(pending_token.text.empty());
    // Prepend any pending whitespaces. If there's none, we skip allocating
    // a copy of incoming_token.preceding_whitespaces.
    incoming_token.preceding_whitespaces =
        MaybeAllocateConcatenation(pending_token.preceding_whitespaces,
                                   incoming_token.preceding_whitespaces);
  }
  return incoming_token;
}

static bool IsQuotedLiteral(const TokenWithLocation& token) {
  return token.kind == STRING_LITERAL || token.kind == BYTES_LITERAL ||
         IsQuotedIdentifier(token);
}

absl::Status MacroExpander::ExpandPotentiallySplicingTokens() {
  ZETASQL_RET_CHECK(!splicing_buffer_.empty());
  TokenWithLocation pending_token{
      .kind = YYUNDEF,
      .location = ParseLocationRange{},
      .text = "",
      .preceding_whitespaces = pending_whitespaces_};
  // Do not forget to reset pending_whitespaces_
  pending_whitespaces_ = "";
  while (!splicing_buffer_.empty()) {
    TokenWithLocation token = splicing_buffer_.front();
    splicing_buffer_.pop();

    if (inside_macro_definition_) {
      // Check the pending token in case there were some pending spaces from the
      // previous statement. But there shouldn't be any splicing.
      ZETASQL_ASSIGN_OR_RETURN(
          pending_token,
          AdvancePendingToken(std::move(pending_token), std::move(token)));
    } else if (token.kind == MACRO_ARGUMENT_REFERENCE ||
               token.kind == MACRO_INVOCATION) {
      ZETASQL_ASSIGN_OR_RETURN(pending_token,
                       ExpandAndMaybeSpliceMacroItem(std::move(token),
                                                     std::move(pending_token)));
    } else if (IsQuotedLiteral(token)) {
      ZETASQL_ASSIGN_OR_RETURN(pending_token, ExpandLiteral(std::move(pending_token),
                                                    std::move(token)));
    } else if (CanSplice(pending_token, token)) {
      ZETASQL_ASSIGN_OR_RETURN(
          pending_token,
          Splice(std::move(pending_token), token.text, token.location.start()));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          pending_token,
          AdvancePendingToken(std::move(pending_token), std::move(token)));
    }
  }

  ZETASQL_RET_CHECK(pending_whitespaces_.empty());
  if (pending_token.kind != YYUNDEF) {
    output_token_buffer_.Push(std::move(pending_token));
    pending_whitespaces_ = "";
  } else if (!pending_token.preceding_whitespaces.empty()) {
    // This is the case where the last part of our chunk has all been empty
    // expansions. We need to hold onto these whitespaces for the next chunk.
    pending_whitespaces_ =
        AllocateString(pending_token.preceding_whitespaces, arena_);
  }
  return absl::OkStatus();
}

absl::Status MacroExpander::ParseAndExpandArgs(
    const TokenWithLocation& unexpanded_macro_invocation_token,
    std::vector<std::vector<TokenWithLocation>>& expanded_args,
    int& out_invocation_end_offset) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();
  ZETASQL_RET_CHECK(expanded_args.empty());

  out_invocation_end_offset =
      unexpanded_macro_invocation_token.location.end().GetByteOffset();

  // The first argument is the macro name
  absl::string_view macro_name =
      GetMacroName(unexpanded_macro_invocation_token);
  expanded_args.push_back(std::vector<TokenWithLocation>{
      {.kind = IDENTIFIER,
       .location = unexpanded_macro_invocation_token.location,
       .text = macro_name,
       .preceding_whitespaces = ""},
      {.kind = YYEOF,
       .location = unexpanded_macro_invocation_token.location,
       .text = "",
       .preceding_whitespaces = ""}});

  if (splicing_buffer_.empty() || splicing_buffer_.front().kind != '(') {
    ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
        unexpanded_macro_invocation_token.location.end(),
        absl::StrFormat("Invocation of macro '%s' missing argument list.",
                        macro_name))));
    return absl::OkStatus();
  }

  const TokenWithLocation opening_paren = splicing_buffer_.front();
  splicing_buffer_.pop();
  ZETASQL_RET_CHECK(opening_paren.kind == '(');
  int arg_start_offset = opening_paren.end_offset();

  // Parse arguments, each being a sequence of tokens.
  struct ParseRange {
    int start_offset = -1;
    int end_offset = -1;
  };
  std::vector<ParseRange> unexpanded_args;
  ZETASQL_RET_CHECK(!splicing_buffer_.empty());
  if (splicing_buffer_.front().kind == ')') {
    // Special case: empty parentheses mean zero arguments, not a single
    // empty argument!
    out_invocation_end_offset = splicing_buffer_.front().end_offset();
    splicing_buffer_.pop();
  } else {
    int num_open_parens = 1;
    while (num_open_parens > 0) {
      ZETASQL_RET_CHECK(!splicing_buffer_.empty());
      TokenWithLocation token = splicing_buffer_.front();
      splicing_buffer_.pop();

      // The current argument ends at the next top-level comma, or the closing
      // parenthesis. Note that an argument may itself have parentheses and
      // commas, for example:
      //     $m( x(  a  ,  b  ),  y  )
      // The arguments to the invocation of $m are `x(a,b)` and y.
      // The comma between `a` and `b` is internal, not top-level.
      if (token.kind == '(') {
        num_open_parens++;
      } else if (token.kind == ',' && num_open_parens == 1) {
        // Top-level comma means the end of the current argument
        unexpanded_args.push_back(
            {.start_offset = arg_start_offset,
             .end_offset =
                 token.start_offset() -
                 static_cast<int>(token.preceding_whitespaces.length())});
        arg_start_offset = token.end_offset();
      } else if (token.kind == ')') {
        num_open_parens--;
        if (num_open_parens == 0) {
          // This was the last argument.
          out_invocation_end_offset = token.end_offset();
          unexpanded_args.push_back(
              {.start_offset = arg_start_offset,
               .end_offset =
                   token.start_offset() -
                   static_cast<int>(token.preceding_whitespaces.length())});
          break;
        }
      }
    }
  }

  for (const auto& [arg_start_offset, arg_end_offset] : unexpanded_args) {
    std::vector<TokenWithLocation> expanded_arg;
    int max_arg_ref_index_in_current_arg;
    ZETASQL_RETURN_IF_ERROR(ExpandMacrosInternal(
        // Expand this arg, but note that we expand in raw tokenization mode:
        // We are not necessarily starting a statement or a script.
        // Pass the input from the beginning of the file, up to the end offset.
        // Note that the start offset is passed separately, in order to reflect
        // the correct location of each token.
        // If we only pass the arg as the input, the location offsets will start
        // from 0.
        token_provider_->CreateNewInstance(token_provider_->filename(),
                                           token_provider_->input(),
                                           arg_start_offset, arg_end_offset),
        language_options_, macro_catalog_, arena_, call_arguments_,
        error_message_options_, parent_location_, location_map_, expanded_arg,
        warnings_, &max_arg_ref_index_in_current_arg));

    max_arg_ref_index_ =
        std::max(max_arg_ref_index_, max_arg_ref_index_in_current_arg);
    ZETASQL_RET_CHECK(!expanded_arg.empty()) << "A proper expansion should have at "
                                        "least the YYEOF token at the end";
    ZETASQL_RET_CHECK(expanded_arg.back().kind == YYEOF);
    expanded_args.push_back(std::move(expanded_arg));
  }

  return absl::OkStatus();
}

absl::Status MacroExpander::ExpandMacroArgumentReference(
    const TokenWithLocation& token,
    std::vector<TokenWithLocation>& expanded_tokens) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();
  ZETASQL_RET_CHECK(expanded_tokens.empty());
  if (call_arguments_.empty()) {
    // This is the top-level, not the body of a macro (otherwise, we'd have at
    // least the macro name as arg #0). This means we just leave the arg ref
    // unexpanded, as opposed to assuming it was passed as if empty.
    expanded_tokens = {token,
                       {.kind = YYEOF,
                        .location = token.location,
                        .text = "",
                        .preceding_whitespaces = ""}};
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(int arg_index, ParseMacroArgIndex(token.text));
  max_arg_ref_index_ = std::max(arg_index, max_arg_ref_index_);

  if (arg_index >= call_arguments_.size()) {
    // TODO: provide the location of the invocation when we have
    // location stacking.
    ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
        token.location.start(),
        absl::StrFormat(
            "Argument index %s out of range. Invocation was provided "
            "only %d arguments.",
            token.text, call_arguments_.size()))));
    expanded_tokens = {TokenWithLocation{.kind = YYEOF,
                                         .location = token.location,
                                         .text = "",
                                         .preceding_whitespaces = ""}};
    return absl::OkStatus();
  }

  // Copy the list, since the same argument may be referenced
  // elsewhere.
  expanded_tokens = call_arguments_[arg_index];
  return absl::OkStatus();
}

absl::StatusOr<MacroExpander::StackFrame> MacroExpander::MakeStackFrame(
    std::string frame_name, ParseLocationRange location) const {
  ErrorSource error_source;
  error_source.set_error_message(absl::StrCat("Expanded from ", frame_name));

  ParseLocationTranslator location_translator(token_provider_->input());
  std::pair<int, int> line_and_column;
  ZETASQL_ASSIGN_OR_RETURN(
      line_and_column,
      location_translator.GetLineAndColumnAfterTabExpansion(location.start()),
      _ << "Location " << location.start().GetString() << "not found in:\n"
        << token_provider_->input());

  ErrorLocation* err_loc = error_source.mutable_error_location();
  if (!token_provider_->filename().empty()) {
    err_loc->set_filename(token_provider_->filename());
  }
  err_loc->set_line(line_and_column.first);
  err_loc->set_column(line_and_column.second);
  err_loc->set_input_start_line_offset(
      error_message_options_.input_original_start_line - 1);
  err_loc->set_input_start_column_offset(
      error_message_options_.input_original_start_column - 1);

  error_source.set_error_message_caret_string(
      GetErrorStringWithCaret(token_provider_->input(), *err_loc));

  return StackFrame{.location = std::move(location),
                    .error_source = error_source,
                    .parent = parent_location_};
}

// Expands the macro invocation starting at the given token.
// REQUIRES: Any arguments must have already been loaded into the splicing
//           buffer.
absl::Status MacroExpander::ExpandMacroInvocation(
    const TokenWithLocation& token, const MacroInfo& macro_info,
    std::vector<TokenWithLocation>& expanded_tokens) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();
  ZETASQL_RET_CHECK(!token.text.empty());
  ZETASQL_RET_CHECK_EQ(token.text.front(), '$');
  ZETASQL_RET_CHECK(token.kind == MACRO_INVOCATION);

  // We expand arguments regardless, even if the macro being invoked does not
  // exist.
  std::vector<std::vector<TokenWithLocation>> expanded_args;

  int invocation_end_offset;
  ZETASQL_RETURN_IF_ERROR(
      ParseAndExpandArgs(token, expanded_args, invocation_end_offset));

  ErrorMessageOptions child_error_message_options = error_message_options_;
  // The line & column in source when expanding the invocation reads from those
  // of the macro in question, not the top-level.
  child_error_message_options.input_original_start_line =
      macro_info.definition_start_line;
  child_error_message_options.input_original_start_column =
      macro_info.definition_start_column;

  // The macro definition can contain anything, not necessarily a statement or
  // a script. Expanding a definition for an invocation always occurs in raw
  // tokenization, without carrying over comments.
  auto child_token_provider = token_provider_->CreateNewInstance(
      macro_info.location.start().filename(), macro_info.source_text,
      /*start_offset=*/macro_info.body_location.start().GetByteOffset(),
      /*end_offset=*/macro_info.body_location.end().GetByteOffset());

  int num_args = static_cast<int>(expanded_args.size()) - 1;

  ZETASQL_ASSIGN_OR_RETURN(
      StackFrame stack_frame,
      MakeStackFrame(absl::StrCat("macro:", macro_info.name()),
                     ParseLocationRange(token.location.start(),
                                        ParseLocationPoint::FromByteOffset(
                                            token.location.start().filename(),
                                            invocation_end_offset))));

  int max_arg_ref_in_definition;
  ZETASQL_RETURN_IF_ERROR(ExpandMacrosInternal(
      std::move(child_token_provider), language_options_, macro_catalog_,
      arena_, std::move(expanded_args), child_error_message_options,
      &stack_frame, location_map_, expanded_tokens, warnings_,
      &max_arg_ref_in_definition));
  if (num_args > max_arg_ref_in_definition) {
    ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
        token.location.start(),
        absl::StrFormat("Macro invocation has too many arguments (%d) while "
                        "the definition only references up to %d arguments",
                        num_args, max_arg_ref_in_definition))));
  }

  // The location map, if needed, only cares about the top level.
  if (location_map_ != nullptr && call_arguments_.empty()) {
    // Leading and trailing whitespace is dropped when replacing the invocation
    // with the expansion, but we have to do it a bit early here for the
    // location map.
    expanded_tokens.front().preceding_whitespaces = "";
    expanded_tokens.back().preceding_whitespaces = "";
    location_map_->insert_or_assign(
        token.start_offset(),
        Expansion{.macro_name = std::string(GetMacroName(token)),
                  .full_match = std::string(absl::ClippedSubstr(
                      token_provider_->input(), token.start_offset(),
                      invocation_end_offset - token.start_offset())),
                  .expansion = TokensToString(expanded_tokens)});
  }
  return absl::OkStatus();
}

absl::StatusOr<TokenWithLocation> MacroExpander::ExpandLiteral(
    TokenWithLocation pending_token, TokenWithLocation literal_token) {
  RETURN_ERROR_IF_OUT_OF_STACK_SPACE();

  if (IsStrict()) {
    // Strict mode does not expand literals.
    return AdvancePendingToken(std::move(pending_token),
                               std::move(literal_token));
  }

  absl::string_view literal_contents;
  ZETASQL_ASSIGN_OR_RETURN(
      QuotingSpec quoting,
      QuotingSpec::FindQuotingKind(literal_token.text, literal_contents));

  // For error offset calculation
  int literal_content_start_offset =
      literal_token.start_offset() +
      static_cast<int>(quoting.prefix().length() +
                       QuoteStr(quoting.quote_kind()).length());

  // We cannot expand like normal, because literals can contain anything,
  // which means we cannot count on our parser. For example, 3m is not a
  // SQL token. Furthermore, consider expanding the literal "#$a('#)", what is
  // a comment and what is just actual text? For tractability, we do not allow
  // arguments. We just have our own miniparser here.
  std::string content;
  for (int num_chars_read = 0; num_chars_read < literal_contents.length();
       num_chars_read++) {
    char cur_char = literal_contents[num_chars_read];

    // If this is not a macro invocation or a macro argument, or if this is
    // a dollar sign at the end, then no expansion will occur. Append
    // directly.
    if (cur_char != '$' || num_chars_read == literal_contents.length() - 1 ||
        !IsIdentifierCharacter(literal_contents[num_chars_read + 1])) {
      absl::StrAppend(&content, std::string(1, cur_char));
      continue;
    }

    // This is a macro token: either an invocation, a macro argument, or a
    // standalone dollar sign with other stuff behind it.
    int end_index = num_chars_read + 1;
    ZETASQL_RET_CHECK(end_index < literal_contents.length());
    if (CanCharStartAnIdentifier(literal_contents[end_index])) {
      do {
        end_index++;
      } while (end_index < literal_contents.size() &&
               IsIdentifierCharacter(literal_contents[end_index]));

      if (end_index < literal_contents.size() &&
          literal_contents[end_index] == '(') {
        ParseLocationPoint paren_location = literal_token.location.start();
        paren_location.SetByteOffset(literal_content_start_offset + end_index);

        ZETASQL_RETURN_IF_ERROR(RaiseErrorOrAddWarning(MakeSqlErrorAt(
            paren_location, "Argument lists are not allowed inside literals")));

        // Best-effort expansion of this invocation, if we can compose an
        // argument list. No quotes or other parens inside.
        int token_end = end_index + 1;
        const auto disallowed_in_expansion_in_literal = [](const char c) {
          return c == '(' || c == ')' || c == '\'' || c == '"';
        };
        while (
            token_end < literal_contents.size() &&
            !disallowed_in_expansion_in_literal(literal_contents[token_end])) {
          token_end++;
        }
        if (token_end == literal_contents.size() ||
            literal_contents[token_end] != ')') {
          return MakeSqlErrorAt(
              paren_location,
              "Nested macro argument lists inside literals are not allowed");
        }
        end_index = token_end + 1;
      }
    } else {
      // If this is an argument reference, pick up the argument position.
      while (end_index < literal_contents.size() &&
             std::isdigit(literal_contents[end_index])) {
        end_index++;
      }
      if (end_index > num_chars_read) {
        ZETASQL_ASSIGN_OR_RETURN(int arg_index,
                         ParseMacroArgIndex(literal_contents.substr(
                             num_chars_read, end_index - num_chars_read)));
        max_arg_ref_index_ = std::max(arg_index, max_arg_ref_index_);
      }
    }

    std::vector<TokenWithLocation> expanded_tokens;
    ParseLocationPoint unexpanded_macro_start_point =
        literal_token.location.start();
    unexpanded_macro_start_point.SetByteOffset(literal_content_start_offset +
                                               num_chars_read);
    num_chars_read = end_index - 1;

    auto child_token_provider = token_provider_->CreateNewInstance(
        token_provider_->filename(), token_provider_->input(),
        /*start_offset=*/unexpanded_macro_start_point.GetByteOffset(),
        /*end_offset=*/literal_content_start_offset + end_index);

    ZETASQL_RETURN_IF_ERROR(ExpandMacrosInternal(
        std::move(child_token_provider), language_options_, macro_catalog_,
        arena_, call_arguments_, error_message_options_, parent_location_,
        location_map_, expanded_tokens, warnings_,
        /*out_max_arg_ref_index=*/nullptr));

    ZETASQL_RET_CHECK(!expanded_tokens.empty())
        << "A proper expansion should have at least the YYEOF token at "
           "the end. Failure was when expanding "
        << literal_token.text;
    ZETASQL_RET_CHECK_EQ(expanded_tokens.back().kind, YYEOF);
    // Pop the trailing space, which is stored on the YYEOF's
    // preceding_whitespaces.
    expanded_tokens.pop_back();

    for (TokenWithLocation& expanded_token : expanded_tokens) {
      // Ensure the incoming text can be safely placed in the current literal
      if (!IsQuotedLiteral(expanded_token)) {
        absl::StrAppend(&content, expanded_token.preceding_whitespaces);
        absl::StrAppend(&content, expanded_token.text);
      } else {
        absl::string_view extracted_token_content;
        ZETASQL_ASSIGN_OR_RETURN(QuotingSpec inner_spec,
                         QuotingSpec::FindQuotingKind(expanded_token.text,
                                                      extracted_token_content));
        if (quoting.literal_kind() != inner_spec.literal_kind()) {
          return MakeSqlErrorAt(
              unexpanded_macro_start_point,
              absl::StrFormat("Cannot expand a %s into a %s.",
                              inner_spec.Description(), quoting.Description()));
        }

        if (AreSame(quoting, inner_spec)) {
          // Expand in the same literal since it's identical, but remove the
          // quotes.
          ZETASQL_RET_CHECK(expanded_token.preceding_whitespaces.empty());
          absl::StrAppend(&content, extracted_token_content);
        } else {
          // We need to break the literal
          TokenWithLocation current_literal = literal_token;
          current_literal.text =
              AllocateString(QuoteText(content, quoting), arena_);
          ZETASQL_ASSIGN_OR_RETURN(current_literal,
                           AdvancePendingToken(std::move(pending_token),
                                               std::move(current_literal)));
          output_token_buffer_.Push(std::move(current_literal));
          if (expanded_token.preceding_whitespaces.empty()) {
            // The literal components need some whitespace separation.
            // see (broken link).
            expanded_token.preceding_whitespaces = " ";
          }
          output_token_buffer_.Push(std::move(expanded_token));

          pending_token = {.kind = YYEOF,
                           .location = literal_token.location,
                           .text = "",
                           .preceding_whitespaces = ""};

          content.clear();
          literal_token.preceding_whitespaces = " ";
        }
      }
    }
  }

  literal_token.text = AllocateString(QuoteText(content, quoting), arena_);
  return literal_token;
}

absl::Status MacroExpander::ExpandMacrosInternal(
    std::unique_ptr<TokenProviderBase> token_provider,
    const LanguageOptions& language_options, const MacroCatalog& macro_catalog,
    zetasql_base::UnsafeArena* arena,
    const std::vector<std::vector<TokenWithLocation>>& call_arguments,
    ErrorMessageOptions error_message_options, StackFrame* parent_location,
    absl::btree_map<size_t, Expansion>* location_map,
    std::vector<TokenWithLocation>& output_token_list,
    std::vector<absl::Status>& warnings, int* out_max_arg_ref_index) {
  auto expander = absl::WrapUnique(new MacroExpander(
      std::move(token_provider), language_options, macro_catalog, arena,
      call_arguments, error_message_options, parent_location));
  expander->location_map_ = location_map;
  do {
    ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation token, expander->GetNextToken());
    output_token_list.push_back(std::move(token));
  } while (output_token_list.back().kind != YYEOF);

  std::vector<absl::Status> new_warnings = expander->ReleaseWarnings();
  std::move(new_warnings.begin(), new_warnings.end(),
            std::back_inserter(warnings));
  if (out_max_arg_ref_index != nullptr) {
    *out_max_arg_ref_index = expander->max_arg_ref_index_;
  }
  return absl::OkStatus();
}

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
