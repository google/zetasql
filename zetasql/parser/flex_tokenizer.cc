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

#include "zetasql/parser/flex_tokenizer.h"

#include <ios>
#include <memory>

#include "zetasql/common/errors.h"
#include "zetasql/parser/flex_istream.h"
#include "zetasql/parser/tm_lexer.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/parse_location.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// TODO: Remove flag when references are gone.
ABSL_FLAG(bool, zetasql_use_customized_flex_istream, true, "Unused");
ABSL_FLAG(bool, use_textmapper_lexer, false,
          "If true, uses TextMapper lexer rather than flex.");
ABSL_FLAG(bool, verify_textmapper_lexer, false,
          "If true, compare the Flex tokens with the TextMapper tokens even if "
          "DEBUG mode is not enabled. This flag should only be set when "
          "`use_textmapper_lexer` is false.");
ABSL_FLAG(bool, skip_textmapper_lexer_verification_under_debug_mode, false,
          "If true, skip the TextMapper lexer verification under DEBUG mode.");

namespace zetasql {
namespace parser {
// Include the helpful type aliases in the namespace within the C++ file so
// that they are useful for free helper functions as well as class member
// functions.
absl::StatusOr<Token> LegacyFlexTokenizer::GetNextToken(
    ParseLocationRange* location) {
  Token token = GetNextTokenFlexImpl(location);
  if (!override_error_.ok()) {
    return override_error_;
  }
  return token;
}

LegacyFlexTokenizer::LegacyFlexTokenizer(absl::string_view filename,
                                         absl::string_view input,
                                         int start_offset)
    : filename_(filename),
      start_offset_(start_offset),
      input_stream_(std::make_unique<StringViewStream>(input)) {
  // Seek the stringstream to the start_offset, and then instruct flex to read
  // from the stream. (Flex has the ability to read multiple consecutive
  // streams, but we only ever feed it one.)
  input_stream_->seekg(start_offset, std::ios_base::beg);
  switch_streams(/*new_in=*/input_stream_.get(), /*new_out=*/nullptr);
}

void LegacyFlexTokenizer::SetOverrideError(const ParseLocationRange& location,
                                           absl::string_view error_message) {
  override_error_ = MakeSqlErrorAtPoint(location.start()) << error_message;
}

absl::StatusOr<Token> TextMapperTokenizer::GetNextToken(
    ParseLocationRange* location) {
  Token token = Next();
  *location = LastTokenLocationWithStartOffset();
  ZETASQL_RETURN_IF_ERROR(override_error_);
  return token;
}

TextMapperTokenizer::TextMapperTokenizer(absl::string_view filename,
                                         absl::string_view input,
                                         int start_offset)
    // We do not use Lexer::Rewind() because its time complexity is
    // O(start_offset). See the comment for `Lexer::start_offset_` in
    // zetasql.tm for more information.
    : Lexer(absl::ClippedSubstr(input, start_offset)) {
  filename_ = filename;
  start_offset_ = start_offset;
}

ZetaSqlTokenizer::ZetaSqlTokenizer(absl::string_view filename,
                                       absl::string_view input,
                                       int start_offset, bool force_flex)
    : filename_(filename),
      input_(input),
      start_offset_(start_offset),
      force_flex_(force_flex) {
  if (!force_flex_ && absl::GetFlag(FLAGS_use_textmapper_lexer)) {
    text_mapper_tokenizer_ =
        std::make_unique<TextMapperTokenizer>(filename, input, start_offset);
  } else {
    flex_tokenizer_ =
        std::make_unique<LegacyFlexTokenizer>(filename, input, start_offset);
  }
}

absl::Status ZetaSqlTokenizer::ValidateTextMapperToken(
    absl::StatusOr<Token> flex_token,
    const ParseLocationRange& flex_token_location) {
  if (absl::GetFlag(
          FLAGS_skip_textmapper_lexer_verification_under_debug_mode)) {
    return absl::OkStatus();
  }
  if (text_mapper_tokenizer_ == nullptr) {
    text_mapper_tokenizer_ =
        std::make_unique<TextMapperTokenizer>(filename_, input_, start_offset_);
  }
  ParseLocationRange text_mapper_location;
  absl::StatusOr<Token> text_mapper_token =
      text_mapper_tokenizer_->GetNextToken(&text_mapper_location);
  ZETASQL_RET_CHECK_EQ(text_mapper_token, flex_token);
  ZETASQL_RET_CHECK_EQ(text_mapper_location, flex_token_location);
  return absl::OkStatus();
}

constexpr absl::string_view kTokenOutOfSyncError =
    "TextMapper token and Flex token are ouf of sync. Did you forget to update "
    "the lexer rule in flex_tokenizer.l or zetasql.tm?";

absl::StatusOr<Token> ZetaSqlTokenizer::GetNextToken(
    ParseLocationRange* location) {
  if (!force_flex_ && absl::GetFlag(FLAGS_use_textmapper_lexer)) {
    return text_mapper_tokenizer_->GetNextToken(location);
  }
  absl::StatusOr<Token> flex_token = flex_tokenizer_->GetNextToken(location);
  // When the flag `verify_textmapper_lexer` or the DEBUG mode is enabled,
  // validate TextMapper produces the same token.
  if (absl::GetFlag(FLAGS_verify_textmapper_lexer)) {
    ZETASQL_RETURN_IF_ERROR(ValidateTextMapperToken(flex_token, *location))
        << kTokenOutOfSyncError;
  } else {
    ZETASQL_DCHECK_OK(ValidateTextMapperToken(flex_token, *location))
        << kTokenOutOfSyncError;
  }
  return flex_token;
}

}  // namespace parser
}  // namespace zetasql
