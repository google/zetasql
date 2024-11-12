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

#ifndef ZETASQL_PARSER_MACROS_FLEX_TOKEN_PROVIDER_H_
#define ZETASQL_PARSER_MACROS_FLEX_TOKEN_PROVIDER_H_

#include <memory>
#include <optional>
#include <queue>

#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/macros/token_provider_base.h"
#include "zetasql/parser/token_with_location.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace parser {
namespace macros {

// Provides the next token from a Flex tokenizer without any macro expansion.
// This is the normal case, where we only have the text and we need to
// tokenize it from the start.
class FlexTokenProvider : public TokenProviderBase {
 public:
  FlexTokenProvider(absl::string_view filename, absl::string_view input,
                    int start_offset, std::optional<int> end_offset);

  FlexTokenProvider(const FlexTokenProvider&) = delete;
  FlexTokenProvider& operator=(const FlexTokenProvider&) = delete;

  std::unique_ptr<TokenProviderBase> CreateNewInstance(
      absl::string_view filename, absl::string_view input, int start_offset,
      std::optional<int> end_offset) const override;

  // Peeks the next token, but does not consume it.
  absl::StatusOr<TokenWithLocation> PeekNextToken() override {
    if (input_token_buffer_.empty()) {
      ZETASQL_ASSIGN_OR_RETURN(TokenWithLocation next_token, GetFlexToken());
      input_token_buffer_.push(next_token);
      return next_token;
    }
    return input_token_buffer_.front();
  }

 protected:
  // Consumes the next token from the buffer, or pull one from Flex if the
  // buffer is empty.
  absl::StatusOr<TokenWithLocation> ConsumeNextTokenImpl() override;

 private:
  // Pulls the next token from Flex.
  absl::StatusOr<TokenWithLocation> GetFlexToken();

  // The ZetaSQL tokenizer which gives us all the tokens.
  std::unique_ptr<ZetaSqlTokenizer> tokenizer_;

  // Used as a buffer when we need a lookahead from the tokenizer.
  // Any tokens here are still unprocessed by the expander.
  std::queue<TokenWithLocation> input_token_buffer_;

  // Location into the current input, used by the tokenizer.
  ParseLocationRange location_;
};

}  // namespace macros
}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_MACROS_FLEX_TOKEN_PROVIDER_H_
