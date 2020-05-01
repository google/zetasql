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

#include "zetasql/public/sql_formatter.h"

#include <memory>
#include <vector>
#include <deque>

#include "zetasql/base/logging.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/parse_tokens.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

absl::Status FormatSql(const std::string& sql, std::string* formatted_sql) {
  ZETASQL_RET_CHECK_NE(formatted_sql, nullptr);
  formatted_sql->clear();

  *formatted_sql = sql;

  std::vector<std::string> formatted_statement;

  ParseResumeLocation location = ParseResumeLocation::FromStringView(sql);
  bool at_end_of_input = false;
  absl::Status return_status = absl::OkStatus();
  while (!at_end_of_input) {
    std::unique_ptr<ParserOutput> parser_output;

    ParseResumeLocation pre_location = location;

    const absl::Status status = ParseNextStatement(
        &location, ParserOptions(), &parser_output, &at_end_of_input);

    if (status.ok()) {
      std::string formatted;

      // Fetch comments in the last location range.
      std::vector<ParseToken> parse_tokens;
      ParseTokenOptions options;
      options.stop_at_end_of_statement = true;

      const absl::Status token_status =
          GetParseTokens(options, &pre_location, &parse_tokens);
      if (token_status.ok()) {
        std::deque<std::pair<std::string, ParseLocationPoint>> comments;
        for (const auto& parse_token : parse_tokens) {
          if (parse_token.IsComment()) {
            comments.push_back(std::make_pair(parse_token.GetSQL(), parse_token.GetLocationRange().start()));
          }
        }
        formatted = UnparseWithComments(parser_output->statement(), comments);
      } else {
        // If GetParseTokens fails, just ignores comments.
        formatted = Unparse(parser_output->statement());
      }
      formatted_statement.push_back(formatted);
    } else {
      const absl::Status out_status = MaybeUpdateErrorFromPayload(
          ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, sql, status);
      if (return_status.ok()) {
        return_status = out_status;
      } else {
        return_status = ::zetasql_base::StatusBuilder(return_status).SetAppend()
                        << "\n"
                        << FormatError(out_status);
      }

      // When statement is not parseable, we proceed to the next semicolon and
      // just emit the original string in between.
      std::vector<ParseToken> parse_tokens;
      ParseTokenOptions options;
      options.stop_at_end_of_statement = true;

      const int statement_start = location.byte_position();
      const absl::Status token_status =
          GetParseTokens(options, &location, &parse_tokens);
      // If GetParseTokens fails, just returns the original sql since there's no
      // way to proceed forward.
      if (!token_status.ok()) {
        return MaybeUpdateErrorFromPayload(
            ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, sql,
            token_status);
      }
      // GetParseTokens() reads until either a semicolon or end of input.
      if (parse_tokens.back().IsEndOfInput()) {
        // When there's trailing whitespace or comment after the last
        // semicolon, parse_tokens will be one END_OF_INPUT token.
        // It should not be treated as a statement. If there's more than one
        // token, then we treat the remainder of the input as a statement.
        if (parse_tokens.size() != 1) {
          formatted_statement.push_back(sql.substr(statement_start));
        }
        at_end_of_input = true;
      } else {
        // The last token parsed must be a semicolon. Do not include it, because
        // we will add one later.
        ZETASQL_RET_CHECK_EQ(parse_tokens.back().GetKeyword(), ";");
        const int statement_length =
            parse_tokens.back().GetLocationRange().start().GetByteOffset() -
            statement_start;
        formatted_statement.push_back(
            sql.substr(statement_start, statement_length));
      }
    }
  }

  // The result from Unparse always ends with '\n'. Strips whitespaces so ';'
  // can follow the statement immediately rather than starting a new line.
  for (auto& e : formatted_statement) {
    absl::StripAsciiWhitespace(&e);
  }

  *formatted_sql = absl::StrCat(absl::StrJoin(formatted_statement, ";\n"), ";");
  return return_status;
}

}  // namespace zetasql
