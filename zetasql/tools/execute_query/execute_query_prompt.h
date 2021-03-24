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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_

#include <deque>
#include <string>

#include "gtest/gtest_prod.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace zetasql {

class ExecuteQueryPrompt {
 public:
  virtual ~ExecuteQueryPrompt() = default;

  // Read next statement. Return empty optional when input is finished (e.g. at
  // EOF).
  virtual zetasql_base::StatusOr<absl::optional<std::string>> Read() = 0;
};

// A prompt implementation returning whole SQL statements. They're read using
// the reader function provided to the constructor. Upon calling Read() the
// reader function is called until either one or multiple complete statements
// has been read or an error occurs.
class ExecuteQueryStatementPrompt : public ExecuteQueryPrompt {
 public:
  // Maximum accepted statement length in bytes
  constexpr static size_t kMaxLength = 8 * 1024 * 1024;

  // Returns status without modification.
  static absl::Status NoOpParserErrorHandler(absl::Status status);

  // `read_next_func` is a function reading more input. Such inputs may contain
  // newlines and don't need to be line-separated. Must not be nullptr. The
  // single boolean parameter informs on whether the requested chunk is
  // a continuation of a statement. Errors (e.g. I/O) are returned to the caller
  // of the prompt's `Read` function. A return value of `nullopt` signals the
  // end of input.
  //
  // `parser_error_handler` is a function given any errors occurring during
  // statement separation. Must not be nullptr. The handler may log the error
  // and return OK to proceed as if nothing happened, therefore handling SQL
  // syntax issues gracefully. When a non-OK status is returned the prompt in
  // turn returns that status from its `Read` function. The caller of `Read`
  // can't easily differentiate between errors originating from `read_next_func`
  // or the statement separation logic.
  explicit ExecuteQueryStatementPrompt(
      std::function<
          zetasql_base::StatusOr<absl::optional<std::string>>(bool continuation)>
          read_next_func,
      std::function<absl::Status(absl::Status status)> parser_error_handler =
          &NoOpParserErrorHandler);
  ExecuteQueryStatementPrompt(const ExecuteQueryStatementPrompt&) = delete;
  ExecuteQueryStatementPrompt& operator=(const ExecuteQueryStatementPrompt&) =
      delete;

  zetasql_base::StatusOr<absl::optional<std::string>> Read() override;

 private:
  void ReadInput(bool continuation);
  void ProcessBuffer();

 private:
  FRIEND_TEST(ExecuteQueryStatementPrompt, LargeInput);

  size_t max_length_ = kMaxLength;
  const std::function<zetasql_base::StatusOr<absl::optional<std::string>>(bool)>
      read_next_func_;
  const std::function<absl::Status(absl::Status)> parser_error_handler_;
  bool continuation_ = false;
  bool eof_ = false;
  absl::Cord buf_;
  std::deque<zetasql_base::StatusOr<absl::optional<std::string>>> queue_;
};

class ExecuteQuerySingleInput : public ExecuteQueryStatementPrompt {
 public:
  explicit ExecuteQuerySingleInput(absl::string_view query);
  ExecuteQuerySingleInput(const ExecuteQuerySingleInput&) = delete;
  ExecuteQuerySingleInput& operator=(const ExecuteQuerySingleInput&) = delete;

 private:
  absl::optional<std::string> ReadNext(bool continuation);

 private:
  const std::string query_;
  bool done_ = false;
};

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_PROMPT_H_
