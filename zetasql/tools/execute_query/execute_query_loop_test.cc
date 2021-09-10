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

#include "zetasql/tools/execute_query/execute_query_loop.h"

#include <iosfwd>
#include <string>
#include <utility>

#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/status_payload_matchers.h"
#include "zetasql/tools/execute_query/execute_query.pb.h"
#include "zetasql/tools/execute_query/execute_query_prompt.h"
#include "zetasql/tools/execute_query/execute_query_tool.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace zetasql {

using execute_query::ParserErrorContext;
using zetasql::testing::StatusHasPayload;
using testing::EqualsProto;
using ::testing::IsEmpty;
using ::zetasql_base::testing::StatusIs;

namespace {
class StaticResultPrompt : public ExecuteQueryPrompt {
 public:
  void set_read_result(absl::StatusOr<absl::optional<std::string>> r) {
    read_result_ = std::move(r);
  }

  absl::StatusOr<absl::optional<std::string>> Read() override {
    return read_result_;
  }

 private:
  absl::StatusOr<absl::optional<std::string>> read_result_;
};
}  // namespace

TEST(ExecuteQueryLoopTest, SelectOne) {
  ExecuteQuerySingleInput prompt{"SELECT 1"};
  ExecuteQueryConfig config;
  std::ostringstream output;
  ExecuteQueryStreamWriter writer{output};

  ZETASQL_EXPECT_OK(ExecuteQueryLoop(prompt, config, writer));
  EXPECT_EQ(output.str(), R"(+---+
|   |
+---+
| 1 |
+---+

)");
}

TEST(ExecuteQueryLoopTest, ReadError) {
  StaticResultPrompt prompt;

  prompt.set_read_result(absl::UnavailableError("test"));

  ExecuteQueryConfig config;
  std::ostringstream output;
  ExecuteQueryStreamWriter writer{output};

  EXPECT_THAT(ExecuteQueryLoop(prompt, config, writer),
              StatusIs(absl::StatusCode::kUnavailable, "test"));
}

TEST(ExecuteQueryLoopTest, NoInput) {
  StaticResultPrompt prompt;

  prompt.set_read_result(absl::nullopt);

  ExecuteQueryConfig config;
  std::ostringstream output;
  ExecuteQueryStreamWriter writer{output};

  ZETASQL_EXPECT_OK(ExecuteQueryLoop(prompt, config, writer));
  EXPECT_THAT(output.str(), IsEmpty());
}

TEST(ExecuteQueryLoopTest, Callback) {
  StaticResultPrompt prompt;

  prompt.set_read_result("\ntest error   ");

  ExecuteQueryConfig config;
  std::ostringstream output;
  ExecuteQueryStreamWriter writer{output};

  const auto handler = [&prompt](absl::Status status) {
    if (status.code() == absl::StatusCode::kUnavailable &&
        status.message() == "input error") {
      return status;
    }

    // The query used invalid syntax
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(status, StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "test error"
                )pb")));

    // Provoke another error
    prompt.set_read_result(absl::UnavailableError("input error"));

    return absl::OkStatus();
  };

  EXPECT_THAT(ExecuteQueryLoop(prompt, config, writer, handler),
              StatusIs(absl::StatusCode::kUnavailable, "input error"));
  EXPECT_THAT(output.str(), IsEmpty());
}

}  // namespace zetasql
