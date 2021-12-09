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

#include "zetasql/tools/execute_query/execute_query_prompt.h"

#include <limits>
#include <string>

#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/status_payload_matchers.h"
#include "zetasql/tools/execute_query/execute_query.pb.h"
#include "zetasql/tools/execute_query/execute_query_prompt_testutils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"

using testing::AllOf;
using testing::AnyOf;
using testing::Contains;
using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::IsEmpty;
using testing::IsSupersetOf;
using testing::Not;
using zetasql_base::testing::IsOk;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

namespace zetasql {

using execute_query::ParserErrorContext;
using zetasql::testing::StatusHasPayload;
using testing::EqualsProto;

namespace {

using ReadResultType = absl::StatusOr<absl::optional<std::string>>;

struct CompletionReq {
  size_t cursor_position;

  ::testing::Matcher<absl::Status> status_matcher = IsOk();
  size_t want_prefix_start = 0;
  ::testing::Matcher<std::vector<std::string>> matcher = IsEmpty();

  void Check(ExecuteQueryStatementPrompt& prompt, const std::string body) const;
};

void CompletionReq::Check(ExecuteQueryStatementPrompt& prompt,
                          const std::string body) const {
  const absl::StatusOr<ExecuteQueryCompletionResult> res =
      prompt.Autocomplete(PrepareCompletionReq(body, cursor_position));

  EXPECT_THAT(res.status(), status_matcher);

  if (res.ok()) {
    EXPECT_THAT(res.value(),
                CompletionResponseMatcher(Eq(want_prefix_start), matcher));
  }
}

struct StmtPromptInput final {
  ReadResultType ret;
  bool want_continuation = false;
  std::vector<CompletionReq> completions;
};

// Run ExecuteQueryStatementPrompt returning the given inputs and expecting the
// given return values or parser errors. All inputs, return values and parser
// errors must be consumed.
void TestStmtPrompt(
    const std::vector<StmtPromptInput>& inputs,
    const std::vector<::testing::Matcher<ReadResultType>>& want) {
  std::unique_ptr<ExecuteQueryStatementPrompt> prompt;

  auto cur_input = inputs.cbegin();
  auto readfunc = [&prompt, &inputs,
                   &cur_input](bool continuation) -> ReadResultType {
    EXPECT_NE(cur_input, inputs.cend()) << "Can't read beyond input";
    EXPECT_EQ(continuation, cur_input->want_continuation);

    EXPECT_THAT(cur_input->completions, IsEmpty());
    CompletionReq{.cursor_position = 0}.Check(*prompt, "");

    return (cur_input++)->ret;
  };

  prompt = std::make_unique<ExecuteQueryStatementPrompt>(readfunc);

  for (const auto& matcher : want) {
    EXPECT_THAT(prompt->Read(), matcher);
  }

  EXPECT_EQ(cur_input, inputs.cend()) << "Not all inputs have been consumed";
}

}  // namespace

TEST(ExecuteQueryStatementPrompt, Empty) { TestStmtPrompt({}, {}); }

TEST(ExecuteQueryStatementPrompt, EmptyInput) {
  TestStmtPrompt(
      {
          {.ret = ""},
          {.ret = ""},
          {.ret = ""},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, SingleLine) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1;"},
          {.ret = "My query 2;"},
          {.ret = "SELECT 3;"},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1;"),
          IsOkAndHolds("My query 2;"),
          IsOkAndHolds("SELECT 3;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, MultipleSelects) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1;"},
          {.ret = "SELECT 2;"},
          {.ret = "SELECT ("},
          {.ret = "foo,\n", .want_continuation = true},
          {.ret = "bar);", .want_continuation = true},
          {.ret = "SELECT 4;"},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1;"),
          IsOkAndHolds("SELECT 2;"),
          IsOkAndHolds("SELECT (foo,\nbar);"),
          IsOkAndHolds("SELECT 4;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, FaultyMixedWithValid) {
  TestStmtPrompt(
      {
          {.ret = ""},
          {.ret = "SELECT 1, (2 +\n"},
          {.ret = "  3);", .want_continuation = true},

          // Unclosed string literal followed by legal statement (the latter is
          // dropped due to the error)
          {.ret = "\";\nSELECT 123; "},

          {.ret = "SELECT\nsomething;"},

          // Missing whitespace between literal and alias
          {.ret = "SELECT 1x;"},

          {.ret = "DROP TABLE MyTable;"},

          // Just some whitespace
          {.ret = "\n"},
          {.ret = "\t"},
          {.ret = ""},

          // Missing whitespace between literal and alias
          {.ret = "SELECT"},
          {.ret = " (", .want_continuation = true},
          {.ret = "\"\"), 1", .want_continuation = true},
          {.ret = "x);\n", .want_continuation = true},

          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1, (2 +\n  3);"),
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(": Unclosed string literal")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "\";\nSELECT 123;"
                )pb"))),
          IsOkAndHolds("SELECT\nsomething;"),
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(
                             ": Missing whitespace between literal and alias")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "SELECT 1x;"
                )pb"))),
          IsOkAndHolds("DROP TABLE MyTable;"),
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(
                             ": Missing whitespace between literal and alias")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "SELECT (\"\"), 1x);"
                )pb"))),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, TripleQuoted) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 99, \"\"\"hello\n"},
          {.ret = "\n", .want_continuation = true},
          {.ret = "line\n", .want_continuation = true},
          {.ret = "", .want_continuation = true},
          {.ret = "\n", .want_continuation = true},
          {.ret = "world\"\"\", 101;", .want_continuation = true},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 99, \"\"\"hello\n\nline\n\nworld\"\"\", 101;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, TerminatedByEOF) {
  TestStmtPrompt(
      {
          {.ret = ""},
          {.ret = "SELECT 1, (2 +"},
          // Statement not terminated with semicolon
          {.ret = "  3)", .want_continuation = true},
          {.ret = absl::nullopt, .want_continuation = true},
      },
      {
          IsOkAndHolds("SELECT 1, (2 +  3)"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, ReadAfterEOF) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1;"},
          {.ret = ""},
          {.ret = ""},
          {.ret = ""},
          {.ret = "SELECT 2;"},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1;"),
          IsOkAndHolds("SELECT 2;"),
          IsOkAndHolds(absl::nullopt),
          IsOkAndHolds(absl::nullopt),
          IsOkAndHolds(absl::nullopt),
          IsOkAndHolds(absl::nullopt),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, MultipleOnSingleLine) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1; SELECT 2;\tSELECT\n"},
          {.ret = "  3;", .want_continuation = true},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1;"),
          IsOkAndHolds("SELECT 2;"),
          IsOkAndHolds("SELECT\n  3;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, SemicolonOnly) {
  TestStmtPrompt(
      {
          {.ret = ";"},
          {.ret = ";;;"},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds(";"),
          IsOkAndHolds(";"),
          IsOkAndHolds(";"),
          IsOkAndHolds(";"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, ReadError) {
  TestStmtPrompt(
      {
          {.ret = absl::NotFoundError("test")},
          {.ret = "SELECT 500;"},
          {.ret = absl::CancelledError("again")},
      },
      {
          StatusIs(absl::StatusCode::kNotFound, "test"),
          IsOkAndHolds("SELECT 500;"),
          StatusIs(absl::StatusCode::kCancelled, "again"),
      });
}

TEST(ExecuteQueryStatementPrompt, SplitKeyword) {
  TestStmtPrompt(
      {
          {.ret = "SEL"},
          {.ret = "ECT 100;", .want_continuation = true},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 100;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, SplitString) {
  TestStmtPrompt(
      {
          {.ret = "\tSELECT \"val"},
          {.ret = "ue"},
          {.ret = "\" ;", .want_continuation = true},
          {.ret = absl::nullopt},
      },
      {
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(": Unclosed string literal")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "SELECT \"val"
                )pb"))),
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(": Unclosed string literal")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "ue\" ;"
                )pb"))),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, StatementWithoutSemicolonAtEOF) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 100; SELECT 200"},
          {.ret = absl::nullopt, .want_continuation = true},
      },
      {
          IsOkAndHolds("SELECT 100;"),
          IsOkAndHolds("SELECT 200"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, UnfinishedStatement) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1; SELECT ("},
          {.ret = absl::nullopt, .want_continuation = true},
      },
      {
          IsOkAndHolds("SELECT 1;"),
          IsOkAndHolds("SELECT ("),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, ContinuedParenthesis) {
  TestStmtPrompt(
      {
          {.ret = "(\n"},
          {.ret = ");\n", .want_continuation = true},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("(\n);"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, UnfinishedParenthesis) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 10; \n"},
          {.ret = "(\n"},
          {.ret = absl::nullopt, .want_continuation = true},
      },
      {
          IsOkAndHolds("SELECT 10;"),
          IsOkAndHolds("("),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, SemicolonInParenthesis) {
  TestStmtPrompt(
      {
          {.ret = "SELECT 1, (;); \n"},
          {.ret = absl::nullopt},
      },
      {
          IsOkAndHolds("SELECT 1, (;"),
          IsOkAndHolds(");"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, RecoverAfterUnclosedString) {
  TestStmtPrompt(
      {
          {.ret = "\";"},
          {.ret = "SELECT 123;"},
          {.ret = absl::nullopt},
      },
      {
          AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(": Unclosed string literal")),
                StatusHasPayload<ParserErrorContext>(EqualsProto(R"pb(
                  text: "\";"
                )pb"))),
          IsOkAndHolds("SELECT 123;"),
          IsOkAndHolds(absl::nullopt),
      });
}

TEST(ExecuteQueryStatementPrompt, LargeInput) {
  const std::string large(32, 'A');
  unsigned int count = 0;

  ExecuteQueryStatementPrompt prompt{[&large, &count](bool continuation) {
    EXPECT_EQ(continuation, ++count > 1);
    return large;
  }};

  EXPECT_EQ(prompt.max_length_, ExecuteQueryStatementPrompt::kMaxLength);

  // First prime number larger than 1 KiB to ensure that division by the length
  // of the "large" string doesn't result in an integral value.
  prompt.max_length_ = 1031;

  for (int i = 0; i < 10; ++i) {
    count = 0;
    EXPECT_THAT(prompt.Read(),
                StatusIs(absl::StatusCode::kResourceExhausted,
                         "Reached maximum statement length of 1 KiB"));
    EXPECT_EQ(count, 1 + (prompt.max_length_ / large.size()));
  }
}

TEST(ExecuteQuerySingleInputTest, ReadEmptyString) {
  ExecuteQuerySingleInput prompt{""};

  EXPECT_THAT(prompt.Read(), IsOkAndHolds(absl::nullopt));
}

TEST(ExecuteQuerySingleInputTest, ReadMultiLine) {
  ExecuteQuerySingleInput prompt{"test\nline; SELECT 100;"};

  EXPECT_THAT(prompt.Read(), IsOkAndHolds("test\nline;"));
  EXPECT_THAT(prompt.Read(), IsOkAndHolds("SELECT 100;"));
  EXPECT_THAT(prompt.Read(), IsOkAndHolds(absl::nullopt));
}

TEST(ExecuteQuerySingleInputTest, UnexpectedEnd) {
  ExecuteQuerySingleInput prompt{"SELECT 99;\nSELECT"};

  EXPECT_THAT(prompt.Read(), IsOkAndHolds("SELECT 99;"));
  EXPECT_THAT(prompt.Read(), IsOkAndHolds("SELECT"));
  EXPECT_THAT(prompt.Read(), IsOkAndHolds(absl::nullopt));
}

}  // namespace zetasql
