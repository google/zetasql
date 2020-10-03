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

#include "zetasql/scripting/parsed_script.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parse_tree.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_join.h"
#include "absl/types/variant.h"

namespace zetasql {
namespace testing {
namespace {

using ::testing::_;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

class TestInput {
 public:
  TestInput(const std::string& sql,
            const std::vector<std::string>& named_parameters,
            const std::string& error = "")
      : sql_(sql), error_(error), owned_names_(named_parameters) {
    ParsedScript::StringSet ids;
    for (const std::string& name : named_parameters) {
      IdString id = IdString::MakeGlobal(name);
      ids.insert(id.ToStringView());
    }
    parameters_ = ids;
  }

  TestInput(const std::string& sql,
            const std::pair<int64_t, int64_t>& positional_parameters,
            const std::string& error = "")
      : sql_(sql), error_(error), parameters_(positional_parameters) {}

  const std::string& sql() const { return sql_; }
  const std::string& error() const { return error_; }

  bool has_named_parameters() const {
    return absl::holds_alternative<ParsedScript::StringSet>(parameters_);
  }

  const ParsedScript::StringSet& named_parameters() const {
    return absl::get<ParsedScript::StringSet>(parameters_);
  }
  const std::pair<int64_t, int64_t>& positional_parameters() const {
    return absl::get<std::pair<int64_t, int64_t>>(parameters_);
  }

 private:
  const std::string sql_;
  const std::string error_;
  const std::vector<std::string> owned_names_;
  absl::variant<ParsedScript::StringSet, std::pair<int64_t, int64_t>> parameters_;
};

class TestCase {
 public:
  explicit TestCase(const std::vector<TestInput>& inputs) : inputs_(inputs) {}

  TestCase(const std::string& sql, const std::string& error)
      : sql_(sql), error_(error) {}

  const std::vector<TestInput>& inputs() const { return inputs_; }
  const std::string& error() const { return error_; }

 private:
  const std::vector<TestInput> inputs_;
  const std::string sql_;
  const std::string error_;
};

class ParametersTest
    : public ::testing::TestWithParam<absl::variant<TestCase, TestInput>> {};

void CheckStatement(const ParseLocationRange& range, const ParsedScript* parsed,
                    const TestInput& stmt) {
  ParsedScript::StringSet actual_named_params =
      parsed->GetNamedParameters(range);
  std::pair<int64_t, int64_t> actual_pos_params =
      parsed->GetPositionalParameters(range);

  if (stmt.has_named_parameters()) {
    std::set<std::string> expected_named_params;
    for (absl::string_view name : stmt.named_parameters()) {
      expected_named_params.insert(absl::AsciiStrToLower(name));
    }
    std::set<std::string> lower_actual_names;
    for (absl::string_view name : actual_named_params) {
      lower_actual_names.insert(absl::AsciiStrToLower(name));
    }
    EXPECT_EQ(expected_named_params, lower_actual_names);
  } else {
    std::pair<int64_t, int64_t> expected_pos_params = stmt.positional_parameters();
    EXPECT_EQ(actual_pos_params, expected_pos_params);
  }
}

void CheckTestCase(const TestCase& test_case) {
  std::string script = absl::StrJoin(
      test_case.inputs(), "\n", [](std::string* out, const TestInput& input) {
        absl::StrAppend(out, input.sql());
      });
  SCOPED_TRACE(script);

  ParsedScript::QueryParameters parameters = absl::nullopt;

  ParsedScript::StringSet names;
  int positionals = 0;
  for (const TestInput& input : test_case.inputs()) {
    if (input.has_named_parameters()) {
      for (absl::string_view id : input.named_parameters()) {
        names.insert(id);
      }
    } else {
      positionals += input.positional_parameters().second;
    }
  }

  if (names.empty()) {
    parameters = positionals;
  } else {
    parameters = names;
  }

  ParserOptions options;
  std::unique_ptr<ParsedScript> parsed =
      ParsedScript::Create(script, options, ERROR_MESSAGE_WITH_PAYLOAD).value();
  ZETASQL_EXPECT_OK(parsed->CheckQueryParameters(parameters));

  absl::Span<const ASTStatement* const> stmts =
      parsed->script()->statement_list();
  for (int i = 0; i < stmts.size(); i++) {
    CheckStatement(stmts[i]->GetParseLocationRange(), parsed.get(),
                   test_case.inputs()[i]);
  }
}

void CheckTestInput(const TestInput& test_input) {
  SCOPED_TRACE(test_input.sql());

  ParsedScript::QueryParameters parameters = absl::nullopt;
  ParsedScript::StringSet names;
  int positionals = 0;
  if (test_input.has_named_parameters()) {
    for (absl::string_view id : test_input.named_parameters()) {
      names.insert(id);
    }
  } else {
    positionals += test_input.positional_parameters().second;
  }
  if (names.empty()) {
    parameters = positionals;
  } else {
    parameters = names;
  }

  ParserOptions options;
  std::unique_ptr<ParsedScript> parsed =
      ParsedScript::Create(test_input.sql(), options,
                           ERROR_MESSAGE_WITH_PAYLOAD)
          .value();
  absl::Status status = parsed->CheckQueryParameters(parameters);

  if (test_input.error().empty()) {
    CheckStatement(parsed->script()->GetParseLocationRange(), parsed.get(),
                   test_input);
  } else {
    EXPECT_THAT(status, StatusIs(_, HasSubstr(test_input.error())));
  }
}

TEST_P(ParametersTest, CheckParameters) {
  absl::variant<TestCase, TestInput> param = GetParam();

  if (absl::holds_alternative<TestCase>(param)) {
    CheckTestCase(absl::get<TestCase>(param));
  } else {
    CheckTestInput(absl::get<TestInput>(param));
  }
}

std::vector<absl::variant<TestCase, TestInput>> GetScripts() {
  std::vector<std::string> empty_named;
  std::pair<int64_t, int64_t> empty_pos;
  return {
      TestInput("SELECT 1;", empty_named),
      TestInput("SELECT 1;", empty_pos),
      TestInput("SELECT @a, @A, @b, @a;", {"a", "b"}),
      TestInput("SELECT @a, @a, @b, @a;", {"a"},
                "Unknown named query parameter: b"),
      TestCase({
          TestInput("SELECT 1;", empty_pos),
          TestInput("SELECT ?;", {0, 1}),
      }),
      TestCase({
          TestInput("SELECT ?;", {0, 1}),
          TestInput("SELECT 1;", empty_pos),
      }),
      TestCase({
          TestInput("SELECT 1;", empty_pos),
          TestInput("SELECT ?;", {0, 1}),
          TestInput("SELECT ?, ?;", {1, 2}),
          TestInput("SELECT ?;", {3, 1}),
          TestInput("SELECT 1;", empty_pos),
      }),
      TestCase({
          TestInput("SELECT 1;", empty_named),
          TestInput("SELECT @a;", {"a"}),
      }),
      TestCase({
          TestInput("SELECT 1;", empty_named),
          TestInput("SELECT @a;", {"a"}),
          TestInput("SELECT @a, @b;", {"a", "b"}),
      }),
      TestCase({
          TestInput("SELECT @a;", {"a"}),
          TestInput("SELECT @b;", {"b"}),
      }),
      TestCase({
          TestInput("SELECT @A;", {"a"}),
          TestInput("SELECT @B;", {"b"}),
      }),
  };
}

INSTANTIATE_TEST_CASE_P(RunParametersTest, ParametersTest,
                        ::testing::ValuesIn(GetScripts()));

}  // namespace
}  // namespace testing
}  // namespace zetasql
