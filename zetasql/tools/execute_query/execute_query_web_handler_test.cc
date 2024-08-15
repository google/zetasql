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

#include "zetasql/tools/execute_query/execute_query_web_handler.h"

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/tools/execute_query/web/embedded_resources.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/algorithm/container.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"

using testing::Eq;
using testing::HasSubstr;
using testing::StartsWith;

namespace zetasql {

namespace {

class FakeQueryWebTemplates : public QueryWebTemplates {
 public:
  FakeQueryWebTemplates(std::string contents, std::string css, std::string body)
      : contents_(std::move(contents)),
        css_(std::move(css)),
        body_(std::move(body)) {}
  ~FakeQueryWebTemplates() override = default;

  const std::string& GetWebPageContents() const override { return contents_; }
  const std::string& GetWebPageCSS() const override { return css_; }
  const std::string& GetWebPageBody() const override { return body_; }

 private:
  std::string contents_;
  std::string css_;
  std::string body_;
};

bool HandleRequest(const ExecuteQueryWebRequest& request,
                   const QueryWebTemplates& templates, std::string& result) {
  ExecuteQueryWebHandler handler(templates);
  return handler.HandleRequest(request, [&result](absl::string_view s) {
    result = s;
    return true;
  });
}

}  // namespace

TEST(ExecuteQueryWebHandlerTest, TestCSS) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({""}, "", "none"),
      FakeQueryWebTemplates("CSS: {{css}}", "some_css", ""), result));
  EXPECT_THAT(result, Eq("CSS: some_css"));
}

TEST(ExecuteQueryWebHandlerTest, TestQueryPreserved) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({""}, "foo bar", "none"),
      FakeQueryWebTemplates("{{> body}}", "", "Query: {{query}}"), result));
  EXPECT_THAT(result, Eq("Query: foo bar"));
}

TEST(ExecuteQueryWebHandlerTest, TestModesPreserved) {
  std::vector<std::string> modes = {"analyze", "explain", "parse"};
  std::string mode_template =
      "{{mode_analyze}}-{{mode_explain}}-{{mode_parse}}";
  std::string result;
  for (int i = 0; i < 20; i++) {
    for (int subset_size = 0; subset_size <= modes.size(); subset_size++) {
      std::vector<std::string> subset = modes;
      std::shuffle(subset.begin(), subset.end(), std::mt19937{});
      subset.resize(subset_size);

      std::vector<std::string> expected_v;
      for (const std::string& s : modes) {
        expected_v.push_back(absl::c_linear_search(subset, s) ? "true" : "");
      }
      std::string expected = absl::StrJoin(expected_v, "-");

      EXPECT_TRUE(HandleRequest(
          ExecuteQueryWebRequest(subset, "foo bar", "none"),
          FakeQueryWebTemplates("{{> body}}", "", mode_template), result));
      EXPECT_THAT(result, Eq(expected))
          << "Failed for subset [" << absl::StrJoin(subset, ",") << "]";
    }
  }
}

TEST(ExecuteQueryWebHandlerTest, TestQueryEscaped) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({""}, "</select> Exploit!", "none"),
      FakeQueryWebTemplates("{{> body}}", "", "Query: {{query}}"), result));
  EXPECT_THAT(result, Eq("Query: &lt;&#x2F;select&gt; Exploit!"));
}

TEST(ExecuteQueryWebHandlerTest, TestQueryResultPresent) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "SELECT 1", "none"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{result}}-{{error}}-{{result_or_error}}"
                            "{{/statements}}"),
      result));
  EXPECT_THAT(result, Eq("true--true"));
}

TEST(ExecuteQueryWebHandlerTest, TestQueryExecutedResult) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "SELECT 1, 'foo'", "none"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{#result_executed}}"
                            "{{#result_executed_table}}"
                            "{{> table}}"
                            "{{/result_executed_table}}"
                            "{{/result_executed}}"
                            "{{/statements}}"),
      result));

  const auto& expected = R"(<table>
  <thead>
    <tr>
        <th>&zwnj;</th>
        <th>&zwnj;</th>
    </tr>
  </thead>
  <tbody>
      <tr>
          <td>1</td>
          <td>foo</td>
      </tr>
  </tbody>
</table>)";
  EXPECT_THAT(result, Eq(expected));
}

TEST(ExecuteQueryWebHandlerTest, TestQueryExecutedSimpleResult) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "DESCRIBE RAND", "none"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{result_executed_text}}"
                            "{{/statements}}"),
      result));
  EXPECT_THAT(result, Eq("Function RAND\nSignature: RAND() -&gt; DOUBLE\n"));
}

TEST(ExecuteQueryWebHandlerTest, TestQueryErrorPresent) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "bad request", "none"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{result}}-{{result_or_error}}-{{error}}"
                            "{{/statements}}"),
      result));
  EXPECT_THAT(result, StartsWith("-true-"));
  EXPECT_THAT(result, HasSubstr("Syntax error"));
}

TEST(ExecuteQueryWebHandlerTest, TestCatalogUsed) {
  std::string result;
  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "DESCRIBE Value", "none"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{error}}{{result_executed_text}}"
                            "{{/statements}}"),
      result));
  EXPECT_THAT(result, Eq("Object not found"));

  EXPECT_TRUE(HandleRequest(
      ExecuteQueryWebRequest({"execute"}, "DESCRIBE Value", "sample"),
      FakeQueryWebTemplates("{{> body}}", "",
                            "{{#statements}}"
                            "{{error}}{{result_executed_text}}"
                            "{{/statements}}"),
      result));
  EXPECT_THAT(result,
              Eq("Table: Value\nColumns:\n  Value   INT64\n  Value_1 INT64\n"));
}

}  // namespace zetasql
