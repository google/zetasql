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

// Test repetitive queries and bisect to find limits of engine behaviour

#include "zetasql/compliance/depth_limit_detector_test_cases.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "zetasql/compliance/depth_limit_detector_internal.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"

// Small limit as we are experimenting with these tests.
ABSL_FLAG(std::size_t, depth_limit_detector_max_sql_bytes, 1000,
          "Do not expand recursive test queries beyond this number of bytes");

namespace zetasql {

std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorReturnCondition& condition) {
  return os << "depth=[" << condition.starting_depth << ","
            << condition.ending_depth << "],status=" << condition.return_status;
}
std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorTestResult& test_result) {
  os << "depth_limit_detector_test_name="
     << test_result.depth_limit_test_case_name << ",conditions={";
  for (const auto& condition :
       test_result.depth_limit_detector_return_conditions) {
    os << condition << ",";
  }
  return os << "}";
}
std::ostream& operator<<(std::ostream& os,
                         const DepthLimitDetectorTestCase& test_case) {
  return os << test_case.depth_limit_test_case_name;
}

namespace {

// Output a template to a string, performing different actions depending
// on the type of the template.
// Currently supports raw strings, incrementing numbers for generation of unique
// names and repeating other templates.
void InstantiateDepthLimitDetectorTemplate(std::string_view part,
                                           std::ostream& output, int) {
  output << part;
}
void InstantiateDepthLimitDetectorTemplate(
    depth_limit_detector_internal::DepthLimitDetectorDepthNumber,
    std::ostream& output, int depth) {
  output << depth;
}

void InstantiateDepthLimitDetectorTemplate(
    const depth_limit_detector_internal::DepthLimitDetectorRepeatedTemplate&
        depth_limit_template,
    std::ostream& output, int depth) {
  for (int i = 1; i <= depth; ++i) {
    for (auto const& part : depth_limit_template) {
      std::visit(
          [&](auto const& typed_part) {
            InstantiateDepthLimitDetectorTemplate(typed_part, output, i);
          },
          part);
    }
  }
}
void InstantiateDepthLimitDetectorTemplate(
    const DepthLimitDetectorTemplate& depth_limit_template,
    std::ostream& output, int depth) {
  for (auto const& part : depth_limit_template) {
    std::visit(
        [&](auto const& typed_part) {
          InstantiateDepthLimitDetectorTemplate(typed_part, output, depth);
        },
        part);
  }
}

// Quickly compare statuses without considering complex payloads.
inline bool StatusCodeAndMessageEqual(absl::Status a, absl::Status b) {
  return a.code() == b.code() && a.message() == b.message();
}

// Add a new test result to those known, either enlarging a range or
// creating a whole new one.
//
// This function can only handle errors that appear once as a single contiguous
// range in the output domain.
// For example, if the tested function returns AAAABBB as depth increases, so
// f(1) = A, f(2) = A, f(3) = A, f(4), = A, f(5) = B, etc. this integration
// called iteratively will create {A:[1-4], B:[5-7]}.
// However, if a function has a pattern like AAABAAA then there is guarantee
// that we would notice the B in the middle. If we do, we fail a ZETASQL_CHECK.
void IntegrateTestResult(DepthLimitDetectorTestResult* result, int depth,
                         absl::Status status) {
  auto insertion_point = std::lower_bound(
      result->depth_limit_detector_return_conditions.begin(),
      result->depth_limit_detector_return_conditions.end(), depth,
      [](const DepthLimitDetectorReturnCondition& i, int depth) {
        return i.ending_depth < depth;
      });
  if (insertion_point !=
      result->depth_limit_detector_return_conditions.begin()) {
    auto preceding_point = insertion_point - 1;
    if (insertion_point ==
            result->depth_limit_detector_return_conditions.end() ||
        !StatusCodeAndMessageEqual(insertion_point->return_status, status)) {
      if (StatusCodeAndMessageEqual(preceding_point->return_status, status)) {
        insertion_point = preceding_point;
      } else {
        ZETASQL_CHECK(depth > preceding_point->ending_depth)  // Crash OK
            << "Disection algorithm cannot split a range "
            << "depth=" << depth << " status=" << status
            << " insertion_point=" << *insertion_point
            << " preceding_point=" << *preceding_point;
      }
    }
  }
  if (insertion_point != result->depth_limit_detector_return_conditions.end() &&
      StatusCodeAndMessageEqual(insertion_point->return_status, status)) {
    insertion_point->starting_depth =
        std::min(insertion_point->starting_depth, depth);
    insertion_point->ending_depth =
        std::max(insertion_point->ending_depth, depth);
    auto following_point = insertion_point + 1;
    if (following_point !=
        result->depth_limit_detector_return_conditions.end()) {
      ZETASQL_CHECK(following_point->starting_depth > depth)  // Crash OK
          << "Disection algorithm cannot overlap a range at the beginning "
          << "depth=" << depth << " status=" << status
          << " insertion_point=" << *insertion_point
          << " following_point=" << *following_point;
    }
    return;
  }

  result->depth_limit_detector_return_conditions.insert(
      insertion_point,
      DepthLimitDetectorReturnCondition({.starting_depth = depth,
                                         .ending_depth = depth,
                                         .return_status = status}));
}

}  // namespace

std::string DepthLimitDetectorTemplateToString(
    const DepthLimitDetectorTemplate& depth_limit_template, int depth) {
  std::ostringstream output;
  InstantiateDepthLimitDetectorTemplate(depth_limit_template, output, depth);
  return output.str();
}
std::string DepthLimitDetectorTemplateToString(
    const DepthLimitDetectorTestCase& depth_case, int depth) {
  return DepthLimitDetectorTemplateToString(depth_case.depth_limit_template,
                                            depth);
}

LanguageOptions DepthLimitDetectorTestCaseLanguageOptions(
    const DepthLimitDetectorTestCase& depth_case) {
  zetasql::LanguageOptions language_options;
  language_options.SetEnabledLanguageFeatures(
      depth_case.depth_limit_required_features);
  return language_options;
}

// Instantiates a query template at many depths in order to expose complex
// resource exhausted conditions (e.g. stack overflows).
DepthLimitDetectorTestResult RunDepthLimitDetectorTestCase(
    DepthLimitDetectorTestCase const& depth_limit_case,
    std::function<absl::Status(std::string_view)> test_driver_function) {
  testing::Test::RecordProperty(
      "depth_limit_detector_test_case_name",
      std::string(depth_limit_case.depth_limit_test_case_name));

  DepthLimitDetectorTestResult result;
  result.depth_limit_test_case_name =
      depth_limit_case.depth_limit_test_case_name;

  auto TryDepth = [&](std::string_view sql, int depth) {
    auto status = test_driver_function(sql);
    IntegrateTestResult(&result, depth, status);
  };

  // Increase depth by 5% each iteration.
  for (int depth = 1; depth < depth_limit_case.depth_limit_max_depth;
       depth = std::max(depth + 1, (depth * 21) / 20)) {
    auto sql = DepthLimitDetectorTemplateToString(
        depth_limit_case.depth_limit_template, depth);
    if (sql.size() > absl::GetFlag(FLAGS_depth_limit_detector_max_sql_bytes)) {
      break;
    }
    TryDepth(sql, depth);
  }

  bool continued_disection;
  int last_ending_depth = 0;
  do {
    continued_disection = false;
    for (DepthLimitDetectorReturnCondition const& condition :
         result.depth_limit_detector_return_conditions) {
      if (condition.starting_depth > last_ending_depth + 1) {
        continued_disection = true;
        int depth = (condition.starting_depth + 1 + last_ending_depth) / 2;
        TryDepth(DepthLimitDetectorTemplateToString(
                     depth_limit_case.depth_limit_template, depth),
                 depth);
        break;
      }
      last_ending_depth = condition.ending_depth;
    }
  } while (continued_disection);

  return result;
}

absl::Span<const std::reference_wrapper<const DepthLimitDetectorTestCase>>
AllDepthLimitDetectorTestCases() {
  using R = depth_limit_detector_internal::DepthLimitDetectorRepeatedTemplate;
  using N = depth_limit_detector_internal::DepthLimitDetectorDepthNumber;
  static std::vector<DepthLimitDetectorTestCase>* cases =
      new std::vector<DepthLimitDetectorTestCase>{
          {
              .depth_limit_test_case_name = "recursive_protobuf_field",
              .depth_limit_template =
                  {"WITH t AS (SELECT CAST(NULL AS zetasql_test__.RecursivePB) "
                   "AS recursive_pb) SELECT ",
                   R({"recursive_pb."}), "int64_val FROM t"},
          },
          {.depth_limit_test_case_name = "recursive_repeated_protobuf_field",
           .depth_limit_template =
               {"WITH t AS (SELECT CAST(NULL AS zetasql_test__.RecursivePB) "
                "AS pb) SELECT pb.",
                R({"repeated_recursive_pb[0]."}), "int64_val FROM t"},
           .depth_limit_required_features =
               {
                   LanguageFeature::FEATURE_V_1_4_BARE_ARRAY_ACCESS,
               }},
          {
              .depth_limit_test_case_name = "nested_ors",
              .depth_limit_template =
                  {"WITH t AS (SELECT 'x' AS f), u AS (SELECT * FROM t WHERE",
                   R({"("}), "TRUE", R({" OR ((f) = ('x')))"}),
                   ") SELECT * FROM u"},
          },
          {.depth_limit_test_case_name = "repeated_ors",
           .depth_limit_template =
               {
                   "WITH t AS (SELECT 'x' AS f) SELECT * FROM t WHERE FALSE",
                   R({" OR f = 'x'"}),
               }},
          {
              .depth_limit_test_case_name = "nested_select",
              .depth_limit_template = {R({"SELECT ("}), "SELECT 1", R({")"}),
                                       " AS c"},
          },
          {
              .depth_limit_test_case_name = "nested_expr_select",
              .depth_limit_template = {"SELECT ", R({"("}), "(SELECT 1) + 2",
                                       R({")"}), " AS c"},
          },
          {
              .depth_limit_test_case_name = "nested_select_from",
              .depth_limit_template = {R({"SELECT * FROM ("}), "SELECT 1",
                                       R({")"}), " AS c"},
          },
          {
              .depth_limit_test_case_name = "nested_cast",
              .depth_limit_template = {"SELECT ", R({"CAST("}), "1",
                                       R({" AS DOUBLE)"}), " AS c"},
          },
          {
              .depth_limit_test_case_name = "nested_safe_cast",
              .depth_limit_template = {"SELECT ", R({"SAFE_CAST("}), "1",
                                       R({" AS DOUBLE)"}), " AS c"},
          },
          {.depth_limit_test_case_name = "nested_least",
           .depth_limit_template = {"SELECT ", R({"LEAST(1,"}), "2", R({")"}),
                                    " AS c"}},
          {
              .depth_limit_test_case_name = "nested_if",
              .depth_limit_template = {"SELECT ", R({"IF(1>2,3,"}), "4",
                                       R({")"}), " AS c"},
          },
          {
              .depth_limit_test_case_name = "nested_md5",
              .depth_limit_template = {"SELECT ", R({"MD5("}), "''", R({")"}),
                                       " AS c"},
          },
          {.depth_limit_test_case_name = "nested_struct_type",
           .depth_limit_template = {"SELECT CAST(NULL AS ", R({"STRUCT<"}),
                                    "DOUBLE", R({">"}), ") AS c"}},
          {
              .depth_limit_test_case_name = "nested_struct",
              .depth_limit_template = {"SELECT ",
                                       R({
                                           "STRUCT(",
                                       }),
                                       "1 AS f", R({") AS S"})},
          },
          {.depth_limit_test_case_name = "nested_flatten",
           .depth_limit_template = {"SELECT ", R({"FLATTEN("}), "NULL",
                                    R({")"}), " AS c"},
           .depth_limit_required_features =
               {
                   LanguageFeature::FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS,
               }},
          {.depth_limit_test_case_name = "nested_replace_fields",
           .depth_limit_template = {"SELECT ", R({"REPLACE_FIELDS("}),
                                    "CAST('int64_key_1: 1 int64_key_2: 2' AS "
                                    "`zetasql_test__.KitchenSinkPB`)",
                                    R({", 1 AS int64_key_1)"}), "AS c"},
           .depth_limit_required_features =
               {
                   LanguageFeature::FEATURE_V_1_3_REPLACE_FIELDS,
               }},
          {
              .depth_limit_test_case_name = "repeated_bitwise_not",
              .depth_limit_template = {"SELECT ", R({"~"}), "1 AS c"},
          },
          {
              .depth_limit_test_case_name = "repeated_plus_one",
              .depth_limit_template = {"SELECT 1", R({"+1"}), " AS c"},
          },
          {
              .depth_limit_test_case_name = "with_joins",
              .depth_limit_template = {"WITH ",
                                       R({"t", N(), " AS (SELECT 1 AS c),"}),
                                       " u AS (SELECT 1 AS c) SELECT * FROM u",
                                       R({" JOIN t", N(), " ON t", N(),
                                          ".c = u.c"})},
              .depth_limit_required_features =
                  {
                      LanguageFeature::FEATURE_V_1_3_ALLOW_CONSECUTIVE_ON,
                  },
          },
          {
              .depth_limit_test_case_name = "select_many_columns",
              .depth_limit_template = {"SELECT 1 AS c0",
                                       R({", ", N(), " AS c", N()})},
              .depth_limit_max_depth =
                  5000,  // As the reference implementations gets very slow
          }};
  static std::vector<std::reference_wrapper<const DepthLimitDetectorTestCase>>*
      case_references = [&]() {
        std::vector<std::reference_wrapper<const DepthLimitDetectorTestCase>>*
            refs = new std::vector<
                std::reference_wrapper<const DepthLimitDetectorTestCase>>();
        for (int64_t i = 0; i < cases->size(); ++i) {
          (*refs).emplace_back((*cases)[i]);
        }
        return refs;
      }();
  return *case_references;
}

}  // namespace zetasql
