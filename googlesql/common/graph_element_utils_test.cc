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

#include "googlesql/common/graph_element_utils.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "googlesql/base/testing/status_matchers.h"
#include "googlesql/public/functions/json.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/testing/test_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"

namespace googlesql {
namespace {

using ::testing::NotNull;
using ::testing::Optional;
using ::absl_testing::IsOkAndHolds;

TEST(GraphElementUtilsTest, IsOrContainsGraphElementTest) {
  TypeFactory factory;
  const Type* string_type = factory.get_string();
  const Type* int_type = factory.get_int64();
  const Type* bytes_type = factory.get_bytes();

  // Basic types
  ASSERT_FALSE(TypeIsOrContainsGraphElement(string_type));
  ASSERT_FALSE(TypeIsOrContainsGraphElement(int_type));
  ASSERT_FALSE(TypeIsOrContainsGraphElement(bytes_type));

  // Node type
  const GraphElementType* node_type;
  GOOGLESQL_ASSERT_OK(factory.MakeGraphElementType(
      {"aml"}, GraphElementType::ElementKind::kNode,
      {{"id", int_type}, {"name", string_type}, {"data", bytes_type}},
      &node_type));
  ASSERT_THAT(node_type, NotNull());
  ASSERT_TRUE(TypeIsOrContainsGraphElement(node_type));

  // Edge type
  const GraphElementType* edge_type;
  GOOGLESQL_ASSERT_OK(factory.MakeGraphElementType(
      {"aml"}, GraphElementType::kEdge,
      {{"transfer_id", int_type}, {"amount", int_type}}, &edge_type));
  ASSERT_THAT(edge_type, NotNull());
  ASSERT_TRUE(TypeIsOrContainsGraphElement(edge_type));

  // Path type
  const GraphPathType* path_type;
  GOOGLESQL_ASSERT_OK(factory.MakeGraphPathType(node_type, edge_type, &path_type));
  ASSERT_THAT(path_type, NotNull());
  ASSERT_TRUE(TypeIsOrContainsGraphElement(path_type));

  // Array of GraphNode
  const ArrayType* array_type;
  GOOGLESQL_ASSERT_OK(factory.MakeArrayType(node_type, &array_type));
  ASSERT_THAT(array_type, NotNull());
  ASSERT_TRUE(TypeIsOrContainsGraphElement(array_type));

  // Struct of GraphEdge and GraphPath
  const StructType* struct_type;
  GOOGLESQL_ASSERT_OK(factory.MakeStructType({{"edge", edge_type}, {"path", path_type}},
                                   &struct_type));
  ASSERT_THAT(struct_type, NotNull());
  ASSERT_TRUE(TypeIsOrContainsGraphElement(struct_type));
}

TEST(GraphElementUtilsTest, MakePropertiesJsonValueTest) {
  const LanguageOptions language_options = LanguageOptions::MaximumFeatures();
  const Value p1_value = Value::Bool(true);
  const Value p2_value = Value::Double(3.14);
  std::vector<Value::Property> properties = {{"p1", p1_value},
                                             {"p2", p2_value}};
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      JSONValue json_value,
      MakePropertiesJsonValue(absl::MakeSpan(properties), language_options));
  EXPECT_EQ(json_value.GetConstRef().GetMembers().size(), 2);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      const bool p1,
      functions::ConvertJsonToBool(json_value.GetConstRef().GetMember("p1")));
  EXPECT_EQ(p1, p1_value.bool_value());
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      const double p2,
      functions::ConvertJsonToDouble(json_value.GetConstRef().GetMember("p2"),
                                     functions::WideNumberMode::kExact,
                                     ProductMode::PRODUCT_INTERNAL));
  EXPECT_EQ(p2, p2_value.double_value());
}

TEST(GraphElementUtilsTest, IsRestrictivePathModeTest) {
  EXPECT_TRUE(IsRestrictivePathMode(ResolvedGraphPathMode::TRAIL));
  EXPECT_TRUE(IsRestrictivePathMode(ResolvedGraphPathMode::ACYCLIC));
  EXPECT_TRUE(IsRestrictivePathMode(ResolvedGraphPathMode::SIMPLE));
  EXPECT_FALSE(IsRestrictivePathMode(ResolvedGraphPathMode::WALK));
}

TEST(GraphElementUtilsTest, HasSearchPrefixTest) {
  EXPECT_TRUE(HasSearchPrefix(ResolvedGraphPathSearchPrefix::ANY));
  EXPECT_TRUE(HasSearchPrefix(ResolvedGraphPathSearchPrefix::SHORTEST));
  EXPECT_TRUE(HasSearchPrefix(ResolvedGraphPathSearchPrefix::CHEAPEST));
  EXPECT_FALSE(HasSearchPrefix(
      ResolvedGraphPathSearchPrefix::PATH_SEARCH_PREFIX_TYPE_UNSPECIFIED));
}

TEST(GraphElementUtilsTest, IntersectPeriodsTest) {
  auto period = [](std::optional<int64_t> start, std::optional<int64_t> end) {
    return test_values::Range(
        start ? Value::TimestampFromUnixMicros(*start) : Value::NullTimestamp(),
        end ? Value::TimestampFromUnixMicros(*end) : Value::NullTimestamp());
  };

  EXPECT_THAT(IntersectPeriods(period(1000, 2000), period(1500, 2500)),
              IsOkAndHolds(Optional(period(1500, 2000))));
  // NULL bounds are unbounded.
  EXPECT_THAT(
      IntersectPeriods(period(std::nullopt, 2500), period(1000, std::nullopt)),
      IsOkAndHolds(Optional(period(1000, 2500))));
  EXPECT_THAT(
      IntersectPeriods(period(1000, std::nullopt), period(std::nullopt, 2500)),
      IsOkAndHolds(Optional(period(1000, 2500))));
  EXPECT_THAT(IntersectPeriods(period(std::nullopt, std::nullopt),
                               period(std::nullopt, std::nullopt)),
              IsOkAndHolds(Optional(period(std::nullopt, std::nullopt))));
  EXPECT_THAT(
      IntersectPeriods(period(std::nullopt, std::nullopt), period(1000, 2000)),
      IsOkAndHolds(Optional(period(1000, 2000))));
  // Disjoint, adjacent (the end is exclusive) and half-unbounded disjoint
  // periods have an empty intersection.
  EXPECT_THAT(IntersectPeriods(period(1000, 2000), period(3000, 4000)),
              IsOkAndHolds(std::nullopt));
  EXPECT_THAT(IntersectPeriods(period(1000, 2000), period(2000, 3000)),
              IsOkAndHolds(std::nullopt));
  EXPECT_THAT(
      IntersectPeriods(period(std::nullopt, 1000), period(1000, std::nullopt)),
      IsOkAndHolds(std::nullopt));
}

TEST(GraphElementUtilsTest, PeriodContainsPointTest) {
  Value start1 = Value::TimestampFromUnixMicros(1000);
  Value end1 = Value::TimestampFromUnixMicros(2000);
  Value period1 = test_values::Range(start1, end1);

  Value point_inside = Value::TimestampFromUnixMicros(1500);
  Value point_outside_before = Value::TimestampFromUnixMicros(500);
  Value point_outside_after = Value::TimestampFromUnixMicros(2500);

  EXPECT_THAT(PeriodContainsPoint(period1, point_inside), IsOkAndHolds(true));
  // The start is inclusive and the end is exclusive.
  EXPECT_THAT(PeriodContainsPoint(period1, start1), IsOkAndHolds(true));
  EXPECT_THAT(PeriodContainsPoint(period1, end1), IsOkAndHolds(false));
  EXPECT_THAT(PeriodContainsPoint(period1, point_outside_before),
              IsOkAndHolds(false));
  EXPECT_THAT(PeriodContainsPoint(period1, point_outside_after),
              IsOkAndHolds(false));
}

}  // namespace
}  // namespace googlesql
