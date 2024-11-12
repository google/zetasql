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

#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/graph_element_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

namespace {

using ::zetasql::test_values::GraphEdge;
using ::zetasql::test_values::GraphNode;
using ::zetasql::test_values::MakeGraphElementType;
using ::zetasql::types::Int32Type;
using ::zetasql::types::StringType;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::TestWithParam;
using ::testing::ValuesIn;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// Parameterized common test that applies to both node and edge.
class GraphElementValueTest
    : public TestWithParam<GraphElementType::ElementKind> {
 protected:
  bool IsNode() const { return GetParam() == GraphElementType::kNode; }
  bool IsEdge() const { return GetParam() == GraphElementType::kEdge; }

  absl::StatusOr<Value> MakeElementByType(
      const GraphElementType* type, std::string identifier,
      std::vector<Value::Property> properties, std::vector<std::string> labels,
      std::string definition_name) {
    return IsNode()
               ? Value::MakeGraphNode(type, std::move(identifier),
                                      std::move(properties), std::move(labels),
                                      std::move(definition_name))
               : Value::MakeGraphEdge(type, std::move(identifier),
                                      std::move(properties), std::move(labels),
                                      std::move(definition_name), "src_node_id",
                                      "dst_node_id");
  }

  Value MakeElement(absl::Span<const std::string> graph_reference,
                    std::string identifier,
                    std::vector<Value::Property> properties,
                    absl::Span<const std::string> labels,
                    std::string definition_name) {
    return IsNode() ? GraphNode(graph_reference, identifier, properties, labels,
                                definition_name)
                    : GraphEdge(graph_reference, identifier, properties, labels,
                                definition_name, "src_node_id", "dst_node_id");
  }
};

INSTANTIATE_TEST_SUITE_P(Common, GraphElementValueTest,
                         ValuesIn({GraphElementType::kNode,
                                   GraphElementType::kEdge}));

TEST_P(GraphElementValueTest, GraphElementNull) {
  const auto* type = MakeGraphElementType({"graph_name"}, GetParam(), {});
  Value value = Value::Null(type);
  EXPECT_TRUE(value.is_null());
  EXPECT_DEATH(value.property_values(), "Null value");
}

TEST_P(GraphElementValueTest, InvalidConstructionDifferentPropertyNames) {
  const Value property_value = Value::String("v0");
  const GraphElementType* type =
      MakeGraphElementType({"graph_name"}, GetParam(), {{"p0", StringType()}});
  const std::vector<Value::Property> properties = {{"p1", property_value}};

  EXPECT_THAT(
      MakeElementByType(type, "id", properties, {"label"}, "ElementTable"),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("Unknown property: p1")));
}

TEST_P(GraphElementValueTest, NonExistentProperties) {
  const Value property_value = Value::Int32(1);
  const GraphElementType* type = MakeGraphElementType(
      {"graph_name"}, GetParam(), {{"p0", StringType()}, {"p1", Int32Type()}});
  const std::vector<Value::Property> properties = {{"p1", property_value}};

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Value graph_value,
      MakeElementByType(type, "id", properties, {"label"}, "ElementTable"));

  // p0 does not exist in graph_value and cannot be found with name.
  EXPECT_THAT(graph_value.FindPropertyByName("p0"),
              StatusIs(absl::StatusCode::kNotFound));

  // p1 exists in graph_value and can be found with name.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Value p1_value,
                       graph_value.FindPropertyByName("p1"));
  EXPECT_EQ(p1_value, property_value);
  EXPECT_EQ(graph_value.DebugString(), "{p0:NULL, p1:1}");
}

TEST_P(GraphElementValueTest, InvalidConstructionDifferentPropertyTypes) {
  const Value p0_value = Value::String("v0");
  const GraphElementType* type = MakeGraphElementType(
      {"graph_name"}, GetParam(), {{"p0", StringType()}, {"p1", Int32Type()}});
  const std::vector<Value::Property> properties = {{"p0", p0_value},
                                                   {"p1", p0_value}};
  EXPECT_THAT(
      MakeElementByType(type, "id", properties, {"label"}, "ElementTable"),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("Expected property value type: INT32, got: STRING")));
}

TEST_P(GraphElementValueTest, EmptyIdentifierCausesConstructionToFail) {
  const GraphElementType* type =
      MakeGraphElementType({"graph_name"}, GetParam(), {});
  const std::vector<Value::Property> properties;
  EXPECT_THAT(
      MakeElementByType(type, "", properties, {"label"}, "ElementTable"),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("Empty identifier")));
}

TEST_P(GraphElementValueTest, GraphElementCommonTest) {
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value element =
      MakeElement({"graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
                  {"label1", "label2"}, "ElementTable");
  EXPECT_EQ(element.type_kind(), TYPE_GRAPH_ELEMENT);
  EXPECT_THAT(element.type()->AsGraphElement()->graph_reference(),
              ElementsAre("graph_name"));
  EXPECT_EQ(element.GetIdentifier(), "id");
  EXPECT_THAT(element.property_values(),
              ElementsAre(Eq(p0_value), Eq(p1_value)));
  EXPECT_THAT(element.FindPropertyByName("p0"), IsOkAndHolds(Eq(p0_value)));
  EXPECT_THAT(element.FindPropertyByName("p1"), IsOkAndHolds(Eq(p1_value)));
  EXPECT_THAT(element.FindPropertyByName("unknown"),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(element.GetLabels(), ElementsAre("label1", "label2"));
  EXPECT_EQ(element.GetDefinitionName(), "ElementTable");

  const Value copied_element = element;
  EXPECT_EQ(copied_element.type_kind(), TYPE_GRAPH_ELEMENT);
  EXPECT_THAT(copied_element.type()->AsGraphElement()->graph_reference(),
              ElementsAre("graph_name"));
  EXPECT_EQ(copied_element.GetIdentifier(), "id");
  EXPECT_THAT(copied_element.property_values(),
              ElementsAre(Eq(p0_value), Eq(p1_value)));
  EXPECT_THAT(copied_element.GetLabels(), ElementsAre("label1", "label2"));
  EXPECT_EQ(copied_element.GetDefinitionName(), "ElementTable");

  EXPECT_TRUE(element.Equals(copied_element));
  EXPECT_FALSE(element.LessThan(copied_element));
  EXPECT_FALSE(copied_element.LessThan(element));
  EXPECT_EQ(element.HashCode(), copied_element.HashCode());

  const Value element_with_same_graph_and_id =
      MakeElement({"graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
                  {"label1", "label2"}, "ElementTable");
  EXPECT_TRUE(element.Equals(element_with_same_graph_and_id));
  EXPECT_FALSE(element.LessThan(element_with_same_graph_and_id));
  EXPECT_FALSE(element_with_same_graph_and_id.LessThan(element));
  EXPECT_EQ(element.HashCode(), element_with_same_graph_and_id.HashCode());

  const Value element_with_same_graph_different_id =
      MakeElement({"graph_name"}, "id2", {{"p0", p0_value}, {"p1", p1_value}},
                  {"label1", "label2"}, "ElementTable");
  EXPECT_FALSE(element.Equals(element_with_same_graph_different_id));
  EXPECT_TRUE(element.LessThan(element_with_same_graph_different_id));
  EXPECT_FALSE(element_with_same_graph_different_id.LessThan(element));
  EXPECT_NE(element.HashCode(),
            element_with_same_graph_different_id.HashCode());

  const Value element_with_different_graph_same_id = MakeElement(
      {"another_graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
      {"label1", "label2"}, "ElementTable");
  EXPECT_FALSE(element.Equals(element_with_different_graph_same_id));
  EXPECT_FALSE(element.LessThan(element_with_different_graph_same_id));
  EXPECT_FALSE(element_with_different_graph_same_id.LessThan(element));
  EXPECT_NE(element.HashCode(),
            element_with_different_graph_same_id.HashCode());
}

TEST(GraphElementValueTest,
     EmptyIdentifierForSourceOrDestNodeFailsEdgeConstruction) {
  const GraphElementType* type =
      MakeGraphElementType({"graph_name"}, GraphElementType::kEdge, {});
  const std::vector<Value::Property> properties;
  EXPECT_THAT(Value::MakeGraphEdge(type, "id", properties, {"label"},
                                   "ElementTable", "", "dst_node_id"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Invalid source node identifier")));
  EXPECT_THAT(Value::MakeGraphEdge(type, "id", properties, {"label"},
                                   "ElementTable", "src_node_id", ""),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Invalid destination node identifier")));
}

TEST(GraphElementValueTest, GraphNodeSpecificTest) {
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value node =
      GraphNode({"graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
                {"label1", "label2"}, "ElementTable");
  EXPECT_TRUE(node.IsNode());
  EXPECT_FALSE(node.IsEdge());
  EXPECT_DEBUG_DEATH(node.GetSourceNodeIdentifier(), "Not an edge");
  EXPECT_DEBUG_DEATH(node.GetDestNodeIdentifier(), "Not an edge");
  EXPECT_EQ(node.ShortDebugString(), "{p0:\"v0\", p1:1}");
  EXPECT_EQ(node.FullDebugString(),
            "GraphNode{$name:\"ElementTable\", $id:b\"id\", "
            "$labels:[\"label1\", \"label2\"], "
            "p0:String(\"v0\"), "
            "p1:Int32(1)}");
}

TEST(GraphElementValueTest, GraphEdgeSpecificTest) {
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value edge = GraphEdge(
      {"graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
      {"label1", "label2"}, "ElementTable", "src_node_id", "dst_node_id");
  EXPECT_FALSE(edge.IsNode());
  EXPECT_TRUE(edge.IsEdge());
  EXPECT_EQ(edge.GetSourceNodeIdentifier(), "src_node_id");
  EXPECT_EQ(edge.GetDestNodeIdentifier(), "dst_node_id");
  EXPECT_EQ(edge.ShortDebugString(), "{p0:\"v0\", p1:1}");
  EXPECT_EQ(edge.FullDebugString(),
            "GraphEdge{$name:\"ElementTable\", $id:b\"id\", "
            "$labels:[\"label1\", \"label2\"], "
            "$source_node_id:b\"src_node_id\", $dest_node_id:b\"dst_node_id\", "
            "p0:String(\"v0\"), p1:Int32(1)}");

  const Value copied_edge = edge;
  EXPECT_EQ(copied_edge.GetIdentifier(), "id");
  EXPECT_THAT(copied_edge.property_values(),
              ElementsAre(Eq(p0_value), Eq(p1_value)));
  EXPECT_EQ(copied_edge.GetSourceNodeIdentifier(), "src_node_id");
  EXPECT_EQ(copied_edge.GetDestNodeIdentifier(), "dst_node_id");
}

// TODO: Nested graph element should be rejected.
TEST(GraphElementValueTest, NestedGraphElementTest) {
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value node =
      GraphNode({"graph_name"}, "id", {{"p0", p0_value}, {"p1", p1_value}},
                {"label"}, "ElementTable");
  const Value nested_node = GraphNode({"graph_name"}, "id2", {{"p", node}},
                                      {"label"}, "ElementTable");
  ZETASQL_EXPECT_OK(nested_node.FindPropertyByName("p"));
}

TEST(GraphElementValueTest, ValueContentEqualsTestSameValue) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  const Value node2 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  EXPECT_TRUE(node1.Equals(node2));
}

TEST(GraphElementValueTest, ValueContentEqualsTestSameValueWithNulls) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::NullInt32()}},
                {"label"}, "ElementTable");
  const Value node2 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::NullInt32()}},
                {"label"}, "ElementTable");
  EXPECT_TRUE(node1.Equals(node2));
}

TEST(GraphElementValueTest, ValueContentEqualsTestDifferentValue) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  const Value node2 =
      GraphNode({"graph_name"}, "id2",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(2)}},
                {"label"}, "ElementTable");
  EXPECT_FALSE(node1.Equals(node2));
}

TEST(GraphElementValueTest, ValueContentEqualsTestDifferentValueWithNulls) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::NullInt32()}},
                {"label"}, "ElementTable");
  const Value node2 =
      GraphNode({"graph_name"}, "id2",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(2)}},
                {"label"}, "ElementTable");
  EXPECT_FALSE(node1.Equals(node2));
}

TEST(GraphElementValueTest, ValueContentEqualsTestDifferentIdentifier) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  const Value node2 =
      GraphNode({"graph_name"}, "id2",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  EXPECT_FALSE(node1.Equals(node2));
}

TEST(GraphElementValueTest, ValueContentEqualsTestDifferentColumns) {
  const Value node1 =
      GraphNode({"graph_name"}, "id1",
                {{"p0", Value::String("v0")}, {"p1", Value::Int32(1)}},
                {"label"}, "ElementTable");
  const Value node2 = GraphNode({"graph_name"}, "id1",
                                {{"p0", Value::String("v0")},
                                 {"p1", Value::Int32(1)},
                                 {"p2", Value::Bool(true)}},
                                {"label"}, "ElementTable");
  EXPECT_FALSE(node1.Equals(node2));
}

TEST_P(GraphElementValueTest, GetLabels) {
  // GetLabels sorts labels in alphabet order case-insensitively and preserves
  // the original case.
  const Value element = MakeElement(
      {"graph_name"}, "id", {}, {"label2", "laBel3", "Label1"}, "ElementTable");
  EXPECT_THAT(element.GetLabels(), ElementsAre("Label1", "label2", "laBel3"));
}

}  // namespace

}  // namespace zetasql
