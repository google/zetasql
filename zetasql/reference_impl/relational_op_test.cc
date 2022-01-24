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

// Tests of relational operators that don't warrant their own files.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/wire_format_lite.h"
#include "zetasql/common/evaluator_test_table.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/test_relational_op.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_test_util.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/clock.h"

using absl::nullopt;

using google::protobuf::internal::WireFormatLite;

using testing::_;
using testing::AnyOf;
using testing::ContainsRegex;
using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Eq;
using testing::HasSubstr;
using testing::IsEmpty;
using testing::IsNull;
using testing::Matcher;
using testing::Not;
using testing::Pointee;
using testing::PrintToString;
using testing::SizeIs;
using testing::TestWithParam;
using testing::UnorderedElementsAreArray;
using testing::ValuesIn;

extern absl::Flag<int64_t>
    FLAGS_zetasql_simple_iterator_call_time_now_rows_period;

namespace zetasql {

using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

using SharedProtoState = TupleSlot::SharedProtoState;

// Teach googletest how to print ColumnFilters.
void PrintTo(const ColumnFilter& filter, std::ostream* os) {
  switch (filter.kind()) {
    case ColumnFilter::kRange:
      *os << "<lower_bound: " << filter.lower_bound()
          << " upper_bound: " << filter.upper_bound() << ">";
      break;
    case ColumnFilter::kInList:
      *os << "<in_list: " << PrintToString(filter.in_list()) << ">";
      break;
    default:
      *os << "Unsupported ColumnFilter Kind";
      break;
  }
}

namespace {

static const auto DEFAULT_ERROR_MODE =
    ResolvedFunctionCallBase::DEFAULT_ERROR_MODE;

// For readability.
std::vector<const TupleSchema*> EmptyParamsSchemas() { return {}; }
std::vector<const TupleData*> EmptyParams() { return {}; }

std::unique_ptr<ScalarFunctionBody> CreateFunction(FunctionKind kind,
                                                   const Type* output_type) {
  LanguageOptions language_options;
  language_options.EnableMaximumLanguageFeaturesForDevelopment();
  return BuiltinScalarFunction::CreateValidated(kind, language_options,
                                                output_type, {})
      .value();
}

absl::StatusOr<std::unique_ptr<ExprArg>> AssignValueToVar(VariableId var,
                                                          const Value& value) {
  ZETASQL_ASSIGN_OR_RETURN(auto const_expr, ConstExpr::Create(value));
  return absl::make_unique<ExprArg>(var, std::move(const_expr));
}

// Convenience method to produce an ExprArg representing "result = v1 + v2".
//
// All operands should have type <type>, which will be the type of the result
// as well.
absl::StatusOr<std::unique_ptr<ExprArg>> ComputeSum(VariableId v1,
                                                    VariableId v2,
                                                    VariableId result,
                                                    const Type* type) {
  std::vector<std::unique_ptr<ValueExpr>> add_args(2);
  ZETASQL_ASSIGN_OR_RETURN(add_args[0], DerefExpr::Create(v1, type));
  ZETASQL_ASSIGN_OR_RETURN(add_args[1], DerefExpr::Create(v2, type));
  ZETASQL_ASSIGN_OR_RETURN(auto add_expr, ScalarFunctionCallExpr::Create(
                                      CreateFunction(FunctionKind::kAdd, type),
                                      std::move(add_args)));
  return absl::make_unique<ExprArg>(result, std::move(add_expr));
}

// Convience method to produce an ExprArg representing "result = v1 + v2".
// This is similar to the above method, except the 2nd operand is a constant,
// rather than a variable.
absl::StatusOr<std::unique_ptr<ExprArg>> ComputeSum(VariableId v1, Value v2,
                                                    VariableId result) {
  std::vector<std::unique_ptr<ValueExpr>> add_args(2);
  ZETASQL_ASSIGN_OR_RETURN(add_args[0], DerefExpr::Create(v1, v2.type()));
  ZETASQL_ASSIGN_OR_RETURN(add_args[1], ConstExpr::Create(v2));
  ZETASQL_ASSIGN_OR_RETURN(
      auto add_expr,
      ScalarFunctionCallExpr::Create(
          CreateFunction(FunctionKind::kAdd, v2.type()), std::move(add_args)));
  return absl::make_unique<ExprArg>(result, std::move(add_expr));
}

// Returns a RelationalOp representing <input>, filtered to include rows only
// where <var> has value less than <value>. <var> and <value> must be the same
// type.
absl::StatusOr<std::unique_ptr<RelationalOp>> FilterLessThan(
    VariableId var, Value value, std::unique_ptr<RelationalOp> input) {
  std::vector<std::unique_ptr<ValueExpr>> less_than_args(2);
  ZETASQL_ASSIGN_OR_RETURN(less_than_args[0], DerefExpr::Create(var, value.type()));
  ZETASQL_ASSIGN_OR_RETURN(less_than_args[1], ConstExpr::Create(value));
  ZETASQL_ASSIGN_OR_RETURN(auto predicate,
                   ScalarFunctionCallExpr::Create(
                       CreateFunction(FunctionKind::kLess, BoolType()),
                       std::move(less_than_args), DEFAULT_ERROR_MODE));
  return FilterOp::Create(std::move(predicate), std::move(input));
}

// Test fixture for implementations of RelationalOp::CreateIterator.
class CreateIteratorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    TypeFactory* type_factory = test_values::static_type_factory();
    ZETASQL_ASSERT_OK(type_factory->MakeProtoType(
        zetasql_test__::KitchenSinkPB::descriptor(), &proto_type_));
    ZETASQL_ASSERT_OK(type_factory->MakeArrayType(proto_type_, &proto_array_type_));
  }

  Value GetProtoValue(int i) const {
    zetasql_test__::KitchenSinkPB proto;
    proto.set_int64_key_1(i);
    proto.set_int64_key_2(10 * i);

    return Value::Proto(proto_type_, SerializeToCord(proto));
  }

  const ProtoType* proto_type_ = nullptr;
  const ArrayType* proto_array_type_ = nullptr;
};

TEST_F(CreateIteratorTest, TestRelationalOp) {
  std::vector<TupleData> tuples;
  std::vector<std::vector<const SharedProtoState*>> shared_states;
  for (int i = 0; i < 5; ++i) {
    TupleData tuple(/*num_slots=*/2);
    shared_states.emplace_back();
    for (int slot_idx = 0; slot_idx < 2; ++slot_idx) {
      TupleSlot* slot = tuple.mutable_slot(slot_idx);
      slot->SetValue(GetProtoValue(i));
      shared_states.back().push_back(slot->mutable_shared_proto_state()->get());
    }
    tuples.push_back(tuple);
  }

  VariableId foo("foo"), bar("bar");
  TestRelationalOp op({foo, bar}, tuples,
                      /*preserves_order=*/true);
  std::unique_ptr<TupleSchema> output_schema = op.CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(foo, bar));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      op.CreateIterator(EmptyParams(), /*num_extra_slots=*/5, &context));
  EXPECT_EQ(iter->DebugString(), "TestTupleIterator");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> output_tuples,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(output_tuples.size(), tuples.size());
  for (int i = 0; i < tuples.size(); ++i) {
    const TupleData& expected = tuples[i];
    const TupleData& actual = output_tuples[i];
    ASSERT_EQ(expected.num_slots() + 5, actual.num_slots());
    for (int j = 0; j < expected.num_slots(); ++j) {
      const TupleSlot& actual_slot = actual.slot(j);
      EXPECT_EQ(expected.slot(j).value(), actual_slot.value());

      const SharedProtoState* shared_state = shared_states[i][j];
      EXPECT_TRUE(actual_slot.mutable_shared_proto_state()->get() ==
                  shared_state);
    }
  }
}

using EvalRelationalOpTest = CreateIteratorTest;

// Tests TestRelationalOp::Eval (which is just RelationalOp::Eval).
TEST_F(EvalRelationalOpTest, TestRelationalOp) {
  std::vector<TupleData> tuples;
  std::vector<std::vector<const SharedProtoState*>> shared_states;
  for (int i = 0; i < 5; ++i) {
    TupleData tuple(/*num_slots=*/2);
    shared_states.emplace_back();
    for (int slot_idx = 0; slot_idx < 2; ++slot_idx) {
      TupleSlot* slot = tuple.mutable_slot(slot_idx);
      slot->SetValue(GetProtoValue(i));
      shared_states.back().push_back(slot->mutable_shared_proto_state()->get());
    }
    tuples.push_back(tuple);
  }

  VariableId foo("foo"), bar("bar");
  TestRelationalOp op({foo, bar}, tuples,
                      /*preserves_order=*/true);
  std::unique_ptr<TupleSchema> output_schema = op.CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(foo, bar));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TupleIterator> iter,
                       op.Eval(EmptyParams(), /*num_extra_slots=*/5, &context));
  EXPECT_EQ(iter->DebugString(),
            "PassThroughTupleIterator(Factory for TestTupleIterator)");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> output_tuples,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(output_tuples.size(), tuples.size());
  for (int i = 0; i < tuples.size(); ++i) {
    const TupleData& expected = tuples[i];
    const TupleData& actual = output_tuples[i];
    ASSERT_EQ(expected.num_slots() + 5, actual.num_slots());
    for (int j = 0; j < expected.num_slots(); ++j) {
      const TupleSlot& actual_slot = actual.slot(j);
      EXPECT_EQ(expected.slot(j).value(), actual_slot.value());

      const SharedProtoState* shared_state = shared_states[i][j];
      EXPECT_TRUE(actual_slot.mutable_shared_proto_state()->get() ==
                  shared_state);
      EXPECT_TRUE(!shared_state->has_value());
    }
  }
}

TEST(ColumnFilterArgTest, InArray) {
  VariableId p("p");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64ArrayType()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto arg, InArrayColumnFilterArg::Create(
                    VariableId("foo"), /*column_idx=*/3, std::move(deref_p)));
  EXPECT_EQ(arg->column_idx(), 3);
  EXPECT_EQ(arg->DebugString(),
            "InArrayColumnFilterArg($foo, column_idx: 3, array: $p)");

  const TupleSchema params_schemas({p});
  const TupleData params_data = CreateTupleDataFromValues({Value::Array(
      DoubleArrayType(),
      {Double(10), Double(11), Double(0.0 / 0.0), Double(12), NullDouble()})});
  ZETASQL_ASSERT_OK(arg->SetSchemasForEvaluation({&params_schemas}));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ColumnFilter> column_filter,
                       arg->Eval({&params_data}, &context));
  ASSERT_EQ(column_filter->kind(), ColumnFilter::kInList);
  EXPECT_THAT(column_filter->in_list(),
              ElementsAre(Double(10), Double(11), Double(12)));

  const TupleData params_null_data =
      CreateTupleDataFromValues({Null(DoubleArrayType())});
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ColumnFilter> column_filter_null,
                       arg->Eval({&params_null_data}, &context));
  ASSERT_EQ(column_filter_null->kind(), ColumnFilter::kInList);
  EXPECT_THAT(column_filter_null->in_list(), IsEmpty());
}

TEST(ColumnFilterArgTest, InList) {
  VariableId p1("p1"), p2("p2"), p3("p3"), p4("p4"), p5("p5");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p1, DerefExpr::Create(p1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p2, DerefExpr::Create(p2, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p3, DerefExpr::Create(p3, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p4, DerefExpr::Create(p4, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p5, DerefExpr::Create(p5, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> elements;
  elements.push_back(std::move(deref_p1));
  elements.push_back(std::move(deref_p2));
  elements.push_back(std::move(deref_p3));
  elements.push_back(std::move(deref_p4));
  elements.push_back(std::move(deref_p5));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto arg, InListColumnFilterArg::Create(
                    VariableId("foo"), /*column_idx=*/3, std::move(elements)));
  EXPECT_EQ(arg->column_idx(), 3);
  EXPECT_EQ(arg->DebugString(),
            "InListColumnFilterArg($foo, column_idx: 3, "
            "elements: ($p1, $p2, $p3, $p4, $p5))");

  const TupleSchema params_schemas({p1, p2, p3, p4, p5});
  const TupleData params_data = CreateTupleDataFromValues(
      {Double(10), Double(11), Double(0.0 / 0.0), Double(12), NullDouble()});
  ZETASQL_ASSERT_OK(arg->SetSchemasForEvaluation({&params_schemas}));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ColumnFilter> column_filter,
                       arg->Eval({&params_data}, &context));
  ASSERT_EQ(column_filter->kind(), ColumnFilter::kInList);
  EXPECT_THAT(column_filter->in_list(),
              ElementsAre(Double(10), Double(11), Double(12)));
}

TEST(ColumnFilterArgTest, HalfUnbounded) {
  for (const HalfUnboundedColumnFilterArg::Kind kind :
       {HalfUnboundedColumnFilterArg::kLE, HalfUnboundedColumnFilterArg::kGE}) {
    ZETASQL_LOG(INFO) << "Testing kind: " << kind;

    VariableId p("p");
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto arg, HalfUnboundedColumnFilterArg::Create(
                                       VariableId("foo"), /*column_idx=*/3,
                                       kind, std::move(deref_p)));
    EXPECT_EQ(arg->column_idx(), 3);
    switch (kind) {
      case HalfUnboundedColumnFilterArg::kLE:
        EXPECT_EQ(
            arg->DebugString(),
            "HalfUnboundedColumnFilterArg($foo, column_idx: 3, filter: <= $p)");
        break;
      case HalfUnboundedColumnFilterArg::kGE:
        EXPECT_EQ(
            arg->DebugString(),
            "HalfUnboundedColumnFilterArg($foo, column_idx: 3, filter: >= $p)");
        break;
    }

    const TupleSchema params_schemas({p});
    ZETASQL_ASSERT_OK(arg->SetSchemasForEvaluation({&params_schemas}));

    for (int i = 0; i < 2; ++i) {
      Value value;
      switch (i) {
        case 0:
          value = Double(10.0);
          break;
        case 1:
          value = NullDouble();
          break;
        case 2:
          value = Double(0.0 / 0.0);
          break;
        default:
          FAIL() << "Unexpected value of i: " << i;
      }
      ZETASQL_LOG(INFO) << "Testing value: " << value;
      const TupleData params_data = CreateTupleDataFromValues({value});

      EvaluationContext context((EvaluationOptions()));
      ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ColumnFilter> column_filter,
                           arg->Eval({&params_data}, &context));

      if (i == 0) {
        ASSERT_EQ(column_filter->kind(), ColumnFilter::kRange);
        switch (kind) {
          case HalfUnboundedColumnFilterArg::kLE:
            EXPECT_FALSE(column_filter->lower_bound().is_valid());
            EXPECT_EQ(column_filter->upper_bound(), Double(10.0));
            break;
          case HalfUnboundedColumnFilterArg::kGE:
            EXPECT_EQ(column_filter->lower_bound(), Double(10.0));
            EXPECT_FALSE(column_filter->upper_bound().is_valid());
            break;
        }
      } else {
        ASSERT_EQ(column_filter->kind(), ColumnFilter::kInList);
        EXPECT_THAT(column_filter->in_list(), IsEmpty());
      }
    }
  }
}

MATCHER_P2(IsRangeColumnFilterWith, lower_bound, upper_bound, "") {
  if (arg.kind() != ColumnFilter::kRange) return false;
  if (lower_bound.is_valid() != arg.lower_bound().is_valid() ||
      upper_bound.is_valid() != arg.upper_bound().is_valid()) {
    return false;
  }
  if (lower_bound.is_valid() && !lower_bound.Equals(arg.lower_bound())) {
    return false;
  }
  if (upper_bound.is_valid() && !upper_bound.Equals(arg.upper_bound())) {
    return false;
  }
  return true;
}

MATCHER_P(IsListColumnFilterWith, in_list, "") {
  return arg.kind() == ColumnFilter::kInList && arg.in_list() == in_list;
}

TEST(IntersectColumnFiltersTest, OneRangeFilter) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(10), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, OneRangeFilterWithNoLowerBound) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Value(), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Value(), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, OneRangeFilterWithNoUpperBound) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Value()));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(10), Value()))));
}

TEST(IntersectColumnFiltersTest, OneRangeFilterWithNoBounds) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Value(), Value()));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Value(), Value()))));
}

TEST(IntersectColumnFiltersTest, OneInListFilter) {
  const std::vector<Value> values = {Int64(10), Int64(20), Int64(30)};
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(values));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsListColumnFilterWith(values))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersOverlap1) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(15), Int64(25)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(15), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersOverlap2) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(15), Int64(25)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(15), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersCoincide) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(10), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersFirstInSecond) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(0), Int64(30)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(10), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersSecondInFirst) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(0), Int64(30)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(10), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersCoincideLeftEdge) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(0), Int64(30)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(0), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(0), Int64(20)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersCoincideRightEdge) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(30)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(20), Int64(30)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsRangeColumnFilterWith(Int64(20), Int64(30)))));
}

TEST(IntersectColumnFiltersTest, TwoRangeFiltersDisjoint) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(10), Int64(20)));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(30), Int64(40)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, TwoInLists) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(20), Int64(40), Int64(60), Int64(80)})));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsListColumnFilterWith(
                  std::vector<Value>({Int64(20), Int64(40)})))));
}

TEST(IntersectColumnFiltersTest, TwoInListsFirstEmpty) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(std::vector<Value>()));
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(20), Int64(40), Int64(60), Int64(80)})));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, TwoInListsSecondEmpty) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(20), Int64(40), Int64(60), Int64(80)})));
  filters.push_back(absl::make_unique<ColumnFilter>(std::vector<Value>()));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, InListAndGeRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(20), Value()));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsListColumnFilterWith(
                  std::vector<Value>({Int64(20), Int64(30), Int64(40)})))));
}

TEST(IntersectColumnFiltersTest, InListAndLeRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Value(), Int64(30)));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsListColumnFilterWith(
                  std::vector<Value>({Int64(10), Int64(20), Int64(30)})))));
}

TEST(IntersectColumnFiltersTest, InListAndRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(20), Int64(30)));
  EXPECT_THAT(EvaluatorTableScanOp::IntersectColumnFilters(filters),
              IsOkAndHolds(Pointee(IsListColumnFilterWith(
                  std::vector<Value>({Int64(20), Int64(30)})))));
}

TEST(IntersectColumnFiltersTest, InListAndDisjointRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(100), Int64(200)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, InListAndDisjointGeRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(100), Value()));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, InListAndDisjointLeRange) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Value(), Int64(0)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

TEST(IntersectColumnFiltersTest, DisjointRangesAndInList) {
  std::vector<std::unique_ptr<ColumnFilter>> filters;
  filters.push_back(absl::make_unique<ColumnFilter>(
      std::vector<Value>({Int64(10), Int64(20), Int64(30), Int64(40)})));
  filters.push_back(absl::make_unique<ColumnFilter>(Int64(30), Value()));
  filters.push_back(absl::make_unique<ColumnFilter>(Value(), Int64(20)));
  EXPECT_THAT(
      EvaluatorTableScanOp::IntersectColumnFilters(filters),
      IsOkAndHolds(Pointee(IsListColumnFilterWith(std::vector<Value>()))));
}

EvaluationOptions GetScramblingEvaluationOptions() {
  EvaluationOptions options;
  options.scramble_undefined_orderings = true;
  return options;
}

EvaluationOptions GetIntermediateMemoryEvaluationOptions(int64_t total_bytes) {
  EvaluationOptions options;
  options.max_intermediate_byte_size = total_bytes;
  return options;
}

TEST_F(CreateIteratorTest, EvaluatorTableScanOp) {
  VariableId x("x"), y("y"), z("z");
  SimpleTable table("TestTable", {{"column0", types::Int64Type()},
                                  {"column1", types::Int64Type()},
                                  {"column2", types::StringType()},
                                  {"column3", proto_type_}});
  table.SetContents(
      {{Int64(10), Int64(100), String("foo1"), GetProtoValue(0)},
       {Int64(20), Int64(200), String("foo2"), GetProtoValue(1)}});

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op,
      EvaluatorTableScanOp::Create(&table, /*alias=*/"", {2, 3, 1},
                                   {"column2", "column3", "column1"}, {x, y, z},
                                   /*and_filters=*/{}, /*read_time=*/nullptr));
  EXPECT_EQ(scan_op->IteratorDebugString(),
            "EvaluatorTableTupleIterator(TestTable)");
  EXPECT_EQ(scan_op->DebugString(),
            "EvaluatorTableScanOp(\n"
            "+-column2#2\n"
            "+-column3#3\n"
            "+-column1#1\n"
            "+-table: TestTable)");
  std::unique_ptr<TupleSchema> output_schema = scan_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x, y, z));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "EvaluatorTableTupleIterator(TestTable)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(
      data[0].slots(),
      ElementsAre(IsTupleSlotWith(String("foo1"), IsNull()),
                  IsTupleSlotWith(GetProtoValue(0), Pointee(Eq(nullopt))),
                  IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(
      data[1].slots(),
      ElementsAre(IsTupleSlotWith(String("foo2"), IsNull()),
                  IsTupleSlotWith(GetProtoValue(1), Pointee(Eq(nullopt))),
                  IsTupleSlotWith(Int64(200), IsNull()), _));

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(EvaluatorTableTupleIterator(TestTable))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, EvaluatorTableScanOpWithColumnFilter) {
  VariableId x("x"), y("y"), z("z");
  EvaluatorTestTable table(
      "TestTable",
      {{"column0", types::Int64Type()},
       {"column1", types::Int64Type()},
       {"column2", types::StringType()},
       {"column3", proto_type_}},
      {{Int64(10), Int64(100), String("foo1"), GetProtoValue(0)},
       {Int64(20), Int64(200), String("foo2"), GetProtoValue(1)}},
      /*end_status=*/absl::OkStatus(), /*column_filter_idxs=*/{0, 1});

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto array_expr,
                       ConstExpr::Create(Int64Array({100, 110})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto filter,
                       InArrayColumnFilterArg::Create(z, /*column_idx=*/2,
                                                      std::move(array_expr)));

  std::vector<std::unique_ptr<ColumnFilterArg>> and_filters;
  and_filters.push_back(std::move(filter));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op,
      EvaluatorTableScanOp::Create(
          &table, /*alias=*/"", {2, 3, 1}, {"column2", "column3", "column1"},
          {x, y, z}, std::move(and_filters), /*read_time=*/nullptr));
  EXPECT_EQ(scan_op->IteratorDebugString(),
            "EvaluatorTableTupleIterator(TestTable)");
  EXPECT_EQ(scan_op->DebugString(),
            "EvaluatorTableScanOp(\n"
            "+-column2#2\n"
            "+-column3#3\n"
            "+-column1#1\n"
            "+-InArrayColumnFilterArg($z, column_idx: 2, "
            "array: ConstExpr([100, 110]))\n"
            "+-table: TestTable)");
  std::unique_ptr<TupleSchema> output_schema = scan_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x, y, z));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "EvaluatorTableTupleIterator(TestTable)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_THAT(
      data[0].slots(),
      ElementsAre(IsTupleSlotWith(String("foo1"), IsNull()),
                  IsTupleSlotWith(GetProtoValue(0), Pointee(Eq(nullopt))),
                  IsTupleSlotWith(Int64(100), IsNull()), _));
}

TEST_F(CreateIteratorTest, EvaluatorTableScanOpFailure) {
  const std::string error = "Failed to read row from TestTable";
  const absl::Status failure = zetasql_base::OutOfRangeErrorBuilder() << error;

  EvaluatorTestTable table("TestTable", {{"column0", types::Int64Type()}},
                           {{Int64(10)}, {Int64(20)}}, failure);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op, EvaluatorTableScanOp::Create(&table, /*alias=*/"", {0},
                                                 {"column0"}, {VariableId("x")},
                                                 /*and_filters=*/{},
                                                 /*read_time=*/nullptr));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  absl::Status status;
  std::vector<TupleData> data = ReadFromTupleIteratorFull(iter.get(), &status);
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(10), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(20), IsNull()), _));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange, error));
}

TEST_F(CreateIteratorTest, EvaluatorTableScanOpCancellation) {
  int64_t num_cancel_calls = 0;
  const std::function<void()> cancel_cb = [&num_cancel_calls]() {
    ++num_cancel_calls;
  };
  EvaluatorTestTable table("TestTable", {{"column0", types::Int64Type()}},
                           {{Int64(10)}, {Int64(20)}}, absl::OkStatus(),
                           /*column_filter_idxs=*/{}, cancel_cb);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op, EvaluatorTableScanOp::Create(&table, /*alias=*/"", {0},
                                                 {"column0"}, {VariableId("x")},
                                                 /*and_filters=*/{},
                                                 /*read_time=*/nullptr));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  std::vector<TupleData> data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));
  EXPECT_EQ(num_cancel_calls, 1);
}

TEST_F(CreateIteratorTest, EvaluatorTableScanOpDeadlineExceeded) {
  absl::SetFlag(&FLAGS_zetasql_simple_iterator_call_time_now_rows_period, 1);
  int64_t num_cancel_calls = 0;
  const std::function<void()> cancel_cb = [&num_cancel_calls]() {
    ++num_cancel_calls;
  };

  std::vector<absl::Time> deadlines;
  const std::function<void(absl::Time)> set_deadline_cb =
      [&deadlines](absl::Time deadline) { deadlines.push_back(deadline); };

  zetasql_base::SimulatedClock clock(absl::UnixEpoch());
  EvaluatorTestTable table("TestTable", {{"column0", types::Int64Type()}},
                           {{Int64(10)}, {Int64(20)}}, absl::OkStatus(),
                           /*column_filter_idxs=*/{}, cancel_cb,
                           set_deadline_cb, &clock);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op, EvaluatorTableScanOp::Create(&table, /*alias=*/"", {0},
                                                 {"column0"}, {VariableId("x")},
                                                 /*and_filters=*/{},
                                                 /*read_time=*/nullptr));

  EvaluationContext context((EvaluationOptions()));
  context.SetClockAndClearCurrentTimestamp(&clock);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  const absl::Time deadline = absl::UnixEpoch() + absl::Seconds(10);
  context.SetStatementEvaluationDeadline(deadline);

  const TupleData* tuple = iter->Next();
  EXPECT_EQ(num_cancel_calls, 0);
  EXPECT_THAT(deadlines, ElementsAre(deadline));
  ASSERT_TRUE(tuple != nullptr);
  EXPECT_EQ(Tuple(&iter->Schema(), tuple).DebugString(), "<x:10>");

  clock.AdvanceTime(absl::Seconds(15));
  tuple = iter->Next();
  EXPECT_EQ(num_cancel_calls, 0);
  EXPECT_THAT(deadlines, ElementsAre(deadline));
  EXPECT_TRUE(tuple == nullptr);
  EXPECT_THAT(iter->Status(), StatusIs(absl::StatusCode::kDeadlineExceeded, _));
}

// Test implementation of CppValueArg; associates the variable with a
// direct std::string (as opposed to a zetasql::Value representing a string).
class TestCppValueArg : public CppValueArg {
 public:
  TestCppValueArg(VariableId var, absl::string_view value)
      : CppValueArg(var, absl::StrCat("TestCppValueArg: ", value)),
        value_(value) {}

  std::unique_ptr<CppValueBase> CreateValue(
      EvaluationContext* context) const override {
    EXPECT_NE(context, nullptr);
    return absl::make_unique<CppValue<std::string>>(value_);
  }

 private:
  std::string value_;
};

// RelationalOp implementation to test consumption of CppValue's produced by
// TestCppValueArg.
//
// Emits one row with schema <output_vars>, consisting of the values from
//   <tuple_vars> and <cpp_vars> concatenated together. CppValue's are converted
//   to zetasql::Value's by assuming they are of type CppValue<std::string>
//   and creating a zetasql::Value to represent the underlying string.
class TestCppValuesOp : public RelationalOp {
 public:
  // tuple_vars: Variables to be consumed, passed in to CreateIterator() via
  //   <params>, using the schema passed into SetSchemasForEvaluation().
  // cpp_vars: C++ Variables to be consumed; values are read from
  //   EvaluationContext, and are assumed to be of type CppValue<std::string>;
  //   Each C++ value translates into a zetasql::Value of STRING type.
  // output_vars: Variables to use in the output schema. Requirement:
  //   output_vars.size() == tuple_vars.size() + cpp_vars.size()
  TestCppValuesOp(std::vector<VariableId> tuple_vars,
                       std::vector<VariableId> cpp_vars,
                       std::vector<VariableId> output_vars)
      : cpp_vars_(cpp_vars), output_vars_(output_vars) {
    // Initialize slot indices for tuple variables to -1, we'll substitute in
    // the actual values during SetSchemasForEvaluation().
    for (const VariableId& v : tuple_vars) {
      tuple_vars_slots_[v] = std::make_pair(-1, -1);
    }
  }

  absl::Status SetSchemasForEvaluation(
      absl::Span<const TupleSchema* const> params_schemas) override {
    for (auto& pair : tuple_vars_slots_) {
      bool found = false;
      for (int i = 0; i < params_schemas.size(); ++i) {
        absl::optional<int> idx =
            params_schemas[i]->FindIndexForVariable(pair.first);
        if (idx.has_value()) {
          pair.second = std::make_pair(i, idx.value());
          found = true;
        }
      }
      if (!found) {
        return zetasql_base::InvalidArgumentErrorBuilder()
               << "Variable " << pair.first << " not found";
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<TupleIterator>> CreateIterator(
      absl::Span<const TupleData* const> params, int num_extra_slots,
      EvaluationContext* context) const override {
    std::vector<TupleData> tuple_data;
    tuple_data.emplace_back(tuple_vars_slots_.size() + cpp_vars_.size() +
                            num_extra_slots);
    // Include tuple variables first. Tuple variables exists to verify that
    // the LetOp is able to handle tuple variables and cpp variables
    // simultaneously.
    int slot_idx = 0;
    for (const auto& pair : tuple_vars_slots_) {
      tuple_data[0]
          .mutable_slot(slot_idx++)
          ->CopyFromSlot(params[pair.second.first]->slot(pair.second.second));
    }

    // Convert the std::string's into zetasql::Value's, and pack into a
    // TupleData to return to the caller.
    for (const auto& var : cpp_vars_) {
      tuple_data[0]
          .mutable_slot(slot_idx++)
          ->SetValue(Value::String(
              *CppValue<std::string>::Get(context->GetCppValue(var))));
    }
    return absl::make_unique<TestTupleIterator>(
        output_vars_, std::move(tuple_data), /*preserves_order=*/true,
        /*end_status=*/absl::OkStatus());
  }

  std::unique_ptr<TupleSchema> CreateOutputSchema() const override {
    return absl::make_unique<TupleSchema>(output_vars_);
  }

  std::string IteratorDebugString() const override {
    return TestTupleIterator::GetDebugString();
  }

  std::string DebugInternal(const std::string& indent,
                            bool verbose) const override {
    return absl::StrCat(
        "ConsumeTestCppValues: ",
        absl::StrJoin(cpp_vars_, ", ",
                      [](std::string* out, VariableId v) {
                        absl::StrAppend(out, v.ToString());
                      }),
        " => ",
        absl::StrJoin(output_vars_, ", ", [](std::string* out, VariableId v) {
          absl::StrAppend(out, v.ToString());
        }));
  }

 private:
  std::map<VariableId, std::pair<int, int>> tuple_vars_slots_;
  // CppValue'd variables; assumed to be instances of TestCppValueArg
  std::vector<VariableId> cpp_vars_;
  std::vector<VariableId> output_vars_;
};

TEST_F(CreateIteratorTest, LetOpCppValues) {
  VariableId a("a"), b("b"), n("n"), x("x"), y("y");
  VariableId out_a("out_a"), out_b("out_b"), out_x("out_x"), out_y("out_y");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_n, DerefExpr::Create(n, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto const_1, ConstExpr::Create(Int64(1)));

  std::vector<std::unique_ptr<ExprArg>> assign;
  assign.push_back(absl::make_unique<ExprArg>(a, std::move(const_1)));
  assign.push_back(absl::make_unique<ExprArg>(b, std::move(deref_n)));

  std::vector<std::unique_ptr<CppValueArg>> cpp_assign;
  cpp_assign.push_back(absl::make_unique<TestCppValueArg>(x, "value_x"));
  cpp_assign.push_back(absl::make_unique<TestCppValueArg>(y, "value_y"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto let_op,
      LetOp::Create(
          std::move(assign), std::move(cpp_assign),
          absl::make_unique<TestCppValuesOp>(
              std::vector<VariableId>{a, b}, std::vector<VariableId>{x, y},
              std::vector<VariableId>{out_a, out_b, out_x, out_y})));

  TupleSchema schema({n});
  ZETASQL_ASSERT_OK(let_op->SetSchemasForEvaluation({&schema}));
  std::unique_ptr<TupleSchema> output_schema = let_op->CreateOutputSchema();
  EXPECT_EQ("<out_a,out_b,out_x,out_y>", output_schema->DebugString());

  TupleData data(CreateTestTupleData({Int64(10)}));
  std::vector<const TupleData*> datas = {&data};
  EvaluationContext context((EvaluationOptions()));
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TupleIterator> iter,
        let_op->CreateIterator(datas, /*num_extra_slots=*/1, &context));
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> output_data,
                         ReadFromTupleIterator(iter.get()));
    ASSERT_EQ(1, output_data.size());
    EXPECT_THAT(output_data[0].slots(),
                ElementsAre(
                    /*out_a: */ IsTupleSlotWith(Int64(1), IsNull()),
                    /*out_b: */ IsTupleSlotWith(Int64(10), IsNull()),
                    /*out_x: */ IsTupleSlotWith(String("value_x"), IsNull()),
                    /*out_y: */ IsTupleSlotWith(String("value_y"), IsNull()),
                    /*extra slot*/ _));
  }

  // Verify that C++ variables have been removed from the EvaluationContext
  // when the LetOpTupleIterator is destroyed.
  EXPECT_THAT(context.GetCppValue(x), IsNull());
  EXPECT_THAT(context.GetCppValue(y), IsNull());
}

TEST_F(CreateIteratorTest, LetOp) {
  VariableId a("a"), x("x"), y("y"), z("z");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto const_1, ConstExpr::Create(Int64(1)));

  std::vector<std::unique_ptr<ValueExpr>> store_a_plus_one_in_x_args;
  store_a_plus_one_in_x_args.push_back(std::move(deref_a));
  store_a_plus_one_in_x_args.push_back(std::move(const_1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto store_a_plus_one_in_x_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(store_a_plus_one_in_x_args)));

  auto store_a_plus_one_in_x =
      absl::make_unique<ExprArg>(x, std::move(store_a_plus_one_in_x_expr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x, DerefExpr::Create(x, Int64Type()));

  auto store_x_in_y = absl::make_unique<ExprArg>(y, std::move(deref_x));

  std::vector<std::vector<const SharedProtoState*>> shared_states;
  const std::vector<TupleData> values = CreateTestTupleDatas(
      {{GetProtoValue(1)}, {GetProtoValue(2)}, {GetProtoValue(3)}},
      &shared_states);

  VariableId column("column");
  auto three_rows = absl::make_unique<TestRelationalOp>(
      std::vector<VariableId>{column}, values,
      /*preserves_order=*/true);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> prepend_rows_with_y_into_z_args;
  prepend_rows_with_y_into_z_args.push_back(
      absl::make_unique<ExprArg>(z, std::move(deref_y)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto prepend_rows_with_y_into_z,
      ComputeOp::Create(std::move(prepend_rows_with_y_into_z_args),
                        std::move(three_rows)));

  std::vector<std::unique_ptr<ExprArg>> let_op_assign;
  let_op_assign.push_back(std::move(store_a_plus_one_in_x));
  let_op_assign.push_back(std::move(store_x_in_y));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto let_op,
      LetOp::Create(std::move(let_op_assign),
                    /*cpp_assign=*/{}, std::move(prepend_rows_with_y_into_z)));
  EXPECT_EQ(let_op->IteratorDebugString(),
            "LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator))");
  EXPECT_EQ(let_op->DebugString(),
            "LetOp(\n"
            "+-assign: {\n"
            "| +-$x := Add($a, ConstExpr(1)),\n"
            "| +-$y := $x},\n"
            "+-cpp_assign: {},\n"
            "+-body: ComputeOp(\n"
            "  +-map: {\n"
            "  | +-$z := $y},\n"
            "  +-input: TestRelationalOp))");
  std::unique_ptr<TupleSchema> output_schema = let_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(column, z));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({a});
  TupleData params_data = CreateTestTupleData({Int64(5)});
  ZETASQL_ASSERT_OK(let_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TupleIterator> iter,
                       let_op->CreateIterator({&params_data},
                                              /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator))");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states[0][0])),
                          IsTupleSlotWith(Int64(6), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states[1][0])),
                          IsTupleSlotWith(Int64(6), IsNull()), _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states[2][0])),
                          IsTupleSlotWith(Int64(6), IsNull()), _));

  // Check that we get an error if the memory bound is too low. This is
  // particularly important if one of the variables holds an array that
  // represents a WITH table.
  EvaluationContext memory_context(
      GetIntermediateMemoryEvaluationOptions(/*total_bytes=*/1));
  EXPECT_THAT(let_op->CreateIterator({&params_data},
                                     /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted));
}

// For readability.
std::vector<JoinOp::HashJoinEqualityExprs> EmptyHashJoinEqualityExprs() {
  return {};
}

TEST_F(CreateIteratorTest, InnerJoin) {
  VariableId x1("x1"), x2("x2"), y1("y1"), y2("y2"), p("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(4), GetProtoValue(4)}},
          &shared_states1),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states2;
  auto input2 = absl::WrapUnique(new TestRelationalOp(
      {y1, y2},
      CreateTestTupleDatas(
          {{Int64(2), GetProtoValue(2)}, {Int64(3), GetProtoValue(3)}},
          &shared_states2),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x1));
  add_args.push_back(std::move(deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less_args;
  less_args.push_back(std::move(add_expr));
  less_args.push_back(std::move(deref_y1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kInnerJoin, EmptyHashJoinEqualityExprs(),
                     std::move(less_expr), std::move(input1), std::move(input2),
                     /*left_outputs=*/{}, /*right_outputs=*/{}));
  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(INNER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(INNER\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: Less(Add($x1, $p), $y1),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x1, x2, y1, y2));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(INNER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states2[0][1])),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states2[1][1])),
                          _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(scramble_iter->DebugString(),
            "ReorderingTupleIterator(JoinTupleIterator(INNER, "
            "left=TestTupleIterator, right=TestTupleIterator))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 5);
  EXPECT_EQ(data[1].num_slots(), 5);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, LeftOuterJoin) {
  VariableId x1("x1"), x2("x2"), y1("y1"), y2("y2"), y1_prime("y1'"),
      y2_prime("y2'"), p("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(4), GetProtoValue(4)}},
          &shared_states1),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states2;
  auto input2 = absl::WrapUnique(new TestRelationalOp(
      {y1, y2},
      CreateTestTupleDatas(
          {{Int64(2), GetProtoValue(2)}, {Int64(3), GetProtoValue(3)}},
          &shared_states2),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x1));
  add_args.push_back(std::move(deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less_args;
  less_args.push_back(std::move(add_expr));
  less_args.push_back(std::move(deref_y1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1_again, DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y2, DerefExpr::Create(y2, proto_type_));
  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y1_prime, std::move(deref_y1_again)));
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y2_prime, std::move(deref_y2)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kLeftOuterJoin, EmptyHashJoinEqualityExprs(),
                     std::move(less_expr), std::move(input1), std::move(input2),
                     /*left_outputs=*/{}, std::move(right_outputs)));

  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(LEFT OUTER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(LEFT OUTER\n"
      "+-right_outputs: {\n"
      "| +-$y1' := $y1,\n"
      "| +-$y2' := $y2},\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: Less(Add($x1, $p), $y1),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(x1, x2, y1_prime, y2_prime));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(LEFT OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states2[0][1])),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states2[1][1])),
                          _));
  EXPECT_THAT(
      data[2].slots(),
      ElementsAre(
          IsTupleSlotWith(Int64(4), IsNull()),
          IsTupleSlotWith(GetProtoValue(4),
                          HasRawPointer(shared_states1[1][1])),
          IsTupleSlotWith(NullInt64(), IsNull()),
          IsTupleSlotWith(Value::Null(proto_type_), Pointee(Eq(nullopt))), _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(scramble_iter->DebugString(),
            "ReorderingTupleIterator(JoinTupleIterator(LEFT OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 3);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 5);
  EXPECT_EQ(data[1].num_slots(), 5);
  EXPECT_EQ(data[2].num_slots(), 5);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, RightOuterJoin) {
  VariableId x1("x1"), x2("x2"), y1("y1"), y2("y2"), y1_prime("y1'"),
      y2_prime("y2'"), p("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states_right;
  auto input_right = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(4), GetProtoValue(4)}},
          &shared_states_right),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states_left;
  auto input_left = absl::WrapUnique(new TestRelationalOp(
      {y1, y2},
      CreateTestTupleDatas(
          {{Int64(2), GetProtoValue(2)}, {Int64(3), GetProtoValue(3)}},
          &shared_states_left),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x1));
  add_args.push_back(std::move(deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less_args;
  less_args.push_back(std::move(add_expr));
  less_args.push_back(std::move(deref_y1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1_again, DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y2, DerefExpr::Create(y2, proto_type_));
  std::vector<std::unique_ptr<ExprArg>> left_outputs;
  left_outputs.push_back(
      absl::make_unique<ExprArg>(y1_prime, std::move(deref_y1_again)));
  left_outputs.push_back(
      absl::make_unique<ExprArg>(y2_prime, std::move(deref_y2)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kRightOuterJoin, EmptyHashJoinEqualityExprs(),
                     std::move(less_expr), std::move(input_left),
                     std::move(input_right), std::move(left_outputs),
                     /*right_outputs=*/{}));

  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(RIGHT OUTER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(RIGHT OUTER\n"
      "+-left_outputs: {\n"
      "| +-$y1' := $y1,\n"
      "| +-$y2' := $y2},\n"
      "+-right_outputs: {},\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: Less(Add($x1, $p), $y1),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(y1_prime, y2_prime, x1, x2));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(RIGHT OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(
      data[0].slots(),
      ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                  IsTupleSlotWith(GetProtoValue(2),
                                  HasRawPointer(shared_states_left[0][1])),
                  IsTupleSlotWith(Int64(1), IsNull()),
                  IsTupleSlotWith(GetProtoValue(1),
                                  HasRawPointer(shared_states_right[0][1])),
                  _));
  EXPECT_THAT(
      data[1].slots(),
      ElementsAre(IsTupleSlotWith(Int64(3), IsNull()),
                  IsTupleSlotWith(GetProtoValue(3),
                                  HasRawPointer(shared_states_left[1][1])),
                  IsTupleSlotWith(Int64(1), IsNull()),
                  IsTupleSlotWith(GetProtoValue(1),
                                  HasRawPointer(shared_states_right[0][1])),
                  _));
  EXPECT_THAT(
      data[2].slots(),
      ElementsAre(
          IsTupleSlotWith(NullInt64(), IsNull()),
          IsTupleSlotWith(Value::Null(proto_type_), Pointee(Eq(nullopt))),
          IsTupleSlotWith(Int64(4), IsNull()),
          IsTupleSlotWith(GetProtoValue(4),
                          HasRawPointer(shared_states_right[1][1])),
          _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(scramble_iter->DebugString(),
            "ReorderingTupleIterator(JoinTupleIterator(RIGHT OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 3);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 5);
  EXPECT_EQ(data[1].num_slots(), 5);
  EXPECT_EQ(data[2].num_slots(), 5);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, FullOuterJoin) {
  VariableId x1("x1"), x2("x2"), y1("y1"), y2("y2"), x1_prime("x1'"),
      x2_prime("x2'"), y1_prime("y1'"), y2_prime("y2'"), param("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(2), GetProtoValue(2)}},
          &shared_states1),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states2;
  auto input2 = absl::WrapUnique(new TestRelationalOp(
      {y1, y2},
      CreateTestTupleDatas(
          {{Int64(2), GetProtoValue(2)}, {Int64(3), GetProtoValue(3)}},
          &shared_states2),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x1));
  add_args.push_back(std::move(deref_param));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> equal_args;
  equal_args.push_back(std::move(add_expr));
  equal_args.push_back(std::move(deref_y1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto equal_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kEqual, BoolType()),
                           std::move(equal_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1_again, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x2, DerefExpr::Create(x2, proto_type_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1_again, DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y2, DerefExpr::Create(y2, proto_type_));

  std::vector<std::unique_ptr<ExprArg>> left_outputs;
  left_outputs.push_back(
      absl::make_unique<ExprArg>(x1_prime, std::move(deref_x1_again)));
  left_outputs.push_back(
      absl::make_unique<ExprArg>(x2_prime, std::move(deref_x2)));

  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y1_prime, std::move(deref_y1_again)));
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y2_prime, std::move(deref_y2)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kFullOuterJoin, EmptyHashJoinEqualityExprs(),
                     std::move(equal_expr), std::move(input1),
                     std::move(input2), std::move(left_outputs),
                     std::move(right_outputs)));

  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(FULL OUTER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(FULL OUTER\n"
      "+-left_outputs: {\n"
      "| +-$x1' := $x1,\n"
      "| +-$x2' := $x2},\n"
      "+-right_outputs: {\n"
      "| +-$y1' := $y1,\n"
      "| +-$y2' := $y2},\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: Equal(Add($x1, $p), $y1),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(x1_prime, x2_prime, y1_prime, y2_prime));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({param});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(FULL OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(
      data[0].slots(),
      ElementsAre(
          IsTupleSlotWith(Int64(1), IsNull()),
          IsTupleSlotWith(GetProtoValue(1),
                          HasRawPointer(shared_states1[0][1])),
          IsTupleSlotWith(NullInt64(), IsNull()),
          IsTupleSlotWith(Value::Null(proto_type_), Pointee(Eq(nullopt))), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states1[1][1])),
                          IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states2[0][1])),
                          _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(NullInt64(), IsNull()),
                          IsTupleSlotWith(Value::Null(proto_type_),
                                          Pointee(Eq(nullopt))),
                          IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states2[1][1])),
                          _));

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, FullOuterAllRightTuplesJoin) {
  VariableId x("x"), y("y"), x_prime("x'"), y_prime("y'"), param("p");

  auto input1 = absl::WrapUnique(
      new TestRelationalOp({x}, CreateTestTupleDatas({{Int64(1)}, {Int64(2)}}),
                           /*preserves_order=*/true));

  auto input2 = absl::WrapUnique(
      new TestRelationalOp({y}, CreateTestTupleDatas({{Int64(2)}}),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x));
  add_args.push_back(std::move(deref_param));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> equal_args;
  equal_args.push_back(std::move(add_expr));
  equal_args.push_back(std::move(deref_y));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto equal_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kEqual, BoolType()),
                           std::move(equal_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_again, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y_again, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> left_outputs;
  left_outputs.push_back(
      absl::make_unique<ExprArg>(x_prime, std::move(deref_x_again)));

  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y_prime, std::move(deref_y_again)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kFullOuterJoin, EmptyHashJoinEqualityExprs(),
                     std::move(equal_expr), std::move(input1),
                     std::move(input2), std::move(left_outputs),
                     std::move(right_outputs)));

  EXPECT_EQ(
      "JoinOp(FULL OUTER\n"
      "+-left_outputs: {\n"
      "| +-$x' := $x},\n"
      "+-right_outputs: {\n"
      "| +-$y' := $y},\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: Equal(Add($x, $p), $y),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x_prime, y_prime));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({param});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(FULL OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(NullInt64(), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(2), IsNull()), _));

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/10));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, CrossApply) {
  VariableId x("x"), x2("x2"), y("y"), z("z"), p("p");

  // The left-hand side is three rows: (x:1), (x:4), and (x:6)
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x}, CreateTestTupleDatas({{Int64(1)}, {Int64(4)}, {Int64(6)}}),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x, DerefExpr::Create(x, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x2, std::move(deref_x)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x2, DerefExpr::Create(x2, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x2));
  add_args.push_back(std::move(deref_y));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  std::vector<std::unique_ptr<ExprArg>> map;
  map.push_back(absl::make_unique<ExprArg>(z, std::move(add_expr)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto let_body,
      ComputeOp::Create(
          std::move(map),
          absl::WrapUnique(new TestRelationalOp(
              {y}, CreateTestTupleDatas({{Int64(-1)}, {Int64(1)}}),
              /*preserves_order=*/true))));

  // The right-hand side is two rows: (y:-1, z:(x+y)) and (y:1, z:(x+y)), where
  // x is the value of the left-hand side.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto input2,
                       LetOp::Create(std::move(let_assign), /*cpp_assign=*/{},
                                     std::move(let_body)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_again, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less1_args;
  less1_args.push_back(std::move(deref_x_again));
  less1_args.push_back(std::move(deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less1,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less1_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_one_more_time,
                       DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_z, DerefExpr::Create(z, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less2_args;
  less2_args.push_back(std::move(deref_x_one_more_time));
  less2_args.push_back(std::move(deref_z));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less2,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less2_args)));

  std::vector<std::unique_ptr<ValueExpr>> and_args;
  and_args.push_back(std::move(less1));
  and_args.push_back(std::move(less2));

  // The join condition is x < p AND x < z, where p is a parameter that is
  // always 5. For the first two rows on the left-hand side, there is one tuple
  // on the right-hand side that satisfies the join condition, but for the third
  // row on the left-hand side, the join condition is not satisfied for any
  // tuple on the right-hand side.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_expr,
      ScalarFunctionCallExpr::Create(
          CreateFunction(FunctionKind::kAnd, BoolType()), std::move(and_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kCrossApply, EmptyHashJoinEqualityExprs(),
                     std::move(join_expr), std::move(input1), std::move(input2),
                     /*left_outputs=*/{}, /*right_outputs=*/{}));
  EXPECT_EQ(
      join_op->IteratorDebugString(),
      "JoinTupleIterator(CROSS APPLY, left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator)))");
  EXPECT_EQ(
      "JoinOp(CROSS APPLY\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: And(Less($x, $p), Less($x, $z)),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: LetOp(\n"
      "  +-assign: {\n"
      "  | +-$x2 := $x},\n"
      "  +-cpp_assign: {},\n"
      "  +-body: ComputeOp(\n"
      "    +-map: {\n"
      "    | +-$z := Add($x2, $y)},\n"
      "    +-input: TestRelationalOp)))",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x, y, z));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(5)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(
      iter->DebugString(),
      "JoinTupleIterator(CROSS APPLY, left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator)))");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(2), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(4), IsNull()),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(5), IsNull()), _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(
      scramble_iter->DebugString(),
      "ReorderingTupleIterator(JoinTupleIterator(CROSS APPLY, "
      "left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator))))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 4);
  EXPECT_EQ(data[1].num_slots(), 4);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> memory_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &memory_context));
  EXPECT_THAT(ReadFromTupleIterator(memory_iter.get()),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, OuterApply) {
  VariableId x("x"), x2("x2"), y("y"), z("z"), z2("z2"), p("p");

  // Three rows: (x:1), (x:4), and (x:6)
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x}, CreateTestTupleDatas({{Int64(1)}, {Int64(4)}, {Int64(6)}}),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x, DerefExpr::Create(x, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x2, std::move(deref_x)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x2, DerefExpr::Create(x2, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> add_args;
  add_args.push_back(std::move(deref_x2));
  add_args.push_back(std::move(deref_y));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(add_args)));

  std::vector<std::unique_ptr<ExprArg>> map;
  map.push_back(absl::make_unique<ExprArg>(z, std::move(add_expr)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto let_body,
      ComputeOp::Create(
          std::move(map),
          absl::WrapUnique(new TestRelationalOp(
              {y}, CreateTestTupleDatas({{Int64(-1)}, {Int64(1)}}),
              /*preserves_order=*/true))));

  // The right-hand side is two rows: (y:-1, z:(x+y)) and (y:1, z:(x+y)), where
  // x is the value of the left-hand side.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto input2,
                       LetOp::Create(std::move(let_assign), /*cpp_assign=*/{},
                                     std::move(let_body)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_again, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less1_args;
  less1_args.push_back(std::move(deref_x_again));
  less1_args.push_back(std::move(deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less1,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less1_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_one_more_time,
                       DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_z, DerefExpr::Create(z, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> less2_args;
  less2_args.push_back(std::move(deref_x_one_more_time));
  less2_args.push_back(std::move(deref_z));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto less2,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(less2_args)));

  std::vector<std::unique_ptr<ValueExpr>> and_args;
  and_args.push_back(std::move(less1));
  and_args.push_back(std::move(less2));

  // The join condition is x < p AND x < z, where p is a parameter that is
  // always 5. For the first two rows on the left-hand side, there is one tuple
  // on the right-hand side that satisfies the join condition, but for the third
  // row on the left-hand side, the join condition is not satisfied for any
  // tuple on the right-hand side.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_expr,
      ScalarFunctionCallExpr::Create(
          CreateFunction(FunctionKind::kAnd, BoolType()), std::move(and_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_z_again, DerefExpr::Create(z, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(z2, std::move(deref_z_again)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kOuterApply, EmptyHashJoinEqualityExprs(),
                     std::move(join_expr), std::move(input1), std::move(input2),
                     /*left_outputs=*/{},
                     /*right_outputs=*/std::move(right_outputs)));
  EXPECT_EQ(
      join_op->IteratorDebugString(),
      "JoinTupleIterator(OUTER APPLY, left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator)))");
  EXPECT_EQ(
      "JoinOp(OUTER APPLY\n"
      "+-right_outputs: {\n"
      "| +-$z2 := $z},\n"
      "+-hash_join_equality_left_exprs: {},\n"
      "+-hash_join_equality_right_exprs: {},\n"
      "+-remaining_condition: And(Less($x, $p), Less($x, $z)),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: LetOp(\n"
      "  +-assign: {\n"
      "  | +-$x2 := $x},\n"
      "  +-cpp_assign: {},\n"
      "  +-body: ComputeOp(\n"
      "    +-map: {\n"
      "    | +-$z := Add($x2, $y)},\n"
      "    +-input: TestRelationalOp)))",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x, z2));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(5)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(
      iter->DebugString(),
      "JoinTupleIterator(OUTER APPLY, left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator)))");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(2), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(4), IsNull()),
                          IsTupleSlotWith(Int64(5), IsNull()), _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(Int64(6), IsNull()),
                          IsTupleSlotWith(NullInt64(), IsNull()), _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(
      scramble_iter->DebugString(),
      "ReorderingTupleIterator(JoinTupleIterator(OUTER APPLY, "
      "left=TestTupleIterator, "
      "right=LetOpTupleIterator(ComputeTupleIterator(TestTupleIterator))))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 3);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 3);
  EXPECT_EQ(data[1].num_slots(), 3);
  EXPECT_EQ(data[2].num_slots(), 3);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> memory_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &memory_context));
  EXPECT_THAT(ReadFromTupleIterator(memory_iter.get()),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, InnerHashJoin) {
  VariableId x1("x1"), x2("x2"), y1("y1"), y2("y2"), p("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(2), GetProtoValue(1)}},
          &shared_states1),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states2;
  auto input2 = absl::WrapUnique(
      new TestRelationalOp({y1, y2},
                           CreateTestTupleDatas({{Int64(1), GetProtoValue(2)},
                                                 {Int64(1), GetProtoValue(3)},
                                                 {Int64(3), GetProtoValue(1)}},
                                                &shared_states2),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto left_deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> left_add_args;
  left_add_args.push_back(std::move(deref_x1));
  left_add_args.push_back(std::move(left_deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto left_add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(left_add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto right_deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> right_add_args;
  right_add_args.push_back(std::move(deref_y1));
  right_add_args.push_back(std::move(right_deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto right_add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, BoolType()),
                           std::move(right_add_args)));

  VariableId a("a"), b("b");
  JoinOp::HashJoinEqualityExprs equality_expr;
  equality_expr.left_expr =
      absl::make_unique<ExprArg>(a, std::move(left_add_expr));
  equality_expr.right_expr =
      absl::make_unique<ExprArg>(b, std::move(right_add_expr));

  std::vector<JoinOp::HashJoinEqualityExprs> equality_exprs;
  equality_exprs.push_back(std::move(equality_expr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto true_expr, ConstExpr::Create(Bool(true)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kInnerJoin, std::move(equality_exprs),
                     std::move(true_expr), std::move(input1), std::move(input2),
                     /*left_outputs=*/{}, /*right_outputs=*/{}));
  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(INNER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(INNER\n"
      "+-hash_join_equality_left_exprs: {\n"
      "| +-$a := Add($x1, $p)},\n"
      "+-hash_join_equality_right_exprs: {\n"
      "| +-$b := Add($y1, $p)},\n"
      "+-remaining_condition: ConstExpr(true),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(x1, x2, y1, y2));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(INNER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states2[0][1])),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states2[1][1])),
                          _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(scramble_iter->DebugString(),
            "ReorderingTupleIterator(JoinTupleIterator(INNER, "
            "left=TestTupleIterator, right=TestTupleIterator))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 5);
  EXPECT_EQ(data[1].num_slots(), 5);

  // Check that if the memory bound is too low, we get an error.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, FullOuterHashJoin) {
  VariableId x1("x1"), x2("x2"), x1_prime("x1'"), x2_prime("x2'"), y1("y1"),
      y2("y2"), y1_prime("y1'"), y2_prime("y2'"), p("p");

  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {x1, x2},
      CreateTestTupleDatas(
          {{Int64(1), GetProtoValue(1)}, {Int64(2), GetProtoValue(1)}},
          &shared_states1),
      /*preserves_order=*/true));

  std::vector<std::vector<const SharedProtoState*>> shared_states2;
  auto input2 = absl::WrapUnique(
      new TestRelationalOp({y1, y2},
                           CreateTestTupleDatas({{Int64(1), GetProtoValue(2)},
                                                 {Int64(1), GetProtoValue(3)},
                                                 {Int64(3), GetProtoValue(1)}},
                                                &shared_states2),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1, DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto left_deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> left_add_args;
  left_add_args.push_back(std::move(deref_x1));
  left_add_args.push_back(std::move(left_deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto left_add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(left_add_args)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1, DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto right_deref_p, DerefExpr::Create(p, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> right_add_args;
  right_add_args.push_back(std::move(deref_y1));
  right_add_args.push_back(std::move(right_deref_p));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto right_add_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, BoolType()),
                           std::move(right_add_args)));

  VariableId a("a"), b("b");
  JoinOp::HashJoinEqualityExprs equality_expr;
  equality_expr.left_expr =
      absl::make_unique<ExprArg>(a, std::move(left_add_expr));
  equality_expr.right_expr =
      absl::make_unique<ExprArg>(b, std::move(right_add_expr));

  std::vector<JoinOp::HashJoinEqualityExprs> equality_exprs;
  equality_exprs.push_back(std::move(equality_expr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto true_expr, ConstExpr::Create(Bool(true)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x1_output,
                       DerefExpr::Create(x1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x2_output,
                       DerefExpr::Create(x2, proto_type_));

  std::vector<std::unique_ptr<ExprArg>> left_outputs;
  left_outputs.push_back(
      absl::make_unique<ExprArg>(x1_prime, std::move(deref_x1_output)));
  left_outputs.push_back(
      absl::make_unique<ExprArg>(x2_prime, std::move(deref_x2_output)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y1_output,
                       DerefExpr::Create(y1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y2_output,
                       DerefExpr::Create(y2, proto_type_));

  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y1_prime, std::move(deref_y1_output)));
  right_outputs.push_back(
      absl::make_unique<ExprArg>(y2_prime, std::move(deref_y2_output)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto join_op,
      JoinOp::Create(JoinOp::kFullOuterJoin, std::move(equality_exprs),
                     std::move(true_expr), std::move(input1), std::move(input2),
                     std::move(left_outputs), std::move(right_outputs)));
  EXPECT_EQ(join_op->IteratorDebugString(),
            "JoinTupleIterator(FULL OUTER, left=TestTupleIterator, "
            "right=TestTupleIterator)");
  EXPECT_EQ(
      "JoinOp(FULL OUTER\n"
      "+-left_outputs: {\n"
      "| +-$x1' := $x1,\n"
      "| +-$x2' := $x2},\n"
      "+-right_outputs: {\n"
      "| +-$y1' := $y1,\n"
      "| +-$y2' := $y2},\n"
      "+-hash_join_equality_left_exprs: {\n"
      "| +-$a := Add($x1, $p)},\n"
      "+-hash_join_equality_right_exprs: {\n"
      "| +-$b := Add($y1, $p)},\n"
      "+-remaining_condition: ConstExpr(true),\n"
      "+-left_input: TestRelationalOp,\n"
      "+-right_input: TestRelationalOp)",
      join_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = join_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(x1_prime, x2_prime, y1_prime, y2_prime));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({p});
  TupleData params_data = CreateTestTupleData({Int64(0)});

  ZETASQL_ASSERT_OK(join_op->SetSchemasForEvaluation({&params_schema}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "JoinTupleIterator(FULL OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 4);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states2[0][1])),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states1[0][1])),
                          IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states2[1][1])),
                          _));
  EXPECT_THAT(
      data[2].slots(),
      ElementsAre(
          IsTupleSlotWith(Int64(2), IsNull()),
          IsTupleSlotWith(GetProtoValue(1),
                          HasRawPointer(shared_states1[1][1])),
          IsTupleSlotWith(NullInt64(), IsNull()),
          IsTupleSlotWith(Value::Null(proto_type_), Pointee(Eq(nullopt))), _));
  EXPECT_THAT(data[3].slots(),
              ElementsAre(IsTupleSlotWith(NullInt64(), IsNull()),
                          IsTupleSlotWith(Value::Null(proto_type_),
                                          Pointee(Eq(nullopt))),
                          IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states2[2][1])),
                          _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      join_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works. We have to use a new iterator variable because
  // the context must outlive the iterator.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> scramble_iter,
      join_op->CreateIterator({&params_data},
                              /*num_extra_slots=*/1, &scramble_context));
  EXPECT_EQ(scramble_iter->DebugString(),
            "ReorderingTupleIterator(JoinTupleIterator(FULL OUTER, "
            "left=TestTupleIterator, right=TestTupleIterator))");
  EXPECT_FALSE(scramble_iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(scramble_iter.get()));
  ASSERT_EQ(data.size(), 4);
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 5);
  EXPECT_EQ(data[1].num_slots(), 5);
  EXPECT_EQ(data[2].num_slots(), 5);
  EXPECT_EQ(data[3].num_slots(), 5);

  // Check that if the memory bound is too low, we return an error when loading
  // the right-hand side into memory.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/100));
  EXPECT_THAT(join_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, SortOpTotalOrder) {
  VariableId a("a"), b("b"), c("c"), param("param"), k("k"), v1("v1"), v2("v2"),
      v3("v3");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_c, DerefExpr::Create(c, proto_type_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v1, std::move(deref_b)));
  values.push_back(absl::make_unique<ExprArg>(v2, std::move(deref_c)));
  values.push_back(absl::make_unique<ExprArg>(v3, std::move(deref_param)));

  std::vector<std::vector<const SharedProtoState*>> shared_states;
  auto input = absl::WrapUnique(new TestRelationalOp(
      {a, b, c},
      CreateTestTupleDatas({{NullInt64(), Int64(10), GetProtoValue(1)},
                            {Int64(2), Int64(20), GetProtoValue(2)},
                            {Int64(3), Int64(30), GetProtoValue(3)}},
                           &shared_states),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/false));
  EXPECT_EQ(sort_op->IteratorDebugString(),
            "SortTupleIterator(TestTupleIterator)");
  EXPECT_EQ(sort_op->DebugString(),
            "SortOp(ordered\n"
            "+-keys: {\n"
            "| +-$k := $a DESC},\n"
            "+-values: {\n"
            "| +-$v1 := $b,\n"
            "| +-$v2 := $c,\n"
            "| +-$v3 := $param},\n"
            "+-input: TestRelationalOp)");

  std::unique_ptr<TupleSchema> output_schema = sort_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(k, v1, v2, v3));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({param});
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation({&params_schema}));

  TupleData params_data = CreateTestTupleData({Int64(100)});

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));

  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(Int64(30), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states[2][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states[1][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(NullInt64(), IsNull()),
                          IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states[0][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that enabling scrambling has no effect because the output is uniquely
  // ordered.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(Int64(30), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states[2][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states[1][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(NullInt64(), IsNull()),
                          IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states[0][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));

  // Check that if the memory bound is too low, we return an error.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/500));
  EXPECT_THAT(sort_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, SortOpIgnoresOrder) {
  VariableId a("a"), b("b"), k("k"), v("v");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(new TestRelationalOp(
      {a, b},
      CreateTestTupleDatas({{Int64(1), Int64(10)}, {Int64(2), Int64(20)}}),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/false,
                     /*is_stable_sort=*/false));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EvaluationContext context((EvaluationOptions()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  // The SortOp does not preserve order (like for a top-level ORDER BY in a
  // subquery), but scrambling is disabled, so the iterator will just be a
  // SortTupleIterator that returns sorted tuples with no scrambling.
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:2,v:20>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:1,v:10>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  // Now that scrambling is enabled, we still disable reordering on the
  // SortTupleIterator, but now we wrap it in a ReorderingTupleIterator to
  // scramble its output.
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(SortTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
}

TEST_F(CreateIteratorTest, SortOpIgnoresOrderSameKey) {
  VariableId a("a"), b("b"), k("k"), v("v");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b},
                           CreateTestTupleDatas({{Int64(1), Int64(10)},
                                                 {Int64(2), Int64(20)},
                                                 {Int64(2), Int64(30)}}),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/false,
                     /*is_stable_sort=*/false));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EvaluationContext context(
      (EvaluationOptions{.always_use_stable_sort = true}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  // The SortOp does not preserve order (like for a top-level ORDER BY in a
  // subquery), but scrambling is disabled, so the iterator will just be a
  // SortTupleIterator that returns sorted tuples with no scrambling.
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
  // Ties are broken in favor of the first tuple in the input.
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:2,v:20>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:2,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:1,v:10>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  // Now that scrambling is enabled, we still disable reordering on the
  // SortTupleIterator, but now we wrap it in a ReorderingTupleIterator to
  // scramble its output.
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(SortTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 3);
}

TEST_F(CreateIteratorTest, SortOpPartialOrder) {
  VariableId a("a"), b("b"), k("k"), v("v");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b},
                           CreateTestTupleDatas({{Int64(1), Int64(10)},
                                                 {Int64(2), Int64(20)},
                                                 {Int64(3), Int64(30)},
                                                 {Int64(3), Int64(31)}}),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/true));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EXPECT_EQ(
      "SortOp(ordered\n"
      "+-keys: {\n"
      "| +-$k := $a DESC},\n"
      "+-values: {\n"
      "| +-$v := $b},\n"
      "+-input: TestRelationalOp)",
      sort_op->DebugString());

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  // The output of SortTupleIterator is not defined because there are two rows
  // for the same key, but scrambling is disabled. SortTupleIterator will just
  // return them in sorted order with ties broken by whichever tuple is first in
  // the input.
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(context.IsDeterministicOutput());
  ASSERT_EQ(data.size(), 4);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:3,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:3,v:31>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:2,v:20>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[3]).DebugString(), "<k:1,v:10>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  EXPECT_EQ(data[2].num_slots(), 2);
  EXPECT_EQ(data[3].num_slots(), 2);

  EvaluationContext scramble_context((GetScramblingEvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());
  ASSERT_EQ(data.size(), 4);
  // Now that scrambling is enabled, SortTupleIterator should scramble the order
  // of tuples with the same key.
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:3,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:3,v:31>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:2,v:20>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[3]).DebugString(), "<k:1,v:10>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  EXPECT_EQ(data[2].num_slots(), 2);
  EXPECT_EQ(data[3].num_slots(), 2);
}

TEST_F(CreateIteratorTest, SortOpPartialOrderMultiNULLs) {
  VariableId a("a"), b("b"), k("k"), v("v");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b},
                           CreateTestTupleDatas({{NullInt64(), Int64(10)},
                                                 {NullInt64(), Int64(20)},
                                                 {Int64(3), Int64(30)}}),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/true));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  // The output of SortTupleIterator is not defined because there are two rows
  // for the same key, but scrambling is disabled. SortTupleIterator will just
  // return them in sorted order with ties broken by whichever tuple is first in
  // the input.
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(context.IsDeterministicOutput());
  ASSERT_EQ(data.size(), 3);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:3,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:NULL,v:10>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:NULL,v:20>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  EXPECT_EQ(data[2].num_slots(), 2);

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());
  ASSERT_EQ(data.size(), 3);
  // Now that scrambling is enabled, SortTupleIterator should scramble the order
  // of tuples with the same key.
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:3,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:NULL,v:10>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:NULL,v:20>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  EXPECT_EQ(data[2].num_slots(), 2);
}

TEST_F(CreateIteratorTest, SortOpPartialOrderStableSort) {
  VariableId a("a"), b("b"), k("k"), v("v");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b},
                           CreateTestTupleDatas({{Int64(1), Int64(10)},
                                                 {Int64(2), Int64(20)},
                                                 {Int64(3), Int64(30)},
                                                 {Int64(3), Int64(31)}}),

                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/true));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EXPECT_EQ(
      "SortOp(ordered\n"
      "+-keys: {\n"
      "| +-$k := $a DESC},\n"
      "+-values: {\n"
      "| +-$v := $b},\n"
      "+-input: TestRelationalOp)",
      sort_op->DebugString());

  EvaluationContext scramble_context((GetScramblingEvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                              &scramble_context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  // The output of SortTupleIterator is typically not defined because there are
  // two rows for the same key (and scrambling is enabled). But the SortOp was
  // configured to use a stable sort, so SortTupleIterator will just return them
  // in sorted order with ties broken by whichever tuple is first in the input.
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());
  ASSERT_EQ(data.size(), 4);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<k:3,v:30>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<k:3,v:31>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[2]).DebugString(), "<k:2,v:20>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[3]).DebugString(), "<k:1,v:10>");
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  EXPECT_EQ(data[2].num_slots(), 2);
  EXPECT_EQ(data[3].num_slots(), 2);
}

// Tests the reordering functionality in SortTupleIterator.
TEST_F(CreateIteratorTest, SortOpPartialInputReordersTest) {
  const int num_keys = 10;
  std::vector<TupleData> tuples;
  for (int k = 0; k < num_keys; ++k) {
    for (int v = 0; v <= k; ++v) {
      tuples.push_back(CreateTestTupleData({Int64(k), Int64(v)}));
    }
  }

  VariableId k("k"), v("v"), a("a"), b("b");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v, std::move(deref_b)));

  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b}, tuples, /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values),
                     /*limit=*/nullptr, /*offset=*/nullptr, std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/false));
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation(EmptyParamsSchemas()));

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                              &scramble_context));
  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));

  const absl::optional<int> opt_k_slot = iter->Schema().FindIndexForVariable(k);
  ASSERT_TRUE(opt_k_slot.has_value());
  const int k_slot = opt_k_slot.value();

  const absl::optional<int> opt_v_slot = iter->Schema().FindIndexForVariable(v);
  ASSERT_TRUE(opt_v_slot.has_value());
  const int v_slot = opt_v_slot.value();

  // data_by_key[k][v] is true if we have seen the tuple (k, v).
  std::vector<std::vector<bool>> data_by_key;
  data_by_key.resize(num_keys);
  for (int k = 0; k < data_by_key.size(); ++k) {
    data_by_key[k].resize(k + 1);
  }

  int last_key = num_keys;
  for (const TupleData& tuple : data) {
    const Value& k_value = tuple.slot(k_slot).value();
    ASSERT_TRUE(k_value.type()->IsInt64());
    const int k = k_value.ToInt64();

    const Value& v_value = tuple.slot(v_slot).value();
    ASSERT_TRUE(v_value.type()->IsInt64());
    const int64_t v = v_value.ToInt64();

    ASSERT_GE(v, 0);
    ASSERT_EQ(data_by_key[k].size(), k + 1);
    ASSERT_LE(v, k);
    ASSERT_FALSE(data_by_key[k][v]) << "k=" << k << " v=" << v;
    data_by_key[k][v] = true;

    // We should see everything for key k + 1 before anything for key k.
    if (k != last_key) {
      ASSERT_EQ(k, last_key - 1);
      last_key = k;
    }
  }

  // Verify that we saw every value once for every k.
  for (int k = 0; k < num_keys; ++k) {
    for (int v = 0; v <= k; ++v) {
      EXPECT_TRUE(data_by_key[k][v]) << "k=" << k << " v=" << v;
    }
  }
}

TEST_F(CreateIteratorTest, SortOpTotalOrderWithLimitAndOffset) {
  VariableId a("a"), b("b"), c("c"), param("param"), k("k"), v1("v1"), v2("v2"),
      v3("v3"), limit("limit"), offset("offset");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(
      absl::make_unique<KeyArg>(k, std::move(deref_a), KeyArg::kDescending));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_c, DerefExpr::Create(c, proto_type_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> values;
  values.push_back(absl::make_unique<ExprArg>(v1, std::move(deref_b)));
  values.push_back(absl::make_unique<ExprArg>(v2, std::move(deref_c)));
  values.push_back(absl::make_unique<ExprArg>(v3, std::move(deref_param)));

  std::vector<std::vector<const SharedProtoState*>> shared_states;
  auto input = absl::WrapUnique(new TestRelationalOp(
      {a, b, c},
      CreateTestTupleDatas({{NullInt64(), Int64(10), GetProtoValue(1)},
                            {Int64(2), Int64(20), GetProtoValue(2)},
                            {Int64(3), Int64(30), GetProtoValue(3)},
                            {Int64(4), Int64(40), GetProtoValue(4)}},
                           &shared_states),
      /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto limit_expr, DerefExpr::Create(limit, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto offset_expr,
                       DerefExpr::Create(offset, Int64Type()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sort_op,
      SortOp::Create(std::move(keys), std::move(values), std::move(limit_expr),
                     std::move(offset_expr), std::move(input),
                     /*is_order_preserving=*/true,
                     /*is_stable_sort=*/false));
  EXPECT_EQ(sort_op->IteratorDebugString(),
            "SortTupleIterator(TestTupleIterator)");
  EXPECT_EQ(sort_op->DebugString(),
            "SortOp(ordered\n"
            "+-keys: {\n"
            "| +-$k := $a DESC},\n"
            "+-values: {\n"
            "| +-$v1 := $b,\n"
            "| +-$v2 := $c,\n"
            "| +-$v3 := $param},\n"
            "+-limit: $limit,\n"
            "+-offset: $offset,\n"
            "+-input: TestRelationalOp)");

  std::unique_ptr<TupleSchema> output_schema = sort_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(k, v1, v2, v3));

  EvaluationContext context((EvaluationOptions()));

  TupleSchema params_schema({limit, offset, param});
  ZETASQL_ASSERT_OK(sort_op->SetSchemasForEvaluation({&params_schema}));
  TupleData params_data(/*num_slots=*/3);
  auto set_params = [&params_data](const Value& value1, const Value& value2) {
    params_data.mutable_slot(0)->SetValue(value1);
    params_data.mutable_slot(1)->SetValue(value2);
    params_data.mutable_slot(2)->SetValue(Int64(100));
  };

  // Bad argument: LIMIT -1 OFFSET 0
  set_params(Int64(-1), Int64(0));
  EXPECT_THAT(
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/0, &context),
      StatusIs(absl::StatusCode::kOutOfRange,
               "Limit requires non-negative count and offset"));

  // Bad argument: LIMIT 1 OFFSET -1
  set_params(Int64(1), Int64(-1));
  EXPECT_THAT(
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/0, &context),
      StatusIs(absl::StatusCode::kOutOfRange,
               "Limit requires non-negative count and offset"));

  // Bad argument: LIMIT NULL OFFSET 0
  set_params(NullInt64(), Int64(0));
  EXPECT_THAT(
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/0, &context),
      StatusIs(absl::StatusCode::kOutOfRange,
               "Limit requires non-null count and offset"));

  // Bad argument: LIMIT 1 OFFSET NULL
  set_params(Int64(1), NullInt64());
  EXPECT_THAT(
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/0, &context),
      StatusIs(absl::StatusCode::kOutOfRange,
               "Limit requires non-null count and offset"));

  set_params(Int64(2), Int64(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      sort_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));

  EXPECT_EQ(iter->DebugString(), "SortTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(Int64(30), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states[2][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states[1][2])),
                          IsTupleSlotWith(Int64(100), IsNull()), _));

  // Check that if the memory bound is too low, we return an error.
  EvaluationContext memory_context(GetIntermediateMemoryEvaluationOptions(
      /*total_bytes=*/500));
  EXPECT_THAT(sort_op->CreateIterator({&params_data},
                                      /*num_extra_slots=*/1, &memory_context),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       HasSubstr("Out of memory")));
}

TEST_F(CreateIteratorTest, ArrayScanOp) {
  VariableId a("a"), p("p"), param("param");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param,
                       DerefExpr::Create(param, proto_array_type_));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op,
      ArrayScanOp::Create(a, p, /*fields=*/{}, std::move(deref_param)));
  EXPECT_EQ(scan_op->IteratorDebugString(), "ArrayScanTupleIterator(<array>)");
  EXPECT_EQ(
      "ArrayScanOp(\n"
      "+-$a := element,\n"
      "+-$p := position,\n"
      "+-array: $param)",
      scan_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = scan_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(a, p));

  TupleSchema params_schema({param});
  TupleData params_data =
      CreateTestTupleData({Array({GetProtoValue(1), GetProtoValue(2)})});

  ZETASQL_ASSERT_OK(scan_op->SetSchemasForEvaluation({&params_schema}));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "ArrayScanTupleIterator([{int64_key_1: 1 int64_key_2: 10}, "
            "{int64_key_1: 2 int64_key_2: 20}])");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(
      data[0].slots(),
      ElementsAre(IsTupleSlotWith(GetProtoValue(1), Pointee(Eq(nullopt))),
                  IsTupleSlotWith(Int64(0), IsNull()), _));
  EXPECT_THAT(
      data[1].slots(),
      ElementsAre(IsTupleSlotWith(GetProtoValue(2), Pointee(Eq(nullopt))),
                  IsTupleSlotWith(Int64(1), IsNull()), _));
  // Ordered arrays always have deterministic output.
  EXPECT_TRUE(context.IsDeterministicOutput());

  // Do it again with cancellation.
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter,
      scan_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());
  absl::Status status;
  data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, scan_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(ArrayScanTupleIterator("
            "[{int64_key_1: 1 int64_key_2: 10}, "
            "{int64_key_1: 2 int64_key_2: 20}]))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, ArrayScanOpNonDeterministic) {
  VariableId a("a"), p("p");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto array_expr,
      ConstExpr::Create(Array({Int64(1), Int64(2)}, kIgnoresOrder)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto scan_op,
                       ArrayScanOp::Create(a, p, {}, std::move(array_expr)));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:1,p:0>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<a:2,p:1>");
  // Check for no extra slots.
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[1].num_slots(), 2);
  // Unordered arrays with position variables are non-deterministic if there is
  // more than one element.
  EXPECT_FALSE(context.IsDeterministicOutput());

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Don't look at 'data' in more detail because it is scrambled.
  EXPECT_FALSE(context.IsDeterministicOutput());
}

TEST_F(CreateIteratorTest, ScanArrayOfStructs) {
  VariableId x("x"), v1("v1"), v2("v2");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto array_expr,
      ConstExpr::Create(Array({Struct({"foo", "bar"}, {Int64(1), Int64(2)})})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto scan_op,
                       ArrayScanOp::Create(x, VariableId(), {{v1, 0}, {v2, 1}},
                                           std::move(array_expr)));
  EXPECT_EQ(
      "ArrayScanOp(\n"
      "+-$x := element,\n"
      "+-$v1 := field[0]:foo,\n"
      "+-$v2 := field[1]:bar,\n"
      "+-array: ConstExpr([{foo:1, bar:2}]))",
      scan_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = scan_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(v1, v2, x));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(),
            "<v1:1,v2:2,x:{foo:1, bar:2}>");
  // Check for no extra slots.
  EXPECT_EQ(data[0].num_slots(), 3);
}

TEST_F(CreateIteratorTest, TableScanAsArray) {
  VariableId v1("v1"), v2("v2");
  Value table = Array({
      Struct({"a", "b"}, {Int64(1), Int64(2)}),
      Struct({"a", "b"}, {Int64(3), Int64(4)})});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto table_as_array_expr,
      TableAsArrayExpr::Create("mytable", table.type()->AsArray()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto scan_op,
      ArrayScanOp::Create(VariableId(), VariableId(), {{v1, 0}, {v2, 1}},
                          std::move(table_as_array_expr)));
  EXPECT_EQ(
      "ArrayScanOp(\n"
      "+-$v1 := field[0]:a,\n"
      "+-$v2 := field[1]:b,\n"
      "+-array: TableAsArrayExpr(mytable))",
      scan_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = scan_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(v1, v2));

  EvaluationContext context_empty((EvaluationOptions()));
  EXPECT_THAT(scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                      &context_empty),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "Table not populated with array: mytable"));

  EvaluationContext context_bad((EvaluationOptions()));
  ZETASQL_ASSERT_OK(context_bad.AddTableAsArray("mytable", /*is_value_table=*/true,
                                        Int32Array({1, 2, 3}),
                                        LanguageOptions()));
  EXPECT_THAT(
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                              &context_bad),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("deviates from the type reported in the catalog")));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK(context.AddTableAsArray("mytable", /*is_value_table=*/false, table,
                                    LanguageOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_TRUE(iter->PreservesOrder());

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<v1:1,v2:2>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<v1:3,v2:4>");
  // Check for no extra slots.
  EXPECT_EQ(data[0].num_slots(), 2);
  EXPECT_EQ(data[0].num_slots(), 2);
  // The output is deterministic because the array position is not retrieved.
  EXPECT_TRUE(context.IsDeterministicOutput());

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK(scramble_context.AddTableAsArray(
      "mytable", /*is_value_table=*/false, table, LanguageOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, scan_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(ArrayScanTupleIterator("
            "[unordered: {a:1, b:2}, {a:3, b:4}]))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Don't look at 'data' in more detail because it is scrambled.
  EXPECT_TRUE(context.IsDeterministicOutput());
}

TEST_F(CreateIteratorTest, UnionAllOp) {
  VariableId a("a"), a1("a1"), a2("a2"), b("b"), b1("b1"), b2("b2");

  std::vector<std::vector<const SharedProtoState*>> shared_states;
  const std::vector<TupleData> datas =
      CreateTestTupleDatas({{Int64(1), GetProtoValue(1)},
                            {Int64(2), GetProtoValue(2)},
                            {Int64(3), GetProtoValue(3)},
                            {Int64(4), GetProtoValue(4)}},
                           &shared_states);
  auto input1 =
      absl::WrapUnique(new TestRelationalOp({a1, b1}, {datas[0], datas[1]},
                                            /*preserves_order=*/true));
  auto input2 =
      absl::WrapUnique(new TestRelationalOp({a2, b2}, {datas[2], datas[3]},
                                            /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a1, DerefExpr::Create(a1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b1, DerefExpr::Create(b1, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a2, DerefExpr::Create(a2, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b2, DerefExpr::Create(b2, Int64Type()));

  UnionAllOp::Input union_input1;
  union_input1.first = std::move(input1);
  union_input1.second.push_back(
      absl::make_unique<ExprArg>(a, std::move(deref_a1)));
  union_input1.second.push_back(
      absl::make_unique<ExprArg>(b, std::move(deref_b1)));

  UnionAllOp::Input union_input2;
  union_input2.first = std::move(input2);
  union_input2.second.push_back(
      absl::make_unique<ExprArg>(a, std::move(deref_a2)));
  union_input2.second.push_back(
      absl::make_unique<ExprArg>(b, std::move(deref_b2)));

  std::vector<UnionAllOp::Input> union_inputs;
  union_inputs.push_back(std::move(union_input1));
  union_inputs.push_back(std::move(union_input2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto union_all_op,
                       UnionAllOp::Create(std::move(union_inputs)));
  ZETASQL_ASSERT_OK(union_all_op->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "UnionAllOp(\n"
      "+-rel[0]: {\n"
      "| +-$a := $a1,\n"
      "| +-$b := $b1,\n"
      "| +-input: TestRelationalOp},\n"
      "+-rel[1]: {\n"
      "  +-$a := $a2,\n"
      "  +-$b := $b2,\n"
      "  +-input: TestRelationalOp})",
      union_all_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema =
      union_all_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(a, b));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      union_all_op->CreateIterator(EmptyParams(),
                                   /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(),
            "UnionAllTupleIterator(TestTupleIterator,TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 4);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(GetProtoValue(1),
                                          HasRawPointer(shared_states[0][1])),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(GetProtoValue(2),
                                          HasRawPointer(shared_states[1][1])),
                          _));
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(Int64(3), IsNull()),
                          IsTupleSlotWith(GetProtoValue(3),
                                          HasRawPointer(shared_states[2][1])),
                          _));
  EXPECT_THAT(data[3].slots(),
              ElementsAre(IsTupleSlotWith(Int64(4), IsNull()),
                          IsTupleSlotWith(GetProtoValue(4),
                                          HasRawPointer(shared_states[3][1])),
                          _));

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(iter, union_all_op->CreateIterator(EmptyParams(),
                                                          /*num_extra_slots=*/1,
                                                          &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(UnionAllTupleIterator("
            "TestTupleIterator,TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, LoopOp) {
  VariableId a("a"), x("x"), y("y"), z("z");

  std::vector<std::unique_ptr<ExprArg>> initial_assign(3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(initial_assign[0], AssignValueToVar(x, Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(initial_assign[1], AssignValueToVar(y, Int64(2)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(initial_assign[2], AssignValueToVar(z, Int64(3)));

  VariableId a_plus_x("a_plus_x"), a_plus_y("a_plus_y"), a_plus_z("a_plus_z");
  std::vector<std::unique_ptr<ExprArg>> body_compute_exprs(3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(body_compute_exprs[0],
                       ComputeSum(a, x, a_plus_x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(body_compute_exprs[1],
                       ComputeSum(a, y, a_plus_y, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(body_compute_exprs[2],
                       ComputeSum(a, z, a_plus_z, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto body_compute_op,
      ComputeOp::Create(
          std::move(body_compute_exprs),
          absl::WrapUnique(new TestRelationalOp(
              {a}, CreateTestTupleDatas({{Int64(10)}, {Int64(20)}}),
              /*preserves_order=*/true))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto body,
                       FilterLessThan(x, Int64(5), std::move(body_compute_op)));

  std::vector<std::unique_ptr<ExprArg>> loop_assign(3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(loop_assign[0], ComputeSum(x, Int64(1), x));
  ZETASQL_ASSERT_OK_AND_ASSIGN(loop_assign[1], ComputeSum(y, Int64(1), y));
  ZETASQL_ASSERT_OK_AND_ASSIGN(loop_assign[2], ComputeSum(z, Int64(1), z));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto loop_op, LoopOp::Create(std::move(initial_assign), std::move(body),
                                   std::move(loop_assign)));

  EXPECT_EQ(loop_op->IteratorDebugString(),
            "LoopTupleIterator: any_rows = false, inner iterator: "
            "FilterTupleIterator(ComputeTupleIterator(TestTupleIterator))");
  ZETASQL_LOG(ERROR) << loop_op->DebugString();
  EXPECT_EQ(loop_op->DebugString(), absl::StripAsciiWhitespace(R"(
LoopOp(
+-initial_assign: {
| +-$x := ConstExpr(1),
| +-$y := ConstExpr(2),
| +-$z := ConstExpr(3)},
+-body: FilterOp(
| +-condition: Less($x, ConstExpr(5)),
| +-input: ComputeOp(
|   +-map: {
|   | +-$a_plus_x := Add($a, $x),
|   | +-$a_plus_y := Add($a, $y),
|   | +-$a_plus_z := Add($a, $z)},
|   +-input: TestRelationalOp)),
+-loop_assign: {
  +-$x := Add($x, ConstExpr(1)),
  +-$y := Add($y, ConstExpr(1)),
  +-$z := Add($z, ConstExpr(1))})
  )"));

  std::unique_ptr<TupleSchema> output_schema = loop_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(a, a_plus_x, a_plus_y, a_plus_z));
  TupleSchema params_schema({});
  ZETASQL_ASSERT_OK(loop_op->SetSchemasForEvaluation({&params_schema}));

  EvaluationContext context((EvaluationOptions()));
  TupleData params_data = CreateTestTupleData({});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      loop_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "LoopTupleIterator: inner iterator: nullptr");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));

  // The loop should run for 4 iterations (x=1,2,3,4) with each iteration
  // producing two rows (a=10, 20), for a total of 8 rows.
  EXPECT_THAT(data, SizeIs(8));

  // Iteration 1, row 1
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(Int64(11), IsNull()),
                          IsTupleSlotWith(Int64(12), IsNull()),
                          IsTupleSlotWith(Int64(13), IsNull()), _));

  // Iteration 1, row 2
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(Int64(21), IsNull()),
                          IsTupleSlotWith(Int64(22), IsNull()),
                          IsTupleSlotWith(Int64(23), IsNull()), _));

  // Iteration 2, row 1
  EXPECT_THAT(data[2].slots(),
              ElementsAre(IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(Int64(12), IsNull()),
                          IsTupleSlotWith(Int64(13), IsNull()),
                          IsTupleSlotWith(Int64(14), IsNull()), _));

  // Iteratino 2, row 2
  EXPECT_THAT(data[3].slots(),
              ElementsAre(IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(Int64(22), IsNull()),
                          IsTupleSlotWith(Int64(23), IsNull()),
                          IsTupleSlotWith(Int64(24), IsNull()), _));

  // Iteration 3, row 1
  EXPECT_THAT(data[4].slots(),
              ElementsAre(IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(Int64(13), IsNull()),
                          IsTupleSlotWith(Int64(14), IsNull()),
                          IsTupleSlotWith(Int64(15), IsNull()), _));

  // Iteration 3, row 2
  EXPECT_THAT(data[5].slots(),
              ElementsAre(IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(Int64(23), IsNull()),
                          IsTupleSlotWith(Int64(24), IsNull()),
                          IsTupleSlotWith(Int64(25), IsNull()), _));

  // Iteration 4, row 1
  EXPECT_THAT(data[6].slots(),
              ElementsAre(IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(Int64(14), IsNull()),
                          IsTupleSlotWith(Int64(15), IsNull()),
                          IsTupleSlotWith(Int64(16), IsNull()), _));

  // Iteration 4, row 2
  EXPECT_THAT(data[7].slots(),
              ElementsAre(IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(Int64(24), IsNull()),
                          IsTupleSlotWith(Int64(25), IsNull()),
                          IsTupleSlotWith(Int64(26), IsNull()), _));
}

TEST_F(CreateIteratorTest, DistinctOp) {
  VariableId a("a"), b("b"), c("c");
  VariableId row_set1("row_set");

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DerefExpr> deref_a,
                       DerefExpr::Create(a, types::Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DerefExpr> deref_b,
                       DerefExpr::Create(b, types::Int64Type()));

  std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(1), Int64(10), Int64(100)},
                            {Int64(2), Int64(20), Int64(200)},
                            {Int64(3), Int64(30), Int64(300)},
                            {Int64(1), Int64(10), Int64(101)}});
  auto input = absl::WrapUnique(new TestRelationalOp({a, b, c}, test_values,
                                                     /*preserves_order=*/true));

  std::vector<std::unique_ptr<KeyArg>> keys;
  keys.push_back(absl::make_unique<KeyArg>(a, std::move(deref_a)));
  keys.push_back(absl::make_unique<KeyArg>(b, std::move(deref_b)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto distinct_op,
      DistinctOp::Create(std::move(input), std::move(keys), row_set1));

  EXPECT_EQ(absl::StripAsciiWhitespace(
                R"(
DistinctOp(
+-input: TestRelationalOp,
+-keys: {
| +-$a := $a,
| +-$b := $b},
+-row_set_id: $row_set := )
)"),
            distinct_op->DebugString());
  EXPECT_EQ("DistinctOp: TestTupleIterator",
            distinct_op->IteratorDebugString());
  EXPECT_EQ("<a,b>", distinct_op->CreateOutputSchema()->DebugString());

  // Set up a row set with some pre-existing data, which should be excluded
  // from the output.
  EvaluationContext context((EvaluationOptions()));
  auto row_set =
    absl::make_unique<CppValue<DistinctRowSet>>(context.memory_accountant());
  absl::Status status;
  ASSERT_TRUE(row_set->value().InsertRowIfNotPresent(
      absl::make_unique<TupleData>(CreateTestTupleData({Int64(3), Int64(30)})),
      &status));
  ASSERT_TRUE(row_set->value().InsertRowIfNotPresent(
      absl::make_unique<TupleData>(CreateTestTupleData({Int64(3), Int64(50)})),
      &status));
  ASSERT_TRUE(context.SetCppValueIfNotPresent(row_set1, std::move(row_set)));

  ZETASQL_ASSERT_OK(distinct_op->SetSchemasForEvaluation({}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      distinct_op->CreateIterator({},
                                  /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "DistinctOp: TestTupleIterator");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));

  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(10), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()), _));
}

TEST_F(CreateIteratorTest, ComputeOp) {
  VariableId a("a"), b("b"), param("param"), minus("minus"), plus("plus");
  std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(1), Int64(10)}, {Int64(2), Int64(20)}});
  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b}, test_values, /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> plus_args;
  plus_args.push_back(std::move(deref_a));
  plus_args.push_back(std::move(deref_b));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto plus_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(plus_args), DEFAULT_ERROR_MODE));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a_again, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_b_again, DerefExpr::Create(b, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> minus_args;
  minus_args.push_back(std::move(deref_a_again));
  minus_args.push_back(std::move(deref_b_again));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto minus_expr,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kSubtract, Int64Type()),
                           std::move(minus_args), DEFAULT_ERROR_MODE));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> args;
  args.push_back(absl::make_unique<ExprArg>(plus, std::move(plus_expr)));
  args.push_back(absl::make_unique<ExprArg>(minus, std::move(minus_expr)));
  args.push_back(absl::make_unique<ExprArg>(param, std::move(deref_param)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto compute_op,
                       ComputeOp::Create(std::move(args), std::move(input)));
  EXPECT_EQ(compute_op->IteratorDebugString(),
            "ComputeTupleIterator(TestTupleIterator)");
  EXPECT_EQ(
      "ComputeOp(\n"
      "+-map: {\n"
      "| +-$plus := Add($a, $b),\n"
      "| +-$minus := Subtract($a, $b),\n"
      "| +-$param := $param},\n"
      "+-input: TestRelationalOp)",
      compute_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = compute_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(),
              ElementsAre(a, b, plus, minus, param));

  TupleSchema params_schema({param});
  std::vector<const SharedProtoState*> params_shared_states;
  TupleData params_data =
      CreateTestTupleData({GetProtoValue(100)}, &params_shared_states);
  const SharedProtoState* params_shared_state = params_shared_states[0];

  ZETASQL_ASSERT_OK(compute_op->SetSchemasForEvaluation({&params_schema}));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      compute_op->CreateIterator({&params_data},
                                 /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "ComputeTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(10), IsNull()),
                          IsTupleSlotWith(Int64(11), IsNull()),
                          IsTupleSlotWith(Int64(-9), IsNull()),
                          IsTupleSlotWith(GetProtoValue(100),
                                          HasRawPointer(params_shared_state)),
                          _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()),
                          IsTupleSlotWith(Int64(22), IsNull()),
                          IsTupleSlotWith(Int64(-18), IsNull()),
                          IsTupleSlotWith(GetProtoValue(100),
                                          HasRawPointer(params_shared_state)),
                          _));

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, compute_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                       &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(ComputeTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, FilterOp) {
  VariableId a("a"), b("b"), param("param");
  const std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(1), Int64(10)},
                            {Int64(3), Int64(30)},
                            {Int64(2), Int64(20)},
                            {Int64(4), Int64(40)}});
  auto input = absl::WrapUnique(
      new TestRelationalOp({a, b}, test_values, /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_param, DerefExpr::Create(param, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(deref_a));
  args.push_back(std::move(deref_param));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto predicate,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kLess, BoolType()),
                           std::move(args), DEFAULT_ERROR_MODE));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto filter_op, FilterOp::Create(std::move(predicate), std::move(input)));
  EXPECT_EQ(filter_op->IteratorDebugString(),
            "FilterTupleIterator(TestTupleIterator)");
  EXPECT_EQ(
      "FilterOp(\n"
      "+-condition: Less($a, $param),\n"
      "+-input: TestRelationalOp)",
      filter_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = filter_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(a, b));

  TupleSchema params_schema({param});
  const TupleData params_data = CreateTestTupleData({Int64(3)});
  ZETASQL_ASSERT_OK(filter_op->SetSchemasForEvaluation({&params_schema}));

  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TupleIterator> iter,
                       filter_op->CreateIterator(
                           {&params_data}, /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "FilterTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()),
                          IsTupleSlotWith(Int64(10), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()),
                          IsTupleSlotWith(Int64(20), IsNull()), _));

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, filter_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                      &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(FilterTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, LimitOp_OrderedInput) {
  VariableId a("a"), b("b"), row_count("row_count"), offset("offset");
  const std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(1), Int64(10)},
                            {Int64(3), Int64(30)},
                            {Int64(2), Int64(20)},
                            {Int64(4), Int64(40)}});

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_row_count,
                       DerefExpr::Create(row_count, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_offset,
                       DerefExpr::Create(offset, Int64Type()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto limit_op,
      LimitOp::Create(std::move(deref_row_count), std::move(deref_offset),
                      absl::WrapUnique(new TestRelationalOp(
                          {a, b}, test_values, /*preserves_order=*/true)),
                      /*is_order_preserving=*/true));
  EXPECT_EQ(limit_op->IteratorDebugString(),
            "LimitTupleIterator(TestTupleIterator)");
  EXPECT_EQ(
      "LimitOp(ordered\n"
      "+-row_count: $row_count,\n"
      "+-offset: $offset,\n"
      "+-input: TestRelationalOp)",
      limit_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = limit_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(a, b));

  TupleSchema params_schema({row_count, offset});
  ZETASQL_ASSERT_OK(limit_op->SetSchemasForEvaluation({&params_schema}));
  TupleData params_data(/*num_slots=*/2);
  auto set_params = [&params_data](const Value& value1, const Value& value2) {
    params_data.mutable_slot(0)->SetValue(value1);
    params_data.mutable_slot(1)->SetValue(value2);
  };

  EvaluationContext scramble_context(GetScramblingEvaluationOptions());

  // Bad argument: LIMIT -1 OFFSET 0
  set_params(Int64(-1), Int64(0));
  EXPECT_THAT(limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/0,
                                       &scramble_context),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "Limit requires non-negative count and offset"));

  // Bad argument: LIMIT 1 OFFSET -1
  set_params(Int64(1), Int64(-1));
  EXPECT_THAT(limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/0,
                                       &scramble_context),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "Limit requires non-negative count and offset"));

  // Bad argument: LIMIT NULL OFFSET 0
  set_params(NullInt64(), Int64(0));
  EXPECT_THAT(limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/0,
                                       &scramble_context),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "Limit requires non-null count and offset"));

  // Bad argument: LIMIT 1 OFFSET NULL
  set_params(Int64(1), NullInt64());
  EXPECT_THAT(limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/0,
                                       &scramble_context),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "Limit requires non-null count and offset"));

  // LIMIT 2 OFFSET 0 (returns 2 rows)
  set_params(Int64(2), Int64(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                               &scramble_context));
  EXPECT_EQ(iter->DebugString(), "LimitTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:1,b:10>");
  EXPECT_EQ(Tuple(&iter->Schema(), &data[1]).DebugString(), "<a:3,b:30>");
  // Check for the extra slot.
  EXPECT_EQ(data[0].num_slots(), 3);
  EXPECT_EQ(data[1].num_slots(), 3);
  // Check for deterministic output.
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());
  // LIMIT 1 OFFSET 1 (returns one row)
  set_params(Int64(1), Int64(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     &scramble_context));
  EXPECT_EQ(iter->DebugString(), "LimitTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:3,b:30>");
  EXPECT_EQ(data[0].num_slots(), 3);
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());

  // LIMIT 2 OFFSET 3 (only returns one row)
  set_params(Int64(2), Int64(3));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     &scramble_context));
  EXPECT_EQ(iter->DebugString(), "LimitTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:4,b:40>");
  EXPECT_EQ(data[0].num_slots(), 3);  // Check for the extra slot.
  // Check for deterministic output.
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());

  // LIMIT 2 OFFSET 4 (returns no rows)
  set_params(Int64(2), Int64(4));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     &scramble_context));
  EXPECT_EQ(iter->DebugString(), "LimitTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_TRUE(data.empty());
  // Check for deterministic output.
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());

  // LIMIT 0 OFFSET 2
  set_params(Int64(0), Int64(4));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     &scramble_context));
  EXPECT_EQ(iter->DebugString(), "LimitTupleIterator(TestTupleIterator)");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_TRUE(data.empty());
  // Check for deterministic output.
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());

  // Limit that doesn't preserve order.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_row_count2,
                       DerefExpr::Create(row_count, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_offset2,
                       DerefExpr::Create(offset, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      limit_op,
      LimitOp::Create(std::move(deref_row_count2), std::move(deref_offset2),
                      absl::WrapUnique(new TestRelationalOp(
                          {a, b}, test_values, /*preserves_order=*/true)),
                      /*is_order_preserving=*/false));
  ZETASQL_ASSERT_OK(limit_op->SetSchemasForEvaluation({&params_schema}));

  // LIMIT 2 OFFSET 0 with a limit that does not preserve order.
  set_params(Int64(2), Int64(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     &scramble_context));
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Don't look at 'data' in more detail because it is scrambled.
  EXPECT_TRUE(scramble_context.IsDeterministicOutput());
}

TEST_F(CreateIteratorTest, LimitOp_UnorderedInput) {
  VariableId a("a"), b("b"), row_count("row_count"), offset("offset");
  const std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(3), Int64(30)}, {Int64(1), Int64(10)}});
  // 'test_values' will come out in reverse order because 'preserves_order' is
  // false.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_row_count,
                       DerefExpr::Create(row_count, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_offset,
                       DerefExpr::Create(offset, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto limit_op,
      LimitOp::Create(std::move(deref_row_count), std::move(deref_offset),
                      absl::WrapUnique(new TestRelationalOp(
                          {a, b}, test_values, /*preserves_order=*/false)),
                      /*is_order_preserving=*/true));

  TupleSchema params_schema({row_count, offset});
  ZETASQL_ASSERT_OK(limit_op->SetSchemasForEvaluation({&params_schema}));
  TupleData params_data(/*num_slots=*/2);
  std::unique_ptr<EvaluationContext> context;
  auto set_params_and_context = [&params_data, &context](const Value& value1,
                                                         const Value& value2) {
    params_data.mutable_slot(0)->SetValue(value1);
    params_data.mutable_slot(1)->SetValue(value2);
    context =
        absl::make_unique<EvaluationContext>(GetScramblingEvaluationOptions());
  };

  // LIMIT 1 OFFSET 0
  set_params_and_context(Int64(1), Int64(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                               context.get()));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(LimitTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:1,b:10>");
  EXPECT_EQ(data[0].num_slots(), 3);  // Check for the extra slot.
  EXPECT_FALSE(context->IsDeterministicOutput());

  // LIMIT 10 OFFSET 0
  // Limit that passes through the entire input is deterministic but the
  // result order is unspecified if the order of the input is unspecified.
  set_params_and_context(Int64(10), Int64(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     context.get()));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(LimitTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  // Don't look at 'data' in more detail because it is scrambled.
  EXPECT_TRUE(context->IsDeterministicOutput());

  // LIMIT 1 OFFSET 5
  // Limit producing empty output is deterministic.
  set_params_and_context(Int64(1), Int64(5));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     context.get()));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(LimitTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  EXPECT_TRUE(data.empty());
  EXPECT_TRUE(context->IsDeterministicOutput());

  // Limit over one row is deterministic.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_row_count2,
                       DerefExpr::Create(row_count, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_offset2,
                       DerefExpr::Create(offset, Int64Type()));
  const std::vector<TupleData> one_row =
      CreateTestTupleDatas({{Int64(1), Int64(10)}});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      limit_op,
      LimitOp::Create(std::move(deref_row_count2), std::move(deref_offset2),
                      absl::WrapUnique(new TestRelationalOp(
                          {a, b}, one_row, /*preserves_order=*/false)),
                      /*is_order_preserving=*/true));
  ZETASQL_ASSERT_OK(limit_op->SetSchemasForEvaluation({&params_schema}));
  // LIMIT 1 OFFSET 0
  set_params_and_context(Int64(1), Int64(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, limit_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                     context.get()));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(LimitTupleIterator(TestTupleIterator))");
  EXPECT_FALSE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(data, ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 1);
  EXPECT_EQ(Tuple(&iter->Schema(), &data[0]).DebugString(), "<a:1,b:10>");
  EXPECT_TRUE(context->IsDeterministicOutput());
}

TEST_F(CreateIteratorTest, EnumerateOp) {
  VariableId count("count");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_count, DerefExpr::Create(count, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto enum_op,
                       EnumerateOp::Create(std::move(deref_count)));
  EXPECT_EQ(enum_op->IteratorDebugString(), "EnumerateTupleIterator(<count>)");
  EXPECT_EQ(enum_op->DebugString(), "EnumerateOp($count)");
  std::unique_ptr<TupleSchema> output_schema = enum_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), IsEmpty());

  TupleSchema params_schema({count});
  ZETASQL_ASSERT_OK(enum_op->SetSchemasForEvaluation({&params_schema}));
  TupleData params_data(/*num_slots=*/1);
  params_data.mutable_slot(0)->SetValue(NullInt64());

  EvaluationContext context((EvaluationOptions()));
  EXPECT_THAT(
      enum_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context),
      StatusIs(absl::StatusCode::kOutOfRange,
               "Enumerate requires non-null count"));

  for (int i = -1; i <= 3; ++i) {
    ZETASQL_LOG(INFO) << "Testing EnumerateOp with count = " << i;
    params_data.mutable_slot(0)->SetValue(Int64(i));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TupleIterator> iter,
                         enum_op->CreateIterator(
                             {&params_data}, /*num_extra_slots=*/1, &context));
    EXPECT_EQ(iter->DebugString(),
              absl::StrCat("EnumerateTupleIterator(", i, ")"));
    EXPECT_TRUE(iter->PreservesOrder());
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                         ReadFromTupleIterator(iter.get()));
    ASSERT_EQ(data.size(), std::max(i, 0));
    for (const TupleData& d : data) {
      // The tuples are empty but they each have the extra slot.
      ASSERT_EQ(d.num_slots(), 1);
      EXPECT_THAT(*d.slot(0).mutable_shared_proto_state(), IsNull());
    }
  }

  // Do a test with cancellation.
  params_data.mutable_slot(0)->SetValue(Int64(2));
  context.ClearDeadlineAndCancellationState();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      enum_op->CreateIterator({&params_data}, /*num_extra_slots=*/1, &context));
  ZETASQL_ASSERT_OK(context.CancelStatement());

  absl::Status status;
  std::vector<TupleData> data = ReadFromTupleIteratorFull(iter.get(), &status);
  EXPECT_TRUE(data.empty());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kCancelled, _));

  // Check that scrambling works.
  EvaluationContext scramble_context(GetScramblingEvaluationOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, enum_op->CreateIterator({&params_data}, /*num_extra_slots=*/1,
                                    &scramble_context));
  EXPECT_EQ(iter->DebugString(),
            "ReorderingTupleIterator(EnumerateTupleIterator(2))");
  EXPECT_FALSE(iter->PreservesOrder());
}

TEST_F(CreateIteratorTest, RootOp) {
  VariableId a("a");
  std::vector<TupleData> test_values =
      CreateTestTupleDatas({{Int64(1)}, {Int64(2)}});
  auto input = absl::WrapUnique(
      new TestRelationalOp({a}, test_values, /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto root_op,
      RootOp::Create(std::move(input), absl::make_unique<RootData>()));
  EXPECT_EQ(root_op->IteratorDebugString(), "TestTupleIterator");
  EXPECT_EQ(
      "RootOp(\n"
      "+-input: TestRelationalOp)",
      root_op->DebugString());
  std::unique_ptr<TupleSchema> output_schema = root_op->CreateOutputSchema();
  EXPECT_THAT(output_schema->variables(), ElementsAre(a));

  ZETASQL_ASSERT_OK(root_op->SetSchemasForEvaluation(/*params_schemas=*/{}));
  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      root_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/1, &context));
  EXPECT_EQ(iter->DebugString(), "TestTupleIterator");
  EXPECT_TRUE(iter->PreservesOrder());
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<TupleData> data,
                       ReadFromTupleIterator(iter.get()));
  ASSERT_EQ(data.size(), 2);
  EXPECT_THAT(data[0].slots(),
              ElementsAre(IsTupleSlotWith(Int64(1), IsNull()), _));
  EXPECT_THAT(data[1].slots(),
              ElementsAre(IsTupleSlotWith(Int64(2), IsNull()), _));
}

// Builds a join between two relations with 'tuple_count' tuples each with
// matching values and a relation1_tuple < relation2_tuple join condition.
absl::StatusOr<std::unique_ptr<JoinOp>> BuildTimeoutTestJoin(int tuple_count) {
  VariableId x("x"), y("y"), yp("y'");

  std::vector<TupleData> input1_tuples;
  std::vector<TupleData> input2_tuples;
  for (int i = 0; i < tuple_count; ++i) {
    const Value value = Int64(i);
    input1_tuples.push_back(CreateTestTupleData({value}));
    input2_tuples.push_back(CreateTestTupleData({value}));
  }

  auto input1 = absl::make_unique<TestRelationalOp>(
      std::vector<VariableId>{x}, input1_tuples, /*preserves_order=*/true);
  auto input2 = absl::make_unique<TestRelationalOp>(
      std::vector<VariableId>{y}, input2_tuples, /*preserves_order=*/true);

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSIGN_OR_RETURN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> join_args;
  join_args.push_back(std::move(deref_x));
  join_args.push_back(std::move(deref_y));

  ZETASQL_ASSIGN_OR_RETURN(auto join_expr,
                   ScalarFunctionCallExpr::Create(
                       CreateFunction(FunctionKind::kLess, BoolType()),
                       std::move(join_args)));

  std::vector<std::unique_ptr<ExprArg>> left_outputs;  // No extra left outputs.

  ZETASQL_ASSIGN_OR_RETURN(auto deref_y_again, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> right_outputs;
  right_outputs.push_back(
      absl::make_unique<ExprArg>(yp, std::move(deref_y_again)));

  ZETASQL_ASSIGN_OR_RETURN(
      auto join_op,
      JoinOp::Create(JoinOp::kLeftOuterJoin, EmptyHashJoinEqualityExprs(),
                     std::move(join_expr), std::move(input1), std::move(input2),
                     std::move(left_outputs), std::move(right_outputs)));
  ZETASQL_RETURN_IF_ERROR(join_op->SetSchemasForEvaluation(EmptyParamsSchemas()));
  return join_op;
}

const int kMediumJoinSize = 100;
const int kLargeJoinSize = 1000;
const absl::Duration kShortTimeout = absl::Milliseconds(30);
const absl::Duration kLongTimeout = absl::Seconds(10);

// Tests that the default no timeout specified behavior successfully evaluates a
// medium size join.
TEST(TimeoutTest, NoTimeout) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<JoinOp> join_op,
                       BuildTimeoutTestJoin(kMediumJoinSize));
  EvaluationContext context((EvaluationOptions()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  ZETASQL_ASSERT_OK(ReadFromTupleIterator(iter.get()).status());
}

// Tests that a join evaluated with a short timeout will return the proper
// timeout error while executing a long join.
TEST(TimeoutTest, ShortTimeout) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<JoinOp> join_op,
                       BuildTimeoutTestJoin(kLargeJoinSize));
  EvaluationContext context((EvaluationOptions()));
  context.SetStatementEvaluationDeadlineFromNow(kShortTimeout);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  EXPECT_THAT(
      ReadFromTupleIterator(iter.get()),
      StatusIs(absl::StatusCode::kResourceExhausted,
               ContainsRegex("The statement has been aborted because the "
                             "statement deadline .+ was exceeded.")));
}

// Tests that setting a short timeout then switching to the default deadline
// produces the expected no timeout behavior when evaluating a medium sized
// join.
TEST(TimeoutTest, ShortTimeoutToNoDeadline) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<JoinOp> join_op,
                       BuildTimeoutTestJoin(kMediumJoinSize));
  EvaluationContext context((EvaluationOptions()));
  context.SetStatementEvaluationDeadlineFromNow(kShortTimeout);
  context.SetStatementEvaluationDeadline(absl::InfiniteFuture());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  ZETASQL_EXPECT_OK(ReadFromTupleIterator(iter.get()).status());
}

// Tests that a join evaluated with a longer timeout will succeed with a medium
// size join.
TEST(TimeoutTest, LongTimeout) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<JoinOp> join_op,
                       BuildTimeoutTestJoin(kMediumJoinSize));
  EvaluationContext context((EvaluationOptions()));
  context.SetStatementEvaluationDeadlineFromNow(kLongTimeout);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<TupleIterator> iter,
      join_op->CreateIterator(EmptyParams(), /*num_extra_slots=*/0, &context));
  ZETASQL_EXPECT_OK(ReadFromTupleIterator(iter.get()).status());
}

}  // namespace
}  // namespace zetasql
