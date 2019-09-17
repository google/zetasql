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

// Tests for ValueExprs not covered by other tests.

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/wire_format_lite.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
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
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

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
using testing::TestWithParam;
using testing::UnorderedElementsAreArray;
using testing::ValuesIn;

namespace zetasql {

using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

static const auto DEFAULT_ERROR_MODE =
    ResolvedFunctionCallBase::DEFAULT_ERROR_MODE;

namespace {

using SharedProtoState = TupleSlot::SharedProtoState;

// For readability.
std::vector<const TupleSchema*> EmptyParamsSchemas() { return {}; }
std::vector<const TupleData*> EmptyParams() { return {}; }

// For convenience.
::zetasql_base::StatusOr<Value> EvalExpr(const ValueExpr& expr,
                                 absl::Span<const TupleData* const> params,
                                 EvaluationContext* context = nullptr) {
  EvaluationContext empty_context((EvaluationOptions()));
  if (context == nullptr) {
    context = &empty_context;
  }
  TupleSlot slot;
  ::zetasql_base::Status status;
  if (!expr.EvalSimple(params, context, &slot, &status)) {
    return status;
  }
  if (!TupleSlot::ShouldStoreSharedProtoStateFor(slot.value().type_kind())) {
    EXPECT_THAT(*slot.mutable_shared_proto_state(), IsNull());
  }
  return slot.value();
}

std::unique_ptr<ScalarFunctionBody> CreateFunction(FunctionKind kind,
                                                   const Type* output_type) {
  LanguageOptions language_options;
  language_options.EnableMaximumLanguageFeaturesForDevelopment();
  return BuiltinScalarFunction::CreateValidated(kind, language_options,
                                                output_type, {})
      .ValueOrDie();
}

// -------------------------------------------------------
// Scalar functions
// -------------------------------------------------------

struct NaryFunctionTemplate {
 public:
  FunctionKind kind;
  QueryParamsWithResult params;

  NaryFunctionTemplate(FunctionKind kind,
                       const std::vector<ValueConstructor>& arguments,
                       const ValueConstructor& result,
                       const std::string& error_message)
      : kind(kind), params(arguments, result, error_message) {}
  NaryFunctionTemplate(FunctionKind kind,
                       const std::vector<ValueConstructor>& arguments,
                       const ValueConstructor& result)
      : kind(kind), params(arguments, result) {}
  NaryFunctionTemplate(
      FunctionKind kind, const QueryParamsWithResult& params)
      : kind(kind), params(params) {
  }
};

std::ostream& operator<<(std::ostream& out, const NaryFunctionTemplate& t) {
  std::vector<std::unique_ptr<ValueExpr>> arguments;
  for (int i = 0; i < t.params.num_params(); ++i) {
    arguments.push_back(ConstExpr::Create(t.params.param(i)).ValueOrDie());
  }
  auto fct_op = ScalarFunctionCallExpr::Create(
                    CreateFunction(t.kind, t.params.GetResultType()),
                    std::move(arguments))
                    .ValueOrDie();
  out << fct_op->DebugString() << " == ";
  if (t.params.HasEmptyFeatureSetAndNothingElse()) {
    out << t.params.result().DebugString(/*verbose=*/true);
  } else {
    out << "(";
    const QueryParamsWithResult::ResultMap& result_map = t.params.results();
    for (auto iter = result_map.begin(); iter != result_map.end(); ++iter) {
      if (iter != result_map.begin()) {
        out << ", ";
      }
      out << iter->first << ":"
          << iter->second.result.DebugString(/*verbose=*/true);
    }
    out << ")";
  }
  return out;
}

std::vector<NaryFunctionTemplate> GetFunctionTemplates(
    FunctionKind kind, const std::vector<QueryParamsWithResult>& tests) {
  std::vector<NaryFunctionTemplate> templates;
  for (const auto& t : tests) {
    templates.emplace_back(kind, t);
  }
  return templates;
}

// Returns only those function templates that have non-null arguments or no
// arguments. This method is used to filter out null templates for Cast()
// because the cast implementation would cast any null input to any output type
// (it relies on the function signatures to prevent these casts in SQL queries).
std::vector<QueryParamsWithResult> NonNullArguments(
    const std::vector<QueryParamsWithResult>& tests) {
  std::vector<QueryParamsWithResult> result;
  for (const auto& t : tests) {
    bool nulls_only = true;
    for (const auto& value : t.params()) {
      if (!value.is_null()) {
        nulls_only = false;
        break;
      }
    }
    if (!t.params().empty() && nulls_only) continue;
    result.push_back(t);
  }
  return result;
}

typedef TestWithParam<NaryFunctionTemplate> NaryFunctionTemplateTest;

TEST_P(NaryFunctionTemplateTest, NaryFunctionTest) {
  const NaryFunctionTemplate& t = GetParam();
  for (const auto& each : t.params.results()) {
    LanguageOptions language_options;
    for (const auto& feature : each.first) {
      language_options.EnableLanguageFeature(feature);
    }
    EvaluationContext context((EvaluationOptions()));
    context.SetLanguageOptions(language_options);

    const Type* first_argument_type;
    bool mismatched_types_other_than_int64_or_uint64 = false;
    std::vector<std::unique_ptr<ValueExpr>> arguments;
    for (int i = 0; i < t.params.num_params(); ++i) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(auto arg, ConstExpr::Create(t.params.param(i)));
      arguments.push_back(std::move(arg));

      const Type* arg_type = arguments.back()->output_type();
      if (i == 0) {
        first_argument_type = arg_type;
      } else if (!first_argument_type->Equals(arg_type)) {
        if ((first_argument_type->kind() != TYPE_INT64 &&
             first_argument_type->kind() != TYPE_UINT64) ||
            (arg_type->kind() != TYPE_INT64 &&
             arg_type->kind() != TYPE_UINT64)) {
          mismatched_types_other_than_int64_or_uint64 = true;
        }
      }
    }
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto function_body,
        BuiltinScalarFunction::CreateValidated(
            t.kind, language_options, t.params.GetResultType(), arguments));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto fct, ScalarFunctionCallExpr::Create(std::move(function_body),
                                                 std::move(arguments)));
    const zetasql_base::StatusOr<Value> function_value =
        EvalExpr(*fct, EmptyParams(), &context);

    if (each.second.status.ok()) {
      if (mismatched_types_other_than_int64_or_uint64) {
        // Some of the compliance tests cover coercion cases for the comparison
        // operators. Since no scalar functions exist for direct comparisons of
        // e.g. NUMERIC with INT64, the reference implementation will return an
        // undefined result.
        continue;
      }
      EXPECT_THAT(function_value, IsOkAndHolds(each.second.result));
    } else {
      EXPECT_FALSE(function_value.status().ok());
      if (each.second.status.code() != zetasql_base::StatusCode::kUnknown) {
        EXPECT_THAT(function_value,
                    StatusIs(each.second.status.code(),
                             HasSubstr(each.second.status.error_message())));
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    UnaryMinus, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kUnaryMinus,
                                  GetFunctionTestsUnaryMinus())));

INSTANTIATE_TEST_SUITE_P(Add, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(FunctionKind::kAdd,
                                                       GetFunctionTestsAdd())));

INSTANTIATE_TEST_SUITE_P(
    Subtract, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kSubtract,
                                  GetFunctionTestsSubtract())));

// TODO: replace individual CAST tests by a wholesale test once datetime
// and bool support is in place.
INSTANTIATE_TEST_SUITE_P(CastBool, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(
                             FunctionKind::kCast,
                             NonNullArguments(GetFunctionTestsCastBool()))));

INSTANTIATE_TEST_SUITE_P(
    CastNumeric, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kCast,
                                  GetFunctionTestsCastNumeric())));

INSTANTIATE_TEST_SUITE_P(
    CastString, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kCast,
                                  GetFunctionTestsCastString())));

INSTANTIATE_TEST_SUITE_P(And, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(FunctionKind::kAnd,
                                                       GetFunctionTestsAnd())));

INSTANTIATE_TEST_SUITE_P(Or, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(FunctionKind::kOr,
                                                       GetFunctionTestsOr())));

INSTANTIATE_TEST_SUITE_P(Not, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(FunctionKind::kNot,
                                                       GetFunctionTestsNot())));

INSTANTIATE_TEST_SUITE_P(
    Equal, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kEqual,
                                  GetFunctionTestsEqual(
                                      /*include_nano_timestamp=*/false))));

INSTANTIATE_TEST_SUITE_P(
    Less, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kLess,
                                  GetFunctionTestsLess(
                                      /*include_nano_timestamp=*/false))));

INSTANTIATE_TEST_SUITE_P(
    LessOrEqual, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(
        FunctionKind::kLessOrEqual,
        GetFunctionTestsLessOrEqual(/*include_nano_timestamp=*/false))));

INSTANTIATE_TEST_SUITE_P(IsNull, NaryFunctionTemplateTest,
                         ValuesIn(GetFunctionTemplates(
                             FunctionKind::kIsNull, GetFunctionTestsIsNull())));

INSTANTIATE_TEST_SUITE_P(
    ArrayAtOffset, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kArrayAtOffset,
                                  GetFunctionTestsAtOffset())));

INSTANTIATE_TEST_SUITE_P(
    SafeArrayAtOffset, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(FunctionKind::kSafeArrayAtOffset,
                                  GetFunctionTestsSafeAtOffset())));

INSTANTIATE_TEST_SUITE_P(
    Least, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(
        FunctionKind::kLeast,
        GetFunctionTestsLeast(/*include_nano_timestamp=*/false))));

INSTANTIATE_TEST_SUITE_P(
    Greatest, NaryFunctionTemplateTest,
    ValuesIn(GetFunctionTemplates(
        FunctionKind::kGreatest,
        GetFunctionTestsGreatest(/*include_nano_timestamp=*/false))));

class EvalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    TypeFactory* type_factory = test_values::static_type_factory();
    ZETASQL_ASSERT_OK(type_factory->MakeProtoType(
        zetasql_test::KitchenSinkPB::descriptor(), &proto_type_));
  }

  Value GetProtoValue(int i) const {
    zetasql_test::KitchenSinkPB proto;
    proto.set_int64_key_1(i);
    proto.set_int64_key_2(10 * i);

    std::string cord;
    CHECK(proto.SerializeToString(&cord));
    return Value::Proto(proto_type_, cord);
  }

  const ProtoType* proto_type_ = nullptr;
};

TEST_F(EvalTest, NewArrayExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one_expr, ConstExpr::Create(Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto null_expr, ConstExpr::Create(NullInt64()));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(one_expr));
  args.push_back(std::move(null_expr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto array_op,
                       NewArrayExpr::Create(Int64ArrayType(), std::move(args)));
  EXPECT_EQ(
      "NewArrayExpr(\n"
      "  type: ARRAY<INT64>,\n"
      "  ConstExpr(Int64(1)),\n"
      "  ConstExpr(Int64(NULL)))",
      array_op->DebugString(true));
  EXPECT_EQ("NewArrayExpr(ConstExpr(1), ConstExpr(NULL))",
            array_op->DebugString());
  Value expected_result = Array({Int64(1), NullInt64()});

  EvaluationContext context((EvaluationOptions()));
  TupleSlot slot;
  ::zetasql_base::Status status;
  ASSERT_TRUE(array_op->EvalSimple(EmptyParams(), &context, &slot, &status))
      << status;
  EXPECT_EQ(expected_result, slot.value());
  EXPECT_EQ(slot.mutable_shared_proto_state()->get(), nullptr);

  // Test that we can't create an array that is too large.
  EvaluationOptions value_size_options;
  value_size_options.max_value_byte_size = 1;
  EvaluationContext value_size_context(value_size_options);
  EXPECT_FALSE(
      array_op->EvalSimple(EmptyParams(), &value_size_context, &slot, &status));
  EXPECT_THAT(status,
              StatusIs(zetasql_base::StatusCode::kOutOfRange,
                       HasSubstr("Cannot construct array Value larger than")));
}

static std::unique_ptr<ValueExpr> DivByZeroErrorExpr() {
  std::vector<std::unique_ptr<ValueExpr>> div_args;
  div_args.push_back(ConstExpr::Create(Int64(1)).ValueOrDie());
  div_args.push_back(ConstExpr::Create(Int64(0)).ValueOrDie());

  return ScalarFunctionCallExpr::Create(
             CreateFunction(FunctionKind::kDiv, Int64Type()),
             std::move(div_args), DEFAULT_ERROR_MODE)
      .ValueOrDie();
}

TEST_F(EvalTest, IfExpr) {
  // Use division by zero to force an error if the wrong branch gets evaluated.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto true_expr, ConstExpr::Create(Bool(true)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one_expr, ConstExpr::Create(Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto if_op_true,
                       IfExpr::Create(std::move(true_expr), std::move(one_expr),
                                      DivByZeroErrorExpr()));
  EXPECT_EQ(
      "IfExpr(\n"
      "+-condition: ConstExpr(true),\n"
      "+-true_value: ConstExpr(1),\n"
      "+-false_value: Div(ConstExpr(1), ConstExpr(0)))",
      if_op_true->DebugString());
  EXPECT_THAT(EvalExpr(*if_op_true, EmptyParams()), IsOkAndHolds(Int64(1)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto false_expr, ConstExpr::Create(Bool(false)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one_expr2, ConstExpr::Create(Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto if_op_false, IfExpr::Create(std::move(false_expr),
                                                        DivByZeroErrorExpr(),
                                                        std::move(one_expr2)));
  EXPECT_THAT(EvalExpr(*if_op_false, EmptyParams()), IsOkAndHolds(Int64(1)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto null_expr, ConstExpr::Create(NullBool()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one_expr3, ConstExpr::Create(Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto if_op_null, IfExpr::Create(std::move(null_expr),
                                                       DivByZeroErrorExpr(),
                                                       std::move(one_expr3)));
  EXPECT_THAT(EvalExpr(*if_op_null, EmptyParams()), IsOkAndHolds(Int64(1)));
}

TEST_F(EvalTest, LetExpr) {
  VariableId a("a"), x("x"), y("y");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x, DerefExpr::Create(x, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_y, DerefExpr::Create(y, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(deref_x));
  args.push_back(std::move(deref_y));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto body,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(args), DEFAULT_ERROR_MODE));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one, ConstExpr::Create(Int64(1)));

  std::vector<std::unique_ptr<ValueExpr>> a_plus_one_args;
  a_plus_one_args.push_back(std::move(deref_a));
  a_plus_one_args.push_back(std::move(one));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto a_plus_one,
                       ScalarFunctionCallExpr::Create(
                           CreateFunction(FunctionKind::kAdd, Int64Type()),
                           std::move(a_plus_one_args), DEFAULT_ERROR_MODE));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_x_again, DerefExpr::Create(x, Int64Type()));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(a_plus_one)));
  let_assign.push_back(absl::make_unique<ExprArg>(y, std::move(deref_x_again)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto let,
                       LetExpr::Create(std::move(let_assign), std::move(body)));
  EXPECT_EQ(
      "LetExpr(\n"
      "+-assign: {\n"
      "| +-$x := Add($a, ConstExpr(1)),\n"
      "| +-$y := $x},\n"
      "+-body: Add($x, $y))",
      let->DebugString());

  const TupleSchema params_schema({a});
  const TupleData params_data = CreateTestTupleData({Int64(5)});
  ZETASQL_ASSERT_OK(let->SetSchemasForEvaluation({&params_schema}));

  EXPECT_THAT(EvalExpr(*let, {&params_data}),
              IsOkAndHolds(Int64(12)));  // (a+1) + (a+1)

  // Check that we get an error if the memory bound is too low. This is
  // particularly important if one of the variables holds an array that
  // represents a WITH table.
  EvaluationOptions options;
  options.max_intermediate_byte_size = 1;
  EvaluationContext memory_context(options);
  EXPECT_THAT(EvalExpr(*let, {&params_data}, &memory_context),
              StatusIs(zetasql_base::StatusCode::kResourceExhausted));
}

TEST_F(EvalTest, ArrayAtOffsetNonDeterminism) {
  VariableId arr("arr"), pos("pos");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_arr,
                       DerefExpr::Create(arr, Int64ArrayType()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_pos, DerefExpr::Create(pos, Int64Type()));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(deref_arr));
  args.push_back(std::move(deref_pos));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto fct, ScalarFunctionCallExpr::Create(
                    CreateFunction(FunctionKind::kArrayAtOffset, Int64Type()),
                    std::move(args)));
  const TupleSchema params_schema({arr, pos});
  ZETASQL_ASSERT_OK(fct->SetSchemasForEvaluation({&params_schema}));

  EvaluationContext context1((EvaluationOptions()));
  // [7,8][offset(1)] == 8, specified order.
  const TupleData params_data1 =
      CreateTestTupleData({Array({Int64(7), Int64(8)}), Int64(1)});
  EXPECT_THAT(EvalExpr(*fct, {&params_data1}, &context1),
              IsOkAndHolds(Int64(8)));
  EXPECT_TRUE(context1.IsDeterministicOutput());

  // [7,8][offset(1)] IN (7, 8), unspecified order.
  EvaluationContext context2((EvaluationOptions()));
  const TupleData params_data2 = CreateTestTupleData(
      {Array({Int64(7), Int64(8)}, kIgnoresOrder), Int64(1)});
  EXPECT_THAT(EvalExpr(*fct, {&params_data2}, &context2),
              IsOkAndHolds(AnyOf(Int64(7), Int64(8))));
  EXPECT_FALSE(context2.IsDeterministicOutput());
}

TEST_F(EvalTest, ArrayReverseNonDeterminism) {
  VariableId arr("arr");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_arr,
                       DerefExpr::Create(arr, Int64ArrayType()));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(deref_arr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto fct, ScalarFunctionCallExpr::Create(
                                     CreateFunction(FunctionKind::kArrayReverse,
                                                    Int64ArrayType()),
                                     std::move(args)));
  const TupleSchema params_schema({arr});
  ZETASQL_ASSERT_OK(fct->SetSchemasForEvaluation({&params_schema}));

  {
    // Deterministic order (single element).

    EvaluationContext context((EvaluationOptions()));
    const TupleData params_data =
        CreateTestTupleData({Array({Int64(70)}, kIgnoresOrder)});
    EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                IsOkAndHolds(values::Int64Array({70})));
    EXPECT_TRUE(context.IsDeterministicOutput());
  }

  {
    // Non-deterministic order (multiple elements).

    EvaluationContext context((EvaluationOptions()));
    const TupleData params_data = CreateTestTupleData(
        {Array({Int64(70), Int64(111), Int64(112)}, kIgnoresOrder)});
    EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                IsOkAndHolds(values::Int64Array({112, 111, 70})));
    EXPECT_FALSE(context.IsDeterministicOutput());
  }

  {
    // Deterministic order (multiple ordered elements).

    EvaluationContext context((EvaluationOptions()));
    const TupleData params_data =
        CreateTestTupleData({Array({Int64(70), Int64(111), Int64(112)})});
    EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                IsOkAndHolds(values::Int64Array({112, 111, 70})));
    EXPECT_TRUE(context.IsDeterministicOutput());
  }
}

TEST_F(EvalTest, SingleValueExpr) {
  VariableId a("a");
  auto input0 =
      absl::WrapUnique(new TestRelationalOp({a}, {},
                                            /*preserves_order=*/true));
  std::vector<std::vector<const SharedProtoState*>> shared_states1;
  auto input1 = absl::WrapUnique(new TestRelationalOp(
      {a}, CreateTestTupleDatas({{GetProtoValue(1)}}, &shared_states1),
      /*preserves_order=*/true));
  auto input2 = absl::WrapUnique(
      new TestRelationalOp({a}, CreateTestTupleDatas({{Int64(1)}, {Int64(2)}}),
                           /*preserves_order=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a0, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a1, DerefExpr::Create(a, Int64Type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_a2, DerefExpr::Create(a, Int64Type()));

  // Empty.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto element0,
      SingleValueExpr::Create(std::move(deref_a0), std::move(input0)));
  ZETASQL_ASSERT_OK(element0->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "SingleValueExpr(\n"
      "+-value: $a,\n"
      "+-input: TestRelationalOp)",
      element0->DebugString());
  EXPECT_THAT(EvalExpr(*element0, EmptyParams()), IsOkAndHolds(NullInt64()));

  // Singleton.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto element1,
      SingleValueExpr::Create(std::move(deref_a1), std::move(input1)));
  ZETASQL_ASSERT_OK(element1->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "SingleValueExpr(\n"
      "+-value: $a,\n"
      "+-input: TestRelationalOp)",
      element1->DebugString());
  EvaluationContext context((EvaluationOptions()));
  TupleSlot result;
  ::zetasql_base::Status status;
  ASSERT_TRUE(element1->EvalSimple(EmptyParams(), &context, &result, &status))
      << status;
  EXPECT_EQ(result.value(), GetProtoValue(1));
  const std::shared_ptr<SharedProtoState> result_shared_state =
      *result.mutable_shared_proto_state();
  EXPECT_THAT(result_shared_state, Pointee(Eq(nullopt)));
  EXPECT_THAT(result_shared_state, HasRawPointer(shared_states1[0][0]));

  // More than one element.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto element2,
      SingleValueExpr::Create(std::move(deref_a2), std::move(input2)));
  ZETASQL_ASSERT_OK(element2->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "SingleValueExpr(\n"
      "+-value: $a,\n"
      "+-input: TestRelationalOp)",
      element2->DebugString());
  EXPECT_THAT(EvalExpr(*element2, EmptyParams()),
              StatusIs(zetasql_base::OUT_OF_RANGE, "More than one element"));
}

TEST_F(EvalTest, ExistsExpr) {
  VariableId a("a");
  auto input0 =
      absl::WrapUnique(new TestRelationalOp({a}, {}, /*preserves_order=*/true));
  auto input1 = absl::WrapUnique(
      new TestRelationalOp({a}, CreateTestTupleDatas({{Int64(1)}}),
                           /*preserves_order=*/true));
  auto input2 = absl::WrapUnique(
      new TestRelationalOp({a}, CreateTestTupleDatas({{Int64(1)}, {Int64(2)}}),
                           /*preserves_order=*/true));

  // Empty.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto exists0, ExistsExpr::Create(std::move(input0)));
  ZETASQL_ASSERT_OK(exists0->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "ExistsExpr(\n"
      "+-input: TestRelationalOp)",
      exists0->DebugString());
  EXPECT_THAT(EvalExpr(*exists0, EmptyParams()), IsOkAndHolds(Bool(false)));

  // Singleton.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto exists1, ExistsExpr::Create(std::move(input1)));
  ZETASQL_ASSERT_OK(exists1->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "ExistsExpr(\n"
      "+-input: TestRelationalOp)",
      exists1->DebugString());
  EXPECT_THAT(EvalExpr(*exists1, EmptyParams()), IsOkAndHolds(Bool(true)));

  // More than one element.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto exists2, ExistsExpr::Create(std::move(input2)));
  ZETASQL_ASSERT_OK(exists2->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "ExistsExpr(\n"
      "+-input: TestRelationalOp)",
      exists2->DebugString());
  EvaluationContext context((EvaluationOptions()));
  EXPECT_THAT(EvalExpr(*exists2, EmptyParams()), IsOkAndHolds(Bool(true)));
}

TEST_F(EvalTest, DerefExprDuplicateIds) {
  const VariableId v("v");
  const VariableId w("w");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto e, DerefExpr::Create(v, Int64Type()));

  const TupleSchema schema1({v, w});
  const TupleSchema schema2({v});
  const TupleSchema schema3({v});

  EXPECT_THAT(
      e->SetSchemasForEvaluation({&schema1, &schema2, &schema3}),
      StatusIs(zetasql_base::INTERNAL, HasSubstr("Duplicate name detected: v")));
}

TEST_F(EvalTest, DerefExprNameNotFound) {
  const VariableId v("v");
  const VariableId w("w");
  const VariableId x("x");
  const VariableId y("y");
  const VariableId z("z");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto e, DerefExpr::Create(v, Int64Type()));

  const TupleSchema schema1({w, x});
  const TupleSchema schema2({y, z});

  EXPECT_THAT(e->SetSchemasForEvaluation({&schema1, &schema2}),
              StatusIs(zetasql_base::INTERNAL, HasSubstr("Missing name: v")));
}

TEST_F(EvalTest, RootExpr) {
  VariableId p("p");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_expr, DerefExpr::Create(p, proto_type_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto root_expr,
      RootExpr::Create(std::move(deref_expr), absl::make_unique<RootData>()));
  EXPECT_EQ(root_expr->DebugString(), "RootExpr($p)");

  const TupleSchema params_schema({p});
  ZETASQL_ASSERT_OK(root_expr->SetSchemasForEvaluation({&params_schema}));

  std::vector<const SharedProtoState*> params_shared_states;
  const TupleData params_data =
      CreateTestTupleData({GetProtoValue(1)}, &params_shared_states);
  EvaluationContext context((EvaluationOptions()));
  TupleSlot result;
  ::zetasql_base::Status status;
  ASSERT_TRUE(root_expr->EvalSimple({&params_data}, &context, &result, &status))
      << status;
  EXPECT_EQ(result.value(), GetProtoValue(1));
  const std::shared_ptr<SharedProtoState> result_shared_state =
      *result.mutable_shared_proto_state();
  EXPECT_THAT(result_shared_state, Pointee(Eq(nullopt)));
  EXPECT_THAT(result_shared_state, HasRawPointer(params_shared_states[0]));
}

}  // namespace
}  // namespace zetasql
