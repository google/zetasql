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

// Tests for ValueExprs not covered by other tests.

#include <cstdint>
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
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/compliance/functions_testlib_common.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/test_relational_op.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/tuple_test_util.h"
#include "zetasql/reference_impl/type_helpers.h"
#include "zetasql/reference_impl/variable_generator.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

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
using testing::Property;
using testing::TestWithParam;
using testing::UnorderedElementsAre;
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
absl::StatusOr<Value> EvalExpr(const ValueExpr& expr,
                               absl::Span<const TupleData* const> params,
                               EvaluationContext* context = nullptr) {
  EvaluationContext empty_context((EvaluationOptions()));
  if (context == nullptr) {
    context = &empty_context;
  }
  TupleSlot slot;
  absl::Status status;
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
      .value();
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
    arguments.push_back(ConstExpr::Create(t.params.param(i)).value());
  }
  auto fct_op = ScalarFunctionCallExpr::Create(
                    CreateFunction(t.kind, t.params.GetResultType()),
                    std::move(arguments))
                    .value();
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
    std::vector<std::unique_ptr<AlgebraArg>> arguments;
    for (int i = 0; i < t.params.num_params(); ++i) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(auto arg, ConstExpr::Create(t.params.param(i)));
      arguments.push_back(absl::make_unique<ExprArg>(std::move(arg)));

      const Type* arg_type = arguments.back()->value_expr()->output_type();
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
    const absl::StatusOr<Value> function_value =
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
      if (each.second.status.code() != absl::StatusCode::kUnknown) {
        EXPECT_THAT(
            function_value,
            StatusIs(each.second.status.code(),
                     HasSubstr(std::string(each.second.status.message()))));
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
        zetasql_test__::KitchenSinkPB::descriptor(), &proto_type_));
  }

  Value GetProtoValue(int i) const {
    zetasql_test__::KitchenSinkPB proto;
    proto.set_int64_key_1(i);
    proto.set_int64_key_2(10 * i);

    return Value::Proto(proto_type_, SerializeToCord(proto));
  }

  const ProtoType* proto_type_ = nullptr;
};

TEST_F(EvalTest, NewStructExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto one_expr, ConstExpr::Create(Int64(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto foo_expr, ConstExpr::Create(String("foo")));

  std::vector<std::unique_ptr<ExprArg>> args;
  args.push_back(absl::make_unique<ExprArg>(std::move(one_expr)));
  args.push_back(absl::make_unique<ExprArg>(std::move(foo_expr)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto struct_op,
      NewStructExpr::Create(
          MakeStructType({{"a", Int64Type()}, {"b", StringType()}}),
          std::move(args)));
  ZETASQL_ASSERT_OK(struct_op->SetSchemasForEvaluation(EmptyParamsSchemas()));
  EXPECT_EQ(
      "NewStructExpr(\n"
      "+-type: STRUCT<a INT64, b STRING>,\n"
      "+-0 a: ConstExpr(1),\n"
      "+-1 b: ConstExpr(\"foo\"))",
      struct_op->DebugString());
  Value result = Struct({"a", "b"}, {Int64(1), String("foo")});
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto value, EvalExpr(*struct_op, EmptyParams()));
  EXPECT_TRUE(result.Equals(value));

  // Test that we can't create a struct that is too large.
  EvaluationOptions value_size_options;
  value_size_options.max_value_byte_size = 1;
  EvaluationContext value_size_context(value_size_options);
  EXPECT_THAT(EvalExpr(*struct_op, EmptyParams(), &value_size_context),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Cannot construct struct Value larger than")));
}

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
  absl::Status status;
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
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Cannot construct array Value larger than")));
}

TEST_F(EvalTest, FieldValueExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto struct_expr,
                       ConstExpr::Create(Struct(
                           {{"foo", GetProtoValue(1)}, {"bar", Int64(0)}})));
  const TupleSlot& slot = struct_expr->slot_test_only();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto field_op,
                       FieldValueExpr::Create(0, std::move(struct_expr)));
  EXPECT_EQ(
      "FieldValueExpr(0:foo, "
      "ConstExpr({foo:{int64_key_1: 1 int64_key_2: 10}, bar:0}))",
      field_op->DebugString());

  EvaluationContext context((EvaluationOptions()));
  TupleSlot result;
  absl::Status status;
  ASSERT_TRUE(field_op->EvalSimple(EmptyParams(), &context, &result, &status))
      << status;
  EXPECT_EQ(GetProtoValue(1), result.value());
  EXPECT_EQ(result.mutable_shared_proto_state()->get(),
            slot.mutable_shared_proto_state()->get());

  // Null struct as input.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto null_struct_expr,
      ConstExpr::Create(Value::Null(MakeStructType({{"foo", Int64Type()}}))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto field_op_null,
      FieldValueExpr::Create(0, std::move(null_struct_expr)));
  EXPECT_THAT(EvalExpr(*field_op_null, EmptyParams()),
              IsOkAndHolds(NullInt64()));
}

static std::unique_ptr<ValueExpr> DivByZeroErrorExpr() {
  std::vector<std::unique_ptr<ValueExpr>> div_args;
  div_args.push_back(ConstExpr::Create(Int64(1)).value());
  div_args.push_back(ConstExpr::Create(Int64(0)).value());

  return ScalarFunctionCallExpr::Create(
             CreateFunction(FunctionKind::kDiv, Int64Type()),
             std::move(div_args), DEFAULT_ERROR_MODE)
      .value();
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
              StatusIs(absl::StatusCode::kResourceExhausted));
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

TEST_F(EvalTest, CodePointsToStringBytesNonDeterminism) {
  for (FunctionKind kind :
       {FunctionKind::kCodePointsToBytes, FunctionKind::kCodePointsToString}) {
    const Type* output_type = BytesType();
    if (kind == FunctionKind::kCodePointsToString) {
      output_type = StringType();
    }

    VariableId arr("arr");

    ZETASQL_ASSERT_OK_AND_ASSIGN(auto deref_arr,
                         DerefExpr::Create(arr, Int64ArrayType()));

    std::vector<std::unique_ptr<ValueExpr>> args;
    args.push_back(std::move(deref_arr));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto fct, ScalarFunctionCallExpr::Create(
                      CreateFunction(kind, output_type), std::move(args)));
    const TupleSchema params_schema({arr});
    ZETASQL_ASSERT_OK(fct->SetSchemasForEvaluation({&params_schema}));

    {
      // Deterministic order (single element).

      EvaluationContext context((EvaluationOptions()));
      const TupleData params_data =
          CreateTestTupleData({Array({Int64(70)}, kIgnoresOrder)});

      if (kind == FunctionKind::kCodePointsToBytes) {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(Bytes("F")));
      } else {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(String("F")));
      }
      EXPECT_TRUE(context.IsDeterministicOutput());
    }

    {
      // Non-deterministic order (multiple elements).

      EvaluationContext context((EvaluationOptions()));
      const TupleData params_data = CreateTestTupleData(
          {Array({Int64(70), Int64(111), Int64(111)}, kIgnoresOrder)});
      if (kind == FunctionKind::kCodePointsToBytes) {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(Bytes("Foo")));
      } else {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(String("Foo")));
      }
      EXPECT_FALSE(context.IsDeterministicOutput());
    }

    {
      // Deterministic order (multiple ordered elements).

      EvaluationContext context((EvaluationOptions()));
      const TupleData params_data =
          CreateTestTupleData({Array({Int64(70), Int64(111), Int64(111)})});
      if (kind == FunctionKind::kCodePointsToBytes) {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(Bytes("Foo")));
      } else {
        EXPECT_THAT(EvalExpr(*fct, {&params_data}, &context),
                    IsOkAndHolds(String("Foo")));
      }
      EXPECT_TRUE(context.IsDeterministicOutput());
    }
  }
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
  absl::Status status;
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
              StatusIs(absl::StatusCode::kOutOfRange, "More than one element"));
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

  EXPECT_THAT(e->SetSchemasForEvaluation({&schema1, &schema2, &schema3}),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Duplicate name detected: v")));
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

  EXPECT_THAT(
      e->SetSchemasForEvaluation({&schema1, &schema2}),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("Missing name: v")));
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
  absl::Status status;
  ASSERT_TRUE(root_expr->EvalSimple({&params_data}, &context, &result, &status))
      << status;
  EXPECT_EQ(result.value(), GetProtoValue(1));
  const std::shared_ptr<SharedProtoState> result_shared_state =
      *result.mutable_shared_proto_state();
  EXPECT_THAT(result_shared_state, Pointee(Eq(nullopt)));
  EXPECT_THAT(result_shared_state, HasRawPointer(params_shared_states[0]));
}

class DMLValueExprEvalTest : public EvalTest {
 public:
  DMLValueExprEvalTest() {
    ZETASQL_CHECK_OK(table_.SetPrimaryKey({0}));
    GetZetaSQLFunctions(type_factory(), ZetaSQLBuiltinFunctionOptions{},
                          &functions_);
  }

  const Table* table() { return &table_; }

  const Function* function(const std::string& name) {
    return functions_[name].get();
  }

 private:
  SimpleTable table_{"test_table",
                     {{"int_val", Int64Type()}, {"str_val", StringType()}}};
  std::map<std::string, std::unique_ptr<Function>> functions_;
};

TEST_F(DMLValueExprEvalTest, DMLInsertValueExpr) {
  // Build a resolved AST for inserting a new row (3, "three") into the table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<const ResolvedDMLValue>> row_values;
  row_values.push_back(MakeResolvedDMLValue(
      MakeResolvedLiteral(Int64Type(), Int64(3), /*has_explicit_type=*/
                          true, /*float_literal_id=*/0)));
  row_values.push_back(MakeResolvedDMLValue(
      MakeResolvedLiteral(StringType(), String("three"), /*has_explicit_type=*/
                          true, /*float_literal_id=*/0)));
  std::vector<std::unique_ptr<const ResolvedInsertRow>> row_list;
  row_list.push_back(MakeResolvedInsertRow(std::move(row_values)));
  std::unique_ptr<ResolvedInsertStmt> stmt = MakeResolvedInsertStmt(
      std::move(table_scan), ResolvedInsertStmt::OR_ERROR,
      /*assert_rows_modified=*/nullptr, /*returning=*/nullptr,
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      /*query_parameter_list=*/{}, /*query=*/nullptr,
      /*query_output_column_list=*/{}, std::move(row_list));

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  for (const auto& value : stmt->row_list(0)->value_list()) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ValueExpr> const_expr,
        ConstExpr::Create(value->value()->GetAs<ResolvedLiteral>()->value()));
    (*resolved_expr_map)[value->value()] = std::move(const_expr);
  }

  // Create the DMLInsertValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLInsertValueExpr> expr,
      DMLInsertValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      LanguageOptions{}));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_TRUE(expr->EvalSimple({}, &context, &result, &status));
  ZETASQL_ASSERT_OK(status);
  EXPECT_EQ(result.value().field(0).int64_value(), 1);
  EXPECT_THAT(result.value().field(1).elements(),
              UnorderedElementsAre(Property(
                  &Value::fields, ElementsAre(Int64(3), String("three")))));
}

TEST_F(DMLValueExprEvalTest,
       DMLInsertValueExprSetsPrimaryKeyValuesToNullWhenDisallowed) {
  // Build a resolved AST for inserting a new row (3, "three") into the table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<const ResolvedDMLValue>> row_values;
  row_values.push_back(MakeResolvedDMLValue(
      MakeResolvedLiteral(Int64Type(), NullInt64(), /*has_explicit_type=*/
                          true, /*float_literal_id=*/0)));
  row_values.push_back(MakeResolvedDMLValue(
      MakeResolvedLiteral(StringType(), String("three"), /*has_explicit_type=*/
                          true, /*float_literal_id=*/0)));
  std::vector<std::unique_ptr<const ResolvedInsertRow>> row_list;
  row_list.push_back(MakeResolvedInsertRow(std::move(row_values)));
  std::unique_ptr<ResolvedInsertStmt> stmt = MakeResolvedInsertStmt(
      std::move(table_scan), ResolvedInsertStmt::OR_ERROR,
      /*assert_rows_modified=*/nullptr, /*returning=*/nullptr,
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      /*query_parameter_list=*/{}, /*query=*/nullptr,
      /*query_output_column_list=*/{}, std::move(row_list));

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  for (const auto& value : stmt->row_list(0)->value_list()) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ValueExpr> const_expr,
        ConstExpr::Create(value->value()->GetAs<ResolvedLiteral>()->value()));
    (*resolved_expr_map)[value->value()] = std::move(const_expr);
  }

  // Create the DMLInsertValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLInsertValueExpr> expr,
      DMLInsertValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};;
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_DISALLOW_NULL_PRIMARY_KEYS);
  context.SetLanguageOptions(language_options);
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      language_options));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_FALSE(expr->EvalSimple({}, &context, &result, &status));
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("INSERT a NULL value into a primary key column")));
}

TEST_F(DMLValueExprEvalTest, DMLDeleteValueExpr) {
  // Build a resolved AST for deleting rows where str_val is null from the
  // table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<ResolvedColumnRef>> resolved_column_refs;
  resolved_column_refs.push_back(MakeResolvedColumnRef(
      table_scan->column_list(1).type(), table_scan->column_list(1),
      /*is_correlated=*/false));
  std::unique_ptr<ResolvedDeleteStmt> stmt = MakeResolvedDeleteStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      /*returning=*/nullptr,
      /*array_offset_column=*/nullptr, /*where_expr=*/
      MakeResolvedFunctionCall(BoolType(), function("$is_null"),
                               *function("$is_null")->GetSignature(0),
                               std::move(resolved_column_refs),
                               ResolvedFunctionCall::DEFAULT_ERROR_MODE));

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  std::vector<std::unique_ptr<ValueExpr>> arguments{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> arg_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->where_expr()
                                 ->GetAs<ResolvedFunctionCall>()
                                 ->argument_list(0)
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->where_expr()
                            ->GetAs<ResolvedFunctionCall>()
                            ->argument_list(0)
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  arguments.push_back(std::move(arg_expr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionKind function_kind,
      BuiltinFunctionCatalog::GetKindByName(stmt->where_expr()
                                                ->GetAs<ResolvedFunctionCall>()
                                                ->function()
                                                ->Name()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> function_call_expr,
      BuiltinScalarFunction::CreateCall(
          function_kind, LanguageOptions{},
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->type(),
          std::move(arguments),
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->error_mode()));
  (*resolved_expr_map)[stmt->where_expr()] = std::move(function_call_expr);

  // Create the DMLDeleteValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLDeleteValueExpr> expr,
      DMLDeleteValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      LanguageOptions{}));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_TRUE(expr->EvalSimple({}, &context, &result, &status));
  ZETASQL_ASSERT_OK(status);
  EXPECT_EQ(result.value().field(0).int64_value(), 2);
  EXPECT_THAT(
      result.value().field(1).elements(),
      UnorderedElementsAre(
          Property(&Value::fields, ElementsAre(Int64(2), NullString())),
          Property(&Value::fields, ElementsAre(Int64(4), NullString()))));
}

TEST_F(DMLValueExprEvalTest, DMLUpdateValueExpr) {
  // Build a resolved AST for updating str_val from null to 'unknown' in the
  // table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<ResolvedUpdateItem>> update_item_list;
  update_item_list.push_back(MakeResolvedUpdateItem(
      MakeResolvedColumnRef(table_scan->column_list(1).type(),
                            table_scan->column_list(1),
                            /*is_correlated=*/false),
      MakeResolvedDMLValue(MakeResolvedLiteral(
          StringType(), String("unknown"), /*has_explicit_type=*/
          true, /*float_literal_id=*/0)),
      /*element_column=*/nullptr, /*array_update_list=*/{}, /*delete_list=*/{},
      /*update_list=*/{}, /*insert_list=*/{}));
  std::vector<std::unique_ptr<ResolvedColumnRef>> resolved_column_refs;
  resolved_column_refs.push_back(MakeResolvedColumnRef(
      table_scan->column_list(1).type(), table_scan->column_list(1),
      /*is_correlated=*/false));
  std::unique_ptr<ResolvedUpdateStmt> stmt = MakeResolvedUpdateStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      /*returning=*/nullptr, /*array_offset_column=*/nullptr,
      /*where_expr=*/
      MakeResolvedFunctionCall(BoolType(), function("$is_null"),
                               *function("$is_null")->GetSignature(0),
                               std::move(resolved_column_refs),
                               ResolvedFunctionCall::DEFAULT_ERROR_MODE),
      /*update_item_list=*/std::move(update_item_list), /*from_scan=*/nullptr);

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  std::vector<std::unique_ptr<ValueExpr>> arguments{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> arg_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->where_expr()
                                 ->GetAs<ResolvedFunctionCall>()
                                 ->argument_list(0)
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->where_expr()
                            ->GetAs<ResolvedFunctionCall>()
                            ->argument_list(0)
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  arguments.push_back(std::move(arg_expr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionKind function_kind,
      BuiltinFunctionCatalog::GetKindByName(stmt->where_expr()
                                                ->GetAs<ResolvedFunctionCall>()
                                                ->function()
                                                ->Name()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> function_call_expr,
      BuiltinScalarFunction::CreateCall(
          function_kind, LanguageOptions{},
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->type(),
          std::move(arguments),
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->error_mode()));
  (*resolved_expr_map)[stmt->where_expr()] = std::move(function_call_expr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> target_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->update_item_list(0)
                                 ->target()
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->update_item_list(0)
                            ->target()
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  (*resolved_expr_map)[stmt->update_item_list(0)->target()] =
      std::move(target_expr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ConstExpr> set_value_expr,
                       ConstExpr::Create(stmt->update_item_list(0)
                                             ->set_value()
                                             ->value()
                                             ->GetAs<ResolvedLiteral>()
                                             ->value()));
  (*resolved_expr_map)[stmt->update_item_list(0)->set_value()->value()] =
      std::move(set_value_expr);
  // Touch the getter to pass CheckFieldsAccessed().
  ASSERT_EQ(stmt->update_item_list(0)->element_column(), nullptr);

  // Create the DMLUpdateValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLUpdateValueExpr> expr,
      DMLUpdateValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      LanguageOptions{}));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_TRUE(expr->EvalSimple({}, &context, &result, &status));
  ZETASQL_ASSERT_OK(status);
  EXPECT_EQ(result.value().field(0).int64_value(), 2);
  EXPECT_THAT(
      result.value().field(1).elements(),
      UnorderedElementsAre(
          Property(&Value::fields, ElementsAre(Int64(2), String("unknown"))),
          Property(&Value::fields, ElementsAre(Int64(4), String("unknown")))));
}

TEST_F(DMLValueExprEvalTest,
       DMLUpdateValueExprModifiesPrimaryKeyWhenDisallowed) {
  // Build a resolved AST for updating str_val from null to 'unknown' in the
  // table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<ResolvedUpdateItem>> update_item_list;
  update_item_list.push_back(MakeResolvedUpdateItem(
      MakeResolvedColumnRef(table_scan->column_list(0).type(),
                            table_scan->column_list(0),
                            /*is_correlated=*/false),
      MakeResolvedDMLValue(MakeResolvedLiteral(Int64Type(),
                                               Int64(0), /*has_explicit_type=*/
                                               true, /*float_literal_id=*/0)),
      /*element_column=*/nullptr, /*array_update_list=*/{}, /*delete_list=*/{},
      /*update_list=*/{}, /*insert_list=*/{}));
  std::vector<std::unique_ptr<ResolvedColumnRef>> resolved_column_refs;
  resolved_column_refs.push_back(MakeResolvedColumnRef(
      table_scan->column_list(1).type(), table_scan->column_list(1),
      /*is_correlated=*/false));
  std::unique_ptr<ResolvedUpdateStmt> stmt = MakeResolvedUpdateStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      /*returning=*/nullptr, /*array_offset_column=*/nullptr,
      /*where_expr=*/
      MakeResolvedFunctionCall(BoolType(), function("$is_null"),
                               *function("$is_null")->GetSignature(0),
                               std::move(resolved_column_refs),
                               ResolvedFunctionCall::DEFAULT_ERROR_MODE),
      /*update_item_list=*/std::move(update_item_list), /*from_scan=*/nullptr);

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  std::vector<std::unique_ptr<ValueExpr>> arguments{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> arg_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->where_expr()
                                 ->GetAs<ResolvedFunctionCall>()
                                 ->argument_list(0)
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->where_expr()
                            ->GetAs<ResolvedFunctionCall>()
                            ->argument_list(0)
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  arguments.push_back(std::move(arg_expr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionKind function_kind,
      BuiltinFunctionCatalog::GetKindByName(stmt->where_expr()
                                                ->GetAs<ResolvedFunctionCall>()
                                                ->function()
                                                ->Name()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> function_call_expr,
      BuiltinScalarFunction::CreateCall(
          function_kind, LanguageOptions{},
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->type(),
          std::move(arguments),
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->error_mode()));
  (*resolved_expr_map)[stmt->where_expr()] = std::move(function_call_expr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> target_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->update_item_list(0)
                                 ->target()
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->update_item_list(0)
                            ->target()
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  (*resolved_expr_map)[stmt->update_item_list(0)->target()] =
      std::move(target_expr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ConstExpr> set_value_expr,
                       ConstExpr::Create(stmt->update_item_list(0)
                                             ->set_value()
                                             ->value()
                                             ->GetAs<ResolvedLiteral>()
                                             ->value()));
  (*resolved_expr_map)[stmt->update_item_list(0)->set_value()->value()] =
      std::move(set_value_expr);
  // Touch the getter to pass CheckFieldsAccessed().
  ASSERT_EQ(stmt->update_item_list(0)->element_column(), nullptr);

  // Create the DMLUpdateValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLUpdateValueExpr> expr,
      DMLUpdateValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};;
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_DISALLOW_PRIMARY_KEY_UPDATES);
  context.SetLanguageOptions(language_options);
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      language_options));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_FALSE(expr->EvalSimple({}, &context, &result, &status));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("modify a primary key column")));
}

TEST_F(DMLValueExprEvalTest,
       DMLUpdateValueExprSetsPrimaryKeyValuesToNullWhenDisallowed) {
  // Build a resolved AST for updating str_val from null to 'unknown' in the
  // table.
  std::unique_ptr<ResolvedTableScan> table_scan = MakeResolvedTableScan(
      {ResolvedColumn{1, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("int_val"), Int64Type()},
       ResolvedColumn{2, zetasql::IdString::MakeGlobal("test_table"),
                      zetasql::IdString::MakeGlobal("str_val"),
                      StringType()}},
      table(), /*for_system_time_expr=*/nullptr);
  std::vector<std::unique_ptr<ResolvedUpdateItem>> update_item_list;
  update_item_list.push_back(MakeResolvedUpdateItem(
      MakeResolvedColumnRef(table_scan->column_list(0).type(),
                            table_scan->column_list(0),
                            /*is_correlated=*/false),
      MakeResolvedDMLValue(
          MakeResolvedLiteral(Int64Type(), NullInt64(), /*has_explicit_type=*/
                              true, /*float_literal_id=*/0)),
      /*element_column=*/nullptr, /*array_update_list=*/{}, /*delete_list=*/{},
      /*update_list=*/{}, /*insert_list=*/{}));
  std::vector<std::unique_ptr<ResolvedColumnRef>> resolved_column_refs;
  resolved_column_refs.push_back(MakeResolvedColumnRef(
      table_scan->column_list(1).type(), table_scan->column_list(1),
      /*is_correlated=*/false));
  std::unique_ptr<ResolvedUpdateStmt> stmt = MakeResolvedUpdateStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      /*returning=*/nullptr, /*array_offset_column=*/nullptr,
      /*where_expr=*/
      MakeResolvedFunctionCall(BoolType(), function("$is_null"),
                               *function("$is_null")->GetSignature(0),
                               std::move(resolved_column_refs),
                               ResolvedFunctionCall::DEFAULT_ERROR_MODE),
      /*update_item_list=*/std::move(update_item_list), /*from_scan=*/nullptr);

  // Create output types.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ArrayType* table_array_type,
      CreateTableArrayType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->IsValueTable(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const StructType* primary_key_type,
      CreatePrimaryKeyType(stmt->table_scan()->column_list(),
                           stmt->table_scan()->table()->PrimaryKey().value(),
                           type_factory()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const StructType* dml_output_type,
                       CreateDMLOutputType(table_array_type, type_factory()));

  // Create a ColumnToVariableMapping.
  auto column_to_variable_mapping = absl::make_unique<ColumnToVariableMapping>(
      absl::make_unique<VariableGenerator>());

  // Build a ResolvedScanMap and a ResolvedExprMap from the AST.
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> table_as_array_expr,
      TableAsArrayExpr::Create(stmt->table_scan()->table()->Name(),
                               table_array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<RelationalOp> relation_op,
      ArrayScanOp::Create(
          /*element=*/VariableId(),
          /*position=*/VariableId(),
          {std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[0]),
                          0),
           std::make_pair(column_to_variable_mapping->GetVariableNameFromColumn(
                              stmt->table_scan()->column_list()[1]),
                          1)},
          std::move(table_as_array_expr)));
  (*resolved_scan_map)[stmt->table_scan()] = std::move(relation_op);
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  std::vector<std::unique_ptr<ValueExpr>> arguments{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> arg_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->where_expr()
                                 ->GetAs<ResolvedFunctionCall>()
                                 ->argument_list(0)
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->where_expr()
                            ->GetAs<ResolvedFunctionCall>()
                            ->argument_list(0)
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  arguments.push_back(std::move(arg_expr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionKind function_kind,
      BuiltinFunctionCatalog::GetKindByName(stmt->where_expr()
                                                ->GetAs<ResolvedFunctionCall>()
                                                ->function()
                                                ->Name()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValueExpr> function_call_expr,
      BuiltinScalarFunction::CreateCall(
          function_kind, LanguageOptions{},
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->type(),
          std::move(arguments),
          stmt->where_expr()->GetAs<ResolvedFunctionCall>()->error_mode()));
  (*resolved_expr_map)[stmt->where_expr()] = std::move(function_call_expr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DerefExpr> target_expr,
      DerefExpr::Create(column_to_variable_mapping->GetVariableNameFromColumn(
                            stmt->update_item_list(0)
                                 ->target()
                                 ->GetAs<ResolvedColumnRef>()
                                 ->column()),
                        stmt->update_item_list(0)
                            ->target()
                            ->GetAs<ResolvedColumnRef>()
                            ->column()
                            .type()));
  (*resolved_expr_map)[stmt->update_item_list(0)->target()] =
      std::move(target_expr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ConstExpr> set_value_expr,
                       ConstExpr::Create(stmt->update_item_list(0)
                                             ->set_value()
                                             ->value()
                                             ->GetAs<ResolvedLiteral>()
                                             ->value()));
  (*resolved_expr_map)[stmt->update_item_list(0)->set_value()->value()] =
      std::move(set_value_expr);
  // Touch the getter to pass CheckFieldsAccessed().
  ASSERT_EQ(stmt->update_item_list(0)->element_column(), nullptr);

  // Create the DMLUpdateValueExpr to be tested.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DMLUpdateValueExpr> expr,
      DMLUpdateValueExpr::Create(
          stmt->table_scan()->table(), table_array_type,
          /*returning_array_type=*/nullptr, primary_key_type, dml_output_type,
          stmt.get(), &stmt->table_scan()->column_list(),
          /*returning_column_values=*/nullptr,
          std::move(column_to_variable_mapping), std::move(resolved_scan_map),
          std::move(resolved_expr_map)));

  // Evaluate and check.
  TupleSlot result;
  absl::Status status;
  // the case for return_all_rows_for_dml = true is covered by the reference
  // implementation compliance tests
  EvaluationOptions options{};
  options.return_all_rows_for_dml = false;
  EvaluationContext context{options};;
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_DISALLOW_NULL_PRIMARY_KEYS);
  context.SetLanguageOptions(language_options);
  ZETASQL_CHECK_OK(context.AddTableAsArray(
      "test_table", /*is_value_table=*/false,
      Value::Array(table_array_type,
                   {Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(1), String("one")}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(2), NullString()}),
                    Value::Struct(table_array_type->element_type()->AsStruct(),
                                  {Int64(4), NullString()})}),
      language_options));
  ZETASQL_ASSERT_OK(expr->SetSchemasForEvaluation({}));
  EXPECT_FALSE(expr->EvalSimple({}, &context, &result, &status));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("set a primary key column to NULL")));
}

// TODO: Many of the tests that use this fixture should probably be
// moved to public/proto_util_test.cc.
class ProtoEvalTest : public ::testing::Test {
 public:
  ProtoEvalTest() : type_factory_(test_values::static_type_factory()) {}
  ~ProtoEvalTest() override {}

  const ProtoType* MakeProtoType(const google::protobuf::Message* msg) {
    const ProtoType* proto_type;
    ZETASQL_CHECK_OK(type_factory_->MakeProtoType(msg->GetDescriptor(), &proto_type));
    return proto_type;
  }

  // Reads 'field_name' of 'msg' using a GetProtoFieldExpr. Crashes on error.
  Value GetProtoFieldOrDie(const google::protobuf::Message* msg,
                           const std::string& field_name) {
    return GetProtoField(msg, field_name).value();
  }

  // Reads 'field_name' of 'proto_value' using a GetProtoFieldExpr. Crashes on
  // error.
  Value GetProtoFieldOrDie(const Value& proto_value,
                           const std::string& field_name) {
    return GetProtoField(proto_value, field_name).value();
  }

  // Reads 'field_name' of 'msg' using a GetProtoFieldExpr.
  absl::StatusOr<Value> GetProtoField(const google::protobuf::Message* msg,
                                      const std::string& field_name) {
    const ProtoType* proto_type = MakeProtoType(msg);
    return GetProtoField(Value::Proto(proto_type, SerializePartialToCord(*msg)),
                         field_name);
  }

  // Reads 'field_name' of 'proto_value' using a GetProtoFieldExpr.
  absl::StatusOr<Value> GetProtoField(const Value& proto_value,
                                      const std::string& field_name) {
    TupleSlot proto_slot;
    proto_slot.SetValue(proto_value);
    EvaluationContext context((EvaluationOptions()));
    ZETASQL_ASSIGN_OR_RETURN(TupleSlot output_slot,
                     EvalGetProtoFieldExpr(proto_slot, field_name,
                                           /*get_has_bit=*/false, &context));
    return output_slot.value();
  }

  // Checks presence of 'field_name' in 'msg' using a GetProtoFieldExpr. Crashes
  // on error.
  Value HasProtoFieldOrDie(const google::protobuf::Message* msg,
                           const std::string& field_name) {
    const ProtoType* proto_type = MakeProtoType(msg);
    return HasProtoFieldOrDie(
        Value::Proto(proto_type, SerializePartialToCord(*msg)), field_name);
  }

  // Checks presence of 'field_name' in 'proto_value' using a
  // GetProtoFieldExpr. Crashes on error.
  Value HasProtoFieldOrDie(const Value& proto_value,
                           const std::string& field_name) {
    TupleSlot proto_slot;
    proto_slot.SetValue(proto_value);
    EvaluationContext context((EvaluationOptions()));
    return EvalGetProtoFieldExpr(proto_slot, field_name, /*get_has_bit=*/true,
                                 &context)
        .value()
        .value();
  }

  // Creates a GetProtoFieldExpr that either accesses or checks the presence of
  // 'field_name'. Invokes it on 'proto_slot' by passing 'proto_slot' in
  // parameter 'p'.
  absl::StatusOr<TupleSlot> EvalGetProtoFieldExpr(const TupleSlot& proto_slot,
                                                  const std::string& field_name,
                                                  bool get_has_bit,
                                                  EvaluationContext* context) {
    const google::protobuf::FieldDescriptor* field_descr =
        proto_slot.value().type()->AsProto()->descriptor()->FindFieldByName(
            field_name);
    ZETASQL_RET_CHECK(field_descr != nullptr);
    const Type* field_type;
    Value default_value;
    ZETASQL_RET_CHECK_OK(GetProtoFieldTypeAndDefault(field_descr, type_factory_,
                                             &field_type, &default_value));

    ProtoFieldAccessInfo access_info;
    access_info.field_info.descriptor = field_descr;
    access_info.field_info.type = field_type;
    access_info.field_info.format = ProtoType::GetFormatAnnotation(field_descr);
    access_info.field_info.get_has_bit = get_has_bit;
    access_info.field_info.default_value = default_value;

    std::vector<absl::StatusOr<TupleSlot>> output_slots;
    ZETASQL_RETURN_IF_ERROR(EvalGetProtoFieldExprs({std::make_pair(&access_info, 1)},
                                           {proto_slot}, context,
                                           &output_slots));
    ZETASQL_RET_CHECK_EQ(output_slots.size(), 1);
    return output_slots[0];
  }

  // Creates a single FieldRegistry for 'infos'. For each (ProtoFieldAccessInfo
  // info, int count) pair, there is a ProtoFieldReader wrapped by 'count'
  // GetProtoFieldExprs. For each TupleSlot in 'proto_slots', evaluates all of
  // the ProtoFieldExprs and appends the results to 'output_slots'. Tests that
  // GetProtoFieldExpr::Eval() only reads from the proto when it should. The
  // actual proto value is represented by a parameter.
  absl::Status EvalGetProtoFieldExprs(
      std::vector<std::pair<const ProtoFieldAccessInfo*, int>> infos,
      const std::vector<TupleSlot>& proto_slots, EvaluationContext* context,
      std::vector<absl::StatusOr<TupleSlot>>* output_slots) {
    ZETASQL_RET_CHECK(!proto_slots.empty());

    context->set_populate_last_get_field_value_call_read_fields_from_proto_map(
        true);

    // Build the GetProtoFieldExprs.
    VariableId p("p");
    std::unique_ptr<ProtoFieldRegistry> registry;
    std::vector<std::unique_ptr<ProtoFieldReader>> field_readers;
    std::vector<std::unique_ptr<GetProtoFieldExpr>> exprs;
    ZETASQL_RETURN_IF_ERROR(CreateGetProtoFieldExprs(
        infos, p, proto_slots[0].value().type()->AsProto(), context, &registry,
        &field_readers, &exprs));

    // Evaluate the GetProtoFieldExprs as described in the method comment.
    for (const TupleSlot& proto_slot : proto_slots) {
      const TupleData params({proto_slot});
      for (int expr_idx = 0; expr_idx < exprs.size(); ++expr_idx) {
        const GetProtoFieldExpr& expr = *exprs[expr_idx];

        TupleSlot output_slot;
        absl::Status status;
        if (!expr.EvalSimple({&params}, context, &output_slot, &status)) {
          return status;
        }
        if (TupleSlot::ShouldStoreSharedProtoStateFor(
                output_slot.value().type_kind())) {
          EXPECT_EQ(proto_slot.mutable_shared_proto_state()->get(),
                    output_slot.mutable_shared_proto_state()->get());
        }
        output_slots->push_back(output_slot);

        bool expect_to_read_fields;
        if (proto_slot.value().is_null()) {
          // We can't read fields from a NULL proto.
          expect_to_read_fields = false;
        } else if (context->options().store_proto_field_value_maps) {
          // We should only read from the proto the first time.
          expect_to_read_fields = (expr_idx == 0);
        } else {
          // We have to read fields from non-NULL protos every time.
          expect_to_read_fields = true;
        }

        const bool read_fields =
            context->last_get_field_value_call_read_fields_from_proto(
                expr.field_reader());
        EXPECT_EQ(read_fields, expect_to_read_fields)
            << "proto_value:\n"
            << proto_slot.value() << "\nexpr_idx = " << expr_idx;
      }
    }

    return absl::OkStatus();
  }

  // Creates a single ProtoFieldAccessInfo for 'infos', and for each
  // (ProtoFieldAccessInfo info, int count) pair, creates a ProtoFieldReader
  // wrapped by 'count' GetProtoFieldExprs.
  absl::Status CreateGetProtoFieldExprs(
      std::vector<std::pair<const ProtoFieldAccessInfo*, int>> infos,
      const VariableId& variable_id, const ProtoType* type,
      EvaluationContext* context, std::unique_ptr<ProtoFieldRegistry>* registry,
      std::vector<std::unique_ptr<ProtoFieldReader>>* field_readers,
      std::vector<std::unique_ptr<GetProtoFieldExpr>>* exprs) {
    *registry = absl::make_unique<ProtoFieldRegistry>(
        /*id=*/0);

    for (const auto& entry : infos) {
      const ProtoFieldAccessInfo* info = entry.first;
      const int count = entry.second;

      const int id = field_readers->size();
      field_readers->push_back(
          absl::make_unique<ProtoFieldReader>(id, *info, registry->get()));
      ProtoFieldReader* field_reader = field_readers->back().get();

      for (int i = 0; i < count; ++i) {
        ZETASQL_ASSIGN_OR_RETURN(auto deref_expr, DerefExpr::Create(variable_id, type));
        ZETASQL_ASSIGN_OR_RETURN(
            auto get_proto_field_expr,
            GetProtoFieldExpr::Create(std::move(deref_expr), field_reader));

        const TupleSchema params_schema({variable_id});
        ZETASQL_RETURN_IF_ERROR(
            get_proto_field_expr->SetSchemasForEvaluation({&params_schema}));

        std::string field_name = info->field_info.descriptor->name();
        if (info->field_info.get_has_bit) {
          field_name = absl::StrCat("has_", field_name);
        }
        EXPECT_EQ(get_proto_field_expr->DebugString(),
                  absl::StrCat("GetProtoFieldExpr(", field_name,
                               ", $p [fid=", id, " rid=0])"));

        exprs->push_back(std::move(get_proto_field_expr));
      }
    }

    return absl::OkStatus();
  }

  absl::Status MakeProto(
      const std::vector<std::pair<std::string, Value>>& fields,
      google::protobuf::Message* out) {
    std::vector<MakeProtoFunction::FieldAndFormat> field_and_formats;
    std::vector<Value> values;
    std::vector<std::unique_ptr<ValueExpr>> arguments;
    for (const auto& p : fields) {
      const std::string& field_name = p.first;
      const Value& value = p.second;
      const auto descr = out->GetDescriptor()->FindFieldByName(field_name);
      ZETASQL_CHECK(descr != nullptr)
          << "No field '" << field_name << "' in proto of type "
          << out->GetDescriptor()->full_name();
      field_and_formats.emplace_back(descr,
                                     ProtoType::GetFormatAnnotation(descr));
      values.push_back(value);
      ZETASQL_ASSIGN_OR_RETURN(auto const_value, ConstExpr::Create(value));
      arguments.push_back(std::move(const_value));
    }
    ZETASQL_ASSIGN_OR_RETURN(auto fct_op,
                     ScalarFunctionCallExpr::Create(
                         absl::make_unique<MakeProtoFunction>(
                             MakeProtoType(out), field_and_formats),
                         std::move(arguments)));
    ZETASQL_ASSIGN_OR_RETURN(Value result, EvalExpr(*fct_op, EmptyParams()));
    ZETASQL_CHECK(result.type()->IsProto());
    out->Clear();
    if (!result.is_null()) {
      EXPECT_TRUE(ParsePartialFromCord(result.ToCord(), out))
          << result.FullDebugString();
    }
    return absl::OkStatus();
  }

  Value MakeProtoValue(const std::vector<std::pair<std::string, Value>>& fields,
                       const google::protobuf::Message& msg) {
    std::unique_ptr<google::protobuf::Message> tmp(msg.New());
    ZETASQL_CHECK_OK(MakeProto(fields, tmp.get()));
    return Value::Proto(MakeProtoType(&msg), SerializePartialToCord(*tmp));
  }

  // 'out' is cleared first.
  std::string FormatProto(
      const std::vector<std::pair<std::string, Value>>& fields,
      google::protobuf::Message* out) {
    absl::Status result = MakeProto(fields, out);
    if (result.ok()) {
      return out->ShortDebugString();
    } else {
      return zetasql::internal::StatusToString(result);
    }
  }

  // 'out' is cleared first.
  std::string FormatProto(const std::string& field_name, const Value& value,
                          google::protobuf::Message* out) {
    return FormatProto({{field_name, value}}, out);
  }

 protected:
  TypeFactory* type_factory_;
};

TEST_F(ProtoEvalTest, CreatePrimitiveProtoFields) {
  zetasql_test__::KitchenSinkPB p;
  // int32_t
  EXPECT_EQ("int32_val: -5", FormatProto("int32_val", Int32(-5), &p));
  EXPECT_EQ("", FormatProto("int32_val", NullInt32(), &p));
  EXPECT_THAT(FormatProto("int32_val", Uint32(0), &p),
              HasSubstr("out_of_range"));
  EXPECT_THAT(FormatProto("int32_val", Array({Int32(0)}), &p),
              HasSubstr("out_of_range"));
  EXPECT_THAT(FormatProto("int32_val",
                          Value::EmptyArray(types::Int32ArrayType()), &p),
              HasSubstr("out_of_range"));
  // uint32_t
  EXPECT_EQ("uint32_val: 5", FormatProto("uint32_val", Uint32(5), &p));
  EXPECT_EQ("", FormatProto("uint32_val", NullUint32(), &p));
  EXPECT_THAT(FormatProto("uint32_val", Int32(0), &p),
              HasSubstr("out_of_range"));

  // int64_t
  EXPECT_EQ("int64_val: -5", FormatProto("int64_val", Int64(-5), &p));
  EXPECT_EQ("", FormatProto("int64_val", NullInt64(), &p));
  EXPECT_THAT(FormatProto("int64_val", Int32(0), &p),
              HasSubstr("out_of_range"));

  // uint64_t
  EXPECT_EQ("uint64_val: 5", FormatProto("uint64_val", Uint64(5), &p));
  EXPECT_EQ("", FormatProto("uint64_val", NullUint64(), &p));
  EXPECT_THAT(FormatProto("uint64_val", Int32(0), &p),
              HasSubstr("out_of_range"));

  // bool
  EXPECT_EQ("bool_val: true", FormatProto("bool_val", True(), &p));
  EXPECT_EQ("", FormatProto("bool_val", NullBool(), &p));
  EXPECT_THAT(FormatProto("bool_val", Int32(1), &p), HasSubstr("out_of_range"));

  // float
  EXPECT_EQ("float_val: 0.3", FormatProto("float_val", Float(0.3), &p));
  EXPECT_EQ("", FormatProto("float_val", NullFloat(), &p));
  EXPECT_THAT(FormatProto("float_val", Double(0.3), &p),
              HasSubstr("out_of_range"));

  // double
  EXPECT_EQ("double_val: 0.5", FormatProto("double_val", Double(0.5), &p));
  EXPECT_EQ("", FormatProto("double_val", NullDouble(), &p));
  EXPECT_THAT(FormatProto("double_val", Float(0.5), &p),
              HasSubstr("out_of_range"));

  // string
  EXPECT_EQ("string_val: \"@\"", FormatProto("string_val", String("@"), &p));
  EXPECT_EQ("", FormatProto("string_val", NullString(), &p));
  EXPECT_THAT(FormatProto("string_val", Bytes("@"), &p),
              HasSubstr("out_of_range"));

  // bytes
  EXPECT_EQ("bytes_val: \"@\"", FormatProto("bytes_val", Bytes("@"), &p));
  EXPECT_EQ("", FormatProto("bytes_val", NullBytes(), &p));
  EXPECT_THAT(FormatProto("bytes_val", String("@"), &p),
              HasSubstr("out_of_range"));

  // fixed32_val
  EXPECT_EQ("fixed32_val: 5", FormatProto("fixed32_val", Uint32(5), &p));
  EXPECT_EQ("", FormatProto("fixed32_val", NullUint32(), &p));
  EXPECT_THAT(FormatProto("fixed32_val", Int32(0), &p),
              HasSubstr("out_of_range"));

  // fixed64_val
  EXPECT_EQ("fixed64_val: 5", FormatProto("fixed64_val", Uint64(5), &p));
  EXPECT_EQ("", FormatProto("fixed64_val", NullUint64(), &p));
  EXPECT_THAT(FormatProto("fixed64_val", Int64(0), &p),
              HasSubstr("out_of_range"));

  // sfixed32_val
  EXPECT_EQ("sfixed32_val: -5", FormatProto("sfixed32_val", Int32(-5), &p));
  EXPECT_EQ("", FormatProto("sfixed32_val", NullInt32(), &p));
  EXPECT_THAT(FormatProto("sfixed32_val", Uint32(0), &p),
              HasSubstr("out_of_range"));

  // sfixed64_val
  EXPECT_EQ("sfixed64_val: -5", FormatProto("sfixed64_val", Int64(-5), &p));
  EXPECT_EQ("", FormatProto("sfixed64_val", NullInt64(), &p));
  EXPECT_THAT(FormatProto("sfixed64_val", Uint64(0), &p),
              HasSubstr("out_of_range"));

  // sint32_val
  EXPECT_EQ("sint32_val: -5", FormatProto("sint32_val", Int32(-5), &p));
  EXPECT_EQ("", FormatProto("sint32_val", NullInt32(), &p));
  EXPECT_THAT(FormatProto("sint32_val", Uint32(0), &p),
              HasSubstr("out_of_range"));

  // sint64_val
  EXPECT_EQ("sint64_val: -5", FormatProto("sint64_val", Int64(-5), &p));
  EXPECT_EQ("", FormatProto("sint64_val", NullInt64(), &p));
  EXPECT_THAT(FormatProto("sint64_val", Uint64(0), &p),
              HasSubstr("out_of_range"));

  // Required field.
  EXPECT_EQ("int64_key_1: -5", FormatProto("int64_key_1", Int64(-5), &p));
  EXPECT_THAT(FormatProto("int64_key_1", NullInt64(), &p),
              HasSubstr("required"));

  // Several fields.
  EXPECT_EQ("int32_val: 1 uint32_val: 2 int64_val: 3 uint64_val: 4",
            FormatProto({
                {"int32_val", Int32(1)},
                {"uint32_val", Uint32(2)},
                {"int64_val", Int64(3)},
                {"uint64_val", Uint64(4)},
            }, &p));
}

TEST_F(ProtoEvalTest, CreateRepeatedProtoFields) {
  zetasql_test__::KitchenSinkPB p;

  // repeated_int32_val
  EXPECT_EQ("repeated_int32_val: -5 repeated_int32_val: -7", FormatProto(
      "repeated_int32_val", Array({Int32(-5), Int32(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_int32_val",
                            Value::EmptyArray(types::Int32ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_int32_val",
                            Value::Null(types::Int32ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_int32_val", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));
  EXPECT_THAT(FormatProto("repeated_int32_val", Int32(0), &p),
              HasSubstr("out_of_range"));

  // repeated_uint32_val
  EXPECT_EQ("repeated_uint32_val: 5 repeated_uint32_val: 7", FormatProto(
      "repeated_uint32_val", Array({Uint32(5), Uint32(7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_uint32_val",
                            Value::EmptyArray(types::Uint32ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_uint32_val",
                            Value::Null(types::Uint32ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_uint32_val", Array({Int32(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_int64_val
  EXPECT_EQ("repeated_int64_val: -5 repeated_int64_val: -7", FormatProto(
      "repeated_int64_val", Array({Int64(-5), Int64(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_int64_val",
                            Value::EmptyArray(types::Int64ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_int64_val",
                            Value::Null(types::Int64ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_int64_val", Array({Uint64(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_uint64_val
  EXPECT_EQ("repeated_uint64_val: 5 repeated_uint64_val: 7", FormatProto(
      "repeated_uint64_val", Array({Uint64(5), Uint64(7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_uint64_val",
                            Value::EmptyArray(types::Uint64ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_uint64_val",
                            Value::Null(types::Uint64ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_uint64_val", Array({Int64(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_bool_val
  EXPECT_EQ("repeated_bool_val: true repeated_bool_val: false", FormatProto(
      "repeated_bool_val", Array({True(), False()}), &p));
  EXPECT_EQ("", FormatProto("repeated_bool_val",
                            Value::EmptyArray(types::BoolArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_bool_val",
                            Value::Null(types::BoolArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_bool_val", Array({Int64(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_float_val
  EXPECT_EQ("repeated_float_val: 0.3 repeated_float_val: 0.5", FormatProto(
      "repeated_float_val", Array({Float(0.3), Float(0.5)}), &p));
  EXPECT_EQ("", FormatProto("repeated_float_val",
                            Value::EmptyArray(types::FloatArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_float_val",
                            Value::Null(types::FloatArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_float_val", Array({Double(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_double_val
  EXPECT_EQ("repeated_double_val: 0.3 repeated_double_val: 0.5", FormatProto(
      "repeated_double_val", Array({Double(0.3), Double(0.5)}), &p));
  EXPECT_EQ("", FormatProto("repeated_double_val",
                            Value::EmptyArray(types::DoubleArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_double_val",
                            Value::Null(types::DoubleArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_double_val", Array({Float(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_string_val
  EXPECT_EQ("repeated_string_val: \"@\" repeated_string_val: \"\"", FormatProto(
      "repeated_string_val", Array({String("@"), String("")}), &p));
  EXPECT_EQ("", FormatProto("repeated_string_val",
                            Value::EmptyArray(types::StringArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_string_val",
                            Value::Null(types::StringArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_string_val", Array({Bytes("")}), &p),
              HasSubstr("out_of_range"));

  // repeated_bytes_val
  EXPECT_EQ("repeated_bytes_val: \"@\" repeated_bytes_val: \"\"", FormatProto(
      "repeated_bytes_val", Array({Bytes("@"), Bytes("")}), &p));
  EXPECT_EQ("", FormatProto("repeated_bytes_val",
                            Value::EmptyArray(types::BytesArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_bytes_val",
                            Value::Null(types::BytesArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_bytes_val", Array({String("")}), &p),
              HasSubstr("out_of_range"));

  // repeated_fixed32_val
  EXPECT_EQ("repeated_fixed32_val: 5 repeated_fixed32_val: 7", FormatProto(
      "repeated_fixed32_val", Array({Uint32(5), Uint32(7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_fixed32_val",
                            Value::EmptyArray(types::Uint32ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_fixed32_val",
                            Value::Null(types::Uint32ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_fixed32_val", Array({Int32(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_fixed64_val
  EXPECT_EQ("repeated_fixed64_val: 5 repeated_fixed64_val: 7", FormatProto(
      "repeated_fixed64_val", Array({Uint64(5), Uint64(7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_fixed64_val",
                            Value::EmptyArray(types::Uint64ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_fixed64_val",
                            Value::Null(types::Uint64ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_fixed64_val", Array({Int64(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_sfixed32_val
  EXPECT_EQ("repeated_sfixed32_val: -5 repeated_sfixed32_val: -7", FormatProto(
      "repeated_sfixed32_val", Array({Int32(-5), Int32(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_sfixed32_val",
                            Value::EmptyArray(types::Int32ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_sfixed32_val",
                            Value::Null(types::Int32ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_sfixed32_val", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_sfixed64_val
  EXPECT_EQ("repeated_sfixed64_val: -5 repeated_sfixed64_val: -7", FormatProto(
      "repeated_sfixed64_val", Array({Int64(-5), Int64(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_sfixed64_val",
                            Value::EmptyArray(types::Int64ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_sfixed64_val",
                            Value::Null(types::Int64ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_sfixed64_val", Array({Uint64(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_sint32_val
  EXPECT_EQ("repeated_sint32_val: -5 repeated_sint32_val: -7", FormatProto(
      "repeated_sint32_val", Array({Int32(-5), Int32(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_sint32_val",
                            Value::EmptyArray(types::Int32ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_sint32_val",
                            Value::Null(types::Int32ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_sint32_val", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_sint64_val
  EXPECT_EQ("repeated_sint64_val: -5 repeated_sint64_val: -7", FormatProto(
      "repeated_sint64_val", Array({Int64(-5), Int64(-7)}), &p));
  EXPECT_EQ("", FormatProto("repeated_sint64_val",
                            Value::EmptyArray(types::Int64ArrayType()), &p));
  EXPECT_EQ("", FormatProto("repeated_sint64_val",
                            Value::Null(types::Int64ArrayType()), &p));
  EXPECT_THAT(FormatProto("repeated_sint64_val", Array({Uint64(0)}), &p),
              HasSubstr("out_of_range"));
}

TEST_F(ProtoEvalTest, CreateNestedProtoFields) {
  zetasql_test__::KitchenSinkPB p;
  zetasql_test__::KitchenSinkPB::Nested nested_pb;
  Value nested_value1 = MakeProtoValue({{"nested_int64", Int64(5)}}, nested_pb);
  Value nested_value2 = MakeProtoValue({{"nested_int64", Int64(7)}}, nested_pb);

  // nested_value
  EXPECT_EQ("nested_value { nested_int64: 5 }",
            FormatProto("nested_value", nested_value1, &p));
  EXPECT_THAT(FormatProto("nested_value", Uint32(0), &p),
              HasSubstr("out_of_range"));

  // nested_repeated_value
  EXPECT_EQ("nested_repeated_value { nested_int64: 5 } "
            "nested_repeated_value { nested_int64: 7 }", FormatProto(
      "nested_repeated_value", Array({nested_value1, nested_value2}), &p));
  EXPECT_THAT(FormatProto("nested_repeated_value", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));

  // repeated_holder
  zetasql_test__::RepeatedHolderPB holder_pb;
  zetasql_test__::TestExtraPB extra_pb;
  Value holder_value = MakeProtoValue({{
      "repeated_field", Array({MakeProtoValue({{
           "int32_val1", Int32(5)}},
           extra_pb)})}},
      holder_pb);
  EXPECT_EQ("repeated_holder { repeated_field { int32_val1: 5 } } "
            "repeated_holder { repeated_field { int32_val1: 5 } }", FormatProto(
      "repeated_holder", Array({holder_value, holder_value}), &p));
  EXPECT_EQ("", FormatProto(
      "repeated_holder",
      Value::EmptyArray(Array({holder_value}).type()->AsArray()), &p));
  EXPECT_EQ("", FormatProto(
      "repeated_holder",
      Value::Null(Array({holder_value}).type()->AsArray()), &p));
  EXPECT_THAT(FormatProto("repeated_holder", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));

  // optional_group
  zetasql_test__::KitchenSinkPB_OptionalGroup group_pb;
  Value group_value = MakeProtoValue({{"int64_val", Int64(5)}}, group_pb);
  EXPECT_EQ("optional_group { int64_val: 5 }",
            FormatProto("optional_group", group_value, &p));
  EXPECT_THAT(FormatProto("optional_group", Uint32(0), &p),
              HasSubstr("out_of_range"));

  // nested_repeated_group
  zetasql_test__::KitchenSinkPB_NestedRepeatedGroup nested_group_pb;
  Value nested_group1 = MakeProtoValue({{"id", Int64(5)}}, nested_group_pb);
  Value nested_group2 = MakeProtoValue({{"id", Int64(7)}}, nested_group_pb);
  EXPECT_EQ("nested_repeated_group { id: 5 } "
            "nested_repeated_group { id: 7 }", FormatProto(
      "nested_repeated_group", Array({nested_group1, nested_group2}), &p));
  EXPECT_EQ("", FormatProto(
      "nested_repeated_group",
      Value::EmptyArray(Array({nested_group1}).type()->AsArray()), &p));
  EXPECT_EQ("", FormatProto(
      "nested_repeated_group",
      Value::Null(Array({nested_group1}).type()->AsArray()), &p));
  EXPECT_THAT(FormatProto("nested_repeated_group", Array({Uint32(0)}), &p),
              HasSubstr("out_of_range"));
}

TEST_F(ProtoEvalTest, CreateEnumProtoFields) {
  zetasql_test__::KitchenSinkPB p;
  const EnumType* enum_type;
  ZETASQL_CHECK_OK(type_factory_->MakeEnumType(
      zetasql_test__::TestEnum_descriptor(), &enum_type));

  // test_enum
  EXPECT_EQ("test_enum: TESTENUM1",
            FormatProto("test_enum", Value::Enum(enum_type, 1), &p));
  EXPECT_EQ("", FormatProto("test_enum", Value::Null(enum_type), &p));
  EXPECT_THAT(FormatProto("test_enum", Int32(1), &p),
              HasSubstr("out_of_range"));

  // repeated_test_enum
  EXPECT_EQ("repeated_test_enum: TESTENUM0 repeated_test_enum: TESTENUM1",
            FormatProto("repeated_test_enum", Array({
                Value::Enum(enum_type, 0), Value::Enum(enum_type, 1)}), &p));
  const ArrayType* enum_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(enum_type, &enum_array_type));
  EXPECT_EQ("", FormatProto("repeated_test_enum",
                            Value::EmptyArray(enum_array_type), &p));
  EXPECT_EQ("", FormatProto("repeated_test_enum",
                            Value::Null(enum_array_type), &p));
  EXPECT_THAT(FormatProto("repeated_test_enum", Array({Int32(0)}), &p),
              HasSubstr("out_of_range"));
}

// Note: the Write methods in proto_util.cc do not check proto annotations,
// i.e., they allow storing integers in timestamp and date fields as well as
// timestamps in integer fields. However, ZetaSQL analyzer will reject such
// use. The test below purposefully does not attempt to exercise such scenarios.
TEST_F(ProtoEvalTest, CreateDateTimeProtoFields) {
  zetasql_test__::KitchenSinkPB p;
  // date
  EXPECT_EQ("date: 5", FormatProto("date", Date(5), &p));
  EXPECT_EQ("", FormatProto("date", NullDate(), &p));
  EXPECT_THAT(FormatProto("date", String(""), &p), HasSubstr("out_of_range"));

  // date64
  EXPECT_EQ("date64: 5", FormatProto("date64", Date(5), &p));
  EXPECT_EQ("", FormatProto("date64", NullDate(), &p));
  EXPECT_THAT(FormatProto("date64", String(""), &p), HasSubstr("out_of_range"));

  // TODO: add tests for date_decimal once implemented.

  // timestamp_seconds
  EXPECT_EQ("timestamp_seconds: 5",
            FormatProto("timestamp_seconds", TimestampFromUnixMicros(5000000),
                        &p));
  EXPECT_EQ("", FormatProto("timestamp_seconds", NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_seconds", String(""), &p),
              HasSubstr("out_of_range"));

  EXPECT_EQ("timestamp_seconds_format: 5",
            FormatProto("timestamp_seconds_format",
                        TimestampFromUnixMicros(5000000), &p));
  EXPECT_EQ("", FormatProto("timestamp_seconds_format", NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_seconds_format", String(""), &p),
              HasSubstr("out_of_range"));

  // timestamp_millis
  EXPECT_EQ("timestamp_millis: 5",
            FormatProto("timestamp_millis", TimestampFromUnixMicros(5000), &p));
  EXPECT_EQ("", FormatProto("timestamp_millis", NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_millis", String(""), &p),
              HasSubstr("out_of_range"));

  EXPECT_EQ("timestamp_millis_format: 5",
            FormatProto("timestamp_millis_format",
                        TimestampFromUnixMicros(5000), &p));
  EXPECT_EQ("", FormatProto("timestamp_millis_format",
                            NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_millis_format", String(""), &p),
              HasSubstr("out_of_range"));

  // timestamp_micros
  EXPECT_EQ("timestamp_micros: 5",
            FormatProto("timestamp_micros", TimestampFromUnixMicros(5), &p));
  EXPECT_EQ("", FormatProto("timestamp_micros", NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_micros", Uint32(1), &p),
              HasSubstr("out_of_range"));

  EXPECT_EQ("timestamp_micros_format: 5",
            FormatProto("timestamp_micros_format",
                        TimestampFromUnixMicros(5), &p));
  EXPECT_EQ("", FormatProto("timestamp_micros_format",
                            NullTimestamp(), &p));
  EXPECT_THAT(FormatProto("timestamp_micros_format", Uint32(1), &p),
              HasSubstr("out_of_range"));

  // repeated_date
  EXPECT_EQ("repeated_date: 5 repeated_date: 7", FormatProto(
      "repeated_date", Array({Date(5), Date(7)}), &p));
  EXPECT_THAT(FormatProto("repeated_date", Array({String("")}), &p),
              HasSubstr("out_of_range"));

  // repeated_date64
  EXPECT_EQ("repeated_date64: 5 repeated_date64: 7", FormatProto(
      "repeated_date64", Array({Date(5), Date(7)}), &p));
  EXPECT_THAT(FormatProto("repeated_date64", Array({String("")}), &p),
              HasSubstr("out_of_range"));

  // repeated_timestamp_seconds
  EXPECT_EQ("repeated_timestamp_seconds: 5 repeated_timestamp_seconds: 7",
            FormatProto("repeated_timestamp_seconds",
                        Array({TimestampFromUnixMicros(5000000),
                               TimestampFromUnixMicros(7000000)}), &p));
  EXPECT_THAT(FormatProto("repeated_timestamp_seconds",
                          Array({String("")}), &p), HasSubstr("out_of_range"));

  // repeated_timestamp_millis
  EXPECT_EQ("repeated_timestamp_millis: 5 repeated_timestamp_millis: 7",
            FormatProto("repeated_timestamp_millis",
                        Array({TimestampFromUnixMicros(5000),
                               TimestampFromUnixMicros(7000)}), &p));
  EXPECT_THAT(FormatProto("repeated_timestamp_millis",
                          Array({String("")}), &p), HasSubstr("out_of_range"));

  // repeated_timestamp_micros
  EXPECT_EQ("repeated_timestamp_micros: 5 repeated_timestamp_micros: 7",
            FormatProto("repeated_timestamp_micros",
                        Array({TimestampFromUnixMicros(5),
                               TimestampFromUnixMicros(7)}), &p));
  EXPECT_THAT(FormatProto("repeated_timestamp_micros",
                          Array({String("")}), &p), HasSubstr("out_of_range"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprPrimitiveProtoFields) {
  zetasql_test__::KitchenSinkPB p;

  // int32_val: default 77
  EXPECT_EQ(Value::Int32(77), GetProtoFieldOrDie(&p, "int32_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "int32_val"));
  p.set_int32_val(123);
  EXPECT_EQ(Value::Int32(123), GetProtoFieldOrDie(&p, "int32_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "int32_val"));

  // uint32_val: default 777
  EXPECT_EQ(Value::Uint32(777), GetProtoFieldOrDie(&p, "uint32_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "uint32_val"));
  p.set_uint32_val(124);
  EXPECT_EQ(Value::Uint32(124), GetProtoFieldOrDie(&p, "uint32_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "uint32_val"));

  // int64_val: default 0
  EXPECT_EQ(Value::Int64(0), GetProtoFieldOrDie(&p, "int64_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "int64_val"));
  p.set_int64_val(-125);
  EXPECT_EQ(Value::Int64(-125), GetProtoFieldOrDie(&p, "int64_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "int64_val"));

  // uint64_val: default 0
  EXPECT_EQ(Value::Uint64(0), GetProtoFieldOrDie(&p, "uint64_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "uint64_val"));
  p.set_uint64_val(125);
  EXPECT_EQ(Value::Uint64(125), GetProtoFieldOrDie(&p, "uint64_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "uint64_val"));

  // string_val: default "default_name"
  EXPECT_EQ(Value::String("default_name"),
            GetProtoFieldOrDie(&p, "string_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "string_val"));
  p.set_string_val("foo");
  EXPECT_EQ(Value::String("foo"), GetProtoFieldOrDie(&p, "string_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "string_val"));

  EXPECT_EQ(Value::Float(0), GetProtoFieldOrDie(&p, "float_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "float_val"));
  p.set_float_val(125.5);
  EXPECT_EQ(Value::Float(125.5), GetProtoFieldOrDie(&p, "float_val"));

  EXPECT_EQ(Value::Double(0), GetProtoFieldOrDie(&p, "double_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "double_val"));
  p.set_double_val(125.7);
  EXPECT_EQ(Value::Double(125.7), GetProtoFieldOrDie(&p, "double_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "double_val"));

  EXPECT_EQ(Value::Bytes(""), GetProtoFieldOrDie(&p, "bytes_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "bytes_val"));
  p.set_bytes_val("bar");
  EXPECT_EQ(Value::Bytes("bar"), GetProtoFieldOrDie(&p, "bytes_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "bytes_val"));

  EXPECT_EQ(Value::Bool(false), GetProtoFieldOrDie(&p, "bool_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "bool_val"));
  p.set_bool_val(true);
  EXPECT_EQ(Value::Bool(true), GetProtoFieldOrDie(&p, "bool_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "bool_val"));

  EXPECT_EQ(Value::Uint32(0), GetProtoFieldOrDie(&p, "fixed32_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "fixed32_val"));
  p.set_fixed32_val(129);
  EXPECT_EQ(Value::Uint32(129), GetProtoFieldOrDie(&p, "fixed32_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "fixed32_val"));

  EXPECT_EQ(Value::Uint64(0), GetProtoFieldOrDie(&p, "fixed64_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "fixed64_val"));
  p.set_fixed64_val(129);
  EXPECT_EQ(Value::Uint64(129), GetProtoFieldOrDie(&p, "fixed64_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "fixed64_val"));

  EXPECT_EQ(Value::Int32(0), GetProtoFieldOrDie(&p, "sint32_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "sint32_val"));
  p.set_sint32_val(-135);
  EXPECT_EQ(Value::Int32(-135), GetProtoFieldOrDie(&p, "sint32_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "sint32_val"));

  EXPECT_EQ(Value::Int64(0), GetProtoFieldOrDie(&p, "sint64_val"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "sint64_val"));
  p.set_sint64_val(-135);
  EXPECT_EQ(Value::Int64(-135), GetProtoFieldOrDie(&p, "sint64_val"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "sint64_val"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprRepeatedProtoFields) {
  zetasql_test__::KitchenSinkPB p;

  // Repeated fields.
  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_int32_val"));
  p.add_repeated_int32_val(140);
  p.add_repeated_int32_val(141);
  EXPECT_EQ(Int32Array({140, 141}),
            GetProtoFieldOrDie(&p, "repeated_int32_val"));

  EXPECT_EQ(Uint32Array({}), GetProtoFieldOrDie(&p, "repeated_uint32_val"));
  p.add_repeated_uint32_val(150);
  p.add_repeated_uint32_val(151);
  EXPECT_EQ(Uint32Array({150, 151}),
            GetProtoFieldOrDie(&p, "repeated_uint32_val"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_int64_val"));
  p.add_repeated_int64_val(160);
  p.add_repeated_int64_val(161);
  EXPECT_EQ(Int64Array({160, 161}),
            GetProtoFieldOrDie(&p, "repeated_int64_val"));

  EXPECT_EQ(Uint64Array({}), GetProtoFieldOrDie(&p, "repeated_uint64_val"));
  p.add_repeated_uint64_val(170);
  p.add_repeated_uint64_val(171);
  EXPECT_EQ(Uint64Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_uint64_val"));

  EXPECT_EQ(StringArray(std::vector<std::string>()),
            GetProtoFieldOrDie(&p, "repeated_string_val"));
  p.add_repeated_string_val("s1");
  p.add_repeated_string_val("s2");
  EXPECT_EQ(StringArray({"s1", "s2"}),
            GetProtoFieldOrDie(&p, "repeated_string_val"));

  EXPECT_EQ(FloatArray({}), GetProtoFieldOrDie(&p, "repeated_float_val"));
  p.add_repeated_float_val(80.5);
  p.add_repeated_float_val(81.5);
  EXPECT_EQ(FloatArray({80.5, 81.5}),
            GetProtoFieldOrDie(&p, "repeated_float_val"));

  EXPECT_EQ(DoubleArray({}), GetProtoFieldOrDie(&p, "repeated_double_val"));
  p.add_repeated_double_val(8.5);
  p.add_repeated_double_val(9.5);
  EXPECT_EQ(DoubleArray({8.5, 9.5}),
            GetProtoFieldOrDie(&p, "repeated_double_val"));

  EXPECT_EQ(BytesArray(std::vector<std::string>()),
            GetProtoFieldOrDie(&p, "repeated_bytes_val"));
  p.add_repeated_bytes_val("b1");
  p.add_repeated_bytes_val("b2");
  EXPECT_EQ(BytesArray({"b1", "b2"}),
            GetProtoFieldOrDie(&p, "repeated_bytes_val"));

  EXPECT_EQ(BoolArray({}), GetProtoFieldOrDie(&p, "repeated_bool_val"));
  p.add_repeated_bool_val(true);
  p.add_repeated_bool_val(false);
  EXPECT_EQ(BoolArray({true, false}),
            GetProtoFieldOrDie(&p, "repeated_bool_val"));

  EXPECT_EQ(Uint32Array({}), GetProtoFieldOrDie(&p, "repeated_fixed32_val"));
  p.add_repeated_fixed32_val(160);
  p.add_repeated_fixed32_val(161);
  EXPECT_EQ(Uint32Array({160, 161}),
            GetProtoFieldOrDie(&p, "repeated_fixed32_val"));

  EXPECT_EQ(Uint64Array({}), GetProtoFieldOrDie(&p, "repeated_fixed64_val"));
  p.add_repeated_fixed64_val(170);
  p.add_repeated_fixed64_val(171);
  EXPECT_EQ(Uint64Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_fixed64_val"));

  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_sfixed32_val"));
  p.add_repeated_sfixed32_val(170);
  p.add_repeated_sfixed32_val(171);
  EXPECT_EQ(Int32Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_sfixed32_val"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_sfixed64_val"));
  p.add_repeated_sfixed64_val(180);
  p.add_repeated_sfixed64_val(181);
  EXPECT_EQ(Int64Array({180, 181}),
            GetProtoFieldOrDie(&p, "repeated_sfixed64_val"));

  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_sint32_val"));
  p.add_repeated_sint32_val(190);
  p.add_repeated_sint32_val(191);
  EXPECT_EQ(Int32Array({190, 191}),
            GetProtoFieldOrDie(&p, "repeated_sint32_val"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_sint64_val"));
  p.add_repeated_sint64_val(200);
  p.add_repeated_sint64_val(201);
  EXPECT_EQ(Int64Array({200, 201}),
            GetProtoFieldOrDie(&p, "repeated_sint64_val"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprPackedProtoFields) {
  zetasql_test__::KitchenSinkPB p;

  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_int32_packed"));
  p.add_repeated_int32_packed(140);
  p.add_repeated_int32_packed(141);
  EXPECT_EQ(Int32Array({140, 141}),
            GetProtoFieldOrDie(&p, "repeated_int32_packed"));

  EXPECT_EQ(Uint32Array({}), GetProtoFieldOrDie(&p, "repeated_uint32_packed"));
  p.add_repeated_uint32_packed(150);
  p.add_repeated_uint32_packed(151);
  EXPECT_EQ(Uint32Array({150, 151}),
            GetProtoFieldOrDie(&p, "repeated_uint32_packed"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_int64_packed"));
  p.add_repeated_int64_packed(160);
  p.add_repeated_int64_packed(161);
  EXPECT_EQ(Int64Array({160, 161}),
            GetProtoFieldOrDie(&p, "repeated_int64_packed"));

  EXPECT_EQ(Uint64Array({}), GetProtoFieldOrDie(&p, "repeated_uint64_packed"));
  p.add_repeated_uint64_packed(170);
  p.add_repeated_uint64_packed(171);
  EXPECT_EQ(Uint64Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_uint64_packed"));

  EXPECT_EQ(FloatArray({}), GetProtoFieldOrDie(&p, "repeated_float_packed"));
  p.add_repeated_float_packed(80.5);
  p.add_repeated_float_packed(81.5);
  EXPECT_EQ(FloatArray({80.5, 81.5}),
            GetProtoFieldOrDie(&p, "repeated_float_packed"));

  EXPECT_EQ(DoubleArray({}), GetProtoFieldOrDie(&p, "repeated_double_packed"));
  p.add_repeated_double_packed(8.5);
  p.add_repeated_double_packed(9.5);
  EXPECT_EQ(DoubleArray({8.5, 9.5}),
            GetProtoFieldOrDie(&p, "repeated_double_packed"));

  EXPECT_EQ(BoolArray({}), GetProtoFieldOrDie(&p, "repeated_bool_packed"));
  p.add_repeated_bool_packed(true);
  p.add_repeated_bool_packed(false);
  EXPECT_EQ(BoolArray({true, false}),
            GetProtoFieldOrDie(&p, "repeated_bool_packed"));

  EXPECT_EQ(Uint32Array({}), GetProtoFieldOrDie(&p, "repeated_fixed32_packed"));
  p.add_repeated_fixed32_packed(160);
  p.add_repeated_fixed32_packed(161);
  EXPECT_EQ(Uint32Array({160, 161}),
            GetProtoFieldOrDie(&p, "repeated_fixed32_packed"));

  EXPECT_EQ(Uint64Array({}), GetProtoFieldOrDie(&p, "repeated_fixed64_packed"));
  p.add_repeated_fixed64_packed(170);
  p.add_repeated_fixed64_packed(171);
  EXPECT_EQ(Uint64Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_fixed64_packed"));

  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_sfixed32_packed"));
  p.add_repeated_sfixed32_packed(170);
  p.add_repeated_sfixed32_packed(171);
  EXPECT_EQ(Int32Array({170, 171}),
            GetProtoFieldOrDie(&p, "repeated_sfixed32_packed"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_sfixed64_packed"));
  p.add_repeated_sfixed64_packed(180);
  p.add_repeated_sfixed64_packed(181);
  EXPECT_EQ(Int64Array({180, 181}),
            GetProtoFieldOrDie(&p, "repeated_sfixed64_packed"));

  EXPECT_EQ(Int32Array({}), GetProtoFieldOrDie(&p, "repeated_sint32_packed"));
  p.add_repeated_sint32_packed(190);
  p.add_repeated_sint32_packed(191);
  EXPECT_EQ(Int32Array({190, 191}),
            GetProtoFieldOrDie(&p, "repeated_sint32_packed"));

  EXPECT_EQ(Int64Array({}), GetProtoFieldOrDie(&p, "repeated_sint64_packed"));
  p.add_repeated_sint64_packed(200);
  p.add_repeated_sint64_packed(201);
  EXPECT_EQ(Int64Array({200, 201}),
            GetProtoFieldOrDie(&p, "repeated_sint64_packed"));

  const EnumType* enum_type;
  ZETASQL_CHECK_OK(type_factory_->MakeEnumType(
      zetasql_test__::TestEnum_descriptor(), &enum_type));
  const ArrayType* enum_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(enum_type, &enum_array_type));
  EXPECT_EQ(Value::EmptyArray(enum_array_type),
            GetProtoFieldOrDie(&p, "repeated_enum_packed"));
  p.add_repeated_enum_packed(zetasql_test__::TESTENUM2);
  p.add_repeated_enum_packed(zetasql_test__::TESTENUM1);
  EXPECT_EQ(
      Value::Array(enum_array_type, {Value::Enum(enum_type, "TESTENUM2"),
                                     Value::Enum(enum_type, "TESTENUM1")}),
      GetProtoFieldOrDie(&p, "repeated_enum_packed"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprProtosEnums) {
  zetasql_test__::KitchenSinkPB p;

  zetasql_test__::KitchenSinkPB::Nested nested_tmp;
  const ProtoType* nested_type = MakeProtoType(&nested_tmp);
  EXPECT_EQ(Value::Null(nested_type), GetProtoFieldOrDie(&p, "nested_value"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "nested_value"));
  EXPECT_EQ(Value::NullInt64(),
            GetProtoFieldOrDie(GetProtoFieldOrDie(&p, "nested_value"),
                               "nested_int64"));
  EXPECT_EQ(Value::NullBool(),
            HasProtoFieldOrDie(GetProtoFieldOrDie(&p, "nested_value"),
                               "nested_int64"));
  zetasql_test__::KitchenSinkPB::Nested* n = p.mutable_nested_value();
  // Default: 88.
  EXPECT_EQ(Value::Int64(88), GetProtoFieldOrDie(n, "nested_int64"));
  n->add_nested_repeated_int64(300);
  n->add_nested_repeated_int64(301);
  ASSERT_TRUE(ParseFromCord(GetProtoFieldOrDie(&p, "nested_value").ToCord(),
                            &nested_tmp));
  EXPECT_EQ(n->DebugString(), nested_tmp.DebugString());
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "nested_value"));

  const ArrayType* nested_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(nested_type, &nested_array_type));
  EXPECT_EQ(Value::Array(nested_array_type, {}),
            GetProtoFieldOrDie(&p, "nested_repeated_value"));
  p.add_nested_repeated_value()->CopyFrom(*n);
  p.add_nested_repeated_value()->CopyFrom(*n);
  absl::Cord nested_bytes = SerializePartialToCord(*n);
  EXPECT_EQ(Value::Array(nested_array_type,
                         {Value::Proto(nested_type, nested_bytes),
                          Value::Proto(nested_type, nested_bytes)}),
            GetProtoFieldOrDie(&p, "nested_repeated_value"));

  const EnumType* enum_type;
  ZETASQL_CHECK_OK(type_factory_->MakeEnumType(
      zetasql_test__::TestEnum_descriptor(), &enum_type));
  EXPECT_EQ(Value::Enum(enum_type, "TESTENUM0"),
            GetProtoFieldOrDie(&p, "test_enum"));  // Default value.
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "test_enum"));
  p.set_test_enum(zetasql_test__::TESTENUM1);
  EXPECT_EQ(Value::Enum(enum_type, "TESTENUM1"),
            GetProtoFieldOrDie(&p, "test_enum"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "test_enum"));

  const ArrayType* enum_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(enum_type, &enum_array_type));
  EXPECT_EQ(Value::EmptyArray(enum_array_type),
            GetProtoFieldOrDie(&p, "repeated_test_enum"));
  p.add_repeated_test_enum(zetasql_test__::TESTENUM2);
  p.add_repeated_test_enum(zetasql_test__::TESTENUM1);
  EXPECT_EQ(
      Value::Array(enum_array_type, {Value::Enum(enum_type, "TESTENUM2"),
                                     Value::Enum(enum_type, "TESTENUM1")}),
      GetProtoFieldOrDie(&p, "repeated_test_enum"));

  // optional_group
  zetasql_test__::KitchenSinkPB::OptionalGroup group_tmp;
  const ProtoType* group_type = MakeProtoType(&group_tmp);
  EXPECT_EQ(Value::Null(group_type), GetProtoFieldOrDie(&p, "optional_group"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "optional_group"));
  EXPECT_EQ(Value::NullInt64(),
            GetProtoFieldOrDie(GetProtoFieldOrDie(&p, "optional_group"),
                               "int64_val"));
  zetasql_test__::KitchenSinkPB::OptionalGroup* g = p.mutable_optional_group();
  g->set_int64_val(500);
  g->add_optionalgroupnested()->set_int64_val(510);
  ASSERT_TRUE(ParseFromCord(GetProtoFieldOrDie(&p, "optional_group").ToCord(),
                            &group_tmp));
  EXPECT_EQ(g->DebugString(), group_tmp.DebugString());
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "optional_group"));

  // nested_repeated_group
  zetasql_test__::KitchenSinkPB::NestedRepeatedGroup repeated_group_tmp;
  const ProtoType* repeated_group_type = MakeProtoType(&repeated_group_tmp);
  const ArrayType* repeated_group_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(
      repeated_group_type, &repeated_group_array_type));
  EXPECT_EQ(Value::Array(repeated_group_array_type, {}),
            GetProtoFieldOrDie(&p, "nested_repeated_group"));
  p.add_nested_repeated_group()->set_id(600);
  p.add_nested_repeated_group()->add_nestedrepeatedgroupnested()->set_id(610);

  absl::Cord group_bytes0 = SerializePartialToCord(p.nested_repeated_group(0));
  absl::Cord group_bytes1 = SerializePartialToCord(p.nested_repeated_group(1));
  EXPECT_EQ(Value::Array(repeated_group_array_type,
                         {Value::Proto(repeated_group_type, group_bytes0),
                          Value::Proto(repeated_group_type, group_bytes1)}),
            GetProtoFieldOrDie(&p, "nested_repeated_group"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprDateTime) {
  zetasql_test__::KitchenSinkPB p;
  EXPECT_EQ(Value::Date(0), GetProtoFieldOrDie(&p, "date"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "date"));
  p.set_date(types::kDateMax);
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "date"));
  EXPECT_EQ(Value::Date(types::kDateMax), GetProtoFieldOrDie(&p, "date"));
  p.set_date(types::kDateMin);
  EXPECT_EQ(Value::Date(types::kDateMin), GetProtoFieldOrDie(&p, "date"));
  // Out-of-bounds date value produces an error.
  p.set_date(types::kDateMax + 1);
  EXPECT_THAT(GetProtoField(&p, "date"),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Corrupted protocol buffer")));
  p.set_date(types::kDateMin - 1);
  EXPECT_THAT(GetProtoField(&p, "date"),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Corrupted protocol buffer")));

  EXPECT_EQ(Value::Date(0), GetProtoFieldOrDie(&p, "date64"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "date64"));
  p.set_date64(102031);
  EXPECT_EQ(Value::Date(102031), GetProtoFieldOrDie(&p, "date64"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "date64"));

  EXPECT_EQ(Value::TimestampFromUnixMicros(0),
            GetProtoFieldOrDie(&p, "timestamp_seconds"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "timestamp_seconds"));
  p.set_timestamp_seconds(types::kTimestampSecondsMin);
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "timestamp_seconds"));
  EXPECT_EQ(
      Value::TimestampFromUnixMicros(types::kTimestampSecondsMin * 1000000),
      GetProtoFieldOrDie(&p, "timestamp_seconds"));
  p.set_timestamp_seconds(types::kTimestampSecondsMax);
  EXPECT_EQ(
      Value::TimestampFromUnixMicros(types::kTimestampSecondsMax * 1000000),
      GetProtoFieldOrDie(&p, "timestamp_seconds"));
  // Out-of-bounds timestamp seconds value produces an error.
  p.set_timestamp_seconds(types::kTimestampMin / 1000000 - 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_seconds").ok());
  p.set_timestamp_seconds(types::kTimestampMax / 1000000 + 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_seconds").ok());

  EXPECT_EQ(Value::TimestampFromUnixMicros(0),
            GetProtoFieldOrDie(&p, "timestamp_millis"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "timestamp_millis"));
  p.set_timestamp_millis(types::kTimestampMillisMin);
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "timestamp_millis"));
  EXPECT_EQ(Value::TimestampFromUnixMicros(types::kTimestampMillisMin * 1000),
            GetProtoFieldOrDie(&p, "timestamp_millis"));
  p.set_timestamp_millis(types::kTimestampMillisMax);
  EXPECT_EQ(Value::TimestampFromUnixMicros(types::kTimestampMillisMax * 1000),
            GetProtoFieldOrDie(&p, "timestamp_millis"));
  // Out-of-bounds timestamp millis value produces an error.
  p.set_timestamp_millis(types::kTimestampMin / 1000 - 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_millis").ok());
  p.set_timestamp_millis(types::kTimestampMax / 1000 + 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_millis").ok());

  EXPECT_EQ(Value::TimestampFromUnixMicros(0),
            GetProtoFieldOrDie(&p, "timestamp_micros"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "timestamp_micros"));
  p.set_timestamp_micros(types::kTimestampMicrosMin);
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "timestamp_micros"));
  EXPECT_EQ(Value::TimestampFromUnixMicros(types::kTimestampMicrosMin),
            GetProtoFieldOrDie(&p, "timestamp_micros"));
  p.set_timestamp_micros(types::kTimestampMicrosMax);
  EXPECT_EQ(Value::TimestampFromUnixMicros(types::kTimestampMicrosMax),
            GetProtoFieldOrDie(&p, "timestamp_micros"));
  // Out-of-bounds timestamp value produces an error.
  p.set_timestamp_micros(types::kTimestampMin - 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_micros").ok());
  p.set_timestamp_micros(types::kTimestampMax + 1);
  EXPECT_FALSE(GetProtoField(&p, "timestamp_micros").ok());
}

TEST_F(ProtoEvalTest, GetProtoFieldExprDefaultAnnotations) {
  zetasql_test__::KitchenSinkPB p;
  EXPECT_EQ(Value::Int64(0), GetProtoFieldOrDie(&p, "int_with_no_default"));
  EXPECT_EQ(Value::Int64(17), GetProtoFieldOrDie(&p, "int_with_default"));
  EXPECT_EQ(Value::NullInt64(),
            GetProtoFieldOrDie(&p, "int_with_no_default_nullable"));
  EXPECT_EQ(Value::NullInt64(),
            GetProtoFieldOrDie(&p, "int_with_default_nullable"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "int_with_no_default"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "int_with_default"));
  EXPECT_EQ(Value::Bool(false),
            HasProtoFieldOrDie(&p, "int_with_no_default_nullable"));
  EXPECT_EQ(Value::Bool(false),
            HasProtoFieldOrDie(&p, "int_with_default_nullable"));
  // Field starting with "has_" works fine assuming the method access is
  // disambiguated properly from the field access.
  EXPECT_EQ(Value::String(""), GetProtoFieldOrDie(&p, "has_confusing_name"));
  EXPECT_EQ(Value::Bool(false), HasProtoFieldOrDie(&p, "has_confusing_name"));
  p.set_has_confusing_name("no");
  EXPECT_EQ(Value::String("no"), GetProtoFieldOrDie(&p, "has_confusing_name"));
  EXPECT_EQ(Value::Bool(true), HasProtoFieldOrDie(&p, "has_confusing_name"));
}

TEST_F(ProtoEvalTest, GetProtoFieldExprLastFieldOccurrence) {
  // The latest value with the given tag is the one than matters.
  zetasql_test__::KitchenSinkPB p;

  p.set_int32_val(5);
  absl::Cord bytes1 = SerializePartialToCord(p);
  p.set_int32_val(7);
  absl::Cord bytes2 = SerializePartialToCord(p);
  absl::Cord bytes;
  bytes.Append(bytes1);
  bytes.Append(bytes2);
  Value proto_value = Value::Proto(MakeProtoType(&p), bytes);
  EXPECT_EQ(Value::Int32(7), GetProtoFieldOrDie(proto_value, "int32_val"));
  // Proto API has the same behavior.
  p.Clear();
  ASSERT_TRUE(ParsePartialFromCord(proto_value.ToCord(), &p));
  EXPECT_EQ(7, p.int32_val());
}

TEST_F(ProtoEvalTest, GetProtoFieldExprsMultipleFieldsMultipleRows) {
  // Create two ProtoFieldInfos. For the first one, create a single
  // ProtoFieldReader used by two GetProtoFieldExpr nodes. For the second one,
  // create a second ProtoFieldReader (and a corresponding GetProtoFieldExpr
  // node). Evaluate all three GetProtoFieldExpr nodes on three protos to
  // ensure the ProtoFieldValueMap is used in the appropriate ways.
  for (bool use_shared_states : {false, true}) {
    ZETASQL_LOG(INFO) << "use_shared_states: " << use_shared_states;
    zetasql_test__::KitchenSinkPB p1;
    p1.set_int64_key_1(1);
    p1.set_int64_key_2(2);
    zetasql_test__::KitchenSinkPB_Nested* nested1 = p1.mutable_nested_value();
    nested1->set_nested_int64(3);

    zetasql_test__::KitchenSinkPB p2;
    p2.set_int64_key_1(10);
    p2.set_int64_key_2(20);
    zetasql_test__::KitchenSinkPB_Nested* nested2 = p2.mutable_nested_value();
    nested2->set_nested_int64(30);

    zetasql_test__::KitchenSinkPB p3;
    p3.set_int64_key_1(100);
    p3.set_int64_key_2(200);
    zetasql_test__::KitchenSinkPB_Nested* nested3 = p3.mutable_nested_value();
    nested3->set_nested_int64(300);

    absl::Cord bytes1 = SerializeToCord(p1);
    absl::Cord bytes2 = SerializeToCord(p2);
    absl::Cord bytes3 = SerializeToCord(p3);

    absl::Cord nested_bytes1 = SerializeToCord(*nested1);
    absl::Cord nested_bytes2 = SerializeToCord(*nested2);
    absl::Cord nested_bytes3 = SerializeToCord(*nested3);

    const std::vector<Value> v = {Value::Proto(MakeProtoType(&p1), bytes1),
                                  Value::Proto(MakeProtoType(&p2), bytes2),
                                  Value::Proto(MakeProtoType(&p3), bytes3)};

    const Value nested_value1 =
        Value::Proto(MakeProtoType(nested1), nested_bytes1);
    const Value nested_value2 =
        Value::Proto(MakeProtoType(nested2), nested_bytes2);
    const Value nested_value3 =
        Value::Proto(MakeProtoType(nested3), nested_bytes3);

    TupleSlot s1;
    s1.SetValue(v[0]);

    TupleSlot s2;
    s2.SetValue(v[1]);

    TupleSlot s3;
    s3.SetValue(v[2]);

    ProtoFieldAccessInfo access_info1;
    ProtoFieldInfo* info1 = &access_info1.field_info;
    info1->descriptor = p1.GetDescriptor()->FindFieldByName("int64_key_1");
    ZETASQL_ASSERT_OK(GetProtoFieldTypeAndDefault(info1->descriptor, type_factory_,
                                          &info1->type, &info1->default_value));
    info1->format = ProtoType::GetFormatAnnotation(info1->descriptor);

    ProtoFieldAccessInfo access_info2;
    ProtoFieldInfo* info2 = &access_info2.field_info;
    info2->descriptor = p2.GetDescriptor()->FindFieldByName("nested_value");
    ZETASQL_ASSERT_OK(GetProtoFieldTypeAndDefault(info2->descriptor, type_factory_,
                                          &info2->type, &info2->default_value));
    info2->format = ProtoType::GetFormatAnnotation(info2->descriptor);

    EvaluationOptions options;
    options.store_proto_field_value_maps = use_shared_states;

    EvaluationContext context(options);
    std::vector<absl::StatusOr<TupleSlot>> output_slots;
    ZETASQL_ASSERT_OK(EvalGetProtoFieldExprs(
        {std::make_pair(&access_info1, 2), std::make_pair(&access_info2, 1)},
        {s1, s2, s3}, &context, &output_slots));
    ASSERT_EQ(output_slots.size(), (2 + 1) * 3);

    // First proto.
    // (The shared state is checked by EvalGetProtoFieldExprs()).
    EXPECT_THAT(output_slots[0], IsOkAndHolds(IsTupleSlotWith(Int64(1), _)));
    EXPECT_THAT(output_slots[1], IsOkAndHolds(IsTupleSlotWith(Int64(1), _)));
    EXPECT_THAT(output_slots[2],
                IsOkAndHolds(IsTupleSlotWith(nested_value1, _)));

    // Second proto.
    EXPECT_THAT(output_slots[3], IsOkAndHolds(IsTupleSlotWith(Int64(10), _)));
    EXPECT_THAT(output_slots[4], IsOkAndHolds(IsTupleSlotWith(Int64(10), _)));
    EXPECT_THAT(output_slots[5],
                IsOkAndHolds(IsTupleSlotWith(nested_value2, _)));

    // Third proto.
    EXPECT_THAT(output_slots[6], IsOkAndHolds(IsTupleSlotWith(Int64(100), _)));
    EXPECT_THAT(output_slots[7], IsOkAndHolds(IsTupleSlotWith(Int64(100), _)));
    EXPECT_THAT(output_slots[8],
                IsOkAndHolds(IsTupleSlotWith(nested_value3, _)));

    if (use_shared_states) {
      ASSERT_THAT(s1, IsTupleSlotWith(v[0], Not(IsNull())));
      ASSERT_THAT(s2, IsTupleSlotWith(v[1], Not(IsNull())));
      ASSERT_THAT(s3, IsTupleSlotWith(v[2], Not(IsNull())));

      std::vector<const SharedProtoState*> shared_states = {
          s1.mutable_shared_proto_state()->get(),
          s2.mutable_shared_proto_state()->get(),
          s3.mutable_shared_proto_state()->get()};

      for (int i = 0; i < 3; ++i) {
        const SharedProtoState& shared_state = *shared_states[i];
        ASSERT_TRUE(shared_state.has_value());
        ASSERT_EQ(shared_state.value().size(), 1);
        const auto& entry = *shared_state.value().begin();
        EXPECT_EQ(entry.first.proto_rep, InternalValue::GetProtoRep(v[i]));
        const ProtoFieldValueList& value_list = *entry.second;

        const int first_field_values[] = {1, 10, 100};
        const int first_field_value = first_field_values[i];

        zetasql_test__::KitchenSinkPB_Nested nested_msg;
        nested_msg.set_nested_int64(3 * first_field_value);
        absl::Cord nested_bytes = SerializeToCord(nested_msg);
        const Value nested_value =
            Value::Proto(MakeProtoType(&nested_msg), nested_bytes);

        EXPECT_THAT(value_list,
                    ElementsAre(IsOkAndHolds(Int64(first_field_value)),
                                IsOkAndHolds(nested_value)));
      }
    } else {
      EXPECT_THAT(s1, IsTupleSlotWith(v[0], Pointee(Eq(nullopt))));
      EXPECT_THAT(s2, IsTupleSlotWith(v[1], Pointee(Eq(nullopt))));
      EXPECT_THAT(s3, IsTupleSlotWith(v[2], Pointee(Eq(nullopt))));
    }
  }
}

}  // namespace
}  // namespace zetasql
