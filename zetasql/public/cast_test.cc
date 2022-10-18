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

#include "zetasql/public/cast.h"

#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "zetasql/base/status.h"

namespace zetasql {

MATCHER_P(StringValueMatches, matcher, "") {
  return ExplainMatchResult(matcher, arg.string_value(), result_listener);
}

using testing::StrCaseEq;
using zetasql_base::testing::IsOkAndHolds;

static TypeFactory* type_factory = new TypeFactory();

static const StructType* SimpleStructType() {
  const StructType* struct_type;
  ZETASQL_EXPECT_OK(type_factory->MakeStructType(
      {{"", type_factory->get_string()}, {"", type_factory->get_string()}},
      &struct_type));
  return struct_type;
}

static const StructType* TimestampStructType() {
  const StructType* struct_type;
  ZETASQL_EXPECT_OK(type_factory->MakeStructType({{"a", type_factory->get_timestamp()},
                                          {"b", type_factory->get_timestamp()}},
                                         &struct_type));
  return struct_type;
}

TEST(CastValueWithTimezoneArgumentTests, TimestampCastTest) {
  // These are done here instead of in compliance tests for now, since the
  // test framework for the compliance tests does not support setting the
  // time zone.  TODO: Allow compliance testing to set the
  // time zone for requests if possible, then move these tests to the
  // compliance tests.
  const Value string_without_timezone = String("1970-01-01 01:01:06");
  const Value string_with_timezone =
      String("1970-01-01 01:01:06 America/Los_Angeles");
  const Value canonical_seconds_string =
      String("1970-01-01 01:01:06-08");
  const Value canonical_millis_string =
      String("1970-01-01 01:01:06.000-08");
  const Value canonical_micros_string =
      String("1970-01-01 01:01:06.000000-08");
  const Value timestamp = TimestampFromUnixMicros(32466000000);

  // TIMESTAMP to string, with zero truncation.
  const Type* string_type = String("").type();
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(0), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(1), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.000001+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(10), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.000010+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(100), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.000100+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(1000), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.001+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(10000), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.010+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(100000), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:00.100+00")));
  EXPECT_THAT(CastValue(TimestampFromUnixMicros(1000000), absl::UTCTimeZone(),
                        LanguageOptions(), string_type),
              IsOkAndHolds(String("1970-01-01 00:00:01+00")));

  // Cast to STRUCT<TIMESTAMP, TIMESTAMP> with los_angeles timezone.
  absl::TimeZone los_angeles;
  absl::LoadTimeZone("America/Los_Angeles", &los_angeles);
  const Value struct_value = Value::Struct(
      SimpleStructType(), {string_with_timezone, string_without_timezone});
  const absl::StatusOr<Value> status_or_value = CastValue(
      struct_value, los_angeles, LanguageOptions(), TimestampStructType());
  ZETASQL_EXPECT_OK(status_or_value);

  const Value casted_struct_value = status_or_value.value();
  EXPECT_TRUE(casted_struct_value.Equals(
      Value::Struct(TimestampStructType(), {timestamp, timestamp})));
}

TEST(ConversionTest, ValueCastTest) {
  const Type* int_type = type_factory->get_int32();
  const Type* string_type = type_factory->get_string();

  Function conversion_function(
      "MyIntToMyString", "engine_defined_conversion", Function::SCALAR,
      /*function_signatures=*/{},
      FunctionOptions().set_evaluator([](const absl::Span<const Value> args) {
        ZETASQL_CHECK_EQ(args.size(), 1);
        return Value::StringValue(std::to_string(args[0].int32_value()));
      }));

  // Check evaluation of valid conversion.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Conversion conversion,
      Conversion::Create(int_type, string_type, &conversion_function,
                         CastFunctionProperty(CastFunctionType::IMPLICIT,
                                              /*coercion_cost=*/50)));
  ASSERT_TRUE(conversion.is_valid());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value casted_value,
                       conversion.evaluator().Eval(Value::Int32(12)));
  EXPECT_EQ(casted_value, Value::String("12"));

  // Check invalid conversion error generation.
  conversion = Conversion::Invalid();
  constexpr const char* const invalid_conversion_message =
      "Attempt to access properties of invalid Conversion";
  EXPECT_FALSE(conversion.is_valid());
  EXPECT_DEATH(conversion.from_type(), invalid_conversion_message);
  EXPECT_DEATH(conversion.to_type(), invalid_conversion_message);
  EXPECT_DEATH(conversion.property(), invalid_conversion_message);
  EXPECT_DEATH(conversion.evaluator().Eval(Value::Int32(12)).value(),
               invalid_conversion_message);
}

TEST(ConversionTest, CanonicalizedNanAndZeroTest) {
  const Type* string_type = type_factory->get_string();
  EXPECT_THAT(
      CastValue(Value::Float(-0.0), absl::UTCTimeZone(), LanguageOptions(),
                string_type, /*catalog=*/nullptr, /*canonicalize_zero=*/true),
      IsOkAndHolds(String("0")));
  EXPECT_THAT(
      CastValue(Value::Double(-0.0), absl::UTCTimeZone(), LanguageOptions(),
                string_type, /*catalog=*/nullptr, /*canonicalize_zero=*/true),
      IsOkAndHolds(String("0")));
  EXPECT_THAT(
      CastValue(Value::Float(-0.0), absl::UTCTimeZone(), LanguageOptions(),
                string_type, /*catalog=*/nullptr, /*canonicalize_zero=*/false),
      IsOkAndHolds(String("-0")));
  EXPECT_THAT(
      CastValue(Value::Double(-0.0), absl::UTCTimeZone(), LanguageOptions(),
                string_type, /*catalog=*/nullptr, /*canonicalize_zero=*/false),
      IsOkAndHolds(String("-0")));
  EXPECT_THAT(CastValue(Value::Float(std::numeric_limits<float>::quiet_NaN()),
                        absl::UTCTimeZone(), LanguageOptions(), string_type),
              IsOkAndHolds(StringValueMatches(StrCaseEq("nan"))));
  EXPECT_THAT(CastValue(Value::Float(std::numeric_limits<double>::quiet_NaN()),
                        absl::UTCTimeZone(), LanguageOptions(), string_type),
              IsOkAndHolds(StringValueMatches(StrCaseEq("nan"))));
  // Negative float NaN.
  EXPECT_THAT(CastValue(Value::Float(absl::bit_cast<float>(0xffc00000u)),
                        absl::UTCTimeZone(), LanguageOptions(), string_type),
              IsOkAndHolds(StringValueMatches(StrCaseEq("nan"))));
  // Negative double NaN.
  EXPECT_THAT(
      CastValue(Value::Float(absl::bit_cast<double>(0xfff8000000000000ul)),
                absl::UTCTimeZone(), LanguageOptions(), string_type),
      IsOkAndHolds(StringValueMatches(StrCaseEq("nan"))));
}

TEST(ConversionTest, ConversionMatchTest) {
  Function conversion_function("Name", "Group", Function::SCALAR);

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Conversion conversion,
        Conversion::Create(
            types::Int32Type(), types::StringType(), &conversion_function,
            CastFunctionProperty(CastFunctionType::EXPLICIT_OR_LITERAL,
                                 /*coercion_cost=*/50)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/true,
        Catalog::ConversionSourceExpressionKind::kOther)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kLiteral)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kParameter)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kOther)));
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Conversion conversion,
        Conversion::Create(
            types::Int32Type(), types::StringType(), &conversion_function,
            CastFunctionProperty(
                CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER,
                /*coercion_cost=*/50)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/true,
        Catalog::ConversionSourceExpressionKind::kOther)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kLiteral)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kParameter)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kOther)));
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Conversion conversion,
        Conversion::Create(types::Int32Type(), types::StringType(),
                           &conversion_function,
                           CastFunctionProperty(CastFunctionType::EXPLICIT,
                                                /*coercion_cost=*/50)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/true,
        Catalog::ConversionSourceExpressionKind::kOther)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kLiteral)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kParameter)));
    EXPECT_FALSE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kOther)));
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Conversion conversion,
        Conversion::Create(types::Int32Type(), types::StringType(),
                           &conversion_function,
                           CastFunctionProperty(CastFunctionType::IMPLICIT,
                                                /*coercion_cost=*/50)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/true,
        Catalog::ConversionSourceExpressionKind::kOther)));
    EXPECT_TRUE(conversion.IsMatch(Catalog::FindConversionOptions(
        /*is_explicit=*/false,
        Catalog::ConversionSourceExpressionKind::kOther)));
  }
}

static void ExecuteTest(const QueryParamsWithResult& test_case) {
  ZETASQL_CHECK_EQ(1, test_case.num_params());
  const Value& from_value = test_case.param(0);
  absl::TimeZone los_angeles;
  absl::LoadTimeZone("America/Los_Angeles", &los_angeles);
  LanguageOptions language_options;
  for (LanguageFeature feature : test_case.required_features()) {
    language_options.EnableLanguageFeature(feature);
  }
  if ((from_value.type()->IsFeatureV12CivilTimeType() ||
       test_case.result().type()->IsFeatureV12CivilTimeType()) &&
      !language_options.LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) {
    return;
  }
  const Type* expected_type = test_case.result().type();
  const absl::StatusOr<Value> status_or_value =
      CastValue(from_value, los_angeles, language_options, expected_type,
                /*catalog=*/nullptr, /*canonicalize_zero=*/true);
  const std::string error_string =
      absl::StrCat("from type: ", from_value.type()->DebugString(),
                   "\nfrom value: ", from_value.FullDebugString(),
                   "\nexpected type: ", expected_type->DebugString(),
                   "\nexpected value: ", test_case.result().FullDebugString());
  if (test_case.status().ok()) {
    ZETASQL_ASSERT_OK(status_or_value) << error_string;
    const Value& coerced_value = status_or_value.value();
    EXPECT_EQ(test_case.result(), coerced_value)
        << error_string
        << "\ncoerced value: " << coerced_value.FullDebugString();
  } else {
    EXPECT_FALSE(status_or_value.ok())
        << error_string
        << "\ncoerced value: " << status_or_value.value().FullDebugString();
  }
}

// Some cast behaviors are not dictated by ZetaSQL, particularly casting
// between PROTO and BYTES.  Engines are free to use different implementations,
// with different semantics.  These tests cover the logic for such casting
// in CastStatusOrValue(), but do not belong in compliance tests since different
// engines could behave different ways and still be compliant.
static std::vector<QueryParamsWithResult>
GetProtoAndBytesCastsWithoutValidation() {
  const ProtoType* kitchen_sink_proto_type;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &kitchen_sink_proto_type));
  const ProtoType* nullable_int_proto_type;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      zetasql_test__::NullableInt::descriptor(), &nullable_int_proto_type));

  return {
      // As currently implemented in CastValue(), casting between BYTES and
      // PROTO does no validation so these succeed.
      {{Proto(nullable_int_proto_type, absl::Cord("bunch of invalid stuff"))},
       Bytes("bunch of invalid stuff")},
      {{Bytes("bunch of invalid stuff")},
       Proto(nullable_int_proto_type, absl::Cord("bunch of invalid stuff"))},
      {{Proto(kitchen_sink_proto_type, absl::Cord("bunch of invalid stuff"))},
       Bytes("bunch of invalid stuff")},
      {{Bytes("bunch of invalid stuff")},
       Proto(kitchen_sink_proto_type, absl::Cord("bunch of invalid stuff"))},
  };
}

typedef testing::TestWithParam<QueryParamsWithResult> CastTemplateTest;

TEST_P(CastTemplateTest, Testlib) {
  const QueryParamsWithResult& expected = GetParam();
  ExecuteTest(expected);
}

INSTANTIATE_TEST_SUITE_P(
    CastProtoBytes, CastTemplateTest,
    testing::ValuesIn(GetProtoAndBytesCastsWithoutValidation()));

INSTANTIATE_TEST_SUITE_P(CastDateTime, CastTemplateTest,
                         testing::ValuesIn(GetFunctionTestsCastDateTime()));

INSTANTIATE_TEST_SUITE_P(CastInterval, CastTemplateTest,
                         testing::ValuesIn(GetFunctionTestsCastInterval()));

INSTANTIATE_TEST_SUITE_P(CastNumeric, CastTemplateTest,
                         testing::ValuesIn(GetFunctionTestsCastNumeric()));

// TODO add tests for NUMERIC.
INSTANTIATE_TEST_SUITE_P(CastComplex, CastTemplateTest,
                         testing::ValuesIn(GetFunctionTestsCastComplex()));

INSTANTIATE_TEST_SUITE_P(CastString, CastTemplateTest,
                         testing::ValuesIn(GetFunctionTestsCastString()));

INSTANTIATE_TEST_SUITE_P(
    CastNumericString, CastTemplateTest,
    testing::ValuesIn(GetFunctionTestsCastNumericString()));

}  // namespace zetasql
