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

#include "zetasql/public/function_signature.h"

#include <memory>
#include <set>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"

namespace zetasql {

using testing::HasSubstr;
using testing::IsNull;
using testing::NotNull;
using zetasql_base::testing::StatusIs;

TEST(FunctionSignatureTests, FunctionArgumentTypeTests) {
  TypeFactory factory;
  FunctionArgumentType fixed_type_int32(factory.get_int32());
  ASSERT_FALSE(fixed_type_int32.IsConcrete());
  fixed_type_int32.set_num_occurrences(0);
  ASSERT_TRUE(fixed_type_int32.IsConcrete());
  EXPECT_FALSE(fixed_type_int32.IsValid().ok());
  fixed_type_int32.set_num_occurrences(2);
  ASSERT_TRUE(fixed_type_int32.IsConcrete());
  EXPECT_FALSE(fixed_type_int32.IsValid().ok());
  fixed_type_int32.set_num_occurrences(1);
  ASSERT_TRUE(fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(fixed_type_int32.IsValid());
  ASSERT_THAT(fixed_type_int32.type(), NotNull());
  ASSERT_EQ(ARG_TYPE_FIXED, fixed_type_int32.kind());
  ASSERT_FALSE(fixed_type_int32.repeated());
  ASSERT_FALSE(fixed_type_int32.optional());
  EXPECT_EQ("INT32",
            fixed_type_int32.UserFacingNameWithCardinality(PRODUCT_INTERNAL));

  FunctionArgumentType repeating_fixed_type_int32(
      factory.get_int32(), FunctionArgumentType::REPEATED);
  ASSERT_FALSE(repeating_fixed_type_int32.IsConcrete());
  repeating_fixed_type_int32.set_num_occurrences(0);
  ASSERT_TRUE(repeating_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(repeating_fixed_type_int32.IsValid());
  repeating_fixed_type_int32.IncrementNumOccurrences();
  ASSERT_TRUE(repeating_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(repeating_fixed_type_int32.IsValid());
  repeating_fixed_type_int32.IncrementNumOccurrences();
  ASSERT_TRUE(repeating_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(repeating_fixed_type_int32.IsValid());
  ASSERT_THAT(repeating_fixed_type_int32.type(), NotNull());
  ASSERT_EQ(ARG_TYPE_FIXED, repeating_fixed_type_int32.kind());
  ASSERT_TRUE(repeating_fixed_type_int32.repeated());
  EXPECT_EQ("[INT32, ...]",
            repeating_fixed_type_int32.UserFacingNameWithCardinality(
                PRODUCT_INTERNAL));

  FunctionArgumentType optional_fixed_type_int32(
      factory.get_int32(), FunctionArgumentType::OPTIONAL);
  ASSERT_FALSE(optional_fixed_type_int32.IsConcrete());
  optional_fixed_type_int32.set_num_occurrences(0);
  ASSERT_TRUE(optional_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(optional_fixed_type_int32.IsValid());
  optional_fixed_type_int32.IncrementNumOccurrences();
  ASSERT_TRUE(optional_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(optional_fixed_type_int32.IsValid());
  optional_fixed_type_int32.IncrementNumOccurrences();
  ASSERT_TRUE(optional_fixed_type_int32.IsConcrete());
  EXPECT_FALSE(optional_fixed_type_int32.IsValid().ok());
  optional_fixed_type_int32.set_num_occurrences(0);
  ASSERT_TRUE(optional_fixed_type_int32.IsConcrete());
  ZETASQL_EXPECT_OK(optional_fixed_type_int32.IsValid());
  ASSERT_THAT(optional_fixed_type_int32.type(), NotNull());
  ASSERT_EQ(ARG_TYPE_FIXED, optional_fixed_type_int32.kind());
  ASSERT_FALSE(optional_fixed_type_int32.repeated());
  ASSERT_TRUE(optional_fixed_type_int32.optional());
  EXPECT_EQ("[INT32]",
            optional_fixed_type_int32.UserFacingNameWithCardinality(
                PRODUCT_INTERNAL));

  FunctionArgumentType any_type(ARG_TYPE_ANY_1);
  ASSERT_FALSE(any_type.IsConcrete());
  ASSERT_THAT(any_type.type(), IsNull());
  ASSERT_EQ(ARG_TYPE_ANY_1, any_type.kind());
  ASSERT_FALSE(any_type.repeated());

  FunctionArgumentType array_of_any_type(
      ARG_ARRAY_TYPE_ANY_1);
  ASSERT_FALSE(array_of_any_type.IsConcrete());
  ASSERT_THAT(array_of_any_type.type(), IsNull());
  ASSERT_EQ(ARG_ARRAY_TYPE_ANY_1, array_of_any_type.kind());
  ASSERT_FALSE(array_of_any_type.repeated());

  FunctionArgumentType proto_any_type(ARG_PROTO_ANY);
  ASSERT_FALSE(proto_any_type.IsConcrete());
  ASSERT_THAT(proto_any_type.type(), IsNull());
  ASSERT_EQ(ARG_PROTO_ANY, proto_any_type.kind());
  ASSERT_FALSE(proto_any_type.repeated());

  FunctionArgumentType struct_any_type(ARG_STRUCT_ANY);
  ASSERT_FALSE(struct_any_type.IsConcrete());
  ASSERT_THAT(struct_any_type.type(), IsNull());
  ASSERT_EQ(ARG_STRUCT_ANY, struct_any_type.kind());
  ASSERT_FALSE(struct_any_type.repeated());

  FunctionArgumentType enum_any_type(ARG_ENUM_ANY);
  ASSERT_FALSE(enum_any_type.IsConcrete());
  ASSERT_THAT(enum_any_type.type(), IsNull());
  ASSERT_EQ(ARG_ENUM_ANY, enum_any_type.kind());
  ASSERT_FALSE(enum_any_type.repeated());
}

void TestDefaultValueAfterSerialization(const FunctionArgumentType& arg_type) {
  FileDescriptorSetMap fdset_map;
  FunctionArgumentTypeProto proto;
  ZETASQL_EXPECT_OK(arg_type.Serialize(&fdset_map, &proto));
  TypeFactory factory;
  std::vector<const google::protobuf::DescriptorPool*> pools(fdset_map.size());
  for (const auto& pair : fdset_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }

  std::unique_ptr<FunctionArgumentType> dummy_type;
  ZETASQL_EXPECT_OK(
      FunctionArgumentType::Deserialize(proto, pools, &factory, &dummy_type));
  EXPECT_TRUE(
      dummy_type->GetDefault().value().Equals(arg_type.GetDefault().value()));
}

TEST(FunctionSignatureTests, FunctionArgumentTypeWithDefaultValues) {
  TypeFactory factory;
  FunctionArgumentTypeOptions invalid_required_arg_type_option =
      FunctionArgumentTypeOptions(FunctionEnums::REQUIRED)
          .set_default(values::String("abc"));
  FunctionArgumentTypeOptions valid_optional_arg_type_option =
      FunctionArgumentTypeOptions(FunctionEnums::OPTIONAL)
          .set_default(values::Int32(10086));
  FunctionArgumentTypeOptions valid_optional_arg_type_option_null =
      FunctionArgumentTypeOptions(FunctionEnums::OPTIONAL)
          .set_default(values::NullInt32());
  FunctionArgumentTypeOptions invalid_repeated_arg_type_option =
      FunctionArgumentTypeOptions(FunctionEnums::REPEATED)
          .set_default(values::Double(3.14));

  FunctionArgumentType required_fixed_type_string(
      factory.get_string(), invalid_required_arg_type_option,
      /*num_occurrences=*/1);
  EXPECT_THAT(
      required_fixed_type_string.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Default value cannot be applied to a REQUIRED argument")));

  FunctionArgumentType repeated_fixed_type_double(
      factory.get_double(), invalid_repeated_arg_type_option,
      /*num_occurrences=*/1);
  EXPECT_THAT(
      repeated_fixed_type_double.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Default value cannot be applied to a REPEATED argument")));

  FunctionArgumentType optional_fixed_type_bytes(factory.get_bytes(),
                                                 valid_optional_arg_type_option,
                                                 /*num_occurrences=*/1);
  EXPECT_THAT(
      optional_fixed_type_bytes.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Default value type does not match the argument type")));

  FunctionArgumentType optional_fixed_type_int64(factory.get_int64(),
                                                 valid_optional_arg_type_option,
                                                 /*num_occurrences=*/1);
  EXPECT_THAT(
      optional_fixed_type_int64.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Default value type does not match the argument type")));

  FunctionArgumentType bad_optional_fixed_type_int64(
      factory.get_int64(), valid_optional_arg_type_option_null,
      /*num_occurrences=*/1);
  EXPECT_THAT(
      bad_optional_fixed_type_int64.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Default value type does not match the argument type")));

  FunctionArgumentType optional_fixed_type_int32(factory.get_int32(),
                                                 valid_optional_arg_type_option,
                                                 /*num_occurrences=*/1);
  EXPECT_TRUE(optional_fixed_type_int32.GetDefault().value().Equals(
      values::Int32(10086)));
  ZETASQL_EXPECT_OK(optional_fixed_type_int32.IsValid());
  TestDefaultValueAfterSerialization(optional_fixed_type_int32);

  FunctionArgumentType optional_fixed_type_int32_null(
      factory.get_int32(), valid_optional_arg_type_option_null,
      /*num_occurrences=*/1);
  EXPECT_TRUE(optional_fixed_type_int32_null.GetDefault().value().Equals(
      values::NullInt32()));
  ZETASQL_EXPECT_OK(optional_fixed_type_int32_null.IsValid());
  TestDefaultValueAfterSerialization(optional_fixed_type_int32_null);

  FunctionArgumentType templated_type_non_null(ARG_TYPE_ANY_1,
                                               valid_optional_arg_type_option);
  EXPECT_TRUE(templated_type_non_null.GetDefault().value().Equals(
      values::Int32(10086)));
  ZETASQL_EXPECT_OK(templated_type_non_null.IsValid());
  TestDefaultValueAfterSerialization(templated_type_non_null);

  FunctionArgumentType templated_type_null(ARG_TYPE_ANY_1,
                                           valid_optional_arg_type_option_null);
  EXPECT_TRUE(
      templated_type_null.GetDefault().value().Equals(values::NullInt32()));
  ZETASQL_EXPECT_OK(templated_type_null.IsValid());
  TestDefaultValueAfterSerialization(templated_type_null);

  FunctionArgumentType relation_type(ARG_TYPE_RELATION,
                                     valid_optional_arg_type_option_null);
  EXPECT_THAT(
      relation_type.IsValid(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("ANY TABLE argument cannot have a default value")));

  FunctionArgumentType model_type(ARG_TYPE_MODEL,
                                  valid_optional_arg_type_option_null);
  EXPECT_THAT(
      model_type.IsValid(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("ANY MODEL argument cannot have a default value")));

  FunctionArgumentType connection_type(ARG_TYPE_CONNECTION,
                                       valid_optional_arg_type_option_null);
  EXPECT_THAT(
      connection_type.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("ANY CONNECTION argument cannot have a default value")));

  FunctionArgumentType descriptor_type(ARG_TYPE_DESCRIPTOR,
                                       valid_optional_arg_type_option_null);
  EXPECT_THAT(
      descriptor_type.IsValid(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("ANY DESCRIPTOR argument cannot have a default value")));
}

// The following helpers generate ZetaSQL function signatures for testing.
//
// Model a nullary function such as NOW()
static FunctionSignature GetNullaryFunction(TypeFactory* factory) {
  FunctionArgumentTypeList arguments;
  FunctionSignature nullary_function(
      FunctionArgumentType(factory->get_timestamp()), arguments, nullptr);
  return nullary_function;
}

// Model simple operator like '+'
static FunctionSignature GetAddFunction(TypeFactory* factory) {
  FunctionArgumentTypeList arguments;
  arguments.push_back(FunctionArgumentType(factory->get_int64()));
  arguments.push_back(FunctionArgumentType(factory->get_int64()));
  FunctionSignature add_function(FunctionArgumentType(factory->get_int64()),
                                 arguments, nullptr);
  return add_function;
}

// Model signature for 'IF <bool> THEN <any> ELSE <any> END'
static FunctionSignature GetIfThenFunction(TypeFactory* factory) {
  FunctionArgumentTypeList arguments;
  arguments.push_back(FunctionArgumentType(factory->get_bool()));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  FunctionSignature if_then_else_signature(
      FunctionArgumentType(ARG_TYPE_ANY_1), arguments, nullptr);
  return if_then_else_signature;
}

// Model signature for:
// CASE WHEN <x1> THEN <y1>
//      WHEN <x2> THEN <y2> ELSE <z> END
static FunctionSignature GetCaseWhenFunction(TypeFactory* factory) {
  FunctionArgumentTypeList arguments;
  arguments.push_back(FunctionArgumentType(factory->get_bool(),
                                           FunctionArgumentType::REPEATED));
  arguments.push_back(
      FunctionArgumentType(ARG_TYPE_ANY_1, FunctionArgumentType::REPEATED));
  arguments.push_back(
      FunctionArgumentType(ARG_TYPE_ANY_1, FunctionArgumentType::OPTIONAL));
  FunctionSignature case_when_signature(FunctionArgumentType(ARG_TYPE_ANY_1),
                                        arguments, nullptr);
  return case_when_signature;
}

// Model signature for:
// CASE <w> WHEN <x1> THEN <y1>
//          WHEN <x2> THEN <y2> ... ELSE <z> END
static FunctionSignature GetCaseValueFunction(
    TypeFactory* factory, FunctionArgumentTypeList* arguments) {
  arguments->clear();
  arguments->push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  arguments->push_back(FunctionArgumentType(ARG_TYPE_ANY_1,
                                            FunctionArgumentType::REPEATED));
  arguments->push_back(FunctionArgumentType(ARG_TYPE_ANY_2,
                                            FunctionArgumentType::REPEATED));
  arguments->push_back(FunctionArgumentType(ARG_TYPE_ANY_2,
                                            FunctionArgumentType::OPTIONAL));
  FunctionSignature case_value_signature(
      FunctionArgumentType(ARG_TYPE_ANY_2), *arguments, -1 /* context_id */);
  return case_value_signature;
}

// Test a function with VOID return type, and some argument options.
static FunctionSignature GetVoidFunction(TypeFactory* factory) {
  FunctionSignature void_func(
      ARG_TYPE_VOID,
      {{types::BoolType()},
       {types::Int64Type(),
           FunctionArgumentTypeOptions().set_is_not_aggregate()},
       {ARG_TYPE_ANY_1, FunctionArgumentTypeOptions().set_must_be_non_null()}},
       -1 /* context_id */);
  return void_func;
}

TEST(FunctionSignatureTests, FunctionSignatureTestsInternalProductMode) {
  TypeFactory factory;

  // Model a nullary function such as NOW()
  FunctionSignature nullary_function = GetNullaryFunction(&factory);
  ASSERT_FALSE(nullary_function.IsConcrete());
  EXPECT_EQ("NOW() -> TIMESTAMP", nullary_function.DebugString("NOW"));
  EXPECT_EQ("() RETURNS TIMESTAMP",
            nullary_function.GetSQLDeclaration({} /* arg_names */,
                                               ProductMode::PRODUCT_INTERNAL));

  // Model simple operator like '+'
  FunctionSignature add_function = GetAddFunction(&factory);
  ASSERT_FALSE(add_function.IsConcrete());
  EXPECT_EQ("ADD(INT64, INT64) -> INT64", add_function.DebugString("ADD"));
  EXPECT_EQ("(INT64, INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({} /* arg_names */,
                                           ProductMode::PRODUCT_INTERNAL));
  EXPECT_EQ("(x INT64, INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x"} /* arg_names */,
                                           ProductMode::PRODUCT_INTERNAL));
  EXPECT_EQ("(x INT64, y INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x", "y"} /* arg_names */,
                                           ProductMode::PRODUCT_INTERNAL));
  EXPECT_EQ("(x INT64, y INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x", "y", "z"} /* arg_names */,
                                           ProductMode::PRODUCT_INTERNAL));

  // Model signature for 'IF <bool> THEN <any> ELSE <any> END'
  FunctionSignature if_then_else_signature = GetIfThenFunction(&factory);
  ASSERT_FALSE(if_then_else_signature.IsConcrete());
  EXPECT_EQ("IF(BOOL, <T1>, <T1>) -> <T1>",
            if_then_else_signature.DebugString("IF"));

  // Model signature for:
  // CASE WHEN <x1> THEN <y1>
  //      WHEN <x2> THEN <y2> ELSE <z> END
  FunctionSignature case_when_signature = GetCaseWhenFunction(&factory);
  ASSERT_FALSE(case_when_signature.IsConcrete());
  EXPECT_EQ("CASE(repeated BOOL, repeated <T1>, optional <T1>) -> <T1>",
            case_when_signature.DebugString("CASE"));
  EXPECT_EQ(
      "(/*repeated*/ BOOL, /*repeated*/ <T1>, /*optional*/ <T1>) "
      "RETURNS <T1>",
      case_when_signature.GetSQLDeclaration({} /* arg_names */,
                                            ProductMode::PRODUCT_INTERNAL));

  // Model signature for:
  // CASE <w> WHEN <x1> THEN <y1>
  //          WHEN <x2> THEN <y2> ... ELSE <z> END
  FunctionArgumentTypeList arguments;
  FunctionSignature case_value_signature =
      GetCaseValueFunction(&factory, &arguments);
  ASSERT_FALSE(case_value_signature.IsConcrete());
  EXPECT_EQ("CASE(<T1>, repeated <T1>, repeated <T2>, optional <T2>) -> <T2>",
            case_value_signature.DebugString("CASE"));

  // Test copying a FunctionSignature, assigning a new context.
  FunctionSignature copy1(case_value_signature, 1234 /* context_id */);
  FunctionSignature copy2(case_value_signature, &arguments /* context_ptr */);
  EXPECT_EQ(case_value_signature.DebugString("abc", true),
            copy1.DebugString("abc", true));
  EXPECT_EQ(case_value_signature.DebugString("", true),
            copy2.DebugString("", true));
  EXPECT_EQ(1234, copy1.context_id());
  EXPECT_EQ(&arguments, copy2.context_ptr());

  // Test a function with VOID return type, and some argument options.
  FunctionSignature void_func = GetVoidFunction(&factory);
  EXPECT_EQ("(BOOL, INT64, <T1>) -> <void>", void_func.DebugString());
  EXPECT_EQ("func(BOOL, INT64 {is_not_aggregate: true}, "
            "<T1> {must_be_non_null: true}) -> <void>",
            void_func.DebugString("func", true /* verbose */));
  EXPECT_EQ("(BOOL, INT64 NOT AGGREGATE, <T1> /*must_be_non_null*/)",
            void_func.GetSQLDeclaration({} /* arg_names */,
                                        ProductMode::PRODUCT_INTERNAL));
  // With argument names, including one that will require quoting.
  EXPECT_EQ("(a BOOL, b INT64 NOT AGGREGATE, `c d` <T1> /*must_be_non_null*/)",
            void_func.GetSQLDeclaration({"a", "b", "c d"},
                                        ProductMode::PRODUCT_INTERNAL));

  // Test DebugString() for a signature with a deprecation warning.
  FreestandingDeprecationWarning warning;
  warning.set_message("foo is deprecated");
  warning.mutable_deprecation_warning()->set_kind(
      DeprecationWarning::PROTO3_FIELD_PRESENCE);
  ErrorLocation* location = warning.mutable_error_location();
  location->set_line(10);
  location->set_column(50);

  FunctionSignature func_with_deprecation_warning =
      GetNullaryFunction(&factory);
  func_with_deprecation_warning.SetAdditionalDeprecationWarnings({warning});
  EXPECT_EQ("() -> TIMESTAMP",
            func_with_deprecation_warning.DebugString(/*function_name=*/"",
                                                      /*verbose=*/false));
  EXPECT_EQ("() -> TIMESTAMP (1 deprecation warning)",
            func_with_deprecation_warning.DebugString(/*function_name=*/"",
                                                      /*verbose=*/true));
}

TEST(FunctionSignatureTests, FunctionSignatureTestsExternalProductMode) {
  TypeFactory factory;

  // Model a nullary function such as NOW()
  FunctionSignature nullary_function = GetNullaryFunction(&factory);
  EXPECT_EQ("() RETURNS TIMESTAMP",
            nullary_function.GetSQLDeclaration({} /* arg_names */,
                                               ProductMode::PRODUCT_EXTERNAL));

  // Model simple operator like '+'
  FunctionSignature add_function = GetAddFunction(&factory);
  EXPECT_EQ("(INT64, INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({} /* arg_names */,
                                           ProductMode::PRODUCT_EXTERNAL));
  EXPECT_EQ("(x INT64, INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x"} /* arg_names */,
                                           ProductMode::PRODUCT_EXTERNAL));
  EXPECT_EQ("(x INT64, y INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x", "y"} /* arg_names */,
                                           ProductMode::PRODUCT_EXTERNAL));
  EXPECT_EQ("(x INT64, y INT64) RETURNS INT64",
            add_function.GetSQLDeclaration({"x", "y", "z"} /* arg_names */,
                                           ProductMode::PRODUCT_EXTERNAL));

  // Model signature for:
  // CASE WHEN <x1> THEN <y1>
  //      WHEN <x2> THEN <y2> ELSE <z> END
  FunctionSignature case_when_signature = GetCaseWhenFunction(&factory);
  EXPECT_EQ(
      "(/*repeated*/ BOOL, /*repeated*/ <T1>, /*optional*/ <T1>) "
      "RETURNS <T1>",
      case_when_signature.GetSQLDeclaration({} /* arg_names */,
                                            ProductMode::PRODUCT_EXTERNAL));

  // Test a function with VOID return type, and some argument options.
  FunctionSignature void_func = GetVoidFunction(&factory);
  EXPECT_EQ("(BOOL, INT64 NOT AGGREGATE, <T1> /*must_be_non_null*/)",
            void_func.GetSQLDeclaration({} /* arg_names */,
                                        ProductMode::PRODUCT_EXTERNAL));
  // With argument names, including one that will require quoting.
  EXPECT_EQ("(a BOOL, b INT64 NOT AGGREGATE, `c d` <T1> /*must_be_non_null*/)",
            void_func.GetSQLDeclaration({"a", "b", "c d"},
                                        ProductMode::PRODUCT_EXTERNAL));
}

TEST(FunctionSignatureTests, FunctionSignatureValidityTests) {
  TypeFactory factory;

  FunctionArgumentTypeList arguments;
  std::unique_ptr<FunctionSignature> signature;

  FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  // repeated result is invalid
  EXPECT_DEBUG_DEATH(
      signature.reset(new FunctionSignature(
          FunctionArgumentType(factory.get_int64(), REPEATED),
          arguments, -1 /* context_id */)),
      "Result type cannot be repeated or optional");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // optional result is invalid
  EXPECT_DEBUG_DEATH(
      signature.reset(new FunctionSignature(
          FunctionArgumentType(factory.get_int64(), OPTIONAL),
          arguments, -1 /* context_id */)),
      "Result type cannot be repeated or optional");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // optional argument that is not last is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "Optional arguments must be at the end of the argument list");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // repeated arguments must be consecutive.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, REPEATED));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, REPEATED));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "Repeated arguments must be consecutive");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // If there is at least one repeated argument, then the number of optional
  // arguments must be less than the number of repeated arguments.

  // 1 repeated, 1 optional
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "The number of repeated arguments \\(1\\) must be greater than the "
      "number of optional arguments \\(1\\)");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // 1 repeated, 2 optional
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "The number of repeated arguments");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // 2 repeated, 2 optional
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "The number of repeated arguments");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // 2 repeated, 3 optional
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "The number of repeated arguments");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // num_occurrences must be the same value for all repeated arguments.
  arguments.assign({{ARG_TYPE_ANY_1, REPEATED, 2},
                    {ARG_TYPE_ANY_1, REPEATED, 1}});
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "num_occurrences");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // repeated relation argument is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, REPEATED));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);

  EXPECT_THAT(signature->IsValidForTableValuedFunction(),
              StatusIs(absl::StatusCode::kInternal,
                       testing::HasSubstr(
                           "Repeated relation argument is not supported")));

  // optional relation following any other optional argument is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1, OPTIONAL));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, OPTIONAL));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  EXPECT_THAT(signature->IsValidForTableValuedFunction(),
              StatusIs(absl::StatusCode::kInternal,
                       testing::HasSubstr("Relation arguments cannot follow "
                                          "repeated or optional arguments")));

  // optional relation following a repeated argument is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(factory.get_int64(), REPEATED));
  arguments.push_back(FunctionArgumentType(factory.get_int64(), REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, OPTIONAL));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);

  EXPECT_THAT(signature->IsValidForTableValuedFunction(),
              StatusIs(absl::StatusCode::kInternal,
                       testing::HasSubstr("Relation arguments cannot follow "
                                          "repeated or optional arguments")));

  // repeated relation following an optional argument is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(factory.get_int64(), REPEATED));
  arguments.push_back(FunctionArgumentType(factory.get_int64(), REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, OPTIONAL));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);

  EXPECT_THAT(signature->IsValidForTableValuedFunction(),
              StatusIs(absl::StatusCode::kInternal,
                       testing::HasSubstr("Relation arguments cannot follow "
                                          "repeated or optional arguments")));

  // required scalar following an optional relation is invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(factory.get_int64()));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, OPTIONAL));
  signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(factory.get_int64()), arguments,
      -1 /* context_id */);
  ZETASQL_EXPECT_OK(signature->IsValid());
  arguments.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "Optional arguments must be at the end of the argument list");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  // repeated relation argument following an optional scalar argument is
  // invalid.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(factory.get_int64(), OPTIONAL));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, REPEATED));
  arguments.push_back(FunctionArgumentType(ARG_TYPE_RELATION, REPEATED));
  EXPECT_DEBUG_DEATH(
      signature.reset(
          new FunctionSignature(FunctionArgumentType(factory.get_int64()),
                                arguments, -1 /* context_id */)),
      "Optional arguments must be at the end of the argument list");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }
}

TEST(FunctionArgumentTypeTests, TestTemplatedKindIsRelated) {
  TypeFactory type_factory;
  FunctionArgumentType arg_type_fixed(type_factory.get_int32());
  FunctionArgumentType arg_type_any_1(ARG_TYPE_ANY_1);
  FunctionArgumentType arg_type_any_2(ARG_TYPE_ANY_2);
  FunctionArgumentType arg_array_type_any_1(ARG_ARRAY_TYPE_ANY_1);
  FunctionArgumentType arg_array_type_any_2(ARG_ARRAY_TYPE_ANY_2);
  FunctionArgumentType arg_proto_any(ARG_PROTO_ANY);
  FunctionArgumentType arg_struct_any(ARG_STRUCT_ANY);
  FunctionArgumentType arg_enum_any(ARG_ENUM_ANY);

  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_1));
  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_FALSE(arg_type_fixed.TemplatedKindIsRelated(ARG_ENUM_ANY));

  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_TRUE(arg_type_any_1.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_TRUE(arg_type_any_1.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_1));
  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_TYPE_ANY_2));
  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_2));
  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_FALSE(arg_type_any_1.TemplatedKindIsRelated(ARG_ENUM_ANY));

  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_TRUE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_TRUE(arg_array_type_any_1.TemplatedKindIsRelated(
      ARG_ARRAY_TYPE_ANY_1));
  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_TYPE_ANY_2));
  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(
      ARG_ARRAY_TYPE_ANY_2));
  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_FALSE(arg_array_type_any_1.TemplatedKindIsRelated(ARG_ENUM_ANY));

  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_1));
  EXPECT_TRUE(arg_type_any_2.TemplatedKindIsRelated(ARG_TYPE_ANY_2));
  EXPECT_TRUE(arg_type_any_2.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_2));
  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_FALSE(arg_type_any_2.TemplatedKindIsRelated(ARG_ENUM_ANY));

  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(
      ARG_ARRAY_TYPE_ANY_1));
  EXPECT_TRUE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_TYPE_ANY_2));
  EXPECT_TRUE(arg_array_type_any_2.TemplatedKindIsRelated(
      ARG_ARRAY_TYPE_ANY_2));
  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_FALSE(arg_array_type_any_2.TemplatedKindIsRelated(ARG_ENUM_ANY));

  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_TYPE_FIXED));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_TYPE_ANY_1));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_1));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_TYPE_ANY_2));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_ARRAY_TYPE_ANY_2));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_PROTO_ANY));
  EXPECT_FALSE(arg_enum_any.TemplatedKindIsRelated(ARG_STRUCT_ANY));
  EXPECT_TRUE(arg_enum_any.TemplatedKindIsRelated(ARG_ENUM_ANY));
}

static void CheckConcreteArgumentType(
    const Type* expected_type,
    const std::unique_ptr<FunctionSignature>& signature,
    int idx) {
  ASSERT_THAT(signature->ConcreteArgumentType(idx), NotNull());
  EXPECT_TRUE(signature->ConcreteArgumentType(idx)->Equals(expected_type));
}

TEST(FunctionSignatureTests, TestConcreteArgumentType) {
  TypeFactory factory;

  FunctionArgumentTypeList arguments;
  std::unique_ptr<FunctionSignature> signature;

  FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;
  FunctionArgumentType::ArgumentCardinality REQUIRED =
      FunctionArgumentType::REQUIRED;

  std::unique_ptr<FunctionArgumentType> result_type;
  result_type =
      absl::make_unique<FunctionArgumentType>(types::Int64Type(), REQUIRED, 0);

  // 0 arguments.
  arguments.clear();
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(0, signature->NumConcreteArguments());

  // 1 required.
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REQUIRED, 1));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(1, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::Int64Type(), signature, 0);

  // 2 required.
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int32Type(), REQUIRED, 1));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(2, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::Int64Type(), signature, 0);
  CheckConcreteArgumentType(types::Int32Type(), signature, 1);

  // 3 required - simulates IF().
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REQUIRED, 1));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(3, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::Int64Type(), signature, 1);
  CheckConcreteArgumentType(types::Int64Type(), signature, 2);

  // 2 repeateds (2), 1 optional (0) -
  //   CASE WHEN . THEN . WHEN . THEN . END
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), OPTIONAL, 0));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(4, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::Int64Type(), signature, 1);
  CheckConcreteArgumentType(types::BoolType(), signature, 2);
  CheckConcreteArgumentType(types::Int64Type(), signature, 3);

  // 2 repeateds (2), 1 optional (1) -
  //   CASE WHEN . THEN . WHEN . THEN . ELSE . END
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int32Type(), OPTIONAL, 1));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(5, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::Int64Type(), signature, 1);
  CheckConcreteArgumentType(types::BoolType(), signature, 2);
  CheckConcreteArgumentType(types::Int64Type(), signature, 3);
  CheckConcreteArgumentType(types::Int32Type(), signature, 4);

  // 2 required, 3 repeateds (2), 1 required, 2 optional (0,0) -
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::StringType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Uint64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::BytesType(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Uint32Type(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int32Type(), OPTIONAL, 0));
  arguments.push_back(FunctionArgumentType(types::DateType(), OPTIONAL, 0));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(9, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::StringType(), signature, 1);
  CheckConcreteArgumentType(types::Uint64Type(), signature, 2);
  CheckConcreteArgumentType(types::Int64Type(), signature, 3);
  CheckConcreteArgumentType(types::BytesType(), signature, 4);
  CheckConcreteArgumentType(types::Uint64Type(), signature, 5);
  CheckConcreteArgumentType(types::Int64Type(), signature, 6);
  CheckConcreteArgumentType(types::BytesType(), signature, 7);
  CheckConcreteArgumentType(types::Uint32Type(), signature, 8);

  // 2 required, 3 repeateds (2), 1 required, 2 optional (1,0) -
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::StringType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Uint64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Int64Type(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::BytesType(), REPEATED, 2));
  arguments.push_back(FunctionArgumentType(types::Uint32Type(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int32Type(), OPTIONAL, 1));
  arguments.push_back(FunctionArgumentType(types::DateType(), OPTIONAL, 0));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(10, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::StringType(), signature, 1);
  CheckConcreteArgumentType(types::Uint64Type(), signature, 2);
  CheckConcreteArgumentType(types::Int64Type(), signature, 3);
  CheckConcreteArgumentType(types::BytesType(), signature, 4);
  CheckConcreteArgumentType(types::Uint64Type(), signature, 5);
  CheckConcreteArgumentType(types::Int64Type(), signature, 6);
  CheckConcreteArgumentType(types::BytesType(), signature, 7);
  CheckConcreteArgumentType(types::Uint32Type(), signature, 8);
  CheckConcreteArgumentType(types::Int32Type(), signature, 9);

  // 2 required, 2 optional (1,0) -
  arguments.clear();
  arguments.push_back(FunctionArgumentType(types::BoolType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::StringType(), REQUIRED, 1));
  arguments.push_back(FunctionArgumentType(types::Int32Type(), OPTIONAL, 1));
  arguments.push_back(FunctionArgumentType(types::DateType(), OPTIONAL, 0));
  signature = absl::make_unique<FunctionSignature>(*result_type, arguments,
                                                   -1 /* context_id */);
  EXPECT_EQ(3, signature->NumConcreteArguments());
  CheckConcreteArgumentType(types::BoolType(), signature, 0);
  CheckConcreteArgumentType(types::StringType(), signature, 1);
  CheckConcreteArgumentType(types::Int32Type(), signature, 2);
}

static std::vector<FunctionArgumentType> GetTemplatedArgumentTypes() {
  std::vector<FunctionArgumentType> templated_types;
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_ANY_1));
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_ANY_2));
  templated_types.push_back(FunctionArgumentType(ARG_ARRAY_TYPE_ANY_1));
  templated_types.push_back(FunctionArgumentType(ARG_ARRAY_TYPE_ANY_2));
  templated_types.push_back(FunctionArgumentType(ARG_PROTO_ANY));
  templated_types.push_back(FunctionArgumentType(ARG_STRUCT_ANY));
  templated_types.push_back(FunctionArgumentType(ARG_ENUM_ANY));
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_ARBITRARY));
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_RELATION));
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_MODEL));
  templated_types.push_back(FunctionArgumentType(ARG_TYPE_CONNECTION));
  return templated_types;
}

static std::vector<FunctionArgumentType> GetNonTemplatedArgumentTypes(
    TypeFactory* factory) {
  std::vector<FunctionArgumentType> non_templated_types;
  non_templated_types.push_back(FunctionArgumentType(ARG_TYPE_VOID));
  // A few examples of ARG_TYPE_FIXED
  non_templated_types.push_back(FunctionArgumentType(factory->get_int32()));
  non_templated_types.push_back(FunctionArgumentType(factory->get_string()));
  return non_templated_types;
}

TEST(FunctionSignatureTests, TestIsTemplatedArgument) {
  TypeFactory factory;
  struct TestCase {
    FunctionArgumentType arg_type;
    bool expected_is_templated;
  };
  std::vector<TestCase> tests;

  // If a new enum value is added to SignatureArgumentKind then it *must*
  // be added to <templated_kinds> or <non_templated_kinds> as appropriate.
  ASSERT_EQ(17, SignatureArgumentKind_ARRAYSIZE);

  std::set<SignatureArgumentKind> templated_kinds;
  templated_kinds.insert(ARG_TYPE_ANY_1);
  templated_kinds.insert(ARG_TYPE_ANY_2);
  templated_kinds.insert(ARG_ARRAY_TYPE_ANY_1);
  templated_kinds.insert(ARG_ARRAY_TYPE_ANY_2);
  templated_kinds.insert(ARG_PROTO_MAP_ANY);
  templated_kinds.insert(ARG_PROTO_MAP_KEY_ANY);
  templated_kinds.insert(ARG_PROTO_MAP_VALUE_ANY);
  templated_kinds.insert(ARG_PROTO_ANY);
  templated_kinds.insert(ARG_STRUCT_ANY);
  templated_kinds.insert(ARG_ENUM_ANY);
  templated_kinds.insert(ARG_TYPE_ARBITRARY);
  templated_kinds.insert(ARG_TYPE_RELATION);
  templated_kinds.insert(ARG_TYPE_MODEL);
  templated_kinds.insert(ARG_TYPE_CONNECTION);
  templated_kinds.insert(ARG_TYPE_DESCRIPTOR);

  std::set<SignatureArgumentKind> non_templated_kinds;
  non_templated_kinds.insert(ARG_TYPE_FIXED);
  non_templated_kinds.insert(ARG_TYPE_VOID);

  for (const FunctionArgumentType& type : GetTemplatedArgumentTypes()) {
    tests.push_back({type, true});
  }

  for (const FunctionArgumentType& type :
           GetNonTemplatedArgumentTypes(&factory)) {
    tests.push_back({type, false});
  }

  // Relation type arguments that have a relation schema defined (in options)
  // are non-templated.
  TVFRelation tvf_relation({});
  FunctionArgumentType arg_type =
      FunctionArgumentType::RelationWithSchema(
          tvf_relation, /*extra_relation_input_columns_allowed=*/false);
  tests.push_back({arg_type, false});

  arg_type =
      FunctionArgumentType::RelationWithSchema(
          tvf_relation, /*extra_relation_input_columns_allowed=*/true);
  tests.push_back({arg_type, false});

  for (const auto& test : tests) {
    EXPECT_EQ(test.expected_is_templated,
              test.arg_type.IsTemplated()) << test.arg_type.DebugString();
  }
}

TEST(FunctionSignatureTests, TestIsTemplatedSignature) {
  TypeFactory factory;
  struct TestCase {
    FunctionSignature signature;
    bool expected_is_templated;
  };
  std::vector<TestCase> tests;

  FunctionArgumentTypeList arguments;
  tests.push_back({FunctionSignature(FunctionArgumentType(factory.get_int32()),
                                     arguments,
                                     /*context_ptr=*/nullptr),
                   /*expected_is_templated=*/false});

  arguments.push_back(FunctionArgumentType(factory.get_int32()));
  arguments.push_back(FunctionArgumentType(factory.get_int64()));
  arguments.push_back(FunctionArgumentType(factory.get_bytes()));
  tests.push_back({FunctionSignature(FunctionArgumentType(factory.get_string()),
                                     arguments,
                                     /*context_ptr=*/nullptr),
                   /*expected_is_templated=*/false});

  for (const FunctionArgumentType& type : GetTemplatedArgumentTypes()) {
    // A signature with a single argument of this templated type.
    tests.push_back(
        {FunctionSignature(FunctionArgumentType(factory.get_int32()),
                           {type},
                           /*context_ptr=*/nullptr),
         /*expected_is_templated=*/true});
    // A signature with a some fixed arguments and also this templated type.
    FunctionArgumentTypeList arguments_with_template = arguments;
    arguments_with_template.push_back(type);
    tests.push_back(
        {FunctionSignature(FunctionArgumentType(factory.get_int32()),
                           arguments_with_template,
                           /*context_ptr=*/nullptr),
         /*expected_is_templated=*/true});
  }

  for (const auto& test : tests) {
    EXPECT_EQ(test.expected_is_templated,
              test.signature.IsTemplated()) << test.signature.DebugString();
  }
}

TEST(FunctionSignatureTests, TestIsDescriptorTableOffsetArgumentValid) {
  TypeFactory factory;
  std::unique_ptr<FunctionSignature> signature;
  TVFRelation tvf_relation({});
  FunctionArgumentType arg_type = FunctionArgumentType::RelationWithSchema(
      tvf_relation, /*extra_relation_input_columns_allowed=*/false);
  FunctionArgumentType retuneType = FunctionArgumentType(factory.get_int32());
  signature.reset(new FunctionSignature(
      retuneType, {arg_type, FunctionArgumentType::AnyDescriptor(0)}, -1));

  ZETASQL_EXPECT_OK(signature->IsValid());

  EXPECT_DEBUG_DEATH(
      signature.reset(new FunctionSignature(
          retuneType, {FunctionArgumentType::AnyDescriptor(3), arg_type}, -1)),
      "should point to a valid table argument");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }

  EXPECT_DEBUG_DEATH(
      signature.reset(new FunctionSignature(
          retuneType, {arg_type, FunctionArgumentType::AnyDescriptor(1)}, -1)),
      "should point to a valid table argument");
  if (!ZETASQL_DEBUG_MODE) {
    EXPECT_FALSE(signature->IsValid().ok());
  }
}

}  // namespace zetasql
