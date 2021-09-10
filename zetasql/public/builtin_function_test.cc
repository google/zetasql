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

#include "zetasql/public/builtin_function.h"

#include <utility>
#include <vector>

#include "zetasql/base/enum_utils.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"

namespace zetasql {

using testing::IsNull;
using testing::NotNull;

using NameToFunctionMap = std::map<std::string, std::unique_ptr<Function>>;

TEST(SimpleBuiltinFunctionTests, ConstructWithProtoTest) {
  ZetaSQLBuiltinFunctionOptionsProto proto =
      ZetaSQLBuiltinFunctionOptionsProto::default_instance();
  ZetaSQLBuiltinFunctionOptions null_option(proto);
  EXPECT_EQ(0, null_option.exclude_function_ids.size());
  EXPECT_EQ(0, null_option.include_function_ids.size());

  proto.add_include_function_ids(FunctionSignatureId::FN_ABS_DOUBLE);
  proto.add_include_function_ids(FunctionSignatureId::FN_ADD_DOUBLE);
  proto.add_include_function_ids(FunctionSignatureId::FN_EQUAL);
  proto.add_exclude_function_ids(FunctionSignatureId::FN_ABS_DOUBLE);
  proto.add_exclude_function_ids(FunctionSignatureId::FN_AND);
  proto.mutable_language_options()->add_enabled_language_features(
      LanguageFeature::FEATURE_TABLESAMPLE);
  proto.mutable_language_options()->add_supported_statement_kinds(
      ResolvedNodeKind::RESOLVED_AGGREGATE_FUNCTION_CALL);

  ZetaSQLBuiltinFunctionOptions option1(proto);
  EXPECT_EQ(2, option1.exclude_function_ids.size());
  EXPECT_EQ(3, option1.include_function_ids.size());
  EXPECT_TRUE(
      option1.include_function_ids.find(FunctionSignatureId::FN_EQUAL) !=
          option1.include_function_ids.end());
  EXPECT_TRUE(
      option1.include_function_ids.find(FunctionSignatureId::FN_ADD_DOUBLE) !=
          option1.include_function_ids.end());
  EXPECT_TRUE(
      option1.include_function_ids.find(FunctionSignatureId::FN_ABS_DOUBLE) !=
          option1.include_function_ids.end());
  EXPECT_TRUE(
      option1.exclude_function_ids.find(FunctionSignatureId::FN_ABS_DOUBLE) !=
          option1.exclude_function_ids.end());
  EXPECT_TRUE(
      option1.exclude_function_ids.find(FunctionSignatureId::FN_AND) !=
          option1.exclude_function_ids.end());
  EXPECT_TRUE(option1.language_options.LanguageFeatureEnabled(
      LanguageFeature::FEATURE_TABLESAMPLE));
  EXPECT_FALSE(option1.language_options.LanguageFeatureEnabled(
        LanguageFeature::FEATURE_ANALYTIC_FUNCTIONS));
  EXPECT_TRUE(option1.language_options.SupportsStatementKind(
      ResolvedNodeKind::RESOLVED_AGGREGATE_FUNCTION_CALL));
  EXPECT_FALSE(option1.language_options.SupportsStatementKind(
      ResolvedNodeKind::RESOLVED_ASSERT_ROWS_MODIFIED));
}

TEST(SimpleBuiltinFunctionTests, ClassAndProtoSize) {
  EXPECT_EQ(2 * sizeof(absl::flat_hash_set<FunctionSignatureId,
                                           FunctionSignatureIdHasher>),
            sizeof(ZetaSQLBuiltinFunctionOptions) - sizeof(LanguageOptions))
      << "The size of ZetaSQLBuiltinFunctionOptions class has changed, "
      << "please also update the proto and serialization code if you "
      << "added/removed fields in it.";
  EXPECT_EQ(
      3, ZetaSQLBuiltinFunctionOptionsProto::descriptor()->field_count())
      << "The number of fields in ZetaSQLBuiltinFunctionOptionsProto has "
      << "changed, please also update the serialization code accordingly.";
}

void ValidateFunction(const LanguageOptions& language_options,
                      absl::string_view function_name,
                      const Function& function) {
  // GetZetaSQLFunctions should all be builtin functions.
  EXPECT_TRUE(function.IsZetaSQLBuiltin());

  // None of the built-in function names/aliases should start with
  // "[a-zA-Z]_", which is reserved for user-defined objects.
  ASSERT_GE(function_name.size(), 2);
  EXPECT_NE('_', function_name[1]) << function_name;

  absl::string_view alias_name = function.alias_name();
  if (!alias_name.empty()) {
    ASSERT_GE(alias_name.size(), 2);
    EXPECT_NE('_', alias_name[1]) << alias_name;
  }

  for (int i = 0; i < 5; ++i) {
    std::vector<InputArgumentType> args(i);
    if (!args.empty()) {
      args[0] = InputArgumentType::LambdaInputArgumentType();
    }
    function.CheckPreResolutionArgumentConstraints(args, language_options)
        .IgnoreError();
    function.GetNoMatchingFunctionSignatureErrorMessage(
        args, language_options.product_mode());
  }
  int ignored;
  function.GetSupportedSignaturesUserFacingText(language_options, &ignored);
}

TEST(SimpleBuiltinFunctionTests, SanityTests) {
  TypeFactory type_factory;
  NameToFunctionMap functions;

  // These settings retrieve the maximum set of functions/signatures
  // possible.
  LanguageOptions language_options;
  language_options.EnableMaximumLanguageFeaturesForDevelopment();
  language_options.set_product_mode(PRODUCT_INTERNAL);
  // Get all the relevant functions for this 'language_options'.
  GetZetaSQLFunctions(&type_factory, language_options, &functions);

  for (const auto& [name, function] : functions) {
    ValidateFunction(language_options, name, *function);
  }

  // Make sure expected functions are found via name mapping.
  for (FunctionSignatureId id :
           zetasql_base::EnumerateEnumValues<FunctionSignatureId>()) {
    // Skip invalid ids.
    switch (id) {
      case __FunctionSignatureId__switch_must_have_a_default__:
      case FN_INVALID_FUNCTION_ID:
        continue;
      // TODO: Remove FN_TIME_FROM_STRING when there are no more
      // references to it.
      case FN_TIME_FROM_STRING:
        continue;
      default:
        break;
    }
    const std::string function_name =
        FunctionSignatureIdToName(static_cast<FunctionSignatureId>(id));

    EXPECT_THAT(zetasql_base::FindOrNull(functions, function_name), NotNull())
        << "Not found (id " << id << "): " << function_name;
  }
}

TEST(SimpleBuiltinFunctionTests, BasicTests) {
  TypeFactory type_factory;
  NameToFunctionMap functions;

  GetZetaSQLFunctions(&type_factory, LanguageOptions(), &functions);

  std::unique_ptr<Function>* function =
      zetasql_base::FindOrNull(functions, "current_timestamp");
  ASSERT_THAT(function, NotNull());
  EXPECT_EQ(1, (*function)->NumSignatures());

  function = zetasql_base::FindOrNull(functions, "$add");
  ASSERT_THAT(function, NotNull());
  EXPECT_EQ(5, (*function)->NumSignatures());
  EXPECT_FALSE((*function)->SupportsSafeErrorMode());

  function = zetasql_base::FindOrNull(functions, "mod");
  ASSERT_THAT(function, NotNull());
  EXPECT_EQ(2, (*function)->NumSignatures());
  EXPECT_TRUE((*function)->SupportsSafeErrorMode());

  function = zetasql_base::FindOrNull(functions, "$case_with_value");
  ASSERT_THAT(function, NotNull());
  ASSERT_EQ(1, (*function)->NumSignatures());
  ASSERT_EQ(4, (*function)->GetSignature(0)->arguments().size());
  ASSERT_EQ(ARG_TYPE_ANY_1,
            (*function)->GetSignature(0)->result_type().kind());
  EXPECT_FALSE((*function)->SupportsSafeErrorMode());

  function = zetasql_base::FindOrNull(functions, "$case_no_value");
  ASSERT_THAT(function, NotNull());
  ASSERT_EQ(1, (*function)->NumSignatures());
  ASSERT_EQ(3, (*function)->GetSignature(0)->arguments().size());
  ASSERT_EQ(ARG_TYPE_ANY_1,
            (*function)->GetSignature(0)->result_type().kind());
  EXPECT_FALSE((*function)->SupportsSafeErrorMode());

  function = zetasql_base::FindOrNull(functions, "concat");
  ASSERT_THAT(function, NotNull());
  ASSERT_EQ(2, (*function)->NumSignatures());
  EXPECT_TRUE((*function)->SupportsSafeErrorMode());
}

TEST(SimpleBuiltinFunctionTests, ExcludedBuiltinFunctionTests) {
  TypeFactory type_factory;
  NameToFunctionMap all_functions;
  NameToFunctionMap functions;

  ZetaSQLBuiltinFunctionOptions options;
  GetZetaSQLFunctions(&type_factory, options, &all_functions);

  // Remove all CONCAT signatures.
  options.exclude_function_ids.insert(FN_CONCAT_STRING);
  options.exclude_function_ids.insert(FN_CONCAT_BYTES);

  // Remove few signatures for MULTIPLY.
  options.exclude_function_ids.insert(FN_MULTIPLY_INT64);
  options.exclude_function_ids.insert(FN_MULTIPLY_DOUBLE);
  options.exclude_function_ids.insert(FN_MULTIPLY_NUMERIC);
  options.exclude_function_ids.insert(FN_MULTIPLY_BIGNUMERIC);

  // Remove all signatures for SUBTRACT.
  options.exclude_function_ids.insert(FN_SUBTRACT_DOUBLE);
  options.exclude_function_ids.insert(FN_SUBTRACT_INT64);
  options.exclude_function_ids.insert(FN_SUBTRACT_UINT64);
  options.exclude_function_ids.insert(FN_SUBTRACT_NUMERIC);
  options.exclude_function_ids.insert(FN_SUBTRACT_BIGNUMERIC);
  options.exclude_function_ids.insert(FN_SUBTRACT_DATE_INT64);

  // Remove few signature for ADD but not all.
  options.exclude_function_ids.insert(FN_ADD_UINT64);
  options.exclude_function_ids.insert(FN_ADD_NUMERIC);
  options.exclude_function_ids.insert(FN_ADD_BIGNUMERIC);
  options.exclude_function_ids.insert(FN_ADD_DATE_INT64);
  options.exclude_function_ids.insert(FN_ADD_INT64_DATE);

  // Get filtered functions.
  GetZetaSQLFunctions(&type_factory, options, &functions);

  std::vector<std::string> functions_not_in_all_functions;
  std::vector<std::string> all_functions_not_in_functions;

  for (const auto& function : all_functions) {
    if (!zetasql_base::ContainsKey(functions, function.first)) {
      all_functions_not_in_functions.push_back(function.first);
    }
  }
  for (const auto& function : functions) {
    if (!zetasql_base::ContainsKey(all_functions, function.first)) {
      functions_not_in_all_functions.push_back(function.first);
    }
  }

  // Two functions excluded completely.
  EXPECT_EQ(2, all_functions.size() - functions.size())
      << "all_functions.size(): " << all_functions.size()
      << "\nfunctions.size(): " << functions.size()
      << "\nfunctions not in all_functions: "
      << absl::StrJoin(functions_not_in_all_functions, ",")
      << "\nall_functions not in functions: "
      << absl::StrJoin(all_functions_not_in_functions, ",");

  // The only signature for CONCAT was removed so no CONCAT function.
  std::unique_ptr<Function>* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_CONCAT_STRING));
  EXPECT_THAT(function, IsNull());

  // All signatures for SUBTRACT were removed so no SUBTRACT function.
  function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_SUBTRACT_INT64));
  EXPECT_THAT(function, IsNull());

  // Two MULTIPLY signatures were removed.
  function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_MULTIPLY_UINT64));
  EXPECT_THAT(function, NotNull());
  ASSERT_EQ(1, (*function)->NumSignatures());
  EXPECT_EQ("(UINT64, UINT64) -> UINT64",
            (*function)->GetSignature(0)->DebugString());

  // One ADD signature was excluded, two remain.
  function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_ADD_INT64));
  EXPECT_THAT(function, NotNull());
  ASSERT_EQ(2, (*function)->NumSignatures());
  EXPECT_EQ("(INT64, INT64) -> INT64",
            (*function)->GetSignature(0)->DebugString());
  EXPECT_EQ("(DOUBLE, DOUBLE) -> DOUBLE",
            (*function)->GetSignature(1)->DebugString());
}

TEST(SimpleBuiltinFunctionTests, IncludedBuiltinFunctionTests) {
  TypeFactory type_factory;
  NameToFunctionMap functions;
  ZetaSQLBuiltinFunctionOptions options;

  // Include signature for CONCAT.
  options.include_function_ids.insert(FN_CONCAT_STRING);
  // Include one signature for MULTIPLY.
  options.include_function_ids.insert(FN_MULTIPLY_INT64);
  // Include two signatures for ADD.
  options.include_function_ids.insert(FN_ADD_INT64);
  options.include_function_ids.insert(FN_ADD_DOUBLE);

  GetZetaSQLFunctions(&type_factory, options, &functions);

  EXPECT_EQ(3, functions.size());

  std::unique_ptr<Function>* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_CONCAT_STRING));
  EXPECT_THAT(function, NotNull());

  // One MULTIPLY signature was included.
  function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_MULTIPLY_INT64));
  EXPECT_THAT(function, NotNull());
  EXPECT_EQ(1, (*function)->NumSignatures());
  EXPECT_EQ("(INT64, INT64) -> INT64",
            (*function)->GetSignature(0)->DebugString());

  // Two of five ADD signatures were included.
  function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_ADD_INT64));
  EXPECT_THAT(function, NotNull());
  EXPECT_EQ(2, (*function)->NumSignatures());
  EXPECT_EQ("(INT64, INT64) -> INT64",
            (*function)->GetSignature(0)->DebugString());
  EXPECT_EQ("(DOUBLE, DOUBLE) -> DOUBLE",
            (*function)->GetSignature(1)->DebugString());
}

TEST(SimpleBuiltinFunctionTests,
     ExcludeBuiltinFunctionSignatureBySupportedTypeTests) {
  TypeFactory type_factory;
  ZetaSQLBuiltinFunctionOptions options;
  // Include signature for timestamp().
  options.include_function_ids.insert(FN_TIMESTAMP_FROM_STRING);
  options.include_function_ids.insert(FN_TIMESTAMP_FROM_DATE);
  options.include_function_ids.insert(FN_TIMESTAMP_FROM_DATETIME);

  {
    NameToFunctionMap functions;
    // FEATURE_V_1_2_CIVIL_TIME is not enabled, the function timestamp() should
    // have only two signatures.
    options.language_options.DisableAllLanguageFeatures();
    GetZetaSQLFunctions(&type_factory, options, &functions);
    std::unique_ptr<Function>* function =
        zetasql_base::FindOrNull(functions, "timestamp");
    ASSERT_TRUE(function != nullptr);
    ASSERT_EQ(2, (*function)->NumSignatures());
    // Assume the order of the signatures is the same as they are added.
    EXPECT_EQ(FN_TIMESTAMP_FROM_STRING,
              (*function)->GetSignature(0)->context_id());
    EXPECT_EQ(FN_TIMESTAMP_FROM_DATE,
              (*function)->GetSignature(1)->context_id());
  }

  {
    NameToFunctionMap functions;
    // Enabling FEATURE_V_1_2_CIVIL_TIME should allow all three signatures to be
    // included for the function timestamp().
    options.language_options.SetEnabledLanguageFeatures(
        {FEATURE_V_1_2_CIVIL_TIME});
    GetZetaSQLFunctions(&type_factory, options, &functions);
    std::unique_ptr<Function>* function =
        zetasql_base::FindOrNull(functions, "timestamp");
    ASSERT_TRUE(function != nullptr);
    ASSERT_EQ(3, (*function)->NumSignatures());
    // Assume the order of the signatures is the same as they are added.
    EXPECT_EQ(FN_TIMESTAMP_FROM_STRING,
              (*function)->GetSignature(0)->context_id());
    EXPECT_EQ(FN_TIMESTAMP_FROM_DATE,
              (*function)->GetSignature(1)->context_id());
    EXPECT_EQ(FN_TIMESTAMP_FROM_DATETIME,
              (*function)->GetSignature(2)->context_id());
  }
}

TEST(SimpleBuiltinFunctionTests, IncludedAndExcludedBuiltinFunctionTests) {
  TypeFactory type_factory;
  NameToFunctionMap functions;
  ZetaSQLBuiltinFunctionOptions options;

  // Include two of three signatures for ADD, but exclude one of them.
  options.include_function_ids.insert(FN_ADD_INT64);
  options.include_function_ids.insert(FN_ADD_DOUBLE);
  options.exclude_function_ids.insert(FN_ADD_DOUBLE);

  GetZetaSQLFunctions(&type_factory, options, &functions);

  EXPECT_EQ(1, functions.size());

  std::unique_ptr<Function>* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_ADD_INT64));
  EXPECT_THAT(function, NotNull());
  EXPECT_EQ(1, (*function)->NumSignatures());
  EXPECT_EQ("(INT64, INT64) -> INT64",
            (*function)->GetSignature(0)->DebugString());
}

TEST(SimpleBuiltinFunctionTests, LanguageOptions) {
  LanguageOptions language_options;

  TypeFactory type_factory;
  NameToFunctionMap functions;

  // With default LanguageOptions, we won't get analytic functions.
  GetZetaSQLFunctions(&type_factory, language_options, &functions);
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_COUNT)));
  EXPECT_FALSE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_RANK)));

  language_options.EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  language_options.EnableMaximumLanguageFeatures();

  functions.clear();
  GetZetaSQLFunctions(&type_factory, language_options, &functions);
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_COUNT)));
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_RANK)));

  // Now test combination of LanguageOptions, inclusions and exclusions.
  // Without enabling FEATURE_ANALYTIC_FUNCTIONS, we don't get FN_RANK, even
  // if we put it on the include_function_ids.
  ZetaSQLBuiltinFunctionOptions options{LanguageOptions()};
  EXPECT_FALSE(options.language_options.LanguageFeatureEnabled(
      FEATURE_ANALYTIC_FUNCTIONS));
  options.include_function_ids.insert(FN_RANK);
  options.include_function_ids.insert(FN_MAX);

  functions.clear();
  GetZetaSQLFunctions(&type_factory, options, &functions);
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_MAX)));
  EXPECT_FALSE(
      zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_COUNT)));
  EXPECT_FALSE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_RANK)));
  EXPECT_FALSE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_LEAD)));

  // When we enable FEATURE_ANALYTIC_FUNCTIONS, inclusion lists apply to
  // analytic functions.
  options.language_options.EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);

  functions.clear();
  GetZetaSQLFunctions(&type_factory, options, &functions);
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_MAX)));
  EXPECT_FALSE(
      zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_COUNT)));
  EXPECT_TRUE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_RANK)));
  EXPECT_FALSE(zetasql_base::ContainsKey(functions, FunctionSignatureIdToName(FN_LEAD)));
}

TEST(SimpleBuiltinFunctionTests, NumericFunctions) {
  TypeFactory type_factory;
  NameToFunctionMap functions;

  // Verify that numeric functions are available when NUMERIC type is enabled.
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_NUMERIC_TYPE);

  // Limit ABS signatures to just one to make it easier to test below.
  ZetaSQLBuiltinFunctionOptions options{language_options};
  options.include_function_ids.insert(FN_ABS_NUMERIC);

  GetZetaSQLFunctions(&type_factory, options, &functions);
  std::unique_ptr<Function>* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(FN_ABS_NUMERIC));
  EXPECT_THAT(function, NotNull());
  EXPECT_EQ(1, (*function)->NumSignatures());
  EXPECT_EQ("(NUMERIC) -> NUMERIC",
            (*function)->GetSignature(0)->DebugString());
}

static ArgumentConstraintsCallback GetPreResolutionArgumentConstraints(
    FunctionSignatureId function_id, const NameToFunctionMap& functions) {
  std::unique_ptr<Function> const* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(function_id));
  if (function == nullptr) {
    return nullptr;
  }
  return (*function)->PreResolutionConstraints();
}

static PostResolutionArgumentConstraintsCallback
GetPostResolutionArgumentConstraints(FunctionSignatureId function_id,
                                     const NameToFunctionMap& functions) {
  std::unique_ptr<Function> const* function =
      zetasql_base::FindOrNull(functions, FunctionSignatureIdToName(function_id));
  if (function == nullptr) {
    return nullptr;
  }
  return (*function)->PostResolutionConstraints();
}

TEST(SimpleFunctionTests, TestCheckArgumentConstraints) {
  TypeFactory type_factory;
  NameToFunctionMap functions;
  ZetaSQLBuiltinFunctionOptions options;

  GetZetaSQLFunctions(&type_factory, options, &functions);

  const Type* int64_type = types::Int64Type();
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();
  const Type* bool_type = types::BoolType();

  const Type* enum_type;
  ZETASQL_ASSERT_OK(type_factory.MakeEnumType(zetasql_test__::TestEnum_descriptor(),
                                      &enum_type));

  const StructType* struct_type;
  ZETASQL_ASSERT_OK(type_factory.MakeStructType(
      {{"a", string_type}, {"b", bytes_type}}, &struct_type));

  const ArrayType* array_type;
  ZETASQL_ASSERT_OK(type_factory.MakeArrayType(int64_type, &array_type));

  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeProtoType(zetasql_test__::KitchenSinkPB::descriptor(),
                                 &proto_type));

  Value delimiter = Value::String(", ");

  FunctionSignature dummy_signature{int64_type,
                                    {{int64_type, /*num_occurrences=*/1},
                                     {int64_type, /*num_occurrences=*/1}},
                                    /*context_id=*/-1};
  const std::vector<FunctionSignatureId> function_ids =
      {FN_LESS, FN_LESS_OR_EQUAL, FN_GREATER_OR_EQUAL, FN_GREATER, FN_EQUAL,
       FN_NOT_EQUAL};
  for (const FunctionSignatureId function_id : function_ids) {
    ASSERT_THAT(GetPreResolutionArgumentConstraints(function_id, functions),
                IsNull());
    PostResolutionArgumentConstraintsCallback post_constraints =
        GetPostResolutionArgumentConstraints(function_id, functions);
    ASSERT_THAT(post_constraints, NotNull());
    ZETASQL_EXPECT_OK(post_constraints(
        dummy_signature,
        {InputArgumentType(int64_type), InputArgumentType(int64_type)},
        LanguageOptions()));

    ZETASQL_EXPECT_OK(post_constraints(
        dummy_signature,
        {InputArgumentType(bool_type), InputArgumentType(bool_type)},
        LanguageOptions()));
    ZETASQL_EXPECT_OK(post_constraints(
        dummy_signature,
        {InputArgumentType(enum_type), InputArgumentType(enum_type)},
        LanguageOptions()));

    const absl::Status struct_type_status = post_constraints(
        dummy_signature,
        {InputArgumentType(struct_type), InputArgumentType(struct_type)},
        LanguageOptions());
    LanguageOptions language_options;

    language_options.EnableLanguageFeature(FEATURE_V_1_1_ARRAY_EQUALITY);
    const absl::Status array_type_status = post_constraints(
        dummy_signature,
        {InputArgumentType(array_type), InputArgumentType(array_type)},
        LanguageOptions());
    const absl::Status array_type_status_with_equality_support =
        post_constraints(
            dummy_signature,
            {InputArgumentType(array_type), InputArgumentType(array_type)},
            language_options);

    if (function_id == FN_EQUAL || function_id == FN_NOT_EQUAL) {
      ZETASQL_EXPECT_OK(struct_type_status);
      EXPECT_FALSE(array_type_status.ok());
      ZETASQL_EXPECT_OK(array_type_status_with_equality_support);
    } else {
      EXPECT_FALSE(struct_type_status.ok());
      EXPECT_FALSE(array_type_status.ok());
    }

    EXPECT_FALSE(post_constraints(dummy_signature,
                                  {InputArgumentType(proto_type),
                                   InputArgumentType(proto_type)},
                                  LanguageOptions())
                     .ok());
  }

  auto constraints = GetPreResolutionArgumentConstraints(FN_MIN, functions);
  ASSERT_THAT(constraints, NotNull());
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(int64_type)}, LanguageOptions()));
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(enum_type)}, LanguageOptions()));
  EXPECT_FALSE(
      constraints({InputArgumentType(struct_type)}, LanguageOptions()).ok());
  EXPECT_FALSE(
      constraints({InputArgumentType(array_type)}, LanguageOptions()).ok());
  EXPECT_FALSE(
      constraints({InputArgumentType(proto_type)}, LanguageOptions()).ok());

  constraints = GetPreResolutionArgumentConstraints(FN_MAX, functions);
  ASSERT_THAT(constraints, NotNull());
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(int64_type)}, LanguageOptions()));
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(enum_type)}, LanguageOptions()));
  EXPECT_FALSE(
      constraints({InputArgumentType(struct_type)}, LanguageOptions()).ok());
  EXPECT_FALSE(
      constraints({InputArgumentType(array_type)}, LanguageOptions()).ok());
  EXPECT_FALSE(
      constraints({InputArgumentType(proto_type)}, LanguageOptions()).ok());

  constraints = GetPreResolutionArgumentConstraints(FN_ARRAY_AGG, functions);
  ASSERT_THAT(constraints, NotNull());
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(int64_type)}, LanguageOptions()));
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(enum_type)}, LanguageOptions()));
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(struct_type)}, LanguageOptions()));
  EXPECT_FALSE(
      constraints({InputArgumentType(array_type)}, LanguageOptions()).ok());
  ZETASQL_EXPECT_OK(constraints({InputArgumentType(proto_type)}, LanguageOptions()));
}

TEST(SimpleFunctionTests, HideFunctionsForExternalMode) {
  TypeFactory type_factory;
  ZetaSQLBuiltinFunctionOptions options;
  for (int i = 0; i < 2; ++i) {
    bool is_external = (i == 1);
    if (is_external) {
      options.language_options.set_product_mode(PRODUCT_EXTERNAL);
    }
    NameToFunctionMap functions;
    GetZetaSQLFunctions(&type_factory, options, &functions);

    // NORMALIZE and NORMALIZE_AND_CASEFOLD take an enum type but are expected
    // to be available in external mode.
    EXPECT_THAT(functions, testing::Contains(testing::Key("normalize")));
    EXPECT_THAT(functions,
                testing::Contains(testing::Key("normalize_and_casefold")));

    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.format_ip") == nullptr);
    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.parse_ip") == nullptr);
    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.format_packed_ip") == nullptr);
    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.parse_packed_ip") == nullptr);
    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.ip_in_net") == nullptr);
    EXPECT_EQ(is_external,
              zetasql_base::FindOrNull(functions, "net.make_net") == nullptr);
    EXPECT_NE(nullptr, zetasql_base::FindOrNull(functions, "array_length"));
  }
}

TEST(SimpleFunctionTests, TestFunctionSignaturesForUnintendedCoercion) {
  TypeFactory type_factory;
  const Type* int32_type = type_factory.get_int32();
  const Type* int64_type = type_factory.get_int64();
  const Type* uint32_type = type_factory.get_uint32();
  const Type* uint64_type = type_factory.get_uint64();
  const Type* float_type = type_factory.get_float();
  const Type* double_type = type_factory.get_double();

  const Function::Mode SCALAR = Function::SCALAR;

  // This is what the original, problematic signature for the 'sign'
  // function was.  We detect that this function has a risky signature
  // of signed integer and floating point, without unsigned integer
  // signatures.
  Function function("sign", Function::kZetaSQLFunctionGroupName, SCALAR,
                    { {int32_type, {int32_type}, FN_SIGN_INT32},
                      {int64_type, {int64_type}, FN_SIGN_INT64},
                      {float_type, {float_type}, FN_SIGN_FLOAT},
                      {double_type, {double_type}, FN_SIGN_DOUBLE}},
                    FunctionOptions());
  EXPECT_TRUE(FunctionMayHaveUnintendedArgumentCoercion(&function))
      << function.DebugString();

  // The checks work across corresponding arguments.  This fails because the
  // second argument only supports signed and floating point types.
  Function function2("fn2", Function::kZetaSQLFunctionGroupName, SCALAR,
                     { {int32_type, {int32_type, int32_type},
                          nullptr /* context_ptr */},
                       {int64_type, {int64_type, int64_type},
                          nullptr /* context_ptr */},
                       {uint32_type, {uint32_type, int32_type},
                          nullptr /* context_ptr */},
                       {uint64_type, {uint64_type, int64_type},
                          nullptr /* context_ptr */},
                       {float_type, {float_type, float_type},
                          nullptr /* context_ptr */},
                       {double_type, {double_type, double_type},
                          nullptr /* context_ptr */}},
                     FunctionOptions());
  EXPECT_TRUE(FunctionMayHaveUnintendedArgumentCoercion(&function2))
      << function2.DebugString();

  // The check works across signatures with different numbers of arguments.
  // For this function, the problematic argument is the third argument, and
  // only some of the signatures have a third argument.
  Function function3("fn3", Function::kZetaSQLFunctionGroupName, SCALAR,
                     { {int32_type, {int64_type, int32_type, int32_type},
                          nullptr /* context_ptr */},
                       {int64_type, {int64_type, int64_type},
                          nullptr /* context_ptr */},
                       {uint32_type, {int32_type, uint32_type, int32_type},
                          nullptr /* context_ptr */},
                       {uint64_type, {uint64_type, int64_type},
                          nullptr /* context_ptr */},
                       {float_type, {uint32_type, float_type, float_type},
                          nullptr /* context_ptr */},
                       {double_type, {double_type, double_type},
                          nullptr /* context_ptr */}},
                     FunctionOptions());
  EXPECT_TRUE(FunctionMayHaveUnintendedArgumentCoercion(&function3))
      << function3.DebugString();

  // The test function does not currently catch a potential problem in this
  // function's signatures since it has all of signed, unsigned, and floating
  // point arguments... even though it allows possibly inadvertent coercions
  // from an UINT64 argument to the DOUBLE signature.
  Function function4("fn4", Function::kZetaSQLFunctionGroupName, SCALAR,
                     { {int32_type, {int32_type}, nullptr /* context_ptr */},
                       {int64_type, {int64_type}, nullptr /* context_ptr */},
                       {uint32_type, {uint32_type}, nullptr /* context_ptr */},
                       {float_type, {float_type}, nullptr /* context_ptr */},
                       {double_type, {double_type}, nullptr /* context_ptr */}},
                     FunctionOptions());
  EXPECT_FALSE(FunctionMayHaveUnintendedArgumentCoercion(&function4))
      << function4.DebugString();
}

TEST(SimpleFunctionTests,
     TestZetaSQLBuiltinFunctionSignaturesForUnintendedCoercion) {
  // This test is intended to catch signatures that may have unintended
  // argument coercions.  Specifically, it looks for functions that have
  // INT64 and DOUBLE signatures that allow arguments to be coerced, but
  // no UINT64 signature.  Such cases are risky, since they allow implicit
  // coercion from UINT64 to DOUBLE, which in most cases is unintended.
  //
  // There are currently no ZetaSQL functions that violate this test.
  // If at some point there needs to be, then we can add an exception for
  // those functions.
  //
  // Note that this analysis does not currently apply any argument
  // constraints that might be present, so it's possible that a function
  // could have arguments that violate this test but whose argument
  // constraints later exclude types the avoid unintended coercion.
  //
  // Note that this only tests ZetaSQL functions - it does not test
  // any function signatures that engines may have added for extensions.
  TypeFactory type_factory;
  NameToFunctionMap functions;

  // These settings retrieve all the functions and signatures.
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  options.set_product_mode(PRODUCT_INTERNAL);

  GetZetaSQLFunctions(&type_factory, options, &functions);

  for (const auto& function_entry : functions) {
    const Function* function = function_entry.second.get();
    // If this test fails, then the function must be examined closely
    // to determine whether or not its signatures are correct, and that
    // there is no unintended argument coercion being allowed (particularly
    // for unsigned integer arguments).
    EXPECT_FALSE(FunctionMayHaveUnintendedArgumentCoercion(function))
        << function->DebugString();
  }
}

}  // namespace zetasql
