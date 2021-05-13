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
  // If at some point there needs to be, then we can add a whitelist for
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
