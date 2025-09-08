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

#include "zetasql/common/builtin_function_internal.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

using ::testing::Eq;
using ::testing::Key;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::StatusIs;

TEST(BuiltinFunctionInternalTest, InsertSimpleTableValuedFunction) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_string()}, -1});
  ZETASQL_ASSERT_OK(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                            signatures, {}));
  EXPECT_THAT(tvfs, UnorderedElementsAre(Key("test_tvf")));
  EXPECT_THAT(tvfs["test_tvf"]->signatures(), SizeIs(1));
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionSignaturesIsEmpty) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  ZETASQL_ASSERT_OK(
      InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf", {}, {}));
  EXPECT_THAT(tvfs, UnorderedElementsAre());
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionSignaturesAreDisabled) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_string()}, -1});
  options.exclude_function_ids.emplace(static_cast<FunctionSignatureId>(-1));
  ZETASQL_ASSERT_OK(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                            signatures, {}));
  EXPECT_THAT(tvfs, UnorderedElementsAre());
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionSignaturesHaveRewriteOptions) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION,
                        {type_factory.get_string()},
                        -1,
                        FunctionSignatureOptions().set_rewrite_options(
                            FunctionSignatureRewriteOptions())});
  EXPECT_THAT(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                              signatures, {}),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionsWithExcludeOptions) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_string()}, -2});
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_bool()}, -1});
  options.exclude_function_ids.insert(static_cast<FunctionSignatureId>(-1));
  ZETASQL_ASSERT_OK(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                            signatures, {}));
  EXPECT_THAT(tvfs, UnorderedElementsAre(Key("test_tvf")));
  EXPECT_THAT(tvfs["test_tvf"]->signatures(), SizeIs(1));
  EXPECT_THAT(
      tvfs["test_tvf"]->GetSignature(0)->DebugString("test_tvf"),
      Eq(FunctionSignature(signatures.at(0).Get()).DebugString("test_tvf")));
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionsWithIncludeOptions) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_string()}, -2});
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_bool()}, -1});
  options.include_function_ids.insert(static_cast<FunctionSignatureId>(-1));
  ZETASQL_ASSERT_OK(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                            signatures, {}));
  EXPECT_THAT(tvfs, UnorderedElementsAre(Key("test_tvf")));
  EXPECT_THAT(tvfs["test_tvf"]->signatures(), SizeIs(1));
  EXPECT_THAT(
      tvfs["test_tvf"]->GetSignature(0)->DebugString("test_tvf"),
      Eq(FunctionSignature(signatures.at(1).Get()).DebugString("test_tvf")));
}

TEST(BuiltinFunctionInternalTest,
     InsertSimpleTableValuedFunctionSignaturesRewriteEnabled) {
  TypeFactory type_factory;
  absl::flat_hash_map<std::string, std::unique_ptr<TableValuedFunction>> tvfs;
  ZetaSQLBuiltinFunctionOptions options;
  std::vector<FunctionSignatureOnHeap> signatures;
  signatures.push_back({ARG_TYPE_RELATION, {type_factory.get_string()}, -1});
  options.rewrite_enabled.emplace(static_cast<FunctionSignatureId>(-1), true);
  EXPECT_THAT(InsertSimpleTableValuedFunction(&tvfs, options, "test_tvf",
                                              signatures, {}),
              StatusIs(absl::StatusCode::kInternal));
}

}  // namespace
}  // namespace zetasql
