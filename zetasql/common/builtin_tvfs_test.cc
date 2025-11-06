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

#include <memory>

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/builtins_output_properties.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace {

class GetVectorSearchTableValuedFunctionsTest : public ::testing::Test {
 protected:
  GetVectorSearchTableValuedFunctionsTest() {
    language_options_.EnableLanguageFeature(FEATURE_VECTOR_SEARCH_TVF);
    options_ =
        std::make_unique<ZetaSQLBuiltinFunctionOptions>(language_options_);
  }

  TypeFactory type_factory_;
  BuiltinsOutputProperties output_properties_;
  LanguageOptions language_options_;
  std::unique_ptr<ZetaSQLBuiltinFunctionOptions> options_;
};

TEST_F(GetVectorSearchTableValuedFunctionsTest, VectorSearchFunction) {
  NameToTableValuedFunctionMap functions;
  ZETASQL_ASSERT_OK(GetVectorSearchTableValuedFunctions(&type_factory_, *options_,
                                                &functions));
  constexpr absl::string_view kVectorSearch = "vector_search";
  ASSERT_TRUE(functions.contains(kVectorSearch));
  EXPECT_EQ(
      functions[kVectorSearch]->DebugString(),
      "ZetaSQL:vector_search\n  (ANY TABLE, STRING column_to_search, ANY "
      "TABLE, optional "
      "STRING query_column_to_search, optional INT64 top_k, optional STRING "
      "distance_type, optional DOUBLE max_distance, optional STRING options) "
      "-> "
      "ANY TABLE");
}

}  // namespace
}  // namespace zetasql
