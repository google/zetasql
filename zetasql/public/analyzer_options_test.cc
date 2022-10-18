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

#include "zetasql/public/analyzer_options.h"

#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST(AnalyzerOptionsTest, CollateFeatureValidation) {
  AnalyzerOptions options;
  ZETASQL_EXPECT_OK(ValidateAnalyzerOptions(options));

  LanguageOptions language;
  language.SetEnabledLanguageFeatures({FEATURE_V_1_3_COLLATION_SUPPORT});
  options.set_language(language);
  EXPECT_TRUE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_3_COLLATION_SUPPORT));
  EXPECT_FALSE(options.language().LanguageFeatureEnabled(
      FEATURE_V_1_3_ANNOTATION_FRAMEWORK));
  EXPECT_THAT(ValidateAnalyzerOptions(options),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("COLLATION_SUPPORT language feature requires "
                                 "the ANNOTATION_FRAMEWORK language feature")));
}

}  // namespace zetasql
