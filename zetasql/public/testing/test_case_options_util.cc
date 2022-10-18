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

#include "zetasql/public/testing/test_case_options_util.h"

#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

absl::StatusOr<LanguageOptions::LanguageFeatureSet> GetRequiredLanguageFeatures(
    const file_based_test_driver::TestCaseOptions& test_case_options) {
  LanguageOptions::LanguageFeatureSet enabled_set;
  const std::string features_string =
      test_case_options.GetString(kLanguageFeatures);
  if (!features_string.empty()) {
    const std::vector<std::string> feature_list =
        absl::StrSplit(features_string, ',');
    for (const std::string& feature_name : feature_list) {
      const std::string full_feature_name =
          absl::StrCat("FEATURE_", feature_name);
      LanguageFeature feature;
      ZETASQL_RET_CHECK(LanguageFeature_Parse(full_feature_name, &feature))
          << full_feature_name;
      enabled_set.insert(feature);
    }
  }
  return enabled_set;
}

}  // namespace zetasql
