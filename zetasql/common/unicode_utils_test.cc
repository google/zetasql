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

#include "zetasql/common/unicode_utils.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "absl/flags/flag.h"

namespace zetasql {

TEST(GetNormalizedAndCasefoldedString, SimpleTests) {
  absl::SetFlag(&FLAGS_zetasql_idstring_allow_unicode_characters, true);
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("FIeld1"), "field1");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("A #2&B* _123"),
            "a #2&b* _123");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("ßſA"), "sssa");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("ßſS"), "ssss");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("12 34"), "12 34");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString(
                ",.<>?/\"\';:{}[]|\\+-=_`~!@#$%^&*()"),
            ",.<>?/\"\';:{}[]|\\+-=_`~!@#$%^&*()");
}

TEST(GetNormalizedAndCasefoldedString, SimpleTests_FlagDisabled) {
  absl::SetFlag(&FLAGS_zetasql_idstring_allow_unicode_characters, false);
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("FIeld1"), "field1");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("A #2&B* _123"),
            "a #2&b* _123");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("ßſA"), "ßſa");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("ßſS"), "ßſs");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString("12 34"), "12 34");
  EXPECT_EQ(zetasql::GetNormalizedAndCasefoldedString(
                ",.<>?/\"\';:{}[]|\\+-=_`~!@#$%^&*()"),
            ",.<>?/\"\';:{}[]|\\+-=_`~!@#$%^&*()");
}
}  // namespace zetasql
