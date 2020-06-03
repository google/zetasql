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

#include "zetasql/public/numeric_value_test_utils.h"

#include "zetasql/public/numeric_value.h"
#include "gtest/gtest.h"
#include "absl/random/random.h"

namespace zetasql {

template <typename T>
void TestMakeRandomCoverage(absl::BitGen* random) {
  // covered[sign][i][j] = true iff there is at least one random value with
  // sign (0=non-negative; 1=negative), exactly i integer digits and exactly j
  // fractional digits.
  bool covered[2][T::kMaxIntegerDigits + 1][T::kMaxFractionalDigits + 1];
  memset(covered, 0, sizeof(covered));
  for (int i = 0; i < 100000; ++i) {
    T v = MakeRandomNumericValue<T>(random);
    std::string str = v.ToString();
    ASSERT_TRUE(!str.empty());
    absl::string_view str_view = str;
    bool negative = false;
    if (str[0] == '-') {
      negative = true;
      str_view.remove_prefix(1);
    }
    size_t num_integer_digits = 0;
    size_t num_fractional_digits = 0;
    auto decimal_point_pos = str_view.find('.');
    if (decimal_point_pos == absl::string_view::npos) {
      num_integer_digits = str_view.size();
    } else {
      num_integer_digits = decimal_point_pos;
      num_fractional_digits = str_view.size() - decimal_point_pos - 1;
    }
    if (num_integer_digits == 1 && str_view[0] == '0') {
      num_integer_digits = 0;
    }
    ASSERT_LE(num_integer_digits, T::kMaxIntegerDigits);
    ASSERT_LE(num_fractional_digits, T::kMaxFractionalDigits);
    covered[negative][num_integer_digits][num_fractional_digits] = true;
  }
  size_t num_covered = 0;
  for (const auto& table : covered) {
    for (const auto& row : table) {
      for (bool cell : row) {
        num_covered += cell;
      }
    }
  }
  // covered[true][0][0] is always false, and hence subtract 1.
  constexpr size_t kNumPossibleCombinations =
      2 * (T::kMaxIntegerDigits + 1) * (T::kMaxFractionalDigits + 1) - 1;
  // Should cover at least 90% of the possile combinations.
  // In an experiment with 200 runs, the minimum coverage is 99.9% for
  // BigNumericValue and 100% for NumericValue.
  EXPECT_GE(num_covered, 9 * kNumPossibleCombinations / 10);
}

TEST(NumericTest, MakeRandom_Coverage) {
  absl::BitGen random;
  TestMakeRandomCoverage<NumericValue>(&random);
}

TEST(BigNumericTest, MakeRandom_Coverage) {
  absl::BitGen random;
  TestMakeRandomCoverage<BigNumericValue>(&random);
}

}  // namespace zetasql
