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

#include "zetasql/public/functions/percentile.h"

#include <cmath>
#include <deque>
#include <limits>
#include <string>

#include "zetasql/common/string_util.h"
#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"

namespace zetasql {
namespace {
template <typename T> using limits = std::numeric_limits<T>;

constexpr double kNaN = limits<double>::quiet_NaN();
constexpr double kInf = limits<double>::infinity();
constexpr double kDoubleMax = limits<double>::max();
constexpr double kDoubleMin = limits<double>::min();
constexpr double kDoubleDenormalMin = limits<double>::denorm_min();
constexpr double kDoubleDenormalMax = kDoubleMin - kDoubleDenormalMin;

TEST(PercentileTest, ComputePercentileIndex) {
  struct TestItem {
    double percentile;
    size_t max_index;
    size_t expected_index;
    long double expected_left_weight;
    long double expected_right_weight;
  };
  constexpr size_t kTwoTo62 = 1ULL << 62;
  constexpr size_t kTwoTo63 = 1ULL << 63;
  constexpr long double kTwoToMinus65 = 0.25 / kTwoTo63;
  constexpr long double kTwoToMinus64 = 0.5 / kTwoTo63;
  constexpr long double kTwoToMinus63 = 1.0 / kTwoTo63;
  constexpr long double kTwoToMinus52 = 1.0 / (1ULL << 52);  // double epsilon
  static const TestItem kTestItems[] = {
      // {percentile, max_index, expected_index, expected_left_weight,
      // expected_right_weight}
      {1, 0, 0, 1, 0},
      {1, 1, 1, 1, 0},
      {1, 99, 99, 1, 0},
      {1, 100, 100, 1, 0},
      {1, kTwoTo63 - 1, kTwoTo63 - 1, 1, 0},
      {1, kTwoTo63, kTwoTo63, 1, 0},
      {1, std::numeric_limits<uint64_t>::max() - 1,
       std::numeric_limits<uint64_t>::max() - 1, 1, 0},
      {1, std::numeric_limits<uint64_t>::max(),
       std::numeric_limits<uint64_t>::max(), 1, 0},

      {0.5, 0, 0, 1, 0},
      {0.5, 1, 0, 0.5, 0.5},
      {0.5, 99, 49, 0.5, 0.5},
      {0.5, 100, 50, 1, 0},
      {0.5, kTwoTo63 - 1, kTwoTo63 / 2 - 1, 0.5, 0.5},
      {0.5, kTwoTo63, kTwoTo63 / 2, 1, 0},
      {0.5, std::numeric_limits<uint64_t>::max() - 1, kTwoTo63 - 1, 1, 0},
      {0.5, std::numeric_limits<uint64_t>::max(), kTwoTo63 - 1, 0.5, 0.5},

      {0.25, 0, 0, 1, 0},
      {0.25, 1, 0, 0.75, 0.25},
      {0.25, 99, 24, 0.25, 0.75},
      {0.25, 100, 25, 1, 0},
      {0.25, kTwoTo63 - 1, kTwoTo63 / 4 - 1, 0.25, 0.75},
      {0.25, kTwoTo63, kTwoTo63 / 4, 1, 0},
      {0.25, std::numeric_limits<uint64_t>::max() - 1, kTwoTo62 - 1, 0.5, 0.5},
      {0.25, std::numeric_limits<uint64_t>::max(), kTwoTo62 - 1, 0.25, 0.75},

      {kTwoToMinus63, 0, 0, 1, 0},
      {kTwoToMinus63, 1, 0, 1.0L - kTwoToMinus63, kTwoToMinus63},
      {kTwoToMinus63, 99, 0, 1.0L - 99 * kTwoToMinus63, 99 * kTwoToMinus63},
      {kTwoToMinus63, 100, 0, 1.0L - 100 * kTwoToMinus63, 100 * kTwoToMinus63},
      {kTwoToMinus63, kTwoTo63 - 1, 0, kTwoToMinus63, 1.0L - kTwoToMinus63},
      {kTwoToMinus63, kTwoTo63, 1, 1, 0},
      {kTwoToMinus63, std::numeric_limits<uint64_t>::max() - 1, 1,
       2 * kTwoToMinus63, 1.0L - 2 * kTwoToMinus63},
      {kTwoToMinus63, std::numeric_limits<uint64_t>::max(), 1, kTwoToMinus63,
       1.0L - kTwoToMinus63},

      {7 * kTwoToMinus65, 0, 0, 1, 0},
      {7 * kTwoToMinus65, 1, 0, 1.0L - 7 * kTwoToMinus65, 7 * kTwoToMinus65},
      {7 * kTwoToMinus65, 99, 0, 1.0L - 99 * 7 * kTwoToMinus65,
       99 * 7 * kTwoToMinus65},
      {7 * kTwoToMinus65, 100, 0, 1.0L - 100 * 7 * kTwoToMinus65,
       100 * 7 * kTwoToMinus65},
      {7 * kTwoToMinus65, kTwoTo63 - 1, 1, 0.25 + 7 * kTwoToMinus65,
       0.75 - 7 * kTwoToMinus65},
      {7 * kTwoToMinus65, kTwoTo63, 1, 0.25, 0.75},
      {7 * kTwoToMinus65, std::numeric_limits<uint64_t>::max() - 1, 3,
       0.5 + 14 * kTwoToMinus65, 0.5 - 14 * kTwoToMinus65},
      {7 * kTwoToMinus65, std::numeric_limits<uint64_t>::max(), 3,
       0.5 + 7 * kTwoToMinus65, 0.5 - 7 * kTwoToMinus65},

      {1 - kTwoToMinus52, 0, 0, 1, 0},
      {1 - kTwoToMinus52, 1, 0, kTwoToMinus52, 1.0L - kTwoToMinus52},
      {1 - kTwoToMinus52, 99, 98, 99 * kTwoToMinus52, 1 - 99 * kTwoToMinus52},
      {1 - kTwoToMinus52, 100, 99, 100 * kTwoToMinus52,
       1 - 100 * kTwoToMinus52},
      {1 - kTwoToMinus52, kTwoTo63 - 1, kTwoTo63 - (1ULL << 11) - 1,
       1 - kTwoToMinus52, kTwoToMinus52},
      {1 - kTwoToMinus52, kTwoTo63, kTwoTo63 - (1ULL << 11), 1, 0},
      {1 - kTwoToMinus52, std::numeric_limits<uint64_t>::max() - 1,
       std::numeric_limits<uint64_t>::max() - (1ULL << 12) - 1,
       1 - 2 * kTwoToMinus52, 2 * kTwoToMinus52},
      {1 - kTwoToMinus52, std::numeric_limits<uint64_t>::max(),
       std::numeric_limits<uint64_t>::max() - (1ULL << 12), 1 - kTwoToMinus52,
       kTwoToMinus52},
  };

  int test_index = 0;
  for (const TestItem& item : kTestItems) {
    SCOPED_TRACE(absl::StrCat("test_index=", test_index, " percentile=",
                              RoundTripDoubleToString(item.percentile),
                              " max_index=", item.max_index));
    ++test_index;
    long double left_weight = -1;
    long double right_weight = -1;
    ZETASQL_ASSERT_OK_AND_ASSIGN(PercentileEvaluator percentile_evalutor,
                         PercentileEvaluator::Create(item.percentile));
    EXPECT_EQ(item.expected_index,
              percentile_evalutor.ComputePercentileIndex(
                  item.max_index, &left_weight, &right_weight));
    // Intentionally use EXPECT_EQ rather than EXPECT_DOUBLE_EQ or EXPECT_NEAR.
    // The results are expected to be precisely equal.
    EXPECT_EQ(item.expected_left_weight, left_weight);
    EXPECT_EQ(item.expected_right_weight, right_weight);
    EXPECT_EQ(1, left_weight + right_weight);

    const double complement_percentile = 1 - item.percentile;
    // When item.percentile is too small, then 1 - item.percentile = 1,
    // and we can't test the complement.
    if (1 - complement_percentile == item.percentile) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(PercentileEvaluator complement_percentile_evalutor,
                           PercentileEvaluator::Create(complement_percentile));
      const size_t index =
          complement_percentile_evalutor.ComputePercentileIndex(
              item.max_index, &left_weight, &right_weight);
      if (item.expected_right_weight > 0) {
        EXPECT_EQ(item.max_index - item.expected_index - 1, index);
        EXPECT_EQ(item.expected_right_weight, left_weight);
        EXPECT_EQ(item.expected_left_weight, right_weight);
      } else {
        EXPECT_EQ(item.max_index - item.expected_index, index);
        EXPECT_EQ(1, left_weight);
        EXPECT_EQ(0, right_weight);
      }
    }
  }

  // Test percentile values so small that the returned index is always 0.
  ASSERT_TRUE(std::isnormal(kDoubleMin));
  ASSERT_FALSE(std::isnormal(kDoubleDenormalMin));
  ASSERT_FALSE(std::isnormal(kDoubleDenormalMax));
  static const double kSmallPercnetiles[] =
      {-0.0, 0.0, kDoubleDenormalMin, kDoubleDenormalMax, kDoubleMin,
       kTwoToMinus64};
  static const size_t kMaxIndexes[] = {
      0, 1, 100, kTwoTo63 - 1, kTwoTo63, std::numeric_limits<uint64_t>::max()};
  for (double percentile : kSmallPercnetiles) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(PercentileEvaluator percentile_evalutor,
                         PercentileEvaluator::Create(percentile));
    for (size_t max_index : kMaxIndexes) {
      SCOPED_TRACE(
          absl::StrCat("percentile=", RoundTripDoubleToString(percentile),
                       " max_index=", max_index));
      long double left_weight = -1;
      long double right_weight = -1;
      EXPECT_EQ(0, percentile_evalutor.ComputePercentileIndex(
          max_index, &left_weight, &right_weight));
      long double expected_right_weight =
          percentile * static_cast<long double>(max_index);
      EXPECT_EQ(expected_right_weight, right_weight);
      EXPECT_EQ(1 - expected_right_weight, left_weight);
    }
  }
}

void VerifyPercentileCont(const std::string& expected_value,
                          std::vector<double> values, size_t num_nulls,
                          double percentile) {
  SCOPED_TRACE(absl::StrCat("percentile=", RoundTripDoubleToString(percentile),
                            " num_nulls=", num_nulls));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PercentileEvaluator percentile_evalutor,
                       PercentileEvaluator::Create(percentile));
  {
    double result = -1234;
    std::string actual_value = "NULL";
    if (percentile_evalutor.ComputePercentileCont<false>(
        values.begin(), values.end(), num_nulls, &result)) {
      actual_value = RoundTripDoubleToString(result);
    }
    EXPECT_EQ(expected_value, actual_value);
  }

  struct IsNaN {
    bool operator()(double value) const {
      return std::isnan(value);
    }
  };
  auto itr = std::partition(values.begin(), values.end(), IsNaN());
  std::sort(itr, values.end());
  {
    double result = -1234;
    std::string actual_value = "NULL";
    if (percentile_evalutor.ComputePercentileCont<true>(
        values.cbegin(), values.cend(), num_nulls, &result)) {
      actual_value = RoundTripDoubleToString(result);
    }
    EXPECT_EQ(expected_value, actual_value);
  }
}

TEST(PercentileTest, ComputePercentileCont) {
  // Args: expected_value, values, num_nulls, percentile

  // Empty inputs.
  VerifyPercentileCont("NULL", {}, 0, 0);
  VerifyPercentileCont("NULL", {}, 0, 0.5);
  VerifyPercentileCont("NULL", {}, 0, 1);

  // Only nulls.
  // Percentile at null.
  VerifyPercentileCont("NULL", {}, 1, 0);
  // Percentile between 2 nulls.
  VerifyPercentileCont("NULL", {}, 10, 0.5);
  // Percentile at null.
  VerifyPercentileCont("NULL", {}, std::numeric_limits<uint64_t>::max(), 1);

  // Only nans.
  // Percentile at nan.
  VerifyPercentileCont("nan", {kNaN}, 0, 0);
  // Percentile between 2 nans.
  VerifyPercentileCont("nan", {kNaN, kNaN, kNaN, kNaN}, 0, 0.5);
  // Percentile at nan.
  VerifyPercentileCont("nan", {kNaN, kNaN, kNaN}, 0, 1);

  // One normal input. Percentile at the input.
  VerifyPercentileCont("1", {1}, 0, 0);
  VerifyPercentileCont("-1", {-1}, 0, 0.5);
  VerifyPercentileCont("0", {0}, 0, 1);

  // [null, null, normal]
  // Percentile at null.
  VerifyPercentileCont("NULL", {0}, 2, 0);
  // Percentile between 2 nulls.
  VerifyPercentileCont("NULL", {kInf}, 2, 0.25);
  // Percentile at null.
  VerifyPercentileCont("NULL", {kInf}, 2, 0.5);
  // Percentile between null and 100.
  VerifyPercentileCont("100", {100}, 2, 0.5000001);
  // Percentile at 100.
  VerifyPercentileCont("100", {100}, 2, 1);

  // [null, nan, normal]
  // Percentile at null.
  VerifyPercentileCont("NULL", {kInf, kNaN}, 1, 0);
  // Percentile between null and nan.
  VerifyPercentileCont("nan", {kInf, kNaN}, 1, 0.25);
  // Percentile at nan.
  VerifyPercentileCont("nan", {kInf, kNaN}, 1, 0.5);
  // Percentile between nan and inf.
  VerifyPercentileCont("nan", {kInf, kNaN}, 1, 0.75);
  // Percentile at inf.
  VerifyPercentileCont("inf", {kInf, kNaN}, 1, 1);

  // [null, -inf, -inf, inf, inf]
  const std::vector<double> inf_values = {kInf, -kInf, -kInf, kInf};
  // Percentile at null.
  VerifyPercentileCont("NULL", inf_values, 1, 0);
  // Percentile between null and -inf.
  VerifyPercentileCont("-inf", inf_values, 1, 0.1);
  // Percentile at -inf.
  VerifyPercentileCont("-inf", inf_values, 1, 0.25);
  // Percentile between 2 -infs.
  VerifyPercentileCont("-inf", inf_values, 1, 0.4);
  // Percentile between -inf and inf.
  VerifyPercentileCont("nan", inf_values, 1, 0.6);
  // Percentile at inf.
  VerifyPercentileCont("inf", inf_values, 1, 0.75);
  // Percentile between 2 infs.
  VerifyPercentileCont("inf", inf_values, 1, 0.8);

  // 1 nan and 8 normal inputs.
  const std::vector<double> values = {
      1, 101, kDoubleDenormalMin, kNaN, kDoubleMax, kInf, 0, -kDoubleMax, -kInf
  };
  // Percentile at nan.
  VerifyPercentileCont("nan", values, 0, 0);
  // Percentile between nan and -inf.
  VerifyPercentileCont("nan", values, 0, 0.06);
  // Percentile at -inf.
  VerifyPercentileCont("-inf", values, 0, 0.125);
  // Percentile between -inf and -kDoubleMax.
  VerifyPercentileCont("-inf", values, 0, 0.15);
  // Percentile at -kDoubleMax.
  VerifyPercentileCont("-1.7976931348623157e+308", values, 0, 0.25);
  // Percentile between -kDoubleMax and 0.
  VerifyPercentileCont("-1.0786158809173895e+308", values, 0, 0.3);
  // Percentile at 0.
  VerifyPercentileCont("0", values, 0, 0.375);
  // Percentile between 0 and kDoubleDenormalMin.
  VerifyPercentileCont("0", values, 0, 0.4375);
  // Percentile at kDoubleDenormalMin.
  VerifyPercentileCont("4.9406564584124654e-324", values, 0, 0.5);
  // Percentile at 1.
  VerifyPercentileCont("1", values, 0, 0.625);
  // Percentile between 1 and 101.
  VerifyPercentileCont("26", values, 0, 0.65625);
  // Percentile between 1 and 101.
  VerifyPercentileCont("51", values, 0, 0.6875);
  // Percentile at 101.
  VerifyPercentileCont("101", values, 0, 0.75);
  // Percentile at kDoubleMax.
  VerifyPercentileCont("1.7976931348623157e+308", values, 0, 0.875);
  // Percentile between kDoubleMax and inf.
  VerifyPercentileCont("inf", values, 0, 0.875001);
  // Percentile at inf.
  VerifyPercentileCont("inf", values, 0, 1);
}

constexpr size_t kNumValues = 8;

static const int kShuffledIndexes[kNumValues] = {3, 4, 6, 2, 1, 7, 0, 5};

template <typename T>
struct PercentileDiscTest : public ::testing::Test {
 protected:
  void VerifyPercentileDisc(size_t expected_value_index, const T* sorted_values,
                            size_t num_values, const int* shuffled_indexes,
                            size_t num_nulls, double percentile);
  void VerifyNonNulls(size_t expected_value_index, size_t num_nulls,
                          double percentile) {
    VerifyPercentileDisc(expected_value_index, kSortedValues, kNumValues,
                         kShuffledIndexes, num_nulls, percentile);
  }
  void VerifyAllNulls(size_t num_nulls, double percentile) {
    VerifyPercentileDisc(0, nullptr, 0, nullptr, num_nulls, percentile);
  }
 private:
  static const T kSortedValues[kNumValues];
};

using PercentileDiscTypes = ::testing::Types<
    int64_t, int32_t, uint64_t, uint32_t, double, float, bool, absl::string_view>;
TYPED_TEST_SUITE(PercentileDiscTest, PercentileDiscTypes);

template <> const int64_t PercentileDiscTest<int64_t>::kSortedValues[] = {
    limits<int64_t>::min(), limits<int64_t>::min() + 1, -10, 0, 1,
    10, limits<int64_t>::max() - 1, limits<int64_t>::max()
};

template <> const int32_t PercentileDiscTest<int32_t>::kSortedValues[] = {
    limits<int32_t>::min(), limits<int32_t>::min() + 1, -10, 0, 1,
    10, limits<int32_t>::max() - 1, limits<int32_t>::max()
};

template <> const uint64_t PercentileDiscTest<uint64_t>::kSortedValues[] = {
    0, 1, 2, 100, 128, 1000, limits<uint64_t>::max() - 1, limits<uint64_t>::max()
};

template <> const uint32_t PercentileDiscTest<uint32_t>::kSortedValues[] = {
    0, 1, 2, 100, 128, 1000, limits<uint32_t>::max() - 1, limits<uint32_t>::max()
};

template <> const double PercentileDiscTest<double>::kSortedValues[] = {
    kNaN, -kInf, -kDoubleMax, 0, kDoubleDenormalMin, kDoubleMin, kDoubleMax,
    kInf
};

template <> const float PercentileDiscTest<float>::kSortedValues[] = {
    limits<float>::quiet_NaN(), -limits<float>::infinity(),
    limits<float>::lowest(), 0, limits<float>::min(), limits<float>::epsilon(),
    limits<float>::max(), limits<float>::infinity()
};

template <> const bool PercentileDiscTest<bool>::kSortedValues[] = {
    false, true, true, true, true, true, true, true
};

template <> const absl::string_view
PercentileDiscTest<absl::string_view>::kSortedValues[] = {
    "", absl::string_view("\0", 1), "!@#$%", "()", "A",
    "Abcdefghijklmnopqrstuvwxyz", "a", "\xFF"
};

template <typename T>
void PercentileDiscTest<T>::VerifyPercentileDisc(
    size_t expected_value_index, const T* sorted_values, size_t num_values,
    const int* shuffled_indexes, size_t num_nulls, double percentile) {
  SCOPED_TRACE(absl::StrCat("percentile=", RoundTripDoubleToString(percentile),
                            " num_nulls=", num_nulls));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PercentileEvaluator percentile_evalutor,
                       PercentileEvaluator::Create(percentile));
  {
    const T* result = percentile_evalutor.ComputePercentileDisc<T, true>(
        sorted_values, sorted_values + num_values, num_nulls);
    EXPECT_EQ(expected_value_index, result - sorted_values);
  }

  {
    // Use deque instead of vector, to avoid the specialization vector<bool>,
    // whose iterator doesn't dereference to bool.
    std::deque<T> unsorted_values(num_values);
    for (int i = 0; i < num_values; ++i) {
      unsorted_values[shuffled_indexes[i]] = sorted_values[i];
    }
    auto result_itr = percentile_evalutor.ComputePercentileDisc<T, false>(
        unsorted_values.begin(), unsorted_values.end(),
        num_nulls);
    if (expected_value_index != num_values) {
      EXPECT_EQ(testing::PrintToString(sorted_values[expected_value_index]),
                testing::PrintToString(*result_itr));
    }
    EXPECT_EQ(expected_value_index, result_itr - unsorted_values.begin());
  }
}

TYPED_TEST(PercentileDiscTest, AllNulls) {
  this->VerifyAllNulls(0, 0);
  this->VerifyAllNulls(0, 0.5);
  this->VerifyAllNulls(0, 1);

  this->VerifyAllNulls(1, 0);
  this->VerifyAllNulls(1, 0.5);
  this->VerifyAllNulls(1, 1);

  this->VerifyAllNulls(std::numeric_limits<uint64_t>::max(), 0);
  this->VerifyAllNulls(std::numeric_limits<uint64_t>::max(), 0.5);
  this->VerifyAllNulls(std::numeric_limits<uint64_t>::max(), 1);
}

TYPED_TEST(PercentileDiscTest, NonEmpty) {
  // 8 non-nulls
  this->VerifyNonNulls(0, 0, 0);
  this->VerifyNonNulls(0, 0, kDoubleDenormalMin);
  this->VerifyNonNulls(0, 0, 0.125);
  this->VerifyNonNulls(1, 0, 0.12500000001);
  this->VerifyNonNulls(1, 0, 0.25);
  this->VerifyNonNulls(2, 0, 0.25000000001);
  this->VerifyNonNulls(2, 0, 0.375);
  this->VerifyNonNulls(3, 0, 0.37500000001);
  this->VerifyNonNulls(3, 0, 0.5);
  this->VerifyNonNulls(4, 0, 0.50000000001);
  this->VerifyNonNulls(4, 0, 0.625);
  this->VerifyNonNulls(5, 0, 0.62500000001);
  this->VerifyNonNulls(5, 0, 0.75);
  this->VerifyNonNulls(6, 0, 0.75000000001);
  this->VerifyNonNulls(6, 0, 0.875);
  this->VerifyNonNulls(7, 0, 0.87500000001);
  this->VerifyNonNulls(7, 0, 1);

  // 24 nulls and 8 non-nulls
  this->VerifyNonNulls(8, 24, 0);
  this->VerifyNonNulls(8, 24, 0.1);
  this->VerifyNonNulls(8, 24, 0.5);
  this->VerifyNonNulls(8, 24, 0.75);
  this->VerifyNonNulls(0, 24, 0.75000000001);
  this->VerifyNonNulls(3, 24, 0.875);
  this->VerifyNonNulls(4, 24, 0.87500000001);
  this->VerifyNonNulls(7, 24, 1);
}

}  // namespace
}  // namespace zetasql
