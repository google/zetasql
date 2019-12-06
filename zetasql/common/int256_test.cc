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

#include "zetasql/common/int256.h"

#include <functional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/macros.h"

namespace zetasql {
namespace {
constexpr unsigned __int128 kuint128max = ~static_cast<unsigned __int128>(0);
constexpr __int128 kint128max = kuint128max >> 1;

constexpr int256 kSortedValues[] = {
    int256(~kint128max, 0),
    -int256(kint128max, kuint128max),
    -int256(2001, 1),
    -int256(2000, 2),
    -int256(2000, 1),
    -int256(1, 0),
    -int256(0, kuint128max),
    int256(-2),
    int256(-1),
    int256(),
    int256(1),
    int256(0, 2),
    int256(0, kuint128max),
    int256(1, 0),
    int256(2000, 1),
    int256(2000, 2),
    int256(2001, 1),
    int256(kint128max, kuint128max),
};

// Manually computed difference between consecutive elemtns in kSortedValues.
constexpr int256 kSortedValueDeltas[] = {
    1,
    int256(kint128max - 2001, kuint128max - 1),
    int256(kuint128max),
    1,
    int256(1999, 1),
    1,
    int256(kuint128max - 2),
    1,
    1,
    1,
    1,
    int256(kuint128max - 2),
    1,
    int256(1999, 1),
    1,
    int256(kuint128max),
    int256(kint128max - 2001, kuint128max - 1),
};

static_assert(ABSL_ARRAYSIZE(kSortedValueDeltas) ==
              ABSL_ARRAYSIZE(kSortedValues) - 1);

constexpr const char* kDecStrings[] = {
    "-5789604461865809771178549250434395392663499233282028201972879200395656481"
    "9968",
    "-5789604461865809771178549250434395392663499233282028201972879200395656481"
    "9967",
    "-680905016208797865390212589470968191123457",
    "-680564733841876926926749214863536422912002",
    "-680564733841876926926749214863536422912001",
    "-340282366920938463463374607431768211456",
    "-340282366920938463463374607431768211455",
    "-2",
    "-1",
    "0",
    "1",
    "2",
    "340282366920938463463374607431768211455",
    "340282366920938463463374607431768211456",
    "680564733841876926926749214863536422912001",
    "680564733841876926926749214863536422912002",
    "680905016208797865390212589470968191123457",
    "57896044618658097711785492504343953926634992332820282019728792003956564819"
    "967",
};

static_assert(ABSL_ARRAYSIZE(kDecStrings) == ABSL_ARRAYSIZE(kSortedValues));

constexpr const char* kHexStrings[] = {
    "-8000000000000000000000000000000000000000000000000000000000000000",
    "-7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    "-7d100000000000000000000000000000001",
    "-7d000000000000000000000000000000002",
    "-7d000000000000000000000000000000001",
    "-100000000000000000000000000000000",
    "-ffffffffffffffffffffffffffffffff",
    "-2",
    "-1",
    "0",
    "1",
    "2",
    "ffffffffffffffffffffffffffffffff",
    "100000000000000000000000000000000",
    "7d000000000000000000000000000000001",
    "7d000000000000000000000000000000002",
    "7d100000000000000000000000000000001",
    "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
};

static_assert(ABSL_ARRAYSIZE(kHexStrings) == ABSL_ARRAYSIZE(kSortedValues));

constexpr const char* kOctStrings[] = {
    "-1000000000000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000",
    "-7777777777777777777777777777777777777777777777777777777777777777777777777"
    "777777777777",
    "-17504000000000000000000000000000000000000000001",
    "-17500000000000000000000000000000000000000000002",
    "-17500000000000000000000000000000000000000000001",
    "-4000000000000000000000000000000000000000000",
    "-3777777777777777777777777777777777777777777",
    "-2",
    "-1",
    "0",
    "1",
    "2",
    "3777777777777777777777777777777777777777777",
    "4000000000000000000000000000000000000000000",
    "17500000000000000000000000000000000000000000001",
    "17500000000000000000000000000000000000000000002",
    "17504000000000000000000000000000000000000000001",
    "77777777777777777777777777777777777777777777777777777777777777777777777777"
    "77777777777",
};

static_assert(ABSL_ARRAYSIZE(kOctStrings) == ABSL_ARRAYSIZE(kSortedValues));

TEST(Int256, Compare) {
  for (int i = 0; i < ABSL_ARRAYSIZE(kSortedValues); ++i) {
    const int256& val = kSortedValues[i];
    EXPECT_EQ(val, val);
    EXPECT_LE(val, val);
    EXPECT_GE(val, val);
    EXPECT_FALSE(val != val);  // NOLINT(readability/check)
    EXPECT_FALSE(val > val);   // NOLINT(readability/check)
    EXPECT_FALSE(val < val);   // NOLINT(readability/check)

    if (i > 0) {
      const int256& smaller_val = kSortedValues[i - 1];
      EXPECT_NE(val, smaller_val);
      EXPECT_GT(val, smaller_val);
      EXPECT_GE(val, smaller_val);
      EXPECT_FALSE(val == smaller_val);  // NOLINT(readability/check)
      EXPECT_FALSE(val < smaller_val);   // NOLINT(readability/check)
      EXPECT_FALSE(val <= smaller_val);  // NOLINT(readability/check)

      EXPECT_NE(smaller_val, val);
      EXPECT_LT(smaller_val, val);
      EXPECT_LE(smaller_val, val);
      EXPECT_FALSE(smaller_val == val);  // NOLINT(readability/check)
      EXPECT_FALSE(smaller_val > val);   // NOLINT(readability/check)
      EXPECT_FALSE(smaller_val >= val);  // NOLINT(readability/check)
    }
  }
}

TEST(Int256, AddAndSubtract) {
  for (int i = 0; i < ABSL_ARRAYSIZE(kSortedValueDeltas); ++i) {
    const int256& d = kSortedValueDeltas[i];
    const int256& x = kSortedValues[i];
    const int256& y = kSortedValues[i + 1];
    EXPECT_EQ(y - x, d);
    EXPECT_EQ(x - y, -d);
    EXPECT_EQ(d + x, y);
    EXPECT_EQ(x + d, y);
    EXPECT_EQ(d - (-x), y);
    EXPECT_EQ(x - (-d), y);
    EXPECT_EQ(-d + (-x), -y);
    EXPECT_EQ(-x + (-d), -y);
  }

  // Test more invariants.
  for (const int256& x : kSortedValues) {
    EXPECT_EQ(x + 0, x);
    EXPECT_EQ(x - 0, x);
    EXPECT_EQ(0 + x, x);
    EXPECT_EQ(0 - x, -x);
    EXPECT_EQ(x - x, 0);
    EXPECT_EQ(x + (-x), 0);
    EXPECT_EQ((-x) + x, 0);

    int256 x_plus_one = x + 1;
    int256 x_minus_one = x - 1;
    EXPECT_EQ(x_plus_one - 1, x);
    EXPECT_EQ(x_minus_one + 1, x);

    int256 x2 = x;
    EXPECT_EQ(x2++, x);
    EXPECT_EQ(x2, x_plus_one);
    EXPECT_EQ(--x2, x);
    EXPECT_EQ(x2, x);
    EXPECT_EQ(++x2, x_plus_one);
    EXPECT_EQ(x2, x_plus_one);
    EXPECT_EQ(x2--, x_plus_one);
    EXPECT_EQ(x2, x);

    for (const int256& y : kSortedValues) {
      const int256 s = x + y;
      EXPECT_EQ(s, y + x);
      EXPECT_EQ(s - x, y);
      EXPECT_EQ(s - y, x);
      EXPECT_EQ(x - s, -y);
      EXPECT_EQ(y - s, -x);
      int256 s2 = x;
      EXPECT_EQ((s2 += y), s);
      EXPECT_EQ(s2, s);
      EXPECT_EQ((s2 -= x), y);
      EXPECT_EQ(s2, y);

      const int256 d = x - y;
      EXPECT_EQ(y + d, x);
      EXPECT_EQ(d + y, x);
      EXPECT_EQ(x - d, y);
      EXPECT_EQ(d - x, -y);
      int256 d2 = x;
      EXPECT_EQ((d2 -= y), d);
      EXPECT_EQ(d2, d);
      EXPECT_EQ((d2 += y), x);
      EXPECT_EQ(d2, x);
    }
  }
}

TEST(Int256, Stream) {
  for (int i = 0; i < ABSL_ARRAYSIZE(kSortedValues); ++i) {
    const int256& val = kSortedValues[i];
    std::ostringstream dec_buf;
    dec_buf << val;
    EXPECT_EQ(dec_buf.str(), kDecStrings[i]);

    std::ostringstream hex_buf;
    hex_buf << std::hex << val;
    EXPECT_EQ(hex_buf.str(), kHexStrings[i]);

    std::ostringstream oct_buf;
    oct_buf << std::oct << val;
    EXPECT_EQ(oct_buf.str(), kOctStrings[i]);

    std::ostringstream hex_showbase_buf;
    hex_showbase_buf << std::showbase << std::hex << val;
    std::string expected_str = kHexStrings[i];
    if (val != 0) {
      expected_str.insert(expected_str[0] == '-', "0x");
    }
    EXPECT_EQ(hex_showbase_buf.str(), expected_str);

    std::ostringstream oct_min_width_buf;
    oct_min_width_buf << std::showbase << std::setw(10) << std::setfill('*')
                      << std::right << std::oct << val;
    expected_str = kOctStrings[i];
    if (val != 0) {
      expected_str.insert(expected_str[0] == '-', "0");
    }
    if (expected_str.size() < 10) {
      expected_str.insert(0, 10 - expected_str.size(), '*');
    }
    EXPECT_EQ(oct_min_width_buf.str(), expected_str);
  }
}

}  // namespace
}  // namespace zetasql
