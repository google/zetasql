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

#include "zetasql/common/fixed_int.h"

#include <functional>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/macros.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "zetasql/base/mathutil.h"

namespace std {
std::ostream& operator<<(std::ostream& o, unsigned __int128 x) {
  return o << absl::uint128(x);
}

std::ostream& operator<<(std::ostream& o, __int128 x) {
  return x < 0 ? (o << '-' << -absl::uint128(x)) : (o << absl::uint128(x));
}
}  // namespace std

namespace zetasql {
namespace {
using int128 = __int128;
using uint128 = unsigned __int128;
constexpr uint128 kuint128max = ~static_cast<uint128>(0);
constexpr int128 kint128max = kuint128max >> 1;
constexpr int128 kint128min = ~kint128max;
constexpr uint64_t kuint64max = ~static_cast<uint64_t>(0);
constexpr int64_t kint64max = kuint64max >> 1;
constexpr int64_t kint64min = ~kint64max;
constexpr uint64_t kuint32max = ~static_cast<uint32_t>(0);
constexpr int64_t kint32max = kuint32max >> 1;
constexpr int64_t kint32min = ~kint32max;

template <typename V>
constexpr V max128();

template <>
constexpr int128 max128<int128>() {
  return kint128max;
}

template <>
constexpr uint128 max128<uint128>() {
  return kuint128max;
}

template <typename V>
constexpr V min128();

template <>
constexpr int128 min128<int128>() {
  return kint128min;
}

template <>
constexpr uint128 min128<uint128>() {
  return 0;
}

constexpr uint64_t kSorted64BitValues[] = {
    0,
    1,
    1000,
    (1ull << 32) - 1,
    (1ull << 32),
    (1ull << 63) - 1,
    (1ull << 63),
    kuint64max,
};

constexpr std::pair<int64_t, absl::string_view>
    kSortedSerializedSigned64BitValues[] = {
        {-4294967296, {"\x00\x00\x00\x00\xff", 5}},
        {-16777216, {"\x00\x00\x00\xff", 4}},
        {-65536, {"\x00\x00\xff", 3}},
        {-32512, {"\x00\x81", 2}},
        {-32768, {"\x00\x80", 2}},
        {-1000, {"\x18\xfc", 2}},
        {-256, {"\x00\xff", 2}},
        {0, {"\x00", 1}},
        {1, {"\x01", 1}},
        {255, {"\xff\x00", 2}},
        {256, {"\x00\x01", 2}},
        {1000, {"\xe8\x03", 2}},
        {32768, {"\x00\x80\x00", 3}},
        {65536, {"\x00\x00\x01", 3}},
        {16777216, {"\x00\x00\x00\x01", 4}},
        {(1LL << 32) - 1, {"\xff\xff\xff\xff\x00", 5}},
        {(1LL << 32), {"\x00\x00\x00\x00\x01", 5}},
        {kint64max, {"\xff\xff\xff\xff\xff\xff\xff\x7f", 8}},
        {kint64min, {"\x00\x00\x00\x00\x00\x00\x00\x80", 8}},
        {-1, {"\xff", 1}},
};

constexpr std::pair<uint64_t, absl::string_view>
    kSortedSerializedUnsigned64BitValues[] = {
        {0, {"\x00", 1}},
        {1, {"\x01", 1}},
        {255, {"\xff", 1}},
        {256, {"\x00\x01", 2}},
        {1000, {"\xe8\x03", 2}},
        {32768, {"\x00\x80", 2}},
        {65536, {"\x00\x00\x01", 3}},
        {16777216, {"\x00\x00\x00\x01", 4}},
        {(1ULL << 32) - 1, {"\xff\xff\xff\xff", 4}},
        {(1ULL << 32), {"\x00\x00\x00\x00\x01", 5}},
        {(1ULL << 63) - 1, {"\xff\xff\xff\xff\xff\xff\xff\x7f", 8}},
        {(1ULL << 63), {"\x00\x00\x00\x00\x00\x00\x00\x80", 8}},
        {kuint64max, {"\xff\xff\xff\xff\xff\xff\xff\xff", 8}},
};

constexpr int64_t kFixedIntCastDoubleTestData64[] = {
    0,
    // * Lowest 53 bits set
    0x1fffffffffffff,
    // * Lowest 54 bits set
    0x3fffffffffffff,
    // * Lowest 55 bits set
    0x7fffffffffffff,
    // Rounding to nearest and ties to even, bits in comments are the 53rd to
    // 56th bits.
    // Round down to 0x7ffffffffffff800
    0x7ffffffffffff880,  // 0, 0, 0, 1
    0x7ffffffffffff900,  // 0, 0, 1, 0
    0x7ffffffffffff9ff,  // 0, 0, 1, 1
    0x7ffffffffffffa00,  // 0, 1, 0, 0 with trailing zeros. Ties to even
    // Round up/down to 0x7ffffffffffffc00
    0x7ffffffffffffa01,  // 0, 1, 0, 0 with one at the end
    0x7ffffffffffffa80,  // 0, 1, 0, 1
    0x7ffffffffffffd00,  // 1, 0, 1, 0
    0x7ffffffffffffdff,  // 1, 0, 1, 1 with trailing ones
    // Round up to 0x8000000000000000
    0x7ffffffffffffe00,  // 1, 1, 0, 0 with trailing zeros. Ties to even
    0x7ffffffffffffe01,  // 1, 1, 0, 0 with one at the end
    // Most significant bit at different position of words (for 32-bit width)
    0x1ffffffff,
    0x1ffffffffffff,
};

constexpr int128 kFixedIntCastDoubleTestData128[] = {
    static_cast<int128>(absl::MakeUint128(0, 0)),
    // * Lowest 53 bits set
    static_cast<int128>(absl::MakeUint128(0, 0x1fffffffffffff)),
    // * Lowest 54 bits set
    static_cast<int128>(absl::MakeUint128(0, 0x3fffffffffffff)),
    // * Lowest 55 bits set
    static_cast<int128>(absl::MakeUint128(0, 0x7fffffffffffff)),
    // Rounding to nearest and ties to even, bits in comments are the 53rd to
    // 56th bits.
    // Round down to 0x7ffffffffffff800 for first 64 bits
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffff880, 0)),  // 0, 0, 0, 1
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffff900, 0)),  // 0, 0, 1, 0
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffff9ff, 0)),  // 0, 0, 1, 1
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffa00,
                          0)),  // 0, 1, 0, 0 with trailing zeros. Ties to even
    // Round up/down to 0x7ffffffffffffc00 for first 64 bits
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffa00,
                          1)),  // 0, 1, 0, 0 with one at the end
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffa80, 0)),  // 0, 1, 0, 1
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffd00, 0)),  // 1, 0, 1, 0
    static_cast<int128>(absl::MakeUint128(
        0x7ffffffffffffdff,
        0xffffffffffffffff)),  // 1, 0, 1, 1 with trailing ones
    // Round up to 0x8000000000000000 for first 64 bits
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffe00,
                          0)),  // 1, 1, 0, 0 with trailing zeros. Ties to even
    static_cast<int128>(
        absl::MakeUint128(0x7ffffffffffffe00,
                          1)),  // 1, 1, 0, 0 with one at the end
    // Most significant bit at different position of words (for 32/64-bit word)
    static_cast<int128>(absl::MakeUint128(0, 0x1ffffffff)),
    static_cast<int128>(absl::MakeUint128(0, 0x1ffffffffffff)),
    static_cast<int128>(absl::MakeUint128(0, 0xffffffffffffffff)),
    static_cast<int128>(absl::MakeUint128(1, 0xffffffffffffffff)),
    static_cast<int128>(absl::MakeUint128(0xffffffff, 0xffffffff00000000)),
    static_cast<int128>(absl::MakeUint128(0x1ffffffff, 0xffffffff00000000)),
    // 53rd significant bit at different position of words (for 32/64-bit word)
    static_cast<int128>(absl::MakeUint128(0, 0x001fffffffffffff)),
    static_cast<int128>(absl::MakeUint128(0, 0x1fffffffffffff00)),
    static_cast<int128>(absl::MakeUint128(0, 0x1fffffffffffffff)),
    static_cast<int128>(absl::MakeUint128(0, 0xffffffffffffffff)),
    static_cast<int128>(absl::MakeUint128(0xfffff, 0xffffffff80000000)),
    static_cast<int128>(absl::MakeUint128(0x1fffff, 0xffffffff00000000)),
    static_cast<int128>(absl::MakeUint128(0xfffffffffffff, 0x8000000000000000)),
    static_cast<int128>(absl::MakeUint128(0x1fffffffffffff, 0)),
};

void CompareDouble(double actual, double expect) {
  EXPECT_EQ(actual, expect);
  zetasql_base::MathUtil::DoubleParts decomposed_actual = zetasql_base::MathUtil::Decompose(actual);
  zetasql_base::MathUtil::DoubleParts decomposed_expect = zetasql_base::MathUtil::Decompose(expect);
  EXPECT_EQ(decomposed_actual.mantissa, decomposed_expect.mantissa);
  EXPECT_EQ(decomposed_actual.exponent, decomposed_expect.exponent);
}

template <typename T, typename V>
void TestFixedIntCastDoubleForType(V x) {
  T fixed_int_val = T(x);
  CompareDouble(static_cast<double>(fixed_int_val), static_cast<double>(x));
}

template <typename T>
void TestFixedUintCastDouble(T x) {
  TestFixedIntCastDoubleForType<FixedUint<32, 4>>(x);
  TestFixedIntCastDoubleForType<FixedUint<32, 6>>(x);
  TestFixedIntCastDoubleForType<FixedUint<32, 8>>(x);
  TestFixedIntCastDoubleForType<FixedUint<64, 2>>(x);
  TestFixedIntCastDoubleForType<FixedUint<64, 4>>(x);
}

template <typename T>
void TestFixedIntCastDouble(T x) {
  TestFixedIntCastDoubleForType<FixedInt<32, 4>>(x);
  TestFixedIntCastDoubleForType<FixedInt<32, 6>>(x);
  TestFixedIntCastDoubleForType<FixedInt<32, 8>>(x);
  TestFixedIntCastDoubleForType<FixedInt<64, 2>>(x);
  TestFixedIntCastDoubleForType<FixedInt<64, 4>>(x);
}

template <typename T, typename V>
void TestShiftOperatorsForType(V x, V filler) {
  for (uint i = 0; i < 128; ++i) {
    T v = (T(x) >>= i);
    EXPECT_EQ(v, T(x >> i)) << std::hex << std::showbase << x << " >> " << i;

    v = (T(x) <<= i);
    EXPECT_EQ(static_cast<V>(v), x << i)
        << std::hex << std::showbase << x << " << " << i;
    if (T::kNumBits > 128) {
      using SignedWord = std::make_signed_t<typename T::Word>;
      EXPECT_EQ(
          static_cast<SignedWord>(v.number()[128 / sizeof(SignedWord) / 8]),
          i == 0 ? filler : static_cast<SignedWord>(x >> (128 - i)))
          << std::hex << std::showbase << x << " << " << i;
    }
  }

  for (uint i : {static_cast<uint>(T::kNumBits), 0xffffffffu}) {
    T v = (T(x) >>= i);
    EXPECT_EQ(v, T(filler)) << std::hex << std::showbase << x << " >> " << i;

    v = (T(x) <<= i);
    EXPECT_EQ(v, T()) << std::hex << std::showbase << x << " << " << i;
  }
}

void TestShiftOperators(uint128 x) {
  TestShiftOperatorsForType<FixedUint<32, 4>, uint128>(x, 0);
  TestShiftOperatorsForType<FixedUint<32, 5>, uint128>(x, 0);
  TestShiftOperatorsForType<FixedUint<64, 2>, uint128>(x, 0);
  TestShiftOperatorsForType<FixedUint<64, 3>, uint128>(x, 0);
  int128 sx = static_cast<int128>(x);
  int128 filler = sx < 0 ? -1 : 0;
  TestShiftOperatorsForType<FixedInt<32, 4>, int128>(sx, filler);
  TestShiftOperatorsForType<FixedInt<32, 5>, int128>(sx, filler);
  TestShiftOperatorsForType<FixedInt<64, 2>, int128>(sx, filler);
  TestShiftOperatorsForType<FixedInt<64, 3>, int128>(sx, filler);
}

template <typename T, typename V>
void TestComparisonOperatorsForType(V x, V y) {
  T tx(x);
  T ty(y);
  EXPECT_EQ(tx == ty, x == y) << x << " == " << y;
  EXPECT_EQ(tx != ty, x != y) << x << " != " << y;
  EXPECT_EQ(tx < ty, x < y) << x << " < " << y;
  EXPECT_EQ(tx > ty, x > y) << x << " > " << y;
  EXPECT_EQ(tx <= ty, x <= y) << x << " <= " << y;
  EXPECT_EQ(tx >= ty, x >= y) << x << " >= " << y;
}

void TestComparisonOperators(uint128 x, uint128 y) {
  TestComparisonOperatorsForType<FixedUint<32, 4>, uint128>(x, y);
  TestComparisonOperatorsForType<FixedUint<32, 5>, uint128>(x, y);
  TestComparisonOperatorsForType<FixedUint<64, 2>, uint128>(x, y);
  TestComparisonOperatorsForType<FixedUint<64, 3>, uint128>(x, y);

  TestComparisonOperatorsForType<FixedInt<32, 4>, int128>(x, y);
  TestComparisonOperatorsForType<FixedInt<32, 5>, int128>(x, y);
  TestComparisonOperatorsForType<FixedInt<64, 2>, int128>(x, y);
  TestComparisonOperatorsForType<FixedInt<64, 3>, int128>(x, y);
}

template <typename T, typename V>
T GetExpectedSum(V x, V y) {
  bool add128_overflow = y >= 0 ? x > max128<V>() - y : x < min128<V>() - y;
  if (!add128_overflow) {
    return T(x + y);
  }
  T expected(static_cast<V>(y >= 0 ? 1LL : -1LL));
  expected <<= 128;
  return expected += T(static_cast<V>(static_cast<uint128>(x) + y));
}

template <typename T, typename V>
T GetExpectedDiff(V x, V y) {
  bool sub128_overflow = y >= 0 ? x < min128<V>() + y : x > max128<V>() + y;
  if (!sub128_overflow) {
    return T(x - y);
  }
  T expected(static_cast<V>(y >= 0 ? -1LL : 1LL));
  expected <<= 128;
  return expected +=
         T(static_cast<V>(static_cast<uint128>(x) - static_cast<uint128>(y)));
}

template <typename T, typename V>
T GetExpectedQuotient(V x, V y) {
  if (std::is_same_v<V, int128> && x == kint128min && y == -1) {
    // This the only case that causes 128-bit division overflow.
    return T() -= T(x);
  }
  return T(x / y);
}

template <typename T, typename V, typename RhsType = T>
void TestArithmeticOperatorsForType(V x, V y) {
  T v = (T(x) += RhsType(y));
  EXPECT_EQ(v, GetExpectedSum<T>(x, y))
      << std::hex << std::showbase << x << " + " << y;

  v = (T(x) -= RhsType(y));
  EXPECT_EQ(v, GetExpectedDiff<T>(x, y))
      << std::hex << std::showbase << x << " - " << y;

#if !defined(ADDRESS_SANITIZER)
  bool mul128_overflow =
      (y > 0 && (x > max128<V>() / y || x < min128<V>() / y)) ||
      (y < 0 && (x < max128<V>() / y || x > min128<V>() / y));
  v = (T(x) *= RhsType(y));
  if (!mul128_overflow) {
    EXPECT_EQ(v, T(x * y)) << std::hex << std::showbase << x << " * " << y;
  }

  if (y != 0) {
    v = (T(x) /= RhsType(y));
    EXPECT_EQ(v, GetExpectedQuotient<T>(x, y))
        << std::hex << std::showbase << x << " / " << y;
  }
#endif
}

template <typename T, typename V>
void TestArithmeticOperatorsWithSelfForType(V x) {
  T t(x);
  EXPECT_EQ(t += t, GetExpectedSum<T>(x, x))
      << std::hex << std::showbase << x << " + " << x;

  t = T(x);
  EXPECT_EQ(t -= t, T()) << std::hex << std::showbase << x << " - " << x;

#if !defined(ADDRESS_SANITIZER)
  bool mul128_overflow =
      (x > 0 && (x > max128<V>() / x || x < min128<V>() / x)) ||
      (x < 0 && (x < max128<V>() / x || x > min128<V>() / x));
  t = T(x);
  t *= t;
  if (!mul128_overflow) {
    EXPECT_EQ(t, T(x * x)) << std::hex << std::showbase << x << " * " << x;
  }

  if (x != 0) {
    t = T(x);
    EXPECT_EQ(t /= t, T(V{1})) << std::hex << std::showbase << x << " / " << x;
  }
#endif
}

void TestExtendAndMultiply(uint64_t x, uint64_t y) {
  uint128 x_times_y = static_cast<uint128>(x) * y;
  uint32_t x_lo = static_cast<uint32_t>(x);
  uint32_t x_hi = static_cast<uint32_t>(x >> 32);
  uint32_t y_lo = static_cast<uint32_t>(y);
  uint32_t y_hi = static_cast<uint32_t>(y >> 32);
  uint128 x_hi_y = (static_cast<uint128>(x_hi) << 64) | y;
  uint128 x_hi_y_times_x_lo = x_hi_y * x_lo;
  std::array<uint32_t, 1> x_lo_array = {x_lo};
  std::array<uint32_t, 3> x_hi_y_as_array = {y_lo, y_hi, x_hi};

  EXPECT_EQ(ExtendAndMultiply(FixedUint<64, 1>(x), FixedUint<64, 1>(y)),
            (FixedUint<64, 2>(x_times_y)))
      << std::hex << std::showbase << x << " * " << y;
  EXPECT_EQ(ExtendAndMultiply(FixedUint<32, 2>(x), FixedUint<32, 2>(y)),
            (FixedUint<32, 4>(x_times_y)))
      << std::hex << std::showbase << x << " * " << y;
  EXPECT_EQ(ExtendAndMultiply(FixedUint<32, 1>(x_lo_array),
                              FixedUint<32, 3>(x_hi_y_as_array)),
            (FixedUint<32, 4>(x_hi_y_times_x_lo)))
      << std::hex << std::showbase << x_lo << " * " << x_hi_y;
  EXPECT_EQ(ExtendAndMultiply(FixedUint<32, 3>(x_hi_y_as_array),
                              FixedUint<32, 1>(x_lo_array)),
            (FixedUint<32, 4>(x_hi_y_times_x_lo)))
      << std::hex << std::showbase << x_hi_y << " * " << x_lo;

  int64_t sx = static_cast<int64_t>(x);
  int64_t sy = static_cast<int64_t>(y);
  int128 sx_times_sy = static_cast<int128>(sx) * sy;
  int32_t sx_lo = static_cast<int32_t>(x_lo);
  int128 sx_hi_y = (static_cast<int128>(static_cast<int32_t>(x_hi)) << 64) | y;
  int128 sx_hi_y_times_x_lo = sx_hi_y * sx_lo;
  EXPECT_EQ(ExtendAndMultiply(FixedInt<64, 1>(sx), FixedInt<64, 1>(sy)),
            (FixedInt<64, 2>(sx_times_sy)))
      << std::hex << std::showbase << sx << " * " << y;
  EXPECT_EQ(ExtendAndMultiply(FixedInt<32, 2>(sx), FixedInt<32, 2>(sy)),
            (FixedInt<32, 4>(sx_times_sy)))
      << std::hex << std::showbase << sx << " * " << sy;
  EXPECT_EQ(ExtendAndMultiply(FixedInt<32, 1>(x_lo_array),
                              FixedInt<32, 3>(x_hi_y_as_array)),
            (FixedInt<32, 4>(sx_hi_y_times_x_lo)))
      << std::hex << std::showbase << sx_lo << " * " << sx_hi_y;
  EXPECT_EQ(ExtendAndMultiply(FixedInt<32, 3>(x_hi_y_as_array),
                              FixedInt<32, 1>(x_lo_array)),
            (FixedInt<32, 4>(sx_hi_y_times_x_lo)))
      << std::hex << std::showbase << sx_hi_y << " * " << sx_lo;
}

void TestArithmeticOperatorsWithWord(uint128 x, uint64_t y) {
  uint32_t y32 = static_cast<uint32_t>(y);
  TestArithmeticOperatorsForType<FixedUint<32, 4>, uint128, uint32_t>(x, y32);
  TestArithmeticOperatorsForType<FixedUint<32, 5>, uint128, uint32_t>(x, y32);
  TestArithmeticOperatorsForType<FixedUint<64, 2>, uint128, uint64_t>(x, y);
  TestArithmeticOperatorsForType<FixedUint<64, 3>, uint128, uint64_t>(x, y);

  TestArithmeticOperatorsForType<FixedInt<32, 4>, int128, uint32_t>(x, y32);
  TestArithmeticOperatorsForType<FixedInt<32, 5>, int128, uint32_t>(x, y32);
  TestArithmeticOperatorsForType<FixedInt<64, 2>, int128, uint64_t>(x, y);
  TestArithmeticOperatorsForType<FixedInt<64, 3>, int128, uint64_t>(x, y);
}

void TestArithmeticOperators(uint128 x, uint128 y) {
  TestArithmeticOperatorsForType<FixedUint<32, 4>, uint128>(x, y);
  TestArithmeticOperatorsForType<FixedUint<32, 5>, uint128>(x, y);
  TestArithmeticOperatorsForType<FixedUint<64, 2>, uint128>(x, y);
  TestArithmeticOperatorsForType<FixedUint<64, 3>, uint128>(x, y);

  TestArithmeticOperatorsForType<FixedInt<32, 4>, int128>(x, y);
  TestArithmeticOperatorsForType<FixedInt<32, 5>, int128>(x, y);
  TestArithmeticOperatorsForType<FixedInt<64, 2>, int128>(x, y);
  TestArithmeticOperatorsForType<FixedInt<64, 3>, int128>(x, y);
}

void TestArithmeticOperatorsWithSelf(uint128 x) {
  TestArithmeticOperatorsWithSelfForType<FixedUint<32, 4>, uint128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedUint<32, 5>, uint128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedUint<64, 2>, uint128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedUint<64, 3>, uint128>(x);

  TestArithmeticOperatorsWithSelfForType<FixedInt<32, 4>, int128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedInt<32, 5>, int128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedInt<64, 2>, int128>(x);
  TestArithmeticOperatorsWithSelfForType<FixedInt<64, 3>, int128>(x);
}

template <template <int, int> class T, int k1, int n1, int k2, int n2>
T<k1, n1> Convert(const T<k2, n2>& src, bool negative) {
  return T<k1, n1>(fixed_int_internal::Convert<k1, n1, k2, n2, false>(
      src.number(), negative));
}

template <template <int, int> class T, typename V128, typename V64>
void TestConversion(V128 x) {
  std::ostringstream s;
  s << "x = " << std::hex << std::showbase << x;
  SCOPED_TRACE(s.str());
  T<64, 3> x_64_3(x);
  T<64, 2> x_64_2(x);
  T<64, 1> x_64_1(static_cast<V64>(x));
  T<32, 5> x_32_5(x);
  T<32, 4> x_32_4(x);
  T<32, 3> x_32_3(static_cast<V64>(x >> 32));
  x_32_3 <<= 32;
  x_32_3 += static_cast<uint32_t>(x);
  V128 x_lo96 = (x << 32) >> 32;

  EXPECT_EQ((T<64, 3>(x_64_2)), x_64_3);
  EXPECT_EQ((T<64, 2>(x_64_2)), x_64_2);
  EXPECT_EQ((T<64, 1>(x_64_2)), x_64_1);
  EXPECT_EQ((T<32, 5>(x_64_2)), x_32_5);
  EXPECT_EQ((T<32, 4>(x_64_2)), x_32_4);
  EXPECT_EQ((T<32, 3>(x_64_2)), x_32_3);

  EXPECT_EQ((T<64, 3>(x_32_4)), x_64_3);
  EXPECT_EQ((T<64, 2>(x_32_4)), x_64_2);
  EXPECT_EQ((T<64, 1>(x_64_2)), x_64_1);
  EXPECT_EQ((T<32, 5>(x_32_4)), x_32_5);
  EXPECT_EQ((T<32, 4>(x_32_4)), x_32_4);
  EXPECT_EQ((T<32, 3>(x_64_2)), x_32_3);

  EXPECT_EQ((T<64, 3>(x_32_3)), (T<64, 3>(x_lo96)));
  EXPECT_EQ((T<64, 2>(x_32_3)), (T<64, 2>(x_lo96)));
  EXPECT_EQ((T<64, 1>(x_32_3)), x_64_1);
  EXPECT_EQ((T<32, 5>(x_32_3)), (T<32, 5>(x_lo96)));
  EXPECT_EQ((T<32, 4>(x_32_3)), (T<32, 4>(x_lo96)));
  EXPECT_EQ((T<32, 3>(x_32_3)), x_32_3);

  // Test the path without optimization.
  bool negative = x < 0;
  EXPECT_EQ((Convert<T, 64, 3>(x_64_2, negative)), x_64_3);
  EXPECT_EQ((Convert<T, 64, 2>(x_64_2, negative)), x_64_2);
  EXPECT_EQ((Convert<T, 64, 1>(x_64_2, negative)), x_64_1);
  EXPECT_EQ((Convert<T, 32, 5>(x_64_2, negative)), x_32_5);
  EXPECT_EQ((Convert<T, 32, 4>(x_64_2, negative)), x_32_4);
  EXPECT_EQ((Convert<T, 32, 3>(x_64_2, negative)), x_32_3);

  EXPECT_EQ((Convert<T, 64, 3>(x_32_4, negative)), x_64_3);
  EXPECT_EQ((Convert<T, 64, 2>(x_32_4, negative)), x_64_2);
  EXPECT_EQ((Convert<T, 64, 1>(x_32_4, negative)), x_64_1);
  EXPECT_EQ((Convert<T, 32, 5>(x_32_4, negative)), x_32_5);
  EXPECT_EQ((Convert<T, 32, 4>(x_32_4, negative)), x_32_4);
  EXPECT_EQ((Convert<T, 32, 3>(x_32_4, negative)), x_32_3);

  negative = x_lo96 < 0;
  EXPECT_EQ((Convert<T, 64, 3>(x_32_3, negative)), (T<64, 3>(x_lo96)));
  EXPECT_EQ((Convert<T, 64, 2>(x_32_3, negative)), (T<64, 2>(x_lo96)));
  EXPECT_EQ((Convert<T, 64, 1>(x_32_3, negative)), x_64_1);
  EXPECT_EQ((Convert<T, 32, 5>(x_32_3, negative)), (T<32, 5>(x_lo96)));
  EXPECT_EQ((Convert<T, 32, 4>(x_32_3, negative)), (T<32, 4>(x_lo96)));
  EXPECT_EQ((Convert<T, 32, 3>(x_32_3, negative)), x_32_3);
}

TEST(FixedInt, FixedUintCastDoubleTest) {
  for (uint64_t val : kFixedIntCastDoubleTestData64) {
    TestFixedUintCastDouble(val);
  }
  for (uint128 val : kFixedIntCastDoubleTestData128) {
    TestFixedUintCastDouble(val);
  }
}

TEST(FixedInt, FixedUintCastDoubleRandomTest) {
  absl::BitGen random;
  for (int i = 0; i < 1000; ++i) {
    uint64_t random_val = absl::Uniform<uint32_t>(random);
    TestFixedUintCastDouble(random_val);
  }
  for (int i = 0; i < 1000; ++i) {
    uint64_t random_val = absl::Uniform<uint64_t>(random);
    TestFixedUintCastDouble(random_val);
  }
  for (int i = 0; i < 1000; ++i) {
    uint64_t random_hi = absl::Uniform<uint64_t>(random);
    uint64_t random_lo = absl::Uniform<uint64_t>(random);
    uint128 random_val =
        static_cast<uint128>(absl::MakeUint128(random_hi, random_lo));
    TestFixedUintCastDouble(random_val);
  }
}

TEST(FixedInt, FixedUintCastDoubleMaxTest) {
  // Given the kNumBitsPerWord can only be 32 or 64, to be casted to double, the
  // actual max bits a FixedUint can have is 32 * 31 = 992
  FixedUint<32, 31> max_fixed_uint;
  max_fixed_uint -= 1;
  double double_max = static_cast<double>(max_fixed_uint);
  // Rounded up to 2^992
  double expect = zetasql_base::MathUtil::IPow(2.0, 992);
  CompareDouble(double_max, expect);
}

TEST(FixedInt, FixedIntCastDoubleTest) {
  for (int64_t val : kFixedIntCastDoubleTestData64) {
    TestFixedIntCastDouble(val);
    TestFixedIntCastDouble(-val);
  }
  for (int128 val : kFixedIntCastDoubleTestData128) {
    TestFixedIntCastDouble(val);
    TestFixedIntCastDouble(-val);
  }
}

TEST(FixedInt, FixedIntCastDoubleRandomTest) {
  absl::BitGen random;
  for (int i = 0; i < 1000; ++i) {
    int64_t random_val = absl::Uniform<int32_t>(absl::IntervalClosed, random,
                                            kint32min, kint32max);
    TestFixedIntCastDouble(random_val);
  }
  for (int i = 0; i < 1000; ++i) {
    int64_t random_val = absl::Uniform<int64_t>(absl::IntervalClosed, random,
                                            kint64min, kint64max);
    TestFixedIntCastDouble(random_val);
  }
  for (int i = 0; i < 1000; ++i) {
    int64_t random_hi = absl::Uniform<int64_t>(absl::IntervalClosed, random,
                                           kint64min, kint64max);
    uint64_t random_lo = absl::Uniform<uint64_t>(random);
    int128 random_val =
        static_cast<int128>(absl::MakeUint128(random_hi, random_lo));
    TestFixedIntCastDouble(random_val);
  }
}

TEST(FixedInt, FixedIntCastDoubleMinMaxTest) {
  // Given the kNumBitsPerWord can only be 32 or 64, to be casted to double, the
  // actual max bits a FixedUint can have is 32 * 31 = 992
  FixedInt<32, 31> max_fixed_int;
  FixedInt<32, 31> min_fixed_int;
  min_fixed_int += 1;
  for (int i = 0; i < 31; ++i) {
    min_fixed_int <<= 31;
  }
  min_fixed_int <<= 30;
  max_fixed_int = -min_fixed_int;
  max_fixed_int -= FixedInt<32, 31>::Word(1);
  double double_max = static_cast<double>(max_fixed_int);
  double double_min = static_cast<double>(min_fixed_int);
  // Rounded up to 2^991
  double expect_max = zetasql_base::MathUtil::IPow(2.0, 991);
  double expect_min = zetasql_base::MathUtil::IPow(-2.0, 991);
  CompareDouble(double_max, expect_max);
  CompareDouble(double_min, expect_min);
}

TEST(FixedInt, OperatorsTest) {
  for (uint64_t x_lo : kSorted64BitValues) {
    for (uint64_t x_hi : kSorted64BitValues) {
      const uint128 x = uint128{x_hi} << 64 | x_lo;
      TestShiftOperators(x);
      TestArithmeticOperatorsWithSelf(x);
      TestExtendAndMultiply(x_lo, x_hi);
      TestConversion<FixedUint, uint128, uint64_t>(x);
      TestConversion<FixedInt, int128, int64_t>(static_cast<int128>(x));

      for (uint64_t y_lo : kSorted64BitValues) {
        TestArithmeticOperatorsWithWord(x, y_lo);

        for (uint64_t y_hi : kSorted64BitValues) {
          const uint128 y = uint128{y_hi} << 64 | y_lo;
          TestArithmeticOperators(x, y);
          TestComparisonOperators(x, y);
        }
      }
    }
  }
}

TEST(FixedInt, OperatorsRandomTest) {
  absl::BitGen random;
  for (int i = 0; i < 10000; ++i) {
    uint64_t x_lo = absl::Uniform<uint64_t>(random);
    uint64_t x_hi = absl::Uniform<uint64_t>(random);
    const uint128 x = uint128{x_hi} << 64 | x_lo;
    uint64_t y_lo = absl::Uniform<uint64_t>(random);
    uint64_t y_hi = absl::Uniform<uint64_t>(random);
    const uint128 y = uint128{y_hi} << 64 | y_lo;

    TestShiftOperators(x);
    TestArithmeticOperatorsWithSelf(x);
    TestArithmeticOperatorsWithWord(x, y_lo);
    TestArithmeticOperators(x, y);
    TestComparisonOperators(x, y);
    TestExtendAndMultiply(x_lo, x_hi);
    TestConversion<FixedUint, uint128, uint64_t>(x);
    TestConversion<FixedInt, int128, int64_t>(static_cast<int128>(x));
  }
}

TEST(FixedInt, FindMSBSetNonZero) {
  FixedUint<32, 2> v1(static_cast<uint64_t>(0));
  FixedUint<32, 2> v2(static_cast<uint64_t>(1));
  FixedUint<32, 2> v3(static_cast<uint64_t>(0x1ffff));
  FixedUint<32, 4> v4(static_cast<uint64_t>(0x1fffffffffffff));
  FixedUint<64, 4> v5(static_cast<uint64_t>(0x1fffffffffffff));
  EXPECT_EQ(0, v1.FindMSBSetNonZero());
  EXPECT_EQ(0, v2.FindMSBSetNonZero());
  EXPECT_EQ(16, v3.FindMSBSetNonZero());
  EXPECT_EQ(52, v4.FindMSBSetNonZero());
  EXPECT_EQ(52, v5.FindMSBSetNonZero());
}

TEST(FixedInt, MultiplyAndCheckOverflow_Overflow) {
  FixedUint<32, 2> v1(static_cast<uint64_t>(0x8000000000000000));
  FixedUint<32, 2> v2(static_cast<uint64_t>(2));
  FixedUint<32, 2> v3(static_cast<uint64_t>(184467459183));
  FixedUint<32, 2> v4(static_cast<uint64_t>(9999999999));
  EXPECT_FALSE(v1.MultiplyAndCheckOverflow(v2));
  EXPECT_FALSE(v3.MultiplyAndCheckOverflow(v4));
}

TEST(FixedInt, Serialization) {
  for (auto pair : kSortedSerializedSigned64BitValues) {
    FixedInt<32, 2> ref1(pair.first);
    FixedInt<64, 1> ref2(pair.first);
    EXPECT_EQ(ref1.SerializeToBytes(), pair.second);
    EXPECT_EQ(ref2.SerializeToBytes(), pair.second);
  }
}

TEST(FixedInt, Deserialization) {
  for (auto pair : kSortedSerializedSigned64BitValues) {
    FixedInt<32, 2> ref1(pair.first);
    FixedInt<64, 1> ref2(pair.first);
    FixedInt<32, 2> test1;
    FixedInt<64, 1> test2;
    EXPECT_TRUE(test1.DeserializeFromBytes(pair.second));
    EXPECT_EQ(test1, ref1);
    EXPECT_TRUE(test2.DeserializeFromBytes(pair.second));
    EXPECT_EQ(test2, ref2);
  }
}

TEST(FixedInt, RandomRoundtrip) {
  absl::BitGen random;
  for (int i = 0; i < 10000; ++i) {
    uint64_t bits = absl::Uniform<uint64_t>(random);
    uint8_t shift = absl::Uniform<uint8_t>(random, 0, 64);
    const int128 x = uint128{bits} << shift;
    const int128 x_neg = -x;
    FixedInt<32, 4> ref(x);
    FixedInt<32, 4> ref_neg(x_neg);
    FixedInt<32, 4> res;
    EXPECT_TRUE(res.DeserializeFromBytes(ref.SerializeToBytes()));
    FixedInt<32, 4> res_neg;
    EXPECT_TRUE(res_neg.DeserializeFromBytes(ref_neg.SerializeToBytes()));
    EXPECT_EQ(res, ref);
    EXPECT_EQ(res_neg, ref_neg);
  }
}

TEST(FixedUint, Serialization) {
  for (auto pair : kSortedSerializedUnsigned64BitValues) {
    FixedUint<32, 2> ref(pair.first);
    EXPECT_EQ(ref.SerializeToBytes(), pair.second);
  }
}

TEST(FixedUint, Deserialization) {
  for (auto pair : kSortedSerializedUnsigned64BitValues) {
    FixedUint<32, 2> ref(pair.first);
    FixedUint<32, 2> test;
    EXPECT_TRUE(test.DeserializeFromBytes(pair.second));
    EXPECT_EQ(test, ref);
  }
}

TEST(FixedUint, RandomRoundtrip) {
  absl::BitGen random;
  for (int i = 0; i < 10000; ++i) {
    uint64_t bits = absl::Uniform<uint64_t>(random);
    uint8_t shift = absl::Uniform<uint8_t>(random, 0, 64);
    const uint128 x = uint128{bits} << shift;
    FixedUint<32, 4> ref(x);
    FixedUint<32, 4> res;
    EXPECT_TRUE(res.DeserializeFromBytes(ref.SerializeToBytes()));
    EXPECT_EQ(res, ref);
  }
}
}  // namespace
}  // namespace zetasql
