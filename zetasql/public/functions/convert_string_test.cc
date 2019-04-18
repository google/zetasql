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

#include "zetasql/public/functions/convert_string.h"

#include <math.h>
#include <limits>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

template <typename T>
void TestRoundtripValueHex(T value) {}

template <typename UIntT>
void TestRoundtripValueHex(
    typename std::enable_if<std::is_integral<UIntT>::value &&
                                !std::is_signed<UIntT>::value &&
                                !std::is_same<UIntT, bool>::value,
                            UIntT>::type value) {
  std::string hexstr = absl::StrCat("0x", absl::Hex(value));
  UIntT hexout;
  zetasql_base::Status error;
  EXPECT_TRUE(StringToNumeric(hexstr, &hexout, &error))
      << " hexstr: " << hexstr;
  ZETASQL_EXPECT_OK(error) << "hexstr: " << hexstr << " hexout:" << hexout;
  EXPECT_EQ(value, hexout) << "hexstr: " << hexstr << " hexout:" << hexout;
}

template <typename IntT>
void TestRoundtripValueHex(
    typename std::enable_if<std::is_integral<IntT>::value &&
                                std::is_signed<IntT>::value,
                            IntT>::type value) {
  std::string sign = value < 0 ? "-" : "";
  std::string hexstr = absl::StrCat(sign, "0x", absl::Hex(std::abs(value)));
  IntT hexout;
  zetasql_base::Status error;
  EXPECT_TRUE(StringToNumeric(hexstr, &hexout, &error))
      << " hexstr: " << hexstr;
  ZETASQL_EXPECT_OK(error) << "hexstr: " << hexstr << " hexout:" << hexout;
  EXPECT_EQ(value, hexout) << "hexstr: " << hexstr << " hexout:" << hexout;
}

// Converting any numeric value to a std::string and then back must always return
// the same value. This is guaranteed by NumericToString() implementation.
template <typename T>
void TestRoundtripValue(T value) {
  std::string str;
  zetasql_base::Status error;
  EXPECT_TRUE(NumericToString<T>(value, &str, &error));
  ZETASQL_EXPECT_OK(error);
  EXPECT_GT(str.size(), 0);

  T out;
  EXPECT_TRUE(StringToNumeric<T>(str, &out, &error));
  EXPECT_TRUE(error.ok());

  TestRoundtripValueHex<T>(value);

  if (!std::numeric_limits<T>::is_integer && std::isnan(value)) {
    EXPECT_TRUE(std::isnan(out));
  } else {
    EXPECT_EQ(value, out);
  }
}

template <typename T>
void TestRoundtrip() {
  TestRoundtripValue<T>(0);
  TestRoundtripValue<T>(1);
  if (std::numeric_limits<T>::is_signed) {
    TestRoundtripValue<T>(-1);
  }
  TestRoundtripValue(std::numeric_limits<T>::min());
  TestRoundtripValue(std::numeric_limits<T>::max());
  if (std::numeric_limits<T>::is_integer || !std::numeric_limits<T>::is_exact) {
    TestRoundtripValue(std::numeric_limits<T>::max() - 1);
    TestRoundtripValue(std::numeric_limits<T>::lowest() + 1);
  }
  TestRoundtripValue(std::numeric_limits<T>::lowest());
  if (std::numeric_limits<T>::has_denorm) {
    TestRoundtripValue(std::numeric_limits<T>::denorm_min());
  }
  if (std::numeric_limits<T>::has_infinity) {
    TestRoundtripValue(std::numeric_limits<T>::infinity());
    TestRoundtripValue(-std::numeric_limits<T>::infinity());
  }
  if (std::numeric_limits<T>::has_quiet_NaN) {
    TestRoundtripValue(std::numeric_limits<T>::quiet_NaN());
  }
}

template <typename T>
void TestSingleChar() {
  for (int i = 0; i < 256; ++i) {
    char buf = i;
    T out;
    zetasql_base::Status error;
    if (absl::ascii_isdigit(i) && !std::is_same<bool, T>::value) {
      EXPECT_TRUE(StringToNumeric<T>(absl::string_view(&buf, 1), &out, &error));
      EXPECT_TRUE(error.ok());
    } else {
      EXPECT_FALSE(
          StringToNumeric<T>(absl::string_view(&buf, 1), &out, &error));
      EXPECT_FALSE(error.ok());
    }
  }
}

template <typename T>
void TestLongString() {
  char buf[256] = {0, };
  T out;
  zetasql_base::Status error;
  EXPECT_FALSE(
      StringToNumeric<T>(absl::string_view(buf, sizeof(buf)), &out, &error));
  EXPECT_FALSE(error.ok());
  EXPECT_TRUE(absl::EndsWith(error.message(), "\0\0\0..."));
}

template <typename T>
void TestAll() {
  TestRoundtrip<T>();
  TestSingleChar<T>();
  TestLongString<T>();
}

TEST(Convert, TestBool) {
  TestAll<bool>();
}

TEST(Convert, TestInt32) {
  TestAll<int32_t>();
}

TEST(Convert, TestInt64) {
  TestAll<int64_t>();
}

TEST(Convert, TestUint32) {
  TestAll<uint32_t>();
}

TEST(Convert, TestUint64) {
  TestAll<uint64_t>();
}

TEST(Convert, TestFloat) {
  TestAll<float>();
}

TEST(Convert, TestDouble) {
  TestAll<double>();
}

}  // namespace functions
}  // namespace zetasql
