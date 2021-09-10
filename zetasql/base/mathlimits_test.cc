//
// Copyright 2018 Google LLC
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

#include "zetasql/base/mathlimits.h"

#include <stdio.h>

#include <cstdint>
#include <limits>

#include "gtest/gtest.h"
#include <cstdint>
#include "zetasql/base/logging.h"
#include "zetasql/base/mathutil.h"

namespace zetasql_base {

template<typename Type>
static void TestMathLimits() {
  const Type kOne = 1;
  const Type kMin = MathLimits<Type>::kMin;
  const Type kMax = MathLimits<Type>::kMax;
  const Type kPosMin = MathLimits<Type>::kPosMin;
  const Type kMax10Exp = MathLimits<Type>::kMax10Exp;
  const Type kMin10Exp = MathLimits<Type>::kMin10Exp;
  const Type kEpsilon = MathLimits<Type>::kEpsilon;

  ZETASQL_LOG(INFO) << "Size = " << sizeof(Type);
  ZETASQL_LOG(INFO) << "kMin = " << kMin;
  ZETASQL_LOG(INFO) << "kMax = " << kMax;
  ZETASQL_LOG(INFO) << "kPosMin = " << kPosMin;
  ZETASQL_LOG(INFO) << "kMax10Exp = " << kMax10Exp;
  ZETASQL_LOG(INFO) << "kMin10Exp = " << kMin10Exp;
  ZETASQL_LOG(INFO) << "kEpsilon = " << kEpsilon;
  ZETASQL_LOG(INFO) << "kStdError = " << MathLimits<Type>::kStdError;

  // Some basic checks:

  ZETASQL_CHECK_EQ(sizeof(typename MathLimits<Type>::UnsignedType), sizeof(Type));

  ZETASQL_CHECK_EQ(kMax, MathLimits<Type>::kPosMax);

  ZETASQL_CHECK_GT(Type(1 + kEpsilon), Type(1));
  if (MathLimits<Type>::kIsSigned) {
    ZETASQL_CHECK_LT(kMin, 0);
    ZETASQL_CHECK_LE(-(kMin + kMax), kEpsilon);
  } else {
    ZETASQL_CHECK_EQ(kMin, 0);
  }

  // Check kMax10Exp's claim:

  Type pow = kOne;
  for (int i = 0; i < kMax10Exp; ++i) {
    pow *= 10;
  }
  ZETASQL_LOG(INFO) << "1 * 10^kMaxExp = " << pow;
  ZETASQL_LOG(INFO) << "kMax - 1 * 10^kMaxExp = " << kMax - pow;
  ZETASQL_CHECK_GT(pow, kOne);
  ZETASQL_CHECK_LE(pow, kMax);
  ZETASQL_CHECK_GE(pow, kPosMin);
  for (int i = 0; i < kMax10Exp; ++i) {
    pow /= 10;
  }
  ZETASQL_LOG(INFO) << "1 * 10^kMaxExp / 10^kMaxExp - 1 = " << pow - kOne;
  ZETASQL_CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));

  // Check kMin10Exp's claim:

  pow = kOne;
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow /= 10;
  }
  ZETASQL_LOG(INFO) << "1 * 10^kMinExp = " << pow;
  ZETASQL_LOG(INFO) << "1 * 10^kMaxExp - kPosMin = " << pow - kPosMin;
  if (MathLimits<Type>::kIsInteger) {
    ZETASQL_CHECK_EQ(pow, kOne);
  } else {
    ZETASQL_CHECK_LT(pow, kOne);
  }
  ZETASQL_CHECK_LE(pow, kMax);
  ZETASQL_CHECK_GE(pow, kPosMin);
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow *= 10;
  }
  ZETASQL_LOG(INFO) << "1 * 10^kMinExp / 10^kMinExp - 1 = " << pow - kOne;
  ZETASQL_CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));
}

TEST(MathLimits, IntMathLimits) {
  // The standard part:
  TestMathLimits<uint8_t>();
  TestMathLimits<uint16_t>();
  TestMathLimits<uint32_t>();
  TestMathLimits<uint64_t>();
  TestMathLimits<int8_t>();
  TestMathLimits<int16_t>();
  TestMathLimits<int32_t>();
  TestMathLimits<int64_t>();

  // Guaranteed size relations:
  ZETASQL_CHECK_LE(MathLimits<uint8_t>::kMax, MathLimits<uint16_t>::kMax);
  ZETASQL_CHECK_LE(MathLimits<uint16_t>::kMax, MathLimits<uint32_t>::kMax);
  ZETASQL_CHECK_LE(MathLimits<uint32_t>::kMax, MathLimits<uint64_t>::kMax);

  ZETASQL_CHECK_LE(MathLimits<int8_t>::kMax, MathLimits<int16_t>::kMax);
  ZETASQL_CHECK_LE(MathLimits<int16_t>::kMax, MathLimits<int32_t>::kMax);
  ZETASQL_CHECK_LE(MathLimits<int32_t>::kMax, MathLimits<int64_t>::kMax);
}

template<typename Type, typename TypeTwo, typename TypeThree>
static void TestFPMathLimits() {
  // The standard part:
  TestMathLimits<Type>();

  const Type kNaN = MathLimits<Type>::kNaN;
  const Type kPosInf = MathLimits<Type>::kPosInf;
  const Type kNegInf = MathLimits<Type>::kNegInf;

  ZETASQL_LOG(INFO) << "Size = " << sizeof(Type);
  ZETASQL_LOG(INFO) << "kNaN = " << kNaN;
  ZETASQL_LOG(INFO) << "kPosInf = " << kPosInf;
  ZETASQL_LOG(INFO) << "kNegInf = " << kNegInf;

  // Special value compatibility:

  ZETASQL_CHECK(MathLimits<TypeTwo>::IsNaN(kNaN));
  ZETASQL_CHECK(MathLimits<TypeTwo>::IsPosInf(kPosInf));
  ZETASQL_CHECK(MathLimits<TypeTwo>::IsNegInf(kNegInf));
  ZETASQL_CHECK(MathLimits<TypeThree>::IsNaN(kNaN));
  ZETASQL_CHECK(MathLimits<TypeThree>::IsPosInf(kPosInf));
  ZETASQL_CHECK(MathLimits<TypeThree>::IsNegInf(kNegInf));

  // Special values and operations over them:

  ZETASQL_CHECK(MathLimits<Type>::IsFinite(0));
  ZETASQL_CHECK(MathLimits<Type>::IsFinite(1.1));
  ZETASQL_CHECK(MathLimits<Type>::IsFinite(-1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsFinite(kNaN));
  ZETASQL_CHECK(!MathLimits<Type>::IsFinite(kPosInf));
  ZETASQL_CHECK(!MathLimits<Type>::IsFinite(kNegInf));

  ZETASQL_CHECK(!MathLimits<Type>::IsNaN(0));
  ZETASQL_CHECK(!MathLimits<Type>::IsNaN(1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsNaN(-1.1));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN));
  ZETASQL_CHECK(!MathLimits<Type>::IsNaN(kPosInf));
  ZETASQL_CHECK(!MathLimits<Type>::IsNaN(kNegInf));

  ZETASQL_CHECK(!MathLimits<Type>::IsInf(0));
  ZETASQL_CHECK(!MathLimits<Type>::IsInf(1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsInf(-1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsInf(kNaN));
  ZETASQL_CHECK(MathLimits<Type>::IsInf(kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsInf(kNegInf));

  ZETASQL_CHECK(!MathLimits<Type>::IsPosInf(0));
  ZETASQL_CHECK(!MathLimits<Type>::IsPosInf(1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsPosInf(-1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsPosInf(kNaN));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf));
  ZETASQL_CHECK(!MathLimits<Type>::IsPosInf(kNegInf));

  ZETASQL_CHECK(!MathLimits<Type>::IsNegInf(0));
  ZETASQL_CHECK(!MathLimits<Type>::IsNegInf(1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsNegInf(-1.1));
  ZETASQL_CHECK(!MathLimits<Type>::IsNegInf(kNaN));
  ZETASQL_CHECK(!MathLimits<Type>::IsNegInf(kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf));

  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN + 1));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN + 1e30));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN + kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN + kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN * 1));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN * 1e30));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN * kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN * kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN / 1));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN / 1e30));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN / kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNaN / kNegInf));

  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kPosInf + kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kPosInf - kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNegInf - kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kPosInf / kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kPosInf / kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNaN(kNegInf / kPosInf));

  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf + 1));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf - 1e30));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf + kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf * kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kPosInf - kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsPosInf(kNegInf * kNegInf));

  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf - 1));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf + 1e30));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf + kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf - kPosInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kPosInf * kNegInf));
  ZETASQL_CHECK(MathLimits<Type>::IsNegInf(kNegInf * kPosInf));

  ZETASQL_CHECK_NE(kNaN, 0);
  ZETASQL_CHECK_NE(kNaN, 1);
  ZETASQL_CHECK_NE(kNaN, kNegInf);
  ZETASQL_CHECK_NE(kNaN, kPosInf);
  ZETASQL_CHECK_NE(kNaN, kNaN);
  ZETASQL_CHECK(!(kNaN < 0));
  ZETASQL_CHECK(!(kNaN > 0));

  ZETASQL_CHECK_NE(kPosInf, 0);
  ZETASQL_CHECK_NE(kPosInf, 1);
  ZETASQL_CHECK_NE(kPosInf, kNegInf);
  ZETASQL_CHECK(!(kPosInf < 0));
  ZETASQL_CHECK_GT(kPosInf, 0);

  ZETASQL_CHECK_NE(kNegInf, 0);
  ZETASQL_CHECK_NE(kNegInf, 1);
  ZETASQL_CHECK_LT(kNegInf, 0);
  ZETASQL_CHECK(!(kNegInf > 0));
}

TEST(MathLimits, FPMathLimits) {
  TestFPMathLimits<float, double, long double>();
  TestFPMathLimits<double, float, long double>();
  TestFPMathLimits<long double, float, double>();
}

}  // namespace zetasql_base
