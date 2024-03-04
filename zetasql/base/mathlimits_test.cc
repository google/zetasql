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

  ABSL_LOG(INFO) << "Size = " << sizeof(Type);
  ABSL_LOG(INFO) << "kMin = " << kMin;
  ABSL_LOG(INFO) << "kMax = " << kMax;
  ABSL_LOG(INFO) << "kPosMin = " << kPosMin;
  ABSL_LOG(INFO) << "kMax10Exp = " << kMax10Exp;
  ABSL_LOG(INFO) << "kMin10Exp = " << kMin10Exp;
  ABSL_LOG(INFO) << "kEpsilon = " << kEpsilon;
  ABSL_LOG(INFO) << "kStdError = " << MathLimits<Type>::kStdError;

  // Some basic checks:

  ABSL_CHECK_EQ(sizeof(typename MathLimits<Type>::UnsignedType), sizeof(Type));

  ABSL_CHECK_EQ(kMax, MathLimits<Type>::kPosMax);

  ABSL_CHECK_GT(Type(1 + kEpsilon), Type(1));
  if (MathLimits<Type>::kIsSigned) {
    ABSL_CHECK_LT(kMin, 0);
    ABSL_CHECK_LE(-(kMin + kMax), kEpsilon);
  } else {
    ABSL_CHECK_EQ(kMin, 0);
  }

  // Check kMax10Exp's claim:

  Type pow = kOne;
  for (int i = 0; i < kMax10Exp; ++i) {
    pow *= 10;
  }
  ABSL_LOG(INFO) << "1 * 10^kMaxExp = " << pow;
  ABSL_LOG(INFO) << "kMax - 1 * 10^kMaxExp = " << kMax - pow;
  ABSL_CHECK_GT(pow, kOne);
  ABSL_CHECK_LE(pow, kMax);
  ABSL_CHECK_GE(pow, kPosMin);
  for (int i = 0; i < kMax10Exp; ++i) {
    pow /= 10;
  }
  ABSL_LOG(INFO) << "1 * 10^kMaxExp / 10^kMaxExp - 1 = " << pow - kOne;
  ABSL_CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));

  // Check kMin10Exp's claim:

  pow = kOne;
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow /= 10;
  }
  ABSL_LOG(INFO) << "1 * 10^kMinExp = " << pow;
  ABSL_LOG(INFO) << "1 * 10^kMaxExp - kPosMin = " << pow - kPosMin;
  if (MathLimits<Type>::kIsInteger) {
    ABSL_CHECK_EQ(pow, kOne);
  } else {
    ABSL_CHECK_LT(pow, kOne);
  }
  ABSL_CHECK_LE(pow, kMax);
  ABSL_CHECK_GE(pow, kPosMin);
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow *= 10;
  }
  ABSL_LOG(INFO) << "1 * 10^kMinExp / 10^kMinExp - 1 = " << pow - kOne;
  ABSL_CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));
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
  ABSL_CHECK_LE(MathLimits<uint8_t>::kMax, MathLimits<uint16_t>::kMax);
  ABSL_CHECK_LE(MathLimits<uint16_t>::kMax, MathLimits<uint32_t>::kMax);
  ABSL_CHECK_LE(MathLimits<uint32_t>::kMax, MathLimits<uint64_t>::kMax);

  ABSL_CHECK_LE(MathLimits<int8_t>::kMax, MathLimits<int16_t>::kMax);
  ABSL_CHECK_LE(MathLimits<int16_t>::kMax, MathLimits<int32_t>::kMax);
  ABSL_CHECK_LE(MathLimits<int32_t>::kMax, MathLimits<int64_t>::kMax);
}

template<typename Type, typename TypeTwo, typename TypeThree>
static void TestFPMathLimits() {
  // The standard part:
  TestMathLimits<Type>();

  const Type kNaN = MathLimits<Type>::kNaN;
  const Type kPosInf = MathLimits<Type>::kPosInf;
  const Type kNegInf = MathLimits<Type>::kNegInf;

  ABSL_LOG(INFO) << "Size = " << sizeof(Type);
  ABSL_LOG(INFO) << "kNaN = " << kNaN;
  ABSL_LOG(INFO) << "kPosInf = " << kPosInf;
  ABSL_LOG(INFO) << "kNegInf = " << kNegInf;

  // Special value compatibility:

  ABSL_CHECK(MathLimits<TypeTwo>::IsNaN(kNaN));
  ABSL_CHECK(MathLimits<TypeTwo>::IsPosInf(kPosInf));
  ABSL_CHECK(MathLimits<TypeTwo>::IsNegInf(kNegInf));
  ABSL_CHECK(MathLimits<TypeThree>::IsNaN(kNaN));
  ABSL_CHECK(MathLimits<TypeThree>::IsPosInf(kPosInf));
  ABSL_CHECK(MathLimits<TypeThree>::IsNegInf(kNegInf));

  // Special values and operations over them:

  ABSL_CHECK(MathLimits<Type>::IsFinite(0));
  ABSL_CHECK(MathLimits<Type>::IsFinite(1.1));
  ABSL_CHECK(MathLimits<Type>::IsFinite(-1.1));
  ABSL_CHECK(!MathLimits<Type>::IsFinite(kNaN));
  ABSL_CHECK(!MathLimits<Type>::IsFinite(kPosInf));
  ABSL_CHECK(!MathLimits<Type>::IsFinite(kNegInf));

  ABSL_CHECK(!MathLimits<Type>::IsNaN(0));
  ABSL_CHECK(!MathLimits<Type>::IsNaN(1.1));
  ABSL_CHECK(!MathLimits<Type>::IsNaN(-1.1));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN));
  ABSL_CHECK(!MathLimits<Type>::IsNaN(kPosInf));
  ABSL_CHECK(!MathLimits<Type>::IsNaN(kNegInf));

  ABSL_CHECK(!MathLimits<Type>::IsInf(0));
  ABSL_CHECK(!MathLimits<Type>::IsInf(1.1));
  ABSL_CHECK(!MathLimits<Type>::IsInf(-1.1));
  ABSL_CHECK(!MathLimits<Type>::IsInf(kNaN));
  ABSL_CHECK(MathLimits<Type>::IsInf(kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsInf(kNegInf));

  ABSL_CHECK(!MathLimits<Type>::IsPosInf(0));
  ABSL_CHECK(!MathLimits<Type>::IsPosInf(1.1));
  ABSL_CHECK(!MathLimits<Type>::IsPosInf(-1.1));
  ABSL_CHECK(!MathLimits<Type>::IsPosInf(kNaN));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf));
  ABSL_CHECK(!MathLimits<Type>::IsPosInf(kNegInf));

  ABSL_CHECK(!MathLimits<Type>::IsNegInf(0));
  ABSL_CHECK(!MathLimits<Type>::IsNegInf(1.1));
  ABSL_CHECK(!MathLimits<Type>::IsNegInf(-1.1));
  ABSL_CHECK(!MathLimits<Type>::IsNegInf(kNaN));
  ABSL_CHECK(!MathLimits<Type>::IsNegInf(kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf));

  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN + 1));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN + 1e30));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN + kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN + kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN * 1));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN * 1e30));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN * kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN * kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN / 1));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN / 1e30));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN / kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNaN / kNegInf));

  ABSL_CHECK(MathLimits<Type>::IsNaN(kPosInf + kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kPosInf - kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNegInf - kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kPosInf / kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kPosInf / kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNaN(kNegInf / kPosInf));

  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf + 1));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf - 1e30));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf + kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf * kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kPosInf - kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsPosInf(kNegInf * kNegInf));

  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf - 1));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf + 1e30));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf + kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf - kPosInf));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kPosInf * kNegInf));
  ABSL_CHECK(MathLimits<Type>::IsNegInf(kNegInf * kPosInf));

  ABSL_CHECK_NE(kNaN, 0);
  ABSL_CHECK_NE(kNaN, 1);
  ABSL_CHECK_NE(kNaN, kNegInf);
  ABSL_CHECK_NE(kNaN, kPosInf);
  ABSL_CHECK_NE(kNaN, kNaN);
  ABSL_CHECK(!(kNaN < 0));
  ABSL_CHECK(!(kNaN > 0));

  ABSL_CHECK_NE(kPosInf, 0);
  ABSL_CHECK_NE(kPosInf, 1);
  ABSL_CHECK_NE(kPosInf, kNegInf);
  ABSL_CHECK(!(kPosInf < 0));
  ABSL_CHECK_GT(kPosInf, 0);

  ABSL_CHECK_NE(kNegInf, 0);
  ABSL_CHECK_NE(kNegInf, 1);
  ABSL_CHECK_LT(kNegInf, 0);
  ABSL_CHECK(!(kNegInf > 0));
}

TEST(MathLimits, FPMathLimits) {
  TestFPMathLimits<float, double, long double>();
  TestFPMathLimits<double, float, long double>();
  TestFPMathLimits<long double, float, double>();
}

}  // namespace zetasql_base
