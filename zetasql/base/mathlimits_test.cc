//
// Copyright 2018 ZetaSQL Authors
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

  LOG(INFO) << "Size = " << sizeof(Type);
  LOG(INFO) << "kMin = " << kMin;
  LOG(INFO) << "kMax = " << kMax;
  LOG(INFO) << "kPosMin = " << kPosMin;
  LOG(INFO) << "kMax10Exp = " << kMax10Exp;
  LOG(INFO) << "kMin10Exp = " << kMin10Exp;
  LOG(INFO) << "kEpsilon = " << kEpsilon;
  LOG(INFO) << "kStdError = " << MathLimits<Type>::kStdError;

  // Some basic checks:

  CHECK_EQ(sizeof(typename MathLimits<Type>::UnsignedType), sizeof(Type));

  CHECK_EQ(kMax, MathLimits<Type>::kPosMax);

  CHECK_GT(Type(1 + kEpsilon), Type(1));
  if (MathLimits<Type>::kIsSigned) {
    CHECK_LT(kMin, 0);
    CHECK_LE(-(kMin + kMax), kEpsilon);
  } else {
    CHECK_EQ(kMin, 0);
  }

  // Check kMax10Exp's claim:

  Type pow = kOne;
  for (int i = 0; i < kMax10Exp; ++i) {
    pow *= 10;
  }
  LOG(INFO) << "1 * 10^kMaxExp = " << pow;
  LOG(INFO) << "kMax - 1 * 10^kMaxExp = " << kMax - pow;
  CHECK_GT(pow, kOne);
  CHECK_LE(pow, kMax);
  CHECK_GE(pow, kPosMin);
  for (int i = 0; i < kMax10Exp; ++i) {
    pow /= 10;
  }
  LOG(INFO) << "1 * 10^kMaxExp / 10^kMaxExp - 1 = " << pow - kOne;
  CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));

  // Check kMin10Exp's claim:

  pow = kOne;
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow /= 10;
  }
  LOG(INFO) << "1 * 10^kMinExp = " << pow;
  LOG(INFO) << "1 * 10^kMaxExp - kPosMin = " << pow - kPosMin;
  if (MathLimits<Type>::kIsInteger) {
    CHECK_EQ(pow, kOne);
  } else {
    CHECK_LT(pow, kOne);
  }
  CHECK_LE(pow, kMax);
  CHECK_GE(pow, kPosMin);
  for (int i = 0; i < -kMin10Exp; ++i) {
    pow *= 10;
  }
  LOG(INFO) << "1 * 10^kMinExp / 10^kMinExp - 1 = " << pow - kOne;
  CHECK(MathUtil::WithinMargin(pow, kOne, MathLimits<Type>::kStdError));
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
  CHECK_LE(MathLimits<uint8_t>::kMax, MathLimits<uint16_t>::kMax);
  CHECK_LE(MathLimits<uint16_t>::kMax, MathLimits<uint32_t>::kMax);
  CHECK_LE(MathLimits<uint32_t>::kMax, MathLimits<uint64_t>::kMax);

  CHECK_LE(MathLimits<int8_t>::kMax, MathLimits<int16_t>::kMax);
  CHECK_LE(MathLimits<int16_t>::kMax, MathLimits<int32_t>::kMax);
  CHECK_LE(MathLimits<int32_t>::kMax, MathLimits<int64_t>::kMax);
}

template<typename Type, typename TypeTwo, typename TypeThree>
static void TestFPMathLimits() {
  // The standard part:
  TestMathLimits<Type>();

  const Type kNaN = MathLimits<Type>::kNaN;
  const Type kPosInf = MathLimits<Type>::kPosInf;
  const Type kNegInf = MathLimits<Type>::kNegInf;

  LOG(INFO) << "Size = " << sizeof(Type);
  LOG(INFO) << "kNaN = " << kNaN;
  LOG(INFO) << "kPosInf = " << kPosInf;
  LOG(INFO) << "kNegInf = " << kNegInf;

  // Special value compatibility:

  CHECK(MathLimits<TypeTwo>::IsNaN(kNaN));
  CHECK(MathLimits<TypeTwo>::IsPosInf(kPosInf));
  CHECK(MathLimits<TypeTwo>::IsNegInf(kNegInf));
  CHECK(MathLimits<TypeThree>::IsNaN(kNaN));
  CHECK(MathLimits<TypeThree>::IsPosInf(kPosInf));
  CHECK(MathLimits<TypeThree>::IsNegInf(kNegInf));

  // Special values and operations over them:

  CHECK(MathLimits<Type>::IsFinite(0));
  CHECK(MathLimits<Type>::IsFinite(1.1));
  CHECK(MathLimits<Type>::IsFinite(-1.1));
  CHECK(!MathLimits<Type>::IsFinite(kNaN));
  CHECK(!MathLimits<Type>::IsFinite(kPosInf));
  CHECK(!MathLimits<Type>::IsFinite(kNegInf));

  CHECK(!MathLimits<Type>::IsNaN(0));
  CHECK(!MathLimits<Type>::IsNaN(1.1));
  CHECK(!MathLimits<Type>::IsNaN(-1.1));
  CHECK(MathLimits<Type>::IsNaN(kNaN));
  CHECK(!MathLimits<Type>::IsNaN(kPosInf));
  CHECK(!MathLimits<Type>::IsNaN(kNegInf));

  CHECK(!MathLimits<Type>::IsInf(0));
  CHECK(!MathLimits<Type>::IsInf(1.1));
  CHECK(!MathLimits<Type>::IsInf(-1.1));
  CHECK(!MathLimits<Type>::IsInf(kNaN));
  CHECK(MathLimits<Type>::IsInf(kPosInf));
  CHECK(MathLimits<Type>::IsInf(kNegInf));

  CHECK(!MathLimits<Type>::IsPosInf(0));
  CHECK(!MathLimits<Type>::IsPosInf(1.1));
  CHECK(!MathLimits<Type>::IsPosInf(-1.1));
  CHECK(!MathLimits<Type>::IsPosInf(kNaN));
  CHECK(MathLimits<Type>::IsPosInf(kPosInf));
  CHECK(!MathLimits<Type>::IsPosInf(kNegInf));

  CHECK(!MathLimits<Type>::IsNegInf(0));
  CHECK(!MathLimits<Type>::IsNegInf(1.1));
  CHECK(!MathLimits<Type>::IsNegInf(-1.1));
  CHECK(!MathLimits<Type>::IsNegInf(kNaN));
  CHECK(!MathLimits<Type>::IsNegInf(kPosInf));
  CHECK(MathLimits<Type>::IsNegInf(kNegInf));

  CHECK(MathLimits<Type>::IsNaN(kNaN + 1));
  CHECK(MathLimits<Type>::IsNaN(kNaN + 1e30));
  CHECK(MathLimits<Type>::IsNaN(kNaN + kPosInf));
  CHECK(MathLimits<Type>::IsNaN(kNaN + kNegInf));
  CHECK(MathLimits<Type>::IsNaN(kNaN * 1));
  CHECK(MathLimits<Type>::IsNaN(kNaN * 1e30));
  CHECK(MathLimits<Type>::IsNaN(kNaN * kPosInf));
  CHECK(MathLimits<Type>::IsNaN(kNaN * kNegInf));
  CHECK(MathLimits<Type>::IsNaN(kNaN / 1));
  CHECK(MathLimits<Type>::IsNaN(kNaN / 1e30));
  CHECK(MathLimits<Type>::IsNaN(kNaN / kPosInf));
  CHECK(MathLimits<Type>::IsNaN(kNaN / kNegInf));

  CHECK(MathLimits<Type>::IsNaN(kPosInf + kNegInf));
  CHECK(MathLimits<Type>::IsNaN(kPosInf - kPosInf));
  CHECK(MathLimits<Type>::IsNaN(kNegInf - kNegInf));
  CHECK(MathLimits<Type>::IsNaN(kPosInf / kPosInf));
  CHECK(MathLimits<Type>::IsNaN(kPosInf / kNegInf));
  CHECK(MathLimits<Type>::IsNaN(kNegInf / kPosInf));

  CHECK(MathLimits<Type>::IsPosInf(kPosInf + 1));
  CHECK(MathLimits<Type>::IsPosInf(kPosInf - 1e30));
  CHECK(MathLimits<Type>::IsPosInf(kPosInf + kPosInf));
  CHECK(MathLimits<Type>::IsPosInf(kPosInf * kPosInf));
  CHECK(MathLimits<Type>::IsPosInf(kPosInf - kNegInf));
  CHECK(MathLimits<Type>::IsPosInf(kNegInf * kNegInf));

  CHECK(MathLimits<Type>::IsNegInf(kNegInf - 1));
  CHECK(MathLimits<Type>::IsNegInf(kNegInf + 1e30));
  CHECK(MathLimits<Type>::IsNegInf(kNegInf + kNegInf));
  CHECK(MathLimits<Type>::IsNegInf(kNegInf - kPosInf));
  CHECK(MathLimits<Type>::IsNegInf(kPosInf * kNegInf));
  CHECK(MathLimits<Type>::IsNegInf(kNegInf * kPosInf));

  CHECK_NE(kNaN, 0);
  CHECK_NE(kNaN, 1);
  CHECK_NE(kNaN, kNegInf);
  CHECK_NE(kNaN, kPosInf);
  CHECK_NE(kNaN, kNaN);
  CHECK(!(kNaN < 0));
  CHECK(!(kNaN > 0));

  CHECK_NE(kPosInf, 0);
  CHECK_NE(kPosInf, 1);
  CHECK_NE(kPosInf, kNegInf);
  CHECK(!(kPosInf < 0));
  CHECK_GT(kPosInf, 0);

  CHECK_NE(kNegInf, 0);
  CHECK_NE(kNegInf, 1);
  CHECK_LT(kNegInf, 0);
  CHECK(!(kNegInf > 0));
}

TEST(MathLimits, FPMathLimits) {
  TestFPMathLimits<float, double, long double>();
  TestFPMathLimits<double, float, long double>();
  TestFPMathLimits<long double, float, double>();
}

}  // namespace zetasql_base
