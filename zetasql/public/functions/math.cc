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

#include "zetasql/public/functions/math.h"

#include <cmath>
#include <cstdint>
#include <type_traits>

#include "absl/base/macros.h"
#include "absl/status/statusor.h"

namespace zetasql {
namespace functions {
namespace {

// Minimum and maximum decimal exponent of numbers representable as double.
// "numeric_limits<double>::digits10 - 1"  is to take into account subnormal
// numbers.
const int kDoubleMinExponent = std::numeric_limits<double>::min_exponent10
                             - std::numeric_limits<double>::digits10 - 1;
const int kDoubleMaxExponent = std::numeric_limits<double>::max_exponent10;

// Minimum and maximum decimal exponent of numbers representable as float.
// "numeric_limits<float>::digits10 - 1"  is to take into account subnormal
// numbers.
const int kFloatMinExponent = std::numeric_limits<float>::min_exponent10
                            - std::numeric_limits<float>::digits10 - 1;
const int kFloatMaxExponent = std::numeric_limits<float>::max_exponent10;

// Precomputed powers of 10 in the entire ranges
// [kDoubleMinExponent, kDoubleMaxExponent] and
// [kFloatMinExponent, kFloatMaxExponent].
// We use "long double" instead of double and "double" for float exponents
// to avoid getting into subnormal numbers, and thus keeping relative error
// better than epsilon.
// Note that on some platforms "long double" may be the same type as double.
// On such platforms results of RoundDecimal() and TruncDecimal() will
// have error greater than 1 ULP if 10^(-digits) is a subnormal double number.
static long double kDecimalExponentDouble[kDoubleMaxExponent -
                                          kDoubleMinExponent + 1];
static double kDecimalExponentFloat[kFloatMaxExponent -
                                    kFloatMinExponent + 1];

void InitExponents() {
  for (int i = 0; i < ABSL_ARRAYSIZE(kDecimalExponentDouble); ++i) {
    // Note: exp10 and exp10l are GNU extensions. Fall back to pow and powl if
    // they're not available.
#ifdef _GNU_SOURCE
    kDecimalExponentDouble[i] = exp10l(i + kDoubleMinExponent);
#else
    kDecimalExponentDouble[i] = powl(10.0, i + kDoubleMinExponent);
#endif
  }
  for (int i = 0; i < ABSL_ARRAYSIZE(kDecimalExponentFloat); ++i) {
#ifdef _GNU_SOURCE
    kDecimalExponentFloat[i] = exp10(i + kFloatMinExponent);
#else
    kDecimalExponentFloat[i] = pow(10.0, i + kFloatMinExponent);
#endif
  }

#ifdef __x86_64__
  // For doubles on x86, the results from pow/exp10 cause issues for rounding
  // whole numbers which is a pretty obvious wart. Instead compute with repeated
  // multiplying/dividing by 10. Unfortunately, for floats doing this creates
  // those issues so we use two different methods for the types.
  //
  // No solution will be fully correct with floating points here, but this
  // prevents the obvious whole number warts.
  long double exponent = 1;
  kDecimalExponentDouble[-kDoubleMinExponent] = 1;
  // Compute exponents above 1.
  for (int i = 0; i < kDoubleMaxExponent; ++i) {
    exponent *= 10;
    kDecimalExponentDouble[1 + i - kDoubleMinExponent] = exponent;
  }
  exponent = 1;
  // Compute exponents below 1.
  for (int i = 0; i < -kDoubleMinExponent; ++i) {
    exponent /= 10;
    kDecimalExponentDouble[-kDoubleMinExponent - i - 1] = exponent;
  }
#endif  // __x86_64__
}

namespace {
static bool module_initialization_complete = []() {
  InitExponents();
  return true;
} ();
}  // namespace

// This function assumes that the FromType is same or wider than the ToType.
template <typename FromType, typename ToType>
static inline bool CastRounded(FromType in, ToType* out) {
  static_assert(std::is_floating_point<FromType>::value,
                "FromType must be floating point type");
  static_assert(std::is_floating_point<ToType>::value,
                "ToType must be floating point type");
  static_assert(std::numeric_limits<FromType>::lowest() <=
                        std::numeric_limits<ToType>::lowest() &&
                    std::numeric_limits<FromType>::max() >=
                        std::numeric_limits<ToType>::max(),
                "FromType must be wider than or equal to ToType");
  if ((in >= std::numeric_limits<ToType>::lowest() &&
       in <= std::numeric_limits<ToType>::max()) ||
      ABSL_PREDICT_FALSE(!std::isfinite(in))) {
    *out = static_cast<ToType>(in);
    return true;
  } else {
    return false;
  }
}

}  // anonymous namespace

template <>
bool RoundDecimal(double in, int64_t digits, double* out, absl::Status* error) {
  if (digits < -kDoubleMaxExponent) {
    *out = 0.0;
    return true;
  }
  if (digits > -kDoubleMinExponent) {
    *out = in;
    return true;
  }
  digits = -digits;
  const long double exp = kDecimalExponentDouble[digits - kDoubleMinExponent];

  long double x = in / exp;
  if (std::numeric_limits<long double>::max_exponent <
          std::numeric_limits<double>::max_exponent * 2 &&
      ABSL_PREDICT_FALSE(!std::isfinite(x))) {
    // "in / exp" overflows when "in" is a big number and we are rounding
    // digits to the right of the decimal point (digits > 0). In that case the
    // rounded value would be the same as "in" and we just return
    // the value of "in".
    *out = in;
    return true;
  }
  long double result = roundl(x) * exp;
  // 'result' may overflow if the exponent size of long double is insufficient
  // for the rounded result.
  if (std::numeric_limits<long double>::max_exponent <
          std::numeric_limits<double>::max_exponent * 2 &&
      ABSL_PREDICT_FALSE(!std::isfinite(result) && std::isfinite(in))) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("ROUND(", in, ", ", digits, ")"), error);
  }
  // Converting the result from long double to double may overflow.
  if (sizeof(long double) > sizeof(double)) {
    if (!CastRounded<long double, double>(result, out)) {
      return internal::SetFloatingPointOverflow(
          absl::StrCat("ROUND(", in, ", ", digits, ")"), error);
    } else {
      return true;
    }
  } else {
    *out = static_cast<double>(result);
    return true;
  }
}

template <>
bool RoundDecimal(float in, int64_t digits, float* out, absl::Status* error) {
  static_assert(std::numeric_limits<double>::max_exponent >=
                std::numeric_limits<float>::max_exponent * 2 ,
                "double's exponent must be wider than float's");
  if (digits < -kFloatMaxExponent) {
    *out = 0.0;
    return true;
  }
  if (digits > -kFloatMinExponent) {
    *out = in;
    return true;
  }
  digits = -digits;
  double exp = kDecimalExponentFloat[digits - kFloatMinExponent];
  // round(in / exp) * exp will never overflow due to the static_assert above.
  // Converting the result from double to float may overflow.
  if (!CastRounded<double, float>(round(in / exp) * exp, out)) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("ROUND(", in, ", ", digits, ")"), error);
  } else {
    return true;
  }
}
template <>
bool TruncDecimal(double in, int64_t digits, double* out, absl::Status* error) {
  if (digits < -kDoubleMaxExponent) {
    *out = 0.0;
    return true;
  }
  if (digits > -kDoubleMinExponent) {
    *out = in;
    return true;
  }
  digits = -digits;
  const long double exp = kDecimalExponentDouble[digits - kDoubleMinExponent];

  long double x = in / exp;
  if (std::numeric_limits<long double>::max_exponent <
          std::numeric_limits<double>::max_exponent * 2 &&
      ABSL_PREDICT_FALSE(!std::isfinite(x))) {
    // "in / exp" overflows when "in" is a big number and we are rounding
    // digits to the right of the decimal point (digits > 0). In that case the
    // truncated value would be the same as "in" and we just return
    // the value of "in".
    *out = in;
    return true;
  }
  *out = truncl(x) * exp;
  // Because truncl always rounds towards zero, the output value is less than
  // the input, so we do not expect an overflow to occur here.
  return true;
}

template <>
bool TruncDecimal(float in, int64_t digits, float* out, absl::Status* error) {
  static_assert(std::numeric_limits<double>::max_exponent >=
                std::numeric_limits<float>::max_exponent * 2 ,
                "double's exponent must be wider than float's");
  if (digits < -kFloatMaxExponent) {
    *out = 0.0;
    return true;
  }
  if (digits > -kFloatMinExponent) {
    *out = in;
    return true;
  }
  digits = -digits;
  double exp = kDecimalExponentFloat[digits - kFloatMinExponent];
  *out = trunc(in / exp) * exp;
  // trunc(in / exp) * exp will never overflow due to the static_assert above.
  // Because truncl always rounds towards zero, the output value is less than
  // the input, so we do not expect an overflow to occur here.
  return true;
}

namespace {
template <typename T>
inline bool SetNumericResultOrError(const absl::StatusOr<T>& status_or_numeric,
                                    T* out, absl::Status* error) {
  if (ABSL_PREDICT_TRUE(status_or_numeric.ok())) {
    *out = status_or_numeric.value();
    return true;
  }
  error->Update(status_or_numeric.status());
  return false;
}
}  // namespace

template <>
bool Round(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Round(0), out, error);
}

template <>
bool RoundDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error) {
  return SetNumericResultOrError(in.Round(digits), out, error);
}

template <>
bool Trunc(NumericValue in, NumericValue *out, absl::Status* error) {
  *out = in.Trunc(0);
  return true;
}

template <>
bool TruncDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error) {
  *out = in.Trunc(digits);
  return true;
}

template <>
bool Ceil(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Ceiling(), out, error);
}

template <>
bool Floor(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Floor(), out, error);
}

template <>
bool Sqrt(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Sqrt(), out, error);
}

template <>
bool Pow(NumericValue in1, NumericValue in2, NumericValue* out,
         absl::Status* error) {
  return SetNumericResultOrError(in1.Power(in2), out, error);
}

template <>
bool Exp(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Exp(), out, error);
}

template <>
bool NaturalLogarithm(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Ln(), out, error);
}

template <>
bool DecimalLogarithm(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Log10(), out, error);
}

template <>
bool Logarithm(NumericValue in1, NumericValue in2, NumericValue* out,
               absl::Status* error) {
  return SetNumericResultOrError(in1.Log(in2), out, error);
}

template <>
bool Ceil(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Ceiling(), out, error);
}

template <>
bool Floor(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Floor(), out, error);
}

template <>
bool Round(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Round(0), out, error);
}

template <>
bool RoundDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error) {
  return SetNumericResultOrError(in.Round(digits), out, error);
}

template <>
bool Trunc(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  *out = in.Trunc(0);
  return true;
}

template <>
bool TruncDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error) {
  *out = in.Trunc(digits);
  return true;
}

template <>
bool Sqrt(BigNumericValue in, BigNumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Sqrt(), out, error);
}

template <>
bool Pow(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
         absl::Status* error) {
  return SetNumericResultOrError(in1.Power(in2), out, error);
}

template <>
bool Exp(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Exp(), out, error);
}

template <>
bool NaturalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error) {
  return SetNumericResultOrError(in.Ln(), out, error);
}

template <>
bool DecimalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error) {
  return SetNumericResultOrError(in.Log10(), out, error);
}

template <>
bool Logarithm(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
               absl::Status* error) {
  return SetNumericResultOrError(in1.Log(in2), out, error);
}

}  // namespace functions
}  // namespace zetasql
