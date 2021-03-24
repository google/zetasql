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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_MATH_H_
#define ZETASQL_PUBLIC_FUNCTIONS_MATH_H_

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/util.h"
#include "zetasql/public/numeric_value.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

template <typename T> inline bool Abs(T in, T *out, absl::Status* error);
template <typename T> inline bool Sign(T in, T *out, absl::Status* error);
template <typename T> inline bool Round(T in, T *out, absl::Status* error);
template <typename T>
bool RoundDecimal(T in, int64_t digits, T* out, absl::Status* error);
template <typename T> inline bool Trunc(T in, T *out, absl::Status* error);
template <typename T>
bool TruncDecimal(T in, int64_t digits, T* out, absl::Status* error);
template <typename T> inline bool Ceil(T in, T *out, absl::Status* error);
template <typename T> inline bool Floor(T in, T *out, absl::Status* error);
template <typename T> inline bool IsInf(T in, bool* out, absl::Status* error);
template <typename T> inline bool IsNan(T in, bool* out, absl::Status* error);
template <typename T> inline bool IeeeDivide(T in1, T in2, T *out,
                                             absl::Status* error);
template <typename T> inline bool Sqrt(T in, T* out, absl::Status* error);
template <typename T> inline bool Pow(T in1, T in2, T* out,
                                      absl::Status* error);
template <typename T> inline bool Exp(T in, T* out, absl::Status* error);
template <typename T> inline bool NaturalLogarithm(T in, T* out,
                                                   absl::Status* error);
template <typename T> inline bool DecimalLogarithm(T in, T* out,
                                                   absl::Status* error);
template <typename T> inline bool Logarithm(T in1, T in2, T* out,
                                            absl::Status* error);
template <typename T> inline bool Cos(T in, T* out, absl::Status* error);
template <typename T> inline bool Cosh(T in, T* out, absl::Status* error);
template <typename T> inline bool Acos(T in, T* out, absl::Status* error);
template <typename T> inline bool Acosh(T in, T* out, absl::Status* error);

template <typename T> inline bool Sin(T in, T* out, absl::Status* error);
template <typename T> inline bool Sinh(T in, T* out, absl::Status* error);
template <typename T> inline bool Asin(T in, T* out, absl::Status* error);
template <typename T> inline bool Asinh(T in, T* out, absl::Status* error);
template <typename T> inline bool Tan(T in, T* out, absl::Status* error);
template <typename T> inline bool Tanh(T in, T* out, absl::Status* error);
template <typename T> inline bool Atan(T in, T* out, absl::Status* error);
template <typename T> inline bool Atanh(T in, T* out, absl::Status* error);
template <typename T> inline bool Atan2(T in1, T in2, T* out,
                                        absl::Status* error);

namespace internal {

inline bool SetFloatingPointError(absl::string_view description,
                                  absl::Status* error) {
  return internal::UpdateError(
      error, absl::StrCat("Floating point error in function: ", description));
}

inline bool SetFloatingPointOverflow(absl::string_view description,
                                     absl::Status* error) {
  return internal::UpdateError(
      error,
      absl::StrCat("Floating point overflow in function: ", description));
}

// Checks if a floating point computation resulted in an error by making
// sure "out" is a finite value. Sets "error" if an error was detected.
template <typename T>
inline bool CheckFloatingPointError(absl::string_view name, T in, T out,
                                    absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  if (ABSL_PREDICT_TRUE(std::isfinite(out))) {
    return true;
  } else if (!std::isfinite(in)) {
    return true;
  } else {
    return SetFloatingPointError(absl::StrCat(name, "(", in, ")"), error);
  }
}

// Similar to CheckFloatinPointError but the error message explicitly says that
// there was an overflow.
template <typename T>
inline bool CheckFloatingPointOverflow(absl::string_view name, T in, T out,
                                       absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  if (ABSL_PREDICT_TRUE(std::isfinite(out))) {
    return true;
  } else if (!std::isfinite(in)) {
    return true;
  } else {
    return SetFloatingPointOverflow(absl::StrCat(name, "(", in, ")"), error);
  }
}

// Similar to CheckFloatingPointError but for functions with two arguments.
template <typename T>
bool CheckFloatingPointError(absl::string_view name, T in1, T in2, T out,
                             absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  if (ABSL_PREDICT_TRUE(std::isfinite(out))) {
    return true;
  } else if (!std::isfinite(in1) || !std::isfinite(in2)) {
    return true;
  } else {
    return SetFloatingPointError(absl::StrCat(name, "(", in1, ", ", in2, ")"),
                                 error);
  }
}

}  // namespace internal

template <typename T>
inline bool Abs(T in, T *out, absl::Status* error) {
  if (in < 0) return UnaryMinus<T, T>(in, out, error);
  *out = in;
  return true;
}

template <>
inline bool Abs(float in, float *out, absl::Status* error) {
  *out = fabs(in);
  return true;
}

template <>
inline bool Abs(double in, double *out, absl::Status* error) {
  *out = fabs(in);
  return true;
}

template <>
inline bool Abs(uint32_t in, uint32_t* out, absl::Status* error) {
  *out = in;
  return true;
}

template <>
inline bool Abs(uint64_t in, uint64_t* out, absl::Status* error) {
  *out = in;
  return true;
}

template <>
inline bool Abs(NumericValue in, NumericValue *out, absl::Status* error) {
  *out = in.Abs();
  return true;
}

template <>
inline bool Abs(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  auto status_or_numeric = in.Abs();
  if (!status_or_numeric.ok()) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("ABS(", in.ToString(), ")"), error);
  }
  *out = status_or_numeric.value();
  return true;
}

template <typename T>
inline bool Sign(T in, T *out, absl::Status* error) {
  if (in == 0) {
    *out = 0;
  } else if (std::isnan(in)) {
    *out = in;
  } else {
    *out = (in > 0 ? 1 : -1);
  }
  return true;
}

template <>
inline bool Sign(NumericValue in, NumericValue *out, absl::Status* error) {
  *out = NumericValue(in.Sign());
  return true;
}

template <>
inline bool Sign(BigNumericValue in, BigNumericValue* out,
                 absl::Status* error) {
  *out = BigNumericValue(in.Sign());
  return true;
}

template <typename T>
inline bool Round(T in, T *out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::round(in);
  return true;
}

template <typename T>
inline bool Trunc(T in, T *out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::trunc(in);
  return true;
}

template <typename T>
inline bool Ceil(T in, T *out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::ceil(in);
  return true;
}

template <typename T>
inline bool Floor(T in, T *out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::floor(in);
  return true;
}

template <typename T>
inline bool IsInf(T in, bool* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::isinf(in);
  return true;
}

template <typename T>
inline bool IsNan(T in, bool* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::isnan(in);
  return true;
}

template <typename T>
inline bool IeeeDivide(T in1, T in2, T* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = in1 / in2;
  return true;
}

template <typename T>
bool Sqrt(T in, T* out, absl::Status* error) {
  if (ABSL_PREDICT_FALSE(in < 0)) {
    return internal::UpdateError(
        error, absl::StrCat("Argument to SQRT cannot be negative: ", in));
  }
  *out = std::sqrt(in);
  return true;
}

template <typename T>
bool Pow(T in1, T in2, T* out, absl::Status* error) {
  *out = pow(in1, in2);
  return internal::CheckFloatingPointError("POW", in1, in2, *out, error);
}

template <typename T>
bool Exp(T in, T* out, absl::Status* error) {
  *out = std::exp(in);
  return internal::CheckFloatingPointError("EXP", in, *out, error);
}

template <typename T>
bool NaturalLogarithm(T in, T* out, absl::Status* error) {
  *out = std::log(in);
  return internal::CheckFloatingPointError("LN", in, *out, error);
}

template <typename T>
bool DecimalLogarithm(T in, T* out, absl::Status* error) {
  *out = std::log10(in);
  return internal::CheckFloatingPointError("LOG10", in, *out, error);
}

template <typename T>
bool Logarithm(T in1, T in2, T* out, absl::Status* error) {
  if (std::isfinite(in2) && in2 > 0) {
    *out = std::log(in1) / std::log(in2);
  } else {
    *out = std::numeric_limits<T>::quiet_NaN();
  }
  return internal::CheckFloatingPointError("LOG", in1, in2, *out, error);
}

template <typename T>
bool Cos(T in, T* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::cos(in);
  // cos() does not introduce Inf or NaN - no error checking required.
  return true;
}

template <typename T>
bool Cosh(T in, T* out, absl::Status* error) {
  *out = std::cosh(in);
  return internal::CheckFloatingPointOverflow("COSH", in, *out, error);
}

template <typename T>
bool Acos(T in, T* out, absl::Status* error) {
  *out = std::acos(in);
  return internal::CheckFloatingPointError("ACOS", in, *out, error);
}

template <typename T>
bool Acosh(T in, T* out, absl::Status* error) {
  *out = std::acosh(in);
  return internal::CheckFloatingPointError("ACOSH", in, *out, error);
}

template <typename T>
bool Sin(T in, T* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::sin(in);
  // sin() does not introduce Inf or NaN - no error checking required.
  return true;
}

template <typename T>
bool Sinh(T in, T* out, absl::Status* error) {
  *out = std::sinh(in);
  return internal::CheckFloatingPointOverflow("SINH", in, *out, error);
}

template <typename T>
bool Asin(T in, T* out, absl::Status* error) {
  *out = std::asin(in);
  return internal::CheckFloatingPointError("ASIN", in, *out, error);
}

template <typename T>
bool Asinh(T in, T* out, absl::Status* error) {
  *out = std::asinh(in);
  return internal::CheckFloatingPointError("ASINH", in, *out, error);
}


template <typename T>
bool Tan(T in, T* out, absl::Status* error) {
  *out = std::tan(in);
  return internal::CheckFloatingPointOverflow("TAN", in, *out, error);
}

template <typename T>
bool Tanh(T in, T* out, absl::Status* error) {
  static_assert(std::is_floating_point<T>::value,
                "T must be floating point type");
  *out = std::tanh(in);
  // tanh() does not introduce Inf or NaN - no error checking required.
  return true;
}

template <typename T>
bool Atan(T in, T* out, absl::Status* error) {
  *out = std::atan(in);
  // atan() does not introduce Inf or NaN - no error checking required.
  return true;
}

template <typename T>
bool Atanh(T in, T* out, absl::Status* error) {
  *out = std::atanh(in);
  return internal::CheckFloatingPointError("ATANH", in, *out, error);
}

template <typename T>
bool Atan2(T in1, T in2, T* out, absl::Status* error) {
  *out = atan2(in1, in2);
  return internal::CheckFloatingPointError("ATAN2", in1, in2, *out, error);
}

// Declare specializations defined in math.cc.

template <>
bool RoundDecimal(double in, int64_t digits, double* out, absl::Status* error);
template <>
bool RoundDecimal(float in, int64_t digits, float* out, absl::Status* error);

template <>
bool TruncDecimal(double in, int64_t digits, double* out, absl::Status* error);
template <>
bool TruncDecimal(float in, int64_t digits, float* out, absl::Status* error);

template <>
bool Round(NumericValue in, NumericValue *out, absl::Status* error);
template <>
bool RoundDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error);
template <>
bool Trunc(NumericValue in, NumericValue *out, absl::Status* error);
template <>
bool TruncDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error);

template <> bool Ceil(NumericValue in, NumericValue *out, absl::Status* error);
template <> bool Floor(NumericValue in, NumericValue *out, absl::Status* error);

template <>
bool Sqrt(NumericValue in, NumericValue* out, absl::Status* error);
template <> bool Pow(
    NumericValue in1, NumericValue in2, NumericValue* out,
    absl::Status* error);
template <>
bool Exp(NumericValue in, NumericValue* out, absl::Status* error);
template <>
bool NaturalLogarithm(NumericValue in, NumericValue* out, absl::Status* error);
template <>
bool DecimalLogarithm(NumericValue in, NumericValue* out, absl::Status* error);
template <>
bool Logarithm(NumericValue in1, NumericValue in2, NumericValue* out,
               absl::Status* error);

template <>
bool Ceil(BigNumericValue in, BigNumericValue* out, absl::Status* error);
template <>
bool Floor(BigNumericValue in, BigNumericValue* out, absl::Status* error);

template <>
bool Round(BigNumericValue in, BigNumericValue* out, absl::Status* error);
template <>
bool RoundDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error);
template <>
bool Trunc(BigNumericValue in, BigNumericValue* out, absl::Status* error);
template <>
bool TruncDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error);

template <>
bool Sqrt(BigNumericValue in, BigNumericValue* out, absl::Status* error);
template <>
bool Pow(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
         absl::Status* error);
template <>
bool Exp(BigNumericValue in, BigNumericValue* out, absl::Status* error);
template <>
bool NaturalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error);
template <>
bool DecimalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error);
template <>
bool Logarithm(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
               absl::Status* error);

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_MATH_H_
