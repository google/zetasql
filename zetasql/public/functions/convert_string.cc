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

#include "zetasql/common/string_util.h"
#include "zetasql/public/functions/util.h"
#include "zetasql/base/string_numbers.h"
#include "absl/base/optimization.h"
#include "zetasql/base/case.h"
#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace functions {

namespace {

std::string FormatError(absl::string_view message, absl::string_view value) {
  static const int kMaxValueLength = 32;

  if (value.length() > kMaxValueLength) {
    return absl::StrCat(message,
                        absl::CEscape(value.substr(0, kMaxValueLength)), "...");
  } else {
    return absl::StrCat(message, absl::CEscape(value));
  }
}

// Trims the leading spaces of the given 'str'.
void TrimLeadingSpaces(absl::string_view* str) {
  while (absl::ConsumePrefix(str, " ")) {
  }
}

// Returns true if the 'str' is in hex format, which assumes no leading
// spaces but may have a sign before '0x'.
bool IsHex(absl::string_view str) {
  if (!str.empty() && (str[0] == '-' || str[0] == '+')) {
    str.remove_prefix(1);
  }
  return str.size() >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X');
}

constexpr absl::string_view kTrueStringValue = "true";
constexpr absl::string_view kFalseStringValue = "false";

}  // anonymous namespace

template <>
bool NumericToString(bool value, std::string* out, zetasql_base::Status* error) {
  if (value) {
    out->assign(kTrueStringValue.data(), kTrueStringValue.length());
  } else {
    out->assign(kFalseStringValue.data(), kFalseStringValue.length());
  }
  return true;
}

template <>
bool NumericToString(int32_t value, std::string* out, zetasql_base::Status* error) {
  out->clear();
  absl::StrAppend(out, value);
  return true;
}

template <>
bool NumericToString(int64_t value, std::string* out, zetasql_base::Status* error) {
  out->clear();
  absl::StrAppend(out, value);
  return true;
}

template <>
bool NumericToString(uint32_t value, std::string* out, zetasql_base::Status* error) {
  out->clear();
  absl::StrAppend(out, value);
  return true;
}

template <>
bool NumericToString(uint64_t value, std::string* out, zetasql_base::Status* error) {
  out->clear();
  absl::StrAppend(out, value);
  return true;
}

template <>
bool NumericToString(float value, std::string* out, zetasql_base::Status* error) {
  *out = RoundTripFloatToString(value);
  return true;
}

template <>
bool NumericToString(double value, std::string* out, zetasql_base::Status* error) {
  *out = RoundTripDoubleToString(value);
  return true;
}

template <>
bool NumericToString(NumericValue value, std::string* out, zetasql_base::Status* error) {
  *out = value.ToString();
  return true;
}

template <>
bool StringToNumeric(absl::string_view value, bool* out, zetasql_base::Status* error) {
  if (zetasql_base::CaseEqual(value, kTrueStringValue)) {
    *out = true;
  } else if (zetasql_base::CaseEqual(value, kFalseStringValue)) {
    *out = false;
  } else {
    return internal::UpdateError(error,
                                 FormatError("Bad bool value: ", value));
  }
  return true;
}

template <>
bool StringToNumeric(absl::string_view value, int32_t* out, zetasql_base::Status* error) {
  TrimLeadingSpaces(&value);
  if (ABSL_PREDICT_FALSE(IsHex(value))) {
    if (ABSL_PREDICT_TRUE(
            zetasql_base::safe_strto32_base(value, out, 16 /* base */)))
      return true;
  } else {
    if (ABSL_PREDICT_TRUE(absl::SimpleAtoi(value, out))) return true;
  }
  return internal::UpdateError(error, FormatError("Bad int32_t value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, int64_t* out, zetasql_base::Status* error) {
  TrimLeadingSpaces(&value);
  if (ABSL_PREDICT_FALSE(IsHex(value))) {
    if (ABSL_PREDICT_TRUE(
            zetasql_base::safe_strto64_base(value, out, 16 /* base */)))
      return true;
  } else {
    if (ABSL_PREDICT_TRUE(absl::SimpleAtoi(value, out))) return true;
  }
  return internal::UpdateError(error, FormatError("Bad int64_t value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, uint32_t* out,
                     zetasql_base::Status* error) {
  TrimLeadingSpaces(&value);
  if (ABSL_PREDICT_FALSE(IsHex(value))) {
    if (ABSL_PREDICT_TRUE(
            zetasql_base::safe_strtou32_base(value, out, 16 /* base */)))
      return true;
  } else {
    if (ABSL_PREDICT_TRUE(absl::SimpleAtoi(value, out))) return true;
  }
  return internal::UpdateError(error, FormatError("Bad uint32_t value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, uint64_t* out,
                     zetasql_base::Status* error) {
  TrimLeadingSpaces(&value);
  if (ABSL_PREDICT_FALSE(IsHex(value))) {
    if (ABSL_PREDICT_TRUE(
            zetasql_base::safe_strtou64_base(value, out, 16 /* base */)))
      return true;
  } else {
    if (ABSL_PREDICT_TRUE(absl::SimpleAtoi(value, out))) return true;
  }
  return internal::UpdateError(error, FormatError("Bad uint64_t value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, float* out, zetasql_base::Status* error) {
  if (ABSL_PREDICT_TRUE(absl::SimpleAtof(value, out))) return true;
  return internal::UpdateError(error, FormatError("Bad float value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, double* out,
                     zetasql_base::Status* error) {
  if (ABSL_PREDICT_TRUE(absl::SimpleAtod(value, out))) return true;
  return internal::UpdateError(error, FormatError("Bad double value: ", value));
}

template <>
bool StringToNumeric(absl::string_view value, NumericValue* out,
                     zetasql_base::Status* error) {
  const auto numeric_status = NumericValue::FromString(value);
  if (ABSL_PREDICT_TRUE(numeric_status.ok())) {
    *out = numeric_status.ValueOrDie();
    return true;
  }
  if (error != nullptr) {
    *error = numeric_status.status();
  }
  return false;
}

}  // namespace functions
}  // namespace zetasql
