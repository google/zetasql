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

#include "zetasql/public/functions/util.h"

#include "zetasql/common/utf_util.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {
namespace internal {

constexpr char ArithmeticType<int32_t>::kName[];
constexpr char ArithmeticType<int64_t>::kName[];
constexpr char ArithmeticType<uint32_t>::kName[];
constexpr char ArithmeticType<uint64_t>::kName[];
constexpr char ArithmeticType<float>::kName[];
constexpr char ArithmeticType<double>::kName[];

bool UpdateError(zetasql_base::Status* status, absl::string_view msg) {
  if (status != nullptr && status->ok()) {
    // 'msg' could potentially contain invalid UTF-8 characters. As example
    // RegEx generates an error for invalid input, but the input could be
    // invalid UTF-8.
    // zetasql_base::Status will generate a warning in DEBUG mode if the error
    // message is not UTF-8, so coerce it to be a valid UTF-8 std::string.
    std::string error = CoerceToWellFormedUTF8(msg);
    *status = zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange, error);
  }
  return false;
}

template <typename T>
std::string UnaryOverflowMessage(T in, absl::string_view operator_symbol) {
  return absl::StrCat(ArithmeticType<T>::kName, " overflow: ", operator_symbol,
                      in);
}

template std::string UnaryOverflowMessage<int32_t>(int32_t in,
                                            absl::string_view operator_symbol);
template std::string UnaryOverflowMessage<int64_t>(int64_t in,
                                            absl::string_view operator_symbol);

template <typename T>
std::string BinaryOverflowMessage(T in1, T in2, absl::string_view operator_symbol) {
  return absl::StrCat(ArithmeticType<T>::kName, " overflow: ", in1,
                      operator_symbol, in2);
}

template std::string BinaryOverflowMessage<int32_t>(int32_t in1, int32_t in2,
                                             absl::string_view operator_symbol);
template std::string BinaryOverflowMessage<int64_t>(int64_t in1, int64_t in2,
                                             absl::string_view operator_symbol);
template std::string BinaryOverflowMessage<uint64_t>(
    uint64_t in1, uint64_t in2, absl::string_view operator_symbol);
template std::string BinaryOverflowMessage<float>(float in1, float in2,
                                             absl::string_view operator_symbol);
template std::string BinaryOverflowMessage<double>(
    double in1, double in2, absl::string_view operator_symbol);

template <typename T>
std::string DivisionByZeroMessage(T in1, T in2) {
  return absl::StrCat("division by zero: ", in1, " / ", in2);
}

template std::string DivisionByZeroMessage<int64_t>(int64_t in1, int64_t in2);
template std::string DivisionByZeroMessage<uint64_t>(uint64_t in1, uint64_t in2);
template std::string DivisionByZeroMessage<double>(double in1, double in2);

}  // namespace internal
}  // namespace functions
}  // namespace zetasql
