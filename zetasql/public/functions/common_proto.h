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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_COMMON_PROTO_H_
#define ZETASQL_PUBLIC_FUNCTIONS_COMMON_PROTO_H_

// This file defines conversion functions for some of the common protocol buffer
// types found in google/protobuf/ and google/type. Supported
// conversions can be found at (broken link).

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "google/protobuf/wrappers.pb.h"
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include <cstdint>
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {
namespace functions {
namespace internal {

// An invokable struct that defines the input->output type relationship for
// conversions of the proto3 WKT wrappers.
struct ValidWrapperConversions {
    bool operator()(google::protobuf::BoolValue);
    double operator()(google::protobuf::DoubleValue);
    float operator()(google::protobuf::FloatValue);
    int64_t operator()(google::protobuf::Int64Value);
    uint64_t operator()(google::protobuf::UInt64Value);
    int32_t operator()(google::protobuf::Int32Value);
    uint32_t operator()(google::protobuf::UInt32Value);
    std::string operator()(google::protobuf::StringValue);
    absl::Cord operator()(google::protobuf::BytesValue);
};
}  // namespace internal


// Converts a ZetaSQL TimeValue to a google::type::TimeOfDay.
absl::Status ConvertTimeToProto3TimeOfDay(TimeValue input,
                                          google::type::TimeOfDay* output);

// Converts a google::type::TimeOfDay to a ZetaSQL TimeValue.
absl::Status ConvertProto3TimeOfDayToTime(const google::type::TimeOfDay& input,
                                          TimestampScale scale,
                                          TimeValue* output);

// Converts a proto3 wrapper for a primitive type (defined
// in google/protobuf/wrappers.proto) to a ZetaSQL type. Conversion
// from google::protobuf::StringValue requires the string to be a valid UTF-8
// encoding. 'output' must have the same type as the 'value' field of the
// wrapper message.
template <typename Wrapper>
absl::Status ConvertProto3WrapperToType(
    const Wrapper& input,
    typename std::result_of<internal::ValidWrapperConversions(Wrapper)>::type*
        output) {
  *output = input.value();
  return absl::OkStatus();
}

template <>
inline absl::Status ConvertProto3WrapperToType(
    const google::protobuf::StringValue& input, std::string* output) {
  if (!IsWellFormedUTF8(input.value())) {
    return MakeEvalError()
           << "Invalid conversion: ZetaSQL strings must be UTF8 encoded"
           << input.DebugString();
  }
  *output = input.value();
  return absl::OkStatus();
}

// Converts a ZetaSQL type to a proto3 wrapper of a primitive type (defined
// in google/protobuf/wrappers.proto). 'input' type must be equal
// to the type of the 'value' field in the wrapper.
template <typename Wrapper>
void ConvertTypeToProto3Wrapper(
    const typename std::result_of<
        internal::ValidWrapperConversions(Wrapper)>::type& input,
    Wrapper* output) {
  output->set_value(input);
}

template <>
inline absl::Status ConvertProto3WrapperToType(
    const google::protobuf::BytesValue& input, absl::Cord* output) {
  *output = absl::Cord(input.value());
  return absl::OkStatus();
}

template <>
inline void ConvertTypeToProto3Wrapper(
    const absl::Cord& input, google::protobuf::BytesValue* output) {
  output->set_value(std::string(input));
}
}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_COMMON_PROTO_H_
