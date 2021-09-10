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

#include "zetasql/public/functions/to_json.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/functions/json_format.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {

constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();
constexpr uint64_t kUint64Min = std::numeric_limits<uint64_t>::min();
constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();

// Returns JSONValue constructed from NumericValue and BigNumericValue.
// If the value is int64_t or uint64_t, use the corresponding value directly.
// If double, checks whether NumericValue/BigNumericValue can be
// converted to DOUBLE without precision loss and use the double value to
// construct the json if no loss. Otherwide returns the rounded value iff
// FEATURE_JSON_STRICT_NUMBER_PARSING is false.
template <typename T>
absl::StatusOr<JSONValue> ToJsonFromNumeric(
    const T& value, bool stringify_wide_number,
    const LanguageOptions& language_options, const std::string_view type_name) {
  if (!value.HasFractionalPart()) {
    // Check whether the value is int64_t
    if (value >= T(kInt64Min) && value <= T(kInt64Max)) {
      ZETASQL_ASSIGN_OR_RETURN(int64_t int64value, value.template To<int64_t>());
      return ToJson(Value::Int64(int64value), stringify_wide_number,
                    language_options);
    }
    // Check whether the value is uint64_t
    if (value >= T(kUint64Min) && value <= T(kUint64Max)) {
      ZETASQL_ASSIGN_OR_RETURN(uint64_t uint64value, value.template To<uint64_t>());
      return ToJson(Value::Uint64(uint64value), stringify_wide_number,
                    language_options);
    }
  }
  // Check whether the value can be converted to double without precision loss
  if (internal::CheckNumberRoundtrip(value.ToString(), value.ToDouble()).ok()) {
    return JSONValue(value.ToDouble());
  }
  if (stringify_wide_number) {
    return JSONValue(value.ToString());
  }
  if (language_options.LanguageFeatureEnabled(
          FEATURE_JSON_STRICT_NUMBER_PARSING)) {
    return MakeEvalError() << "Failed to convert type " << type_name
                           << " to JSON";
  }
  return JSONValue(value.ToDouble());
}

// Returns the JSONValue from float and double.
// If the value is Infinity, -Infinity, or NaN, returns the json string
// representation. Otherwise returns json number type.
template <typename FloatType>
JSONValue ToJsonFromFloat(FloatType value) {
  if (std::isnan(value)) {
    return JSONValue(std::string("NaN"));
  }
  if (std::isinf(value)) {
    return value > 0 ? JSONValue(std::string("Infinity"))
                     : JSONValue(std::string("-Infinity"));
  }
  ZETASQL_DCHECK(std::isfinite(value))
      << "Floating point number with unexpected properties" << value;
  return JSONValue(value);
}

// Helper function for ToJson except that this function internally keeps
// tracking of <current_nesting_level> for STRUCT and ARRAY and checks stack
// space when <current_nesting_level> not less than
// kNestingLevelStackCheckThreshold. Returns StatusCode::kResourceExhausted when
// stack overflows.
absl::StatusOr<JSONValue> ToJsonHelper(const Value& value,
                                       bool stringify_wide_numbers,
                                       const LanguageOptions& language_options,
                                       int current_nesting_level) {
  // Check the stack usage iff the <current_neesting_level> not less than
  // kNestingLevelStackCheckThreshold.
  if (current_nesting_level >= kNestingLevelStackCheckThreshold) {
  }
  if (value.is_null()) {
    return JSONValue();
  }
  switch (value.type_kind()) {
    case TYPE_BOOL:
      return JSONValue(value.bool_value());
    case TYPE_INT32:
      return JSONValue(static_cast<int64_t>(value.int32_value()));
    case TYPE_UINT32:
      return JSONValue(static_cast<uint64_t>(value.uint32_value()));
    case TYPE_INT64: {
      const int64_t local = value.int64_value();
      if (stringify_wide_numbers && (local < kMinLosslessInt64ValueForJson ||
                                     local > kMaxLosslessInt64ValueForJson)) {
        return JSONValue(std::to_string(local));
      } else {
        return JSONValue(local);
      }
    }
    case TYPE_UINT64: {
      const uint64_t local = value.uint64_value();
      if (stringify_wide_numbers && local > kMaxLosslessInt64ValueForJson) {
        return JSONValue(std::to_string(local));
      } else {
        return JSONValue(local);
      }
    }
    case TYPE_FLOAT:
      return ToJsonFromFloat(value.float_value());
    case TYPE_DOUBLE:
      return ToJsonFromFloat(value.double_value());
    case TYPE_NUMERIC:
      return ToJsonFromNumeric(
          value.numeric_value(), stringify_wide_numbers, language_options,
          value.type()->ShortTypeName(language_options.product_mode()));
    case TYPE_BIGNUMERIC:
      return ToJsonFromNumeric(
          value.bignumeric_value(), stringify_wide_numbers, language_options,
          value.type()->ShortTypeName(language_options.product_mode()));
      break;
    case TYPE_STRING: {
      return JSONValue(value.string_value());
    }
    case TYPE_BYTES: {
      std::string tmp;
      JsonFromBytes(value.bytes_value(), &tmp, /*quote_output_string=*/false);
      return JSONValue(std::move(tmp));
    }
    case TYPE_TIMESTAMP: {
      std::string timestamp_string;
      ZETASQL_RETURN_IF_ERROR(JsonFromTimestamp(value.ToTime(), &timestamp_string,
                                        /*quote_output_string=*/false));
      return JSONValue(std::move(timestamp_string));
    }
    case TYPE_DATE: {
      std::string date_string;
      ZETASQL_RETURN_IF_ERROR(JsonFromDate(value.date_value(), &date_string,
                                   /*quote_output_string=*/false));
      return JSONValue(std::move(date_string));
    }
    case TYPE_DATETIME: {
      std::string datetime_string;
      ZETASQL_RETURN_IF_ERROR(JsonFromDatetime(value.datetime_value(), &datetime_string,
                                       /*quote_output_string=*/false));
      return JSONValue(std::move(datetime_string));
    }
    case TYPE_TIME: {
      std::string time_string;
      ZETASQL_RETURN_IF_ERROR(JsonFromTime(value.time_value(), &time_string,
                                   /*quote_output_string=*/false));
      return JSONValue(std::move(time_string));
    }
    case TYPE_INTERVAL:
      return JSONValue(value.interval_value().ToISO8601());
    case TYPE_JSON: {
      if (value.is_validated_json()) {
        return JSONValue::CopyFrom(value.json_value());
      }
      auto input_json = JSONValue::ParseJSONString(
          value.json_value_unparsed(),
          JSONParsingOptions{
              .legacy_mode = language_options.LanguageFeatureEnabled(
                  FEATURE_JSON_LEGACY_PARSE),
              .strict_number_parsing = language_options.LanguageFeatureEnabled(
                  FEATURE_JSON_STRICT_NUMBER_PARSING)});
      if (!input_json.ok()) {
        return MakeEvalError() << input_json.status().message();
      }
      return input_json;
    }
    case TYPE_STRUCT: {
      JSONValue json_value;
      JSONValueRef json_value_ref = json_value.GetRef();
      json_value_ref.SetToEmptyObject();
      const StructType* struct_type = value.type()->AsStruct();
      int field_index = 0;
      for (const auto& field_value : value.fields()) {
        std::string_view name = struct_type->field(field_index++).name;
        // If there is already a member existed, skip the further
        // processing as we only keep the first value of each member.
        if (json_value_ref.HasMember(name)) {
          continue;
        }
        ZETASQL_ASSIGN_OR_RETURN(
            JSONValue json_member_value,
            ToJsonHelper(field_value, stringify_wide_numbers, language_options,
                         current_nesting_level + 1));
        JSONValueRef member_value_ref = json_value_ref.GetMember(name);
        member_value_ref.Set(std::move(json_member_value));
      }
      return json_value;
    }
    case TYPE_ARRAY: {
      JSONValue json_value;
      JSONValueRef json_value_ref = json_value.GetRef();
      json_value_ref.SetToEmptyArray();
      if (!value.elements().empty()) {
        json_value_ref.GetArrayElement(value.num_elements() - 1);
        int element_index = 0;
        for (const auto& element_value : value.elements()) {
          ZETASQL_ASSIGN_OR_RETURN(
              JSONValue json_element,
              ToJsonHelper(element_value, stringify_wide_numbers,
                           language_options, current_nesting_level + 1));
          json_value_ref.GetArrayElement(element_index)
              .Set(std::move(json_element));
          element_index++;
        }
      }
      return json_value;
    }
    case TYPE_ENUM: {
      return JSONValue(value.enum_name());
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unsupported argument type "
             << value.type()->ShortTypeName(language_options.product_mode())
             << " for TO_JSON";
  }
}

}  // namespace

absl::StatusOr<JSONValue> ToJson(const Value& value,
                                 bool stringify_wide_numbers,
                                 const LanguageOptions& language_options) {
  return ToJsonHelper(value, stringify_wide_numbers, language_options,
                      /*current_nesting_level=*/0);
}

}  // namespace functions
}  // namespace zetasql
