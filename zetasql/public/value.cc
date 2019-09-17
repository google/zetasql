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

#include "zetasql/public/value.h"

#include <string.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <stack>
#include <utility>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/message_differencer.h"
#include "zetasql/common/string_util.h"
#include "zetasql/public/functions/comparison.h"
#include "zetasql/public/functions/convert_proto.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/memory/memory.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/time_proto_util.h"

using zetasql::types::BoolArrayType;
using zetasql::types::BoolType;
using zetasql::types::BytesArrayType;
using zetasql::types::BytesType;
using zetasql::types::DatetimeType;
using zetasql::types::DateType;
using zetasql::types::DoubleArrayType;
using zetasql::types::DoubleType;
using zetasql::types::FloatArrayType;
using zetasql::types::FloatType;
using zetasql::types::GeographyType;
using zetasql::types::Int32ArrayType;
using zetasql::types::Int32Type;
using zetasql::types::Int64ArrayType;
using zetasql::types::Int64Type;
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::StringArrayType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using zetasql::types::TimeType;
using zetasql::types::Uint32ArrayType;
using zetasql::types::Uint32Type;
using zetasql::types::Uint64ArrayType;
using zetasql::types::Uint64Type;

using absl::Substitute;

namespace zetasql {

// -------------------------------------------------------
// Value
// -------------------------------------------------------

std::ostream& operator<<(std::ostream& out, const Value& value) {
  return out << value.FullDebugString();
}

// Null value constructor.
Value::Value(const Type* type)
    : type_kind_(type->kind()), is_null_(true) {
  CHECK(type != nullptr);
  switch (type_kind()) {
    case TYPE_INT32:
    case TYPE_INT64:
    case TYPE_UINT32:
    case TYPE_UINT64:
    case TYPE_BOOL:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DATE:
    case TYPE_TIMESTAMP:
    case TYPE_TIME:
    case TYPE_DATETIME:
      break;
    case TYPE_STRING:
    case TYPE_BYTES:
      string_ptr_ = new StringRef();
      break;
    case TYPE_GEOGRAPHY:
      geography_ptr_ = new GeographyRef();
      break;
    case TYPE_NUMERIC:
      numeric_ptr_ = new NumericRef();
      break;
    case TYPE_ENUM:
      enum_type_ = type->AsEnum();
      break;
    case TYPE_ARRAY:
    case TYPE_STRUCT:
      list_ptr_ = new TypedList(type);
      break;
    case TYPE_PROTO:
      proto_ptr_ = new ProtoRep(type->AsProto(),
                                "");
      break;
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      // Handling this case is only allowed internally.
      LOG(FATAL) << "Cannot construct value with type " << type_kind();
  }
}

void Value::CopyFrom(const Value& that) {
  // Self-copy check is done in the copy constructor. Here we just DCHECK that.
  DCHECK_NE(this, &that);
  memcpy(this, &that, sizeof(Value));
  switch (that.type_kind_) {
    case TYPE_STRUCT:
    case TYPE_ARRAY:
      list_ptr_->Ref();
      break;
    case TYPE_STRING:
    case TYPE_BYTES:
      string_ptr_->Ref();
      break;
    case TYPE_GEOGRAPHY:
      geography_ptr_->Ref();
      break;
    case TYPE_NUMERIC:
      numeric_ptr_->Ref();
      break;
    case TYPE_PROTO:
      proto_ptr_->Ref();
      break;
  }
}

Value::Value(TypeKind type_kind, int64_t value)
    : type_kind_(type_kind) {
  switch (type_kind) {
    case TYPE_DATE:
      CHECK_LE(value, types::kDateMax);
      CHECK_GE(value, types::kDateMin);
      int32_value_ = value;
      break;
    default:
      LOG(FATAL) << "Invalid use of private constructor: " << type_kind;
  }
}

Value::Value(absl::Time t) : type_kind_(TYPE_TIMESTAMP) {
  CHECK(functions::IsValidTime(t));
  timestamp_seconds_ = absl::ToUnixSeconds(t);
  subsecond_nanos_ =
      (t - absl::FromUnixSeconds(timestamp_seconds_)) / absl::Nanoseconds(1);
}

Value::Value(TimeValue time) : type_kind_(TYPE_TIME) {
  CHECK(time.IsValid());
  subsecond_nanos_ = time.Nanoseconds();
  bit_field_32_value_ = time.Packed32TimeSeconds();
}

Value::Value(DatetimeValue datetime) : type_kind_(TYPE_DATETIME) {
  CHECK(datetime.IsValid());
  subsecond_nanos_ = datetime.Nanoseconds();
  bit_field_64_value_ = datetime.Packed64DatetimeSeconds();
}

Value::Value(const EnumType* enum_type, int64_t value)
    : type_kind_(TYPE_ENUM) {
  const std::string* unused;
  if (value >= std::numeric_limits<int32_t>::min() &&
      value <= std::numeric_limits<int32_t>::max() &&
      enum_type->FindName(value, &unused)) {
    // As only int32_t range enum values are supported, value can safely be casted
    // to int32_t once verified under enum range.
    enum_value_ = static_cast<int32_t>(value);
    enum_type_ = enum_type;
  } else {
    type_kind_ = kInvalidTypeKind;
    enum_value_ = 0;
    enum_type_ = nullptr;
  }
}

Value::Value(const EnumType* enum_type, absl::string_view name)
    : type_kind_(TYPE_ENUM) {
  int32_t number;
  if (enum_type->FindNumber(std::string(name), &number)) {
    enum_value_ = number;
    enum_type_ = enum_type;
  } else {
    type_kind_ = kInvalidTypeKind;
    enum_value_ = 0;
    enum_type_ = nullptr;
  }
}

Value::Value(const ProtoType* proto_type, const std::string& value)
    : type_kind_(TYPE_PROTO),
      proto_ptr_(new ProtoRep(proto_type, std::move(value))) {}

#ifdef NDEBUG
static constexpr bool kDebugMode = false;
#else
static constexpr bool kDebugMode = true;
#endif

Value Value::ArrayInternal(bool safe, const ArrayType* array_type,
                           OrderPreservationKind order_kind,
                           std::vector<Value>&& values) {
  Value result(array_type);
  result.is_null_ = false;
  result.order_kind_ = order_kind;
  std::vector<Value>& value_list = result.list_ptr_->values();
  value_list = std::move(values);
  if (kDebugMode || safe) {
    for (const Value& v : value_list) {
      CHECK(v.type()->Equals(array_type->element_type()))
          << "Array element " << v << " must be of type "
          << array_type->element_type()->DebugString();
    }
  }
  return result;
}

Value Value::StructInternal(bool safe, const StructType* struct_type,
                            std::vector<Value>&& values) {
  Value result(struct_type);
  result.is_null_ = false;
  std::vector<Value>& value_list = result.list_ptr_->values();
  value_list = std::move(values);
  if (kDebugMode || safe) {
    // Check that values are compatible with the type.
    CHECK_EQ(struct_type->num_fields(), value_list.size());
    for (int i = 0; i < value_list.size(); ++i) {
      const Type* field_type = struct_type->field(i).type;
      const Type* value_type = value_list[i].type();
      CHECK(field_type->Equals(value_type))
          << "\nField type: " << field_type->DebugString()
          << "\nvs\nValue type: " << value_type->DebugString();
    }
  }
  return result;
}

const Type* Value::type() const {
  CHECK(is_valid()) << DebugString();
  switch (type_kind()) {
    case TYPE_INT32: return Int32Type();
    case TYPE_INT64: return Int64Type();
    case TYPE_UINT32: return Uint32Type();
    case TYPE_UINT64: return Uint64Type();
    case TYPE_BOOL: return BoolType();
    case TYPE_FLOAT: return FloatType();
    case TYPE_DOUBLE: return DoubleType();
    case TYPE_STRING: return StringType();
    case TYPE_BYTES: return BytesType();
    case TYPE_DATE: return DateType();
    case TYPE_TIMESTAMP: return TimestampType();
    case TYPE_TIME: return TimeType();
    case TYPE_DATETIME: return DatetimeType();
    case TYPE_GEOGRAPHY: return GeographyType();
    case TYPE_NUMERIC: return NumericType();
    case TYPE_ENUM: return enum_type_;
    case TYPE_ARRAY:
    case TYPE_STRUCT:
      return list_ptr_->type();
    case TYPE_PROTO:
      return proto_ptr_->type();
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      LOG(DFATAL) << "Invalid type: " << type_kind();
      break;
  }
  return nullptr;
}

const std::vector<Value>& Value::fields() const {
  CHECK_EQ(TYPE_STRUCT, type_kind_);
  CHECK(!is_null()) << "Null value";
  return list_ptr_->values();
}

const std::vector<Value>& Value::elements() const {
  CHECK_EQ(TYPE_ARRAY, type_kind_);
  CHECK(!is_null()) << "Null value";
  return list_ptr_->values();
}

Value Value::TimestampFromUnixMicros(int64_t v) {
  CHECK(functions::IsValidTimestamp(v, functions::kMicroseconds)) << v;
  return Value(absl::FromUnixMicros(v));
}

Value Value::TimeFromPacked64Micros(int64_t v) {
  TimeValue time = TimeValue::FromPacked64Micros(v);
  CHECK(time.IsValid()) << "int64 " << v
                        << " decodes to an invalid time value: "
                        << time.DebugString();
  return Value(time);
}

Value Value::DatetimeFromPacked64Micros(int64_t v) {
  DatetimeValue datetime = DatetimeValue::FromPacked64Micros(v);
  CHECK(datetime.IsValid())
      << "int64 " << v
      << " decodes to an invalid datetime value: " << datetime.DebugString();
  return Value(datetime);
}

const std::string& Value::enum_name() const {
  CHECK_EQ(TYPE_ENUM, type_kind_) << "Not an enum value";
  CHECK(!is_null()) << "Null value";
  const std::string* enum_name = nullptr;
  CHECK(type()->AsEnum()->FindName(enum_value_, &enum_name))
      << "Value " << enum_value_ << " not in "
      << type()->AsEnum()->enum_descriptor()->DebugString();
  return *enum_name;
}

int64_t Value::ToInt64() const {
  CHECK(!is_null()) << "Null value";
  switch (type_kind_) {
    case TYPE_INT64: return int64_value_;
    case TYPE_INT32: return int32_value_;
    case TYPE_UINT32: return uint32_value_;
    case TYPE_BOOL: return bool_value_;
    case TYPE_DATE: return int32_value_;
    case TYPE_TIMESTAMP:
      return ToUnixMicros();
    case TYPE_ENUM:
      return enum_value_;
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
    case TYPE_TIME:
    case TYPE_DATETIME:
    default:
      LOG(FATAL) << "Cannot coerce " << TypeKind_Name(type_kind())
                 << " to int64";
  }
}

uint64_t Value::ToUint64() const {
  CHECK(!is_null()) << "Null value";
  switch (type_kind_) {
    case TYPE_UINT64: return uint64_value_;
    case TYPE_UINT32: return uint32_value_;
    case TYPE_BOOL: return bool_value_;
    default:
      LOG(FATAL) << "Cannot coerce to uint64";
      return 0;
  }
}

double Value::ToDouble() const {
  CHECK(!is_null()) << "Null value";
  switch (type_kind_) {
    case TYPE_BOOL: return bool_value_;
    case TYPE_DATE: return int32_value_;
    case TYPE_DOUBLE: return double_value_;
    case TYPE_FLOAT: return float_value_;
    case TYPE_INT32: return int32_value_;
    case TYPE_UINT32: return uint32_value_;
    case TYPE_UINT64: return uint64_value_;
    case TYPE_INT64:
      return int64_value_;
    case TYPE_NUMERIC:
      return numeric_value().ToDouble();
    case TYPE_ENUM:
      return enum_value_;
    case TYPE_TIMESTAMP:
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
    case TYPE_TIME:
    case TYPE_DATETIME:
    default:
      LOG(FATAL) << "Cannot coerce to double";
  }
}

uint64_t Value::physical_byte_size() const {
  if (is_null()) {
    return sizeof(Value);
  }
  uint64_t physical_size = sizeof(Value);
  switch (type_kind_) {
    case TYPE_BOOL:
    case TYPE_DATE:
    case TYPE_DOUBLE:
    case TYPE_FLOAT:
    case TYPE_INT32:
    case TYPE_UINT32:
    case TYPE_UINT64:
    case TYPE_INT64:
    case TYPE_TIME:
    case TYPE_DATETIME:
    case TYPE_ENUM:
    case TYPE_TIMESTAMP:
      break;
    case TYPE_NUMERIC:
      physical_size += sizeof(NumericRef);
      break;
    case TYPE_STRING:
    case TYPE_BYTES:
      physical_size += string_ptr_->physical_byte_size();
      break;
    case TYPE_ARRAY:
    case TYPE_STRUCT:
      physical_size += list_ptr_->physical_byte_size();
      break;
    case TYPE_PROTO:
      physical_size += proto_ptr_->physical_byte_size();
      break;
    case TYPE_GEOGRAPHY:
      physical_size += geography_ptr_->physical_byte_size();
      break;
    default:
      LOG(FATAL) << "Cannot determine physical byte size: " << type_kind_;
  }
  return physical_size;
}

std::string Value::ToCord() const {
  CHECK(!is_null()) << "Null value";
  switch (type_kind_) {
    case TYPE_STRING:
    case TYPE_BYTES:
      return string_ptr_->value();
    case TYPE_PROTO:
      return proto_ptr_->value();
    default:
      LOG(FATAL) << "Cannot coerce to Cord";
      return "";
  }
}

absl::Time Value::ToTime() const {
  CHECK(!is_null()) << "Null value";
  CHECK_EQ(TYPE_TIMESTAMP, type_kind_) << "Not a timestamp value";
  return absl::FromUnixSeconds(timestamp_seconds_) +
         absl::Nanoseconds(subsecond_nanos_);
}

int64_t Value::ToUnixMicros() const { return absl::ToUnixMicros(ToTime()); }

int64_t Value::ToPacked64TimeMicros() const {
  return (static_cast<int64_t>(bit_field_32_value_) << kMicrosShift) |
         (subsecond_nanos_ / 1000);
}

int64_t Value::ToPacked64DatetimeMicros() const {
  return (bit_field_64_value_ << kMicrosShift) | (subsecond_nanos_ / 1000);
}

zetasql_base::Status Value::ToUnixNanos(int64_t* nanos) const {
  if (functions::FromTime(ToTime(), functions::kNanoseconds, nanos)) {
    return ::zetasql_base::OkStatus();
  }
  return zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                      absl::StrCat("Timestamp value in Unix epoch nanoseconds "
                                   "exceeds 64 bit: ",
                                   DebugString()));
}

google::protobuf::Message* Value::ToMessage(
    google::protobuf::DynamicMessageFactory* message_factory,
    bool return_null_on_error) const {
  CHECK(type()->IsProto());
  CHECK(!is_null());
  std::unique_ptr<google::protobuf::Message> m(
      message_factory->GetPrototype(type()->AsProto()->descriptor())->New());
  const bool success = m->ParsePartialFromString(ToCord());
  if (!success && return_null_on_error) return nullptr;
  return m.release();
}

const Value& Value::FindFieldByName(absl::string_view name) const {
  CHECK(type()->IsStruct());
  CHECK(!is_null()) << "Null value";
  if (!name.empty()) {
    // Find field position.
    for (int i = 0; i < type()->AsStruct()->num_fields(); i++) {
      if (type()->AsStruct()->field(i).name == name) {
        return field(i);
      }
    }
  }
  static Value invalid_value;
  return invalid_value;
}

// Always returns false.
static bool TypesDiffer(const Value& x, const Value& y, std::string* reason) {
  if (reason) {
    absl::StrAppend(
        reason,
        absl::Substitute("Types differ: {$0} vs. {$1} respectively of "
                         "values {$2} and {$3}\n",
                         x.type()->DebugString(), y.type()->DebugString(),
                         x.DebugString(), y.DebugString()));
  }
  return false;
}

void Value::DeepOrderKindSpec::FillSpec(const Value& v) {
  if (v.is_null()) {
    return;
  }
  switch (v.type_kind()) {
    case TYPE_ARRAY:
      if (v.order_kind() == kIgnoresOrder) {
        ignores_order = true;
      }
      if (children.empty()) {
        children.resize(1);
      }
      for (int i = 0; i < v.num_elements(); i++) {
        children[0].FillSpec(v.element(i));
      }
      break;
    case TYPE_STRUCT:
      if (children.empty()) {
        children.resize(v.num_fields());
      }
      DCHECK_EQ(children.size(), v.num_fields());
      for (int i = 0; i < v.num_fields(); i++) {
        children[i].FillSpec(v.field(i));
      }
      break;
    default:
      return;
  }
}

// x is the expected value whose orderedness is taken into account when
// allow_bags = true.
bool Value::EqualsInternal(const Value& x, const Value& y, bool allow_bags,
                           DeepOrderKindSpec* deep_order_spec,
                           FloatMargin float_margin, std::string* reason) {
  if (!x.is_valid()) { return !y.is_valid(); }
  if (!y.is_valid()) { return false; }

  if (x.type_kind() != y.type_kind()) return TypesDiffer(x, y, reason);
  if ((x.type_kind() == TYPE_ENUM || x.type_kind() == TYPE_ARRAY ||
       x.type_kind() == TYPE_PROTO) &&
      !x.type()->Equivalent(y.type())) {
    return TypesDiffer(x, y, reason);
  }
  if (x.is_null() != y.is_null()) return false;
  // TODO: We need to check TYPE_STRUCT values, that they have the same
  // number and comparable types of fields. This requires modifying
  // Value::num_fields() to not call Value::fields().
  if (x.is_null() && y.is_null()) return true;

  std::unique_ptr<DeepOrderKindSpec> owned_deep_order_spec;
  if (allow_bags && deep_order_spec == nullptr) {
    owned_deep_order_spec = absl::make_unique<DeepOrderKindSpec>();
    deep_order_spec = owned_deep_order_spec.get();
    if (!x.type()->Equivalent(y.type())) {
      return false;
    }
    deep_order_spec->FillSpec(x);
    deep_order_spec->FillSpec(y);
  }

  switch (x.type_kind()) {
    case TYPE_INT32: return x.int32_value() == y.int32_value();
    case TYPE_INT64: return x.int64_value() == y.int64_value();
    case TYPE_UINT32: return x.uint32_value() == y.uint32_value();
    case TYPE_UINT64: return x.uint64_value() == y.uint64_value();
    case TYPE_BOOL: return x.bool_value() == y.bool_value();
    case TYPE_FLOAT:
      return float_margin.Equal(x.float_value(), y.float_value());
    case TYPE_DOUBLE:
      return float_margin.Equal(x.double_value(), y.double_value());
    case TYPE_STRING: return x.string_value() == y.string_value();
    case TYPE_BYTES: return x.bytes_value() == y.bytes_value();
    case TYPE_DATE: return x.date_value() == y.date_value();
    case TYPE_TIMESTAMP:
      return x.timestamp_seconds_ == y.timestamp_seconds_ &&
             x.subsecond_nanos_ == y.subsecond_nanos_;
    case TYPE_TIME:
      return x.bit_field_32_value_ == y.bit_field_32_value_ &&
             x.subsecond_nanos_ == y.subsecond_nanos_;
    case TYPE_DATETIME:
      return x.bit_field_64_value_ == y.bit_field_64_value_ &&
             x.subsecond_nanos_ == y.subsecond_nanos_;
    case TYPE_NUMERIC:
      return x.numeric_value() == y.numeric_value();
    case TYPE_ENUM:
      return x.enum_value() == y.enum_value();
    case TYPE_ARRAY: {
      if (x.num_elements() != y.num_elements()) {
        if (reason) {
          absl::StrAppend(
              reason,
              Substitute(
                  "Number of array elements is {$0} and {$1} in respective "
                  "arrays {$2} and {$3}\n",
                  x.num_elements(), y.num_elements(), x.DebugString(),
                  y.DebugString()));
        }
        return false;
      }
      auto element_order_spec =
          allow_bags ? &deep_order_spec->children[0] : nullptr;
      if (allow_bags && deep_order_spec->ignores_order) {
        return EqualElementMultiSet(x, y, element_order_spec, float_margin,
                                    reason);
      }
      for (int i = 0; i < x.num_elements(); i++) {
        if (!EqualsInternal(x.element(i), y.element(i), allow_bags,
                            element_order_spec, float_margin, reason)) {
          return false;
        }
      }
      return true;
    }
    case TYPE_STRUCT:
      // Structs are considered equal when they have the same number of fields
      // and the field values are equal. Types of x and y may differ.
      if (x.num_fields() != y.num_fields()) {
        if (reason) {
          absl::StrAppend(
              reason,
              Substitute(
                  "Number of struct fields is {$0} and {$1} in respective "
                  "structs {$2} and {$3}\n",
                  x.num_fields(), y.num_fields(), x.DebugString(),
                  y.DebugString()));
        }
        return false;
      }
      for (int i = 0; i < x.num_fields(); i++) {
        auto field_order_spec =
            allow_bags ? &deep_order_spec->children[i] : nullptr;
        if (!EqualsInternal(x.field(i), y.field(i), allow_bags,
                            field_order_spec, float_margin, reason)) {
          return false;
        }
      }
      return true;
    case TYPE_PROTO: {
      // Shortcut fast case.
      if (x.ToCord() == y.ToCord()) return true;

      // We use the descriptor from x.  The implementation of Type equality
      // currently means the descriptors must be identical.  If we relax that,
      // it is possible this comparison would be assymetric, but only in
      // unusual cases where a message field is unknown on one side but not
      // the other, and doesn't compare identically as bytes.
      google::protobuf::DynamicMessageFactory factory;
      const google::protobuf::Message* prototype = factory.GetPrototype(
          x.type()->AsProto()->descriptor());

      std::unique_ptr<google::protobuf::Message> x_msg(prototype->New());
      std::unique_ptr<google::protobuf::Message> y_msg(prototype->New());
      if (!x_msg->ParsePartialFromString(x.ToCord()) ||
          !y_msg->ParsePartialFromString(y.ToCord())) {
        return false;
      }

      // This does exact comparison of doubles.  It is possible to customize it
      // using set_float_comparison(MessageDifferencer::APPROXIMATE), which
      // makes it use zetasql_base::MathUtil::AlmostEqual, or to set up default
      // FieldComparators for even more control of comparisons.
      // TODO We could use one of those options if
      // !float_margin.IsExactEquality().
      // HashCode would need to be updated.
      google::protobuf::util::MessageDifferencer differencer;
      std::string differencer_reason;
      if (reason != nullptr) {
        differencer.ReportDifferencesToString(&differencer_reason);
      }
      const bool result = differencer.Compare(*x_msg, *y_msg);
      if (!differencer_reason.empty()) {
        absl::StrAppend(reason, differencer_reason);
        // The newline will be added already.
        DCHECK_EQ(differencer_reason[differencer_reason.size() - 1], '\n')
            << differencer_reason;
      }
      return result;
    }
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      LOG(FATAL) << "Unexpected expected internally only: " << x.type_kind();
  }
}

struct InternalComparer {
  explicit InternalComparer(FloatMargin float_margin_arg,
                            Value::DeepOrderKindSpec* order_spec_arg)
      : float_margin(float_margin_arg), order_spec(order_spec_arg) {}
  size_t operator()(const zetasql::Value& x,
                    const zetasql::Value& y) const {
    return Value::EqualsInternal(x, y,
                                 true,  // allow_bags
                                 order_spec, float_margin,
                                 nullptr);  // reason
  }
  FloatMargin float_margin;
  Value::DeepOrderKindSpec* order_spec;
};

// Hasher used by EqualElementMultiSet in tests only.
struct InternalHasher {
  explicit InternalHasher(FloatMargin float_margin_arg)
      : float_margin(float_margin_arg) {}
  size_t operator()(const zetasql::Value& x) const {
    return x.HashCodeInternal(float_margin);
  }
  FloatMargin float_margin;
};

// Compares arrays as multisets. Used in tests only. The current algorithm,
// which counts the number of the same elements, may return false negatives if
// !float_margin.IsExactEquality(). Specifically, the method may return 'false'
// on almost-equal bags if those contain elements for which approximate equality
// is non-transitive, e.g., {a, b, c} such that a~b==true, b~c==true,
// a~c==false. See a repro in value_test.cc:AlmostEqualsStructArray.
// TODO: potential fix is to implement Hopcroft-Karp algorithm:
// http://en.wikipedia.org/wiki/Hopcroft%E2%80%93Karp_algorithm
// Its complexity is O(|E|*sqrt(|V|)). Computing E requires |V|^2 comparisons,
// so we get O(|V|^2.5).
bool Value::EqualElementMultiSet(const Value& x, const Value& y,
                                 DeepOrderKindSpec* deep_order_spec,
                                 FloatMargin float_margin, std::string* reason) {
  using ValueCountMap =
      absl::flat_hash_map<Value, int, InternalHasher, InternalComparer>;
  InternalHasher hasher(float_margin);
  InternalComparer comparer(float_margin, deep_order_spec);
  ValueCountMap x_multiset(x.num_elements(), hasher, comparer);
  ValueCountMap y_multiset(x.num_elements(), hasher, comparer);
  DCHECK_EQ(x.num_elements(), y.num_elements());
  for (int i = 0; i < x.num_elements(); i++) {
    x_multiset[x.element(i)]++;
    y_multiset[y.element(i)]++;
  }
  for (const auto& p : x_multiset) {
    const Value& element = p.first;
    auto it = y_multiset.find(element);
    if (it == y_multiset.end()) {
      if (reason) {
        absl::StrAppend(
            reason, Substitute("Multiset element $0 of $1 is missing in $2\n",
                               element.DebugString(), x.DebugString(),
                               y.DebugString()));
      }
      return false;
    }
    if (it->second != p.second) {
      if (reason) {
        absl::StrAppend(
            reason,
            Substitute(
                "Number of occurrences of multiset element $0 is $1 and $2 "
                "respectively in multisets $3 and $4\n",
                element.DebugString(), p.second, it->second, x.DebugString(),
                y.DebugString()));
      }
      return false;
    }
  }
  if (x_multiset.size() == y_multiset.size()) {
    return true;  // All of x is in y and the sizes agree.
  }
  if (reason) {
    // There exists an element in y that's missing from x. Report it.
    for (const auto& p : y_multiset) {
      const Value& element = p.first;
      if (x_multiset.find(element) == x_multiset.end()) {
        absl::StrAppend(
            reason, Substitute("Multiset element $0 of $1 is missing in $2\n",
                               element.DebugString(), y.DebugString(),
                               x.DebugString()));
      }
    }
    DCHECK(!reason->empty());
  }
  return false;
}

// Function used to switch of a pair of TypeKinds
static constexpr uint64_t TYPE_KIND_PAIR(TypeKind kind1, TypeKind kind2) {
  return (static_cast<uint64_t>(kind1) << 16) | static_cast<uint64_t>(kind2);
}

static bool TypesSupportSqlEquals(const Type* type1, const Type* type2) {
  switch (TYPE_KIND_PAIR(type1->kind(), type2->kind())) {
    case TYPE_KIND_PAIR(TYPE_INT32, TYPE_INT32):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_INT64):
    case TYPE_KIND_PAIR(TYPE_UINT32, TYPE_UINT32):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_BOOL, TYPE_BOOL):
    case TYPE_KIND_PAIR(TYPE_STRING, TYPE_STRING):
    case TYPE_KIND_PAIR(TYPE_BYTES, TYPE_BYTES):
    case TYPE_KIND_PAIR(TYPE_DATE, TYPE_DATE):
    case TYPE_KIND_PAIR(TYPE_TIMESTAMP, TYPE_TIMESTAMP):
    case TYPE_KIND_PAIR(TYPE_TIME, TYPE_TIME):
    case TYPE_KIND_PAIR(TYPE_DATETIME, TYPE_DATETIME):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
    case TYPE_KIND_PAIR(TYPE_FLOAT, TYPE_FLOAT):
    case TYPE_KIND_PAIR(TYPE_DOUBLE, TYPE_DOUBLE):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_INT64):
      return true;
    case TYPE_KIND_PAIR(TYPE_STRUCT, TYPE_STRUCT): {
      const StructType* struct_type1 = type1->AsStruct();
      const StructType* struct_type2 = type2->AsStruct();
      if (struct_type1->num_fields() != struct_type2->num_fields()) {
        return false;
      }
      for (int i = 0; i < struct_type1->num_fields(); ++i) {
        if (!TypesSupportSqlEquals(struct_type1->field(i).type,
                                   struct_type2->field(i).type)) {
          return false;
        }
      }
      return true;
    }
    case TYPE_KIND_PAIR(TYPE_ARRAY, TYPE_ARRAY):
      return TypesSupportSqlEquals(type1->AsArray()->element_type(),
                                   type2->AsArray()->element_type());
    default:
      return false;
  }
}

// This method is unit tested indirectly through the reference implementation
// compliance and unit tests.
Value Value::SqlEquals(const Value& that) const {
  if (!TypesSupportSqlEquals(type(), that.type())) return Value();

  if (is_null() || that.is_null()) return values::NullBool();

  switch (TYPE_KIND_PAIR(type_kind(), that.type_kind())) {
    case TYPE_KIND_PAIR(TYPE_INT32, TYPE_INT32):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_INT64):
    case TYPE_KIND_PAIR(TYPE_UINT32, TYPE_UINT32):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_BOOL, TYPE_BOOL):
    case TYPE_KIND_PAIR(TYPE_STRING, TYPE_STRING):
    case TYPE_KIND_PAIR(TYPE_BYTES, TYPE_BYTES):
    case TYPE_KIND_PAIR(TYPE_DATE, TYPE_DATE):
    case TYPE_KIND_PAIR(TYPE_TIMESTAMP, TYPE_TIMESTAMP):
    case TYPE_KIND_PAIR(TYPE_TIME, TYPE_TIME):
    case TYPE_KIND_PAIR(TYPE_DATETIME, TYPE_DATETIME):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
      return Value::Bool(Equals(that));

    case TYPE_KIND_PAIR(TYPE_STRUCT, TYPE_STRUCT): {
      if (num_fields() != that.num_fields()) {
        return values::False();
      }
      bool saw_null_field_comparison = false;
      for (int i = 0; i < num_fields(); ++i) {
        const Value result = field(i).SqlEquals(that.field(i));
        if (!result.is_valid()) {
          return Value();
        }
        if (result.is_null()) {
          // We had a field comparison that was null. Remember that. We still
          // have to continue looking at the remaining fields rather than return
          // because we might have a later field that compares false which would
          // make the entire comparison false.
          saw_null_field_comparison = true;
        } else if (!result.bool_value()) {
          return values::False();
        }
      }
      if (saw_null_field_comparison) {
        return values::NullBool();
      }
      return values::True();
    }

    case TYPE_KIND_PAIR(TYPE_FLOAT, TYPE_FLOAT):
      // false if NaN
      return Value::Bool(float_value() == that.float_value());
    case TYPE_KIND_PAIR(TYPE_DOUBLE, TYPE_DOUBLE):
      // false if NaN
      return Value::Bool(double_value() == that.double_value());
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_UINT64):
      return Value::Bool(
          functions::Compare64(int64_value(), that.uint64_value()) == 0);
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_INT64):
      return Value::Bool(
          functions::Compare64(that.int64_value(), uint64_value()) == 0);
    case TYPE_KIND_PAIR(TYPE_ARRAY, TYPE_ARRAY): {
      if (num_elements() != that.num_elements()) {
        return values::False();
      }
      bool saw_null_element_comparison = false;
      for (int i = 0; i < num_elements(); ++i) {
        Value result = element(i).SqlEquals(that.element(i));
        if (result.is_null()) {
          // Keeps track of whether there was null in comparison but do not
          // return early as there might be a later element that compares false
          // which would make the entire comparison false.
          saw_null_element_comparison = true;
        } else if (!result.bool_value()) {
          return values::False();
        }
      }
      if (saw_null_element_comparison) {
        return Value::NullBool();
      }
      return values::True();
    }
    default:
      return Value();
  }
}

size_t Value::HashCode() const { return absl::Hash<Value>()(*this); }

// A dummy struct to allow an alternative hashing scheme within the
// absl hashing framework.  This overrides all double and float hashes
// to be constants, and overrides recursive hashes to use this hasher.
// This is used for tests which want to be more lenient (i.e. use a float
// margin) on float equality.
// Note, this is not guaranteed to produce identical hash-values for float-less
// Value objects.
struct ValueHasherIgnoringFloat {
  const Value& v;

  template <typename H>
  friend H AbslHashValue(H h, const ValueHasherIgnoringFloat& v) {
    return HashInternal(std::move(h), v.v);
  }

  template <typename H>
  static H HashInternal(H h, const Value& v) {
    static constexpr uint64_t kFloatApproximateHashCode = 0x1192AA60660CCFABull;
    static constexpr uint64_t kDoubleApproximateHashCode = 0x520C31647E82D8E6ull;
    if (!v.is_valid() || v.is_null()) {
      // Check this first as type_kind() will crash in this case.
      return AbslHashValue(std::move(h), v);
    }
    switch (v.type_kind()) {
      case TYPE_FLOAT:
        return H::combine(std::move(h), kFloatApproximateHashCode);
      case TYPE_DOUBLE:
        return H::combine(std::move(h), kDoubleApproximateHashCode);
      case TYPE_ARRAY: {
        // We must hash arrays as if unordered to support hash_map and hash_set
        // of values containing arrays with order_kind()=kIgnoresOrder.
        // absl::Hash lacks support for unordered containers, so we create a
        // cheapo solution of just adding the hashcodes.
        absl::Hash<ValueHasherIgnoringFloat> element_hasher;
        size_t combined_hash = 1;
        for (int i = 0; i < v.num_elements(); i++) {
          combined_hash +=
              element_hasher(ValueHasherIgnoringFloat{v.element(i)});
        }
        return H::combine(std::move(h), TYPE_ARRAY, combined_hash);
      }
      case TYPE_STRUCT: {
        h = H::combine(std::move(h), TYPE_STRUCT);
        for (int i = 0; i < v.num_fields(); i++) {
          h = HashInternal(std::move(h), v.field(i));
        }
        return h;
      }
      default:
        return AbslHashValue(std::move(h), v);
    }
  }
};

size_t Value::HashCodeInternal(FloatMargin float_margin) const {
  if (float_margin.IsExactEquality()) {
    return HashCode();
  } else {
    // If using inexactly equality, just have all floats/doubles hash to a
    // constant, and let equality deal with float_margin.
    return absl::Hash<ValueHasherIgnoringFloat>()(
        ValueHasherIgnoringFloat{*this});
  }
}

bool Value::LessThan(const Value& that) const {
  if (type_kind_ == that.type_kind_) {
    // Note that because we don't check type for nulls, this means we may return
    // true when the type of 'this' and 'that' is different and one of them is
    // null. E.g. when comparing two enums, if 'this' is null and 'that' is not
    // null, we return true even though they may be incompatible.
    if (is_null() && !that.is_null()) return true;
    if (that.is_null()) return false;
    switch (type_kind()) {
      case TYPE_INT32: return int32_value() < that.int32_value();
      case TYPE_INT64: return int64_value() < that.int64_value();
      case TYPE_UINT32: return uint32_value() < that.uint32_value();
      case TYPE_UINT64: return uint64_value() < that.uint64_value();
      case TYPE_BOOL: return bool_value() < that.bool_value();
      case TYPE_FLOAT:
        if (std::isnan(float_value()) && !std::isnan(that.float_value())) {
          return true;
        }
        if (std::isnan(that.float_value())) {
          return false;
        }
        return float_value() < that.float_value();
      case TYPE_DOUBLE:
        if (std::isnan(double_value()) && !std::isnan(that.double_value())) {
          return true;
        }
        if (std::isnan(that.double_value())) {
          return false;
        }
        return double_value() < that.double_value();
      case TYPE_STRING: return string_value() < that.string_value();
      case TYPE_BYTES: return bytes_value() < that.bytes_value();
      case TYPE_DATE: return date_value() < that.date_value();
      case TYPE_TIMESTAMP:
        return ToTime() < that.ToTime();
      case TYPE_TIME:
        return bit_field_32_value_ < that.bit_field_32_value_||
               (bit_field_32_value_ == that.bit_field_32_value_ &&
                subsecond_nanos_ < that.subsecond_nanos_);
      case TYPE_DATETIME:
        return bit_field_64_value_ < that.bit_field_64_value_ ||
               (bit_field_64_value_ == that.bit_field_64_value_ &&
                subsecond_nanos_ < that.subsecond_nanos_);
      case TYPE_NUMERIC:
        return numeric_value() < that.numeric_value();
      case TYPE_ENUM:
        // TODO: change this to return false for consistency and to
        // avoid crashes.
        CHECK(type()->Equivalent(that.type()))
            << "Cannot compare enum of type "
            << type()->DebugString() << " and " << that.type()->DebugString();
        return enum_value() < that.enum_value();
      case TYPE_STRUCT:
        if (num_fields() != that.num_fields()) return false;
        // Because we return true as soon as 'LessThan' returns true for a
        // field (without checking types of all fields), we may return true for
        // incompatible types. This behavior is OK for now because we consider
        // LessThan for incompatible types is undefined.
        for (int i = 0; i < num_fields(); i++) {
          if (field(i).LessThan(that.field(i))) {
            return true;
          } else if (that.field(i).LessThan(field(i))) {
            return false;
          }
        }
        return false;
      case TYPE_ARRAY:
        for (int i = 0; i < std::min(num_elements(), that.num_elements());
             ++i) {
          if (element(i).LessThan(that.element(i))) return true;
          if (that.element(i).LessThan(element(i))) return false;
        }
        return num_elements() < that.num_elements();
      case TYPE_GEOGRAPHY:
      case TYPE_PROTO:
      case TYPE_UNKNOWN:
      case __TypeKind__switch_must_have_a_default__:
        LOG(FATAL) << "Cannot compare " << type()->DebugString()
                   << " to " << that.type()->DebugString();
    }
  }
  return false;
}

static bool TypesSupportSqlLessThan(const Type* type1, const Type* type2) {
  switch (TYPE_KIND_PAIR(type1->kind(), type2->kind())) {
    case TYPE_KIND_PAIR(TYPE_INT32, TYPE_INT32):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_INT64):
    case TYPE_KIND_PAIR(TYPE_UINT32, TYPE_UINT32):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_BOOL, TYPE_BOOL):
    case TYPE_KIND_PAIR(TYPE_STRING, TYPE_STRING):
    case TYPE_KIND_PAIR(TYPE_BYTES, TYPE_BYTES):
    case TYPE_KIND_PAIR(TYPE_DATE, TYPE_DATE):
    case TYPE_KIND_PAIR(TYPE_TIMESTAMP, TYPE_TIMESTAMP):
    case TYPE_KIND_PAIR(TYPE_TIME, TYPE_TIME):
    case TYPE_KIND_PAIR(TYPE_DATETIME, TYPE_DATETIME):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
    case TYPE_KIND_PAIR(TYPE_FLOAT, TYPE_FLOAT):
    case TYPE_KIND_PAIR(TYPE_DOUBLE, TYPE_DOUBLE):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_INT64):
      return true;
    case TYPE_KIND_PAIR(TYPE_ARRAY, TYPE_ARRAY):
      return TypesSupportSqlLessThan(type1->AsArray()->element_type(),
                                     type2->AsArray()->element_type());
    default:
      return false;
  }
}

// This method is unit tested indirectly through the reference implementation
// compliance and unit tests.
Value Value::SqlLessThan(const Value& that) const {
  if (!TypesSupportSqlLessThan(type(), that.type())) return Value();

  if (is_null() || that.is_null()) return values::NullBool();

  switch (TYPE_KIND_PAIR(type_kind(), that.type_kind())) {
    case TYPE_KIND_PAIR(TYPE_INT32, TYPE_INT32):
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_INT64):
    case TYPE_KIND_PAIR(TYPE_UINT32, TYPE_UINT32):
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_UINT64):
    case TYPE_KIND_PAIR(TYPE_BOOL, TYPE_BOOL):
    case TYPE_KIND_PAIR(TYPE_STRING, TYPE_STRING):
    case TYPE_KIND_PAIR(TYPE_BYTES, TYPE_BYTES):
    case TYPE_KIND_PAIR(TYPE_DATE, TYPE_DATE):
    case TYPE_KIND_PAIR(TYPE_TIMESTAMP, TYPE_TIMESTAMP):
    case TYPE_KIND_PAIR(TYPE_TIME, TYPE_TIME):
    case TYPE_KIND_PAIR(TYPE_DATETIME, TYPE_DATETIME):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
      return Value::Bool(LessThan(that));
    case TYPE_KIND_PAIR(TYPE_FLOAT, TYPE_FLOAT):
      return Value::Bool(float_value() < that.float_value());  // false if NaN
    case TYPE_KIND_PAIR(TYPE_DOUBLE, TYPE_DOUBLE):
      return Value::Bool(double_value() < that.double_value());  // false if NaN
    case TYPE_KIND_PAIR(TYPE_INT64, TYPE_UINT64):
      return Value::Bool(
          functions::Compare64(int64_value(), that.uint64_value()) < 0);
    case TYPE_KIND_PAIR(TYPE_UINT64, TYPE_INT64):
      return Value::Bool(
          functions::Compare64(that.int64_value(), uint64_value()) > 0);

    case TYPE_KIND_PAIR(TYPE_ARRAY, TYPE_ARRAY): {
      const int shorter_array_size =
          std::min(num_elements(), that.num_elements());
      // Compare array elements one by one. If we find that the first array is
      // less or greater than the second, then ignore the remaining elements and
      // return the result. If we find a NULL element, then the comparison
      // results in NULL.
      for (int i = 0; i < shorter_array_size; ++i) {
        // Evaluate if the element of the first array is less than the element
        // of the second array.
        const Value first_result = element(i).SqlLessThan(that.element(i));
        if (first_result.is_null()) {
          // If the comparison returned NULL, then return NULL.
          return Value::NullBool();
        }
        if (first_result.bool_value()) {
          return values::True();
        }

        // Evaluate if the element of the second array is less than the element
        // of the first array.
        const Value second_result = that.element(i).SqlLessThan(element(i));
        if (second_result.is_null()) {
          // If the comparison returned NULL, then return NULL. This shouldn't
          // happen since 'first_result' was not NULL, but we check anyway just
          // to be safe.
          return Value::NullBool();
        }
        if (second_result.bool_value()) {
          return values::False();
        }

        // Otherwise the array elements are not less and not greater, but may
        // not be 'equal' (e.g., if one of the elements is NaN, which always
        // compares as false).
        const Value equals_result = element(i).SqlEquals(that.element(i));
        if (equals_result.is_null()) {
          // This shouldn't happen since 'first_result' was not NULL, but we
          // check anyway just to be safe.
          return Value::NullBool();
        }
        if (!equals_result.bool_value()) {
          return values::False();
        }
      }

      // If we got here, then the first <shorter_array_size> elements are all
      // equal. So if the left array is shorter than the right array then it is
      // less.
      return Value::Bool(num_elements() < that.num_elements());
    }
    default:
      return Value();
  }
}

static std::string CapitalizedNameForType(const Type* type) {
  switch (type->kind()) {
    case TYPE_INT32:
      return "Int32";
    case TYPE_INT64:
      return "Int64";
    case TYPE_UINT32:
      return "Uint32";
    case TYPE_UINT64:
      return "Uint64";
    case TYPE_BOOL:
      return "Bool";
    case TYPE_FLOAT:
      return "Float";
    case TYPE_DOUBLE:
      return "Double";
    case TYPE_STRING:
      return "String";
    case TYPE_BYTES:
      return "Bytes";
    case TYPE_DATE:
      return "Date";
    case TYPE_TIMESTAMP:
      return "Timestamp";
    case TYPE_TIME:
      return "Time";
    case TYPE_DATETIME:
      return "Datetime";
    case TYPE_GEOGRAPHY:
      return "Geography";
    case TYPE_NUMERIC:
      return "Numeric";
    case TYPE_ENUM:
      return absl::StrCat("Enum<",
                          type->AsEnum()->enum_descriptor()->full_name(), ">");
    case TYPE_ARRAY:
      return absl::StrCat(
          "Array<",
          static_cast<const ArrayType*>(type)->element_type()->DebugString(),
          ">");
    case TYPE_STRUCT:
      return "Struct";
    case TYPE_PROTO:
      CHECK(type->AsProto()->descriptor() != nullptr);
      return absl::StrCat("Proto<", type->AsProto()->descriptor()->full_name(),
                          ">");
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      LOG(FATAL) << "Unexpected type kind expected internally only: "
                 << type->kind();
  }
}

std::string Value::DebugString(bool verbose) const {
  std::map<const Value*, std::string> debug_string_map;
  std::vector<const Value*> inner_values;
  std::stack<const Value*> stack;
  stack.push(this);

  // First pass - get an ordered list of inner values to get debug strings of.
  while (!stack.empty()) {
    const Value* value = stack.top();
    stack.pop();
    inner_values.push_back(value);
    if (value->type_kind_ != kInvalidTypeKind && !value->is_null()) {
      switch (value->type_kind()) {
        case TYPE_STRUCT: {
          const StructType* struct_type = value->type()->AsStruct();
          for (int i = 0; i < struct_type->num_fields(); i++) {
            stack.push(&value->fields()[i]);
          }
        } break;
        case TYPE_ARRAY:
          for (const auto& elem : value->elements()) {
            stack.push(&elem);
          }
          break;
        default:
          break;
      }
    }
  }

  // Now, traverse the list of inner values in reverse order so that parents
  // don't get processed until all children are processed.  Calculate the debug
  // std::string, storing the result on the map, using the map instead of a recursive
  // call to retrieve child values.
  for (int i = static_cast<int>(inner_values.size()) - 1; i >= 0; --i) {
    debug_string_map[inner_values[i]] =
        inner_values[i]->DebugStringInternal(verbose, debug_string_map);
  }

  return debug_string_map[inner_values[0]];
}

std::string Value::DebugStringInternal(
    bool verbose,
    const std::map<const Value*, std::string>& debug_string_map) const {
  if (type_kind_ == kInvalidTypeKind) { return "Uninitialized value"; }
  if (!is_valid())
    return absl::StrCat("Invalid value, type_kind: ", type_kind_);
  // Note: This method previously had problems with large stack size because
  // of recursion for structs and arrays.  Using StrCat/StrAppend in particular
  // adds large stack size per argument for the AlphaNum object.
  std::string s;
  if (is_null()) {
    s = "NULL";
  } else {
    switch (type_kind()) {
      case TYPE_INT32:
        s = absl::StrCat(int32_value());
        break;
      case TYPE_INT64:
        s = absl::StrCat(int64_value());
        break;
      case TYPE_UINT32:
        s = absl::StrCat(uint32_value());
        break;
      case TYPE_UINT64:
        s = absl::StrCat(uint64_value());
        break;
      case TYPE_BOOL:
        s = (bool_value() ? "true" : "false");
        break;
      case TYPE_FLOAT:
        s = RoundTripFloatToString(float_value());
        break;
      case TYPE_DOUBLE:
        // TODO I would like to change this so it returns "1.0" rather
        // than "1", like GetSQL(), but that affects a lot of client code.
        s = RoundTripDoubleToString(double_value());
        break;
      case TYPE_STRING:
        s = ToStringLiteral(string_value());
        break;
      case TYPE_BYTES:
        s = ToBytesLiteral(bytes_value());
        break;
      case TYPE_DATE:
        // Failure cannot actually happen in this context since date_value()
        // is guaranteed to be valid.
        ZETASQL_CHECK_OK(functions::ConvertDateToString(date_value(), &s));
        break;
      case TYPE_TIMESTAMP:
        // Failure cannot actually happen in this context since the value
        // is guaranteed to be valid.
        ZETASQL_CHECK_OK(functions::ConvertTimestampToString(
            ToTime(), functions::kNanoseconds, "+0" /* timezone */, &s));
        break;
      case TYPE_TIME:
        s = time_value().DebugString();
        break;
      case TYPE_DATETIME:
        s = datetime_value().DebugString();
        break;
      case TYPE_NUMERIC:
        s = numeric_value().ToString();
        break;
      case TYPE_ENUM:
        if (verbose) {
          s = absl::StrCat(enum_name(), ":", enum_value());
        } else {
          s = enum_name();
        }
        break;
      case TYPE_ARRAY: {
        // We don't do a stack check for arrays because we can't have recursion
        // on arrays without also having structs, and we'll catch the stack
        // overflow on the struct.
        std::vector<std::string> elems;
        for (const auto& t : elements()) {
          elems.push_back(debug_string_map.at(&t));
        }
        std::string order_str = order_kind() == kIgnoresOrder ? "unordered: " : "";
        s = absl::StrCat("[", order_str, absl::StrJoin(elems, ", "), "]");
        break;
      }
      case TYPE_STRUCT: {
        std::vector<std::string> fstr;
          const StructType* struct_type = type()->AsStruct();
          for (int i = 0; i < struct_type->num_fields(); i++) {
            const std::string field_value = debug_string_map.at(&fields()[i]);
            if (struct_type->field(i).name.empty()) {
              fstr.push_back(field_value);
            } else {
              fstr.push_back(
                  absl::StrCat(struct_type->field(i).name, ":", field_value));
            }
          }
        s = absl::StrCat("{", absl::StrJoin(fstr, ", "), "}");
        break;
      }
      case TYPE_PROTO: {
        CHECK(type()->AsProto()->descriptor() != nullptr);
        google::protobuf::DynamicMessageFactory message_factory;
        std::unique_ptr<google::protobuf::Message> m(
            this->ToMessage(&message_factory,
                            /*return_null_on_error=*/true));
        if (m == nullptr) {
          s = "{<unparseable>}";
        } else {
          s = absl::StrCat(
              "{", verbose ? m->DebugString() : m->ShortDebugString(), "}");
        }
        break;
      }
      case TYPE_UNKNOWN:
      case __TypeKind__switch_must_have_a_default__:
        LOG(FATAL) << "Unexpected type kind expected internally only: "
                   << type_kind();
    }
  }
  if (ABSL_PREDICT_FALSE(verbose)) {
    if (!is_null() && type_kind() == TYPE_ARRAY) {
      // We exclude the full type name for non-NULL arrays.
      // Note that this also means we don't see a type for non-empty arrays.
      s = absl::StrCat("Array", s);
    } else if (!is_null() &&
               (type_kind() == TYPE_STRUCT || type_kind() == TYPE_PROTO)) {
      // For structs and protos, we don't want to add extra parentheses.
      s = absl::StrCat(CapitalizedNameForType(type()), s);
    } else {
      // In the normal case, we make "<type>(<value>)".
      s = absl::StrCat(CapitalizedNameForType(type()), "(", s, ")");
    }
  }
  return s;
}

// Format will wrap arrays and structs.
std::string Value::Format() const {
  return FormatInternal(0, true /* force type */);
}

// NOTE: There is a similar method in ../resolved_ast/sql_builder.cc.
//
// This is also basically the same as GetSQLLiteral below, except this adds
// CASTs and explicit type names so the exact value comes back out.
std::string Value::GetSQL(ProductMode mode) const {
  const Type* type = this->type();

  if (is_null()) {
    return absl::StrCat("CAST(NULL AS ", type->TypeName(mode), ")");
  }

  if (type->IsTimestamp() ||
      type->IsCivilDateOrTimeType()) {
    // Use DATE, DATETIME, TIME and TIMESTAMP literal syntax.
    return absl::StrCat(type->TypeName(mode), " ",
                        ToStringLiteral(DebugString()));
  }
  if (type->IsSimpleType()) {
    // Floats and doubles like "inf" and "nan" need to be quoted.
    if (type->IsFloat() && !std::isfinite(float_value())) {
      return absl::StrCat(
          "CAST(", ToStringLiteral(RoundTripFloatToString(float_value())),
          " AS ", type->TypeName(mode), ")");
    }
    if (type->IsDouble() && !std::isfinite(double_value())) {
      return absl::StrCat(
          "CAST(", ToStringLiteral(RoundTripDoubleToString(double_value())),
          " AS ", type->TypeName(mode), ")");
    }

    if (type->IsDouble()) {
      std::string s = RoundTripDoubleToString(double_value());
      // Make sure that doubles always print with a . or an 'e' so they
      // don't look like integers.
      if (s.find_first_not_of("-0123456789") == std::string::npos) {
        s.append(".0");
      }
      return s;
    }

    if (type->kind() == TYPE_NUMERIC) {
      return absl::StrCat(
          "NUMERIC ", ToStringLiteral(numeric_value().ToString()));
    }

    // We need a cast for all numeric types except int64_t and double.
    if (type->IsNumerical() && !type->IsInt64() && !type->IsDouble()) {
      return absl::StrCat("CAST(", DebugString(), " AS ", type->TypeName(mode),
                          ")");
    } else {
      return DebugString();
    }
  }

  if (type->IsEnum()) {
    return absl::StrCat("CAST(", ToStringLiteral(DebugString()), " AS ",
                        type->TypeName(mode), ")");
  }
  if (type->IsProto()) {
    return absl::StrCat("CAST(", ToBytesLiteral(std::string(ToCord())), " AS ",
                        type->TypeName(mode), ")");
  }
  if (type->IsStruct()) {
    std::vector<std::string> fields_sql;
    for (const Value& field_value : fields()) {
      fields_sql.push_back(field_value.GetSQL(mode));
    }
    DCHECK_EQ(type->AsStruct()->num_fields(), fields_sql.size());
    return absl::StrCat(type->TypeName(mode), "(",
                        absl::StrJoin(fields_sql, ", "), ")");
  }
  if (type->IsArray()) {
    std::vector<std::string> elements_sql;
    for (const Value& element : elements()) {
      elements_sql.push_back(element.GetSQL(mode));
    }
    return absl::StrCat(type->TypeName(mode), "[",
                        absl::StrJoin(elements_sql, ", "), "]");
  }

  return DebugString();
}

// This is basically the same as GetSQL() above, except this doesn't add CASTs
// or explicit type names if the literal would be valid without them.
std::string Value::GetSQLLiteral(ProductMode mode) const {
  const Type* type = this->type();

  if (is_null()) {
    return "NULL";
  }

  if (type->IsTimestamp() ||
      type->IsCivilDateOrTimeType()) {
    // Use DATE, DATETIME, TIME and TIMESTAMP literal syntax.
    return absl::StrCat(type->TypeName(mode), " ",
                        ToStringLiteral(DebugString()));
  }

  if (type->IsSimpleType()) {
    // Floats and doubles like "inf" and "nan" need to be quoted.
    if (type->IsFloat() && !std::isfinite(float_value())) {
      return absl::StrCat(
          "CAST(", ToStringLiteral(RoundTripFloatToString(float_value())),
          " AS ", type->TypeName(mode), ")");
    }
    if (type->IsDouble() && !std::isfinite(double_value())) {
      return absl::StrCat(
          "CAST(", ToStringLiteral(RoundTripDoubleToString(double_value())),
          " AS ", type->TypeName(mode), ")");
    }

    if (type->IsDouble() || type->IsFloat()) {
      std::string s = type->IsDouble() ? RoundTripDoubleToString(double_value())
                                  : RoundTripFloatToString(float_value());
      // Make sure that doubles always print with a . or an 'e' so they
      // don't look like integers.
      if (s.find_first_not_of("-0123456789") == std::string::npos) {
        s.append(".0");
      }
      return s;
    }

    if (type->kind() == TYPE_NUMERIC) {
      return absl::StrCat(
          "NUMERIC ", ToStringLiteral(numeric_value().ToString()));
    }

    return DebugString();
  }

  if (type->IsEnum()) {
    return ToStringLiteral(DebugString());
  }
  if (type->IsProto()) {
    google::protobuf::DynamicMessageFactory message_factory;
    std::unique_ptr<google::protobuf::Message> message(this->ToMessage(&message_factory));
    zetasql_base::Status status;
    std::string out;
    if (functions::ProtoToString(message.get(), &out, &status)) {
      return ToStringLiteral(std::string(out));
    } else {
      // This branch is not expected, but try to return something.
      return ToStringLiteral(message->ShortDebugString());
    }
  }

  if (type->IsStruct()) {
    if (type->AsStruct()->num_fields() == 0) {
      return "STRUCT()";
    }
    std::vector<std::string> fields_sql;
    for (const Value& field_value : fields()) {
      fields_sql.push_back(field_value.GetSQLLiteral(mode));
    }
    DCHECK_EQ(type->AsStruct()->num_fields(), fields_sql.size());
    return absl::StrCat((type->AsStruct()->num_fields() == 1 ? "STRUCT" : ""),
                        "(", absl::StrJoin(fields_sql, ", "), ")");
  }
  if (type->IsArray()) {
    std::vector<std::string> elements_sql;
    for (const Value& element : elements()) {
      elements_sql.push_back(element.GetSQLLiteral(mode));
    }
    return absl::StrCat("[", absl::StrJoin(elements_sql, ", "), "]");
  }

  return DebugString();
}

std::string RepeatString(const std::string& text, int times) {
  CHECK_GE(times, 0);
  std::string result;
  result.reserve(text.size() * times);
  for (int i = 0; i < times; ++i) {
    result.append(text);
  }
  return result;
}

// Number of columns per indentation.
const int kIndentStep = 2;
// Character used to indent.
const char* kIndentChar = " ";

// A magic number of columns that we try to fit formatted values within.
const int kWrapCols = 78;
// A maximum length for a formatted element in a single line (NONE) formatting.
// If any element exceeds this in AUTO mode it will trigger an INDENT style
// wrap.
const int kMaxSingleLineElement = 20;
// A maximum number of columns accepted for a COLUMN style indent in AUTO mode.
// Any formatting that would cause a deeper indent becomes INDENT.
const int kMaxColumnIndent = 15;

std::string Indent(int columns) { return RepeatString(kIndentChar, columns); }

// Returns the length of the longest line in a multi line formatted std::string.
size_t LongestLine(const std::string& formatted) {
  int64_t longest = 0;
  for (absl::string_view line : absl::StrSplit(formatted, '\n')) {
    int64_t line_length = line.size();
    longest = std::max(longest, line_length);
  }
  return longest;
}

// Add to the indentation of all lines other than the first.
std::string ReIndentTail(const std::string& formatted, int added_depth) {
  std::vector<std::string> lines =
      absl::StrSplit(formatted, "\n  ", absl::SkipWhitespace());
  return absl::StrJoin(lines, absl::StrCat("\n", Indent(added_depth)));
}

enum class WrapStyle {
  // [a, b, c]
  NONE,
  // [
  //   a,
  //   b,
  //   c
  // ]
  INDENT,
  // [a,
  //  b,
  //  c]
  COLUMN,
  // Formatter picks a format to fit within a column limit.
  AUTO,
};

// Finds the first instance of "$0" inside a substitution template.
static int FindSubstitutionMarker(absl::string_view block_template) {
  int marker_index = 0;
  while (marker_index < static_cast<int64_t>(block_template.size()) - 1) {
    if (block_template[marker_index] == '$') {
      // Break upon finding "$0"
      if (block_template[marker_index + 1] == '0') {
        return marker_index;
      // Skip an extra character upon seeing "$$", since this is the escape
      // sequence for a single '$'.
      } else if (block_template[marker_index + 1] == '$') {
        ++marker_index;
      }
    }
    ++marker_index;
  }
  return marker_index;
}

std::string FormatBlock(absl::string_view block_template,
                   const std::vector<std::string>& elements, const std::string& separator,
                   int block_indent_cols, WrapStyle wrap_style) {
  // The length of the template std::string preceding the substitution marker.
  // This prefix may or may not have line returns.
  int prefix_len = FindSubstitutionMarker(block_template);
  // The position of the last line-return before the substitution marker
  // or minus one.
  int last_line_start = block_template.rfind("\n", prefix_len) + 1;
  // The column at which "COLUMN" style will wrap.
  int column_wrap_len = block_indent_cols + prefix_len - last_line_start;
  // The column at which "INDENT" style will wrap.
  int indent_wrap_len = block_indent_cols + kIndentStep;

  if (wrap_style == WrapStyle::AUTO) {
    int count = elements.size();
    size_t sum_length = 0;
    size_t max_length = 0;
    bool multi_line_child = false;
    for (const std::string& elem : elements) {
      int line_return_pos = elem.find("\n");
      if (line_return_pos == std::string::npos) {
        sum_length += elem.size();
        max_length = std::max(max_length, elem.size());
      } else {
        multi_line_child = true;
        max_length = std::max(max_length, LongestLine(elem));
      }
    }
    int sep_size = separator.size() + 1;
    // Length of formatting to a single line.
    int single_line_length =
        (prefix_len - last_line_start) + sum_length + ((count - 1) * sep_size);
    if (count == 0) {
      wrap_style = WrapStyle::NONE;
    } else if (!multi_line_child && count > 1 &&
               max_length > kMaxSingleLineElement) {
      wrap_style = WrapStyle::INDENT;
    } else if (!multi_line_child && single_line_length < kWrapCols) {
      wrap_style = WrapStyle::NONE;
    } else if ((prefix_len - last_line_start) <= kMaxColumnIndent &&
               (column_wrap_len + max_length) < kWrapCols) {
      wrap_style = WrapStyle::COLUMN;
    } else {
      wrap_style = WrapStyle::INDENT;
    }
  }

  std::string pre = "";
  std::string sep = absl::StrCat(separator, " ");
  std::string post = "";
  std::vector<std::string> indented_elements;
  switch (wrap_style) {
    case WrapStyle::NONE:
    case WrapStyle::AUTO:
      break;
    case WrapStyle::INDENT:
      pre = absl::StrCat("\n", Indent(indent_wrap_len));
      sep = absl::StrCat(separator, "\n", Indent(indent_wrap_len));
      post = absl::StrCat("\n", Indent(block_indent_cols));
      break;
    case WrapStyle::COLUMN: {
      sep = absl::StrCat(",\n", Indent(column_wrap_len));
      // Multi-line elements were formatted assuming they are at
      // block_indent_cols. They are actually at column_wrap_len.  Fix.
      int additional_indent = column_wrap_len - indent_wrap_len + kIndentStep;
      for (const std::string& elem : elements) {
        indented_elements.push_back(ReIndentTail(elem, additional_indent));
      }
      break;
    }
  }
  const std::vector<std::string>& parts =
      wrap_style == WrapStyle::COLUMN ? indented_elements : elements;
  return Substitute(block_template,
                    absl::StrCat(pre, absl::StrJoin(parts, sep), post));
}

enum class ArrayElemFormat { ALL, NONE, FIRST_LEVEL_ONLY, };

const int kArrayIndent = 6;   // Length of "ARRAY<"
const int kStructIndent = 7;  // Length of "STRUCT<"

// Helps FormatInternal print value types. This is a specific format for
// types, so we choose not to add this as a generally used method on Type.
std::string FormatType(const Type* type, ArrayElemFormat elem_format,
                  int indent_cols) {
  ArrayElemFormat continue_elem_format =
      elem_format == ArrayElemFormat::FIRST_LEVEL_ONLY ? ArrayElemFormat::NONE
                                                       : elem_format;
  if (type->IsArray()) {
    std::string element_type =
        elem_format != ArrayElemFormat::NONE
            ? FormatType(type->AsArray()->element_type(), continue_elem_format,
                         indent_cols + kArrayIndent)
            : "";
    return Substitute("ARRAY<$0>", element_type);
  } else if (type->IsStruct()) {
    const StructType* struct_type = type->AsStruct();
    std::vector<std::string> fields(struct_type->num_fields());
    for (int i = 0; i < struct_type->num_fields(); ++i) {
      const StructType::StructField& field = struct_type->field(i);
      fields[i] = FormatType(field.type, continue_elem_format,
                             indent_cols + kStructIndent);
      if (!field.name.empty()) {
        fields[i] = Substitute("$0 $1", field.name, fields[i]);
      }
    }
    return FormatBlock("STRUCT<$0>", fields, ",", indent_cols, WrapStyle::AUTO);
  } else if (type->IsProto()) {
    CHECK(type->AsProto()->descriptor() != nullptr);
    return Substitute("PROTO<$0>", type->AsProto()->descriptor()->full_name());
  } else if (type->IsEnum()) {
    return Substitute("ENUM<$0>",
                      type->AsEnum()->enum_descriptor()->full_name());
  } else {
    return type->DebugString(type /* verbose */);
  }
}

std::string Value::FormatInternal(int indent, bool force_type) const {
  if (type()->IsArray()) {
    // If the array is null or empty, print the whole type because there
    // are no printed elements that provide type information of nested arrays.
    // If there are values, FormatType elides element types for nested arrays.
    // This is to keep types as readable as possible.
    ArrayElemFormat elem_style = (is_null() || elements().empty())
                                     ? ArrayElemFormat::ALL
                                     : ArrayElemFormat::FIRST_LEVEL_ONLY;
    std::string type_string = FormatType(type(), elem_style, indent);
    if (is_null()) {
      return absl::StrCat(type_string, "(NULL)");
    }
    std::vector<std::string> element_strings(elements().size());
    for (int i = 0; i < elements().size(); ++i) {
      element_strings[i] = elements()[i].FormatInternal(indent + kIndentStep,
                                                        false /* force_type */);
    }
    // Sanitize any '$' characters before creating substitution template. "$$"
    // is replaced by "$" in the output from absl::Substitute.
    std::string sanitized_type_string =
        absl::StrReplaceAll(type_string, {{"$", "$$"}});
    std::string templ = absl::StrCat(sanitized_type_string, "[$0]");
    // Force a wrap after the type if the type consumes multiple lines and
    // there is more than one element (or one element over multiple lines).
    if (type_string.find("\n") != std::string::npos &&
        (elements().size() > 1 ||
         (!elements().empty() &&
          element_strings[0].find("\n") != std::string::npos))) {
      templ = absl::StrCat(sanitized_type_string, "\n", Indent(indent), "[$0]");
    }
    return FormatBlock(templ, element_strings, ",", indent, WrapStyle::AUTO);
  } else if (type()->IsStruct()) {
    std::string type_string =
        force_type ? FormatType(type(), ArrayElemFormat::NONE, indent) : "";
    if (is_null()) {
      return force_type ? Substitute("$0(NULL)", type_string) : "NULL";
    }
    const StructType* struct_type = type()->AsStruct();
    std::vector<std::string> field_strings(struct_type->num_fields());
    for (int i = 0; i < struct_type->num_fields(); i++) {
      field_strings[i] = fields()[i].FormatInternal(indent + kIndentStep,
                                                    false /* force_type */);
    }
    // Sanitize any '$' characters before creating substitution template. "$$"
    // is replaced by "$" in the output from absl::Substitute.
    std::string templ =
        absl::StrCat(absl::StrReplaceAll(type_string, {{"$", "$$"}}), "{$0}");
    return FormatBlock(templ, field_strings, ",", indent, WrapStyle::AUTO);
  } else if (type()->IsProto()) {
    std::string type_string =
        force_type ? FormatType(type(), ArrayElemFormat::NONE, indent) : "";
    if (is_null()) {
      return force_type ? Substitute("$0(NULL)", type_string) : "NULL";
    }
    google::protobuf::DynamicMessageFactory message_factory;
    std::unique_ptr<google::protobuf::Message> m(this->ToMessage(&message_factory));
    // Split and re-wrap the proto debug std::string to achieve proper indentation.
    std::vector<std::string> field_strings =
        absl::StrSplit(m->DebugString(), '\n', absl::SkipWhitespace());
    bool wraps = field_strings.size() > 1;
    // We don't need to sanitize the type std::string here since proto field names
    // cannot contain '$' characters.
    return FormatBlock(absl::StrCat(type_string, "{$0}"), field_strings, "",
                       indent, wraps ? WrapStyle::INDENT : WrapStyle::NONE);
  } else {
    return DebugString(force_type);
  }
}

bool Value::ParseInteger(absl::string_view input, Value* value) {
  int64_t int64_value;
  uint64_t uint64_value;
  if (functions::StringToNumeric(input, &int64_value, nullptr)) {
    *value = Value::Int64(int64_value);
    return true;
  }
  // Could not parse into int64_t, try uint64_t.
  if (functions::StringToNumeric(input, &uint64_value, nullptr)) {
    *value = Value::Uint64(uint64_value);
    return true;
  }
  return false;
}

// -------------------------------------------------------
// Value constructors
// -------------------------------------------------------

namespace values {

Value Int32Array(absl::Span<const int32_t> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Int32(v));
  }
  return Value::Array(Int32ArrayType(), value_vector);
}

Value Int64Array(absl::Span<const int64_t> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Int64(v));
  }
  return Value::Array(Int64ArrayType(), value_vector);
}

Value Uint32Array(absl::Span<const uint32_t> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Uint32(v));
  }
  return Value::Array(Uint32ArrayType(), value_vector);
}

Value Uint64Array(absl::Span<const uint64_t> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Uint64(v));
  }
  return Value::Array(Uint64ArrayType(), value_vector);
}

Value BoolArray(const std::vector<bool>& values) {
  std::vector<Value> value_vector;
  value_vector.reserve(values.size());
  for (auto v : values) {
    value_vector.push_back(Bool(v));
  }
  return Value::Array(BoolArrayType(), value_vector);
}

Value FloatArray(absl::Span<const float> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Float(v));
  }
  return Value::Array(FloatArrayType(), value_vector);
}

Value DoubleArray(absl::Span<const double> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Double(v));
  }
  return Value::Array(DoubleArrayType(), value_vector);
}

Value StringArray(absl::Span<const std::string> values) {
  std::vector<Value> value_vector;
  for (const std::string& v : values) {
    value_vector.push_back(String(v));
  }
  return Value::Array(StringArrayType(), value_vector);
}

Value BytesArray(absl::Span<const std::string> values) {
  std::vector<Value> value_vector;
  for (const std::string& v : values) {
    value_vector.push_back(Bytes(v));
  }
  return Value::Array(BytesArrayType(), value_vector);
}

Value NumericArray(absl::Span<const NumericValue> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Value::Numeric(v));
  }
  return Value::Array(NumericArrayType(), value_vector);
}

}  // namespace values

zetasql_base::Status Value::Serialize(ValueProto* value_proto) const {
  value_proto->Clear();
  if (is_null()) {
    return ::zetasql_base::OkStatus();
  }

  switch (type_kind()) {
    case TYPE_INT32:
      value_proto->set_int32_value(int32_value());
      break;
    case TYPE_INT64:
      value_proto->set_int64_value(int64_value());
      break;
    case TYPE_UINT32:
      value_proto->set_uint32_value(uint32_value());
      break;
    case TYPE_UINT64:
      value_proto->set_uint64_value(uint64_value());
      break;
    case TYPE_BOOL:
      value_proto->set_bool_value(bool_value());
      break;
    case TYPE_FLOAT:
      value_proto->set_float_value(float_value());
      break;
    case TYPE_DOUBLE:
      value_proto->set_double_value(double_value());
      break;
    case TYPE_NUMERIC:
      value_proto->set_numeric_value(numeric_value().SerializeAsProtoBytes());
      break;
    case TYPE_STRING:
      value_proto->set_string_value(string_value());
      break;
    case TYPE_BYTES:
      value_proto->set_bytes_value(bytes_value());
      break;
    case TYPE_DATE:
      value_proto->set_date_value(date_value());
      break;
    case TYPE_TIMESTAMP: {
      ZETASQL_RETURN_IF_ERROR(zetasql_base::EncodeGoogleApiProto(
          ToTime(), value_proto->mutable_timestamp_value()));
      break;
    }
    case TYPE_DATETIME: {
      auto* datetime_proto = value_proto->mutable_datetime_value();
      datetime_proto->set_bit_field_datetime_seconds(
          datetime_value().Packed64DatetimeSeconds());
      datetime_proto->set_nanos(datetime_value().Nanoseconds());
      break;
    }
    case TYPE_TIME:
      value_proto->set_time_value(time_value().Packed64TimeNanos());
      break;
    case TYPE_ENUM:
      value_proto->set_enum_value(enum_value());
      break;
    case TYPE_ARRAY: {
      // Create array_value so the result array is not NULL even when there
      // are no elements.
      auto* array_proto = value_proto->mutable_array_value();
      for (const Value& element : elements()) {
        ZETASQL_RETURN_IF_ERROR(element.Serialize(array_proto->add_element()));
      }
      break;
    }
    case TYPE_STRUCT: {
      // Create struct_value so the result struct is not NULL even when there
      // are no fields in it.
      auto* struct_proto = value_proto->mutable_struct_value();
      for (const Value& field : fields()) {
        ZETASQL_RETURN_IF_ERROR(field.Serialize(struct_proto->add_field()));
      }
      break;
    }
    case TYPE_PROTO:
      value_proto->set_proto_value(ToCord());
      break;
    default:
      return zetasql_base::Status(
          zetasql_base::StatusCode::kInternal,
          absl::StrCat("Unsupported type ", type()->DebugString()));
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status TypeMismatchError(const ValueProto& value_proto,
                                      const Type* type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInternal,
      absl::StrCat("Type mismatch: provided type ", type->DebugString(),
                   " but proto <", value_proto.ShortDebugString(),
                   "> doesn't have field of that type and is not null."));
}

zetasql_base::StatusOr<Value> Value::Deserialize(const ValueProto& value_proto,
                                         const Type* type) {
  if (value_proto.value_case() == ValueProto::VALUE_NOT_SET) {
    return Null(type);
  }
  switch (type->kind()) {
    case TYPE_INT32:
      if (!value_proto.has_int32_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Int32(value_proto.int32_value());
    case TYPE_INT64:
      if (!value_proto.has_int64_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Int64(value_proto.int64_value());
    case TYPE_UINT32:
      if (!value_proto.has_uint32_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Uint32(value_proto.uint32_value());
    case TYPE_UINT64:
      if (!value_proto.has_uint64_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Uint64(value_proto.uint64_value());
    case TYPE_BOOL:
      if (!value_proto.has_bool_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Bool(value_proto.bool_value());
    case TYPE_FLOAT:
      if (!value_proto.has_float_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Float(value_proto.float_value());
    case TYPE_DOUBLE:
      if (!value_proto.has_double_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Double(value_proto.double_value());
    case TYPE_NUMERIC: {
      if (!value_proto.has_numeric_value()) {
        return TypeMismatchError(value_proto, type);
      }
      ZETASQL_ASSIGN_OR_RETURN(NumericValue numeric_v,
                       NumericValue::DeserializeFromProtoBytes(
                           value_proto.numeric_value()));
      return Numeric(numeric_v);
    }
    case TYPE_STRING:
      if (!value_proto.has_string_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return String(value_proto.string_value());
    case TYPE_BYTES:
      if (!value_proto.has_bytes_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Bytes(value_proto.bytes_value());
    case TYPE_DATE:
      if (!value_proto.has_date_value()) {
        return TypeMismatchError(value_proto, type);
      }
      if (!functions::IsValidDate(value_proto.date_value())) {
        return zetasql_base::Status(
            zetasql_base::StatusCode::kOutOfRange,
            absl::StrCat("Invalid value for DATE: ", value_proto.date_value()));
      }

      return Date(value_proto.date_value());
    case TYPE_TIMESTAMP: {
      if (!value_proto.has_timestamp_value()) {
        return TypeMismatchError(value_proto, type);
      }

      auto time_or =
          zetasql_base::DecodeGoogleApiProto(value_proto.timestamp_value());
      if (!time_or.ok()) {
        return zetasql_base::Status(
            zetasql_base::StatusCode::kOutOfRange,
            absl::StrCat("Invalid value for TIMESTAMP",
                         value_proto.timestamp_value().DebugString()));
      } else {
        absl::Time t = time_or.ValueOrDie();
        if (!functions::IsValidTime(t)) {
          return zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                              absl::StrCat("Invalid value for TIMESTAMP: ",
                                           absl::FormatTime(t)));
        } else {
          return Timestamp(t);
        }
      }
    }
    case TYPE_DATETIME: {
      if (!value_proto.has_datetime_value()) {
        return TypeMismatchError(value_proto, type);
      }
      DatetimeValue wrapper = DatetimeValue::FromPacked64SecondsAndNanos(
          value_proto.datetime_value().bit_field_datetime_seconds(),
          value_proto.datetime_value().nanos());
      if (!wrapper.IsValid()) {
        return zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                            "Invalid value for DATETIME");
      }
      return Datetime(wrapper);
    }
    case TYPE_TIME: {
      if (!value_proto.has_time_value()) {
        return TypeMismatchError(value_proto, type);
      }
      TimeValue wrapper = TimeValue::FromPacked64Nanos(
          value_proto.time_value());
      if (!wrapper.IsValid()) {
        return zetasql_base::Status(zetasql_base::StatusCode::kOutOfRange,
                            "Invalid value for TIME");
      }
      return Time(wrapper);
    }
    case TYPE_ENUM:
      if (!value_proto.has_enum_value()) {
        return TypeMismatchError(value_proto, type);
      }
      if (type->AsEnum()->enum_descriptor()->FindValueByNumber(
          value_proto.enum_value()) == nullptr) {
        return zetasql_base::Status(
            zetasql_base::StatusCode::kOutOfRange,
            absl::StrCat("Invalid value for ", type->DebugString(), ": ",
                         value_proto.enum_value()));
      }
      return Enum(type->AsEnum(), value_proto.enum_value());
    case TYPE_ARRAY: {
      if (!value_proto.has_array_value()) {
        return TypeMismatchError(value_proto, type);
      }
      std::vector<Value> elements;
      for (const auto& element : value_proto.array_value().element()) {
        auto status_or_value =
            Deserialize(element, type->AsArray()->element_type());
        ZETASQL_RETURN_IF_ERROR(status_or_value.status());
        elements.push_back(status_or_value.ValueOrDie());
      }
      return Array(type->AsArray(), elements);
    }
    case TYPE_STRUCT: {
      if (!value_proto.has_struct_value()) {
        return TypeMismatchError(value_proto, type);
      }
      const StructType* struct_type = type->AsStruct();
      if (value_proto.struct_value().field_size() !=
          struct_type->num_fields()) {
        return zetasql_base::Status(
            zetasql_base::StatusCode::kInternal,
            absl::StrCat("Type mismatch for struct. Type has ",
                         struct_type->num_fields(), " fields, but proto has ",
                         value_proto.struct_value().field_size(), " fields."));
      }
      std::vector<Value> fields;
      for (int i = 0; i < struct_type->num_fields(); i++) {
        auto status_or_value = Deserialize(value_proto.struct_value().field(i),
                                           struct_type->field(i).type);
        ZETASQL_RETURN_IF_ERROR(status_or_value.status());
        fields.emplace_back(std::move(*status_or_value));
      }
      return Struct(struct_type, fields);
    }
    case TYPE_PROTO:
      if (!value_proto.has_proto_value()) {
        return TypeMismatchError(value_proto, type);
      }
      return Proto(type->AsProto(), value_proto.proto_value());
    default:
      return zetasql_base::Status(
          zetasql_base::StatusCode::kInternal,
          absl::StrCat("Unsupported type ", type->DebugString()));
  }
}

}  // namespace zetasql
