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

// This file implements inline methods of zetasql::Value. It is not intended
// to be read or included by users.



#ifndef ZETASQL_PUBLIC_VALUE_INL_H_
#define ZETASQL_PUBLIC_VALUE_INL_H_

#include <stddef.h>
#include <string.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"  
#include <cstdint>
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/simple_reference_counted.h"

namespace zetasql {

class Value::TypedList : public zetasql_base::SimpleReferenceCounted {
 public:
  explicit TypedList(const Type* type) : type_(type) { CHECK(type != nullptr); }

  TypedList(const TypedList&) = delete;
  TypedList& operator=(const TypedList&) = delete;

  const Type* type() const { return type_; }
  std::vector<Value>& values() { return values_; }
  uint64_t physical_byte_size() const {
    if (physical_byte_size_.has_value()) {
      return physical_byte_size_.value();
    }
    uint64_t size = sizeof(TypedList);
    for (const Value& value : values_) {
      size += value.physical_byte_size();
    }
    physical_byte_size_ = size;
    return size;
  }

 private:
  const Type* type_;  // not owned
  std::vector<Value> values_;
  mutable absl::optional<uint64_t> physical_byte_size_;
};

// -------------------------------------------------------
// ProtoRep
// -------------------------------------------------------
// Even though Cord is internally reference counted, ProtoRep is reference
// counted so that the internal representation can keep track of state
// associated with a ProtoRep (specifically, already deserialized fields).
class Value::ProtoRep : public zetasql_base::SimpleReferenceCounted {
 public:
  using Cord = std::string;
  ProtoRep(const ProtoType* type, Cord value)
      : type_(type), value_(std::move(value)) {
    CHECK(type != nullptr);
    CHECK(type->descriptor() != nullptr);
  }

  ProtoRep(const ProtoRep&) = delete;
  ProtoRep& operator=(const ProtoRep&) = delete;

  const ProtoType* type() const { return type_; }
  const Cord& value() const { return value_; }
  uint64_t physical_byte_size() const { return sizeof(ProtoRep) + value_.size(); }

 private:
  const ProtoType* type_;
  const Cord value_;
};

class Value::GeographyRef final : public zetasql_base::SimpleReferenceCounted {
 public:
  GeographyRef() {}
  GeographyRef(const GeographyRef&) = delete;
  GeographyRef& operator=(const GeographyRef&) = delete;

  const uint64_t physical_byte_size() const {
    return sizeof(GeographyRef);
  }
};

// -------------------------------------------------------
// NumericRef is ref count wrapper around NumericValue.
// -------------------------------------------------------
class Value::NumericRef : public zetasql_base::SimpleReferenceCounted {
 public:
  NumericRef() {}
  explicit NumericRef(const NumericValue& value)
      : value_(value) {
  }

  NumericRef(const NumericRef&) = delete;
  NumericRef& operator=(const NumericRef&) = delete;

  const NumericValue& value() {
    return value_;
  }

 private:
  NumericValue value_;
};

// -------------------------------------------------------
// StringRef is ref count wrapper around std::string.
// -------------------------------------------------------
class Value::StringRef : public zetasql_base::SimpleReferenceCounted {
 public:
  StringRef() {}
  explicit StringRef(std::string value)
      : value_(std::move(value)) {
  }

  StringRef(const StringRef&) = delete;
  StringRef& operator=(const StringRef&) = delete;

  const std::string& value() const {
    return value_;
  }

  uint64_t physical_byte_size() const {
    return sizeof(StringRef) + value_.size() * sizeof(char);
  }

 private:
  const std::string value_;
};

// -------------------------------------------------------
// Value
// -------------------------------------------------------

// Invalid value.
inline Value::Value() {
}

inline Value::Value(const Value& that) {
  CopyFrom(that);
}

inline void Value::Clear() {
  switch (type_kind_) {
    case TYPE_STRING:
    case TYPE_BYTES:
      string_ptr_->Unref();
      break;
    case TYPE_ARRAY:
    case TYPE_STRUCT:
      // TODO This recursively deletes Values, so for deeply nested
      // struct types, we can get a stack overflow.
      list_ptr_->Unref();
      break;
    case TYPE_GEOGRAPHY:
      geography_ptr_->Unref();
      break;
    case TYPE_NUMERIC:
      numeric_ptr_->Unref();
      break;
    case TYPE_PROTO:
      proto_ptr_->Unref();
      break;
    default:
      break;
  }
  type_kind_ = kInvalidTypeKind;
}

inline const Value& Value::operator=(const Value& that) {
  // Self-copying must not clear the contents of the value.
  if (this == &that) {
    return *this;
  }
  Clear();
  CopyFrom(that);
  return *this;
}

// GCC does not like our use of memcpy here. So we disable its warnings.
#if defined(__GNUC__) && (((__GNUC__ * 100) + __GNUC_MINOR__) >= 800)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
inline Value::Value(Value&& that) noexcept {  // NOLINT(build/c++11)
  memcpy(this, &that, sizeof(Value));
  // Invalidate 'that' to disable its destructor.
  that.type_kind_ = kInvalidTypeKind;
}

// Self-move of an rvalue reference leaves 'that' and consequently 'this' in an
// unspecified state. Hence, we don't need to check for 'this == &that' and
// simply set 'that' to an invalid value. See
// http://en.cppreference.com/w/cpp/utility/move for more context.
inline Value& Value::operator=(Value&& that) noexcept {  // NOLINT(build/c++11)
  // Clear "this" to destroy all pointers.
  Clear();
  memcpy(this, &that, sizeof(Value));
  // Invalidate 'that' to disable its destructor.
  that.type_kind_ = kInvalidTypeKind;
  return *this;
}

#if defined(__GNUC__) && (((__GNUC__ * 100) + __GNUC_MINOR__) >= 800)
#pragma GCC diagnostic pop
#endif

inline Value::Value(int32_t value)
    : type_kind_(TYPE_INT32), int32_value_(value) {
}

inline Value::Value(int64_t value)
    : type_kind_(TYPE_INT64), int64_value_(value) {
}

inline Value::Value(uint32_t value)
    : type_kind_(TYPE_UINT32), uint32_value_(value) {
}

inline Value::Value(uint64_t value)
    : type_kind_(TYPE_UINT64), uint64_value_(value) {
}

inline Value::Value(bool value)
    : type_kind_(TYPE_BOOL), bool_value_(value) {
}

inline Value::Value(float value)
    : type_kind_(TYPE_FLOAT), float_value_(value) {
}

inline Value::Value(double value)
    : type_kind_(TYPE_DOUBLE), double_value_(value) {
}

inline Value::Value(TypeKind type_kind, std::string value)
    : type_kind_(static_cast<int16_t>(type_kind)),
      string_ptr_(new StringRef(std::move(value))) {
  CHECK(type_kind == TYPE_STRING ||
        type_kind == TYPE_BYTES);
}

inline Value::Value(const NumericValue& numeric)
    : type_kind_(TYPE_NUMERIC), numeric_ptr_(new NumericRef(numeric)) {
}

inline Value Value::Struct(const StructType* type,
                           absl::Span<const Value> values) {
  std::vector<Value> value_copies(values.begin(), values.end());
  return StructInternal(/*safe=*/true, type, std::move(value_copies));
}

inline Value Value::UnsafeStruct(const StructType* type,
                                 std::vector<Value>&& values) {
  return StructInternal(/*safe=*/false, type, std::move(values));
}

inline Value Value::Array(const ArrayType* array_type,
                          absl::Span<const Value> values) {
  std::vector<Value> value_copies(values.begin(), values.end());
  return ArrayInternal(/*safe=*/true, array_type, kPreservesOrder,
                       std::move(value_copies));
}

inline Value Value::UnsafeArray(const ArrayType* array_type,
                                std::vector<Value>&& values) {
  return ArrayInternal(/*safe=*/false, array_type, kPreservesOrder,
                       std::move(values));
}

inline Value Value::EmptyArray(const ArrayType* array_type) {
  return Array(array_type, {});
}

inline Value Value::Int32(int32_t v) { return Value(v); }
inline Value Value::Int64(int64_t v) { return Value(v); }
inline Value Value::Uint32(uint32_t v) { return Value(v); }
inline Value Value::Uint64(uint64_t v) { return Value(v); }
inline Value Value::Bool(bool v) { return Value(v); }
inline Value Value::Float(float v) { return Value(v); }
inline Value Value::Double(double v) { return Value(v); }

inline Value Value::StringValue(std::string v) {
  return Value(TYPE_STRING, std::move(v));
}
inline Value Value::String(absl::string_view v) {
  return Value(TYPE_STRING, std::string(v));
}

template <size_t N>
inline Value Value::String(const char (&str)[N]) {
  return Value::String(std::string(str, N - 1));
}

inline Value Value::Bytes(std::string v) { return Value(TYPE_BYTES, std::move(v)); }
inline Value Value::Bytes(absl::string_view v) {
  return Value(TYPE_BYTES, std::string(v));
}
template <size_t N>
inline Value Value::Bytes(const char (&str)[N]) {
  return Value::Bytes(std::string(str, N - 1));
}

inline Value Value::Date(int32_t v) {
  return Value(TYPE_DATE, v);
}
inline Value Value::Timestamp(absl::Time t) { return Value(t); }
inline Value Value::Time(TimeValue time) {
  return Value(time);
}
inline Value Value::Datetime(DatetimeValue datetime) {
  return Value(datetime);
}
inline Value Value::Numeric(NumericValue v) {
  return Value(v);
}
inline Value Value::Enum(const EnumType* type, int64_t value) {
  return Value(type, value);
}
inline Value Value::Enum(const EnumType* type, absl::string_view name) {
  return Value(type, name);
}
inline Value Value::Proto(const ProtoType* type, const std::string& value) {
  return Value(type, std::move(value));
}

inline Value Value::NullInt32() { return Value(types::Int32Type()); }
inline Value Value::NullInt64() { return Value(types::Int64Type()); }
inline Value Value::NullUint32() { return Value(types::Uint32Type()); }
inline Value Value::NullUint64() { return Value(types::Uint64Type()); }
inline Value Value::NullBool() { return Value(types::BoolType()); }
inline Value Value::NullFloat() { return Value(types::FloatType()); }
inline Value Value::NullDouble() { return Value(types::DoubleType()); }
inline Value Value::NullString() { return Value(types::StringType()); }
inline Value Value::NullBytes() { return Value(types::BytesType()); }
inline Value Value::NullDate() { return Value(types::DateType()); }
inline Value Value::NullTimestamp() {
  return Value(types::TimestampType());
}
inline Value Value::NullTime() {
  return Value(types::TimeType());
}
inline Value Value::NullDatetime() {
  return Value(types::DatetimeType());
}
inline Value Value::NullGeography() {
  return Value(types::GeographyType());
}
inline Value Value::NullNumeric() {
  return Value(types::NumericType());
}
inline Value Value::EmptyGeography() {
  CHECK(false);
  return NullGeography();
}

inline Value Value::Null(const Type* type) {
  return Value(type);
}

inline Value::~Value() {
  Clear();
}

inline TypeKind Value::type_kind() const {
  CHECK(is_valid()) << DebugString();
  return static_cast<TypeKind>(type_kind_);
}

inline bool Value::is_null() const {
  CHECK(is_valid()) << DebugString();
  return is_null_;
}

inline bool Value::is_empty_array() const {
  CHECK(is_valid()) << DebugString();
  return type()->IsArray() && !is_null() && empty();
}

inline bool Value::order_kind() const {
  CHECK_EQ(TYPE_ARRAY, type_kind_);
  return order_kind_;
}

inline bool Value::is_valid() const {
  static_assert(TYPE_UNKNOWN == 0 && kInvalidTypeKind == -1,
                "Revisit implementation");
  // This check assumes that valid TypeKind values are positive.
  return type_kind_ > 0;
}

inline int32_t Value::int32_value() const {
  CHECK_EQ(TYPE_INT32, type_kind_) << "Not an int32_t value";
  CHECK(!is_null_) << "Null value";
  return int32_value_;
}

inline int64_t Value::int64_value() const {
  CHECK_EQ(TYPE_INT64, type_kind_) << "Not an int64_t value";
  CHECK(!is_null_) << "Null value";
  return int64_value_;
}

inline uint32_t Value::uint32_value() const {
  CHECK_EQ(TYPE_UINT32, type_kind_) << "Not a uint32_t value";
  CHECK(!is_null_) << "Null value";
  return uint32_value_;
}

inline uint64_t Value::uint64_value() const {
  CHECK_EQ(TYPE_UINT64, type_kind_) << "Not a uint64_t value";
  CHECK(!is_null_) << "Null value";
  return uint64_value_;
}

inline bool Value::bool_value() const {
  CHECK_EQ(TYPE_BOOL, type_kind_) << "Not a bool value";
  CHECK(!is_null_) << "Null value";
  return bool_value_;
}

inline float Value::float_value() const {
  CHECK_EQ(TYPE_FLOAT, type_kind_) << "Not a float value";
  CHECK(!is_null_) << "Null value";
  return float_value_;
}

inline double Value::double_value() const {
  CHECK_EQ(TYPE_DOUBLE, type_kind_) << "Not a double value";
  CHECK(!is_null_) << "Null value";
  return double_value_;
}

inline const std::string& Value::string_value() const {
  CHECK_EQ(TYPE_STRING, type_kind_) << "Not a std::string value";
  CHECK(!is_null_) << "Null value";
  return string_ptr_->value();
}

inline const std::string& Value::bytes_value() const {
  CHECK_EQ(TYPE_BYTES, type_kind_) << "Not a bytes value";
  CHECK(!is_null_) << "Null value";
  return string_ptr_->value();
}

inline int32_t Value::date_value() const {
  CHECK_EQ(TYPE_DATE, type_kind_) << "Not a date value";
  CHECK(!is_null_) << "Null value";
  return int32_value_;
}

inline int32_t Value::enum_value() const {
  CHECK_EQ(TYPE_ENUM, type_kind_) << "Not an enum value";
  CHECK(!is_null_) << "Null value";
  return enum_value_;
}

inline TimeValue Value::time_value() const {
  return TimeValue::FromPacked32SecondsAndNanos(
      bit_field_32_value_, subsecond_nanos_);
}

inline DatetimeValue Value::datetime_value() const {
  return DatetimeValue::FromPacked64SecondsAndNanos(
      bit_field_64_value_, subsecond_nanos_);
}

inline const NumericValue& Value::numeric_value() const {
  CHECK_EQ(TYPE_NUMERIC, type_kind_) << "Not a numeric type";
  CHECK(!is_null_) << "Null value";
  return numeric_ptr_->value();
}

inline bool Value::empty() const {
  return elements().empty();
}

inline int Value::num_elements() const {
  return elements().size();
}

inline int Value::num_fields() const {
  return fields().size();
}

inline const Value& Value::field(int i) const {
  return fields()[i];
}

inline const Value& Value::element(int i) const {
  CHECK(type()->IsArray());
  return elements()[i];
}

inline bool Value::Equals(const Value& that) const {
  return EqualsInternal(*this, that, false /* allow_bags */,
                        nullptr /* deep_order_spec */, kExactFloatMargin,
                        nullptr /* reason */);
}

template <typename H>
H AbslHashValue(H h, const Value& v) {
  return v.HashValueInternal<H>(std::move(h));
}

template <typename H>
H Value::HashValueInternal(H h) const {
  // These codes are picked arbitrarily.
  static constexpr uint64_t kFloatNanHashCode = 0x739EF9A0B2C15522ull;
  static constexpr uint64_t kDoubleNanHashCode = 0xA00397BC84F93AA7ull;
  static constexpr uint64_t kGeographyHashCode = 0x98389DC9632631AEull;

  if (!is_valid() || is_null()) {
    // Note that invalid Values have their own TypeKind, so hash codes for
    // invalid Values do not collide with hash codes for NULL values.
    return H::combine(std::move(h), type_kind_);
  }
  switch (type_kind()) {
    case TYPE_INT32: {
      return H::combine(std::move(h), int32_value_);
    }
    case TYPE_INT64: {
      return H::combine(std::move(h), int64_value_);
    }
    case TYPE_UINT32: {
      return H::combine(std::move(h), uint32_value_);
    }
    case TYPE_UINT64: {
      return H::combine(std::move(h), uint64_value_);
    }
    case TYPE_BOOL: {
      return H::combine(std::move(h), bool_value_);
    }
    case TYPE_FLOAT: {
      if (std::isnan(float_value_)) {
        return H::combine(std::move(h), kFloatNanHashCode);
      } else {
        return H::combine(std::move(h), float_value_);
      }
    }
    case TYPE_DOUBLE: {
      if (std::isnan(double_value_)) {
        return H::combine(std::move(h), kDoubleNanHashCode);
      } else {
        return H::combine(std::move(h), double_value_);
      }
    }
    case TYPE_STRING:
    case TYPE_BYTES: {
      return H::combine(std::move(h), string_ptr_->value());
    }
    case TYPE_DATE: {
      return H::combine(std::move(h), int32_value_);
    }
    case TYPE_TIMESTAMP: {
      return H::combine(std::move(h), type_kind_, timestamp_seconds_,
                        subsecond_nanos_);
    }
    case TYPE_TIME: {
      return H::combine(std::move(h), type_kind_, bit_field_32_value_,
                        subsecond_nanos_);
    }
    case TYPE_DATETIME: {
      return H::combine(std::move(h), type_kind_, bit_field_64_value_,
                        subsecond_nanos_);
    }
    case TYPE_ENUM: {
      return H::combine(std::move(h), enum_value_);
    }
    case TYPE_ARRAY: {
      // We must hash arrays as if unordered to support hash_map and hash_set of
      // values containing arrays with order_kind()=kIgnoresOrder.
      // absl::Hash lacks support for unordered containers, so we create a
      // cheapo solution of just adding the hashcodes.
      absl::Hash<Value> element_hasher;
      size_t combined_hash = 1;
      for (int i = 0; i < num_elements(); i++) {
        combined_hash += element_hasher(element(i));
      }
      return H::combine(std::move(h), type_kind_, combined_hash);
    }
    case TYPE_STRUCT: {
      // combine is an ordered combine, which is what we want.
      return H::combine(std::move(h), type_kind_, fields());
    }
    case TYPE_PROTO: {
      // No efficient way to compute a hash on protobufs, so just let equals
      // sort it out.
      return H::combine(std::move(h), TYPE_PROTO);
    }
    case TYPE_NUMERIC: {
      return H::combine(std::move(h), numeric_value());
    }
    case TYPE_GEOGRAPHY: {
      // We have no good hasher for geography (??)
      // so we just rely on a constant for hashing.
      return H::combine(std::move(h), kGeographyHashCode);
    }
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      LOG(DFATAL) << "Unexpected expected internally only: " << type_kind_;
  }
  return H::combine(std::move(h), type_kind_);
}

template <>
inline Value Value::Make<int32_t>(int32_t value) { return Value::Int32(value); }
template <>
inline Value Value::Make<int64_t>(int64_t value) { return Value::Int64(value); }
template <>
inline Value Value::Make<uint32_t>(uint32_t value) { return Value::Uint32(value); }
template <>
inline Value Value::Make<uint64_t>(uint64_t value) { return Value::Uint64(value); }
template <>
inline Value Value::Make<bool>(bool value) { return Value::Bool(value); }
template <>
inline Value Value::Make<float>(float value) { return Value::Float(value); }
template <>
inline Value Value::Make<double>(double value) { return Value::Double(value); }
template <>
inline Value Value::Make<NumericValue>(NumericValue value) {
  return Value::Numeric(value);
}

template <>
inline Value Value::MakeNull<int32_t>() { return Value::NullInt32(); }
template <>
inline Value Value::MakeNull<int64_t>() { return Value::NullInt64(); }
template <>
inline Value Value::MakeNull<uint32_t>() { return Value::NullUint32(); }
template <>
inline Value Value::MakeNull<uint64_t>() { return Value::NullUint64(); }
template <>
inline Value Value::MakeNull<bool>() { return Value::NullBool(); }
template <>
inline Value Value::MakeNull<float>() { return Value::NullFloat(); }
template <>
inline Value Value::MakeNull<double>() { return Value::NullDouble(); }
template <>
inline Value Value::MakeNull<NumericValue>() { return Value::NullNumeric(); }

template <> inline int32_t Value::Get<int32_t>() const { return int32_value(); }
template <> inline int64_t Value::Get<int64_t>() const { return int64_value(); }
template <> inline uint32_t Value::Get<uint32_t>() const { return uint32_value(); }
template <> inline uint64_t Value::Get<uint64_t>() const { return uint64_value(); }
template <> inline bool Value::Get<bool>() const { return bool_value(); }
template <> inline float Value::Get<float>() const { return float_value(); }
template <> inline double Value::Get<double>() const { return double_value(); }
template <>
inline NumericValue Value::Get<NumericValue>() const { return numeric_value(); }

namespace values {

inline Value Int32(int32_t v) { return Value::Int32(v); }
inline Value Int64(int64_t v) { return Value::Int64(v); }
inline Value Uint32(uint32_t v) { return Value::Uint32(v); }
inline Value Uint64(uint64_t v) { return Value::Uint64(v); }
inline Value Bool(bool v) { return Value::Bool(v); }
inline Value Float(float v) { return Value::Float(v); }
inline Value Double(double v) { return Value::Double(v); }
inline Value String(absl::string_view v) { return Value::String(v); }
template <size_t N>
inline Value String(const char (&str)[N]) { return Value::String(str); }
inline Value Bytes(absl::string_view v) { return Value::Bytes(v); }
template <size_t N>
inline Value Bytes(const char (&str)[N]) { return Value::Bytes(str); }
inline Value Date(int32_t v) { return Value::Date(v); }
inline Value Timestamp(absl::Time time) { return Value::Timestamp(time); }
inline Value TimestampFromUnixMicros(int64_t v) {
  return Value::TimestampFromUnixMicros(v);
}
inline Value Time(TimeValue time) { return Value::Time(time); }
inline Value TimeFromPacked64Micros(int64_t v) {
  return Value::TimeFromPacked64Micros(v);
}
inline Value Datetime(DatetimeValue datetime) {
  return Value::Datetime(datetime);
}
inline Value DatetimeFromPacked64Micros(int64_t v) {
  return Value::DatetimeFromPacked64Micros(v);
}
inline Value Numeric(NumericValue v) { return Value::Numeric(v); }

inline Value Numeric(int64_t v) { return Value::Numeric(NumericValue(v)); }

inline Value Enum(const EnumType* enum_type, int32_t value) {
  return Value::Enum(enum_type, value);
}
inline Value Enum(const EnumType* enum_type, absl::string_view name) {
  return Value::Enum(enum_type, name);
}
inline Value Struct(const StructType* type, absl::Span<const Value> values) {
  return Value::Struct(type, values);
}
inline Value UnsafeStruct(const StructType* type, std::vector<Value>&& values) {
  return Value::UnsafeStruct(type, std::move(values));
}

inline Value Proto(const ProtoType* proto_type, const std::string& value) {
  return Value::Proto(proto_type, std::move(value));
}
inline Value Proto(const ProtoType* proto_type, const google::protobuf::Message& msg) {
  std::string bytes;
  CHECK(msg.SerializeToString(&bytes));
  return Value::Proto(proto_type, std::move(bytes));
}
inline Value EmptyArray(const ArrayType* type) {
  return Value::Array(type, {});
}
inline Value Array(const ArrayType* type, absl::Span<const Value> values) {
  return Value::Array(type, values);
}
inline Value UnsafeArray(const ArrayType* type, std::vector<Value>&& values) {
  return Value::UnsafeArray(type, std::move(values));
}
inline Value True() {
  return Value::Bool(true);
}
inline Value False() {
  return Value::Bool(false);
}
inline Value EmptyGeography() { return Value::EmptyGeography(); }
inline Value NullInt32() { return Value::NullInt32(); }
inline Value NullInt64() { return Value::NullInt64(); }
inline Value NullUint32() { return Value::NullUint32(); }
inline Value NullUint64() { return Value::NullUint64(); }
inline Value NullBool() { return Value::NullBool(); }
inline Value NullFloat() { return Value::NullFloat(); }
inline Value NullDouble() { return Value::NullDouble(); }
inline Value NullString() { return Value::NullString(); }
inline Value NullBytes() { return Value::NullBytes(); }
inline Value NullDate() { return Value::NullDate(); }
inline Value NullTimestamp() { return Value::NullTimestamp(); }
inline Value NullTime() { return Value::NullTime(); }
inline Value NullDatetime() { return Value::NullDatetime(); }
inline Value NullGeography() { return Value::NullGeography(); }
inline Value NullNumeric() { return Value::NullNumeric(); }
inline Value Null(const Type* type) { return Value::Null(type); }

inline Value Invalid() { return Value::Invalid(); }

}  // namespace values
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_VALUE_INL_H_
