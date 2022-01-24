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

#include "zetasql/public/value.h"

#include <string.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
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
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value_content.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "zetasql/base/simple_reference_counted.h"
#include "zetasql/base/status_macros.h"

using zetasql::types::BigNumericArrayType;
using zetasql::types::BigNumericType;
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
using zetasql::types::JsonArrayType;
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::TimestampArrayType;
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

void Value::SetMetadataForNonSimpleType(const Type* type, bool is_null,
                                        bool preserves_order) {
  ZETASQL_DCHECK(!type->IsSimpleType());
  metadata_ = Metadata(type, is_null, preserves_order);
  internal::TypeStoreHelper::RefFromValue(type->type_store_);
}

// Null value constructor.
Value::Value(const Type* type, bool is_null, OrderPreservationKind order_kind) {
  ZETASQL_CHECK(type != nullptr);

  if (type->IsSimpleType()) {
    metadata_ = Metadata(type->kind(), is_null, order_kind,
                         /*value_extended_content=*/0);
  } else {
    SetMetadataForNonSimpleType(type, is_null, order_kind);
  }
}

void Value::CopyFrom(const Value& that) {
  // Self-copy check is done in the copy constructor. Here we just ZETASQL_DCHECK that.
  ZETASQL_DCHECK_NE(this, &that);
  memcpy(this, &that, sizeof(Value));
  if (!is_valid()) {
    return;
  }

  if (metadata_.has_type_pointer()) {
    internal::TypeStoreHelper::RefFromValue(metadata_.type()->type_store_);
    // TODO: currently struct and array maintain a reference
    // counter for their value. To improve performance of struct/array copying,
    // instead of incrementing types reference counter on each Value copy, we
    // can just do a single decrement when Value reference counter reaches zero.

    if (!is_null()) {
      ValueContent copied_content{};
      that.type()->CopyValueContent(that.GetContent(), &copied_content);
      SetContent(copied_content);
    }
  } else {
    // When we already have a type_kind, we know that we are a SimpleType.
    // Dispatching directly to the SimpleType implementation avoids unnecessary
    // virtual calls and switches.
    if (!is_null()) {
      ValueContent copied_content{};
      SimpleType::CopyValueContent(metadata_.type_kind(), that.GetContent(),
                                   &copied_content);
      SetContent(copied_content);
    }
  }
}

Value::Value(TypeKind type_kind, int64_t value) : metadata_(type_kind) {
  switch (type_kind) {
    case TYPE_DATE:
      ZETASQL_CHECK_LE(value, types::kDateMax);
      ZETASQL_CHECK_GE(value, types::kDateMin);
      int32_value_ = value;
      break;
    default:
      ZETASQL_LOG(FATAL) << "Invalid use of private constructor: " << type_kind;
  }
}

Value::Value(absl::Time t) {
  ZETASQL_CHECK(functions::IsValidTime(t));
  timestamp_seconds_ = absl::ToUnixSeconds(t);
  const int32_t subsecond_nanos =
      (t - absl::FromUnixSeconds(timestamp_seconds_)) / absl::Nanoseconds(1);
  metadata_ = Metadata(TypeKind::TYPE_TIMESTAMP, subsecond_nanos);
}

Value::Value(TimeValue time)
    : metadata_(TypeKind::TYPE_TIME, time.Nanoseconds()),
      bit_field_32_value_(time.Packed32TimeSeconds()) {
  ZETASQL_CHECK(time.IsValid());
}

Value::Value(DatetimeValue datetime)
    : metadata_(TypeKind::TYPE_DATETIME, datetime.Nanoseconds()),
      bit_field_64_value_(datetime.Packed64DatetimeSeconds()) {
  ZETASQL_CHECK(datetime.IsValid());
}

Value::Value(const EnumType* enum_type, int64_t value) {
  const std::string* unused;
  if (value >= std::numeric_limits<int32_t>::min() &&
      value <= std::numeric_limits<int32_t>::max() &&
      enum_type->FindName(value, &unused)) {
    // As only int32_t range enum values are supported, value can safely be casted
    // to int32_t once verified under enum range.
    SetMetadataForNonSimpleType(enum_type);
    enum_value_ = static_cast<int32_t>(value);
  } else {
    metadata_ = Metadata::Invalid();
  }
}

Value::Value(const EnumType* enum_type, absl::string_view name) {
  int32_t number;
  if (enum_type->FindNumber(std::string(name), &number)) {
    SetMetadataForNonSimpleType(enum_type);
    enum_value_ = static_cast<int32_t>(number);
  } else {
    metadata_ = Metadata::Invalid();
  }
}

Value::Value(const ProtoType* proto_type, absl::Cord value)
    : proto_ptr_(new internal::ProtoRep(proto_type, std::move(value))) {
  SetMetadataForNonSimpleType(proto_type);
}

Value::Value(const ExtendedType* extended_type, const ValueContent& value) {
  ZETASQL_DCHECK_EQ(value.simple_type_extended_content_, 0);
  SetMetadataForNonSimpleType(extended_type);
  SetContent(value);
}

#ifdef NDEBUG
static constexpr bool kDebugMode = false;
#else
static constexpr bool kDebugMode = true;
#endif

absl::StatusOr<Value> Value::MakeArrayInternal(bool already_validated,
                                               const ArrayType* array_type,
                                               OrderPreservationKind order_kind,
                                               std::vector<Value> values) {
  if (!already_validated || kDebugMode) {
    for (const Value& v : values) {
      ZETASQL_RET_CHECK(v.type()->Equals(array_type->element_type()))
          << "Array element " << v << " must be of type "
          << array_type->element_type()->DebugString();
    }
  }
  Value result(array_type, /*is_null=*/false, order_kind);
  result.list_ptr_ = new TypedList(array_type);
  std::vector<Value>& value_list = result.list_ptr_->values();
  value_list = std::move(values);
  return result;
}

absl::StatusOr<Value> Value::MakeStructInternal(bool already_validated,
                                                const StructType* struct_type,
                                                std::vector<Value> values) {
  if (!already_validated || kDebugMode) {
    // Check that values are compatible with the type.
    ZETASQL_RET_CHECK_EQ(struct_type->num_fields(), values.size());
    for (int i = 0; i < values.size(); ++i) {
      const Type* field_type = struct_type->field(i).type;
      const Type* value_type = values[i].type();
      ZETASQL_RET_CHECK(field_type->Equals(value_type))
          << "\nField type: " << field_type->DebugString()
          << "\nvs\nValue type: " << value_type->DebugString();
    }
  }
  Value result(struct_type, /*is_null=*/false, kPreservesOrder);
  result.list_ptr_ = new TypedList(struct_type);
  std::vector<Value>& value_list = result.list_ptr_->values();
  value_list = std::move(values);
  return result;
}

const Type* Value::type() const {
  ZETASQL_CHECK(is_valid()) << DebugString();
  return metadata_.type();
}

const std::vector<Value>& Value::fields() const {
  ZETASQL_CHECK_EQ(TYPE_STRUCT, metadata_.type_kind());
  ZETASQL_CHECK(!is_null()) << "Null value";
  return list_ptr_->values();
}

const std::vector<Value>& Value::elements() const {
  ZETASQL_CHECK_EQ(TYPE_ARRAY, metadata_.type_kind());
  ZETASQL_CHECK(!is_null()) << "Null value";
  return list_ptr_->values();
}

Value Value::TimestampFromUnixMicros(int64_t v) {
  ZETASQL_CHECK(functions::IsValidTimestamp(v, functions::kMicroseconds)) << v;
  return Value(absl::FromUnixMicros(v));
}

Value Value::TimeFromPacked64Micros(int64_t v) {
  TimeValue time = TimeValue::FromPacked64Micros(v);
  ZETASQL_CHECK(time.IsValid()) << "int64 " << v
                        << " decodes to an invalid time value: "
                        << time.DebugString();
  return Value(time);
}

Value Value::DatetimeFromPacked64Micros(int64_t v) {
  DatetimeValue datetime = DatetimeValue::FromPacked64Micros(v);
  ZETASQL_CHECK(datetime.IsValid())
      << "int64 " << v
      << " decodes to an invalid datetime value: " << datetime.DebugString();
  return Value(datetime);
}

const std::string& Value::enum_name() const {
  ZETASQL_CHECK_EQ(TYPE_ENUM, metadata_.type_kind()) << "Not an enum value";
  ZETASQL_CHECK(!is_null()) << "Null value";
  const std::string* enum_name = nullptr;
  ZETASQL_CHECK(type()->AsEnum()->FindName(enum_value(), &enum_name))
      << "Value " << enum_value() << " not in "
      << type()->AsEnum()->enum_descriptor()->DebugString();
  return *enum_name;
}

int64_t Value::ToInt64() const {
  ZETASQL_CHECK(!is_null()) << "Null value";
  switch (metadata_.type_kind()) {
    case TYPE_INT64: return int64_value_;
    case TYPE_INT32: return int32_value_;
    case TYPE_UINT32: return uint32_value_;
    case TYPE_BOOL: return bool_value_;
    case TYPE_DATE: return int32_value_;
    case TYPE_TIMESTAMP:
      return ToUnixMicros();
    case TYPE_ENUM:
      return enum_value();
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
    case TYPE_TIME:
    case TYPE_DATETIME:
    default:
      ZETASQL_LOG(FATAL) << "Cannot coerce " << TypeKind_Name(type_kind())
                 << " to int64";
  }
}

uint64_t Value::ToUint64() const {
  ZETASQL_CHECK(!is_null()) << "Null value";
  switch (metadata_.type_kind()) {
    case TYPE_UINT64: return uint64_value_;
    case TYPE_UINT32: return uint32_value_;
    case TYPE_BOOL: return bool_value_;
    default:
      ZETASQL_LOG(FATAL) << "Cannot coerce to uint64";
      return 0;
  }
}

double Value::ToDouble() const {
  ZETASQL_CHECK(!is_null()) << "Null value";
  switch (metadata_.type_kind()) {
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
    case TYPE_BIGNUMERIC:
      return bignumeric_value().ToDouble();
    case TYPE_ENUM:
      return enum_value();
    case TYPE_TIMESTAMP:
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_PROTO:
    case TYPE_TIME:
    case TYPE_DATETIME:
    default:
      ZETASQL_LOG(FATAL) << "Cannot coerce to double";
  }
}

uint64_t Value::physical_byte_size() const {
  uint64_t physical_size = sizeof(Value);
  if (!has_content()) {
    return physical_size;
  }

  physical_size +=
      DoesTypeUseValueList()
          ? list_ptr_->physical_byte_size()
          : type()->GetValueContentExternallyAllocatedByteSize(GetContent());
  return physical_size;
}

absl::Cord Value::ToCord() const {
  ZETASQL_CHECK(!is_null()) << "Null value";
  switch (metadata_.type_kind()) {
    case TYPE_STRING:
    case TYPE_BYTES:
      return absl::Cord(string_ptr_->value());
    case TYPE_PROTO:
      return proto_ptr_->value();
    default:
      ZETASQL_LOG(FATAL) << "Cannot coerce to Cord";
      return absl::Cord();
  }
}

absl::Time Value::ToTime() const {
  ZETASQL_CHECK(!is_null()) << "Null value";
  ZETASQL_CHECK_EQ(TYPE_TIMESTAMP, metadata_.type_kind()) << "Not a timestamp value";
  return absl::FromUnixSeconds(timestamp_seconds_) +
         absl::Nanoseconds(subsecond_nanos());
}

int64_t Value::ToUnixMicros() const { return absl::ToUnixMicros(ToTime()); }

int64_t Value::ToPacked64TimeMicros() const {
  return (static_cast<int64_t>(bit_field_32_value_) << kMicrosShift) |
         (subsecond_nanos() / 1000);
}

int64_t Value::ToPacked64DatetimeMicros() const {
  return (bit_field_64_value_ << kMicrosShift) | (subsecond_nanos() / 1000);
}

absl::Status Value::ToUnixNanos(int64_t* nanos) const {
  if (functions::FromTime(ToTime(), functions::kNanoseconds, nanos)) {
    return absl::OkStatus();
  }
  return absl::Status(absl::StatusCode::kOutOfRange,
                      absl::StrCat("Timestamp value in Unix epoch nanoseconds "
                                   "exceeds 64 bit: ",
                                   DebugString()));
}

ValueContent Value::extended_value() const {
  ZETASQL_CHECK_EQ(type_kind(), TYPE_EXTENDED);
  return GetContent();
}

google::protobuf::Message* Value::ToMessage(
    google::protobuf::DynamicMessageFactory* message_factory,
    bool return_null_on_error) const {
  ZETASQL_CHECK(type()->IsProto());
  ZETASQL_CHECK(!is_null());
  std::unique_ptr<google::protobuf::Message> m(
      message_factory->GetPrototype(type()->AsProto()->descriptor())->New());
  const bool success = m->ParsePartialFromString(std::string(ToCord()));
  if (!success && return_null_on_error) return nullptr;
  return m.release();
}

const Value& Value::FindFieldByName(absl::string_view name) const {
  ZETASQL_CHECK(type()->IsStruct());
  ZETASQL_CHECK(!is_null()) << "Null value";
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
      ZETASQL_DCHECK_EQ(children.size(), v.num_fields());
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
                           const ValueEqualityCheckOptions& options) {
  std::string* reason = options.reason;
  if (!x.is_valid()) { return !y.is_valid(); }
  if (!y.is_valid()) { return false; }

  if (!x.type()->Equivalent(y.type())) return TypesDiffer(x, y, reason);

  if (x.is_null() != y.is_null()) return false;
  if (x.is_null() && y.is_null()) return true;

  std::unique_ptr<DeepOrderKindSpec> owned_deep_order_spec;
  if (allow_bags && deep_order_spec == nullptr) {
    owned_deep_order_spec = absl::make_unique<DeepOrderKindSpec>();
    deep_order_spec = owned_deep_order_spec.get();

    deep_order_spec->FillSpec(x);
    deep_order_spec->FillSpec(y);
  }

  // TODO: move struct and array logic into Type subclasses.
  switch (x.type_kind()) {
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
        return EqualElementMultiSet(x, y, element_order_spec, options);
      }
      for (int i = 0; i < x.num_elements(); i++) {
        if (!EqualsInternal(x.element(i), y.element(i), allow_bags,
                            element_order_spec, options)) {
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
                            field_order_spec, options)) {
          return false;
        }
      }
      return true;
    default: {
      return x.type()->ValueContentEquals(x.GetContent(), y.GetContent(),
                                          options);
    }
  }
}

struct InternalComparer {
  explicit InternalComparer(const ValueEqualityCheckOptions& options,
                            Value::DeepOrderKindSpec* order_spec_arg)
      : options(options), order_spec(order_spec_arg) {}
  size_t operator()(const zetasql::Value& x,
                    const zetasql::Value& y) const {
    return Value::EqualsInternal(x, y,
                                 /*allow_bags=*/true, order_spec, options);
  }
  const ValueEqualityCheckOptions options;
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
                                 const ValueEqualityCheckOptions& options) {
  std::string* reason = options.reason;
  using ValueCountMap =
      absl::flat_hash_map<Value, int, InternalHasher, InternalComparer>;

  InternalHasher hasher(options.float_margin);
  InternalComparer comparer(options, deep_order_spec);
  ValueCountMap x_multiset(x.num_elements(), hasher, comparer);
  ValueCountMap y_multiset(x.num_elements(), hasher, comparer);
  ZETASQL_DCHECK_EQ(x.num_elements(), y.num_elements());
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
    ZETASQL_DCHECK(!reason->empty());
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
    case TYPE_KIND_PAIR(TYPE_INTERVAL, TYPE_INTERVAL):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
    case TYPE_KIND_PAIR(TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
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
    case TYPE_KIND_PAIR(TYPE_INTERVAL, TYPE_INTERVAL):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
    case TYPE_KIND_PAIR(TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
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
// absl hashing framework.  This overrides hashes for double, float and protos
// with floating-point fields to be constants, and overrides recursive hashes to
// use this hasher. This is used for tests which want to be more lenient (i.e.
// use a float margin) on float equality. Note, this is not guaranteed to
// produce identical hash-values for float-less Value objects.
struct ValueHasherIgnoringFloat {
  const Value& v;

  template <typename H>
  friend H AbslHashValue(H h, const ValueHasherIgnoringFloat& v) {
    return HashInternal(std::move(h), v.v);
  }

  template <typename H>
  static H HashInternal(H h, const Value& v) {
    static constexpr uint64_t kFloatApproximateHashCode = 0x1192AA60660CCFABull;
    static constexpr uint64_t kDoubleApproximateHashCode =
        0x520C31647E82D8E6ull;
    static constexpr uint64_t
        kMessageWithFloatingPointFieldApproximateHashCode =
            0x1F6432686AAF52A4ull;
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
      case TYPE_PROTO: {
        if (HasFloatingPointFields(v.type()->AsProto()->descriptor())) {
          return H::combine(std::move(h),
                            kMessageWithFloatingPointFieldApproximateHashCode);
        }
        ABSL_FALLTHROUGH_INTENDED;
      }
      default:
        return AbslHashValue(std::move(h), v);
    }
  }

 private:
  static bool HasFloatingPointFields(const google::protobuf::Descriptor* d) {
    for (int i = 0; i < d->field_count(); ++i) {
      const google::protobuf::FieldDescriptor* f = d->field(i);
      if (f->type() == google::protobuf::FieldDescriptor::TYPE_FLOAT ||
          f->type() == google::protobuf::FieldDescriptor::TYPE_DOUBLE) {
        return true;
      } else if (f->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE &&
                 HasFloatingPointFields(f->message_type())) {
        return true;
      }
    }
    return false;
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
  if (!type()->Equivalent(that.type())) {
    return false;  // Behavior is undefined, so just return false.
  }

  // Note that because we don't check type for nulls, this means we may return
  // true when the type of 'this' and 'that' is different and one of them is
  // null. E.g. when comparing two enums, if 'this' is null and 'that' is not
  // null, we return true even though they may be incompatible.
  if (is_null() && !that.is_null()) return true;
  if (that.is_null()) return false;

  switch (type_kind()) {
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
      for (int i = 0; i < std::min(num_elements(), that.num_elements()); ++i) {
        if (element(i).LessThan(that.element(i))) return true;
        if (that.element(i).LessThan(element(i))) return false;
      }
      return num_elements() < that.num_elements();
    default:
      return type()->ValueContentLess(GetContent(), that.GetContent(),
                                      that.type());
  }
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
    case TYPE_KIND_PAIR(TYPE_INTERVAL, TYPE_INTERVAL):
    case TYPE_KIND_PAIR(TYPE_ENUM, TYPE_ENUM):
    case TYPE_KIND_PAIR(TYPE_NUMERIC, TYPE_NUMERIC):
    case TYPE_KIND_PAIR(TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
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
    case TYPE_KIND_PAIR(TYPE_INTERVAL, TYPE_INTERVAL):
      return Value::Bool(LessThan(that));
    case TYPE_KIND_PAIR(TYPE_BIGNUMERIC, TYPE_BIGNUMERIC):
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
    case TYPE_INTERVAL:
      return "Interval";
    case TYPE_GEOGRAPHY:
      return "Geography";
    case TYPE_NUMERIC:
      return "Numeric";
    case TYPE_BIGNUMERIC:
      return "BigNumeric";
    case TYPE_JSON:
      return "Json";
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
      ZETASQL_CHECK(type->AsProto()->descriptor() != nullptr);
      return absl::StrCat("Proto<", type->AsProto()->descriptor()->full_name(),
                          ">");
    case TYPE_EXTENDED:
      // TODO: move this logic into an appropriate function of
      // Type's interface.
      return type->ShortTypeName(ProductMode::PRODUCT_EXTERNAL);
    case TYPE_UNKNOWN:
    case __TypeKind__switch_must_have_a_default__:
      ZETASQL_LOG(FATAL) << "Unexpected type kind expected internally only: "
                 << type->kind();
  }
}

// static
std::string Value::ComplexValueToDebugString(const Value* root, bool verbose) {
  std::string result;
  struct Entry {
    const Value* value;
    size_t next_child_index;
  };
  std::stack<Entry> stack;
  stack.push(Entry{root, 0});
  do {
    const Entry top = stack.top();
    const Type* type = top.value->type();
    ZETASQL_DCHECK(type->kind() == TYPE_STRUCT || type->kind() == TYPE_ARRAY);
    ZETASQL_DCHECK(!top.value->is_null());
    const std::vector<Value>* children = nullptr;
    char closure = '\0';
    const StructType* struct_type = nullptr;
    if (type->kind() == TYPE_STRUCT) {
      if (top.next_child_index == 0) {
        if (verbose) {
          result.append(CapitalizedNameForType(type));
        }
        result.push_back('{');
      }
      children = &top.value->fields();
      closure = '}';
      struct_type = type->AsStruct();
    } else {
      if (top.next_child_index == 0) {
        if (verbose) {
          if (top.value->elements().empty()) {
            result.append(CapitalizedNameForType(type));
          } else {
            result.append("Array");
          }
        }
        if (top.value->order_kind() == kIgnoresOrder) {
          result.append("[unordered: ");
        } else {
          result.push_back('[');
        }
      }
      children = &top.value->elements();
      closure = ']';
    }
    const size_t num_children = children->size();
    size_t child_index = top.next_child_index;
    while (true) {
      if (child_index >= num_children) {
        result.push_back(closure);
        stack.pop();
        break;
      }
      if (child_index != 0) {
        result.append(", ");
      }
      const Value& child = children->at(child_index);
      if (struct_type != nullptr) {
        const std::string& field_name = struct_type->fields()[child_index].name;
        if (!field_name.empty()) {
          result.append(field_name);
          result.push_back(':');
        }
      }
      ++child_index;
      if (!child.is_null() && (child.type_kind() == TYPE_STRUCT ||
                               child.type_kind() == TYPE_ARRAY)) {
        stack.top().next_child_index = child_index;
        stack.push(Entry{&child, 0});
        break;
      }
      // For leaf nodes, it is fine to recursively call DebugString once.
      result.append(child.DebugString(verbose));
    }
  } while (!stack.empty());
  return result;
}

std::string Value::DebugString(bool verbose) const {
  if (metadata_.type_kind() == kInvalidTypeKind) {
    return "Uninitialized value";
  }
  if (!is_valid())
    return absl::StrCat("Invalid value, type_kind: ", metadata_.type_kind());
  // Note: This method previously had problems with large stack size because
  // of recursion for structs and arrays.  Using StrCat/StrAppend in particular
  // adds large stack size per argument for the AlphaNum object.
  std::string s;
  bool add_type_prefix = verbose;
  if (is_null()) {
    s = "NULL";
  } else {
    switch (type_kind()) {
      case TYPE_ARRAY:
      case TYPE_STRUCT:
        // TODO: move struct/array logic into Type subclasses.
        s = ComplexValueToDebugString(this, verbose);
        add_type_prefix = false;
        break;
      default: {
        Type::FormatValueContentOptions options;
        options.product_mode = ProductMode::PRODUCT_INTERNAL;
        options.mode = Type::FormatValueContentOptions::Mode::kDebug;
        options.verbose = verbose;

        s = type()->FormatValueContent(GetContent(), options);
        break;
      }
    }
  }

  if (add_type_prefix) {
    if (type_kind() == TYPE_PROTO && !is_null()) {
      // Proto types wrap their values using curly brackets, so don't need
      // to add additional parentheses.
      return absl::StrCat(CapitalizedNameForType(type()), s);
    }

    return absl::StrCat(CapitalizedNameForType(type()), "(", s, ")");
  }
  return s;
}

// Format will wrap arrays and structs.
std::string Value::Format() const {
  return FormatInternal(0, true /* force type */);
}

namespace {

std::string ComplexValueToString(
    const Value* root, ProductMode mode, bool as_literal,
    std::string (Value::*leaf_to_string_fn)(ProductMode mode) const) {
  std::string result;
  struct Entry {
    const Value* value;
    size_t next_child_index;
  };
  std::stack<Entry> stack;
  stack.push(Entry{root, 0});
  do {
    const Entry top = stack.top();
    const Type* type = top.value->type();
    ZETASQL_DCHECK(type->kind() == TYPE_STRUCT || type->kind() == TYPE_ARRAY);
    ZETASQL_DCHECK(!top.value->is_null());
    const std::vector<Value>* children = nullptr;
    char closure = '\0';
    if (type->kind() == TYPE_STRUCT) {
      if (top.next_child_index == 0) {
        if (!as_literal) {
          result.append(type->TypeName(mode));
          result.push_back('(');
        } else if (type->AsStruct()->num_fields() <= 1) {
          result.append("STRUCT(");
        } else {
          result.push_back('(');
        }
      }
      children = &top.value->fields();
      closure = ')';
    } else {
      if (top.next_child_index == 0) {
        if (!as_literal) {
          result.append(type->TypeName(mode));
        }
        result.push_back('[');
      }
      children = &top.value->elements();
      closure = ']';
    }
    const size_t num_children = children->size();
    size_t child_index = top.next_child_index;
    while (true) {
      if (child_index >= num_children) {
        result.push_back(closure);
        stack.pop();
        break;
      }
      if (child_index != 0) {
        result.append(", ");
      }
      const Value& child = children->at(child_index);
      ++child_index;
      if (!child.is_null() && (child.type_kind() == TYPE_STRUCT ||
                               child.type_kind() == TYPE_ARRAY)) {
        stack.top().next_child_index = child_index;
        stack.push(Entry{&child, 0});
        break;
      }
      result.append((child.*leaf_to_string_fn)(mode));
    }
  } while (!stack.empty());
  return result;
}
}  // namespace

// NOTE: There is a similar method in ../resolved_ast/sql_builder.cc.
//
// This is also basically the same as GetSQLLiteral below, except this adds
// CASTs and explicit type names so the exact value comes back out.
std::string Value::GetSQL(ProductMode mode) const {
  return GetSQLInternal<false, true>(mode);
}

// This is basically the same as GetSQL() above, except this doesn't add CASTs
// or explicit type names if the literal would be valid without them.
std::string Value::GetSQLLiteral(ProductMode mode) const {
  return GetSQLInternal<true, true>(mode);
}

template <bool as_literal, bool maybe_add_simple_type_prefix>
std::string Value::GetSQLInternal(ProductMode mode) const {
  const Type* type = this->type();

  if (is_null()) {
    return as_literal
               ? "NULL"
               : absl::StrCat("CAST(NULL AS ", type->TypeName(mode), ")");
  }

  if (type->kind() == TYPE_STRUCT || type->kind() == TYPE_ARRAY) {
    // TODO: move struct/array logic into Type subclasses.
    return ComplexValueToString(
        this, mode, as_literal,
        // For leaf nodes, it is fine to recursively call GetSQLInternal once.
        &Value::GetSQLInternal<as_literal, maybe_add_simple_type_prefix>);
  }

  Type::FormatValueContentOptions options;
  options.product_mode = mode;
  if (as_literal) {
    options.mode = maybe_add_simple_type_prefix
                       ? Type::FormatValueContentOptions::Mode::kSQLLiteral
                       : Type::FormatValueContentOptions::Mode::kDebug;
  } else {
    options.mode = Type::FormatValueContentOptions::Mode::kSQLExpression;
  }

  return type->FormatValueContent(GetContent(), options);
}

std::string RepeatString(const std::string& text, int times) {
  ZETASQL_CHECK_GE(times, 0);
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

// Returns the length of the longest line in a multi line formatted string.
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
                        const std::vector<std::string>& elements,
                        const std::string& separator, int block_indent_cols,
                        WrapStyle wrap_style) {
  // The length of the template string preceding the substitution marker.
  // This prefix may or may not have line returns.
  int prefix_len = FindSubstitutionMarker(block_template);
  // The position of the last line-return before the substitution marker
  // or minus one.
  int last_line_start = block_template.rfind('\n', prefix_len) + 1;
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
      int line_return_pos = elem.find('\n');
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
    ZETASQL_CHECK(type->AsProto()->descriptor() != nullptr);
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
    if (absl::StrContains(type_string, '\n') &&
        (elements().size() > 1 ||
         (!elements().empty() &&
          absl::StrContains(element_strings[0], '\n')))) {
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
    // Split and re-wrap the proto debug string to achieve proper indentation.
    std::vector<std::string> field_strings = absl::StrSplit(
         m->DebugString(),
        '\n', absl::SkipWhitespace());
    bool wraps = field_strings.size() > 1;
    // We don't need to sanitize the type string here since proto field names
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

Value StringArray(absl::Span<const absl::Cord* const> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(String(*v));
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

Value BytesArray(absl::Span<const absl::Cord* const> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Bytes(*v));
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

Value BigNumericArray(absl::Span<const BigNumericValue> values) {
  std::vector<Value> value_vector;
  for (auto v : values) {
    value_vector.push_back(Value::BigNumeric(v));
  }
  return Value::Array(BigNumericArrayType(), value_vector);
}

Value JsonArray(absl::Span<const JSONValue> values) {
  std::vector<Value> value_vector;
  for (const auto& v : values) {
    value_vector.push_back(Value::Json(JSONValue::CopyFrom(v.GetConstRef())));
  }
  return Value::Array(JsonArrayType(), value_vector);
}

Value UnvalidatedJsonStringArray(absl::Span<const std::string> values) {
  std::vector<Value> value_vector;
  for (const auto& v : values) {
    value_vector.push_back(Value::UnvalidatedJsonString(v));
  }
  return Value::Array(JsonArrayType(), value_vector);
}

Value TimestampArray(absl::Span<const absl::Time> values) {
  std::vector<Value> value_vector;
  for (const auto& v : values) {
    value_vector.push_back(Value::Timestamp(v));
  }
  return Value::Array(TimestampArrayType(), value_vector);
}

}  // namespace values

absl::Status Value::Serialize(ValueProto* value_proto) const {
  value_proto->Clear();
  if (is_null()) {
    return absl::OkStatus();
  }

  switch (type_kind()) {
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
    default: {
      ZETASQL_RETURN_IF_ERROR(type()->SerializeValueContent(GetContent(), value_proto));
      break;
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<Value> Value::Deserialize(const ValueProto& value_proto,
                                         const Type* type) {
  if (value_proto.value_case() == ValueProto::VALUE_NOT_SET) {
    return Null(type);
  }
  switch (type->kind()) {
    case TYPE_ARRAY: {
      if (!value_proto.has_array_value()) {
        return type->TypeMismatchError(value_proto);
      }
      std::vector<Value> elements;
      elements.reserve(value_proto.array_value().element().size());
      for (const auto& element : value_proto.array_value().element()) {
        auto status_or_value =
            Deserialize(element, type->AsArray()->element_type());
        ZETASQL_RETURN_IF_ERROR(status_or_value.status());
        elements.push_back(status_or_value.value());
      }
      return ArraySafe(type->AsArray(), std::move(elements));
    }
    case TYPE_STRUCT: {
      if (!value_proto.has_struct_value()) {
        return type->TypeMismatchError(value_proto);
      }
      const StructType* struct_type = type->AsStruct();
      if (value_proto.struct_value().field_size() !=
          struct_type->num_fields()) {
        return absl::Status(
            absl::StatusCode::kInternal,
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
    default: {
      ValueContent content;
      ZETASQL_RETURN_IF_ERROR(type->DeserializeValueContent(value_proto, &content));

      Value result(type);
      result.SetContent(content);
      return result;
    }
  }
}

ValueContent Value::GetContent() const {
  ZETASQL_DCHECK(has_content());
  // If type is less than 64bit, the padding bytes of the union uninitialized.
  // The first byte must be initialized in any case.
  // Suppress msan check for the potentially uninitialized bytes.
  ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(
      reinterpret_cast<const char*>(&int64_value_) + 1,
      sizeof(int64_value_) - 1);
  return ValueContent(int64_value_, metadata_.can_store_value_extended_content()
                                        ? metadata_.value_extended_content()
                                        : 0);
}

void Value::SetContent(const ValueContent& content) {
  ZETASQL_DCHECK(metadata_.is_valid());

  int64_value_ = content.content_;
  metadata_ = metadata_.has_type_pointer()
                  ? Metadata(metadata_.type(), /*is_null=*/false,
                             metadata_.preserves_order())
                  : Metadata(metadata_.type_kind(), /*is_null=*/false,
                             metadata_.preserves_order(),
                             content.simple_type_extended_content_);
}

Value::Metadata::Content* Value::Metadata::content() {
  static_assert(sizeof(Content) == sizeof(int64_t));
  return reinterpret_cast<Content*>(&data_);
}

const Value::Metadata::Content* Value::Metadata::content() const {
  return reinterpret_cast<const Content*>(&data_);
}

const Type* Value::Metadata::type() const {
  if (content()->has_type_pointer()) return content()->type();
  return types::TypeFromSimpleTypeKind(
      static_cast<TypeKind>(content()->kind()));
}

TypeKind Value::Metadata::type_kind() const {
  if (content()->has_type_pointer()) return content()->type()->kind();
  return static_cast<TypeKind>(content()->kind());
}

bool Value::Metadata::is_null() const { return content()->is_null(); }

bool Value::Metadata::preserves_order() const {
  return content()->preserves_order();
}

bool Value::Metadata::has_type_pointer() const {
  return content()->has_type_pointer();
}

bool Value::Metadata::can_store_value_extended_content() const {
  return !has_type_pointer();
}

int32_t Value::Metadata::value_extended_content() const {
  ZETASQL_CHECK(can_store_value_extended_content());
  return content()->value_extended_content();
}

bool Value::Metadata::is_valid() const {
  if (content()->has_type_pointer()) return true;
  return content()->kind() > 0;
}

Value::Metadata::Metadata(const Type* type, bool is_null,
                          bool preserves_order) {
  *content() = Content(type, is_null, preserves_order);
  ZETASQL_DCHECK(content()->has_type_pointer());
  ZETASQL_DCHECK(content()->type() == type);
  ZETASQL_DCHECK(content()->preserves_order() == preserves_order);
  ZETASQL_DCHECK(content()->is_null() == is_null);
}

}  // namespace zetasql
