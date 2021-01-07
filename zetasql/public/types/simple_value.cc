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

#include "zetasql/public/types/simple_value.h"

namespace zetasql {

// static
SimpleValue SimpleValue::String(std::string v) {
  return SimpleValue(TYPE_STRING, std::move(v));
}

// static
SimpleValue SimpleValue::Int64(int64_t v) { return SimpleValue(TYPE_INT64, v); }

SimpleValue::SimpleValue(SimpleValue&& that) {
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(SimpleValue));
  // Invalidate 'that' to disable its destructor.
  that.type_ = TYPE_INVALID;
}

SimpleValue& SimpleValue::operator=(const SimpleValue& that) {
  // Self-copying must not clear the contents of the value.
  if (this == &that) {
    return *this;
  }
  Clear();
  CopyFrom(that);
  return *this;
}

SimpleValue& SimpleValue::operator=(SimpleValue&& that) {
  Clear();
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(SimpleValue));
  // Invalidate 'that' to disable its destructor.
  that.type_ = TYPE_INVALID;
  return *this;
}

void SimpleValue::Clear() {
  switch (type_) {
    case TYPE_STRING:
      string_ptr_->Unref();
      break;
    case TYPE_INVALID:
    case TYPE_INT64:
      // Nothing to clear.
      break;
    default:
      ZETASQL_CHECK(false) << "All ValueType must be explicitly handled in Clear()";
  }
  type_ = TYPE_INVALID;
}

int64_t SimpleValue::GetEstimatedOwnedMemoryBytesSize() const {
  switch (type_) {
    case TYPE_STRING:
      return sizeof(SimpleValue) + string_ptr_->physical_byte_size();
    case TYPE_INVALID:
    case TYPE_INT64:
      return sizeof(SimpleValue);
    default:
      ZETASQL_CHECK(false) << "All ValueType must be explicitly handled";
  }
}

void SimpleValue::CopyFrom(const SimpleValue& that) {
  // Self-copy check is done in the copy constructor. Here we just ZETASQL_DCHECK that.
  ZETASQL_DCHECK_NE(this, &that);
  // NOLINTNEXTLINE - suppress clang-tidy warning on not TriviallyCopyable.
  memcpy(this, &that, sizeof(SimpleValue));
  if (!IsValid()) {
    return;
  }
  switch (type_) {
    case TYPE_STRING:
      string_ptr_->Ref();
      break;
    case TYPE_INVALID:
    case TYPE_INT64:
      // memcpy() has copied all the data.
      break;
  }
}

int64_t SimpleValue::int64_value() const {
  ZETASQL_CHECK(has_int64_value()) << "Not an int64_t value";
  return int64_value_;
}

const std::string& SimpleValue::string_value() const {
  ZETASQL_CHECK(has_string_value()) << "Not a string value";
  return string_ptr_->value();
}

absl::Status SimpleValue::Serialize(SimpleValueProto* proto) const {
  switch (type_) {
    case SimpleValue::TYPE_INVALID:
      ZETASQL_RET_CHECK_FAIL() << "SimpleValue with TYPE_INVALID cannot be serialized";
      break;
    case SimpleValue::TYPE_INT64:
      proto->set_int64_value(int64_value());
      break;
    case SimpleValue::TYPE_STRING:
      proto->set_string_value(string_value());
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unknown ValueType: " << type_;
  }
  return absl::OkStatus();
}

// static
zetasql_base::StatusOr<SimpleValue> SimpleValue::Deserialize(
    const SimpleValueProto& proto) {
  SimpleValue value;
  switch (proto.value_case()) {
    case SimpleValueProto::kInt64Value:
      value = SimpleValue::Int64(proto.int64_value());
      break;
    case SimpleValueProto::kStringValue:
      value = SimpleValue::String(proto.string_value());
      break;
    case SimpleValueProto::VALUE_NOT_SET:
      ZETASQL_RET_CHECK_FAIL() << "No value set on SimpleValueProto::value";
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unknown simpleValueProto.value_case():"
                       << proto.value_case();
  }
  return value;
}

bool SimpleValue::Equals(const SimpleValue& that) const {
  if (type_ != that.type_) {
    return false;
  }
  switch (type_) {
    case TYPE_INT64:
      return int64_value_ == that.int64_value();
    case TYPE_STRING:
      return string_value() == that.string_value();
    case TYPE_INVALID:
      return true;
  }
}

std::string SimpleValue::DebugString() const {
  switch (type_) {
    case TYPE_INT64:
      return std::to_string(int64_value());
    case TYPE_STRING:
      return absl::StrCat("\"", string_value(), "\"");
    case TYPE_INVALID:
      return "<INVALID>";
  }
}

}  // namespace zetasql
