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

#ifndef ZETASQL_PUBLIC_TYPES_VALUE_REPRESENTATIONS_H_
#define ZETASQL_PUBLIC_TYPES_VALUE_REPRESENTATIONS_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "zetasql/base/logging.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/value_content.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "zetasql/base/simple_reference_counted.h"

// This file contains classes that are used to represent values of ZetaSQL
// types. They are intended for internal use only and shouldn't be referenced
// from outside of ZetaSQL
// TODO: move each of these classes into corresponding type's .cc files,
// when type interface refactoring is complete and we don't anymore have any
// dependencies on these representations from Value class.
// TODO: make a template to represent simple type values to avoid
// duplication. We can also use this template in the future to split SimpleType
// into more granular classes.

namespace zetasql {

class ProtoType;
class Type;

namespace internal {  // For ZetaSQL internal use only

class ValueContentContainerElement {
 public:
  ValueContentContainerElement() = default;
  explicit ValueContentContainerElement(ValueContent content)
      : content_(content) {}
  bool is_null() const { return !content_.has_value(); }
  ValueContent value_content() const { return content_.value(); }

 private:
  std::optional<ValueContent> content_;
};

// b/155192766: Interface that allows classes in "type" package to access
// elements of container types as ValueContent or null. (Container types are
// ones which value consists of other Values, such as Array, Struct, and Range)
//
// For container types operations such as equality, format, and others requires
// recursively do these operations for its elements, and those elements can't
// be accessed as Value since then there will be a circular dependency
// (Value uses Type, and ArrayType, StructType, RangeType use Value)
class ValueContentContainer {
 public:
  virtual ~ValueContentContainer() = default;
  // Returns a value content of i-th element if the element
  // or nullopt if element is null
  virtual ValueContentContainerElement element(int i) const = 0;
  virtual int64_t num_elements() const = 0;
  virtual uint64_t physical_byte_size() const = 0;

  // Returns this container as const SubType*. Must only be used when it
  // is known that the object *is* this subclass.
  template <class SubType>
  const SubType* GetAs() const {
    return static_cast<const SubType*>(this);
  }
};

// -------------------------------------------------------
// ValueContentContainerRef is a ref count wrapper around a pointer to
// ValueContentContainer.
// -------------------------------------------------------
class ValueContentContainerRef final : public zetasql_base::SimpleReferenceCounted {
 public:
  explicit ValueContentContainerRef(
      std::unique_ptr<ValueContentContainer> container, bool preserves_order)
      : container_(std::move(container)), preserves_order_(preserves_order) {}

  ValueContentContainerRef(const ValueContentContainerRef&) = delete;
  ValueContentContainerRef& operator=(const ValueContentContainerRef&) = delete;

  const ValueContentContainer* const value() const { return container_.get(); }

  const uint64_t physical_byte_size() const {
    return sizeof(ValueContentContainerRef) + container_->physical_byte_size();
  }

  const bool preserves_order() const { return preserves_order_; }

 private:
  const std::unique_ptr<ValueContentContainer> container_;
  const bool preserves_order_ = false;
};

// -------------------------------------------------------
// ProtoRep
// -------------------------------------------------------
// Even though Cord is internally reference counted, ProtoRep is reference
// counted so that the internal representation can keep track of state
// associated with a ProtoRep (specifically, already deserialized fields).
class ProtoRep : public zetasql_base::SimpleReferenceCounted {
 public:
  ProtoRep(const ProtoType* type, absl::Cord value) : value_(std::move(value)) {
    ZETASQL_CHECK(type != nullptr);
  }

  ProtoRep(const ProtoRep&) = delete;
  ProtoRep& operator=(const ProtoRep&) = delete;

  const absl::Cord& value() const { return value_; }
  uint64_t physical_byte_size() const {
    return sizeof(ProtoRep) + value_.size();
  }

 private:
  const absl::Cord value_;
};

class GeographyRef final : public zetasql_base::SimpleReferenceCounted {
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
class NumericRef : public zetasql_base::SimpleReferenceCounted {
 public:
  NumericRef() {}
  explicit NumericRef(const NumericValue& value) : value_(value) {}

  NumericRef(const NumericRef&) = delete;
  NumericRef& operator=(const NumericRef&) = delete;

  const NumericValue& value() { return value_; }

 private:
  NumericValue value_;
};

// -------------------------------------------------------------
// BigNumericRef is ref count wrapper around BigNumericValue.
// -------------------------------------------------------------
class BigNumericRef : public zetasql_base::SimpleReferenceCounted {
 public:
  BigNumericRef() {}
  explicit BigNumericRef(const BigNumericValue& value) : value_(value) {}

  BigNumericRef(const BigNumericRef&) = delete;
  BigNumericRef& operator=(const BigNumericRef&) = delete;

  const BigNumericValue& value() { return value_; }

 private:
  BigNumericValue value_;
};

// -------------------------------------------------------------
// IntervalRef is ref count wrapper around IntervalValue.
// -------------------------------------------------------------
class IntervalRef : public zetasql_base::SimpleReferenceCounted {
 public:
  IntervalRef() {}
  explicit IntervalRef(const IntervalValue& value) : value_(value) {}

  IntervalRef(const IntervalRef&) = delete;
  IntervalRef& operator=(const IntervalRef&) = delete;

  const IntervalValue& value() { return value_; }

 private:
  IntervalValue value_;
};

// -------------------------------------------------------
// StringRef is ref count wrapper around string.
// -------------------------------------------------------
class StringRef : public zetasql_base::SimpleReferenceCounted {
 public:
  StringRef() {}
  explicit StringRef(std::string value) : value_(std::move(value)) {}

  StringRef(const StringRef&) = delete;
  StringRef& operator=(const StringRef&) = delete;

  const std::string& value() const { return value_; }

  uint64_t physical_byte_size() const {
    return sizeof(StringRef) + value_.size() * sizeof(char);
  }

 private:
  const std::string value_;
};

// -------------------------------------------------------
// JsonRef is ref count wrapper around JSONValue and String. The JSON value is
// either represented using a json 'document' object (DOM) or an unparsed
// string. When storing an unparsed string, there is no guarantee that the
// string is a valid JSON document. An instance of JSONValue can only store one
// of the two and not both.
// -------------------------------------------------------
class JSONRef : public zetasql_base::SimpleReferenceCounted {
 public:
  // Constructs a JSON value holding a null JSON document.
  JSONRef() {}
  // Constructs a JSON value holding an unparsed JSON string. The constructor
  // does not verify if 'str' is a valid JSON document.
  explicit JSONRef(std::string value) : value_(std::move(value)) {}
  explicit JSONRef(JSONValue value) : value_(std::move(value)) {}

  JSONRef(const JSONRef&) = delete;
  JSONRef& operator=(const JSONRef&) = delete;

  // Returns the json document representation if the value is represented
  // through the document object. Otherwrise, returns null.
  std::optional<JSONValueConstRef> document() {
    JSONValue* document = std::get_if<JSONValue>(&value_);
    if (document != nullptr) {
      return document->GetConstRef();
    }
    return std::nullopt;
  }

  // Returns the unparsed string representation if the value is represented
  // through an unparsed string. Otherwrise, returns null. There is no guarantee
  // that the unparsed string is a valid JSON document.
  const std::string* unparsed_string() const {
    return std::get_if<std::string>(&value_);
  }

  uint64_t physical_byte_size() const {
    if (std::holds_alternative<std::string>(value_)) {
      return sizeof(JSONRef) + std::get<std::string>(value_).size();
    } else {
      return sizeof(JSONRef) +
             std::get<JSONValue>(value_).GetConstRef().SpaceUsed();
    }
  }

  // Returns the string representation of the JSONValue.
  std::string ToString() const {
    if (std::holds_alternative<std::string>(value_)) {
      return std::get<std::string>(value_);
    } else {
      return std::get<JSONValue>(value_).GetConstRef().ToString();
    }
  }

 private:
  std::variant<JSONValue, std::string> value_;
};

}  // namespace internal
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_VALUE_REPRESENTATIONS_H_
